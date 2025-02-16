/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
#include "crane/NuRaftLogStore.h"

namespace crane {
namespace Internal {

NuRaftLogStore::NuRaftLogStore()
    : start_idx_(1),
      raft_server_bwd_pointer_(nullptr),
      disk_emul_delay(0),
      disk_emul_thread_(nullptr),
      disk_emul_thread_stop_signal_(false),
      disk_emul_last_durable_index_(0) {
  // Dummy entry for index 0.
  std::shared_ptr<buffer> buf = buffer::alloc(sz_ulong);
  logs_[0] = std::make_shared<log_entry>(0, buf);
}

NuRaftLogStore::~NuRaftLogStore() {
  if (disk_emul_thread_) {
    disk_emul_thread_stop_signal_ = true;
    disk_emul_ea_.invoke();
    if (disk_emul_thread_->joinable()) {
      disk_emul_thread_->join();
    }
  }
}

std::shared_ptr<log_entry> NuRaftLogStore::make_clone(
    const std::shared_ptr<log_entry>& entry) {
  // NOTE:
  //   Timestamp is used only when `replicate_log_timestamp_` option is on.
  //   Otherwise, log store does not need to store or load it.
  std::shared_ptr<log_entry> clone = std::make_shared<log_entry>(
      entry->get_term(), buffer::clone(entry->get_buf()), entry->get_val_type(),
      entry->get_timestamp(), entry->has_crc32(), entry->get_crc32(), false);
  return clone;
}

uint64_t NuRaftLogStore::next_slot() const {
  std::lock_guard<std::mutex> l(logs_lock_);
  // Exclude the dummy entry.
  return start_idx_ + logs_.size() - 1;
}

uint64_t NuRaftLogStore::start_index() const { return start_idx_; }

std::shared_ptr<log_entry> NuRaftLogStore::last_entry() const {
  uint64_t next_idx = next_slot();
  std::lock_guard<std::mutex> l(logs_lock_);
  auto entry = logs_.find(next_idx - 1);
  if (entry == logs_.end()) {
    entry = logs_.find(0);
  }

  return make_clone(entry->second);
}

uint64_t NuRaftLogStore::append(std::shared_ptr<log_entry>& entry) {
  std::shared_ptr<log_entry> clone = make_clone(entry);

  std::lock_guard<std::mutex> l(logs_lock_);
  size_t idx = start_idx_ + logs_.size() - 1;
  logs_[idx] = clone;

  if (disk_emul_delay) {
    uint64_t cur_time = timer_helper::get_timeofday_us();
    disk_emul_logs_being_written_[cur_time + disk_emul_delay * 1000] = idx;
    disk_emul_ea_.invoke();
  }

  return idx;
}

void NuRaftLogStore::write_at(uint64_t index,
                              std::shared_ptr<log_entry>& entry) {
  std::shared_ptr<log_entry> clone = make_clone(entry);

  // Discard all logs equal to or greater than `index.
  std::lock_guard<std::mutex> l(logs_lock_);
  auto itr = logs_.lower_bound(index);
  while (itr != logs_.end()) {
    itr = logs_.erase(itr);
  }
  logs_[index] = clone;

  if (disk_emul_delay) {
    uint64_t cur_time = timer_helper::get_timeofday_us();
    disk_emul_logs_being_written_[cur_time + disk_emul_delay * 1000] = index;

    // Remove entries greater than `index`.
    auto entry = disk_emul_logs_being_written_.begin();
    while (entry != disk_emul_logs_being_written_.end()) {
      if (entry->second > index) {
        entry = disk_emul_logs_being_written_.erase(entry);
      } else {
        entry++;
      }
    }
    disk_emul_ea_.invoke();
  }
}

std::shared_ptr<std::vector<std::shared_ptr<log_entry>>>
NuRaftLogStore::log_entries(uint64_t start, uint64_t end) {
  std::shared_ptr<std::vector<std::shared_ptr<log_entry>>> ret =
      std::make_shared<std::vector<std::shared_ptr<log_entry>>>();

  ret->resize(end - start);
  uint64_t cc = 0;
  for (uint64_t i = start; i < end; ++i) {
    std::shared_ptr<log_entry> src;
    {
      std::lock_guard<std::mutex> l(logs_lock_);
      auto entry = logs_.find(i);
      if (entry == logs_.end()) {
        entry = logs_.find(0);
        assert(0);
      }
      src = entry->second;
    }
    (*ret)[cc++] = make_clone(src);
  }
  return ret;
}

std::shared_ptr<std::vector<std::shared_ptr<log_entry>>>
NuRaftLogStore::log_entries_ext(uint64_t start, uint64_t end,
                                int64 batch_size_hint_in_bytes) {
  std::shared_ptr<std::vector<std::shared_ptr<log_entry>>> ret =
      std::make_shared<std::vector<std::shared_ptr<log_entry>>>();

  if (batch_size_hint_in_bytes < 0) {
    return ret;
  }

  size_t accum_size = 0;
  for (uint64_t ii = start; ii < end; ++ii) {
    std::shared_ptr<log_entry> src = nullptr;
    {
      std::lock_guard<std::mutex> l(logs_lock_);
      auto entry = logs_.find(ii);
      if (entry == logs_.end()) {
        entry = logs_.find(0);
        assert(0);
      }
      src = entry->second;
    }
    ret->push_back(make_clone(src));
    accum_size += src->get_buf().size();
    if (batch_size_hint_in_bytes &&
        accum_size >= (uint64_t)batch_size_hint_in_bytes)
      break;
  }
  return ret;
}

std::shared_ptr<log_entry> NuRaftLogStore::entry_at(uint64_t index) {
  std::shared_ptr<log_entry> src;
  {
    std::lock_guard<std::mutex> l(logs_lock_);
    auto entry = logs_.find(index);
    if (entry == logs_.end()) {
      entry = logs_.find(0);
    }
    src = entry->second;
  }
  return make_clone(src);
}

uint64_t NuRaftLogStore::term_at(uint64_t index) {
  uint64_t term;
  {
    std::lock_guard<std::mutex> l(logs_lock_);
    auto entry = logs_.find(index);
    if (entry == logs_.end()) {
      entry = logs_.find(0);
    }
    term = entry->second->get_term();
  }
  return term;
}

std::shared_ptr<buffer> NuRaftLogStore::pack(uint64_t index, int32 cnt) {
  std::vector<std::shared_ptr<buffer>> logs;

  size_t size_total = 0;
  for (uint64_t ii = index; ii < index + cnt; ++ii) {
    std::shared_ptr<log_entry> le;
    {
      std::lock_guard<std::mutex> l(logs_lock_);
      le = logs_[ii];
    }
    assert(le.get());
    std::shared_ptr<buffer> buf = le->serialize();
    size_total += buf->size();
    logs.push_back(buf);
  }

  std::shared_ptr<buffer> buf_out =
      buffer::alloc(sizeof(int32) + cnt * sizeof(int32) + size_total);
  buf_out->pos(0);
  buf_out->put((int32)cnt);

  for (auto& entry : logs) {
    std::shared_ptr<buffer>& bb = entry;
    buf_out->put((int32)bb->size());
    buf_out->put(*bb);
  }
  return buf_out;
}

void NuRaftLogStore::apply_pack(uint64_t index, buffer& pack) {
  pack.pos(0);
  int32 num_logs = pack.get_int();

  for (int32 ii = 0; ii < num_logs; ++ii) {
    uint64_t cur_idx = index + ii;
    int32 buf_size = pack.get_int();

    std::shared_ptr<buffer> buf_local = buffer::alloc(buf_size);
    pack.get(buf_local);

    std::shared_ptr<log_entry> le = log_entry::deserialize(*buf_local);
    {
      std::lock_guard<std::mutex> l(logs_lock_);
      logs_[cur_idx] = le;
    }
  }

  {
    std::lock_guard<std::mutex> l(logs_lock_);
    auto entry = logs_.upper_bound(0);
    if (entry != logs_.end()) {
      start_idx_ = entry->first;
    } else {
      start_idx_ = 1;
    }
  }
}

bool NuRaftLogStore::compact(uint64_t last_log_index) {
  std::lock_guard<std::mutex> l(logs_lock_);
  for (uint64_t ii = start_idx_; ii <= last_log_index; ++ii) {
    auto entry = logs_.find(ii);
    if (entry != logs_.end()) {
      logs_.erase(entry);
    }
  }

  // WARNING:
  //   Even though nothing has been erased,
  //   we should set `start_idx_` to new index.
  if (start_idx_ <= last_log_index) {
    start_idx_ = last_log_index + 1;
  }
  return true;
}

bool NuRaftLogStore::flush() {
  disk_emul_last_durable_index_ = next_slot() - 1;
  return true;
}

void NuRaftLogStore::set_disk_delay(raft_server* raft, size_t delay_ms) {
  disk_emul_delay = delay_ms;
  raft_server_bwd_pointer_ = raft;

  if (!disk_emul_thread_) {
    disk_emul_thread_ = std::unique_ptr<std::thread>(
        new std::thread(&NuRaftLogStore::disk_emul_loop, this));
  }
}

uint64_t NuRaftLogStore::last_durable_index() {
  uint64_t last_log = next_slot() - 1;
  if (!disk_emul_delay) {
    return last_log;
  }

  return disk_emul_last_durable_index_;
}

void NuRaftLogStore::disk_emul_loop() {
  // This thread mimics async disk writes.

  size_t next_sleep_us = 100 * 1000;
  while (!disk_emul_thread_stop_signal_) {
    disk_emul_ea_.wait_us(next_sleep_us);
    disk_emul_ea_.reset();
    if (disk_emul_thread_stop_signal_) break;

    uint64_t cur_time = timer_helper::get_timeofday_us();
    next_sleep_us = 100 * 1000;

    bool call_notification = false;
    {
      std::lock_guard<std::mutex> l(logs_lock_);
      // Remove all timestamps equal to or smaller than `cur_time`,
      // and pick the greatest one among them.
      auto entry = disk_emul_logs_being_written_.begin();
      while (entry != disk_emul_logs_being_written_.end()) {
        if (entry->first <= cur_time) {
          disk_emul_last_durable_index_ = entry->second;
          entry = disk_emul_logs_being_written_.erase(entry);
          call_notification = true;
        } else {
          break;
        }
      }

      entry = disk_emul_logs_being_written_.begin();
      if (entry != disk_emul_logs_being_written_.end()) {
        next_sleep_us = entry->first - cur_time;
      }
    }

    if (call_notification) {
      raft_server_bwd_pointer_->notify_log_append_completion(true);
    }
  }
}
}  // namespace Internal
}  // namespace crane
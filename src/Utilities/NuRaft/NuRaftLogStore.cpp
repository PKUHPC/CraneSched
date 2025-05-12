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

NuRaftLogStore::NuRaftLogStore(raft_server* server_ptr)
    : m_start_idx_(1),
      m_raft_server_bwd_pointer_(server_ptr),
      m_last_durable_index_(0),
      m_durable_thread_stop_signal_(false) {
  // Dummy entry for index 0.
  std::shared_ptr<buffer> buf = buffer::alloc(sz_ulong);
  m_logs_[0] = std::make_shared<log_entry>(0, buf);

  m_durable_thread_ = std::unique_ptr<std::thread>(
      new std::thread(&NuRaftLogStore::durable_thread_, this));
}

NuRaftLogStore::~NuRaftLogStore() {
  if (m_durable_thread_) {
    m_durable_thread_stop_signal_ = true;
    m_durable_ea_.invoke();
    if (m_durable_thread_->joinable()) {
      m_durable_thread_->join();
    }
  }
  // The durable thread terminates before the db client is closed
  if (m_logs_db_) {
    auto result = m_logs_db_->Close();
    if (!result)
      CRANE_ERROR("Error occurred when closing the embedded db of logs!");
  }
}

bool NuRaftLogStore::init(const std::string& db_backend,
                          const std::string& db_path) {
  if (db_backend == "Unqlite") {
#ifdef CRANE_HAVE_UNQLITE
    m_logs_db_ = std::make_unique<UnqliteDb>();
#else
    CRANE_ERROR(
        "Select unqlite as the embedded db but it's not been compiled.");
    return false;
#endif

  } else if (db_backend == "BerkeleyDB") {
#ifdef CRANE_HAVE_BERKELEY_DB
    m_logs_db_ = std::make_unique<BerkeleyDb>();
#else
    CRANE_ERROR(
        "Select Berkeley DB as the embedded db but it's not been compiled.");
    return false;
#endif

  } else {
    CRANE_ERROR("Invalid embedded database backend: {}", db_backend);
    return false;
  }

  m_has_servers_run_ = std::filesystem::exists(db_path + "_logs");

  auto result = m_logs_db_->Init(db_path + "_logs");
  if (!result) return false;

  // Restore logs from db
  m_logs_db_->IterateAllKv(
      [&](std::string&& key, std::vector<uint8_t>&& value) {
        if (!std::ranges::all_of(key, ::isdigit) || key == "0") return true;
        std::shared_ptr<buffer> buf = buffer::alloc(value.size());
        buf->put_raw(value.data(), value.size());

        buf->pos(0);
        std::shared_ptr<log_entry> entry = log_entry::deserialize(*buf);

        m_logs_[std::stoul(key)] = entry;
        return true;
      });

  if (m_logs_.size() > 1) m_start_idx_ = std::next(m_logs_.begin())->first;

  return true;
}

uint64_t NuRaftLogStore::next_slot() const {
  std::lock_guard<std::mutex> l(m_logs_lock_);
  // Exclude the dummy entry.
  return m_start_idx_ + m_logs_.size() - 1;
}

uint64_t NuRaftLogStore::start_index() const { return m_start_idx_; }

std::shared_ptr<log_entry> NuRaftLogStore::last_entry() const {
  std::lock_guard<std::mutex> l(m_logs_lock_);

  uint64_t next_idx = m_start_idx_ + m_logs_.size() - 1;
  auto entry = m_logs_.find(next_idx - 1);
  if (entry == m_logs_.end()) {
    // return dummy constant entry means there are no logs present.
    entry = m_logs_.find(0);
  }

  return make_clone_(entry->second);
}

uint64_t NuRaftLogStore::append(std::shared_ptr<log_entry>& entry) {
  std::shared_ptr<log_entry> clone = make_clone_(entry);

  std::lock_guard<std::mutex> l(m_logs_lock_);
  size_t idx = m_start_idx_ + m_logs_.size() - 1;
  m_logs_[idx] = clone;

  m_logs_being_written_.push(idx);
  m_durable_ea_.invoke();

  return idx;
}

void NuRaftLogStore::write_at(uint64_t index,
                              std::shared_ptr<log_entry>& entry) {
  std::lock_guard<std::mutex> l(m_logs_lock_);
  // Remove entries greater than `index`.
  int len = m_logs_being_written_.size();
  for (int i = 0; i < len; i++) {
    int li = m_logs_being_written_.front();
    if (li <= index) {
      m_logs_being_written_.push(li);
    }
    m_logs_being_written_.pop();
  }
  m_durable_ea_.invoke();

  txn_id_t txn_id = 0;
  auto res = m_logs_db_->Begin();
  if (res.has_value()) {
    txn_id = res.value();
  } else {
    CRANE_ERROR("Failed to begin a transaction.");
  }

  std::shared_ptr<log_entry> clone = make_clone_(entry);
  // Discard all logs equal to or greater than `index.
  auto itr = m_logs_.lower_bound(index);
  while (itr != m_logs_.end()) {
    m_logs_db_->Delete(txn_id, std::to_string(itr->first));
    itr = m_logs_.erase(itr);
  }
  m_logs_[index] = clone;
  auto buf = entry->serialize();

  m_logs_db_->Store(txn_id, std::to_string(index), buf->data(), buf->size());
  m_logs_db_->Commit(txn_id);
}

std::vector<std::shared_ptr<log_entry>> NuRaftLogStore::all_log_entries() {
  std::vector<std::shared_ptr<log_entry>> ret;

  std::lock_guard<std::mutex> l(m_logs_lock_);
  if (!m_logs_.empty()) {
    ret.resize(m_logs_.size());
    uint64_t pos = 0;
    for (const auto& log : m_logs_) {
      ret[pos++] = log.second;
    }
  }

  return ret;
}

std::shared_ptr<std::vector<std::shared_ptr<log_entry>>>
NuRaftLogStore::log_entries(uint64_t start, uint64_t end) {
  std::shared_ptr<std::vector<std::shared_ptr<log_entry>>> ret =
      std::make_shared<std::vector<std::shared_ptr<log_entry>>>();

  ret->resize(end - start);
  uint64_t pos = 0;
  for (uint64_t i = start; i < end; ++i) {
    std::shared_ptr<log_entry> src;
    {
      std::lock_guard<std::mutex> l(m_logs_lock_);
      auto entry = m_logs_.find(i);
      if (entry == m_logs_.end()) {
        entry = m_logs_.find(0);
        assert(0);
      }
      src = entry->second;
    }
    (*ret)[pos++] = make_clone_(src);
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
  for (uint64_t i = start; i < end; ++i) {
    std::shared_ptr<log_entry> src;
    {
      std::lock_guard<std::mutex> l(m_logs_lock_);
      auto entry = m_logs_.find(i);
      if (entry == m_logs_.end()) {
        entry = m_logs_.find(0);
        assert(0);
      }
      src = entry->second;
    }
    ret->push_back(make_clone_(src));
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
    std::lock_guard<std::mutex> l(m_logs_lock_);
    auto entry = m_logs_.find(index);
    if (entry == m_logs_.end()) {
      entry = m_logs_.find(0);
    }
    src = entry->second;
  }
  return make_clone_(src);
}

uint64_t NuRaftLogStore::term_at(uint64_t index) {
  uint64_t term;
  {
    std::lock_guard<std::mutex> l(m_logs_lock_);
    auto entry = m_logs_.find(index);
    if (entry == m_logs_.end()) {
      entry = m_logs_.find(0);
    }
    term = entry->second->get_term();
  }
  return term;
}

std::shared_ptr<buffer> NuRaftLogStore::pack(uint64_t index, int32 cnt) {
  std::vector<std::shared_ptr<buffer>> logs;

  size_t size_total = 0;
  for (uint64_t i = index; i < index + cnt; ++i) {
    std::shared_ptr<log_entry> le;
    {
      std::lock_guard<std::mutex> l(m_logs_lock_);
      le = m_logs_[i];
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

  for (int32 i = 0; i < num_logs; ++i) {
    uint64_t cur_idx = index + i;
    int32 buf_size = pack.get_int();

    std::shared_ptr<buffer> buf_local = buffer::alloc(buf_size);
    pack.get(buf_local);

    std::shared_ptr<log_entry> le = log_entry::deserialize(*buf_local);
    {
      std::lock_guard<std::mutex> l(m_logs_lock_);
      m_logs_[cur_idx] = le;
    }
  }

  {
    std::lock_guard<std::mutex> l(m_logs_lock_);
    auto entry = m_logs_.upper_bound(0);
    if (entry != m_logs_.end()) {
      m_start_idx_ = entry->first;
    } else {
      m_start_idx_ = 1;
    }
  }
}

bool NuRaftLogStore::compact(log_index_t last_log_index) {
  std::lock_guard<std::mutex> l(m_logs_lock_);

  txn_id_t txn_id = 0;
  auto res = m_logs_db_->Begin();
  if (res.has_value()) {
    txn_id = res.value();
  } else {
    CRANE_ERROR("Failed to begin a transaction.");
  }

  for (log_index_t i = m_start_idx_; i <= last_log_index; ++i) {
    auto entry = m_logs_.find(i);
    if (entry != m_logs_.end()) {
      m_logs_.erase(entry);
      m_logs_db_->Delete(txn_id, std::to_string(i));
    }
  }
  m_logs_db_->Commit(txn_id);

  // WARNING:
  //   Even though nothing has been erased,
  //   we should set `start_idx_` to new index.
  if (m_start_idx_ <= last_log_index) {
    m_start_idx_ = last_log_index + 1;
  }
  return true;
}

bool NuRaftLogStore::flush() {
  auto result = m_logs_db_->Begin();
  txn_id_t txn_id = 0;
  if (result.has_value()) {
    txn_id = result.value();
  } else {
    return false;
  }

  std::lock_guard<std::mutex> l(m_logs_lock_);
  for (const auto& [k, v] : m_logs_) {
    std::shared_ptr<buffer> buf = v->serialize();
    m_logs_db_->Store(txn_id, std::to_string(k), buf->data(), buf->size());
  }

  if (m_logs_db_->Commit(txn_id).has_value()) {
    m_last_durable_index_ = m_start_idx_ + m_logs_.size() - 2;
    return true;
  } else {
    return false;
  }
}

std::shared_ptr<log_entry> NuRaftLogStore::make_clone_(
    const std::shared_ptr<log_entry>& entry) {
  // NOTE:
  //   Timestamp is used only when `replicate_log_timestamp_` option is on.
  //   Otherwise, log store does not need to store or load it.
  std::shared_ptr<log_entry> clone = std::make_shared<log_entry>(
      entry->get_term(), buffer::clone(entry->get_buf()), entry->get_val_type(),
      entry->get_timestamp(), entry->has_crc32(), entry->get_crc32(), false);
  return clone;
}

void NuRaftLogStore::durable_thread_() {
  while (!m_durable_thread_stop_signal_) {
    m_durable_ea_.wait();
    m_durable_ea_.reset();
    if (m_durable_thread_stop_signal_) break;

    bool call_notification = false;
    {
      std::lock_guard<std::mutex> l(m_logs_lock_);
      // Remove all timestamps equal to or smaller than `cur_time`,
      // and pick the greatest one among them.3

      txn_id_t txn_id;
      if (!m_logs_being_written_.empty()) {
        auto res = m_logs_db_->Begin();
        if (res.has_value()) {
          txn_id = res.value();
        } else {
          CRANE_ERROR("Failed to begin a transaction.");
          return;
        }
      } else
        return;

      int max_retry = 0;
      while (!m_logs_being_written_.empty()) {
        log_index_t i = m_logs_being_written_.front();
        std::shared_ptr<buffer> buf = m_logs_[i]->serialize();
        auto res = m_logs_db_->Store(txn_id, std::to_string(i), buf->data(),
                                     buf->size());
        if (res.has_value()) {
          m_logs_being_written_.pop();
          call_notification = true;
        } else {
          max_retry++;
          if (max_retry > 5) break;
        }
      }

      m_logs_db_->Commit(txn_id);
    }

    if (call_notification && m_raft_server_bwd_pointer_) {
      m_raft_server_bwd_pointer_->notify_log_append_completion(true);
    }
  }
}

}  // namespace Internal
}  // namespace crane
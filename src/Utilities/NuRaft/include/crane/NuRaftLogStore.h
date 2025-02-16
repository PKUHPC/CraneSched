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
#pragma once

#include <atomic>
#include <cassert>
#include <map>
#include <mutex>

#include "libnuraft/event_awaiter.hxx"
#include "libnuraft/internal_timer.hxx"
#include "libnuraft/log_store.hxx"
#include "libnuraft/nuraft.hxx"
#include "libnuraft/raft_server.hxx"

namespace crane {
namespace Internal {

using namespace nuraft;

class NuRaftLogStore : public log_store {
 public:
  NuRaftLogStore();

  ~NuRaftLogStore() override;

  __nocopy__(NuRaftLogStore);

 public:
  uint64_t next_slot() const override;

  uint64_t start_index() const override;

  std::shared_ptr<log_entry> last_entry() const override;

  uint64_t append(std::shared_ptr<log_entry>& entry) override;

  void write_at(uint64_t index, std::shared_ptr<log_entry>& entry) override;

  std::shared_ptr<std::vector<std::shared_ptr<log_entry>>> log_entries(
      uint64_t start, uint64_t end) override;

  std::shared_ptr<std::vector<std::shared_ptr<log_entry>>> log_entries_ext(
      uint64_t start, uint64_t end,
      int64 batch_size_hint_in_bytes = 0) override;

  std::shared_ptr<log_entry> entry_at(uint64_t index) override;

  uint64_t term_at(uint64_t index) override;

  std::shared_ptr<buffer> pack(uint64_t index, int32 cnt) override;

  void apply_pack(uint64_t index, buffer& pack) override;

  bool compact(uint64_t last_log_index) override;

  bool flush() override;

  void close() {};

  uint64_t last_durable_index() override;

  void set_disk_delay(raft_server* raft, size_t delay_ms);

 private:
  static std::shared_ptr<log_entry> make_clone(
      const std::shared_ptr<log_entry>& entry);

  void disk_emul_loop();

  /**
   * Map of <log index, log data>.
   */
  std::map<uint64_t, std::shared_ptr<log_entry>> logs_;

  /**
   * Lock for `logs_`.
   */
  mutable std::mutex logs_lock_;

  /**
   * The index of the first log.
   */
  std::atomic<uint64_t> start_idx_;

  /**
   * Backward pointer to Raft server.
   */
  raft_server* raft_server_bwd_pointer_;

  // Testing purpose --------------- BEGIN

  /**
   * If non-zero, this log store will emulate the disk write delay.
   */
  std::atomic<size_t> disk_emul_delay;

  /**
   * Map of <timestamp, log index>, emulating logs that is being written to
   * disk. Log index will be regarded as "durable" after the corresponding
   * timestamp.
   */
  std::map<uint64_t, uint64_t> disk_emul_logs_being_written_;

  /**
   * Thread that will update `last_durable_index_` and call
   * `notify_log_append_completion` at proper time.
   */
  std::unique_ptr<std::thread> disk_emul_thread_;

  /**
   * Flag to terminate the thread.
   */
  std::atomic<bool> disk_emul_thread_stop_signal_;

  /**
   * Event awaiter that emulates disk delay.
   */
  EventAwaiter disk_emul_ea_;

  /**
   * Last written log index.
   */
  std::atomic<uint64_t> disk_emul_last_durable_index_;

  // Testing purpose --------------- END
};
}  // namespace Internal
}  // namespace crane

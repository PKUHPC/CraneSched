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
#include <queue>

#include "crane/EmbeddedDb.h"
#include "crane/Logger.h"
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
  using log_index_t = uint64_t;

  NuRaftLogStore(raft_server* server_ptr);

  ~NuRaftLogStore() override;

  __nocopy__(NuRaftLogStore);

 public:
  bool init(const std::string& db_backend, const std::string& db_path);

  uint64_t next_slot() const override;

  uint64_t start_index() const override;

  std::shared_ptr<log_entry> last_entry() const override;

  uint64_t append(std::shared_ptr<log_entry>& entry) override;

  void write_at(log_index_t index, std::shared_ptr<log_entry>& entry) override;

  std::vector<std::shared_ptr<log_entry>> all_log_entries();

  std::shared_ptr<std::vector<std::shared_ptr<log_entry>>> log_entries(
      log_index_t start, log_index_t end) override;

  std::shared_ptr<std::vector<std::shared_ptr<log_entry>>> log_entries_ext(
      log_index_t start, log_index_t end,
      int64 batch_size_hint_in_bytes = 0) override;

  std::shared_ptr<log_entry> entry_at(log_index_t index) override;

  log_index_t term_at(log_index_t index) override;

  std::shared_ptr<buffer> pack(log_index_t index, int32 cnt) override;

  void apply_pack(log_index_t index, buffer& pack) override;

  bool compact(log_index_t last_log_index) override;

  bool flush() override;

  uint64_t last_durable_index() override { return m_last_durable_index_; }

  bool ExternElemStore(const std::string& key, const void* data,
                       size_t len) const {
    if (key.empty() || std::ranges::all_of(key, ::isdigit)) {
      CRANE_ERROR("Illegal key to store in log db.");
      return false;
    }

    return m_logs_db_->Store(0, key, data, len).has_value();
  }

  std::shared_ptr<buffer> ExternElemFetch(const std::string& key) const {
    size_t n_bytes{0};

    auto result = m_logs_db_->Fetch(0, key, nullptr, &n_bytes);
    if (!result) {
      if (result.error() != kNotFound) {
        CRANE_ERROR("Unexpected error when fetching the size of string key '{}'",
                    key);
      }
      return nullptr;
    }

    std::shared_ptr<buffer> buf = buffer::alloc(n_bytes);

    result = m_logs_db_->Fetch(0, key, buf->data(), &n_bytes);
    if (!result) {
      CRANE_ERROR("Unexpected error when fetching the data of string key '{}'",
                  key);
      return nullptr;
    }

    return buf;
  }

  bool HasServersRun() const { return m_has_servers_run_;}

  void get_all_keys() {
    std::cout << "keys" << std::endl;
    m_logs_db_->IterateAllKv(
        [&](std::string&& key, std::vector<uint8_t>&& value) {
          std::cout << key << std::endl;
          return true;
        });
  };

 private:
  static std::shared_ptr<log_entry> make_clone_(
      const std::shared_ptr<log_entry>& entry);

  //  void disk_emul_loop();
  void durable_thread_();

  /**
   * Map of <log index, log data>.
   */
  std::map<log_index_t, std::shared_ptr<log_entry>> m_logs_;

  /**
   * Lock for `logs_`.
   */
  mutable std::mutex logs_lock_;

  /**
   * The index of the first log.
   */
  std::atomic<log_index_t> start_idx_;

  /**
   * Backward pointer to Raft server.
   */
  raft_server* raft_server_bwd_pointer_;

  /**
   * Last written log index.
   */
  std::atomic<log_index_t> m_last_durable_index_;

  /**
   * logs that is being written to disk.
   */
  std::queue<log_index_t> m_logs_being_written_;

  /**
   * Thread that will update `last_durable_index_` and call
   * `notify_log_append_completion` at proper time.
   */
  std::unique_ptr<std::thread> m_durable_thread_;

  /**
   * Flag to terminate the thread.
   */
  std::atomic<bool> m_durable_thread_stop_signal_;

  /**
   * Event awaiter
   */
  EventAwaiter m_durable_ea_;

  /**
   *  log is durable after being written to db
   */
  std::unique_ptr<IEmbeddedDb> m_logs_db_;

  bool m_has_servers_run_;
};
}  // namespace Internal
}  // namespace crane

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

#include "NuRaftLogStore.h"
#include "libnuraft/nuraft.hxx"

namespace crane {
namespace Internal {

using namespace nuraft;

class NuRaftStateManager : public state_mgr {
 public:
  NuRaftStateManager(int srv_id, const std::string& endpoint,
                     raft_server* server_ptr)
      : m_id_(srv_id),
        m_endpoint_(endpoint),
        m_log_store_(std::make_shared<NuRaftLogStore>(server_ptr)) {
    m_srv_config_ = std::make_shared<srv_config>(srv_id, endpoint);

    // Initial cluster config: contains only one server (myself).
    m_saved_config_ = std::make_shared<cluster_config>();
    m_saved_config_->get_servers().push_back(m_srv_config_);

    m_log_store_->init(g_config.CraneEmbeddedDbBackend,
                       g_config.CraneCtldDbPath);
  }

  ~NuRaftStateManager() override = default;

  std::shared_ptr<cluster_config> load_config() override {
    auto buf = m_log_store_->ExternElemFetch(s_config_key_str_);
    if (buf) m_saved_config_ = cluster_config::deserialize(*buf);

    return m_saved_config_;
  }

  void save_config(const cluster_config& config) override {
    std::shared_ptr<buffer> buf = config.serialize();
    m_log_store_->ExternElemStore(s_config_key_str_, buf->data(), buf->size());
    m_saved_config_ = cluster_config::deserialize(*buf);
  }

  void save_state(const srv_state& state) override {
    std::shared_ptr<buffer> buf = state.serialize();
    m_log_store_->ExternElemStore(s_state_key_str_, buf->data(), buf->size());
    m_saved_state_ = srv_state::deserialize(*buf);
  }

  std::shared_ptr<srv_state> read_state() override {
    auto buf = m_log_store_->ExternElemFetch(s_state_key_str_);
    if (buf) m_saved_state_ = srv_state::deserialize(*buf);

    return m_saved_state_;
  }

  std::shared_ptr<log_store> load_log_store() override { return m_log_store_; }

  int32 server_id() override { return m_id_; }

  void system_exit(const int exit_code) override {
    CRANE_CRITICAL("The Raft server terminated abnormally, exit code : {}",
                   exit_code);
    //    exit(exit_code);
  }

  std::shared_ptr<srv_config> get_srv_config() const { return m_srv_config_; }

  bool HasServersRun() const { return m_log_store_->HasServersRun(); }

  void get_all_keys() { m_log_store_->get_all_keys(); }

 private:
  int m_id_;
  std::string m_endpoint_;
  std::shared_ptr<NuRaftLogStore> m_log_store_;
  std::shared_ptr<srv_config> m_srv_config_;
  std::shared_ptr<cluster_config> m_saved_config_;
  std::shared_ptr<srv_state> m_saved_state_;
  inline static std::string const s_config_key_str_{"CF"};
  inline static std::string const s_state_key_str_{"ST"};
};
}  // namespace Internal
}  // namespace crane

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
  NuRaftStateManager(int srv_id, const std::string& endpoint)
      : my_id_(srv_id),
        my_endpoint_(endpoint),
        cur_log_store_(std::make_shared<NuRaftLogStore>()) {
    my_srv_config_ = std::make_shared<srv_config>(srv_id, endpoint);

    // Initial cluster config: contains only one server (myself).
    saved_config_ = std::make_shared<cluster_config>();
    saved_config_->get_servers().push_back(my_srv_config_);
  }

  ~NuRaftStateManager() override = default;

  std::shared_ptr<cluster_config> load_config() override {
    // Just return in-memory data in this example.
    // May require reading from disk here, if it has been written to disk.
    return saved_config_;
  }

  void save_config(const cluster_config& config) override {
    // Just keep in memory in this example.
    // Need to write to disk here, if want to make it durable.
    std::shared_ptr<buffer> buf = config.serialize();
    saved_config_ = cluster_config::deserialize(*buf);
  }

  void save_state(const srv_state& state) override {
    // Just keep in memory in this example.
    // Need to write to disk here, if want to make it durable.
    std::shared_ptr<buffer> buf = state.serialize();
    saved_state_ = srv_state::deserialize(*buf);
  }

  std::shared_ptr<srv_state> read_state() override {
    // Just return in-memory data in this example.
    // May require reading from disk here, if it has been written to disk.
    return saved_state_;
  }

  std::shared_ptr<log_store> load_log_store() override {
    return cur_log_store_;
  }

  int32 server_id() override { return my_id_; }

  void system_exit(const int exit_code) override {}

  std::shared_ptr<srv_config> get_srv_config() const { return my_srv_config_; }

 private:
  int my_id_;
  std::string my_endpoint_;
  std::shared_ptr<NuRaftLogStore> cur_log_store_;
  std::shared_ptr<srv_config> my_srv_config_;
  std::shared_ptr<cluster_config> saved_config_;
  std::shared_ptr<srv_state> saved_state_;
};
}  // namespace Internal
}  // namespace crane

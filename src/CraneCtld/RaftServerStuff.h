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

#include <libnuraft/nuraft.hxx>
#include <memory>

#include "CraneStateMachine.h"
#include "TaskScheduler.h"
#include "crane/NuRaftLogStore.h"
#include "crane/NuRaftLoggerWrapper.h"
#include "crane/NuRaftStateManager.h"
#include "protos/Crane.grpc.pb.h"

namespace Ctld {

class CraneStateMachine;

using namespace nuraft;

class RaftServerStuffBase {
 public:
  using raft_result = cmd_result<std::shared_ptr<buffer>>;

  RaftServerStuffBase() = default;

  virtual ~RaftServerStuffBase() = default;

  virtual void Init() {}

  virtual void Shutdown() {}

  virtual bool CheckServerNodeExist(int server_id) const {
    if (server_id == m_default_server_id_)
      return true;
    else
      return false;
  }

  virtual bool AddServerAsync(int server_id, const std::string& endpoint) {
    return false;
  }

  virtual bool AddServer(int server_id, const std::string& endpoint) {
    return false;
  }

  virtual bool RemoveServer(int server_id) { return false; }

  virtual bool AppendLog(std::shared_ptr<nuraft::buffer> new_log) {
    return false;
  }

  // virtual bool RegisterToLeader(const std::string& leader_hostname,
  //                               const std::string& grpc_port) {
  //   return false;
  // }

  virtual void YieldLeadership(int next_leader_id) const {}

  virtual int GetLeaderId() const { return m_default_server_id_; }

  virtual bool IsLeader() const { return true; }

  virtual int GetServerId() const { return m_default_server_id_; }

  virtual CraneStateMachine* GetStateMachine() { return nullptr; }

  virtual void GetNodeStatus(crane::grpc::QueryLeaderInfoReply* response) {}

 private:
  int m_default_server_id_ = 0;
};

class RaftServerStuff final : public RaftServerStuffBase {
 public:
  explicit RaftServerStuff(int server_id, const std::string& hostname, int port)
      : m_server_id_(server_id),
        m_port_(port),
        m_endpoint_(fmt::format("{}:{}", hostname, port)) {};

  ~RaftServerStuff() override { Shutdown(); }

  void Init() override;

  void Shutdown() override;

  bool CheckServerNodeExist(int server_id) const override;

  bool AddServerAsync(int server_id, const std::string& endpoint) override;

  bool AddServer(int server_id, const std::string& endpoint) override;

  bool RemoveServer(int server_id) override;

  bool AppendLog(std::shared_ptr<nuraft::buffer> new_log) override;

  // bool RegisterToLeader(const std::string& leader_hostname,
  //                       const std::string& grpc_port) override;

  void YieldLeadership(int next_leader_id = -1) const override {
    if (next_leader_id == m_server_id_) return;
    return m_raft_instance_->yield_leadership(false, next_leader_id);
  };

  int GetLeaderId() const override { return m_raft_instance_->get_leader(); };

  bool IsLeader() const override { return m_raft_instance_->is_leader(); };

  int GetServerId() const override { return m_server_id_; };

  CraneStateMachine* GetStateMachine() override;

  void GetNodeStatus(crane::grpc::QueryLeaderInfoReply* response) override;

  // void get_all_keys() {
  //   static_cast<crane::Internal::NuRaftStateManager*>(m_state_mgr_.get())
  //       ->get_all_keys();
  // };

 private:
  void handle_result(raft_result& result, ptr<std::exception>& err);

  static cb_func::ReturnCode StatusChangeCallback(cb_func::Type type,
                                                  cb_func::Param* p);
  int m_server_id_;
  std::string m_endpoint_;
  int m_port_;

  std::atomic<bool> m_exit_ = false;

  // State machine.
  std::shared_ptr<state_machine> m_state_machine_;

  // State manager.
  std::shared_ptr<state_mgr> m_state_mgr_;

  // Logger
  std::shared_ptr<logger> m_logger_;

  // Raft launcher.
  raft_launcher m_launcher_;

  // Raft server instance.
  std::shared_ptr<raft_server> m_raft_instance_;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::RaftServerStuffBase> g_raft_server;
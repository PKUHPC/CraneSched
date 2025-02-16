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
#include "CtldPublicDefs.h"
#include "TaskScheduler.h"
#include "crane/NuRaftLogStore.h"
#include "crane/NuRaftLoggerWrapper.h"
#include "crane/NuRaftStateManager.h"

namespace Ctld {

class CraneStateMachine;

using namespace nuraft;

class RaftServerStuff {
 public:
  RaftServerStuff(int server_id, const std::string& ip, int port)
      : m_server_id_(server_id), m_ip_(ip), m_port_(port){};

  ~RaftServerStuff();

  void Init();

  bool AddServerAsync(int server_id, const std::string& endpoint);

  bool AddServer(int server_id, const std::string& endpoint);

  void server_list();

  bool AppendLog(std::shared_ptr<nuraft::buffer> new_log);

  int GetLeaderId() { return m_raft_instance_->get_leader(); };

  bool IsLeader() { return m_raft_instance_->is_leader(); };

  CraneStateMachine* GetStateMachine();

  void print_status() {
    std::shared_ptr<nuraft::log_store> ls = m_state_mgr_->load_log_store();

    std::cout << "my server id: " << m_server_id_ << std::endl
              << "leader id: " << m_raft_instance_->get_leader() << std::endl
              << "Raft log range: ";
    if (ls->start_index() >= ls->next_slot()) {
      // Start index can be the same as next slot when the log store is empty.
      std::cout << "(empty)" << std::endl;
    } else {
      std::cout << ls->start_index() << " - " << (ls->next_slot() - 1)
                << std::endl;
    }
    std::cout << "last committed index: "
              << m_raft_instance_->get_committed_log_idx() << std::endl
              << "current term: " << m_raft_instance_->get_term() << std::endl
              << "last snapshot log index: "
              << (m_state_machine_->last_snapshot()
                      ? m_state_machine_->last_snapshot()->get_last_log_idx()
                      : 0)
              << std::endl
              << "last snapshot log term: "
              << (m_state_machine_->last_snapshot()
                      ? m_state_machine_->last_snapshot()->get_last_log_term()
                      : 0)
              << std::endl;
  }

 private:
  static cb_func::ReturnCode StatusChangeCallback(cb_func::Type type,
                                                  cb_func::Param* p);
  int m_server_id_;
  std::string m_ip_;
  int m_port_;

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

inline std::unique_ptr<Ctld::RaftServerStuff> g_raft_server;
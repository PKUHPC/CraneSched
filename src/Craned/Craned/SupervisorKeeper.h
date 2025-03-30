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

#include "CranedPublicDefs.h"
// Precompiled header comes first.

#include <protos/Supervisor.grpc.pb.h>

#include "JobManager.h"
#include "crane/AtomicHashMap.h"
namespace Craned {

class SupervisorClient {
 public:
  CraneExpected<pid_t> ExecuteTask(const crane::grpc::TaskToD& task);

  CraneExpected<std::pair<task_id_t, pid_t>> CheckTaskStatus();

  CraneErrCode TerminateTask(bool mark_as_orphaned, bool terminated_by_user);
  CraneErrCode ChangeTaskTimeLimit(absl::Duration time_limit);

  void InitChannelAndStub(const std::string& endpoint);

 private:
  std::shared_ptr<grpc::Channel> m_channel_;

  std::unique_ptr<crane::grpc::supervisor::Supervisor::Stub> m_stub_;
};

class SupervisorKeeper {
 public:
  SupervisorKeeper() = default;
  ~SupervisorKeeper() {
    CRANE_DEBUG("Destroy SupervisorKeeper.");
  }

  /**
   * @brief Query all existing supervisor for task they hold.
   * @return job_id and pid from supervisors
   */
  CraneExpected<std::unordered_map<task_id_t, pid_t>> Init();

  void AddSupervisor(task_id_t task_id);
  void RemoveSupervisor(task_id_t task_id);

  std::shared_ptr<SupervisorClient> GetStub(task_id_t task_id);

 private:
  absl::flat_hash_map<task_id_t, std::shared_ptr<SupervisorClient>>
      m_supervisor_map;
  absl::Mutex m_mutex;
};
}  // namespace Craned

inline std::unique_ptr<Craned::SupervisorKeeper> g_supervisor_keeper;
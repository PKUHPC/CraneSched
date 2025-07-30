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

class SupervisorStub {
 public:
  CraneErrCode ExecuteTask();
  CraneExpected<EnvMap> QueryStepEnv();
  CraneExpected<std::pair<task_id_t, pid_t>> CheckStatus();

  CraneErrCode TerminateTask(bool mark_as_orphaned, bool terminated_by_user);
  CraneErrCode ChangeTaskTimeLimit(absl::Duration time_limit);
  CraneErrCode ShutdownSupervisor();
  CraneErrCode ReceivePmixPort(task_id_t task_id, uint32_t pmix_port, const std::string& craned_id);

  void InitChannelAndStub(const std::string& endpoint);

 private:
  std::shared_ptr<grpc::Channel> m_channel_;

  std::unique_ptr<crane::grpc::supervisor::Supervisor::Stub> m_stub_;
};

class SupervisorKeeper {
 public:
  SupervisorKeeper() = default;
  ~SupervisorKeeper() = default;

  SupervisorKeeper(const SupervisorKeeper&) = delete;
  SupervisorKeeper& operator=(const SupervisorKeeper&) = delete;

  SupervisorKeeper(SupervisorKeeper&&) = delete;
  SupervisorKeeper& operator=(SupervisorKeeper&&) = delete;

  /**
   * @brief Query all existing supervisor for task they hold.
   * @return job_id and pid from supervisors. Error when socket file
   * scanning fails, supervisors are unreachable, or task status queries fail
   * with specific error codes.
   */
  CraneExpected<std::unordered_map<task_id_t, pid_t>> InitAndGetRecoveredMap();

  void AddSupervisor(task_id_t task_id);
  void RemoveSupervisor(task_id_t task_id);

  std::shared_ptr<SupervisorStub> GetStub(task_id_t task_id);

  std::set<task_id_t> GetRunningSteps();

 private:
  absl::flat_hash_map<task_id_t, std::shared_ptr<SupervisorStub>>
      m_supervisor_map_;
  absl::Mutex m_mutex_;
};
}  // namespace Craned

inline std::unique_ptr<Craned::SupervisorKeeper> g_supervisor_keeper;
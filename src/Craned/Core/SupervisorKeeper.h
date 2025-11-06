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

namespace Craned {

class SupervisorStub {
 public:
  SupervisorStub(job_id_t job_id, step_id_t step_id);
  SupervisorStub(const std::string& endpoint);
  CraneErrCode ExecuteStep();
  CraneExpected<EnvMap> QueryStepEnv();
  CraneExpected<std::tuple<job_id_t, step_id_t, pid_t, StepStatus>>
  CheckStatus();

  CraneErrCode TerminateTask(bool mark_as_orphaned, bool terminated_by_user);
  CraneErrCode ChangeTaskTimeLimit(absl::Duration time_limit);
  CraneErrCode ShutdownSupervisor();

 private:
  void InitChannelAndStub_(const std::string& endpoint);
  std::shared_ptr<grpc::Channel> m_channel_;

  std::unique_ptr<crane::grpc::supervisor::Supervisor::Stub> m_stub_;
};

class SupervisorManager {
 public:
  struct SupervisorRecoverInfo {
    pid_t pid;
    StepStatus status;
    std::shared_ptr<SupervisorStub> supervisor_stub;
  };
  /**
   * @brief Query all existing supervisor for task they hold.
   * @return job_id and pid from supervisors. Error when socket file
   * scanning fails, supervisors are unreachable, or task status queries fail
   * with specific error codes.
   */
  [[nodiscard]] static CraneExpected<absl::flat_hash_map<
      std::pair<job_id_t, step_id_t>, SupervisorRecoverInfo>>
  InitAndGetRecoveredMap();
};
}  // namespace Craned

/**
 * Copyright (c) 2025 Peking University and Peking University
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

#include "SupervisorManager.h"

namespace Craned {

struct StepInstance {
  job_id_t job_id;
  step_id_t step_id;

  pid_t supv_pid;

  crane::grpc::StepToD step_to_d;

  std::atomic_bool err_before_supv_start{false};

  StepStatus status{StepStatus::Invalid};
  std::shared_ptr<SupervisorStub> supervisor_stub{nullptr};
  std::unique_ptr<CgroupInterface> crane_cgroup{nullptr};
  std::unique_ptr<CgroupInterface> user_cgroup{nullptr};

  explicit StepInstance(const crane::grpc::StepToD& step_to_d);
  // For step recovery
  explicit StepInstance(const crane::grpc::StepToD& step_to_d, pid_t supv_pid,
                        StepStatus status,
                        std::shared_ptr<SupervisorStub> supervisor_stub);
  ~StepInstance() = default;
  void CleanUp();

  [[nodiscard]] bool IsDaemonStep() const noexcept {
    return step_to_d.step_type() == crane::grpc::StepType::DAEMON;
  }

  [[nodiscard]] bool IsContainer() const noexcept {
    return step_to_d.has_container_meta();
  }

  [[nodiscard]] bool IsRunning() const noexcept {
    return status == StepStatus::Running;
  }

  [[nodiscard]] std::string StepIdString() const noexcept {
    return std::format("{}.{}", job_id, step_id);
  }

  CraneErrCode CreateCg();

  CraneErrCode SpawnSupervisor(const EnvMap& job_env_map);

  void GotNewStatus(const StepStatus& new_status);

  void ExecuteStepAsync();

  CraneExpected<void> TerminateStep(bool mark_as_orphaned,
                                    bool terminated_by_user);

 private:
  /**
   * Send a signal to the process group of pid. For kill uninitialized
   * Supervisor only.
   * This function ASSUMES that ALL processes belongs to the
   * process group with the PGID set to the PID of the first process in this
   * TaskExecutionInstance.
   * @param signum the value of signal.
   * @return if the signal is sent successfully, kOk is returned.
   * otherwise, kGenericFailure is returned.
   */
  static CraneErrCode KillPid_(pid_t pid, int signum);
};
}  // namespace Craned

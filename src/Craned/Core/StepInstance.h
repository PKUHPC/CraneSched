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

#include "SupervisorStub.h"

namespace Craned {

struct StepInstance {
  job_id_t job_id;
  step_id_t step_id;

  pid_t supv_pid;

  crane::grpc::StepToD step_to_d;

  std::atomic_bool err_before_supv_start{false};
  // Stored when Completing is received from supervisor.
  // Used by FreeJobs to send the real terminal status after cleanup.
  struct PendingTerminalStatus {
    crane::grpc::JobStatus final_status;
    uint32_t exit_code;
    std::string reason;
    google::protobuf::Timestamp timestamp;
  };
  std::optional<PendingTerminalStatus> pending_terminal_status;

  // When true, status changes from this step are NOT forwarded to CraneCtld.
  // Used for invalid steps (not tracked by CraneCtld) that need local cleanup.
  bool silent_cleanup{false};

  StepStatus status{StepStatus::Invalid};
  std::shared_ptr<SupervisorStub> supervisor_stub{nullptr};

  // job_id/step_id/system group
  std::unique_ptr<CgroupInterface> crane_cgroup{nullptr};

  // Cgroup str for this step's system cgroup (w/o "crane/" prefix).
  // e.g. "job_1/step_0/system" (v1) or "overflow/job_1/step_0/system" (v2).
  std::string cg_str;

  // Job's cgroup path info (for constructing child paths and cpuset migration).
  Common::CgroupPathInfo job_path_info;

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

  CraneErrCode Prepare(const Common::CgroupPathInfo& job_path_info);

  CraneErrCode SpawnSupervisor(const EnvMap& job_env_map);

  void GotNewStatus(const StepStatus& new_status);

  void ExecuteStepAsync();

  // Not implemented yet.
  CraneExpected<void> TerminateStep(
      crane::grpc::TerminateSource terminate_source);
};
}  // namespace Craned

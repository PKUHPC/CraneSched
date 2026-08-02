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

namespace Craned {

enum class SupervisorRunState : uint8_t {
  Untracked,
  Running,
  Exited,
};

struct SupervisorExitEvent {
  StepKey step_key;
  pid_t pid;
  uint32_t exit_code;
  std::string reason;
};

// Tracks only Supervisor process facts. Step completion and cleanup remain
// owned by JobManager.
class SupervisorProcessTracker {
 public:
  using Clock = std::chrono::steady_clock;
  using TimePoint = Clock::time_point;

  void RegisterLocal(const StepKey& key, pid_t pid);
  void RegisterRecovered(const StepKey& key, pid_t pid);

  void ObserveChildExit(pid_t pid, int wait_status,
                        TimePoint observed_at = Clock::now());
  void ObserveMissing(const StepKey& key, pid_t pid,
                      TimePoint observed_at = Clock::now());

  // Probes recovered Supervisors and returns newly expired exit observations.
  std::vector<SupervisorExitEvent> Poll(TimePoint now = Clock::now());

  SupervisorRunState GetRunState(const StepKey& key, pid_t pid) const;
  void Unregister(const StepKey& key, pid_t pid);

 private:
  enum class Origin : uint8_t {
    LocalChild,
    Recovered,
  };

  struct Running {};

  struct ExitObserved {
    uint32_t exit_code;
    std::string reason;
    TimePoint terminal_status_deadline;
  };

  struct ExitReconciled {};

  using ProcessState = std::variant<Running, ExitObserved, ExitReconciled>;

  struct TrackedSupervisor {
    StepKey step_key;
    Origin origin;
    ProcessState state;
  };

  void Register_(const StepKey& key, pid_t pid, Origin origin);
  void ObserveExit_(const StepKey& key, pid_t pid, uint32_t exit_code,
                    std::string reason, TimePoint observed_at);

  mutable absl::Mutex m_mtx_;
  absl::flat_hash_map<pid_t, TrackedSupervisor> m_supervisors_
      ABSL_GUARDED_BY(m_mtx_);
  absl::flat_hash_map<pid_t, ExitObserved> m_early_child_exits_
      ABSL_GUARDED_BY(m_mtx_);
};

}  // namespace Craned

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

#include "SupervisorProcessTracker.h"

#include <sys/wait.h>

#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstring>
#include <utility>

namespace Craned {

using namespace std::chrono_literals;

namespace {

constexpr auto kSupervisorExitStatusGrace = 2s;
constexpr auto kUnregisteredChildExitRetention = 10s;

std::optional<std::pair<uint32_t, std::string>> BuildSupervisorExitInfo(
    pid_t pid, int status) {
  if (WIFSIGNALED(status)) {
    int signal = WTERMSIG(status);
    return std::make_pair(
        static_cast<uint32_t>(ExitCode::kTerminationSignalBase + signal),
        fmt::format("Supervisor pid {} exited unexpectedly due to signal {}.",
                    pid, signal));
  }

  if (WIFEXITED(status)) {
    int exit_code = WEXITSTATUS(status);
    return std::make_pair(
        static_cast<uint32_t>(exit_code),
        fmt::format("Supervisor pid {} exited unexpectedly with code {}.", pid,
                    exit_code));
  }

  return std::nullopt;
}

}  // namespace

void SupervisorProcessTracker::RegisterLocal(const StepKey& key, pid_t pid) {
  Register_(key, pid, Origin::LocalChild);
}

void SupervisorProcessTracker::RegisterRecovered(const StepKey& key,
                                                 pid_t pid) {
  Register_(key, pid, Origin::Recovered);
}

void SupervisorProcessTracker::Register_(const StepKey& key, pid_t pid,
                                         Origin origin) {
  if (pid <= 0) return;

  absl::MutexLock lock(&m_mtx_);
  auto tracked_it = m_supervisors_.find(pid);
  if (tracked_it != m_supervisors_.end() &&
      tracked_it->second.step_key != key) {
    CRANE_WARN(
        "Supervisor pid {} was registered for Step #{}.{} and is now reused "
        "by Step #{}.{}; replacing the stale registration.",
        pid, tracked_it->second.step_key.first,
        tracked_it->second.step_key.second, key.first, key.second);
    m_supervisors_.erase(tracked_it);
    tracked_it = m_supervisors_.end();
  }

  if (tracked_it == m_supervisors_.end()) {
    ProcessState state = Running{};
    if (auto early_it = m_early_child_exits_.find(pid);
        early_it != m_early_child_exits_.end()) {
      state = std::move(early_it->second);
      m_early_child_exits_.erase(early_it);
      CRANE_DEBUG(
          "[Step #{}.{}] Matched Supervisor pid {} to an already reaped "
          "child.",
          key.first, key.second, pid);
    }
    m_supervisors_.emplace(pid, TrackedSupervisor{.step_key = key,
                                                  .origin = origin,
                                                  .state = std::move(state)});
    return;
  }

  tracked_it->second.origin = origin;
}

void SupervisorProcessTracker::ObserveChildExit(pid_t pid, int wait_status,
                                                TimePoint observed_at) {
  auto exit_info = BuildSupervisorExitInfo(pid, wait_status);
  if (!exit_info.has_value()) {
    CRANE_TRACE("Ignoring non-terminal child status {} for pid {}", wait_status,
                pid);
    return;
  }

  std::optional<StepKey> key;
  {
    absl::MutexLock lock(&m_mtx_);
    auto tracked_it = m_supervisors_.find(pid);
    if (tracked_it == m_supervisors_.end()) {
      m_early_child_exits_.try_emplace(
          pid, ExitObserved{.exit_code = exit_info->first,
                            .reason = std::move(exit_info->second),
                            .terminal_status_deadline =
                                observed_at + kSupervisorExitStatusGrace});
    } else {
      key = tracked_it->second.step_key;
      if (std::holds_alternative<Running>(tracked_it->second.state)) {
        tracked_it->second.state =
            ExitObserved{.exit_code = exit_info->first,
                         .reason = std::move(exit_info->second),
                         .terminal_status_deadline =
                             observed_at + kSupervisorExitStatusGrace};
      }
    }
  }

  if (key.has_value()) {
    CRANE_DEBUG("[Step #{}.{}] Reaped Supervisor pid {}.", key->first,
                key->second, pid);
  } else {
    CRANE_TRACE(
        "Reaped unregistered child pid {}; retaining its status briefly in "
        "case Supervisor registration is still in flight.",
        pid);
  }
}

void SupervisorProcessTracker::ObserveMissing(const StepKey& key, pid_t pid,
                                              TimePoint observed_at) {
  ObserveExit_(key, pid, ExitCode::EC_RPC_ERR,
               fmt::format("Supervisor pid {} is no longer running.", pid),
               observed_at);
}

void SupervisorProcessTracker::ObserveExit_(const StepKey& key, pid_t pid,
                                            uint32_t exit_code,
                                            std::string reason,
                                            TimePoint observed_at) {
  if (pid <= 0) return;

  absl::MutexLock lock(&m_mtx_);
  auto tracked_it = m_supervisors_.find(pid);
  if (tracked_it != m_supervisors_.end() &&
      tracked_it->second.step_key != key) {
    return;
  }

  if (tracked_it == m_supervisors_.end()) {
    tracked_it =
        m_supervisors_
            .emplace(pid, TrackedSupervisor{.step_key = key,
                                            .origin = Origin::Recovered,
                                            .state = Running{}})
            .first;
  }

  if (!std::holds_alternative<Running>(tracked_it->second.state)) return;
  tracked_it->second.state = ExitObserved{
      .exit_code = exit_code,
      .reason = std::move(reason),
      .terminal_status_deadline = observed_at + kSupervisorExitStatusGrace};
}

std::vector<SupervisorExitEvent> SupervisorProcessTracker::Poll(TimePoint now) {
  std::vector<std::pair<pid_t, StepKey>> recovered_supervisors;
  {
    absl::MutexLock lock(&m_mtx_);
    recovered_supervisors.reserve(m_supervisors_.size());
    for (const auto& [pid, supervisor] : m_supervisors_) {
      if (supervisor.origin != Origin::Recovered ||
          !std::holds_alternative<Running>(supervisor.state)) {
        continue;
      }
      recovered_supervisors.emplace_back(pid, supervisor.step_key);
    }
  }

  for (const auto& [pid, key] : recovered_supervisors) {
    if (kill(pid, 0) == 0 || errno == EPERM) continue;
    if (errno != ESRCH) {
      CRANE_WARN(
          "[Step #{}.{}] Failed to probe recovered Supervisor pid {}: {}",
          key.first, key.second, pid, strerror(errno));
      continue;
    }

    CRANE_WARN("[Step #{}.{}] Recovered Supervisor pid {} exited.", key.first,
               key.second, pid);
    ObserveMissing(key, pid, now);
  }

  std::vector<SupervisorExitEvent> exits;
  absl::MutexLock lock(&m_mtx_);
  for (auto& [pid, supervisor] : m_supervisors_) {
    auto* exit = std::get_if<ExitObserved>(&supervisor.state);
    if (exit == nullptr || now < exit->terminal_status_deadline) continue;

    exits.emplace_back(SupervisorExitEvent{.step_key = supervisor.step_key,
                                           .pid = pid,
                                           .exit_code = exit->exit_code,
                                           .reason = std::move(exit->reason)});
    supervisor.state = ExitReconciled{};
  }

  for (auto it = m_early_child_exits_.begin();
       it != m_early_child_exits_.end();) {
    if (now >=
        it->second.terminal_status_deadline + kUnregisteredChildExitRetention) {
      auto erase_it = it++;
      m_early_child_exits_.erase(erase_it);
    } else {
      ++it;
    }
  }
  return exits;
}

SupervisorRunState SupervisorProcessTracker::GetRunState(const StepKey& key,
                                                         pid_t pid) const {
  absl::MutexLock lock(&m_mtx_);
  auto it = m_supervisors_.find(pid);
  if (it == m_supervisors_.end() || it->second.step_key != key)
    return SupervisorRunState::Untracked;
  return std::holds_alternative<Running>(it->second.state)
             ? SupervisorRunState::Running
             : SupervisorRunState::Exited;
}

void SupervisorProcessTracker::Unregister(const StepKey& key, pid_t pid) {
  if (pid <= 0) return;
  absl::MutexLock lock(&m_mtx_);
  auto it = m_supervisors_.find(pid);
  if (it != m_supervisors_.end() && it->second.step_key == key)
    m_supervisors_.erase(it);
}

}  // namespace Craned

/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

#pragma once

#include <atomic>
#include <cstddef>
#include <functional>
#include <utility>

namespace Craned::detail {

template <typename Result, typename CgroupResult, typename RemoveDirectory>
Result FinalizeStepCgroupCleanup(const CgroupResult& cgroup_result,
                                 RemoveDirectory&& remove_directory) {
  Result result;
  result.processes_drained = cgroup_result.processes_drained;
  result.cgroup_destroyed = cgroup_result.cgroup_destroyed;
  if (!result.cgroup_destroyed) {
    result.step_directory_removed = false;
    return result;
  }
  result.step_directory_removed =
      std::invoke(std::forward<RemoveDirectory>(remove_directory));
  return result;
}

template <typename SubmitCleanup, typename FinalizeResult, typename Result>
void CoordinateCgroupCleanup(SubmitCleanup&& submit_cleanup,
                             FinalizeResult&& finalize_result,
                             std::function<void(Result)> completion) {
  std::invoke(
      std::forward<SubmitCleanup>(submit_cleanup),
      [finalize_result = std::forward<FinalizeResult>(finalize_result),
       completion = std::move(completion)](const auto& cgroup_result) mutable {
        auto result = std::invoke(finalize_result, cgroup_result);
        if (completion) completion(std::move(result));
      });
}

class CleanupCompletionBarrier {
 public:
  CleanupCompletionBarrier(std::size_t pending, bool succeeded,
                           std::function<void(bool)> completion)
      : pending_(pending),
        succeeded_(succeeded),
        completion_(std::move(completion)) {}

  void Complete(bool succeeded) {
    if (!succeeded) succeeded_.store(false, std::memory_order_release);
    if (pending_.fetch_sub(1, std::memory_order_acq_rel) == 1 && completion_)
      completion_(succeeded_.load(std::memory_order_acquire));
  }

 private:
  std::atomic<std::size_t> pending_;
  std::atomic_bool succeeded_;
  std::function<void(bool)> completion_;
};

template <typename StepCleanup, typename JobCleanup, typename EraseStep,
          typename SendTerminal, typename ResolveWaiters>
void RunDaemonCleanupSequence(StepCleanup&& step_cleanup,
                              JobCleanup&& job_cleanup, EraseStep&& erase_step,
                              SendTerminal&& send_terminal,
                              ResolveWaiters&& resolve_waiters) {
  std::invoke(std::forward<StepCleanup>(step_cleanup));
  std::invoke(std::forward<JobCleanup>(job_cleanup));
  std::invoke(std::forward<EraseStep>(erase_step));
  std::invoke(std::forward<SendTerminal>(send_terminal));
  std::invoke(std::forward<ResolveWaiters>(resolve_waiters));
}

template <typename StopCpuPool, typename StopFastPath,
          typename StopExecutionFlow, typename StopTracing, typename StopPlugin>
void RunCranedResourceShutdownSequence(StopCpuPool&& stop_cpu_pool,
                                       StopFastPath&& stop_fast_path,
                                       StopExecutionFlow&& stop_execution_flow,
                                       StopTracing&& stop_tracing,
                                       StopPlugin&& stop_plugin) {
  std::invoke(std::forward<StopCpuPool>(stop_cpu_pool));
  std::invoke(std::forward<StopFastPath>(stop_fast_path));
  std::invoke(std::forward<StopExecutionFlow>(stop_execution_flow));
  std::invoke(std::forward<StopTracing>(stop_tracing));
  std::invoke(std::forward<StopPlugin>(stop_plugin));
}

}  // namespace Craned::detail

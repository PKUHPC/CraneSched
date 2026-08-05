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
#include <mutex>
#include <optional>
#include <string>
#include <utility>

namespace Craned::Supervisor::detail {

[[nodiscard]] constexpr bool ChildHandshakeSucceeded(bool message_received,
                                                     bool child_ready) {
  return message_received && child_ready;
}

template <typename Cause>
struct TaskFinalizationUpdate {
  std::optional<Cause> cause;
  std::optional<std::string> reason;
};

// Finalization has two distinct phases. An explicit intent (cancel, timeout,
// spawn failure, ...) may be recorded before the process exit is observed;
// the later notification then consumes that intent and finalizes exactly once.
// The first explicit intent wins, while a natural-exit notification never
// overwrites an already-recorded intent.
template <typename Cause>
class TaskFinalizationState {
 public:
  using Update = TaskFinalizationUpdate<Cause>;

  [[nodiscard]] bool RecordIntent(Update update) {
    if (!update.cause.has_value()) return false;

    std::lock_guard lock{mutex_};
    if (finalized_ || intent_.cause.has_value()) return false;
    intent_ = std::move(update);
    return true;
  }

  [[nodiscard]] std::optional<Update> TryFinalize() {
    std::lock_guard lock{mutex_};
    if (finalized_) return std::nullopt;

    finalized_ = true;
    return intent_;
  }

  [[nodiscard]] bool HasIntent() const {
    std::lock_guard lock{mutex_};
    return intent_.cause.has_value();
  }

 private:
  mutable std::mutex mutex_;
  Update intent_;
  bool finalized_{false};
};

template <typename TaskId>
struct TaskFinalizationRequest {
  TaskId task_id{};
};

template <typename Request, typename Queue>
class TaskFinalizationMailbox {
 public:
  [[nodiscard]] bool Enqueue(Request request) {
    if (queue_.enqueue(std::move(request))) return true;
    enqueue_failed_.store(true, std::memory_order_release);
    return false;
  }

  [[nodiscard]] bool TryDequeue(Request* request) {
    return request != nullptr && queue_.try_dequeue(*request);
  }

  [[nodiscard]] bool ConsumeEnqueueFailure() {
    return enqueue_failed_.exchange(false, std::memory_order_acq_rel);
  }

 private:
  Queue queue_;
  std::atomic_bool enqueue_failed_{false};
};

}  // namespace Craned::Supervisor::detail

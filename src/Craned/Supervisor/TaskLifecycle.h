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

namespace Craned::Supervisor::detail {

[[nodiscard]] constexpr bool ChildHandshakeSucceeded(bool message_received,
                                                     bool child_ready) {
  return message_received && child_ready;
}

class TaskFinalizationGate {
 public:
  [[nodiscard]] bool TryEnter() {
    bool expected = false;
    return m_entered_.compare_exchange_strong(
        expected, true, std::memory_order_acq_rel, std::memory_order_acquire);
  }

 private:
  std::atomic_bool m_entered_{false};
};

}  // namespace Craned::Supervisor::detail

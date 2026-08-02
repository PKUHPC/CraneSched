/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "TaskLifecycle.h"

namespace Craned::Supervisor::detail {
namespace {

TEST(ChildHandshakeTest, RequiresReceivedAffirmativeChildResponse) {
  EXPECT_TRUE(ChildHandshakeSucceeded(true, true));
  EXPECT_FALSE(ChildHandshakeSucceeded(false, true));
  EXPECT_FALSE(ChildHandshakeSucceeded(true, false));
  EXPECT_FALSE(ChildHandshakeSucceeded(false, false));
}

TEST(TaskFinalizationGateTest, AllowsExactlyOneCaller) {
  TaskFinalizationGate gate;
  std::atomic_uint successful_callers{0};
  std::vector<std::thread> callers;
  callers.reserve(16);

  for (int i = 0; i < 16; ++i) {
    callers.emplace_back([&] {
      if (gate.TryEnter())
        successful_callers.fetch_add(1, std::memory_order_relaxed);
    });
  }
  for (auto& caller : callers) caller.join();

  EXPECT_EQ(successful_callers.load(std::memory_order_relaxed), 1U);
  EXPECT_FALSE(gate.TryEnter());
}

}  // namespace
}  // namespace Craned::Supervisor::detail

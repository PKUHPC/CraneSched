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
#include "concurrentqueue/concurrentqueue.h"

namespace Craned::Supervisor::detail {
namespace {

enum class TestFinalizeCause : uint8_t {
  kNatural,
  kSpawnFailed,
  kCancelled,
};

TEST(ChildHandshakeTest, RequiresReceivedAffirmativeChildResponse) {
  EXPECT_TRUE(ChildHandshakeSucceeded(true, true));
  EXPECT_FALSE(ChildHandshakeSucceeded(false, true));
  EXPECT_FALSE(ChildHandshakeSucceeded(true, false));
  EXPECT_FALSE(ChildHandshakeSucceeded(false, false));
}

TEST(TaskFinalizationStateTest, NaturalExitPreservesRecordedFailure) {
  TaskFinalizationState<TestFinalizeCause> state;
  EXPECT_TRUE(state.RecordIntent({.cause = TestFinalizeCause::kSpawnFailed,
                                  .reason = "child handshake failed"}));

  auto decision = state.TryFinalize();
  ASSERT_TRUE(decision.has_value());
  EXPECT_EQ(decision->cause, TestFinalizeCause::kSpawnFailed);
  EXPECT_EQ(decision->reason, "child handshake failed");
}

TEST(TaskFinalizationStateTest, FirstExplicitIntentWins) {
  TaskFinalizationState<TestFinalizeCause> state;
  EXPECT_TRUE(state.RecordIntent(
      {.cause = TestFinalizeCause::kCancelled, .reason = "cancel requested"}));
  EXPECT_FALSE(state.RecordIntent({.cause = TestFinalizeCause::kSpawnFailed,
                                   .reason = "late spawn failure"}));

  auto decision = state.TryFinalize();
  ASSERT_TRUE(decision.has_value());
  EXPECT_EQ(decision->cause, TestFinalizeCause::kCancelled);
  EXPECT_EQ(decision->reason, "cancel requested");
}

using TestFinalizationRequest = TaskFinalizationRequest<uint32_t>;
using TestFinalizationMailbox = TaskFinalizationMailbox<
    TestFinalizationRequest,
    moodycamel::ConcurrentQueue<TestFinalizationRequest>>;

TEST(TaskFinalizationMailboxTest,
     RecordedIntentSurvivesCrossProducerQueueOrdering) {
  TestFinalizationMailbox mailbox;
  TaskFinalizationState<TestFinalizeCause> state;
  std::atomic_bool intent_recorded{false};
  std::atomic_bool natural_enqueued{false};
  std::atomic_bool enqueue_succeeded{true};

  std::thread explicit_producer([&] {
    EXPECT_TRUE(state.RecordIntent({.cause = TestFinalizeCause::kCancelled,
                                    .reason = "cancel requested"}));
    intent_recorded.store(true, std::memory_order_release);
    while (!natural_enqueued.load(std::memory_order_acquire))
      std::this_thread::yield();
    if (!mailbox.Enqueue({.task_id = 7}))
      enqueue_succeeded.store(false, std::memory_order_release);
  });
  std::thread natural_producer([&] {
    while (!intent_recorded.load(std::memory_order_acquire))
      std::this_thread::yield();
    if (!mailbox.Enqueue({.task_id = 7}))
      enqueue_succeeded.store(false, std::memory_order_release);
    natural_enqueued.store(true, std::memory_order_release);
  });
  explicit_producer.join();
  natural_producer.join();
  ASSERT_TRUE(enqueue_succeeded.load(std::memory_order_acquire));

  std::size_t dequeued = 0;
  std::optional<TaskFinalizationUpdate<TestFinalizeCause>> decision;
  TestFinalizationRequest request;
  while (mailbox.TryDequeue(&request)) {
    EXPECT_EQ(request.task_id, 7);
    ++dequeued;
    auto candidate = state.TryFinalize();
    if (candidate.has_value()) decision = std::move(candidate);
  }

  EXPECT_EQ(dequeued, 2);
  ASSERT_TRUE(decision.has_value());
  EXPECT_EQ(decision->cause, TestFinalizeCause::kCancelled);
  EXPECT_EQ(decision->reason, "cancel requested");
}

TEST(TaskFinalizationStateTest, FinalizesExactlyOnceAcrossConcurrentCallers) {
  TaskFinalizationState<TestFinalizeCause> state;
  EXPECT_TRUE(state.RecordIntent(
      {.cause = TestFinalizeCause::kCancelled, .reason = std::nullopt}));
  std::atomic_uint successful_callers{0};
  std::vector<std::thread> callers;
  callers.reserve(16);

  for (int i = 0; i < 16; ++i) {
    callers.emplace_back([&] {
      if (state.TryFinalize().has_value())
        successful_callers.fetch_add(1, std::memory_order_relaxed);
    });
  }
  for (auto& caller : callers) caller.join();

  EXPECT_EQ(successful_callers.load(std::memory_order_relaxed), 1U);
  EXPECT_FALSE(state.TryFinalize().has_value());
  EXPECT_FALSE(state.RecordIntent(
      {.cause = TestFinalizeCause::kSpawnFailed, .reason = std::nullopt}));
}

class RejectingFinalizationQueue {
 public:
  bool enqueue(TestFinalizationRequest&&) { return false; }
  bool try_dequeue(TestFinalizationRequest&) { return false; }
};

TEST(TaskFinalizationMailboxTest, LatchesEnqueueFailureUntilConsumed) {
  TaskFinalizationMailbox<TestFinalizationRequest, RejectingFinalizationQueue>
      mailbox;

  EXPECT_FALSE(mailbox.Enqueue({.task_id = 11}));
  EXPECT_TRUE(mailbox.ConsumeEnqueueFailure());
  EXPECT_FALSE(mailbox.ConsumeEnqueueFailure());
}

}  // namespace
}  // namespace Craned::Supervisor::detail

// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include "cri/api.pb.h"
// CRI signal enums must be included before system signal macros.

#include <gtest/gtest.h>

// Exercise the same polling path as JobManager without starting its event
// loop or requiring a controller, root privileges, or a writable cgroup.
#include "../../src/Craned/Core/JobManager.cpp"

namespace Craned {
namespace {

class StepCompletionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    original_timeout_ = g_config.CranedConf.SupervisorCompletionTimeoutSec;
    g_config.CranedConf.SupervisorCompletionTimeoutSec = 30;
    int fds[2];
    ASSERT_EQ(pipe(fds), 0);
    pid_ = fork();
    if (pid_ == 0) {
      close(fds[1]);
      char byte;
      while (read(fds[0], &byte, 1) < 0 && errno == EINTR) {}
      _exit(0);
    }
    close(fds[0]);
    cleanup_fd_ = fds[1];
    ASSERT_GT(pid_, 0);

    crane::grpc::StepToD spec;
    spec.set_job_id(42);
    spec.set_step_id(1);
    spec.set_step_type(crane::grpc::PRIMARY);
    spec.set_type(crane::grpc::Batch);
    step_ = std::make_unique<StepInstance>(spec);
    step_->status = StepStatus::Completing;
    step_->supv_pid = pid_;
    state_ = CompletingStepState{.step = step_.get()};
  }

  void TearDown() override {
    if (cleanup_fd_ >= 0) close(cleanup_fd_);
    if (pid_ > 0) {
      kill(pid_, SIGKILL);
      ReapSupervisor();
    }
    g_config.CranedConf.SupervisorCompletionTimeoutSec = original_timeout_;
  }

  void ReportTerminal(StepStatus status = StepStatus::Completed,
                      uint32_t exit_code = 0) {
    google::protobuf::Timestamp timestamp;
    timestamp.set_seconds(1700000000);
    timestamp.set_nanos(123456789);
    step_->pending_terminal_status =
        StepInstance::PendingTerminalStatus{.final_status = status,
                                            .exit_code = exit_code,
                                            .reason = "reported result",
                                            .timestamp = timestamp};
  }

  void ExpectReportedTerminal(StepStatus status = StepStatus::Completed,
                              uint32_t exit_code = 0) {
    ASSERT_TRUE(step_->pending_terminal_status.has_value());
    const auto& terminal = *step_->pending_terminal_status;
    EXPECT_EQ(terminal.final_status, status);
    EXPECT_EQ(terminal.exit_code, exit_code);
    EXPECT_EQ(terminal.reason, "reported result");
    EXPECT_EQ(terminal.timestamp.seconds(), 1700000000);
    EXPECT_EQ(terminal.timestamp.nanos(), 123456789);
  }

  bool PollAt(std::chrono::milliseconds elapsed) {
    return CheckSupervisorCompletion_(state_,
                                      state_.completion_started_at + elapsed);
  }

  void ReapSupervisor() {
    int result;
    do {
      result = waitpid(pid_, &wait_status_, 0);
    } while (result < 0 && errno == EINTR);
    ASSERT_EQ(result, pid_);
    pid_ = -1;
  }

  void FinishCleanup() {
    close(cleanup_fd_);
    cleanup_fd_ = -1;
    ReapSupervisor();
  }

  std::unique_ptr<StepInstance> step_;
  CompletingStepState state_;
  int wait_status_{};

 private:
  uint32_t original_timeout_{};
  pid_t pid_{-1};
  int cleanup_fd_{-1};
};

TEST_F(StepCompletionTest, ThreeSecondCleanupCompletesNormally) {
  ReportTerminal();
  const auto deadline = std::chrono::steady_clock::now() + 3100ms;
  while (std::chrono::steady_clock::now() < deadline) {
    EXPECT_FALSE(
        CheckSupervisorCompletion_(state_, std::chrono::steady_clock::now()));
    EXPECT_FALSE(state_.sigkill_sent);
    std::this_thread::sleep_for(200ms);
  }
  FinishCleanup();
  ASSERT_TRUE(WIFEXITED(wait_status_));
  EXPECT_EQ(WEXITSTATUS(wait_status_), 0);
  EXPECT_TRUE(
      CheckSupervisorCompletion_(state_, std::chrono::steady_clock::now()));
  ExpectReportedTerminal();
}

TEST_F(StepCompletionTest, TimeoutUsesElapsedTimeInsteadOfPollCount) {
  ReportTerminal();
  for (int i = 0; i < 200; ++i) EXPECT_FALSE(PollAt(1s));
  EXPECT_FALSE(state_.sigkill_sent);
  EXPECT_FALSE(PollAt(29999ms));
  EXPECT_FALSE(state_.sigkill_sent);
  EXPECT_FALSE(PollAt(30s));
  EXPECT_TRUE(state_.sigkill_sent);
  ReapSupervisor();
  ASSERT_TRUE(WIFSIGNALED(wait_status_));
  EXPECT_EQ(WTERMSIG(wait_status_), SIGKILL);
  EXPECT_TRUE(PollAt(30200ms));
  ExpectReportedTerminal();
}

class ReportedStepCompletionTest
    : public StepCompletionTest,
      public ::testing::WithParamInterface<std::pair<StepStatus, uint32_t>> {};

TEST_P(ReportedStepCompletionTest, ForcedCleanupPreservesAllTerminalFields) {
  const auto [status, exit_code] = GetParam();
  ReportTerminal(status, exit_code);
  EXPECT_FALSE(PollAt(30s));
  EXPECT_TRUE(state_.sigkill_sent);
  // The process may still be present as a zombie on another timer tick.
  EXPECT_FALSE(PollAt(31s));
  ExpectReportedTerminal(status, exit_code);
  ReapSupervisor();
  ASSERT_TRUE(WIFSIGNALED(wait_status_));
  EXPECT_EQ(WTERMSIG(wait_status_), SIGKILL);
  EXPECT_TRUE(PollAt(32s));
  ExpectReportedTerminal(status, exit_code);
}

INSTANTIATE_TEST_SUITE_P(TerminalResults, ReportedStepCompletionTest,
                         ::testing::Values(std::pair{StepStatus::Completed, 0U},
                                           std::pair{StepStatus::Failed, 42U},
                                           std::pair{StepStatus::Cancelled,
                                                     265U}));

TEST_F(StepCompletionTest, ConfiguredTimeoutIsRespected) {
  ReportTerminal();
  g_config.CranedConf.SupervisorCompletionTimeoutSec = 60;
  EXPECT_FALSE(PollAt(30s));
  EXPECT_FALSE(state_.sigkill_sent);
  EXPECT_FALSE(PollAt(59999ms));
  EXPECT_FALSE(state_.sigkill_sent);
  EXPECT_FALSE(PollAt(60s));
  EXPECT_TRUE(state_.sigkill_sent);
  ExpectReportedTerminal();
}

TEST_F(StepCompletionTest, UnreportedResultStillFailsOnSupervisorTimeout) {
  EXPECT_FALSE(PollAt(30s));
  EXPECT_TRUE(state_.sigkill_sent);
  ReapSupervisor();
  EXPECT_TRUE(PollAt(30200ms));
  ASSERT_TRUE(step_->pending_terminal_status.has_value());
  EXPECT_EQ(step_->pending_terminal_status->final_status, StepStatus::Failed);
  EXPECT_EQ(step_->pending_terminal_status->exit_code, ExitCode::EC_RPC_ERR);
}

TEST_F(StepCompletionTest, ExitedSupervisorWaitsForInFlightTerminalStatus) {
  FinishCleanup();
  EXPECT_FALSE(PollAt(200ms));
  EXPECT_FALSE(step_->pending_terminal_status.has_value());
  ReportTerminal();
  EXPECT_TRUE(PollAt(400ms));
  ExpectReportedTerminal();
}

TEST_F(StepCompletionTest, MissingTerminalStatusStillFailsAfterGracePeriod) {
  FinishCleanup();
  for (int i = 1; i < kMaxStatusWaitRetryCount; ++i)
    EXPECT_FALSE(PollAt(i * 200ms));
  EXPECT_TRUE(PollAt(kMaxStatusWaitRetryCount * 200ms));
  ASSERT_TRUE(step_->pending_terminal_status.has_value());
  EXPECT_EQ(step_->pending_terminal_status->final_status, StepStatus::Failed);
  EXPECT_EQ(step_->pending_terminal_status->exit_code, ExitCode::EC_RPC_ERR);
}

TEST_F(StepCompletionTest, SilentCleanupDoesNotWaitForTerminalStatus) {
  step_->silent_cleanup = true;
  FinishCleanup();
  EXPECT_TRUE(PollAt(200ms));
  EXPECT_FALSE(step_->pending_terminal_status.has_value());
}

}  // namespace
}  // namespace Craned

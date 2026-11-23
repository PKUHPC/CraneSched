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

class JobManagerTest : public ::testing::Test {
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

  void ReportTerminal() {
    google::protobuf::Timestamp timestamp;
    timestamp.set_seconds(1700000000);
    timestamp.set_nanos(123456789);
    step_->pending_terminal_status = StepInstance::PendingTerminalStatus{
        .final_status = StepStatus::Completed,
        .exit_code = 0,
        .reason = "reported result",
        .timestamp = timestamp};
  }

  void ExpectReportedTerminal() {
    ASSERT_TRUE(step_->pending_terminal_status.has_value());
    const auto& terminal = *step_->pending_terminal_status;
    EXPECT_EQ(terminal.final_status, StepStatus::Completed);
    EXPECT_EQ(terminal.exit_code, 0U);
    EXPECT_EQ(terminal.reason, "reported result");
    EXPECT_EQ(terminal.timestamp.seconds(), 1700000000);
    EXPECT_EQ(terminal.timestamp.nanos(), 123456789);
  }

  bool PollAt(std::chrono::milliseconds elapsed) {
    return CheckSupervisorCompletion_(state_,
                                      state_.completion_started_at + elapsed);
  }

  void ReapSupervisor() {
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    do {
      const pid_t result = waitpid(pid_, &wait_status_, WNOHANG);
      if (result == pid_) {
        pid_ = -1;
        return;
      }
      ASSERT_TRUE(result == 0 || (result < 0 && errno == EINTR));
      std::this_thread::sleep_for(10ms);
    } while (std::chrono::steady_clock::now() < deadline);
    FAIL() << "Supervisor child did not exit before the test deadline";
  }

  std::unique_ptr<StepInstance> step_;
  CompletingStepState state_;
  int wait_status_{};

 private:
  uint32_t original_timeout_{};
  pid_t pid_{-1};
  int cleanup_fd_{-1};
};

TEST_F(JobManagerTest, PreservesReportedResultAtConfiguredTimeout) {
  g_config.CranedConf.SupervisorCompletionTimeoutSec = 5;
  ReportTerminal();

  for (int i = 0; i < 20; ++i) ASSERT_FALSE(PollAt(1s));
  ASSERT_FALSE(PollAt(4999ms));
  ASSERT_FALSE(state_.sigkill_sent);

  ASSERT_FALSE(PollAt(5s));
  ASSERT_TRUE(state_.sigkill_sent);
  ASSERT_NO_FATAL_FAILURE(ReapSupervisor());
  ASSERT_TRUE(WIFSIGNALED(wait_status_));
  EXPECT_EQ(WTERMSIG(wait_status_), SIGKILL);
  EXPECT_TRUE(PollAt(5200ms));
  ExpectReportedTerminal();
}

TEST_F(JobManagerTest, UnreportedResultStillFailsOnSupervisorTimeout) {
  EXPECT_FALSE(PollAt(30s));
  ASSERT_TRUE(state_.sigkill_sent);
  ASSERT_NO_FATAL_FAILURE(ReapSupervisor());
  EXPECT_TRUE(PollAt(30200ms));
  ASSERT_TRUE(step_->pending_terminal_status.has_value());
  EXPECT_EQ(step_->pending_terminal_status->final_status, StepStatus::Failed);
  EXPECT_EQ(step_->pending_terminal_status->exit_code, ExitCode::EC_RPC_ERR);
}

}  // namespace
}  // namespace Craned

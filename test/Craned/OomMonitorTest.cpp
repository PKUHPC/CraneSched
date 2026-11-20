// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <fstream>
#include <optional>
#include <string>

#include "OomMonitor.h"

namespace Craned::Common {
namespace {

class OomMonitorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "crane-oom-XXXXXX").string();
    ASSERT_NE(mkdtemp(pattern.data()), nullptr);
    root_ = pattern;
    job_ = root_ / "job";
    step_ = job_ / "step_1/user";
    task_ = step_ / "task_0";
    std::filesystem::create_directories(task_);
  }

  void TearDown() override { std::filesystem::remove_all(root_); }

  static void Write(const std::filesystem::path& file, std::string_view data) {
    std::ofstream output(file, std::ios::trunc);
    ASSERT_TRUE(output.is_open());
    output << data;
    ASSERT_TRUE(output.good());
  }

  static void V1(const std::filesystem::path& path,
                 std::optional<uint64_t> kills = std::nullopt) {
    std::filesystem::create_directories(path);
    std::string data = "oom_kill_disable 0\nunder_oom 0\n";
    if (kills) data += "oom_kill " + std::to_string(*kills) + "\n";
    Write(path / "memory.oom_control", data);
    Write(path / "cgroup.event_control", "");
  }

  static void V2(const std::filesystem::path& path, uint64_t kills,
                 uint64_t oom = 0) {
    Write(path / "memory.events",
          "low 0\nhigh 1\nmax 2\noom " + std::to_string(oom) + "\noom_kill " +
              std::to_string(kills) + "\noom_group_kill 0\n");
  }

  static int EventFd(const std::filesystem::path& path) {
    std::ifstream input(path / "cgroup.event_control");
    int event = -1, control = -1;
    input >> event >> control;
    EXPECT_GE(event, 0);
    EXPECT_GE(control, 0);
    EXPECT_NE(fcntl(control, F_GETFD) & FD_CLOEXEC, 0);
    return event;
  }

  static void Notify(const std::filesystem::path& path, uint64_t count = 1) {
    ASSERT_EQ(eventfd_write(EventFd(path), count), 0);
  }

  std::filesystem::path root_, job_, step_, task_;
};

TEST_F(OomMonitorTest, DistinguishesUnsupportedCounterFromZeroAndIoFailure) {
  V1(step_);
  auto count = OomMonitor::ReadOomKillCount(step_, false);
  ASSERT_FALSE(count);
  EXPECT_EQ(count.error(), std::errc::not_supported);
  V1(step_, 0);
  count = OomMonitor::ReadOomKillCount(step_, false);
  ASSERT_TRUE(count);
  EXPECT_EQ(*count, 0);
  std::filesystem::remove(step_ / "memory.oom_control");
  count = OomMonitor::ReadOomKillCount(step_, false);
  ASSERT_FALSE(count);
  EXPECT_EQ(count.error(), std::errc::no_such_file_or_directory);
}

TEST_F(OomMonitorTest, RejectsMalformedCountersInsteadOfAssumingZero) {
  for (const auto& text :
       {"oom_kill broken\n", "oom_kill -1\n", "oom_kill 1suffix\n",
        "oom_kill\n", "oom_kill 0\noom_kill 1\n", "garbage\n"}) {
    SCOPED_TRACE(text);
    Write(step_ / "memory.oom_control", text);
    auto count = OomMonitor::ReadOomKillCount(step_, false);
    ASSERT_FALSE(count);
    EXPECT_EQ(count.error(), std::errc::invalid_argument);
  }
}

TEST_F(OomMonitorTest, V2UsesKillDeltaAndRetainsEvidence) {
  V2(step_, 4, 8);
  OomMonitor monitor;
  ASSERT_TRUE(monitor.Start(step_, job_, true));
  V2(step_, 4, 9);
  EXPECT_TRUE(monitor.Sample().workload.empty());
  V2(step_, 5, 9);
  EXPECT_FALSE(monitor.Sample().workload.empty());
  monitor.Stop();
  std::filesystem::remove(step_ / "memory.events");
  EXPECT_FALSE(monitor.Sample().workload.empty());
}

TEST_F(OomMonitorTest, V2ReadFailureDoesNotInventAnOom) {
  V2(step_, 0);
  OomMonitor monitor;
  ASSERT_TRUE(monitor.Start(step_, job_, true));
  std::filesystem::remove(step_ / "memory.events");
  EXPECT_TRUE(monitor.Sample().workload.empty());
}

TEST_F(OomMonitorTest, V1ReadsTaskLocalCounterBeforeTaskCgroupRemoval) {
  V1(step_, 0);
  V1(task_, 3);
  OomMonitor monitor;
  ASSERT_TRUE(monitor.Start(step_, job_, false));
  ASSERT_TRUE(monitor.AddTask(task_));
  EXPECT_TRUE(monitor.Sample().workload.empty());
  V1(task_, 4);  // Parent's v1 counter deliberately stays zero.
  monitor.FinishTask(task_);
  std::filesystem::remove_all(task_);
  EXPECT_NE(monitor.Sample().workload.find("task_0"), std::string::npos);
  monitor.Stop();
  EXPECT_FALSE(monitor.GetEvidence().workload.empty());
}

TEST_F(OomMonitorTest, V1CounterCanDetectOomAfterFirstTaskFinished) {
  auto second = step_ / "task_1";
  V1(step_, 0);
  V1(task_, 0);
  V1(second, 0);
  OomMonitor monitor;
  ASSERT_TRUE(monitor.Start(step_, job_, false));
  ASSERT_TRUE(monitor.AddTask(task_));
  ASSERT_TRUE(monitor.AddTask(second));
  monitor.FinishTask(task_);
  EXPECT_TRUE(monitor.GetEvidence().workload.empty());
  V1(second, 1);
  monitor.FinishTask(second);
  EXPECT_NE(monitor.GetEvidence().workload.find("task_1"), std::string::npos);
}

TEST_F(OomMonitorTest, StopCollectsTaskEvidenceWithoutIndividualCleanup) {
  V1(step_, 0);
  V1(task_, 0);
  OomMonitor monitor;
  ASSERT_TRUE(monitor.Start(step_, job_, false));
  ASSERT_TRUE(monitor.AddTask(task_));
  V1(task_, 1);
  monitor.Stop();
  EXPECT_NE(monitor.GetEvidence().workload.find("task_0"), std::string::npos);
}

TEST_F(OomMonitorTest, OldV1RetainsNotificationWithoutPollingThread) {
  V1(job_);
  V1(step_);
  V1(task_);
  OomMonitor monitor;
  ASSERT_TRUE(monitor.Start(step_, job_, false));
  ASSERT_TRUE(monitor.AddTask(task_));
  int event_fd = EventFd(task_);
  EXPECT_NE(fcntl(event_fd, F_GETFD) & FD_CLOEXEC, 0);
  EXPECT_NE(fcntl(event_fd, F_GETFL) & O_NONBLOCK, 0);
  EXPECT_TRUE(monitor.Sample().workload.empty());
  // Simulate the kernel writing a notification before the shell finishes.
  Notify(task_, 2);
  monitor.FinishTask(task_);
  EXPECT_NE(monitor.Sample().workload.find("OOM notification"),
            std::string::npos);
  EXPECT_TRUE(monitor.GetEvidence().ancestor.empty());
  EXPECT_EQ(fcntl(event_fd, F_GETFD), -1);
  EXPECT_EQ(errno, EBADF);
}

TEST_F(OomMonitorTest, ParentNotificationDoesNotClaimSiblingTaskWasKilled) {
  V1(job_);
  V1(step_);
  V1(task_);
  OomMonitor monitor;
  ASSERT_TRUE(monitor.Start(step_, job_, false));
  ASSERT_TRUE(monitor.AddTask(task_));
  Notify(job_);
  Notify(step_);
  Notify(task_);
  EXPECT_TRUE(monitor.Sample().workload.empty());
  EXPECT_NE(monitor.GetEvidence().ancestor.find("victim step is unknown"),
            std::string::npos);
  monitor.FinishTask(task_);
  // Draining the ancestor eventfd must not turn the latched child event local.
  EXPECT_TRUE(monitor.Sample().workload.empty());
  EXPECT_FALSE(monitor.GetEvidence().ancestor.empty());
}

TEST_F(OomMonitorTest,
       UnavailableAncestorPreventsUnscopedNotificationAttribution) {
  V1(step_);
  V1(task_);
  OomMonitor monitor;
  EXPECT_FALSE(monitor.Start(step_, job_, false));
  ASSERT_TRUE(monitor.AddTask(task_));
  Notify(task_);
  monitor.FinishTask(task_);
  EXPECT_TRUE(monitor.Sample().workload.empty());
}

TEST_F(OomMonitorTest, ClosingWatchBeforeDeletionIgnoresRemovalNotification) {
  V1(job_);
  V1(step_);
  V1(task_);
  OomMonitor monitor;
  ASSERT_TRUE(monitor.Start(step_, job_, false));
  ASSERT_TRUE(monitor.AddTask(task_));
  int kernel_reference = dup(EventFd(task_));
  ASSERT_GE(kernel_reference, 0);
  monitor.FinishTask(task_);
  // Model the kernel's removal wakeup after the user has stopped monitoring.
  ASSERT_EQ(eventfd_write(kernel_reference, 1), 0);
  close(kernel_reference);
  EXPECT_TRUE(monitor.Sample().workload.empty());
  monitor.Stop();
  EXPECT_TRUE(monitor.GetEvidence().workload.empty());
}

TEST_F(OomMonitorTest, MissingRegistrationInterfaceIsAReportedFailure) {
  V1(job_);
  V1(step_);
  std::filesystem::remove(step_ / "cgroup.event_control");
  OomMonitor monitor;
  EXPECT_FALSE(monitor.Start(step_, job_, false));
  EXPECT_TRUE(monitor.Sample().workload.empty());
}

TEST_F(OomMonitorTest, RestartDoesNotReuseEvidenceFromPreviousWorkload) {
  V2(step_, 0);
  OomMonitor monitor;
  ASSERT_TRUE(monitor.Start(step_, job_, true));
  V2(step_, 1);
  EXPECT_FALSE(monitor.Sample().workload.empty());
  monitor.Stop();
  ASSERT_TRUE(monitor.Start(step_, job_, true));
  EXPECT_TRUE(monitor.Sample().workload.empty());
}

}  // namespace
}  // namespace Craned::Common

// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include "cri/api.pb.h"
// CRI signal enums must be included before system signal macros.

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>

#include <fstream>

#include "../../src/Craned/Supervisor/TaskManager.h"

namespace Craned::Supervisor {
namespace {

// Opt-in kernel test. The supplied directory must be a writable memory cgroup
// with delegation to create child cgroups. Only the test's own subtree changes.
class OomCgroupIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    const char* base = std::getenv("CRANE_TEST_MEMORY_CGROUP_ROOT");
    if (!base) GTEST_SKIP() << "No delegated memory cgroup supplied";
    struct statfs fs{};
    ASSERT_EQ(statfs(base, &fs), 0);
    constexpr long kCgroupV1Magic = 0x27e0eb;
    constexpr long kCgroupV2Magic = 0x63677270;
    ASSERT_TRUE(fs.f_type == kCgroupV1Magic || fs.f_type == kCgroupV2Magic);
    v2_ = fs.f_type == kCgroupV2Magic;
    std::string pattern =
        (std::filesystem::path(base) / "crane-oom-test-XXXXXX").string();
    ASSERT_NE(mkdtemp(pattern.data()), nullptr);
    root_ = pattern;
    paths_.push_back(root_);
    if (v2_)
      ASSERT_TRUE(Write(root_ / "cgroup.subtree_control", "+memory"));
    else
      ASSERT_TRUE(Write(root_ / "memory.use_hierarchy", "1"));
    // Bound the whole test even if a lower-level limit is unexpectedly ignored.
    ASSERT_TRUE(SetLimit(root_, 256 * 1024 * 1024));
    job_ = root_ / "job";
    step_ = job_ / "step_1/user";
    task_ = step_ / "task_0";
    for (const auto& path : {job_, step_.parent_path(), step_, task_}) {
      ASSERT_TRUE(std::filesystem::create_directory(path));
      paths_.push_back(path);
      if (v2_ && path != task_)
        ASSERT_TRUE(Write(path / "cgroup.subtree_control", "+memory"));
    }
  }

  void TearDown() override {
    for (auto it = paths_.rbegin(); it != paths_.rend(); ++it) {
      // All test processes have exited before normal teardown. A failed test
      // must still clean up only its own explicitly-created cgroups.
      std::ifstream procs(*it / "cgroup.procs");
      pid_t pid;
      while (procs >> pid) kill(pid, SIGKILL);
      EXPECT_EQ(rmdir(it->c_str()), 0) << *it << ": " << strerror(errno);
    }
  }

  static bool Write(const std::filesystem::path& path, std::string_view value) {
    std::ofstream file(path);
    file << value;
    file.close();
    return !file.fail();
  }

  bool SetLimit(const std::filesystem::path& path, uint64_t bytes) {
    if (!Write(path / (v2_ ? "memory.max" : "memory.limit_in_bytes"),
               std::to_string(bytes)))
      return false;
    auto swap =
        path / (v2_ ? "memory.swap.max" : "memory.memsw.limit_in_bytes");
    if (std::filesystem::exists(swap))
      return Write(swap, v2_ ? "0" : std::to_string(bytes));
    return true;
  }

  void Run(const std::filesystem::path& limit_path, bool oom) {
    ASSERT_TRUE(SetLimit(limit_path, 32 * 1024 * 1024));
    StepToSupv spec;
    spec.set_type(crane::grpc::Batch);
    spec.set_step_type(crane::grpc::PRIMARY);
    StepInstance step(spec);
    ASSERT_TRUE(step.oom_monitor.Start(step_, job_, v2_));
    ASSERT_TRUE(step.oom_monitor.AddTask(task_));

    int output[2];
    ASSERT_EQ(pipe2(output, O_CLOEXEC), 0);
    // Keep the shell alive; allow the Python child to be the kernel's victim.
    std::string script =
        "python3 -c 'import os, signal; "
        "open(\"/proc/self/oom_score_adj\", \"w\").write(\"1000\"); ";
    script += oom ? "a=bytearray(128*1024*1024)'"
                  : "os.kill(os.getpid(), signal.SIGKILL)'";
    script += "; rc=$?; echo python_rc=$rc; exit 0";

    pid_t pid = fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
      close(output[0]);
      if (dup2(output[1], STDOUT_FILENO) == -1 ||
          dup2(output[1], STDERR_FILENO) == -1)
        _exit(120);
      close(output[1]);
      if (!Write("/proc/self/oom_score_adj", "-1000") ||
          !Write(task_ / "cgroup.procs", std::to_string(getpid())))
        _exit(121);
      // Bound failure time without introducing timing-based OOM assertions.
      execlp("timeout", "timeout", "--signal=KILL", "20s", "/bin/bash", "-c",
             script.c_str(), nullptr);
      _exit(122);
    }
    close(output[1]);
    std::string text;
    char buffer[1024];
    ssize_t n;
    while ((n = read(output[0], buffer, sizeof(buffer))) != 0) {
      if (n < 0) {
        if (errno == EINTR) continue;
        break;
      }
      text.append(buffer, n);
    }
    close(output[0]);
    int status;
    ASSERT_EQ(waitpid(pid, &status, 0), pid);
    ASSERT_TRUE(WIFEXITED(status)) << text;
    ASSERT_EQ(WEXITSTATUS(status), 0) << text;
    ASSERT_NE(text.find("python_rc=137"), std::string::npos) << text;

    ProcInstance process(&step, 0);
    ASSERT_TRUE(process.HandleSigchld(pid, status));
    step.oom_monitor.FinishTask(task_);
    step.RecordTaskResult(StepStatus::Completed, 0, std::nullopt,
                          *process.GetFinalInfo());
    step.FinalizeOomStatus();
    EXPECT_EQ(step.final_termination_status.final_status_on_termination,
              oom ? StepStatus::OutOfMemory : StepStatus::Completed)
        << text;
    EXPECT_EQ(step.final_termination_status.max_exit_code, 0);
    if (oom)
      EXPECT_FALSE(
          step.final_termination_status.final_reason_on_termination.empty());
  }

  bool v2_{};
  std::filesystem::path root_, job_, step_, task_;
  std::vector<std::filesystem::path> paths_;
};

TEST_F(OomCgroupIntegrationTest, ChildOomAtTaskLimitDespiteShellExitZero) {
  Run(task_, true);
}

TEST_F(OomCgroupIntegrationTest, ChildOomAtStepLimitDespiteShellExitZero) {
  Run(step_, true);
}

TEST_F(OomCgroupIntegrationTest, ChildOomAtJobLimitDespiteShellExitZero) {
  Run(job_, true);
}

TEST_F(OomCgroupIntegrationTest, ManualSigkillIsNotAnOom) { Run(task_, false); }

}  // namespace
}  // namespace Craned::Supervisor

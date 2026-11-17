// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include "cri/api.pb.h"
// CRI signal enums must be included before system signal macros.

#include <grp.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <fstream>

#include "../../src/Craned/Supervisor/TaskManager.h"

namespace Craned::Supervisor {
namespace {
class ScriptPermissionsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    old_directory_ = g_config.CraneScriptDir;
    old_job_ = g_config.JobId;
    old_step_ = g_config.StepId;
    old_umask_ = umask(0);
    std::string pattern = "/tmp/crane-script-permissions-XXXXXX";
    ASSERT_NE(mkdtemp(pattern.data()), nullptr);
    directory_ = pattern;
    ASSERT_TRUE(util::os::EnsureServiceDirectory(directory_, 0755));
    g_config.CraneScriptDir = directory_;
    g_config.JobId = 42;
    g_config.StepId = 0;
    path_ = directory_ / "Crane-42.0.sh";
    spec_.set_type(crane::grpc::Batch);
    spec_.set_step_type(crane::grpc::PRIMARY);
    spec_.set_uid(geteuid());
    spec_.add_gids(getegid());
    spec_.set_sh_script("#!/bin/sh\nexit 0\n");
  }

  void TearDown() override {
    g_config.CraneScriptDir = std::move(old_directory_);
    g_config.JobId = old_job_;
    g_config.StepId = old_step_;
    umask(old_umask_);
    if (!directory_.empty()) std::filesystem::remove_all(directory_);
  }

  void CheckScript() {
    struct stat st{};
    ASSERT_EQ(stat(path_.c_str(), &st), 0);
    EXPECT_EQ(st.st_mode & 07777, 0700);
    EXPECT_EQ(st.st_uid, spec_.uid());
    EXPECT_EQ(st.st_gid, spec_.gids(0));
    std::ifstream input(path_);
    EXPECT_EQ(std::string(std::istreambuf_iterator<char>(input), {}),
              spec_.sh_script());
    EXPECT_EQ(umask(0), 0);
    EXPECT_EQ(std::distance(std::filesystem::directory_iterator(directory_),
                            std::filesystem::directory_iterator{}),
              1);
  }

  StepToSupv spec_;
  std::filesystem::path directory_, path_;

 private:
  std::filesystem::path old_directory_;
  job_id_t old_job_{};
  step_id_t old_step_{};
  mode_t old_umask_{};
};

TEST_F(ScriptPermissionsTest,
       PrepareCreatesPrivateExecutableAndCleanupRemovesIt) {
  StepInstance step(spec_);
  ASSERT_EQ(step.Prepare(), CraneErrCode::SUCCESS);
  ASSERT_EQ(step.script_path, path_);
  CheckScript();
  step.CleanUp();
  EXPECT_FALSE(std::filesystem::exists(path_));
}

TEST_F(ScriptPermissionsTest, ReplacesExistingScriptWithoutReusingItsInode) {
  std::ofstream(path_) << "old content";
  ASSERT_EQ(chmod(path_.c_str(), 0755), 0);
  struct stat before{};
  ASSERT_EQ(stat(path_.c_str(), &before), 0);
  StepInstance step(spec_);
  ASSERT_EQ(step.Prepare(), CraneErrCode::SUCCESS);
  CheckScript();
  struct stat after{};
  ASSERT_EQ(stat(path_.c_str(), &after), 0);
  EXPECT_NE(before.st_ino, after.st_ino);
}

TEST_F(ScriptPermissionsTest, FailedPreparationRemovesUnpublishedFile) {
  spec_.clear_gids();
  StepInstance step(spec_);
  EXPECT_EQ(step.Prepare(), CraneErrCode::ERR_SYSTEM_ERR);
  EXPECT_FALSE(step.script_path.has_value());
  EXPECT_TRUE(std::filesystem::is_empty(directory_));
}

TEST_F(ScriptPermissionsTest, ScriptExecutesAsTheJobUser) {
  if (geteuid() != 0) GTEST_SKIP() << "Requires root to test another UID";
  spec_.set_uid(65534);
  spec_.set_gids(0, 65534);
  StepInstance step(spec_);
  ASSERT_EQ(step.Prepare(), CraneErrCode::SUCCESS);
  CheckScript();
  const pid_t pid = fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    if (setgroups(0, nullptr) != 0 || setgid(65534) != 0 || setuid(65534) != 0)
      _exit(2);
    execl("/bin/bash", "bash", "-c", path_.c_str(), nullptr);
    _exit(3);
  }
  int status{};
  ASSERT_EQ(waitpid(pid, &status, 0), pid);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST_F(ScriptPermissionsTest, OtherUserInJobGroupCannotReadScript) {
  if (geteuid() != 0) GTEST_SKIP() << "Requires root to test another UID";
  spec_.set_uid(65534);
  spec_.set_gids(0, 65534);
  StepInstance step(spec_);
  ASSERT_EQ(step.Prepare(), CraneErrCode::SUCCESS);
  const pid_t pid = fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    if (setgroups(0, nullptr) != 0 || setgid(65534) != 0 || setuid(65533) != 0)
      _exit(2);
    int fd = open(path_.c_str(), O_RDONLY);
    _exit(fd == -1 && errno == EACCES ? 0 : 1);
  }
  int status{};
  ASSERT_EQ(waitpid(pid, &status, 0), pid);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);
}
}  // namespace
}  // namespace Craned::Supervisor

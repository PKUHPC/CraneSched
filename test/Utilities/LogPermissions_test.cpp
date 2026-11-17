// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include <grp.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <fstream>

#include "crane/Logger.h"
#include "crane/OS.h"

namespace {
class LogPermissionsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    old_umask_ = umask(0);
    old_logger_ = spdlog::default_logger();
    std::string pattern = "/tmp/crane-log-permissions-XXXXXX";
    ASSERT_NE(mkdtemp(pattern.data()), nullptr);
    root_ = pattern;
    ASSERT_EQ(chmod(root_.c_str(), 0755), 0);
    directory_ = root_ / "supervisor";
    log_ = directory_ / "1.0.log";
  }

  void TearDown() override {
    spdlog::shutdown();
    spdlog::set_default_logger(std::move(old_logger_));
    umask(old_umask_);
    if (!root_.empty()) std::filesystem::remove_all(root_);
  }

  void Init(bool private_file = true) {
    InitLogger(spdlog::level::info, log_, false, 64, 2,
               kDefaultAsyncLogQueueSize, kDefaultAsyncLogThreadCount,
               kDefaultAsyncLogBlockWhenFull, private_file);
  }

  void Check(const std::filesystem::path& path, mode_t permissions) {
    struct stat st{};
    ASSERT_EQ(stat(path.c_str(), &st), 0) << path;
    EXPECT_EQ(st.st_mode & 07777, permissions) << path;
    EXPECT_EQ(st.st_uid, geteuid()) << path;
    EXPECT_EQ(st.st_gid, getegid()) << path;
  }

  std::filesystem::path root_, directory_, log_;

 private:
  mode_t old_umask_{};
  std::shared_ptr<spdlog::logger> old_logger_;
};

TEST_F(LogPermissionsTest, CreationAndRotationStayPrivateWithoutChangingUmask) {
  Init();
  Check(directory_, 0700);
  Check(log_, 0600);
  // Exercise the production sink synchronously so rotation has completed
  // before inspecting files, without sleeps or asynchronous flush assumptions.
  spdlog::logger logger("rotation-test",
                        spdlog::default_logger()->sinks().front());
  for (int i = 0; i < 6; ++i) logger.info("{} {}", i, std::string(80, 'x'));
  logger.flush();
  Check(log_, 0600);
  Check(directory_ / "1.0.1.log", 0600);
  Check(directory_ / "1.0.2.log", 0600);
  EXPECT_EQ(umask(0), 0);
  auto output = root_ / "job.out";
  std::ofstream(output) << "job output";
  Check(output, 0666);
}

TEST_F(LogPermissionsTest, RepairsExistingDirectoryAndRetainedLogs) {
  ASSERT_TRUE(std::filesystem::create_directory(directory_));
  auto rotated = directory_ / "1.0.1.log";
  std::ofstream(log_) << "old log";
  std::ofstream(rotated) << "old rotated log";
  ASSERT_EQ(chmod(directory_.c_str(), 0777), 0);
  if (geteuid() == 0) {
    ASSERT_EQ(chown(directory_.c_str(), 65534, 65534), 0);
    ASSERT_EQ(chown(log_.c_str(), 65534, 65534), 0);
    ASSERT_EQ(chown(rotated.c_str(), 65534, 65534), 0);
  }
  Init();
  Check(directory_, 0700);
  Check(log_, 0600);
  Check(rotated, 0600);
  std::ifstream input(log_);
  EXPECT_EQ(std::string(std::istreambuf_iterator<char>(input), {}), "old log");
}

TEST_F(LogPermissionsTest, OrdinaryUserCannotReadLogEvenInServiceGroup) {
  if (geteuid() != 0) GTEST_SKIP() << "Requires root to test another UID";
  Init();
  const pid_t pid = fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    if (setgroups(0, nullptr) != 0 || setuid(65534) != 0) _exit(2);
    int fd = open(log_.c_str(), O_RDONLY);
    _exit(fd == -1 && errno == EACCES ? 0 : 1);
  }
  int status{};
  ASSERT_EQ(waitpid(pid, &status, 0), pid);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST_F(LogPermissionsTest, RejectsLinkedLogWithoutChangingTarget) {
  ASSERT_TRUE(std::filesystem::create_directory(directory_));
  auto other = root_ / "other";
  std::ofstream(other) << "unchanged";
  std::filesystem::create_symlink(other, log_);
  EXPECT_THROW(Init(), spdlog::spdlog_ex);
  Check(other, 0666);
}

TEST_F(LogPermissionsTest, RejectsLinkedDirectory) {
  auto other = root_ / "other-directory";
  ASSERT_TRUE(std::filesystem::create_directory(other));
  std::filesystem::create_directory_symlink(other, directory_);
  EXPECT_THROW(Init(), spdlog::spdlog_ex);
  Check(other, 0777);
}

TEST_F(LogPermissionsTest, OtherLoggersKeepExistingPermissionBehavior) {
  Init(false);
  Check(log_, 0666);
}

TEST_F(LogPermissionsTest, ScriptDirectoryIsServiceOwnedAndTraversable) {
  ASSERT_TRUE(std::filesystem::create_directory(directory_));
  ASSERT_EQ(chmod(directory_.c_str(), 0777), 0);
  auto result = util::os::EnsureServiceDirectory(directory_, 0755);
  ASSERT_TRUE(result) << result.error().message();
  Check(directory_, 0755);
}
}  // namespace

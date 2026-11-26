/**
 * Copyright (c) 2024-2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_join.h>
#include <grp.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <fstream>
#include <string>
#include <utility>
#include <vector>

#include "crane/Logger.h"
#include "crane/OS.h"
#include "crane/String.h"

TEST(String, ParseNodeList) {
  using util::ParseHostList;

  std::string host_list{"aaa[1-2,3],bbb"};
  std::list<std::string> parsed_list;

  bool ok = ParseHostList(host_list, &parsed_list);
  GTEST_LOG_(INFO) << "Parsing result: " << ok;
  EXPECT_TRUE(ok);

  if (ok) {
    std::string parsed_list_str = absl::StrJoin(parsed_list, " ");
    GTEST_LOG_(INFO) << "Parsed List: " << parsed_list_str;
    EXPECT_EQ(parsed_list_str, "aaa1 aaa2 aaa3 bbb");
  }
}

TEST(String, ShortHostnameKeepsTheFirstDnsLabel) {
  EXPECT_EQ(util::ShortHostname("node01.pku.edu.cn"), "node01");
  EXPECT_EQ(util::ShortHostname("node01"), "node01");
}

TEST(String, ParseHostListKeepsDeterministicOrder) {
  using util::ParseHostList;

  std::list<std::string> parsed_list;
  ASSERT_TRUE(ParseHostList("node[02,01],node03", &parsed_list));
  EXPECT_EQ(absl::StrJoin(parsed_list, " "), "node02 node01 node03");

  parsed_list.clear();
  ASSERT_TRUE(ParseHostList("rack[1-2]n[01-02]", &parsed_list));
  EXPECT_EQ(absl::StrJoin(parsed_list, " "),
            "rack1n01 rack1n02 rack2n01 rack2n02");
}

TEST(String, ParseHostListSupportsSharedNonDotSuffix) {
  using util::ParseHostList;

  std::list<std::string> parsed_list;
  ASSERT_TRUE(ParseHostList("b1u01n1,b2u[05,02]n3,b3u03n4", &parsed_list));
  EXPECT_EQ(absl::StrJoin(parsed_list, " "), "b1u01n1 b2u05n3 b2u02n3 b3u03n4");

  parsed_list.clear();
  ASSERT_TRUE(ParseHostList("node[01-02]-gpu", &parsed_list));
  EXPECT_EQ(absl::StrJoin(parsed_list, " "), "node01-gpu node02-gpu");
}

TEST(String, ParseHostListSupportsSlurmHostlistForms) {
  using util::ParseHostList;

  const std::vector<std::pair<std::string, std::string>> test_cases = {
      {"node[01,03-04]", "node01 node03 node04"},
      {"node[01-02].example.com", "node01.example.com node02.example.com"},
      {"node[08,10-11],login", "node08 node10 node11 login"},
      {"gpu[01-02]a[1-2]-x", "gpu01a1-x gpu01a2-x gpu02a1-x gpu02a2-x"},
      {"node[01, 03], node05", "node01 node03 node05"},
      {"node[01]foo[02]", "node01foo02"},
  };

  for (const auto& [host_string, expected] : test_cases) {
    SCOPED_TRACE(host_string);
    std::list<std::string> parsed_list;
    ASSERT_TRUE(ParseHostList(host_string, &parsed_list));
    EXPECT_EQ(absl::StrJoin(parsed_list, " "), expected);
  }
}

TEST(String, ParseHostListRejectsMalformedExpressions) {
  using util::ParseHostList;

  const std::vector<std::string> malformed_host_lists = {
      "node[01",   "node01]",   "node[[01]]",   "node[]",
      "node[01,]", "node[,01]", "node[01,,02]", "node[foo]",
      "node[01-]", "node[-02]", "node[01-02",   "node[01]tail]",
  };

  for (const auto& host_string : malformed_host_lists) {
    SCOPED_TRACE(host_string);
    std::list<std::string> parsed_list;
    EXPECT_FALSE(ParseHostList(host_string, &parsed_list));
  }
}

TEST(String, HostNameListToStr) {
  using util::HostNameListToStr;

  clock_t start, end;
  start = clock();
  std::string host_list{
      "a[01-99]s[01-05]c[001-100],a[30-40,501-600]s[03-07]c[201-300]"};
  std::list<std::string> parsed_list;

  bool ok = util::ParseHostList(host_list, &parsed_list);
  EXPECT_TRUE(ok);
  GTEST_LOG_(INFO) << "n: " << parsed_list.size();

  end = clock();
  double elapsedTime = static_cast<double>(end - start) / CLOCKS_PER_SEC;
  GTEST_LOG_(INFO) << "ParseHostList time: " << elapsedTime << "s";

  if (ok) {
    start = end;
    std::string res = util::HostNameListToStr(parsed_list);

    end = clock();
    elapsedTime = static_cast<double>(end - start) / CLOCKS_PER_SEC;
    GTEST_LOG_(INFO) << "HostNameListToStr time: " << elapsedTime << "s";

    EXPECT_EQ(res, host_list);
    GTEST_LOG_(INFO) << "Parsing result: " << res;
  }
}

namespace {
class FilePermissionsTest : public ::testing::Test {
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

  void Init(LogFilePolicy policy = LogFilePolicy::PRIVATE_DIRECTORY) {
    InitLogger(spdlog::level::info, log_, false, 64, 2,
               kDefaultAsyncLogQueueSize, kDefaultAsyncLogThreadCount,
               kDefaultAsyncLogBlockWhenFull, policy);
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

class PrivateLogFileTest : public FilePermissionsTest,
                           public ::testing::WithParamInterface<LogFilePolicy> {
 protected:
  void Init() { FilePermissionsTest::Init(GetParam()); }

  mode_t DirectoryMode() const {
    return GetParam() == LogFilePolicy::PRIVATE_DIRECTORY ? 0700 : 0755;
  }
};

INSTANTIATE_TEST_SUITE_P(Policies, PrivateLogFileTest,
                         ::testing::Values(LogFilePolicy::PRIVATE_FILE,
                                           LogFilePolicy::PRIVATE_DIRECTORY));

TEST_P(PrivateLogFileTest, CreationAndRotationStayPrivateWithoutChangingUmask) {
  Init();
  Check(directory_, DirectoryMode());
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

TEST_P(PrivateLogFileTest, RepairsExistingLogsAndOwnership) {
  ASSERT_TRUE(std::filesystem::create_directory(directory_));
  auto rotated = directory_ / "1.0.1.log";
  std::ofstream(log_) << "old log";
  std::ofstream(rotated) << "old rotated log";
  ASSERT_EQ(chmod(directory_.c_str(), 0755), 0);
  if (geteuid() == 0) {
    ASSERT_EQ(chown(log_.c_str(), 65534, 65534), 0);
    ASSERT_EQ(chown(rotated.c_str(), 65534, 65534), 0);
  }
  Init();
  Check(directory_, DirectoryMode());
  Check(log_, 0600);
  Check(rotated, 0600);
  std::ifstream input(log_);
  EXPECT_EQ(std::string(std::istreambuf_iterator<char>(input), {}), "old log");
}

TEST_P(PrivateLogFileTest, OrdinaryUserCannotReadLogEvenInServiceGroup) {
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

TEST_P(PrivateLogFileTest, RejectsLinkedLogWithoutChangingTarget) {
  ASSERT_TRUE(std::filesystem::create_directory(directory_));
  auto other = root_ / "other";
  std::ofstream(other) << "unchanged";
  for (const auto& path : {log_, directory_ / "1.0.1.log"}) {
    SCOPED_TRACE(path.string());
    for (bool hard_link : {false, true}) {
      SCOPED_TRACE(hard_link ? "hard link" : "symbolic link");
      if (hard_link)
        std::filesystem::create_hard_link(other, path);
      else
        std::filesystem::create_symlink(other, path);
      EXPECT_THROW(Init(), spdlog::spdlog_ex);
      Check(other, 0666);
      EXPECT_TRUE(std::filesystem::remove(path));
    }
  }
}

TEST_F(FilePermissionsTest, RepairsExistingPrivateDirectoryOwnership) {
  ASSERT_TRUE(std::filesystem::create_directory(directory_));
  if (geteuid() == 0) ASSERT_EQ(chown(directory_.c_str(), 65534, 65534), 0);
  Init();
  Check(directory_, 0700);
}

TEST_F(FilePermissionsTest, PrivateFilesPreserveSharedDirectoryAndOtherFiles) {
  ASSERT_TRUE(std::filesystem::create_directory(directory_));
  ASSERT_EQ(chmod(directory_.c_str(), 0751), 0);
  auto other = directory_ / "1.0.notes.log";
  auto script = directory_ / "job.sh";
  std::ofstream(other) << "another file";
  std::ofstream(script) << "#!/bin/sh\nexit 0\n";
  ASSERT_EQ(chmod(script.c_str(), 0755), 0);
  Init(LogFilePolicy::PRIVATE_FILE);
  Check(directory_, 0751);
  Check(log_, 0600);
  Check(other, 0666);
  Check(script, 0755);
  if (geteuid() != 0) return;
  const pid_t pid = fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    if (setgroups(0, nullptr) != 0 || setuid(65534) != 0) _exit(2);
    execl(script.c_str(), script.c_str(), nullptr);
    _exit(1);
  }
  int status{};
  ASSERT_EQ(waitpid(pid, &status, 0), pid);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST_F(FilePermissionsTest, PrivateFileAcceptsRelativeFilename) {
  const auto previous_directory = std::filesystem::current_path();
  absl::Cleanup restore_directory = [&] {
    std::filesystem::current_path(previous_directory);
  };
  std::filesystem::current_path(root_);
  log_ = "craned.log";
  Init(LogFilePolicy::PRIVATE_FILE);
  Check(log_, 0600);
  Check(root_, 0755);
}

TEST_F(FilePermissionsTest, RejectsLinkedDirectory) {
  auto other = root_ / "other-directory";
  ASSERT_TRUE(std::filesystem::create_directory(other));
  std::filesystem::create_directory_symlink(other, directory_);
  EXPECT_THROW(Init(), spdlog::spdlog_ex);
  Check(other, 0777);
}

TEST_F(FilePermissionsTest, OtherLoggersKeepExistingPermissionBehavior) {
  InitLogger(spdlog::level::info, log_, false, 64, 2);
  Check(log_, 0666);
}

TEST_F(FilePermissionsTest, ScriptDirectoryIsServiceOwnedAndTraversable) {
  ASSERT_TRUE(std::filesystem::create_directory(directory_));
  ASSERT_EQ(chmod(directory_.c_str(), 0777), 0);
  auto result = util::os::EnsureServiceDirectory(directory_, 0755);
  ASSERT_TRUE(result) << result.error().message();
  Check(directory_, 0755);
}

TEST_F(FilePermissionsTest, CreatesMissingAncestorsWithExplicitPermissions) {
  auto nested = root_ / "missing-parent" / "supervisor";
  auto result = util::os::EnsureServiceDirectory(nested, 0700);
  ASSERT_TRUE(result) << result.error().message();
  Check(root_ / "missing-parent", 0755);
  Check(nested, 0700);
}

TEST_F(FilePermissionsTest, AllowsTrustedAncestorSymlink) {
  auto real_parent = root_ / "real-parent";
  ASSERT_TRUE(std::filesystem::create_directory(real_parent));
  auto trusted_link = root_ / "trusted-link";
  std::filesystem::create_directory_symlink(real_parent, trusted_link);

  auto nested = trusted_link / "missing-parent" / "supervisor";
  auto result = util::os::EnsureServiceDirectory(nested, 0700);
  ASSERT_TRUE(result) << result.error().message();
  Check(real_parent / "missing-parent", 0755);
  Check(real_parent / "missing-parent" / "supervisor", 0700);
}

TEST_F(FilePermissionsTest, TrailingSlashDoesNotFollowLinkedDirectory) {
  auto target = root_ / "target";
  ASSERT_TRUE(std::filesystem::create_directory(target));
  ASSERT_EQ(chmod(target.c_str(), 0777), 0);
  auto link = root_ / "linked";
  std::filesystem::create_directory_symlink(target, link);

  auto result = util::os::EnsureServiceDirectory(link.string() + "/", 0700);
  ASSERT_FALSE(result);
  Check(target, 0777);
}
}  // namespace

#include "CgroupV2Fs.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <csignal>
#include <filesystem>
#include <format>
#include <fstream>

namespace {

using Craned::Common::CG_V2_REQUIRED_CONTROLLERS;
using Craned::Common::CgroupV2CleanupMode;
using Craned::Common::CgroupV2FsBackend;
using Craned::Common::CgConstant::ControllerFile;

std::atomic<uint64_t> g_temp_id{0};

void WriteText(const std::filesystem::path& path, std::string_view text) {
  std::ofstream ofs(path, std::ios::trunc);
  ASSERT_TRUE(ofs.is_open()) << path;
  ofs << text;
  ASSERT_TRUE(ofs.good()) << path;
}

std::string ReadText(const std::filesystem::path& path) {
  std::ifstream ifs(path);
  EXPECT_TRUE(ifs.is_open()) << path;
  return {std::istreambuf_iterator<char>(ifs),
          std::istreambuf_iterator<char>()};
}

class FakeCgroupFs {
 public:
  FakeCgroupFs() {
    root_ = std::filesystem::temp_directory_path() /
            std::format("crane_cgroup_v2_fs_test_{}_{}", getpid(),
                        g_temp_id.fetch_add(1));
    std::filesystem::create_directories(root_);
    PrepareNode(root_);
  }

  ~FakeCgroupFs() {
    std::error_code ec;
    std::filesystem::remove_all(root_, ec);
  }

  const std::filesystem::path& Root() const { return root_; }

  void PrepareNode(const std::filesystem::path& path,
                   std::string_view subtree_control = "") {
    std::filesystem::create_directories(path);
    WriteText(path / "cgroup.controllers", "cpu memory io cpuset pids\n");
    WriteText(path / "cgroup.subtree_control", subtree_control);
    WriteText(path / "cgroup.events", "populated 0\n");
    WriteText(path / "cgroup.procs", "");
  }

 private:
  std::filesystem::path root_;
};

TEST(CgroupV2FsBackendTest, CreateOrOpenMergesControllerEnableAndUsesCache) {
  FakeCgroupFs fs;
  fs.PrepareNode(fs.Root() / "crane");

  CgroupV2FsBackend backend(CgroupV2CleanupMode::SYNC_RMDIR, fs.Root());
  ASSERT_TRUE(backend.Probe(CG_V2_REQUIRED_CONTROLLERS));

  auto result =
      backend.CreateOrOpen("crane/job_1", CG_V2_REQUIRED_CONTROLLERS, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(std::filesystem::exists(fs.Root() / "crane/job_1"));
  EXPECT_EQ("+cpu +memory +io +cpuset",
            ReadText(fs.Root() / "cgroup.subtree_control"));
  EXPECT_EQ("+cpu +memory +io +cpuset",
            ReadText(fs.Root() / "crane/cgroup.subtree_control"));

  std::filesystem::remove(fs.Root() / "crane/cgroup.subtree_control");
  result =
      backend.CreateOrOpen("crane/job_2", CG_V2_REQUIRED_CONTROLLERS, false);
  EXPECT_TRUE(result.has_value());
  EXPECT_TRUE(std::filesystem::exists(fs.Root() / "crane/job_2"));
}

TEST(CgroupV2FsBackendTest, InodeChangeInvalidatesControllerCache) {
  FakeCgroupFs fs;
  fs.PrepareNode(fs.Root() / "crane");

  CgroupV2FsBackend backend(CgroupV2CleanupMode::SYNC_RMDIR, fs.Root());
  ASSERT_TRUE(backend.Probe(CG_V2_REQUIRED_CONTROLLERS));
  ASSERT_TRUE(
      backend.CreateOrOpen("crane/job_1", CG_V2_REQUIRED_CONTROLLERS, false)
          .has_value());

  std::filesystem::remove_all(fs.Root() / "crane");
  fs.PrepareNode(fs.Root() / "crane");

  EXPECT_TRUE(
      backend.CreateOrOpen("crane/job_2", CG_V2_REQUIRED_CONTROLLERS, false)
          .has_value());
  EXPECT_EQ("+cpu +memory +io +cpuset",
            ReadText(fs.Root() / "crane/cgroup.subtree_control"));
}

TEST(CgroupV2FsBackendTest, DirectResourceWriteAndMigrationUseCgroupFiles) {
  FakeCgroupFs fs;
  fs.PrepareNode(fs.Root() / "crane");
  fs.PrepareNode(fs.Root() / "crane/job_1");
  WriteText(fs.Root() / "crane/job_1/cpu.max", "");

  CgroupV2FsBackend backend(CgroupV2CleanupMode::SYNC_RMDIR, fs.Root());
  ASSERT_TRUE(backend.WriteControllerFile("crane/job_1",
                                          ControllerFile::CPU_MAX_V2,
                                          "1000 65536"));
  EXPECT_EQ("1000 65536", ReadText(fs.Root() / "crane/job_1/cpu.max"));

  ASSERT_TRUE(backend.MigrateProcIn("crane/job_1", 12345));
  EXPECT_EQ("12345", ReadText(fs.Root() / "crane/job_1/cgroup.procs"));
}

TEST(CgroupV2FsBackendTest, SigkillUsesCgroupKillButSigtermDoesNot) {
  FakeCgroupFs fs;
  fs.PrepareNode(fs.Root() / "crane");
  fs.PrepareNode(fs.Root() / "crane/job_1");
  WriteText(fs.Root() / "crane/job_1/cgroup.kill", "");

  CgroupV2FsBackend backend(CgroupV2CleanupMode::SYNC_RMDIR, fs.Root());
  ASSERT_TRUE(backend.KillAllProcesses("crane/job_1", SIGKILL));
  EXPECT_EQ("1", ReadText(fs.Root() / "crane/job_1/cgroup.kill"));

  WriteText(fs.Root() / "crane/job_1/cgroup.kill", "");
  ASSERT_TRUE(backend.KillAllProcesses("crane/job_1", SIGTERM));
  EXPECT_EQ("", ReadText(fs.Root() / "crane/job_1/cgroup.kill"));
}

TEST(CgroupV2FsBackendTest, AsyncJanitorDrainsQueuedRmdir) {
  FakeCgroupFs fs;
  auto path = fs.Root() / "crane/job_1";
  std::filesystem::create_directories(path);

  CgroupV2FsBackend backend(CgroupV2CleanupMode::ASYNC_RMDIR, fs.Root());
  ASSERT_TRUE(backend.Destroy("crane/job_1"));
  EXPECT_TRUE(backend.DrainJanitor(std::chrono::seconds{2}));
  EXPECT_FALSE(std::filesystem::exists(path));
}

}  // namespace

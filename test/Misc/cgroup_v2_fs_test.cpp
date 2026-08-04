// The generated CRI Signal enum must be parsed before signal macros from GTest.
// clang-format off
#include "CgroupV2Fs.h"
#include <gtest/gtest.h>
// clang-format on
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <deque>
#include <filesystem>
#include <format>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <thread>
#include <type_traits>
#include <vector>

namespace {

using Craned::Common::CG_V2_REQUIRED_CONTROLLERS;
using Craned::Common::CgroupDestroyCompletion;
using Craned::Common::CgroupInterface;
using Craned::Common::CgroupManager;
using Craned::Common::CgroupV2CleanupMode;
using Craned::Common::CgroupV2FsBackend;
using Craned::Common::CgConstant::ControllerFile;

using CgroupDestroySignature =
    bool (CgroupInterface::*)(CgroupDestroyCompletion);
using KillAndDestroySignature = void (*)(
    std::unique_ptr<CgroupInterface>, CgroupManager::CgroupCleanupCompletion);

static_assert(std::is_same_v<decltype(&CgroupInterface::Destroy),
                             CgroupDestroySignature>);
static_assert(std::is_same_v<decltype(&CgroupManager::KillAndDestroyCgroup),
                             KillAndDestroySignature>);

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

class RecordingCgroup final : public CgroupInterface {
 public:
  RecordingCgroup(std::vector<std::string>* calls,
                  std::deque<bool> empty_results, bool destroy_result,
                  int* last_signal = nullptr)
      : CgroupInterface("crane/job_1", nullptr),
        calls_(calls),
        empty_results_(std::move(empty_results)),
        destroy_result_(destroy_result),
        last_signal_(last_signal) {}

  bool SetCpuCoreLimit(double) override { return true; }
  bool SetCpuShares(uint64_t) override { return true; }
  bool SetCpuSet(const std::unordered_set<uint32_t>&) override { return true; }
  bool SetCpusetMems(const std::string&) override { return true; }
  bool SetMemoryLimitBytes(uint64_t) override { return true; }
  bool SetMemorySwLimitBytes(uint64_t) override { return true; }
  bool SetMemorySoftLimitBytes(uint64_t) override { return true; }
  bool SetBlockioWeight(uint64_t) override { return true; }
  bool SetDeviceAccess(const std::unordered_set<SlotId>&, bool, bool,
                       bool) override {
    return true;
  }

  bool KillAllProcesses(int signum) override {
    calls_->emplace_back("kill");
    if (last_signal_ != nullptr) *last_signal_ = signum;
    return true;
  }

  bool Empty() override {
    calls_->emplace_back("empty");
    if (empty_results_.empty()) return true;
    const bool result = empty_results_.front();
    empty_results_.pop_front();
    return result;
  }

  bool Destroy(CgroupDestroyCompletion completion) override {
    calls_->emplace_back("destroy");
    if (completion) completion(destroy_result_);
    return destroy_result_;
  }

 private:
  std::vector<std::string>* calls_;
  std::deque<bool> empty_results_;
  bool destroy_result_;
  int* last_signal_;
};

TEST(CgroupCleanupContractTest, DrainsBeforeDestroyAndCompletesAfterDestroy) {
  std::vector<std::string> calls;
  std::optional<CgroupManager::CgroupCleanupResult> result;
  int last_signal = 0;
  auto cgroup = std::make_unique<RecordingCgroup>(
      &calls, std::deque<bool>{false, true}, true, &last_signal);

  CgroupManager::KillAndDestroyCgroup(
      std::move(cgroup),
      [&](CgroupManager::CgroupCleanupResult cleanup_result) {
        calls.emplace_back("completion");
        result = cleanup_result;
      });

  EXPECT_EQ(calls, (std::vector<std::string>{"empty", "kill", "empty",
                                             "destroy", "completion"}));
  EXPECT_EQ(last_signal, SIGKILL);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->processes_drained);
  EXPECT_TRUE(result->cgroup_destroyed);
  EXPECT_TRUE(result->Succeeded());
}

TEST(CgroupCleanupContractTest, PropagatesDestroyFailureExactlyOnce) {
  std::vector<std::string> calls;
  std::optional<CgroupManager::CgroupCleanupResult> result;

  CgroupManager::KillAndDestroyCgroup(
      std::make_unique<RecordingCgroup>(&calls, std::deque<bool>{true}, false),
      [&](CgroupManager::CgroupCleanupResult cleanup_result) {
        calls.emplace_back("completion");
        ASSERT_FALSE(result.has_value());
        result = cleanup_result;
      });

  EXPECT_EQ(calls,
            (std::vector<std::string>{"empty", "destroy", "completion"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->processes_drained);
  EXPECT_FALSE(result->cgroup_destroyed);
  EXPECT_FALSE(result->Succeeded());
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
  ASSERT_TRUE(backend.WriteControllerFile(
      "crane/job_1", ControllerFile::CPU_MAX_V2, "1000 65536"));
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
  std::atomic_uint completion_count{0};
  std::atomic_bool completion_succeeded{false};
  std::atomic_bool completion_observed_removal{false};
  ASSERT_TRUE(backend.Destroy("crane/job_1", [&](bool succeeded) {
    completion_succeeded.store(succeeded, std::memory_order_release);
    completion_observed_removal.store(!std::filesystem::exists(path),
                                      std::memory_order_release);
    completion_count.fetch_add(1, std::memory_order_release);
  }));
  EXPECT_TRUE(backend.DrainJanitor(std::chrono::seconds{2}));
  EXPECT_FALSE(std::filesystem::exists(path));
  EXPECT_EQ(completion_count.load(std::memory_order_acquire), 1U);
  EXPECT_TRUE(completion_succeeded.load(std::memory_order_acquire));
  EXPECT_TRUE(completion_observed_removal.load(std::memory_order_acquire));
}

TEST(CgroupV2FsBackendTest, AsyncJanitorRetriesAndCompletesExactlyOnce) {
  FakeCgroupFs fs;
  auto path = fs.Root() / "crane/job_1";
  std::filesystem::create_directories(path);
  auto blocker = path / "regular-file-blocks-rmdir";
  WriteText(blocker, "block");

  CgroupV2FsBackend backend(CgroupV2CleanupMode::ASYNC_RMDIR, fs.Root());
  std::atomic_uint completion_count{0};
  std::atomic_bool completion_succeeded{false};
  ASSERT_TRUE(backend.Destroy("crane/job_1", [&](bool succeeded) {
    completion_succeeded.store(succeeded, std::memory_order_release);
    completion_count.fetch_add(1, std::memory_order_release);
  }));

  // The initial attempt sees the regular file and fails. Removing it before
  // the first backoff expires lets the queued retry prove its callback path.
  std::this_thread::sleep_for(std::chrono::milliseconds{100});
  ASSERT_TRUE(std::filesystem::remove(blocker));
  EXPECT_TRUE(backend.DrainJanitor(std::chrono::seconds{2}));
  EXPECT_FALSE(std::filesystem::exists(path));
  EXPECT_TRUE(completion_succeeded.load(std::memory_order_acquire));
  EXPECT_EQ(completion_count.load(std::memory_order_acquire), 1U);
}

TEST(CgroupV2FsBackendTest, DestructorDrainsRetryBeforeStoppingJanitor) {
  FakeCgroupFs fs;
  auto path = fs.Root() / "crane/job_1";
  std::filesystem::create_directories(path);
  auto blocker = path / "regular-file-blocks-rmdir";
  WriteText(blocker, "block");

  std::atomic_uint completion_count{0};
  std::atomic_bool completion_succeeded{false};
  std::jthread unblocker([blocker] {
    std::this_thread::sleep_for(std::chrono::milliseconds{100});
    std::error_code ec;
    std::filesystem::remove(blocker, ec);
  });
  {
    CgroupV2FsBackend backend(CgroupV2CleanupMode::ASYNC_RMDIR, fs.Root());
    ASSERT_TRUE(backend.Destroy("crane/job_1", [&](bool succeeded) {
      completion_succeeded.store(succeeded, std::memory_order_release);
      completion_count.fetch_add(1, std::memory_order_release);
    }));
  }
  unblocker.join();

  EXPECT_FALSE(std::filesystem::exists(path));
  EXPECT_TRUE(completion_succeeded.load(std::memory_order_acquire));
  EXPECT_EQ(completion_count.load(std::memory_order_acquire), 1U);
}

TEST(CgroupV2FsBackendTest, SyncRmdirCompletesAfterPhysicalRemoval) {
  FakeCgroupFs fs;
  auto path = fs.Root() / "crane/job_1";
  std::filesystem::create_directories(path);

  CgroupV2FsBackend backend(CgroupV2CleanupMode::SYNC_RMDIR, fs.Root());
  bool completion_called = false;
  ASSERT_TRUE(backend.Destroy("crane/job_1", [&](bool succeeded) {
    EXPECT_TRUE(succeeded);
    EXPECT_FALSE(std::filesystem::exists(path));
    completion_called = true;
  }));
  EXPECT_TRUE(completion_called);
}

}  // namespace

// Parse generated CRI enums before GTest defines signal-related macros.
// clang-format off
#include "CgroupManager.h"
#include "crane/ExecutionFlow.h"
#include <gtest/gtest.h>
// clang-format on

#include <csignal>
#include <deque>
#include <memory>
#include <optional>
#include <type_traits>
#include <vector>

#ifndef CRANE_CGROUP_CLEANUP_FLOW_EXPECTED
#  error "CRANE_CGROUP_CLEANUP_FLOW_EXPECTED must be set by the matrix target"
#endif

namespace {

using Craned::Common::CgroupDestroyCompletion;
using Craned::Common::CgroupInterface;
using Craned::Common::CgroupManager;

static_assert(crane::kExecutionFlowCompiledIn ==
              static_cast<bool>(CRANE_CGROUP_CLEANUP_FLOW_EXPECTED));

using CgroupDestroySignature =
    bool (CgroupInterface::*)(CgroupDestroyCompletion);
using KillAndDestroySignature = void (*)(
    std::unique_ptr<CgroupInterface>, CgroupManager::CgroupCleanupCompletion);

static_assert(std::is_same_v<decltype(&CgroupInterface::Destroy),
                             CgroupDestroySignature>);
static_assert(std::is_same_v<decltype(&CgroupManager::KillAndDestroyCgroup),
                             KillAndDestroySignature>);

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

}  // namespace

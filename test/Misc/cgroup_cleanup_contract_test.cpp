// Parse generated CRI enums before GTest defines signal-related macros.
// clang-format off
#include "CgroupManager.h"
#include "CleanupLifecycle.h"
#include "crane/ExecutionFlow.h"
#include <gtest/gtest.h>
// clang-format on

#include <csignal>
#include <deque>
#include <functional>
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

class DeferredDestroyCgroup final : public CgroupInterface {
 public:
  DeferredDestroyCgroup(std::vector<std::string>* calls,
                        CgroupDestroyCompletion* deferred)
      : CgroupInterface("crane/job_1/step_1/system", nullptr),
        calls_(calls),
        deferred_(deferred) {}

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
  bool KillAllProcesses(int) override { return true; }
  bool Empty() override {
    calls_->emplace_back("empty");
    return true;
  }
  bool Destroy(CgroupDestroyCompletion completion) override {
    calls_->emplace_back("destroy-submitted");
    *deferred_ = std::move(completion);
    return true;
  }

 private:
  std::vector<std::string>* calls_;
  CgroupDestroyCompletion* deferred_;
};

class RejectedDestroyCgroup final : public CgroupInterface {
 public:
  explicit RejectedDestroyCgroup(std::vector<std::string>* calls)
      : CgroupInterface("crane/job_1/step_1/system", nullptr), calls_(calls) {}

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
  bool KillAllProcesses(int) override { return true; }
  bool Empty() override {
    calls_->emplace_back("empty");
    return true;
  }
  bool Destroy(CgroupDestroyCompletion completion) override {
    calls_->emplace_back("destroy-rejected");
    if (completion) completion(false);
    return false;
  }

 private:
  std::vector<std::string>* calls_;
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

struct StepResult {
  bool processes_drained{true};
  bool cgroup_destroyed{true};
  bool step_directory_removed{true};
};

TEST(CgroupCleanupContractTest,
     StepDirectoryRemovalRunsOnlyAfterPhysicalDestroyCompletion) {
  std::vector<std::string> calls;
  CgroupDestroyCompletion deferred;
  std::optional<StepResult> result;

  Craned::detail::CoordinateCgroupCleanup(
      [&](CgroupManager::CgroupCleanupCompletion completion) {
        CgroupManager::KillAndDestroyCgroup(
            std::make_unique<DeferredDestroyCgroup>(&calls, &deferred),
            std::move(completion));
      },
      [&](const CgroupManager::CgroupCleanupResult& cgroup_result) {
        return Craned::detail::FinalizeStepCgroupCleanup<StepResult>(
            cgroup_result, [&] {
              calls.emplace_back("remove-step-directory");
              return true;
            });
      },
      std::function<void(StepResult)>{[&](StepResult cleanup_result) {
        calls.emplace_back("completion");
        result = cleanup_result;
      }});

  EXPECT_EQ(calls, (std::vector<std::string>{"empty", "destroy-submitted"}));
  EXPECT_FALSE(result.has_value());
  ASSERT_TRUE(static_cast<bool>(deferred));

  deferred(true);

  EXPECT_EQ(calls,
            (std::vector<std::string>{"empty", "destroy-submitted",
                                      "remove-step-directory", "completion"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->processes_drained);
  EXPECT_TRUE(result->cgroup_destroyed);
  EXPECT_TRUE(result->step_directory_removed);
}

TEST(CgroupCleanupContractTest,
     DeferredDestroyFailureSkipsParentRemovalAndCompletesOnce) {
  std::vector<std::string> calls;
  CgroupDestroyCompletion deferred;
  std::optional<StepResult> result;
  int completion_count = 0;

  Craned::detail::CoordinateCgroupCleanup(
      [&](CgroupManager::CgroupCleanupCompletion completion) {
        CgroupManager::KillAndDestroyCgroup(
            std::make_unique<DeferredDestroyCgroup>(&calls, &deferred),
            std::move(completion));
      },
      [&](const CgroupManager::CgroupCleanupResult& cgroup_result) {
        return Craned::detail::FinalizeStepCgroupCleanup<StepResult>(
            cgroup_result, [&] {
              calls.emplace_back("remove-step-directory");
              return true;
            });
      },
      std::function<void(StepResult)>{[&](StepResult cleanup_result) {
        ++completion_count;
        calls.emplace_back("completion");
        result = cleanup_result;
      }});

  ASSERT_TRUE(static_cast<bool>(deferred));
  deferred(false);

  EXPECT_EQ(calls, (std::vector<std::string>{"empty", "destroy-submitted",
                                             "completion"}));
  EXPECT_EQ(completion_count, 1);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->processes_drained);
  EXPECT_FALSE(result->cgroup_destroyed);
  EXPECT_FALSE(result->step_directory_removed);
}

TEST(CgroupCleanupContractTest,
     RejectedDestroySkipsParentRemovalAndCompletesOnce) {
  std::vector<std::string> calls;
  std::optional<StepResult> result;
  int completion_count = 0;

  Craned::detail::CoordinateCgroupCleanup(
      [&](CgroupManager::CgroupCleanupCompletion completion) {
        CgroupManager::KillAndDestroyCgroup(
            std::make_unique<RejectedDestroyCgroup>(&calls),
            std::move(completion));
      },
      [&](const CgroupManager::CgroupCleanupResult& cgroup_result) {
        return Craned::detail::FinalizeStepCgroupCleanup<StepResult>(
            cgroup_result, [&] {
              calls.emplace_back("remove-step-directory");
              return true;
            });
      },
      std::function<void(StepResult)>{[&](StepResult cleanup_result) {
        ++completion_count;
        calls.emplace_back("completion");
        result = cleanup_result;
      }});

  EXPECT_EQ(calls, (std::vector<std::string>{"empty", "destroy-rejected",
                                             "completion"}));
  EXPECT_EQ(completion_count, 1);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->processes_drained);
  EXPECT_FALSE(result->cgroup_destroyed);
  EXPECT_FALSE(result->step_directory_removed);
}

TEST(CgroupCleanupContractTest, CoordinatorAdaptsJobCleanupResultExactlyOnce) {
  std::vector<std::string> calls;
  CgroupDestroyCompletion deferred;
  std::vector<bool> results;

  Craned::detail::CoordinateCgroupCleanup(
      [&](CgroupManager::CgroupCleanupCompletion completion) {
        CgroupManager::KillAndDestroyCgroup(
            std::make_unique<DeferredDestroyCgroup>(&calls, &deferred),
            std::move(completion));
      },
      [&](const CgroupManager::CgroupCleanupResult& cgroup_result) {
        calls.emplace_back("adapt-job-result");
        return cgroup_result.Succeeded();
      },
      std::function<void(bool)>{[&](bool succeeded) {
        calls.emplace_back("completion");
        results.push_back(succeeded);
      }});

  EXPECT_EQ(calls, (std::vector<std::string>{"empty", "destroy-submitted"}));
  deferred(true);
  EXPECT_EQ(calls,
            (std::vector<std::string>{"empty", "destroy-submitted",
                                      "adapt-job-result", "completion"}));
  EXPECT_EQ(results, (std::vector<bool>{true}));
}

TEST(CgroupCleanupContractTest, AggregatesDaemonCleanupCompletionsExactlyOnce) {
  std::vector<bool> results;
  Craned::detail::CleanupCompletionBarrier barrier(
      2, true, [&](bool succeeded) { results.push_back(succeeded); });
  barrier.Complete(true);
  EXPECT_TRUE(results.empty());
  barrier.Complete(false);
  EXPECT_EQ(results, (std::vector<bool>{false}));
}

TEST(CgroupCleanupContractTest, PreservesDaemonCleanupAndWaiterOrder) {
  std::vector<std::string> calls;
  Craned::detail::RunDaemonCleanupSequence(
      [&] { calls.emplace_back("step-cleanup"); },
      [&] { calls.emplace_back("epilog-and-job-cleanup"); },
      [&] { calls.emplace_back("erase-step"); },
      [&] { calls.emplace_back("terminal-status"); },
      [&] { calls.emplace_back("resolve-waiters"); });
  EXPECT_EQ(calls, (std::vector<std::string>{
                       "step-cleanup", "epilog-and-job-cleanup", "erase-step",
                       "terminal-status", "resolve-waiters"}));
}

TEST(CgroupCleanupContractTest, PreservesDaemonResourceShutdownOrder) {
  std::vector<std::string> calls;
  Craned::detail::RunCranedResourceShutdownSequence(
      [&] { calls.emplace_back("cpu-pool"); },
      [&] { calls.emplace_back("cgroup-fast-path"); },
      [&] { calls.emplace_back("execution-flow"); },
      [&] { calls.emplace_back("tracing"); },
      [&] { calls.emplace_back("plugin"); });
  EXPECT_EQ(calls,
            (std::vector<std::string>{"cpu-pool", "cgroup-fast-path",
                                      "execution-flow", "tracing", "plugin"}));
}

}  // namespace

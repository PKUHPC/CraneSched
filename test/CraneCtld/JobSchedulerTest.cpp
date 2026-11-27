// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <memory>
#include <utility>
#include <vector>

#include "Accounting/AccountMetaContainer.h"
#include "Array.h"
#include "Database/EmbeddedDbClient.h"
#include "Database/LegacyEmbeddedDbClient.h"
#include "JobScheduler.h"

namespace {

constexpr uint64_t kMemPerCpu = 3872ULL * 1024 * 1024;
constexpr uint64_t kJobMemPerNode = 4 * kMemPerCpu;
constexpr char kPartition[] = "step-memory-test";

class JobSchedulerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    original_partitions_ = std::exchange(g_config.Partitions, {});
    original_nodes_ = std::exchange(g_config.Nodes, {});
    auto& partition = g_config.Partitions[kPartition];
    partition.default_mem_per_cpu = kMemPerCpu;
    partition.max_mem_per_cpu = kMemPerCpu;
    SetJobAllocation(1);
  }

  void TearDown() override {
    g_config.Partitions = std::move(original_partitions_);
    g_config.Nodes = std::move(original_nodes_);
  }

  void SetJobAllocation(uint32_t nodes,
                        uint64_t memory_per_node = kJobMemPerNode) {
    crane::grpc::JobToCtld request;
    request.set_type(crane::grpc::Batch);
    request.set_uid(1001);
    request.add_gids(1001);
    request.set_partition_name(kPartition);
    request.set_node_num_min(nodes);
    request.set_node_num_max(nodes);
    request.set_ntasks_per_node(4);
    request.set_ntasks(4 * nodes);
    request.set_cpus_per_task(1);
    request.set_mem_per_node(memory_per_node);
    request.mutable_time_limit()->set_seconds(3600);
    job_.SetFieldsByJobToCtld(request);
    job_.SetJobId(42);
    job_.SetReqTotalResView(job_.req_node_res_view * nodes +
                            job_.req_task_res_view * request.ntasks());

    ResourceV3 allocation;
    std::vector<CranedId> craned_ids;
    for (uint32_t i = 0; i < nodes; ++i) {
      auto node = "node" + std::to_string(i);
      craned_ids.push_back(node);
      g_config.Partitions.at(kPartition).nodes.insert(node);
      g_config.Nodes.try_emplace(node, std::make_shared<Ctld::Config::Node>());
      ResourceInNodeV3 resource;
      resource.GetCpuSet().cpu_count = cpu_t{4};
      resource.GetCpuSet().core_ids = {0, 1, 2, 3};
      resource.SetMemoryBytes(memory_per_node);
      resource.SetMemorySwBytes(memory_per_node);
      allocation.AddResourceInNode(node, resource);
    }
    job_.SetCranedIds(std::move(craned_ids));
    job_.SetStepResAvail(allocation);
    job_.SetAllocatedRes(std::move(allocation));
  }

  crane::grpc::StepToCtld Request(uint32_t ntasks = 1, uint32_t nodes = 1,
                                  uint32_t cpus_per_task = 1) {
    crane::grpc::StepToCtld request;
    request.set_job_id(job_.JobId());
    request.set_uid(job_.uid);
    request.add_gids(1001);
    request.set_type(crane::grpc::Interactive);
    request.mutable_interactive_meta()->set_interactive_type(crane::grpc::Crun);
    request.mutable_time_limit()->set_seconds(60);
    request.set_node_num(nodes);
    request.set_ntasks(ntasks);
    request.set_cpus_per_task(cpus_per_task);
    // crun inherits these fields even when -n selects fewer tasks.
    request.set_ntasks_per_node(4);
    request.set_mem_per_node(kJobMemPerNode);
    return request;
  }

  std::unique_ptr<Ctld::CommonStepInCtld> MakeStep(
      const crane::grpc::StepToCtld& request, step_id_t step_id = 1) {
    auto step = std::make_unique<Ctld::CommonStepInCtld>();
    step->SetFieldsByStepToCtld(request);
    step->SetStepId(step_id);
    step->job = &job_;
    return step;
  }

  static CraneExpected<void> Admit(Ctld::CommonStepInCtld* step) {
    auto result = Ctld::JobScheduler::AcquireStepAttributes(step);
    if (!result) return result;
    return Ctld::JobScheduler::CheckStepValidity(step);
  }

  Ctld::JobInCtld job_;

 private:
  decltype(g_config.Partitions) original_partitions_;
  decltype(g_config.Nodes) original_nodes_;
};

TEST_F(JobSchedulerTest, SchedulesFewerTasksWithInheritedNodeMemory) {
  auto step = MakeStep(Request(2));
  ASSERT_TRUE(Admit(step.get()).has_value());
  EXPECT_EQ(step->req_node_res_view.GetMemoryBytes(), kJobMemPerNode);

  job_.AddStep(std::move(step));
  std::vector<Ctld::CommonStepInCtld*> scheduled;
  ASSERT_EQ(job_.SchedulePendingSteps(&scheduled), 1);
  ASSERT_EQ(scheduled.size(), 1);
  EXPECT_EQ(scheduled.front()->task_res_map.size(), 2);
  const auto& allocation = scheduled.front()->AllocatedRes().At("node0");
  EXPECT_EQ(allocation.GetMemoryBytes(), kJobMemPerNode);
  EXPECT_EQ(allocation.GetCpuSet().cpu_count, cpu_t{2});
}

TEST_F(JobSchedulerTest, PreservesMemoryPerCpuAbovePartitionLimit) {
  auto request = Request();
  request.clear_mem_per_node();
  request.set_mem_per_cpu(2 * kMemPerCpu);
  auto step = MakeStep(request);
  ASSERT_TRUE(Admit(step.get()).has_value());
  EXPECT_EQ(step->req_task_res_view.GetMemoryBytes(), 2 * kMemPerCpu);
  EXPECT_EQ(step->req_task_res_view.GetMemorySwBytes(), 2 * kMemPerCpu);

  job_.AddStep(std::move(step));
  std::vector<Ctld::CommonStepInCtld*> scheduled;
  ASSERT_EQ(job_.SchedulePendingSteps(&scheduled), 1);
  EXPECT_EQ(job_.StepResAvail().At("node0").GetMemoryBytes(), 2 * kMemPerCpu);
}

TEST_F(JobSchedulerTest, PreservesNodeMemoryAfterPartitionLimitIsLowered) {
  auto& partition = g_config.Partitions.at(kPartition);
  partition.max_mem_per_cpu = 0;
  partition.max_mem_per_node = kMemPerCpu;
  auto request = Request();
  request.set_mem_per_node(2 * kMemPerCpu);
  auto step = MakeStep(request);
  ASSERT_TRUE(Admit(step.get()).has_value());
  EXPECT_EQ(step->req_node_res_view.GetMemoryBytes(), 2 * kMemPerCpu);
  EXPECT_EQ(step->req_node_res_view.GetMemorySwBytes(), 2 * kMemPerCpu);
}

TEST_F(JobSchedulerTest, DoesNotReduceTaskCountForPartitionNodeMemoryLimit) {
  g_config.Partitions.at(kPartition).max_mem_per_node = 3 * kMemPerCpu;
  auto request = Request(4);
  request.clear_mem_per_node();
  request.set_mem_per_cpu(kMemPerCpu);
  auto step = MakeStep(request);
  ASSERT_TRUE(Admit(step.get()).has_value());
  EXPECT_EQ(step->ntasks_per_node_min, 4);
  EXPECT_EQ(step->ntasks_per_node_max, 4);
  EXPECT_EQ(step->req_total_res_view.GetMemoryBytes(), kJobMemPerNode);
}

TEST_F(JobSchedulerTest, RejectsMemoryBeyondParentAllocationWithoutClamping) {
  g_config.Partitions.at(kPartition).max_mem_per_node = kJobMemPerNode;
  for (bool per_cpu : {false, true}) {
    SCOPED_TRACE(per_cpu);
    auto request = Request();
    if (per_cpu) {
      request.clear_mem_per_node();
      request.set_mem_per_cpu(kJobMemPerNode + 1);
    } else {
      request.set_mem_per_node(kJobMemPerNode + 1);
    }
    auto step = MakeStep(request);
    ASSERT_TRUE(
        Ctld::JobScheduler::AcquireStepAttributes(step.get()).has_value());
    EXPECT_EQ(step->req_total_res_view.GetMemoryBytes(), kJobMemPerNode + 1);
    const auto result = Ctld::JobScheduler::CheckStepValidity(step.get());
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), CraneErrCode::ERR_NO_ENOUGH_NODE);
  }
}

TEST_F(JobSchedulerTest, ChecksFractionalCpuAgainstWholeTaskCapacity) {
  SetJobAllocation(2);
  auto allocation = job_.AllocatedRes();
  auto& first_node = allocation.At("node0").GetCpuSet();
  auto& second_node = allocation.At("node1").GetCpuSet();
  first_node.cpu_count = second_node.cpu_count =
      cpu_t::from_raw_value(cpu_t{4}.raw_value() - 1);

  auto request = Request(1, 1, 4);
  auto step = MakeStep(request);
  ASSERT_TRUE(
      Ctld::JobScheduler::AcquireStepAttributes(step.get()).has_value());
  job_.SetAllocatedRes(ResourceV3{allocation});
  const auto result = Ctld::JobScheduler::CheckStepValidity(step.get());
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), CraneErrCode::ERR_NO_ENOUGH_NODE);

  second_node.cpu_count = cpu_t{4};
  job_.SetStepResAvail(allocation);
  job_.SetAllocatedRes(std::move(allocation));
  ASSERT_TRUE(Ctld::JobScheduler::CheckStepValidity(step.get()).has_value());
  job_.AddStep(std::move(step));
  std::vector<Ctld::CommonStepInCtld*> scheduled;
  ASSERT_EQ(job_.SchedulePendingSteps(&scheduled), 1);
  ASSERT_EQ(scheduled.size(), 1);
  EXPECT_EQ(scheduled.front()->CranedIds(), std::vector<CranedId>{"node1"});
}

TEST_F(JobSchedulerTest, RejectsInsufficientCombinedCapacityOfRequestedNodes) {
  SetJobAllocation(3);
  auto request = Request(5, 2);
  request.clear_mem_per_node();
  request.set_mem_per_cpu(2 * kMemPerCpu);
  auto step = MakeStep(request);
  ASSERT_TRUE(
      Ctld::JobScheduler::AcquireStepAttributes(step.get()).has_value());
  ASSERT_TRUE(step->req_total_res_view <= job_.req_total_res_view);
  const auto result = Ctld::JobScheduler::CheckStepValidity(step.get());
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), CraneErrCode::ERR_NO_ENOUGH_NODE);
}

TEST_F(JobSchedulerTest, SchedulesUnevenTasksAcrossThreeNodes) {
  SetJobAllocation(3, 2 * kMemPerCpu);
  g_config.Partitions.at(kPartition).max_mem_per_node = 2 * kMemPerCpu;
  auto request = Request(5, 3);
  request.clear_mem_per_node();
  request.set_mem_per_cpu(kMemPerCpu);
  auto step = MakeStep(request);
  ASSERT_TRUE(Admit(step.get()).has_value());
  job_.AddStep(std::move(step));
  std::vector<Ctld::CommonStepInCtld*> scheduled;
  ASSERT_EQ(job_.SchedulePendingSteps(&scheduled), 1);
  ASSERT_EQ(scheduled.size(), 1);
  EXPECT_EQ(scheduled.front()->task_res_map.size(), 5);
  std::vector<size_t> tasks_per_node;
  for (const auto& [node, tasks] : scheduled.front()->craned_task_map)
    tasks_per_node.push_back(tasks.size());
  std::ranges::sort(tasks_per_node);
  EXPECT_EQ(tasks_per_node, (std::vector<size_t>{1, 2, 2}));
}

TEST_F(JobSchedulerTest, UsesLargestCapacitiesAcrossRequestedNodes) {
  SetJobAllocation(3);
  auto allocation = job_.AllocatedRes();
  for (const auto* node : {"node0", "node1"}) {
    allocation.At(node).SetMemoryBytes(3 * kMemPerCpu);
    allocation.At(node).SetMemorySwBytes(3 * kMemPerCpu);
  }
  job_.SetStepResAvail(allocation);
  job_.SetAllocatedRes(std::move(allocation));
  auto request = Request(7, 2);
  request.clear_mem_per_node();
  request.set_mem_per_cpu(kMemPerCpu);
  auto step = MakeStep(request);
  ASSERT_TRUE(Admit(step.get()).has_value());
  job_.AddStep(std::move(step));
  std::vector<Ctld::CommonStepInCtld*> scheduled;
  ASSERT_EQ(job_.SchedulePendingSteps(&scheduled), 1);
  ASSERT_EQ(scheduled.size(), 1);
  EXPECT_EQ(scheduled.front()->task_res_map.size(), 7);
  EXPECT_EQ(scheduled.front()->craned_task_map.size(), 2);
  EXPECT_EQ(scheduled.front()->craned_task_map.at("node2").size(), 4);
}

TEST_F(JobSchedulerTest, ChecksCapacityWithinNodeFilters) {
  SetJobAllocation(2);
  auto allocation = job_.AllocatedRes();
  allocation.At("node0").SetMemoryBytes(kMemPerCpu);
  allocation.At("node0").SetMemorySwBytes(kMemPerCpu);
  job_.SetStepResAvail(allocation);
  job_.SetAllocatedRes(std::move(allocation));
  for (bool include : {false, true}) {
    SCOPED_TRACE(include);
    auto request = Request(2);
    request.clear_mem_per_node();
    request.set_mem_per_cpu(kMemPerCpu);
    if (include)
      request.set_nodelist("node0");
    else
      request.set_excludes("node1");
    auto step = MakeStep(request);
    ASSERT_TRUE(
        Ctld::JobScheduler::AcquireStepAttributes(step.get()).has_value());
    const auto result = Ctld::JobScheduler::CheckStepValidity(step.get());
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), CraneErrCode::ERR_NO_ENOUGH_NODE);
  }
}

}  // namespace

namespace Ctld {
class ArrayRequeueCancellationTest : public ::testing::Test {
 protected:
  class FailingStore : public IEmbeddedDb {
   public:
    FailingStore(std::unique_ptr<IEmbeddedDb> store, bool fail_delete)
        : store_(std::move(store)), fail_delete_(fail_delete) {}
    std::expected<void, DbErrorCode> Init(const std::string& path) override {
      return store_->Init(path);
    }
    std::expected<void, DbErrorCode> Close() override {
      return store_->Close();
    }
    std::expected<void, DbErrorCode> Store(txn_id_t txn, const std::string& key,
                                           const void* data,
                                           size_t len) override {
      if (!fail_delete_ && !failed) {
        failed = true;
        return std::unexpected(DbErrorCode::OTHER);
      }
      return store_->Store(txn, key, data, len);
    }
    std::expected<size_t, DbErrorCode> Fetch(txn_id_t txn,
                                             const std::string& key, void* buf,
                                             size_t* len) override {
      return store_->Fetch(txn, key, buf, len);
    }
    std::expected<void, DbErrorCode> Delete(txn_id_t txn,
                                            const std::string& key) override {
      if (fail_delete_ && !failed) {
        failed = true;
        return std::unexpected(DbErrorCode::OTHER);
      }
      return store_->Delete(txn, key);
    }
    std::expected<txn_id_t, DbErrorCode> Begin() override {
      return store_->Begin();
    }
    std::expected<void, DbErrorCode> Commit(txn_id_t txn) override {
      return store_->Commit(txn);
    }
    std::expected<void, DbErrorCode> Abort(txn_id_t txn) override {
      return store_->Abort(txn);
    }
    std::expected<void, DbErrorCode> IterateAllKv(KvIterFunc fn) override {
      return store_->IterateAllKv(std::move(fn));
    }
    const std::string& DbPath() override { return store_->DbPath(); }
    bool failed{false};

   private:
    std::unique_ptr<IEmbeddedDb> store_;
    bool fail_delete_;
  };

  void ExerciseFailedRequeue(bool fail_counter_reset) {
    auto& scheduler = *g_job_scheduler;
    auto* parent = MakeParent(scheduler);
    const auto parent_id = parent->JobId();
    auto& arrays = *scheduler.m_array_manager_;
    auto child = arrays.MaterializeChildForAllocation(parent, absl::Now());
    ASSERT_NE(child, nullptr);
    const auto child_id = child->JobId();
    const auto child_db_id = child->JobDbId();
    child->SetCranedIds({"node0"});
    child->SetStatus(crane::grpc::Failed);
    child->SetExitCode(17);
    child->SetStartTime(absl::FromUnixSeconds(100));
    child->SetEndTime(absl::FromUnixSeconds(120));
    ASSERT_TRUE(g_embedded_db_client->UpdateRuntimeAttrOfJob(
        0, child_db_id, child->RuntimeAttr()));
    const auto terminal_attr = child->RuntimeAttr();

    auto step = std::make_unique<CommonStepInCtld>();
    step->job = child.get();
    step->job_id = child_id;
    ASSERT_TRUE(g_embedded_db_client->AppendSteps({step.get()}));
    auto& db = static_cast<LegacyEmbeddedDbClient&>(*g_embedded_db_client);
    auto& store = fail_counter_reset ? db.m_step_var_db_ : db.m_variable_db_;
    auto failing =
        std::make_unique<FailingStore>(std::move(store), fail_counter_reset);
    auto* fault = failing.get();
    store = std::move(failing);

    // Cover a recorded result before the drop and a later explicit cancel.
    if (!fail_counter_reset)
      arrays.RecordParentTerminalResult(parent_id, crane::grpc::Deadline,
                                        ExitCode::EC_REACHED_DEADLINE);
    std::vector<std::unique_ptr<JobInCtld>> jobs;
    jobs.emplace_back(std::move(child));
    auto final_parents = scheduler.ResetAndRequeueJobs_(jobs);
    EXPECT_TRUE(fault->failed);
    ASSERT_NE(jobs.front(), nullptr);
    EXPECT_EQ(jobs.front()->RuntimeAttr().SerializeAsString(),
              terminal_attr.SerializeAsString());
    EXPECT_TRUE(arrays.UnfinishedChildJobIdsForParent(parent_id).empty());
    EXPECT_FALSE(scheduler.m_pending_job_map_.contains(child_id));
    crane::grpc::JobInEmbeddedDb stored;
    EXPECT_FALSE(
        g_embedded_db_client->FetchJobDataInDb(0, child_db_id, &stored));
    if (fail_counter_reset) {
      EXPECT_TRUE(final_parents.empty());
      EXPECT_TRUE(arrays.IsRegisteredParent(parent_id));
      ExpectSubmitSlots(*parent, 1);
      arrays.RecordParentTerminalResult(parent_id, crane::grpc::Cancelled,
                                        ExitCode::EC_TERMINATED);
      auto result = arrays.ApplyParentTerminalResult(parent_id, absl::Now());
      final_parents = std::move(result.final_parents);
      absl::MutexLock guard(&scheduler.m_pending_job_map_mtx_);
      scheduler.SpliceFinalArrayParentsFromPendingMapNoLock_(final_parents);
    }
    ASSERT_EQ(final_parents.size(), 1U);
    ASSERT_NE(final_parents.front().parent_job, nullptr);
    EXPECT_EQ(final_parents.front().array_job_id, parent_id);
    EXPECT_EQ(parent->Status(), fail_counter_reset ? crane::grpc::Cancelled
                                                   : crane::grpc::Deadline);
    EXPECT_FALSE(arrays.IsRegisteredParent(parent_id));
    EXPECT_FALSE(scheduler.m_pending_job_map_.contains(parent_id));
    ExpectSubmitSlots(*parent, 0);
    EXPECT_FALSE(g_account_meta_container->UserHasJob(parent->Username()));
  }

  void ExercisePendingChildCancellation() {
    auto& scheduler = *g_job_scheduler;
    auto* parent = MakeParent(scheduler);
    auto parent_id = parent->JobId();
    auto child = scheduler.m_array_manager_->MaterializeChildForAllocation(
        parent, absl::Now());
    ASSERT_NE(child, nullptr);
    auto child_id = child->JobId();
    child->ResetForRequeue();
    scheduler.RequeueRecoveredJobIntoPendingQueueLock_(std::move(child));
    ExpectSubmitSlots(*parent, 1);
    {
      absl::MutexLock guard(&scheduler.m_pending_job_map_mtx_);
      auto ids = scheduler.CancelArrayParentNoLock_(
          parent_id, crane::grpc::Cancelled, ExitCode::EC_TERMINATED, true);
      ASSERT_EQ(ids.size(), 1U);
      EXPECT_EQ(ids.front(), child_id);
    }

    EXPECT_FALSE(scheduler.m_pending_job_map_.contains(child_id));
    JobScheduler::CancelJobQueueElem queued{
        JobScheduler::CancelArrayParentQueueElem{}};
    ASSERT_TRUE(scheduler.m_cancel_job_queue_.try_dequeue(queued));
    ASSERT_TRUE(
        std::holds_alternative<JobScheduler::CancelArrayParentQueueElem>(
            queued));
    ASSERT_TRUE(scheduler.m_cancel_job_queue_.try_dequeue(queued));
    auto* cancelled =
        std::get_if<JobScheduler::CancelPendingJobQueueElem>(&queued);
    ASSERT_NE(cancelled, nullptr);
    ASSERT_NE(cancelled->job, nullptr);
    EXPECT_EQ(cancelled->job->JobId(), child_id);
    EXPECT_EQ(cancelled->finish_status, crane::grpc::Cancelled);
    // Exercise the same accounting operation as the Pending queue consumer.
    // The child must not consume the parent's only remaining submit slot.
    auto release_count = scheduler.QosSubmitReserveCount_(*cancelled->job);
    EXPECT_EQ(release_count, 0U);
    g_account_meta_container->FreeMetaSubmitResource(*cancelled->job,
                                                     release_count);
    EXPECT_TRUE(g_account_meta_container->UserHasJob(parent->Username()));
    // A duplicate request queues only the parent, never the same child twice.
    JobScheduler::CancelJobQueueElem duplicate{
        JobScheduler::CancelArrayParentQueueElem{}};
    {
      absl::MutexLock guard(&scheduler.m_pending_job_map_mtx_);
      scheduler.CancelArrayParentNoLock_(parent_id, crane::grpc::Cancelled,
                                         ExitCode::EC_TERMINATED, true);
    }
    ASSERT_TRUE(scheduler.m_cancel_job_queue_.try_dequeue(duplicate));
    EXPECT_TRUE(
        std::holds_alternative<JobScheduler::CancelArrayParentQueueElem>(
            duplicate));
    EXPECT_FALSE(scheduler.m_cancel_job_queue_.try_dequeue(duplicate));
    cancelled->job->SetStatus(crane::grpc::Cancelled);
    auto final = scheduler.m_array_manager_->OnChildTerminal(
        cancelled->job.get(), {cancelled->job.get()});
    ASSERT_EQ(final.size(), 1U);
    EXPECT_EQ(parent->Status(), crane::grpc::Cancelled);
    EXPECT_EQ(parent->ExitCode(), ExitCode::EC_TERMINATED);
    EXPECT_FALSE(g_account_meta_container->UserHasJob(parent->Username()));
    ExpectSubmitSlots(*parent, 0);
  }

  void ExerciseTerminationBeforeChildCompletion(crane::grpc::JobStatus status) {
    auto& scheduler = *g_job_scheduler;
    auto parent = MakeParent(scheduler);
    auto parent_id = parent->JobId();
    auto child = scheduler.m_array_manager_->MaterializeChildForAllocation(
        parent, absl::Now());
    ASSERT_NE(child, nullptr);
    AddParentDependents(parent);

    const auto expected_exit_code = status == crane::grpc::Deadline
                                        ? ExitCode::EC_REACHED_DEADLINE
                                        : ExitCode::EC_TERMINATED;
    if (status == crane::grpc::Deadline) {
      scheduler.m_job_deadline_timer_queue_.enqueue(parent_id);
      scheduler.CancelDeadlineJobCb_();
    } else {
      absl::MutexLock guard(&scheduler.m_pending_job_map_mtx_);
      scheduler.CancelArrayParentNoLock_(parent_id, status, expected_exit_code,
                                         false);
    }
    EXPECT_EQ(parent->Status(), crane::grpc::Pending);
    EXPECT_EQ(parent->StartTime(), absl::UnixEpoch());
    EXPECT_EQ(parent->EndTime(), absl::UnixEpoch());
    EXPECT_EQ(scheduler.m_array_manager_->MaterializeChildForAllocation(
                  parent, absl::Now()),
              nullptr);
    JobScheduler::DependencyEvent event;
    EXPECT_FALSE(scheduler.m_dependency_event_queue_.try_dequeue(event));

    // Complete the last child while the actual parent message is still queued.
    child->SetStatus(crane::grpc::Failed);
    {
      absl::MutexLock guard(&scheduler.m_pending_job_map_mtx_);
      auto final = scheduler.m_array_manager_->OnChildTerminal(child.get(), {});
      ASSERT_EQ(final.size(), 1U);
    }
    EXPECT_EQ(parent->Status(), status);
    EXPECT_EQ(parent->ExitCode(), expected_exit_code);
    ExpectParentEvents(scheduler, *parent);
    EXPECT_FALSE(g_account_meta_container->UserHasJob(parent->Username()));

    scheduler.CleanCancelJobQueueCb_();
    EXPECT_FALSE(scheduler.m_dependency_event_queue_.try_dequeue(event));
  }

  void ExerciseParentWithoutChildren() {
    auto& scheduler = *g_job_scheduler;
    auto* parent = MakeParent(scheduler);
    AddParentDependents(parent);
    auto* arrays = scheduler.m_array_manager_.get();
    {
      absl::MutexLock guard(&scheduler.m_pending_job_map_mtx_);
      scheduler.CancelArrayParentNoLock_(parent->JobId(), crane::grpc::Deadline,
                                         ExitCode::EC_REACHED_DEADLINE, false);
    }
    EXPECT_EQ(parent->Status(), crane::grpc::Pending);
    EXPECT_EQ(parent->EndTime(), absl::UnixEpoch());
    JobScheduler::DependencyEvent event;
    EXPECT_FALSE(scheduler.m_dependency_event_queue_.try_dequeue(event));
    // A later cancellation may widen the scope but must keep the first result.
    arrays->RecordParentTerminalResult(parent->JobId(), crane::grpc::Cancelled,
                                       ExitCode::EC_TERMINATED);
    const auto finish_time =
        absl::FromUnixSeconds(1234) + absl::Milliseconds(500);
    auto result =
        arrays->ApplyParentTerminalResult(parent->JobId(), finish_time);
    ASSERT_EQ(result.final_parents.size(), 1U);
    EXPECT_EQ(parent->Status(), crane::grpc::Deadline);
    EXPECT_EQ(parent->ExitCode(), ExitCode::EC_REACHED_DEADLINE);
    EXPECT_EQ(parent->StartTime(), absl::FromUnixSeconds(1234));
    EXPECT_EQ(parent->EndTime(), absl::FromUnixSeconds(1234));
    ExpectParentEvents(scheduler, *parent);
  }

  void ExerciseDeadlineBeforeCancel() {
    auto& scheduler = *g_job_scheduler;
    auto* parent = MakeParent(scheduler, 2);
    auto& arrays = *scheduler.m_array_manager_;
    auto child = arrays.MaterializeChildForAllocation(parent, absl::Now());
    ASSERT_NE(child, nullptr);
    auto other_child =
        arrays.MaterializeChildForAllocation(parent, absl::Now());
    ASSERT_NE(other_child, nullptr);
    child->ResetForRequeue();
    const auto child_id = child->JobId();
    scheduler.m_pending_job_map_.emplace(child_id, std::move(child));
    other_child->SetStatus(crane::grpc::Configuring);
    auto* configuring_child = other_child.get();
    scheduler.m_running_job_map_.emplace(other_child->JobId(),
                                         std::move(other_child));

    AddParentDependents(parent);
    scheduler.m_job_deadline_timer_queue_.enqueue(parent->JobId());
    scheduler.CancelDeadlineJobCb_();
    scheduler.CleanCancelJobQueueCb_();
    EXPECT_EQ(parent->Status(), crane::grpc::Deadline);
    EXPECT_TRUE(scheduler.m_pending_job_map_.contains(child_id));
    EXPECT_FALSE(configuring_child->CancelRequested());
    const auto first_end_time = parent->EndTime();
    crane::grpc::JobInEmbeddedDb stored;
    ASSERT_TRUE(
        g_embedded_db_client->FetchJobDataInDb(0, parent->JobDbId(), &stored));
    EXPECT_EQ(stored.runtime_attr().status(), crane::grpc::Deadline);
    EXPECT_EQ(stored.runtime_attr().exit_code(), ExitCode::EC_REACHED_DEADLINE);

    {
      absl::MutexLock guard(&scheduler.m_pending_job_map_mtx_);
      scheduler.CancelArrayParentNoLock_(parent->JobId(),
                                         crane::grpc::Cancelled,
                                         ExitCode::EC_TERMINATED, true);
    }
    EXPECT_FALSE(scheduler.m_pending_job_map_.contains(child_id));
    // Inspect Pending dispatch without invoking external history storage.
    JobScheduler::CancelJobQueueElem queued{
        JobScheduler::CancelArrayParentQueueElem{}};
    ASSERT_TRUE(scheduler.m_cancel_job_queue_.try_dequeue(queued));
    ASSERT_TRUE(
        std::holds_alternative<JobScheduler::CancelArrayParentQueueElem>(
            queued));
    auto parent_cancel = std::move(queued);
    ASSERT_TRUE(scheduler.m_cancel_job_queue_.try_dequeue(queued));
    auto* pending =
        std::get_if<JobScheduler::CancelPendingJobQueueElem>(&queued);
    ASSERT_NE(pending, nullptr);
    ASSERT_NE(pending->job, nullptr);
    EXPECT_EQ(pending->job->JobId(), child_id);
    EXPECT_EQ(pending->finish_status, crane::grpc::Cancelled);
    scheduler.m_cancel_job_queue_.enqueue(std::move(parent_cancel));
    scheduler.CleanCancelJobQueueCb_();
    EXPECT_TRUE(configuring_child->CancelRequested());
    EXPECT_EQ(parent->Status(), crane::grpc::Deadline);
    EXPECT_EQ(parent->EndTime(), first_end_time);
    EXPECT_EQ(arrays.UnfinishedChildJobIdsForParent(parent->JobId()).size(),
              2U);
    EXPECT_TRUE(arrays.IsRegisteredParent(parent->JobId()));
    EXPECT_TRUE(g_account_meta_container->UserHasJob(parent->Username()));
    ExpectSubmitSlots(*parent, 2);

    pending->job->SetStatus(pending->finish_status);
    EXPECT_TRUE(arrays.OnChildTerminal(pending->job.get(), {}).empty());
    EXPECT_TRUE(arrays.IsRegisteredParent(parent->JobId()));
    EXPECT_TRUE(g_account_meta_container->UserHasJob(parent->Username()));
    ExpectSubmitSlots(*parent, 2);

    configuring_child->SetStatus(crane::grpc::Cancelled);
    configuring_child->SetExitCode(ExitCode::EC_TERMINATED);
    auto final_parents = arrays.OnChildTerminal(configuring_child, {});
    ASSERT_EQ(final_parents.size(), 1U);
    EXPECT_EQ(final_parents.front().array_job_id, parent->JobId());
    EXPECT_EQ(parent->Status(), crane::grpc::Deadline);
    EXPECT_EQ(parent->ExitCode(), ExitCode::EC_REACHED_DEADLINE);
    EXPECT_FALSE(arrays.IsRegisteredParent(parent->JobId()));
    EXPECT_FALSE(g_account_meta_container->UserHasJob(parent->Username()));
    ExpectSubmitSlots(*parent, 0);
    ExpectParentEvents(scheduler, *parent);
    EXPECT_TRUE(arrays.OnChildTerminal(configuring_child, {}).empty());
  }

  JobInCtld* MakeParent(JobScheduler& scheduler, uint32_t concurrency = 1) {
    auto parent = std::make_unique<JobInCtld>();
    crane::grpc::JobToCtld submission;
    submission.set_uid(::getuid());
    submission.set_type(crane::grpc::Batch);
    submission.mutable_time_limit()->set_seconds(300);
    submission.mutable_array_spec()->set_start(1);
    submission.mutable_array_spec()->set_end(2);
    submission.mutable_array_spec()->set_max_concurrent(concurrency);
    parent->SetFieldsByJobToCtld(submission);
    parent->account_chain.emplace_back("test-account");
    parent->SetStatus(crane::grpc::Pending);
    parent->SetStartTime(absl::UnixEpoch());
    parent->SetEndTime(absl::UnixEpoch());
    EXPECT_TRUE(g_embedded_db_client->AppendJobsToPendingAndAdvanceJobIds(
        {parent.get()}));
    g_account_meta_container->MallocMetaSubmitResource(*parent, concurrency);
    g_account_meta_container->UserAddJob(parent->Username(), concurrency);
    scheduler.m_array_manager_->RegisterParent(parent.get());
    auto* raw = parent.get();
    scheduler.m_pending_job_map_.emplace(raw->JobId(), std::move(parent));
    return raw;
  }

  void ExpectSubmitSlots(const JobInCtld& job, uint32_t expected) {
    uint32_t reserved = 0;
    g_account_meta_container->m_qos_meta_map_.if_contains(
        job.qos,
        [&](const auto& entry) { reserved = entry.second.submit_jobs_count; });
    EXPECT_EQ(reserved, expected);
  }

  void AddParentDependents(JobInCtld* parent) {
    parent->AddDependent(crane::grpc::AFTER, 900);
    parent->AddDependent(crane::grpc::AFTER_ANY, 901);
    parent->AddDependent(crane::grpc::AFTER_OK, 902);
    parent->AddDependent(crane::grpc::AFTER_NOT_OK, 903);
  }

  void ExpectParentEvents(JobScheduler& scheduler, const JobInCtld& parent) {
    for (auto [dependent, time] : {std::pair{900U, parent.StartTime()},
                                   {901U, parent.EndTime()},
                                   {902U, absl::InfiniteFuture()},
                                   {903U, parent.EndTime()}}) {
      JobScheduler::DependencyEvent event;
      ASSERT_TRUE(scheduler.m_dependency_event_queue_.try_dequeue(event));
      EXPECT_EQ(event.dependent_job_id, dependent);
      EXPECT_EQ(event.dependee_job_id, parent.JobId());
      EXPECT_EQ(event.event_time, time);
    }
    JobScheduler::DependencyEvent event;
    EXPECT_FALSE(scheduler.m_dependency_event_queue_.try_dequeue(event));
  }

  void SetUp() override {
    char path[] = "/tmp/crane-array-requeue-XXXXXX";
    ASSERT_NE(::mkdtemp(path), nullptr);
    db_dir_ = path;
    g_embedded_db_client = MakeEmbeddedDbClient("Unqlite");
    ASSERT_TRUE(g_embedded_db_client->Init((db_dir_ / "jobs").string()));
    g_account_meta_container = std::make_unique<AccountMetaContainer>();
    g_job_scheduler = std::make_unique<JobScheduler>();
    loop_ = uvw::loop::create();
    g_job_scheduler->m_cancel_job_async_handle_ =
        loop_->resource<uvw::async_handle>();
  }
  void TearDown() override {
    if (g_job_scheduler) {
      g_job_scheduler->m_cancel_job_async_handle_->close();
      loop_->run();
    }
    g_job_scheduler.reset();
    g_account_meta_container.reset();
    g_embedded_db_client.reset();
    if (!db_dir_.empty()) std::filesystem::remove_all(db_dir_);
  }
  std::filesystem::path db_dir_;
  std::shared_ptr<uvw::loop> loop_;
};

#ifdef CRANE_HAVE_UNQLITE
TEST_F(ArrayRequeueCancellationTest, FailedStepCounterResetAllowsLaterCancel) {
  ExerciseFailedRequeue(true);
}
TEST_F(ArrayRequeueCancellationTest,
       FailedPendingWriteFinalizesRecordedDeadline) {
  ExerciseFailedRequeue(false);
}
TEST_F(ArrayRequeueCancellationTest, CancelAfterChildIsRequeued) {
  ExercisePendingChildCancellation();
}
TEST_F(ArrayRequeueCancellationTest, RecordedCancelPrecedesChildCompletion) {
  ExerciseTerminationBeforeChildCompletion(crane::grpc::Cancelled);
}
TEST_F(ArrayRequeueCancellationTest, DeadlinePrecedesChildCompletion) {
  ExerciseTerminationBeforeChildCompletion(crane::grpc::Deadline);
}
TEST_F(ArrayRequeueCancellationTest, ParentWithoutChildrenUsesOneFinishTime) {
  ExerciseParentWithoutChildren();
}
TEST_F(ArrayRequeueCancellationTest,
       ParentDeadlineKeepsResultAfterLaterCancel) {
  ExerciseDeadlineBeforeCancel();
}
#endif
}  // namespace Ctld

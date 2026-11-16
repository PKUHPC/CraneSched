// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include <gtest/gtest.h>

#include <memory>
#include <tuple>
#include <utility>
#include <vector>

#include "JobScheduler.h"

namespace {

constexpr uint64_t kMemPerCpu = 3872ULL * 1024 * 1024;
constexpr uint64_t kJobMemPerNode = 4 * kMemPerCpu;
constexpr char kPartition[] = "step-memory-test";

class StepResourceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    original_partitions_ = std::exchange(g_config.Partitions, {});
    auto& partition = g_config.Partitions[kPartition];
    partition.default_mem_per_cpu = kMemPerCpu;
    partition.max_mem_per_cpu = kMemPerCpu;
    SetJobAllocation(1);
  }

  void TearDown() override {
    g_config.Partitions = std::move(original_partitions_);
  }

  void SetJobAllocation(uint32_t nodes) {
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
    request.set_mem_per_cpu(kMemPerCpu);
    request.mutable_time_limit()->set_seconds(3600);
    job_.SetFieldsByJobToCtld(request);
    job_.SetJobId(42);
    job_.SetReqTotalResView(job_.req_task_res_view * request.ntasks());

    ResourceV3 allocation;
    std::vector<CranedId> craned_ids;
    for (uint32_t i = 0; i < nodes; ++i) {
      auto node = "node" + std::to_string(i);
      craned_ids.push_back(node);
      g_config.Partitions.at(kPartition).nodes.insert(node);
      ResourceInNodeV3 resource;
      resource.GetCpuSet().cpu_count = cpu_t{4};
      resource.GetCpuSet().core_ids = {0, 1, 2, 3};
      resource.SetMemoryBytes(kJobMemPerNode);
      resource.SetMemorySwBytes(kJobMemPerNode);
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
};

class StepInheritedMemoryTest : public StepResourceTest,
                                public ::testing::WithParamInterface<
                                    std::tuple<uint32_t, uint32_t, uint32_t>> {
};

TEST_P(StepInheritedMemoryTest, SchedulesRequestedTasksWithFullNodeMemory) {
  const auto [nodes, ntasks, cpus_per_task] = GetParam();
  SetJobAllocation(nodes);
  auto step = MakeStep(Request(ntasks, nodes, cpus_per_task));
  ASSERT_TRUE(Admit(step.get()).has_value());
  EXPECT_EQ(step->req_node_res_view.GetMemoryBytes(), kJobMemPerNode);
  EXPECT_EQ(step->req_total_res_view.GetMemoryBytes(), nodes * kJobMemPerNode);

  job_.AddStep(std::move(step));
  std::vector<Ctld::CommonStepInCtld*> scheduled;
  ASSERT_EQ(job_.SchedulePendingSteps(&scheduled), 1);
  ASSERT_EQ(scheduled.size(), 1);
  const auto allocated = scheduled.front()->AllocatedRes();
  EXPECT_EQ(allocated.EachNodeResMap().size(), nodes);
  EXPECT_EQ(scheduled.front()->task_res_map.size(), ntasks);
  ResourceView total;
  for (const auto& [node, resource] : allocated.EachNodeResMap()) {
    total += resource;
    EXPECT_EQ(resource.GetMemoryBytes(), kJobMemPerNode);
    EXPECT_EQ(job_.StepResAvail().At(node).GetMemoryBytes(), 0);
  }
  EXPECT_EQ(total.GetCpuCount(), cpu_t{ntasks * cpus_per_task});
}

INSTANTIATE_TEST_SUITE_P(
    SmallAndFullSteps, StepInheritedMemoryTest,
    ::testing::Values(std::tuple{1U, 1U, 1U}, std::tuple{1U, 2U, 1U},
                      std::tuple{1U, 4U, 1U}, std::tuple{1U, 1U, 4U},
                      std::tuple{2U, 2U, 1U}, std::tuple{2U, 4U, 1U},
                      std::tuple{2U, 8U, 1U}));

TEST_F(StepResourceTest, PreservesMemoryPerCpuAbovePartitionLimit) {
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

TEST_F(StepResourceTest, PreservesNodeMemoryAfterPartitionLimitIsLowered) {
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

TEST_F(StepResourceTest, DoesNotReduceTaskCountForPartitionNodeMemoryLimit) {
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

TEST_F(StepResourceTest, RejectsMemoryBeyondParentAllocationWithoutClamping) {
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

TEST_F(StepResourceTest, WaitsForMemoryHeldByAnotherStep) {
  for (step_id_t id : {1U, 2U}) {
    auto step = MakeStep(Request(), id);
    ASSERT_TRUE(Admit(step.get()).has_value());
    job_.AddStep(std::move(step));
  }
  std::vector<Ctld::CommonStepInCtld*> scheduled;
  ASSERT_EQ(job_.SchedulePendingSteps(&scheduled), 1);
  ASSERT_EQ(scheduled.size(), 1);
  EXPECT_EQ(scheduled.front()->StepId(), 1);
  EXPECT_EQ(job_.GetStep(2)->Status(), crane::grpc::Pending);
  EXPECT_EQ(job_.StepResAvail().At("node0").GetCpuSet().cpu_count, cpu_t{3});
  EXPECT_EQ(job_.StepResAvail().At("node0").GetMemoryBytes(), 0);

  auto finished = job_.EraseStep(1);
  scheduled.clear();
  ASSERT_EQ(job_.SchedulePendingSteps(&scheduled), 1);
  ASSERT_EQ(scheduled.size(), 1);
  EXPECT_EQ(scheduled.front()->StepId(), 2);
}

TEST_F(StepResourceTest, RejectsBothMemoryDimensions) {
  auto request = Request();
  request.set_mem_per_cpu(kMemPerCpu);
  auto step = MakeStep(request);
  const auto result = Ctld::JobScheduler::AcquireStepAttributes(step.get());
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), CraneErrCode::ERR_INVALID_RESOURCE);
}

TEST_F(StepResourceTest, JobStillEnforcesPartitionMemoryPerCpu) {
  auto request = job_.JobToCtld();
  request.clear_mem_per_cpu();
  request.set_mem_per_node(kJobMemPerNode);
  request.set_ntasks(1);
  request.set_ntasks_per_node(1);
  Ctld::JobInCtld job;
  job.SetFieldsByJobToCtld(request);
  const auto result = Ctld::JobScheduler::AcquireJobAttributes(&job);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), CraneErrCode::ERR_INVALID_PARAM);
}

}  // namespace

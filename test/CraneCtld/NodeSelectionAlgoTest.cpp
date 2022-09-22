#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>

#include "TaskScheduler.h"

/** Outdated test code.
 *  Keep it for reference.

TEST(NodeSelectionAlgo, MinLoadFirst) {  // NOLINT(cert-err58-cpp)
  using namespace Ctld;

  g_meta_container = std::make_unique<CranedMetaContainerSimpleImpl>();
  const std::string partition_name{"Part1"};

  uint32_t part_id = g_meta_container->AllocPartitionId(partition_name);
  ASSERT_EQ(part_id, 0);

  uint32_t node_index = g_meta_container->AllocNodeIndexInPartition(part_id);
  ASSERT_EQ(node_index, 0);

  AllocatableResource node_resource{};
  node_resource.cpu_count = 16;
  // unlimited memory for convenience
  // Subtract 1000 because we have running tasks.
  node_resource.memory_bytes = std::numeric_limits<uint64_t>::max() - 1000;
  node_resource.memory_sw_bytes = std::numeric_limits<uint64_t>::max() - 1000;

  CranedStaticMeta node_static{.craned_index = node_index,
                               .ipv4_addr = "",
                               .port = 0,
                               .node_name = "0",
                               .partition_id = part_id,
                               .partition_name = partition_name,
                               .res = {.allocatable_resource = node_resource}};
  g_meta_container->CranedUp(node_static);

  AllocatableResource allocatable_resource_r1;
  allocatable_resource_r1.cpu_count = 3;
  Resources resources_r1{allocatable_resource_r1};

  AllocatableResource allocatable_resource_r2;
  allocatable_resource_r2.cpu_count = 3;
  Resources resources_r2{allocatable_resource_r2};

  AllocatableResource allocatable_resource_r3;
  allocatable_resource_r3.cpu_count = 2;
  Resources resources_r3{allocatable_resource_r3};

  g_meta_container->MallocResourceFromNode({part_id, node_index}, 1,
                                           resources_r1);
  g_meta_container->MallocResourceFromNode({part_id, node_index}, 2,
                                           resources_r2);
  g_meta_container->MallocResourceFromNode({part_id, node_index}, 3,
                                           resources_r3);

  auto all_part_meta = g_meta_container->GetAllPartitionsMetaMapPtr();

  absl::flat_hash_map<uint32_t, std::unique_ptr<ITask>> running_tasks;
  absl::btree_map<uint32_t, std::unique_ptr<ITask>> pending_tasks;
  INodeSelectionAlgo::NodeSelectionResult selection_result;

  auto task_r1 = std::make_unique<BatchTask>();
  auto task_r2 = std::make_unique<BatchTask>();
  auto task_r3 = std::make_unique<BatchTask>();

  absl::Time now = absl::FromUnixSeconds(ToUnixSeconds(absl::Now()));
  task_r1->start_time = now - absl::Seconds(100);
  task_r1->time_limit = absl::Seconds(100 + 20);
  task_r1->resources = resources_r1;

  task_r2->start_time = now - absl::Seconds(100);
  task_r2->time_limit = absl::Seconds(100 + 40);
  task_r2->resources = resources_r2;

  task_r3->start_time = now - absl::Seconds(100);
  task_r3->time_limit = absl::Seconds(100 + 40);
  task_r3->resources = resources_r3;

  task_r1->task_id = 1;
  task_r2->task_id = 2;
  task_r3->task_id = 3;
  task_r1->partition_id = part_id;
  task_r2->partition_id = part_id;
  task_r3->partition_id = part_id;
  running_tasks.emplace(1, std::move(task_r1));
  running_tasks.emplace(2, std::move(task_r2));
  running_tasks.emplace(3, std::move(task_r3));

  auto task_p1 = std::make_unique<BatchTask>();
  auto task_p2 = std::make_unique<BatchTask>();
  auto task_p3 = std::make_unique<BatchTask>();
  auto task_p4 = std::make_unique<BatchTask>();
  auto task_p5 = std::make_unique<BatchTask>();
  auto task_p6 = std::make_unique<BatchTask>();
  auto task_p7 = std::make_unique<BatchTask>();
  auto task_p8 = std::make_unique<BatchTask>();

  AllocatableResource allocatable_resource_p1;
  allocatable_resource_p1.cpu_count = 11;
  Resources resources_p1{allocatable_resource_p1};
  task_p1->resources = resources_p1;

  AllocatableResource allocatable_resource_p2;
  allocatable_resource_p2.cpu_count = 3;
  Resources resources_p2{allocatable_resource_p2};
  task_p2->resources = resources_p2;

  AllocatableResource allocatable_resource_p3;
  allocatable_resource_p3.cpu_count = 2;
  Resources resources_p3{allocatable_resource_p3};
  task_p3->resources = resources_p3;

  AllocatableResource allocatable_resource_p4;
  allocatable_resource_p4.cpu_count = 2;
  Resources resources_p4{allocatable_resource_p4};
  task_p4->resources = resources_p4;

  AllocatableResource allocatable_resource_p5;
  allocatable_resource_p5.cpu_count = 3;
  Resources resources_p5{allocatable_resource_p5};
  task_p5->resources = resources_p5;

  AllocatableResource allocatable_resource_p6;
  allocatable_resource_p6.cpu_count = 14;
  Resources resources_p6{allocatable_resource_p6};
  task_p6->resources = resources_p6;

  AllocatableResource allocatable_resource_p7;
  allocatable_resource_p7.cpu_count = 6;
  Resources resources_p7{allocatable_resource_p7};
  task_p7->resources = resources_p7;

  AllocatableResource allocatable_resource_p8;
  allocatable_resource_p8.cpu_count = 2;
  Resources resources_p8{allocatable_resource_p8};
  task_p8->resources = resources_p8;

  task_p1->time_limit = absl::Seconds(29);
  task_p2->time_limit = absl::Seconds(100);
  task_p3->time_limit = absl::Seconds(120);
  task_p4->time_limit = absl::Seconds(23);
  task_p5->time_limit = absl::Seconds(10);
  task_p6->time_limit = absl::Seconds(20);
  task_p7->time_limit = absl::Seconds(9);
  task_p8->time_limit = absl::Seconds(20);

  task_p1->task_id = 11;
  task_p2->task_id = 12;
  task_p3->task_id = 13;
  task_p4->task_id = 14;
  task_p5->task_id = 15;
  task_p6->task_id = 16;
  task_p7->task_id = 17;
  task_p8->task_id = 18;
  task_p1->partition_id = part_id;
  task_p2->partition_id = part_id;
  task_p3->partition_id = part_id;
  task_p4->partition_id = part_id;
  task_p5->partition_id = part_id;
  task_p6->partition_id = part_id;
  task_p7->partition_id = part_id;
  task_p8->partition_id = part_id;
  pending_tasks.emplace(11, std::move(task_p1));
  pending_tasks.emplace(12, std::move(task_p2));
  pending_tasks.emplace(13, std::move(task_p3));
  pending_tasks.emplace(14, std::move(task_p4));
  pending_tasks.emplace(15, std::move(task_p5));
  pending_tasks.emplace(16, std::move(task_p6));
  pending_tasks.emplace(17, std::move(task_p7));
  pending_tasks.emplace(18, std::move(task_p8));

  std::unique_ptr<INodeSelectionAlgo> algo = std::make_unique<MinLoadFirst>();
  std::list<INodeSelectionAlgo::NodeSelectionResult> result;
  algo->NodeSelect(*all_part_meta, running_tasks, &pending_tasks, &result);

  // Two runnable pending task: P5 and P8
  ASSERT_EQ(result.size(), 2);

  auto iter = result.begin();
  ASSERT_EQ(iter->first->task_id, 15);
  ASSERT_EQ(iter->first->start_time - now, absl::Seconds(0));

  std::advance(iter, 1);
  ASSERT_EQ(iter->first->task_id, 18);
  ASSERT_EQ(iter->first->start_time - now, absl::Seconds(0));
}

 */
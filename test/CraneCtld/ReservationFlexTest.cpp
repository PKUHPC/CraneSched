// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include <gtest/gtest.h>

#include "Accounting/AccountMetaContainer.h"
#include "Accounting/LicenseManager.h"
#include "JobScheduler.h"

// Offline placement tests do not initialize database-backed account metadata.
extern "C" std::expected<void, std::string>
__wrap__ZN4Ctld20AccountMetaContainer17CheckMetaResourceB5cxx11ERKNS_16PdJobInSchedulerE(
    Ctld::AccountMetaContainer*, const Ctld::PdJobInScheduler&) {
  return {};
}

namespace {
using namespace Ctld;

absl::Time At(int seconds) { return absl::FromUnixSeconds(seconds); }

ResourceView Cpu(int count) {
  ResourceView result;
  result.SetCpuCount(cpu_t{count});
  return result;
}

void ExpectResources(const ResourceView& actual, const ResourceView& expected) {
  EXPECT_TRUE(actual <= expected);
  EXPECT_TRUE(expected <= actual);
}

TEST(TimeAvailResViewTest, MergesCoincidentChangesAndCopiesIndependently) {
  TimeAvailResMap p{{At(0), Cpu(4)},
                    {At(10), {}},
                    {At(20), Cpu(4)},
                    {absl::InfiniteFuture(), {}}};
  TimeAvailResMap r{{At(0), {}}, {At(10), Cpu(4)}, {At(20), {}}};
  TimeAvailResView view(p, &r);
  auto it = view.begin();
  auto copy = it++;
  EXPECT_EQ(copy->first, At(0));
  EXPECT_EQ(it->first, At(10));
  EXPECT_EQ(view.EstimatedSize(), p.size() + r.size());
  for (; it != view.end(); ++it)
    EXPECT_EQ(it->second.CpuCountDouble(),
              it->first == absl::InfiniteFuture() ? 0 : 4);
}

TEST(TimeAvailResViewTest, MismatchedBreakpointsAndMissingSide) {
  TimeAvailResMap p{{At(0), Cpu(3)}, {At(4), Cpu(1)}, {At(9), Cpu(4)}};
  TimeAvailResMap r{{At(2), Cpu(2)}, {At(6), {}}};
  TimeAvailResView view(p, &r);
  std::vector<int> observed;
  for (auto [_, resource] : view)
    observed.push_back(static_cast<int>(resource.GetCpuCount()));
  EXPECT_EQ(observed, (std::vector<int>{3, 5, 3, 1, 4}));
  EXPECT_EQ(view.EstimatedSize(), p.size() + r.size());
}

class ReservationFlexTest : public ::testing::Test {
 protected:
  void SetUp() override {
    saved_config_ = std::move(g_config);
    saved_nodes_ = std::move(g_meta_container);
    saved_licenses_ = std::move(g_license_manager);
    saved_account_meta_ = std::move(g_account_meta_container);
    g_config = Config{};
    g_config.ScheduledBatchSize = 100;
    g_config.DefaultPartition = "batch";
    for (const auto& id : {"n1", "n2"}) {
      auto node = std::make_shared<Config::Node>();
      node->cpu = 4;
      node->memory_bytes = 8192;
      g_config.Nodes.emplace(id, node);
      g_config.NodeOrder.push_back(id);
      g_config.CranedIdByAlias.emplace(id, id);
      g_config.Partitions["batch"].nodes.emplace(id);
      g_config.Partitions["shared"].nodes.emplace(id);
    }
    g_config.PartitionOrder = {"batch", "shared"};
    g_meta_container = std::make_unique<CranedMetaContainer>();
    g_meta_container->InitFromConfig(g_config);
    for (const auto& id : {"n1", "n2"}) g_meta_container->CranedUp(id, {});
    g_license_manager = std::make_unique<LicenseManager>();
    g_account_meta_container = std::make_unique<AccountMetaContainer>();
  }

  void TearDown() override {
    g_meta_container = std::move(saved_nodes_);
    g_license_manager = std::move(saved_licenses_);
    g_account_meta_container = std::move(saved_account_meta_);
    g_config = std::move(saved_config_);
  }

  void Reserve(std::string name, int start, int end, bool flex = true,
               std::string node = "n1") {
    ResvMeta meta;
    meta.name = name;
    meta.start_time = At(start);
    meta.end_time = At(end);
    meta.flex = flex;
    meta.craned_ids.insert(node);
    {
      auto n = g_meta_container->GetCranedMetaPtr(node);
      meta.res_total.AddResourceInNode(node, n->res_total);
      n->resv_in_node_map.emplace(name, std::make_pair(At(start), At(end)));
    }
    meta.res_avail = meta.res_total;
    g_meta_container->GetResvMetaMapExclusivePtr()->emplace(name,
                                                            std::move(meta));
  }

  PdJobInScheduler& Pending(std::string reservation, int duration,
                            std::string node = "n1", int cpus = 4,
                            int nodes = 1, std::string partition = "batch") {
    JobInCtld source;
    source.SetJobId(next_id_++);
    source.partition_id = partition;
    source.reservation = reservation;
    source.time_limit = absl::Seconds(duration);
    source.node_num = nodes;
    source.ntasks = nodes;
    source.ntasks_per_node_min = 1;
    source.ntasks_per_node_max = 1;
    source.exclusive = false;
    source.req_task_res_view = Cpu(cpus);
    source.req_total_res_view = Cpu(cpus * nodes);
    if (!node.empty()) source.included_nodes.insert(node);
    pending_.push_back(std::make_unique<PdJobInScheduler>(&source));
    return *pending_.back();
  }

  ResourceV3 Allocation(std::string node = "n1") {
    ResourceV3 allocation;
    allocation.AddResourceInNode(
        node, g_meta_container->GetCranedMetaPtr(node)->res_total);
    return allocation;
  }

  void Running(int end, std::string reservation = "", std::string node = "n1") {
    JobInCtld source;
    source.SetJobId(next_id_++);
    source.partition_id = "batch";
    source.reservation = reservation;
    source.SetStartTime(At(90));
    source.SetEndTime(At(end));
    source.SetAllocatedRes(Allocation(node));
    running_.push_back(std::make_unique<RnJobInScheduler>(&source));
  }

  void Schedule(int now = 100) {
    BasicPriority priority;
    SchedulerAlgo algo(&priority);
    algo.NodeSelect(At(now), running_, pending_);
  }

  Config saved_config_;
  std::unique_ptr<CranedMetaContainer> saved_nodes_;
  std::unique_ptr<LicenseManager> saved_licenses_;
  std::unique_ptr<AccountMetaContainer> saved_account_meta_;
  std::vector<std::unique_ptr<PdJobInScheduler>> pending_;
  std::vector<std::unique_ptr<RnJobInScheduler>> running_;
  job_id_t next_id_{1};
};

TEST_F(ReservationFlexTest, StartsEarlyAndCrossesBothBoundaries) {
  Reserve("r", 110, 120);
  auto& flex = Pending("r", 40);
  auto& normal = Pending("", 10);
  Schedule();
  EXPECT_TRUE(flex.reason.empty());
  EXPECT_EQ(flex.start_time, At(100));
  EXPECT_EQ(flex.end_time, At(140));
  EXPECT_EQ(normal.start_time, At(140));
}

TEST_F(ReservationFlexTest, OrdinaryReservationRetainsWindowRestriction) {
  Reserve("r", 90, 120, false);
  auto& too_long = Pending("r", 21);
  auto& fits = Pending("r", 20);
  Schedule();
  EXPECT_FALSE(too_long.reason.empty());
  EXPECT_TRUE(fits.reason.empty());
  EXPECT_EQ(fits.end_time, At(120));
}

TEST_F(ReservationFlexTest, ProtectsOtherReservationAndAdmissionCutoff) {
  Reserve("r", 90, 120);
  Reserve("other", 130, 160);
  auto& long_job = Pending("r", 31);
  auto& fits = Pending("r", 30);
  Schedule();
  EXPECT_FALSE(long_job.reason.empty());
  EXPECT_EQ(long_job.reason, "Resource");
  EXPECT_TRUE(fits.reason.empty());
  EXPECT_EQ(fits.end_time, At(130));
}

TEST_F(ReservationFlexTest, MixedNodesCanExceedReservationSize) {
  Reserve("r", 90, 120);
  auto& flex = Pending("r", 40, "", 4, 2);
  auto& normal = Pending("", 10, "n2");
  Schedule();
  EXPECT_TRUE(flex.reason.empty());
  EXPECT_EQ(flex.allocated_res.EachNodeResMap().size(), 2);
  EXPECT_EQ(normal.start_time, At(140));
}

TEST_F(ReservationFlexTest, AllOutsideAllocationAndSharedPartitions) {
  Reserve("r", 90, 120);
  auto& first = Pending("r", 40, "n2");
  auto& second = Pending("", 10, "n2", 4, 1, "shared");
  Schedule();
  EXPECT_TRUE(first.reason.empty());
  EXPECT_EQ(first.craned_ids, (std::vector<CranedId>{"n2"}));
  EXPECT_EQ(second.start_time, At(140));
}

TEST_F(ReservationFlexTest, FutureShadowProtectsTailAndPermitsShortBackfill) {
  Reserve("r", 110, 120);
  Running(105);
  auto& high = Pending("r", 30, "", 4, 2);
  auto& short_job = Pending("", 5, "n2");
  auto& long_job = Pending("", 20, "n2");
  Schedule();
  EXPECT_EQ(high.start_time, At(105));
  EXPECT_TRUE(short_job.reason.empty());
  EXPECT_EQ(short_job.end_time, At(105));
  EXPECT_EQ(long_job.start_time, At(135));
}

TEST_F(ReservationFlexTest, ExpiredReservationKeepsRunningOccupancy) {
  Reserve("r", 80, 100);
  Running(140, "r");
  auto& expired = Pending("r", 10, "n2");
  auto& normal = Pending("", 10);
  auto& outside = Pending("", 10, "n2");
  Schedule();
  EXPECT_EQ(expired.reason, "Reservation Expired");
  EXPECT_EQ(normal.start_time, At(140));
  EXPECT_TRUE(outside.reason.empty());
}

TEST_F(ReservationFlexTest, DelayedCleanupOccupiesNextReservation) {
  Reserve("old", 80, 100);
  Reserve("next", 100, 120, false);
  Running(99, "old");
  auto& next = Pending("next", 10);
  Schedule();
  EXPECT_EQ(next.start_time, At(101));
}

TEST_F(ReservationFlexTest, SeparateFlexReservationsShareOutsideCapacity) {
  Reserve("a", 90, 120);
  Reserve("b", 130, 160);
  auto& first = Pending("a", 20, "n2", 2);
  auto& second = Pending("b", 20, "n2", 2);
  auto& ordinary = Pending("", 10, "n2", 1);
  Schedule();
  EXPECT_TRUE(first.reason.empty());
  EXPECT_TRUE(second.reason.empty());
  EXPECT_EQ(ordinary.start_time, At(120));
  const auto& first_cores =
      first.allocated_res.EachNodeResMap().at("n2").GetCpuSet().core_ids;
  const auto& second_cores =
      second.allocated_res.EachNodeResMap().at("n2").GetCpuSet().core_ids;
  for (auto core : first_cores) EXPECT_FALSE(second_cores.contains(core));
}

TEST_F(ReservationFlexTest,
       ReservationAccountingUsesScheduledResourcesAndRetainsEmptyReference) {
  Reserve("r", 90, 120);
  auto& outside = Pending("r", 10, "n2");
  Schedule();
  g_meta_container->MallocResourceFromResv("r", 1, outside.resv_allocated_res);
  {
    auto meta = g_meta_container->GetResvMetaPtr("r");
    EXPECT_TRUE(meta->rn_job_res_map.contains(1));
    EXPECT_TRUE(meta->rn_job_res_map.at(1).EachNodeResMap().empty());
    ExpectResources(meta->res_avail.View(), meta->res_total.View());
  }
  pending_.clear();
  auto& mixed = Pending("r", 10, "", 4, 2);
  Schedule();
  ASSERT_TRUE(mixed.reason.empty());
  g_meta_container->MallocResourceFromResv("r", 2, mixed.resv_allocated_res);
  {
    auto meta = g_meta_container->GetResvMetaPtr("r");
    EXPECT_EQ(meta->res_avail.View().CpuCountDouble(), 0);
    EXPECT_EQ(meta->rn_job_res_map.at(2).EachNodeResMap().size(), 1);
  }
  g_meta_container->FreeResourceFromResv("r", 1);
  g_meta_container->FreeResourceFromResv("r", 2);
  auto meta = g_meta_container->GetResvMetaPtr("r");
  EXPECT_TRUE(meta->rn_job_res_map.empty());
  ExpectResources(meta->res_avail.View(), meta->res_total.View());
}

TEST_F(ReservationFlexTest,
       TimeExtensionAndResumeRespectActualNodeReservations) {
  Reserve("r", 90, 120);
  Reserve("other", 140, 160, false, "n2");
  auto outside = Allocation("n2");
  EXPECT_EQ(
      g_meta_container->CheckReservationEndTime("r", outside, At(110), At(140))
          .code(),
      CraneErrCode::SUCCESS);
  EXPECT_NE(
      g_meta_container->CheckReservationEndTime("r", outside, At(110), At(141))
          .code(),
      CraneErrCode::SUCCESS);
  EXPECT_EQ(g_meta_container
                ->CheckReservationEndTime("r", Allocation(), At(110), At(200))
                .code(),
            CraneErrCode::SUCCESS);
  EXPECT_NE(
      g_meta_container->CheckReservationEndTime("", outside, At(110), At(141))
          .code(),
      CraneErrCode::SUCCESS);
  EXPECT_EQ(g_meta_container
                ->CheckReservationEndTime("missing", outside, At(110), At(120))
                .code(),
            CraneErrCode::SUCCESS);
  g_meta_container->GetResvMetaPtr("r")->flex = false;
  EXPECT_NE(g_meta_container
                ->CheckReservationEndTime("r", Allocation(), At(110), At(121))
                .code(),
            CraneErrCode::SUCCESS);
}

TEST_F(ReservationFlexTest, RestoreExpiredFlexBeforeAllocatedJobReferences) {
  crane::grpc::CreateReservationRequest stored;
  stored.set_reservation_name("expired");
  stored.set_start_time_unix_seconds(80);
  stored.set_duration_seconds(20);
  stored.set_craned_regex("n1");
  stored.set_partition("batch");
  stored.set_creation_order(7);
  stored.set_flex(true);
  stored.add_allowed_accounts("students");
  crane::grpc::CreateReservationRequest recovered;
  ASSERT_TRUE(recovered.ParseFromString(stored.SerializeAsString()));
  ASSERT_TRUE(g_meta_container->RestoreReservation(recovered));
  g_meta_container->MallocResourceFromResv("expired", 42, {});
  {
    auto meta = g_meta_container->GetResvMetaPtr("expired");
    EXPECT_TRUE(meta->flex);
    EXPECT_EQ(meta->creation_order, 7);
    EXPECT_EQ(meta->end_time, At(100));
    EXPECT_TRUE(meta->accounts.contains("students"));
    EXPECT_FALSE(meta->accounts_black_list);
    EXPECT_TRUE(meta->rn_job_res_map.contains(42));
  }
  Running(140, "expired", "n2");
  auto& pending = Pending("expired", 10);
  auto& ordinary = Pending("", 10, "n2");
  Schedule();
  EXPECT_EQ(pending.reason, "Reservation Expired");
  EXPECT_EQ(ordinary.start_time, At(140));
}

TEST_F(ReservationFlexTest, RestoresLegacyNonFlexAndShrinksMissingNodes) {
  crane::grpc::CreateReservationRequest stored;
  stored.set_reservation_name("legacy");
  stored.set_start_time_unix_seconds(90);
  stored.set_duration_seconds(30);
  stored.set_craned_regex("n1");
  ASSERT_TRUE(g_meta_container->RestoreReservation(stored));
  EXPECT_FALSE(g_meta_container->GetResvMetaPtr("legacy")->flex);
  stored.set_reservation_name("shrunk");
  stored.set_craned_regex("n[2-3]");
  ASSERT_TRUE(g_meta_container->RestoreReservation(stored));
  EXPECT_EQ(g_meta_container->GetResvMetaPtr("shrunk")->craned_ids,
            (absl::flat_hash_set<CranedId>{"n2"}));
  EXPECT_TRUE(
      g_meta_container->GetCranedMetaPtr("n2")->resv_in_node_map.contains(
          "shrunk"));
}

TEST_F(ReservationFlexTest, BackfillMatchesPerSecondFeasibility) {
  Reserve("r", 110, 125);
  Reserve("other", 140, 150);
  Running(108);
  for (int duration = 1; duration <= 40; ++duration) {
    pending_.clear();
    auto& job = Pending("r", duration);
    Schedule();
    int expected = -1;
    for (int start = 100; start < 125 && expected < 0; ++start) {
      bool feasible = true;
      for (int t = start; t < start + duration; ++t)
        if (t < 108 || (t >= 140 && t < 150)) feasible = false;
      if (feasible) expected = start;
    }
    if (expected < 0)
      EXPECT_FALSE(job.reason.empty()) << duration;
    else
      EXPECT_EQ(job.start_time, At(expected)) << duration;
  }
}
}  // namespace

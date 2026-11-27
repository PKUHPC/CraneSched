// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <string>
#include <vector>

#include "CtldPublicDefs.h"
#include "Node/CranedMetaContainer.h"
#include "RpcService/CranedKeeper.h"

namespace {

constexpr uint64_t kGiB = 1024ULL * 1024 * 1024;

// One static node whose NodeName, NodeHostname and NodeAddr all differ, and
// the configured FUTURE nodes f1, f2 (4 CPUs) and f3 (8 CPUs, bigmem).
class FutureNodeTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    // Node lifecycle changes only take the keeper's lifecycle lock here.
    g_config.CraneCtldDebugLevel = "off";
    g_craned_keeper = std::make_unique<Ctld::CranedKeeper>(0);
  }
  static void TearDownTestSuite() { g_craned_keeper.reset(); }

  void SetUp() override {
    original_config_ = std::move(g_config);
    g_config = Ctld::Config{};
    db_dir_ = std::filesystem::temp_directory_path() /
              fmt::format("crane_future_node_test_{}", getpid());
    std::filesystem::create_directories(db_dir_);
    g_config.CraneCtldDbPath = db_dir_ / "cranectld.db";

    AddConfiguredNode("s1", 4, false);
    g_config.Nodes.at("s1")->node_hostname = "s1-host";
    g_config.Nodes.at("s1")->node_addr = "10.0.0.1";
    AddConfiguredNode("f1", 4, true);
    AddConfiguredNode("f2", 4, true);
    AddConfiguredNode("f3", 8, true);
    g_config.Nodes.at("f3")->features = {"bigmem"};
    g_config.PartitionOrder = {"batch"};
    RestartCtld();
  }

  void TearDown() override {
    g_meta_container.reset();
    std::filesystem::remove_all(db_dir_);
    g_config = std::move(original_config_);
  }

  static void AddConfiguredNode(const std::string& name, uint32_t cpu,
                                bool future) {
    auto node = std::make_shared<Ctld::Config::Node>();
    node->node_hostname = name;
    node->node_addr = name;
    node->cpu = cpu;
    node->memory_bytes = 8 * kGiB;
    node->node_topo_info.sockets = 2;
    node->flags[Ctld::NodeFlags::Future] = future;
    g_config.Nodes.emplace(name, std::move(node));
    g_config.NodeOrder.emplace_back(name);
    g_config.Partitions["batch"].nodes.emplace(name);
  }

  // Aliases of mapped FUTURE nodes are rebuilt from the persisted node state.
  static void RestartCtld() {
    g_config.CranedIdByAlias = {{"s1-host", "s1"}};
    for (const auto& name : g_config.NodeOrder)
      g_config.CranedIdByAlias.emplace(name, name);
    g_meta_container = std::make_unique<Ctld::CranedMetaContainer>();
    g_meta_container->InitFromConfig(g_config);
  }

  static std::expected<CranedId, std::string> Match(
      const std::string& hostname, const std::string& addr, uint32_t cpu = 4,
      const std::string& feature = "") {
    crane::grpc::FutureNodeCandidate machine;
    machine.set_hostname(hostname);
    machine.set_cpu(cpu);
    machine.set_memory_bytes(8 * kGiB);
    machine.set_feature(feature);
    return g_meta_container->MatchFutureNode(machine, addr);
  }

  static crane::grpc::CranedInfo ReadNode(const std::string& name) {
    crane::grpc::QueryClusterInfoRequest request;
    request.set_include_nodes(true);
    request.add_filter_nodes(name);
    auto reply = g_meta_container->QueryClusterInfo(request);
    return reply.nodes_size() == 1 ? reply.nodes(0) : crane::grpc::CranedInfo{};
  }

  static crane::grpc::ModifyCranedStateReply ResetToFuture(
      const std::vector<std::string>& names) {
    crane::grpc::ModifyCranedStateRequest request;
    request.set_new_state(crane::grpc::CRANE_RESET_TO_FUTURE);
    for (const auto& name : names) request.add_craned_ids(name);
    return g_meta_container->ChangeNodeState(request);
  }

  Ctld::Config original_config_;
  std::filesystem::path db_dir_;
};

TEST_F(FutureNodeTest, MachineIsMatchedByResourcesFeatureAndHostname) {
  EXPECT_EQ(ReadNode("f1").resource_state(), crane::grpc::CRANE_FUTURE);
  EXPECT_FALSE(Match("small", "10.0.1.9", 2));
  EXPECT_FALSE(Match("gpu01", "10.0.1.9", 8, "gpu"));
  // The feature is matched case-insensitively and outranks the name order.
  EXPECT_EQ(Match("big01", "10.0.1.3", 8, "BigMem"), "f3");
  // A hostname that is a FUTURE NodeName can only be mapped to that node,
  // even if another matching node sorts first.
  EXPECT_EQ(Match("f2", "10.0.1.2"), "f2");
  EXPECT_EQ(Match("worker01", "10.0.1.1"), "f1");
  EXPECT_FALSE(Match("worker02", "10.0.1.4"));

  // The runtime hostname becomes an alias of the logical node.
  auto node = ReadNode("worker01");
  EXPECT_EQ(node.hostname(), "f1");
  EXPECT_EQ(node.node_hostname(), "worker01");
  EXPECT_EQ(node.node_addr(), "10.0.1.1");
  EXPECT_NE(node.resource_state(), crane::grpc::CRANE_FUTURE);

  // A restarted craned keeps its node; only the address is refreshed.
  EXPECT_EQ(Match("worker01", "10.0.1.5"), "f1");
  EXPECT_EQ(ReadNode("f1").node_addr(), "10.0.1.5");
  EXPECT_FALSE(Match("worker01", "10.0.1.5", 2));
}

TEST_F(FutureNodeTest, StepCarriesEndpointsOfStaticAndFutureNodes) {
  ASSERT_EQ(Match("worker01", "10.0.1.1"), "f1");
  const std::vector<CranedId> craned_ids{"s1", "f1"};
  Ctld::JobInCtld job;
  job.SetJobId(1);
  job.type = crane::grpc::Batch;
  job.SetCranedIds(std::vector<CranedId>{craned_ids});
  job.executing_craned_ids = {craned_ids.begin(), craned_ids.end()};
  ResourceV3 alloc_res;
  for (const auto& craned_id : craned_ids)
    alloc_res.AddResourceInNode(craned_id, ResourceInNodeV3{});
  job.SetAllocatedRes(std::move(alloc_res));

  // A FUTURE craned has no local node configuration, so the daemon step must
  // carry the endpoint of every allocated node.
  Ctld::DaemonStepInCtld daemon_step;
  daemon_step.InitFromJob(job);
  const auto step_to_d = daemon_step.GetStepToD("f1");
  ASSERT_EQ(step_to_d.node_endpoints_size(), 2);
  EXPECT_EQ(step_to_d.node_endpoints().at("s1").hostname(), "s1-host");
  EXPECT_EQ(step_to_d.node_endpoints().at("s1").addr(), "10.0.0.1");
  EXPECT_EQ(step_to_d.node_endpoints().at("f1").hostname(), "worker01");
  EXPECT_EQ(step_to_d.node_endpoints().at("f1").addr(), "10.0.1.1");
}

TEST_F(FutureNodeTest, ResetReleasesBindingForAnotherMachine) {
  ASSERT_EQ(Match("worker01", "10.0.1.1"), "f1");
  g_meta_container->CranedUp("f1", {});
  auto reply = ResetToFuture({"f1", "s1"});
  EXPECT_TRUE(reply.modified_nodes().empty());
  ASSERT_EQ(reply.not_modified_reasons_size(), 2);
  EXPECT_EQ(reply.not_modified_reasons(0),
            "Node is online. Stop its craned first.");
  EXPECT_EQ(reply.not_modified_reasons(1),
            "Only FUTURE nodes can have their machine binding reset.");

  g_meta_container->CranedDown("f1");
  reply = ResetToFuture({"f1"});
  ASSERT_EQ(reply.modified_nodes_size(), 1);
  EXPECT_EQ(ReadNode("f1").resource_state(), crane::grpc::CRANE_FUTURE);
  EXPECT_EQ(ReadNode("f1").node_hostname(), "f1");
  EXPECT_EQ(Ctld::ResolveCranedIdAlias("worker01"), "worker01");
  // The old machine no longer owns f1, so another one can take it.
  EXPECT_EQ(Match("worker02", "10.0.1.2"), "f1");
}

TEST_F(FutureNodeTest, CreatedNodesAndBindingsSurviveRestart) {
  crane::grpc::CreateNodesRequest request;
  auto add = [&](const std::string& name, uint32_t cpu,
                 const std::string& partition) {
    auto* definition = request.add_nodes();
    definition->set_name(name);
    definition->set_cpu(cpu);
    definition->set_memory_bytes(8 * kGiB);
    definition->set_sockets(2);
    definition->add_partitions(partition);
  };
  add("d1", 4, "batch");
  add("d1", 4, "batch");
  add("s1-host", 4, "batch");
  add("d2", 3, "batch");
  add("d3", 4, "missing");
  const auto reply = g_meta_container->CreateNodes(request);
  ASSERT_EQ(reply.created_nodes_size(), 1);
  EXPECT_EQ(reply.created_nodes(0), "d1");
  EXPECT_EQ(std::vector<std::string>(reply.not_created_reasons().begin(),
                                     reply.not_created_reasons().end()),
            (std::vector<std::string>{
                "Duplicate node name in request.",
                "Node name or alias already exists.",
                "CPU and memory must be positive; sockets must evenly divide "
                "CPUs.",
                "Unknown partition missing."}));

  // Both a created node and a configured FUTURE node are mapped.
  ASSERT_EQ(Match("worker01", "10.0.1.1"), "d1");
  ASSERT_EQ(Match("worker02", "10.0.1.2"), "f1");
  RestartCtld();

  crane::grpc::QueryClusterInfoRequest query;
  query.set_include_partitions(true);
  const auto partition =
      g_meta_container->QueryClusterInfo(query).partitions(0);
  EXPECT_EQ(partition.total_nodes(), 5);
  EXPECT_EQ(partition.hostlist(), "d1,f[1-3],s1");
  EXPECT_EQ(ReadNode("worker01").hostname(), "d1");
  EXPECT_EQ(ReadNode("worker01").node_addr(), "10.0.1.1");
  EXPECT_EQ(ReadNode("worker02").hostname(), "f1");
  EXPECT_EQ(ReadNode("f2").resource_state(), crane::grpc::CRANE_FUTURE);
  // The restored binding is reused instead of claiming another node.
  EXPECT_EQ(Match("worker01", "10.0.1.1"), "d1");
}

}  // namespace

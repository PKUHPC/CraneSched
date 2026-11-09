// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include <gtest/gtest.h>

#include <list>
#include <set>
#include <string>

// Test the production helpers without exporting them or replacing dependencies.
#include "RpcService/CtldGrpcServer.cpp"
#include "google/protobuf/util/message_differencer.h"
#include "protos/Crane.pb.h"

namespace {
using crane::grpc::JobInfo;
using crane::grpc::QUERY_VISIBILITY_DETAILS;
using crane::grpc::QUERY_VISIBILITY_PUBLIC;
using Ctld::BuildJobQueryReply;
using Ctld::ProjectJobForQuery;
constexpr char kPrivate[] = "private-query-test-marker";

void AddUnknown(google::protobuf::Message* message) {
  message->GetReflection()->MutableUnknownFields(message)->AddLengthDelimited(
      12345, kPrivate);
}

JobInfo MakeJob(uint32_t id = 71, uint32_t owner = 1001,
                uint32_t priority = 9) {
  JobInfo job;
  job.set_job_id(id);
  job.set_uid(owner);
  job.set_gid(100);
  job.set_name("public-job");
  job.set_type(crane::grpc::Container);
  job.set_username("alice");
  job.set_account("shared-account");
  job.set_partition("cpu");
  job.set_qos("normal");
  job.set_reservation("resv");
  job.set_wckey("project");
  job.set_node_num(2);
  job.set_ntasks(8);
  job.set_status(crane::grpc::Running);
  job.set_priority(priority);
  job.set_held(true);
  job.set_exit_code(3);
  job.set_requeue_count(2);
  job.set_requeue_if_failed(true);
  job.set_exclusive(true);
  job.set_submit_hostname("login");
  job.add_req_nodes("node1");
  job.add_exclude_nodes("node2");
  job.add_execution_node("node1");
  job.set_craned_list("node1");
  (*job.mutable_licenses_count())["test-license"] = 2;
  job.mutable_time_limit()->set_seconds(3600);
  job.mutable_start_time()->set_seconds(100);
  job.mutable_end_time()->set_seconds(200);
  job.mutable_submit_time()->set_seconds(50);
  job.mutable_elapsed_time()->set_seconds(100);
  job.mutable_deadline_time()->set_seconds(300);
  auto* res = job.mutable_req_total_res_view();
  res->set_cpu_count(8);
  res->set_memory_bytes(1024);
  res->set_memory_sw_bytes(2048);
  auto& gres = (*res->mutable_gres_map()->mutable_name_gres_map())["gpu"];
  gres.set_total(2);
  (*gres.mutable_specified())["test-gpu"] = 2;
  *job.mutable_allocated_res_view() = *res;
  job.mutable_array_spec()->set_start(1);
  job.mutable_array_spec()->set_end(10);
  job.mutable_array_spec()->set_stride(2);
  job.mutable_array_spec()->set_max_concurrent(3);
  job.mutable_array_task()->set_array_job_id(70);
  job.mutable_array_task()->set_task_id(1);
  auto* dependency = job.mutable_dependency_status();
  dependency->set_is_or(true);
  dependency->mutable_ready_time()->set_seconds(500);
  auto* condition = dependency->add_pending();
  condition->set_job_id(69);
  condition->set_type(crane::grpc::AFTER_OK);
  condition->set_delay_seconds(60);
  job.set_cmd_line(kPrivate);
  job.set_cwd(kPrivate);
  job.set_extra_attr(kPrivate);
  (*job.mutable_env())[kPrivate] = kPrivate;
  job.mutable_pod_meta()->set_name(kPrivate);

  auto* step = job.add_step_info_list();
  step->set_job_id(job.job_id());
  step->set_step_id(1);
  step->set_uid(job.uid());
  step->add_gid(100);
  step->set_type(crane::grpc::Container);
  step->set_name("public-step");
  step->set_status(crane::grpc::Running);
  step->set_node_num(2);
  step->set_ntasks(8);
  step->set_held(true);
  step->set_exit_code(3);
  step->set_craned_list("node1");
  step->add_execution_node("node1");
  step->add_req_nodes("node1");
  step->add_exclude_nodes("node2");
  *step->mutable_req_total_res_view() = *res;
  *step->mutable_allocated_res_view() = *res;
  *step->mutable_time_limit() = job.time_limit();
  *step->mutable_start_time() = job.start_time();
  *step->mutable_end_time() = job.end_time();
  *step->mutable_submit_time() = job.submit_time();
  *step->mutable_elapsed_time() = job.elapsed_time();
  *step->mutable_deadline_time() = job.deadline_time();
  step->set_cmd_line(kPrivate);
  step->set_cwd(kPrivate);
  step->set_extra_attr(kPrivate);
  auto* container = step->mutable_container_meta();
  container->set_command(kPrivate);
  container->add_args(kPrivate);
  container->set_workdir(kPrivate);
  (*container->mutable_env())[kPrivate] = kPrivate;
  (*container->mutable_mounts())[kPrivate] = kPrivate;
  (*container->mutable_labels())[kPrivate] = kPrivate;
  (*container->mutable_annotations())[kPrivate] = kPrivate;
  container->mutable_image()->set_image("test-image");
  container->mutable_image()->set_username(kPrivate);
  container->mutable_image()->set_password(kPrivate);
  return job;
}

TEST(JobQuery, PublicViewPreservesEveryKnownSummaryField) {
  const auto source = MakeJob();
  auto expected = source;
  expected.clear_cmd_line();
  expected.clear_cwd();
  expected.clear_env();
  expected.clear_extra_attr();
  expected.clear_pod_meta();
  expected.set_query_visibility(QUERY_VISIBILITY_PUBLIC);
  for (auto& step : *expected.mutable_step_info_list()) {
    step.clear_cmd_line();
    step.clear_cwd();
    step.clear_extra_attr();
    step.mutable_container_meta()->Clear();
    step.set_query_visibility(QUERY_VISIBILITY_PUBLIC);
  }
  const auto result = ProjectJobForQuery(source, 1002, false);
  EXPECT_TRUE(
      google::protobuf::util::MessageDifferencer::Equals(expected, result));
  EXPECT_EQ(result.SerializeAsString().find(kPrivate), std::string::npos);
}

TEST(JobQuery, OwnerAndAdministratorRetainDetailsExceptPassword) {
  const auto source = MakeJob();
  auto expected = source;
  expected.set_query_visibility(QUERY_VISIBILITY_DETAILS);
  expected.mutable_step_info_list(0)->set_query_visibility(
      QUERY_VISIBILITY_DETAILS);
  expected.mutable_step_info_list(0)
      ->mutable_container_meta()
      ->mutable_image()
      ->clear_password();
  for (const auto uid : {1001U, 0U, 2000U}) {
    const auto result = ProjectJobForQuery(source, uid, uid != 1001);
    EXPECT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(expected, result));
  }
  EXPECT_EQ(source.step_info_list(0).container_meta().image().password(),
            kPrivate);
}

TEST(JobQuery, UnauthenticatedClaimsCannotReadDetailsEvenForRoot) {
  auto source = MakeJob();
  source.set_uid(0);
  for (const bool claimed_admin : {false, true}) {
    const auto result = ProjectJobForQuery(source, std::nullopt, claimed_admin);
    EXPECT_EQ(result.query_visibility(), QUERY_VISIBILITY_PUBLIC);
    EXPECT_EQ(result.SerializeAsString().find(kPrivate), std::string::npos);
  }
}

TEST(JobQuery, StepsRequireParentAndStepOwnership) {
  auto source = MakeJob();
  source.mutable_step_info_list(0)->set_uid(1002);
  auto result = ProjectJobForQuery(source, 1001, false);
  EXPECT_EQ(result.query_visibility(), QUERY_VISIBILITY_DETAILS);
  EXPECT_EQ(result.step_info_list(0).query_visibility(),
            QUERY_VISIBILITY_PUBLIC);
  EXPECT_TRUE(result.step_info_list(0).cwd().empty());
  result = ProjectJobForQuery(source, 1002, false);
  EXPECT_EQ(result.query_visibility(), QUERY_VISIBILITY_PUBLIC);
  EXPECT_EQ(result.step_info_list(0).query_visibility(),
            QUERY_VISIBILITY_PUBLIC);
  result = ProjectJobForQuery(source, 0, true);
  EXPECT_EQ(result.step_info_list(0).query_visibility(),
            QUERY_VISIBILITY_DETAILS);
  EXPECT_TRUE(
      result.step_info_list(0).container_meta().image().password().empty());
}

TEST(JobQuery, UnknownNestedFieldsCannotLeakOrAlterSource) {
  auto source = MakeJob();
  AddUnknown(&source);
  AddUnknown(source.mutable_start_time());
  AddUnknown(source.mutable_req_total_res_view());
  AddUnknown(source.mutable_req_total_res_view()->mutable_gres_map());
  AddUnknown(&(*source.mutable_req_total_res_view()
                    ->mutable_gres_map()
                    ->mutable_name_gres_map())["gpu"]);
  AddUnknown(source.mutable_array_spec());
  AddUnknown(source.mutable_array_task());
  AddUnknown(source.mutable_dependency_status()->mutable_pending(0));
  AddUnknown(source.mutable_dependency_status()->mutable_ready_time());
  AddUnknown(source.mutable_step_info_list(0));
  AddUnknown(source.mutable_step_info_list(0)->mutable_container_meta());
  const auto before = source;
  const auto public_view = ProjectJobForQuery(source, 1002, false);
  EXPECT_EQ(public_view.SerializeAsString().find(kPrivate), std::string::npos);
  const auto details = ProjectJobForQuery(source, 1001, false);
  EXPECT_EQ(details.cwd(), kPrivate);
  EXPECT_TRUE(
      google::protobuf::util::MessageDifferencer::Equals(before, source));
}

TEST(JobQuery, PreservesOneofAndOptionalPresence) {
  auto source = MakeJob();
  source.set_pending_reason("Resources");
  source.mutable_dependency_status()->set_infinite_future(false);
  source.mutable_array_spec()->clear_stride();
  source.mutable_array_spec()->clear_max_concurrent();
  auto result = ProjectJobForQuery(source, 1002, false);
  EXPECT_TRUE(result.has_pending_reason());
  EXPECT_FALSE(result.has_craned_list());
  EXPECT_TRUE(result.dependency_status().has_infinite_future());
  EXPECT_FALSE(result.dependency_status().infinite_future());
  EXPECT_FALSE(result.array_spec().has_stride());
  EXPECT_FALSE(result.array_spec().has_max_concurrent());
  source.mutable_dependency_status()->set_infinite_past(true);
  result = ProjectJobForQuery(source, 1002, false);
  EXPECT_TRUE(result.dependency_status().has_infinite_past());
}

TEST(JobQuery, ContainerKindSurvivesRedactionWithoutIncludingHostScripts) {
  for (const auto kind : {crane::grpc::PRIMARY, crane::grpc::COMMON}) {
    for (const bool has_container : {false, true}) {
      auto source = MakeJob();
      auto* step = source.mutable_step_info_list(0);
      step->set_step_type(kind);
      if (!has_container) step->clear_container_meta();
      for (const auto uid : {1001U, 1002U}) {
        const auto result = ProjectJobForQuery(source, uid, false);
        JobInfo parsed;
        ASSERT_TRUE(parsed.ParseFromString(result.SerializeAsString()));
        EXPECT_EQ(parsed.step_info_list(0).has_container_meta(), has_container);
        if (uid == 1002)
          EXPECT_EQ(parsed.step_info_list(0).container_meta().ByteSizeLong(),
                    0);
      }
    }
  }
}

TEST(JobQuery, CallerUidPresenceIsDifferentFromRoot) {
  crane::grpc::QueryJobsInfoRequest request;
  EXPECT_FALSE(request.has_uid());
  request.set_uid(0);
  crane::grpc::QueryJobsInfoRequest parsed;
  ASSERT_TRUE(parsed.ParseFromString(request.SerializeAsString()));
  EXPECT_TRUE(parsed.has_uid());
  EXPECT_EQ(parsed.uid(), 0);
}

TEST(JobQuery, ReplySortsLimitsAndRedactsEveryReturnedJob) {
  std::unordered_map<job_id_t, JobInfo> jobs{{1, MakeJob(1, 1002, 5)},
                                             {2, MakeJob(2, 1002, 9)},
                                             {3, MakeJob(3, 1002, 100)}};
  jobs.at(1).set_status(crane::grpc::Pending);
  jobs.at(2).set_status(crane::grpc::Pending);
  const auto reply = BuildJobQueryReply(jobs, 2, 1001, false);
  ASSERT_TRUE(reply.ok());
  ASSERT_EQ(reply.job_info_list_size(), 2);
  EXPECT_TRUE(reply.has_more());
  EXPECT_EQ(reply.job_info_list(0).job_id(), 2);
  EXPECT_EQ(reply.job_info_list(1).job_id(), 1);
  EXPECT_EQ(reply.SerializeAsString().find(kPrivate), std::string::npos);
  for (const auto& job : reply.job_info_list()) {
    EXPECT_EQ(job.query_visibility(), QUERY_VISIBILITY_PUBLIC);
    ASSERT_EQ(job.step_info_list_size(), 1);
    EXPECT_EQ(job.step_info_list(0).query_visibility(),
              QUERY_VISIBILITY_PUBLIC);
  }
}

TEST(JobQuery, SequentialCallerViewsPreserveSourceRecords) {
  const std::unordered_map<job_id_t, JobInfo> jobs{{1, MakeJob(1, 1001, 2)},
                                                   {2, MakeJob(2, 1002, 1)}};
  const auto first = jobs.at(1).SerializeAsString();
  const auto second = jobs.at(2).SerializeAsString();
  for (uint32_t uid : {1001U, 1002U}) {
    const auto reply = BuildJobQueryReply(jobs, 2, uid, false);
    ASSERT_TRUE(reply.ok());
    ASSERT_EQ(reply.job_info_list_size(), 2);
    EXPECT_FALSE(reply.has_more());
    for (const auto& job : reply.job_info_list()) {
      const bool owner = job.uid() == uid;
      EXPECT_EQ(job.cmd_line(), owner ? kPrivate : "");
      EXPECT_EQ(job.query_visibility(),
                owner ? QUERY_VISIBILITY_DETAILS : QUERY_VISIBILITY_PUBLIC);
      ASSERT_EQ(job.step_info_list_size(), 1);
      const auto& step = job.step_info_list(0);
      EXPECT_EQ(step.cmd_line(), owner ? kPrivate : "");
      EXPECT_TRUE(step.container_meta().image().password().empty());
    }
  }
  EXPECT_EQ(jobs.at(1).SerializeAsString(), first);
  EXPECT_EQ(jobs.at(2).SerializeAsString(), second);
}

TEST(JobQuery, ReplyRedactsCompletedJobsAndMergedSteps) {
  auto running = MakeJob(1);
  auto* finished_step = running.add_step_info_list();
  *finished_step = running.step_info_list(0);
  finished_step->set_step_id(7);
  finished_step->set_status(crane::grpc::Completed);
  auto completed = MakeJob(2);
  completed.set_status(crane::grpc::Completed);
  const auto reply =
      BuildJobQueryReply({{1, running}, {2, completed}}, 2, 1002, false);
  ASSERT_TRUE(reply.ok());
  ASSERT_EQ(reply.job_info_list_size(), 2);
  ASSERT_EQ(reply.job_info_list(0).step_info_list_size(), 2);
  EXPECT_EQ(reply.job_info_list(0).step_info_list(1).step_id(), 7);
  EXPECT_EQ(reply.job_info_list(1).status(), crane::grpc::Completed);
  EXPECT_EQ(reply.SerializeAsString().find(kPrivate), std::string::npos);
}

TEST(JobQuery, EmptyReplyIsSuccessfulWithoutExtraMetadata) {
  const auto reply =
      BuildJobQueryReply({}, kDefaultQueryJobNumLimit, std::nullopt, false);
  EXPECT_TRUE(reply.ok());
  EXPECT_FALSE(reply.has_more());
  EXPECT_TRUE(reply.job_info_list().empty());
  EXPECT_EQ(crane::grpc::QueryJobsInfoReply::descriptor()->field_count(), 3);
}

TEST(JobQuery, RpcRejectsNotReadyAndMissingUidWithoutReturningStaleData) {
  Ctld::CraneCtldServiceImpl service(nullptr);
  grpc::ServerContext context;
  crane::grpc::QueryJobsInfoRequest request;
  crane::grpc::QueryJobsInfoReply reply;
  const bool was_ready = g_runtime_status.srv_ready.load();
  // Both failures precede authentication and any scheduler/database access.
  for (bool ready : {false, true}) {
    g_runtime_status.srv_ready = ready;
    reply.set_ok(true);
    *reply.add_job_info_list() = MakeJob();
    const auto status = service.QueryJobsInfo(&context, &request, &reply);
    EXPECT_EQ(status.error_code(), ready ? grpc::StatusCode::INVALID_ARGUMENT
                                         : grpc::StatusCode::UNAVAILABLE);
    EXPECT_FALSE(reply.ok());
    EXPECT_TRUE(reply.job_info_list().empty());
  }
  g_runtime_status.srv_ready = was_ready;
}

void ExpectFields(const google::protobuf::Descriptor* descriptor,
                  std::initializer_list<const char*> classified) {
  std::set<std::string> actual;
  for (int i = 0; i < descriptor->field_count(); ++i)
    actual.emplace(descriptor->field(i)->name());
  EXPECT_EQ(actual,
            (std::set<std::string>(classified.begin(), classified.end())))
      << descriptor->full_name()
      << ": classify new fields before exposing them";
}

TEST(JobQuery, QuerySchemaRequiresExplicitFieldClassification) {
  ExpectFields(JobInfo::descriptor(), {"type",
                                       "job_id",
                                       "name",
                                       "partition",
                                       "uid",
                                       "gid",
                                       "time_limit",
                                       "start_time",
                                       "end_time",
                                       "submit_time",
                                       "account",
                                       "node_num",
                                       "cmd_line",
                                       "cwd",
                                       "username",
                                       "qos",
                                       "req_total_res_view",
                                       "licenses_count",
                                       "req_nodes",
                                       "exclude_nodes",
                                       "extra_attr",
                                       "reservation",
                                       "pod_meta",
                                       "step_info_list",
                                       "dependency_status",
                                       "held",
                                       "status",
                                       "exit_code",
                                       "priority",
                                       "pending_reason",
                                       "craned_list",
                                       "elapsed_time",
                                       "execution_node",
                                       "exclusive",
                                       "allocated_res_view",
                                       "wckey",
                                       "env",
                                       "submit_hostname",
                                       "ntasks",
                                       "deadline_time",
                                       "requeue_count",
                                       "requeue_if_failed",
                                       "array_spec",
                                       "array_task",
                                       "query_visibility"});
  ExpectFields(crane::grpc::StepInfo::descriptor(), {"type",
                                                     "step_type",
                                                     "job_id",
                                                     "step_id",
                                                     "name",
                                                     "uid",
                                                     "gid",
                                                     "time_limit",
                                                     "start_time",
                                                     "end_time",
                                                     "submit_time",
                                                     "node_num",
                                                     "cmd_line",
                                                     "cwd",
                                                     "ntasks",
                                                     "req_total_res_view",
                                                     "req_nodes",
                                                     "exclude_nodes",
                                                     "extra_attr",
                                                     "container_meta",
                                                     "held",
                                                     "status",
                                                     "exit_code",
                                                     "craned_list",
                                                     "elapsed_time",
                                                     "execution_node",
                                                     "allocated_res_view",
                                                     "deadline_time",
                                                     "query_visibility"});
  ExpectFields(crane::grpc::ResourceView::descriptor(),
               {"cpu_count", "memory_bytes", "memory_sw_bytes", "gres_map"});
  ExpectFields(crane::grpc::GresMap::descriptor(), {"name_gres_map"});
  ExpectFields(crane::grpc::GresCount::descriptor(), {"total", "specified"});
  ExpectFields(crane::grpc::ArraySpec::descriptor(),
               {"start", "end", "stride", "max_concurrent"});
  ExpectFields(crane::grpc::ArrayTaskIdentity::descriptor(),
               {"array_job_id", "task_id"});
  ExpectFields(
      crane::grpc::DependencyStatus::descriptor(),
      {"pending", "is_or", "ready_time", "infinite_future", "infinite_past"});
  ExpectFields(crane::grpc::DependencyCondition::descriptor(),
               {"job_id", "type", "delay_seconds"});
}
}  // namespace

namespace {
class NodeQueryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    original_config_ = std::move(g_config);
    g_config = Ctld::Config{};
    auto node = std::make_shared<Ctld::Config::Node>();
    node->cpu = 4;
    node->memory_bytes = 8192;
    node->node_topo_info.sockets = 2;
    g_config.Nodes.emplace("n1", node);
    g_config.NodeOrder = {"n1"};
    g_config.CranedIdByAlias.emplace("n1", "n1");
    g_config.Partitions["batch"].nodes.emplace("n1");
    g_config.PartitionOrder = {"batch"};
    nodes_.InitFromConfig(g_config);
    nodes_.CranedUp("n1", {});
  }
  void TearDown() override { g_config = std::move(original_config_); }
  crane::grpc::CranedInfo ReadNode() {
    return nodes_.QueryCranedInfo("n1").craned_info_list(0);
  }
  Ctld::Config original_config_;
  Ctld::CranedMetaContainer nodes_;
};

TEST_F(NodeQueryTest, NativeOutageRetainsEventWithoutFormattingReason) {
  crane::grpc::ModifyCranedStateRequest request;
  request.add_craned_ids("n1");
  request.set_uid(1000);
  request.set_new_state(crane::grpc::CRANE_DRAIN);
  request.set_reason("maintenance");
  nodes_.ChangeNodeState(request);
  nodes_.CranedDown("n1");
  auto down = ReadNode();
  EXPECT_EQ(down.state_reason(), "maintenance");
  EXPECT_EQ(down.state_reason_uid(), 1000);
  ASSERT_TRUE(down.has_outage());
  EXPECT_EQ(down.outage().uid(), getuid());
  EXPECT_FALSE(down.responding());
  EXPECT_TRUE(down.has_registered());
  nodes_.CranedUp("n1", {});
  EXPECT_TRUE(ReadNode().responding());
  EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
      down.outage(), ReadNode().outage()));
  // Updating the reason while still drained acknowledges the previous outage.
  request.set_reason("new maintenance");
  nodes_.ChangeNodeState(request);
  EXPECT_FALSE(ReadNode().has_outage());
  nodes_.CranedDown("n1");
  nodes_.CranedUp("n1", {});
  ASSERT_TRUE(ReadNode().has_outage());
  request.set_new_state(crane::grpc::CRANE_NONE);
  nodes_.ChangeNodeState(request);
  EXPECT_FALSE(ReadNode().has_outage());
  EXPECT_TRUE(ReadNode().state_reason().empty());
  EXPECT_FALSE(ReadNode().has_state_reason_uid());
  nodes_.CranedDown("n1");
  ASSERT_TRUE(ReadNode().has_outage());
  nodes_.CranedUp("n1", {});
  EXPECT_FALSE(ReadNode().has_outage());
}

TEST_F(NodeQueryTest, ExpectedPowerDisconnectHasNoOutageEvent) {
  {
    auto meta = nodes_.GetCranedMetaPtr("n1");
    meta->power_state = crane::grpc::CRANE_POWER_POWERING_OFF;
  }
  nodes_.CranedDown("n1");
  EXPECT_FALSE(ReadNode().has_outage());
}

TEST_F(NodeQueryTest, SnapshotPreservesExactCleanupAndNativeHoldings) {
  ResourceInNodeV3 held, cleanup;
  held.GetCpuSet().cpu_count = cpu_t::from_raw_value(66);
  held.SetMemoryBytes(5000);
  cleanup.GetCpuSet().cpu_count = cpu_t::from_raw_value(1);
  cleanup.SetMemoryBytes(4000);
  {
    auto meta = nodes_.GetCranedMetaPtr("n1");
    meta->res_in_use = held;
    meta->rn_job_res_map.emplace(2, cleanup);
    meta->remote_meta.cpu_load = 731;
    meta->static_meta.node_topo_info.sockets = 8;
  }
  Ctld::AllocationCleanupMap ending{{2, {"n1"}}};
  crane::grpc::QueryClusterInfoRequest request;
  request.set_include_nodes(true);
  request.set_include_allocation_cleanup(true);
  const auto reply = nodes_.QueryClusterInfo(request, ending);
  ASSERT_EQ(reply.nodes_size(), 1);
  EXPECT_TRUE(reply.has_observed_at());
  const auto& node = reply.nodes(0);
  EXPECT_TRUE(node.cleanup_pending());
  EXPECT_DOUBLE_EQ(node.cleanup_resources().cpu_count(), 1.0 / 256);
  EXPECT_EQ(node.cleanup_resources().memory_bytes(), 4000);
  EXPECT_DOUBLE_EQ(node.res_alloc().cpu_count(), 66.0 / 256);
  EXPECT_EQ(node.res_alloc().memory_bytes(), 5000);
  EXPECT_EQ(node.configured_sockets(), 2);
  EXPECT_EQ(node.node_topo_info().sockets(), 8);
  ending.at(2).clear();
  auto finished = nodes_.QueryClusterInfo(request, ending).nodes(0);
  EXPECT_FALSE(finished.cleanup_pending());
  EXPECT_EQ(finished.cleanup_resources().memory_bytes(), 4000);
  nodes_.CranedDown("n1");
  // The native query keeps the observed sample; clients decide how to display
  // it.
  EXPECT_EQ(ReadNode().cpu_load(), 731);
}

TEST_F(NodeQueryTest, ClusterQueryScopesPreserveOrderMembershipAndTotals) {
  g_config.Nodes.emplace(
      "n2", std::make_shared<Ctld::Config::Node>(*g_config.Nodes.at("n1")));
  g_config.NodeOrder = {"n2", "n1"};
  g_config.CranedIdByAlias.emplace("alias2", "n2");
  g_config.Partitions["batch"].nodes.emplace("n2");
  g_config.Partitions["batch"].nodelist_str = "n[1-2]";
  g_config.Partitions["shared"].nodes = {"n1", "n2"};
  g_config.Partitions["empty"] = {};
  g_config.PartitionOrder = {"shared", "batch", "empty"};
  g_config.DefaultPartition = "batch";
  g_config.Partitions["batch"].priority = 7;
  Ctld::CranedMetaContainer nodes;
  nodes.InitFromConfig(g_config);
  nodes.CranedUp("n1", {});
  nodes.CranedUp("n2", {});
  crane::grpc::QueryClusterInfoRequest request;
  request.set_include_nodes(true);
  request.set_include_partitions(true);
  auto reply = nodes.QueryClusterInfo(request);
  ASSERT_EQ(reply.nodes_size(), 2);
  EXPECT_EQ(reply.nodes(0).hostname(), "n2");
  EXPECT_EQ(reply.nodes(1).hostname(), "n1");
  EXPECT_EQ(reply.nodes(0).partition_names_size(), 2);
  EXPECT_FALSE(reply.nodes(0).has_cleanup_resources());
  ASSERT_EQ(reply.partitions_size(), 3);
  EXPECT_EQ(reply.partitions(0).name(), "shared");
  EXPECT_EQ(reply.partitions(1).name(), "batch");
  EXPECT_TRUE(reply.partitions(1).is_default());
  EXPECT_EQ(reply.partitions(1).priority(), 7);
  EXPECT_EQ(reply.partitions(2).total_nodes(), 0);

  request.add_filter_nodes("alias2");
  request.add_filter_partitions("batch");
  reply = nodes.QueryClusterInfo(request);
  ASSERT_EQ(reply.nodes_size(), 1);
  EXPECT_EQ(reply.nodes(0).hostname(), "n2");
  EXPECT_EQ(reply.nodes(0).partition_names_size(), 2);
  ASSERT_EQ(reply.partitions_size(), 1);
  EXPECT_EQ(reply.partitions(0).total_nodes(), 2);
  EXPECT_EQ(reply.partitions(0).hostlist(), "n[1-2]");

  request.clear_filter_nodes();
  request.add_filter_nodes("missing");
  reply = nodes.QueryClusterInfo(request);
  EXPECT_TRUE(reply.nodes().empty());
  ASSERT_EQ(reply.partitions_size(), 1);
  EXPECT_EQ(reply.partitions(0).total_nodes(), 2);
  request.clear_filter_partitions();
  request.add_filter_partitions("missing");
  request.clear_filter_nodes();
  reply = nodes.QueryClusterInfo(request);
  EXPECT_TRUE(reply.nodes().empty());
  EXPECT_TRUE(reply.partitions().empty());
}

TEST_F(NodeQueryTest, ClusterQueryIncludesOnlyRequestedDomains) {
  Ctld::ResvMeta reservation;
  reservation.name = "test";
  reservation.start_time = absl::FromUnixSeconds(100);
  reservation.end_time = absl::FromUnixSeconds(200);
  {
    auto reservations = nodes_.GetResvMetaMapExclusivePtr();
    reservations->emplace(reservation.name, std::move(reservation));
  }
  crane::grpc::QueryClusterInfoRequest request;
  request.set_include_nodes(true);
  auto reply = nodes_.QueryClusterInfo(request);
  EXPECT_EQ(reply.nodes_size(), 1);
  EXPECT_TRUE(reply.partitions().empty());
  EXPECT_TRUE(reply.reservations().empty());
  request.set_include_nodes(false);
  request.set_include_reservations(true);
  reply = nodes_.QueryClusterInfo(request);
  EXPECT_TRUE(reply.nodes().empty());
  EXPECT_TRUE(reply.partitions().empty());
  EXPECT_EQ(reply.reservations_size(), 1);
  EXPECT_TRUE(reply.ok());
}

TEST_F(NodeQueryTest, AllAndSingleReservationQueriesExposeActualAllocation) {
  Ctld::ResvMeta reservation;
  reservation.name = "test";
  reservation.part_id = "batch";
  reservation.creation_order = 19;
  reservation.start_time = absl::FromUnixSeconds(100);
  reservation.end_time = absl::FromUnixSeconds(200);
  reservation.craned_ids = {"n1", "n2", "n3"};
  ResourceInNodeV3 capacity;
  capacity.GetCpuSet().cpu_count = cpu_t{4};
  reservation.res_total.AddResourceInNode("n2", capacity);
  reservation.res_avail = reservation.res_total;
  reservation.accounts = {"account"};
  reservation.users_black_list = true;
  reservation.users = {"excluded"};
  {
    auto reservations = nodes_.GetResvMetaMapExclusivePtr();
    reservations->emplace(reservation.name, std::move(reservation));
  }
  const auto all = nodes_.QueryAllResvInfo();
  const auto one = nodes_.QueryResvInfo("test");
  ASSERT_EQ(all.reservation_info_list_size(), 1);
  ASSERT_EQ(one.reservation_info_list_size(), 1);
  EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(all, one));
  const auto& info = one.reservation_info_list(0);
  EXPECT_EQ(info.creation_order(), 19);
  EXPECT_EQ(info.craned_regex(), "n2");
  ASSERT_EQ(info.allocated_nodes_size(), 1);
  EXPECT_EQ(info.allocated_nodes(0), "n2");
  EXPECT_EQ(info.res_total().cpu_count(), 4);
  EXPECT_EQ(info.res_avail().cpu_count(), 4);
  EXPECT_EQ(info.res_alloc().cpu_count(), 0);
  EXPECT_EQ(info.allowed_accounts(0), "account");
  EXPECT_EQ(info.denied_users(0), "excluded");
  EXPECT_EQ(nodes_.QueryResvInfo("absent").reservation_info_list_size(), 0);
}

class CranedIdOrderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    original_config_ = std::move(g_config);
    g_config = Ctld::Config{};
  }

  void TearDown() override { g_config = std::move(original_config_); }

  Ctld::Config original_config_;
};

TEST_F(CranedIdOrderTest, UsesConfiguredOrderForSubsetsAndUnknownIds) {
  g_config.NodeOrder = {"n2", "n1", "n3", "n4"};
  g_config.BuildNodeOrderIndex();

  std::list<CranedId> ids{"z-unknown", "n4", "n1", "a-unknown", "n2"};
  Ctld::OrderCranedIdListByNodeOrder(&ids);

  EXPECT_EQ(ids,
            (std::list<CranedId>{"n2", "n1", "n4", "a-unknown", "z-unknown"}));
}

TEST_F(CranedIdOrderTest, RebuildingIndexUsesOnlyTheNewConfiguration) {
  g_config.NodeOrder = {"n3", "n1", "n2"};
  g_config.BuildNodeOrderIndex();
  std::list<CranedId> ids{"n2", "n1", "n3"};
  Ctld::OrderCranedIdListByNodeOrder(&ids);
  EXPECT_EQ(ids, (std::list<CranedId>{"n3", "n1", "n2"}));

  g_config.NodeOrder = {"n2"};
  g_config.BuildNodeOrderIndex();
  Ctld::OrderCranedIdListByNodeOrder(&ids);
  EXPECT_EQ(ids, (std::list<CranedId>{"n2", "n1", "n3"}));

  g_config.NodeOrder.clear();
  g_config.BuildNodeOrderIndex();
  Ctld::OrderCranedIdListByNodeOrder(&ids);
  EXPECT_EQ(ids, (std::list<CranedId>{"n1", "n2", "n3"}));
}

}  // namespace

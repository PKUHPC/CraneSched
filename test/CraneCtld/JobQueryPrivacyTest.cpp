// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include <gtest/gtest.h>

#include <set>
#include <string>

#include "RpcService/JobQueryPrivacy.h"
#include "google/protobuf/util/message_differencer.h"
#include "protos/Crane.pb.h"

namespace {
using crane::grpc::JobInfo;
using crane::grpc::QUERY_VISIBILITY_DETAILS;
using crane::grpc::QUERY_VISIBILITY_PUBLIC;
using Ctld::ProjectJobForQuery;
constexpr char kPrivate[] = "private-query-test-marker";

void AddUnknown(google::protobuf::Message* message) {
  message->GetReflection()->MutableUnknownFields(message)->AddLengthDelimited(
      12345, kPrivate);
}

JobInfo MakeJob() {
  JobInfo job;
  job.set_job_id(71);
  job.set_uid(1001);
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
  job.set_priority(9);
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

TEST(JobQueryPrivacy, PublicViewPreservesEveryKnownSummaryField) {
  const auto source = MakeJob();
  auto expected = source;
  expected.clear_cmd_line();
  expected.clear_cwd();
  expected.clear_env();
  expected.clear_extra_attr();
  expected.clear_pod_meta();
  expected.set_query_visibility(QUERY_VISIBILITY_PUBLIC);
  for (auto& step : *expected.mutable_step_info_list()) {
    step.set_is_container(step.has_container_meta());
    step.clear_cmd_line();
    step.clear_cwd();
    step.clear_extra_attr();
    step.clear_container_meta();
    step.set_query_visibility(QUERY_VISIBILITY_PUBLIC);
  }
  const auto result = ProjectJobForQuery(source, 1002, false);
  EXPECT_TRUE(
      google::protobuf::util::MessageDifferencer::Equals(expected, result));
  EXPECT_EQ(result.SerializeAsString().find(kPrivate), std::string::npos);
}

TEST(JobQueryPrivacy, OwnerAndAdministratorRetainDetailsExceptPassword) {
  const auto source = MakeJob();
  auto expected = source;
  expected.set_query_visibility(QUERY_VISIBILITY_DETAILS);
  expected.mutable_step_info_list(0)->set_is_container(true);
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

TEST(JobQueryPrivacy, UnauthenticatedClaimsCannotReadDetailsEvenForRoot) {
  auto source = MakeJob();
  source.set_uid(0);
  for (const bool claimed_admin : {false, true}) {
    const auto result = ProjectJobForQuery(source, std::nullopt, claimed_admin);
    EXPECT_EQ(result.query_visibility(), QUERY_VISIBILITY_PUBLIC);
    EXPECT_EQ(result.SerializeAsString().find(kPrivate), std::string::npos);
  }
}

TEST(JobQueryPrivacy, StepsRequireParentAndStepOwnership) {
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

TEST(JobQueryPrivacy, UnknownNestedFieldsCannotLeakOrAlterSource) {
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
  const auto before = source;
  const auto public_view = ProjectJobForQuery(source, 1002, false);
  EXPECT_EQ(public_view.SerializeAsString().find(kPrivate), std::string::npos);
  const auto details = ProjectJobForQuery(source, 1001, false);
  EXPECT_EQ(details.cwd(), kPrivate);
  EXPECT_TRUE(
      google::protobuf::util::MessageDifferencer::Equals(before, source));
}

TEST(JobQueryPrivacy, PreservesOneofAndOptionalPresence) {
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

TEST(JobQueryPrivacy,
     ContainerKindSurvivesRedactionWithoutIncludingHostScripts) {
  for (const auto kind : {crane::grpc::PRIMARY, crane::grpc::COMMON}) {
    for (const bool has_container : {false, true}) {
      auto source = MakeJob();
      auto* step = source.mutable_step_info_list(0);
      step->set_step_type(kind);
      if (!has_container) step->clear_container_meta();
      for (const auto uid : {1001U, 1002U}) {
        const auto result = ProjectJobForQuery(source, uid, false);
        EXPECT_EQ(result.step_info_list(0).is_container(), has_container);
        if (uid == 1002)
          EXPECT_FALSE(result.step_info_list(0).has_container_meta());
      }
    }
  }
}

TEST(JobQueryPrivacy, CallerUidPresenceIsDifferentFromRoot) {
  crane::grpc::QueryJobsInfoRequest request;
  EXPECT_FALSE(request.has_uid());
  request.set_uid(0);
  crane::grpc::QueryJobsInfoRequest parsed;
  ASSERT_TRUE(parsed.ParseFromString(request.SerializeAsString()));
  EXPECT_TRUE(parsed.has_uid());
  EXPECT_EQ(parsed.uid(), 0);
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

TEST(JobQueryPrivacy, QuerySchemaRequiresExplicitFieldClassification) {
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
                                                     "query_visibility",
                                                     "is_container"});
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

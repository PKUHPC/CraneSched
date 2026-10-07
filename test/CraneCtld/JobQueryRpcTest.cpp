// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>

#include "RpcService/JobQuery.h"
#include "protos/Crane.grpc.pb.h"

namespace {
using crane::grpc::JobInfo;
using crane::grpc::QUERY_VISIBILITY_DETAILS;
using crane::grpc::QUERY_VISIBILITY_PUBLIC;
using crane::grpc::QueryJobsInfoReply;
using crane::grpc::QueryJobsInfoRequest;
constexpr char kPrivate[] = "private-rpc-query-marker";
constexpr char kPassword[] = "registry-password-marker";

JobInfo MakeJob(uint32_t id, uint32_t owner, uint32_t priority = 1) {
  JobInfo job;
  job.set_job_id(id);
  job.set_uid(owner);
  job.set_name("public-job");
  job.set_priority(priority);
  job.set_status(crane::grpc::Running);
  job.set_cmd_line(kPrivate);
  job.set_cwd(kPrivate);
  (*job.mutable_env())["PRIVATE"] = kPrivate;
  auto* step = job.add_step_info_list();
  step->set_job_id(id);
  step->set_step_id(0);
  step->set_uid(owner);
  step->set_cmd_line(kPrivate);
  step->mutable_container_meta()->mutable_image()->set_password(kPassword);
  return job;
}

// Transport adapter around the very same handler used by CraneCtldServiceImpl.
// Only identity and read dependencies are fakes; no daemon or database starts.
class QueryTestService final : public crane::grpc::CraneCtld::Service {
 public:
  Ctld::JobQueryDependencies deps{};

  grpc::Status QueryJobsInfo(grpc::ServerContext*,
                             const QueryJobsInfoRequest* request,
                             QueryJobsInfoReply* response) override {
    return Ctld::HandleJobQuery(*request, response, deps);
  }
};

class JobQueryRpcTest : public testing::Test {
 protected:
  void SetUp() override {
    auto& deps = service_.deps;
    deps.ready = true;
    deps.tls_enabled = true;
    deps.default_limit = 2;
    deps.authenticate = [this](uint32_t uid) {
      ++auth_calls_;
      authenticated_uid_ = uid;
      return auth_status_;
    };
    deps.resolve_admin = [this](uint32_t uid, bool* admin) {
      ++role_calls_;
      EXPECT_EQ(uid, authenticated_uid_);
      *admin = grant_admin_;
      return role_status_;
    };
    deps.resolve_node_alias = [](const std::string& node) {
      return node == "node-alias" ? "node1" : node;
    };
    deps.query_ram = [this](const auto* request, auto* jobs, size_t limit) {
      ++ram_calls_;
      seen_request_ = *request;
      ram_limit_ = limit;
      *jobs = live_;
    };
    deps.query_history = [this](const auto* request, auto* jobs, size_t limit) {
      ++history_calls_;
      seen_request_ = *request;
      history_limit_ = limit;
      for (const auto& [id, job] : history_) jobs->try_emplace(id, job);
      return history_ok_;
    };
    deps.query_history_steps = [this](const auto* request, auto* jobs) {
      ++step_calls_;
      seen_request_ = *request;
      for (const auto& [id, job] : history_) {
        auto it = jobs->find(id);
        if (it == jobs->end()) continue;
        for (const auto& step : job.step_info_list())
          *it->second.add_step_info_list() = step;
      }
      return history_ok_;
    };
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                             &port);
    builder.RegisterService(&service_);
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);
    ASSERT_GT(port, 0);
    stub_ = crane::grpc::CraneCtld::NewStub(
        grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                            grpc::InsecureChannelCredentials()));
    request_.set_uid(1001);
  }

  void TearDown() override {
    if (server_) server_->Shutdown();
  }

  grpc::Status Query() {
    reply_.Clear();
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(5));
    return stub_->QueryJobsInfo(&context, request_, &reply_);
  }

  void ExpectNoDataRead() {
    EXPECT_EQ(ram_calls_, 0);
    EXPECT_EQ(history_calls_, 0);
    EXPECT_EQ(step_calls_, 0);
    EXPECT_TRUE(reply_.job_info_list().empty());
    EXPECT_FALSE(reply_.ok());
  }

  void ExpectPublic(const JobInfo& job) {
    EXPECT_EQ(job.query_visibility(), QUERY_VISIBILITY_PUBLIC);
    EXPECT_EQ(job.SerializeAsString().find(kPrivate), std::string::npos);
    EXPECT_EQ(job.SerializeAsString().find(kPassword), std::string::npos);
    EXPECT_EQ(job.name(), "public-job");
    for (const auto& step : job.step_info_list()) {
      EXPECT_EQ(step.query_visibility(), QUERY_VISIBILITY_PUBLIC);
      EXPECT_FALSE(step.has_container_meta());
    }
  }

  QueryTestService service_;
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<crane::grpc::CraneCtld::Stub> stub_;
  QueryJobsInfoRequest request_, seen_request_;
  QueryJobsInfoReply reply_;
  Ctld::JobQueryResult live_, history_;
  grpc::Status auth_status_, role_status_;
  bool grant_admin_ = false, history_ok_ = true;
  int auth_calls_ = 0, role_calls_ = 0, ram_calls_ = 0;
  int history_calls_ = 0, step_calls_ = 0;
  uint32_t authenticated_uid_ = 0;
  size_t ram_limit_ = 0, history_limit_ = 0;
};

TEST_F(JobQueryRpcTest, ReadinessFailurePrecedesIdentityAndDataReads) {
  service_.deps.ready = false;
  EXPECT_EQ(Query().error_code(), grpc::StatusCode::UNAVAILABLE);
  EXPECT_EQ(auth_calls_, 0);
  ExpectNoDataRead();
}

TEST_F(JobQueryRpcTest, MissingUidIsRejectedBeforeIdentityAndDataReads) {
  request_.clear_uid();
  EXPECT_EQ(Query().error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  EXPECT_EQ(auth_calls_, 0);
  ExpectNoDataRead();
}

TEST_F(JobQueryRpcTest, ExplicitAuthenticatedRootUidIsNotMissing) {
  request_.set_uid(0);
  grant_admin_ = true;
  live_.emplace(1, MakeJob(1, 1002));
  ASSERT_TRUE(Query().ok());
  ASSERT_TRUE(reply_.ok());
  ASSERT_EQ(reply_.job_info_list_size(), 1);
  EXPECT_EQ(authenticated_uid_, 0);
  EXPECT_EQ(role_calls_, 1);
  EXPECT_EQ(reply_.job_info_list(0).query_visibility(),
            QUERY_VISIBILITY_DETAILS);
  EXPECT_EQ(reply_.job_info_list(0).cmd_line(), kPrivate);
  EXPECT_EQ(reply_.SerializeAsString().find(kPassword), std::string::npos);
}

TEST_F(JobQueryRpcTest, InvalidCertificateStopsBeforeRoleLookupAndDataReads) {
  auth_status_ = {grpc::StatusCode::UNAUTHENTICATED,
                  "certificate UID mismatch"};
  live_.emplace(1, MakeJob(1, 1001));
  EXPECT_EQ(Query().error_code(), grpc::StatusCode::UNAUTHENTICATED);
  EXPECT_EQ(role_calls_, 0);
  ExpectNoDataRead();
}

TEST_F(JobQueryRpcTest, FailedRoleLookupDoesNotGrantDetailsOrReadData) {
  role_status_ = {grpc::StatusCode::PERMISSION_DENIED, "unknown user"};
  grant_admin_ = true;
  EXPECT_EQ(Query().error_code(), grpc::StatusCode::PERMISSION_DENIED);
  EXPECT_EQ(auth_calls_, 1);
  ExpectNoDataRead();
}

TEST_F(JobQueryRpcTest, NonTlsOwnerAndRootClaimsRemainPublic) {
  service_.deps.tls_enabled = false;
  grant_admin_ = true;
  live_.emplace(1, MakeJob(1, 1001));
  for (auto uid : {1001u, 0u}) {
    request_.set_uid(uid);
    ASSERT_TRUE(Query().ok());
    ASSERT_TRUE(reply_.ok());
    ASSERT_EQ(reply_.job_info_list_size(), 1);
    ExpectPublic(reply_.job_info_list(0));
  }
  EXPECT_EQ(auth_calls_, 0);
  EXPECT_EQ(role_calls_, 0);
}

TEST_F(JobQueryRpcTest, MixedOwnersAndSequentialQueriesDoNotMutateSources) {
  live_.emplace(1, MakeJob(1, 1001, 3));
  live_.emplace(2, MakeJob(2, 1002, 2));
  const auto original1 = live_.at(1).SerializeAsString();
  const auto original2 = live_.at(2).SerializeAsString();
  ASSERT_TRUE(Query().ok());
  ASSERT_EQ(reply_.job_info_list_size(), 2);
  EXPECT_EQ(reply_.job_info_list(0).cmd_line(), kPrivate);
  ExpectPublic(reply_.job_info_list(1));
  request_.set_uid(1002);
  ASSERT_TRUE(Query().ok());
  ASSERT_EQ(reply_.job_info_list_size(), 2);
  ExpectPublic(reply_.job_info_list(0));
  EXPECT_EQ(reply_.job_info_list(1).query_visibility(),
            QUERY_VISIBILITY_DETAILS);
  EXPECT_EQ(reply_.job_info_list(1).cmd_line(), kPrivate);
  EXPECT_EQ(reply_.job_info_list(1).step_info_list(0).cmd_line(), kPrivate);
  EXPECT_EQ(reply_.SerializeAsString().find(kPassword), std::string::npos);
  EXPECT_EQ(live_.at(1).SerializeAsString(), original1);
  EXPECT_EQ(live_.at(2).SerializeAsString(), original2);
}

TEST_F(JobQueryRpcTest, RamOnlyPathPreservesSortingLimitAndProbe) {
  live_.emplace(1, MakeJob(1, 1002, 5));
  live_.emplace(2, MakeJob(2, 1002, 9));
  live_.emplace(3, MakeJob(3, 1002, 100));
  live_.at(1).set_status(crane::grpc::Pending);
  live_.at(2).set_status(crane::grpc::Pending);
  ASSERT_TRUE(Query().ok());
  ASSERT_TRUE(reply_.ok());
  ASSERT_EQ(reply_.job_info_list_size(), 2);
  EXPECT_EQ(reply_.job_info_list(0).job_id(), 2);
  EXPECT_EQ(reply_.job_info_list(1).job_id(), 1);
  EXPECT_TRUE(reply_.has_more());
  EXPECT_EQ(ram_limit_, 3);
  EXPECT_EQ(history_calls_, 0);
  EXPECT_EQ(step_calls_, 0);
  for (const auto& job : reply_.job_info_list()) ExpectPublic(job);
}

TEST_F(JobQueryRpcTest, RamProbePathProjectsHistoricalStepsBeforeReturning) {
  request_.set_option_include_completed_jobs(true);
  request_.set_num_limit(1);
  live_.emplace(1, MakeJob(1, 1002, 9));
  live_.emplace(2, MakeJob(2, 1002, 1));
  history_.emplace(1, MakeJob(1, 1002));
  history_.at(1).mutable_step_info_list(0)->set_step_id(7);
  ASSERT_TRUE(Query().ok());
  ASSERT_TRUE(reply_.ok());
  ASSERT_EQ(reply_.job_info_list_size(), 1);
  ASSERT_EQ(reply_.job_info_list(0).step_info_list_size(), 2);
  EXPECT_EQ(reply_.job_info_list(0).step_info_list(1).step_id(), 7);
  ExpectPublic(reply_.job_info_list(0));
  EXPECT_EQ(step_calls_, 1);
  EXPECT_EQ(history_calls_, 0);
  EXPECT_TRUE(reply_.has_more());
  EXPECT_EQ(live_.at(1).step_info_list_size(), 1);
  EXPECT_EQ(history_.at(1).step_info_list(0).cmd_line(), kPrivate);
}

TEST_F(JobQueryRpcTest, HistoryPathProjectsMergedRecordsAndUsesFullProbe) {
  request_.set_option_include_completed_jobs(true);
  live_.emplace(1, MakeJob(1, 1002, 30));
  history_.emplace(1, MakeJob(1, 1002, 1));
  history_.emplace(2, MakeJob(2, 1002, 20));
  history_.emplace(3, MakeJob(3, 1002, 10));
  ASSERT_TRUE(Query().ok());
  ASSERT_TRUE(reply_.ok());
  ASSERT_EQ(reply_.job_info_list_size(), 2);
  EXPECT_EQ(reply_.job_info_list(0).job_id(), 1);
  EXPECT_EQ(reply_.job_info_list(0).priority(), 30);
  EXPECT_EQ(reply_.job_info_list(1).job_id(), 2);
  EXPECT_EQ(history_calls_, 1);
  EXPECT_EQ(step_calls_, 0);
  EXPECT_EQ(history_limit_, 3);
  EXPECT_TRUE(reply_.has_more());
  for (const auto& job : reply_.job_info_list()) ExpectPublic(job);
  EXPECT_EQ(history_.at(2).cmd_line(), kPrivate);
}

TEST_F(JobQueryRpcTest, BothStorageFailuresReturnNoPartialPrivateRecords) {
  request_.set_option_include_completed_jobs(true);
  request_.set_num_limit(1);
  history_ok_ = false;
  history_.emplace(1, MakeJob(1, 1001));
  ASSERT_TRUE(Query().ok());
  EXPECT_FALSE(reply_.ok());
  EXPECT_TRUE(reply_.job_info_list().empty());
  EXPECT_EQ(history_calls_, 1);
  live_.emplace(1, MakeJob(1, 1001));
  live_.emplace(2, MakeJob(2, 1001));
  ASSERT_TRUE(Query().ok());
  EXPECT_FALSE(reply_.ok());
  EXPECT_TRUE(reply_.job_info_list().empty());
  EXPECT_EQ(step_calls_, 1);
}

TEST_F(JobQueryRpcTest, EmptyHistoryResultIsSuccessfulWithoutExtraMetadata) {
  request_.set_option_include_completed_jobs(true);
  ASSERT_TRUE(Query().ok());
  EXPECT_TRUE(reply_.ok());
  EXPECT_FALSE(reply_.has_more());
  EXPECT_TRUE(reply_.job_info_list().empty());
  EXPECT_EQ(history_calls_, 1);
  EXPECT_EQ(QueryJobsInfoReply::descriptor()->field_count(), 3);
}

TEST_F(JobQueryRpcTest,
       FiltersAndNodeAliasesArePreservedWithoutRequestMutation) {
  request_.add_filter_nodename_list("node-alias");
  request_.add_filter_users("different-user");
  request_.add_filter_accounts("shared-account");
  auto* selector = request_.add_filter_job_ids();
  selector->set_job_id(42);
  selector->add_steps(3);
  request_.set_option_include_completed_jobs(true);
  request_.set_num_limit(7);
  const auto original = request_.SerializeAsString();
  ASSERT_TRUE(Query().ok());
  EXPECT_EQ(authenticated_uid_, 1001);
  auto expected = request_;
  expected.set_filter_nodename_list(0, "node1");
  EXPECT_EQ(seen_request_.SerializeAsString(), expected.SerializeAsString());
  EXPECT_EQ(request_.SerializeAsString(), original);
  EXPECT_EQ(ram_limit_, 8);
  EXPECT_EQ(history_limit_, 8);
}
}  // namespace

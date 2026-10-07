// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <openssl/ec.h>
#include <openssl/pem.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>

#include "Account/AccountManager.h"
#include "JobScheduler.h"
#include "RpcService/CtldGrpcServer.h"
#include "Security/VaultClient.h"
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

// The loopback transport supplies an ephemeral certificate to the real RPC
// implementation. The TLS handshake itself is outside this isolated test.
std::string MakeCertificate(uint32_t uid) {
  std::unique_ptr<EVP_PKEY_CTX, decltype(&EVP_PKEY_CTX_free)> key_context(
      EVP_PKEY_CTX_new_id(EVP_PKEY_EC, nullptr), EVP_PKEY_CTX_free);
  EVP_PKEY* generated_key = nullptr;
  if (!key_context || EVP_PKEY_keygen_init(key_context.get()) <= 0 ||
      EVP_PKEY_CTX_set_ec_paramgen_curve_nid(key_context.get(),
                                             NID_X9_62_prime256v1) <= 0 ||
      EVP_PKEY_keygen(key_context.get(), &generated_key) <= 0)
    return {};
  std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(generated_key,
                                                          EVP_PKEY_free);
  std::unique_ptr<X509, decltype(&X509_free)> cert(X509_new(), X509_free);
  std::unique_ptr<BIO, decltype(&BIO_free)> pem(BIO_new(BIO_s_mem()), BIO_free);
  if (!key || !cert || !pem) return {};
  const auto cn = std::to_string(uid) + ".job-query-test";
  if (X509_set_version(cert.get(), 2) != 1 ||
      ASN1_INTEGER_set(X509_get_serialNumber(cert.get()), uid + 1) != 1 ||
      !X509_gmtime_adj(X509_getm_notBefore(cert.get()), -60) ||
      !X509_gmtime_adj(X509_getm_notAfter(cert.get()), 3600) ||
      X509_set_pubkey(cert.get(), key.get()) != 1 ||
      X509_NAME_add_entry_by_txt(
          X509_get_subject_name(cert.get()), "CN", MBSTRING_ASC,
          reinterpret_cast<const unsigned char*>(cn.c_str()), -1, -1, 0) != 1 ||
      X509_set_issuer_name(cert.get(), X509_get_subject_name(cert.get())) !=
          1 ||
      X509_sign(cert.get(), key.get(), EVP_sha256()) <= 0 ||
      PEM_write_bio_X509(pem.get(), cert.get()) != 1)
    return {};
  char* data = nullptr;
  const auto size = BIO_get_mem_data(pem.get(), &data);
  return {data, static_cast<size_t>(size)};
}

using JobQueryResult = std::unordered_map<job_id_t, JobInfo>;

struct QueryReadState {
  QueryJobsInfoRequest seen_request_;
  JobQueryResult live_, history_;
  bool grant_admin_ = false, history_ok_ = true, role_ok_ = true;
  int auth_calls_ = 0, role_calls_ = 0, ram_calls_ = 0;
  int history_calls_ = 0, step_calls_ = 0;
  uint32_t authenticated_uid_ = 0;
  size_t ram_limit_ = 0, history_limit_ = 0;
};

QueryReadState* active_state = nullptr;

class QueryTestService final : public crane::grpc::CraneCtld::Service {
 public:
  std::string certificate;

  grpc::Status QueryJobsInfo(grpc::ServerContext* context,
                             const QueryJobsInfoRequest* request,
                             QueryJobsInfoReply* response) override {
    if (!certificate.empty())
      std::const_pointer_cast<grpc::AuthContext>(context->auth_context())
          ->AddProperty("x509_pem_cert", certificate);
    return implementation_.QueryJobsInfo(context, request, response);
  }

 private:
  Ctld::CraneCtldServiceImpl implementation_{nullptr};
};

class JobQueryRpcTest : public testing::Test, protected QueryReadState {
 protected:
  void SetUp() override {
    active_state = this;
    g_runtime_status.srv_ready = true;
    g_config.ListenConf.TlsConfig.Enabled = true;
    g_config.CranedIdByAlias["node-alias"] = "node1";
    g_config.CtldConf.SchedulerRpcThreadPoolSize = 1;
    // Construct clients, but never call Init()/Connect() or start a database.
    g_account_manager = std::make_unique<Ctld::AccountManager>();
    g_job_scheduler = std::make_unique<Ctld::JobScheduler>();
    g_vault_client = std::make_unique<Ctld::Security::VaultClient>();

    service_.certificate = MakeCertificate(1001);
    ASSERT_FALSE(service_.certificate.empty());
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                             &port);
    builder.RegisterService(&service_);
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);
    ASSERT_GT(port, 0);
    address_ = "127.0.0.1:" + std::to_string(port);
    request_.set_uid(1001);
    request_.set_num_limit(2);
  }

  void TearDown() override {
    if (server_) server_->Shutdown();
    g_job_scheduler.reset();
    g_account_manager.reset();
    g_vault_client.reset();
    g_runtime_status.srv_ready = false;
    g_config.CranedIdByAlias.clear();
    active_state = nullptr;
  }

  grpc::Status Query() {
    reply_.Clear();
    // Authentication context belongs to the connection. A caller change must
    // get its own connection so a previous certificate cannot be reused.
    grpc::ChannelArguments arguments;
    arguments.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);
    auto stub = crane::grpc::CraneCtld::NewStub(grpc::CreateCustomChannel(
        address_, grpc::InsecureChannelCredentials(), arguments));
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(5));
    return stub->QueryJobsInfo(&context, request_, &reply_);
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
  std::string address_;
  QueryJobsInfoRequest request_;
  QueryJobsInfoReply reply_;
};

TEST_F(JobQueryRpcTest, ZeroLimitUsesProductionDefault) {
  request_.set_num_limit(0);
  ASSERT_TRUE(Query().ok());
  EXPECT_TRUE(reply_.ok());
  EXPECT_EQ(ram_limit_, kDefaultQueryJobNumLimit + 1);
}

TEST_F(JobQueryRpcTest, MissingCertificateStopsBeforeReads) {
  service_.certificate.clear();
  EXPECT_EQ(Query().error_code(), grpc::StatusCode::UNAUTHENTICATED);
  EXPECT_EQ(role_calls_, 0);
  ExpectNoDataRead();
}

TEST_F(JobQueryRpcTest, ReadinessFailurePrecedesIdentityAndDataReads) {
  g_runtime_status.srv_ready = false;
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
  service_.certificate = MakeCertificate(0);
  ASSERT_FALSE(service_.certificate.empty());
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
  service_.certificate = MakeCertificate(1002);
  ASSERT_FALSE(service_.certificate.empty());
  live_.emplace(1, MakeJob(1, 1001));
  EXPECT_EQ(Query().error_code(), grpc::StatusCode::UNAUTHENTICATED);
  EXPECT_EQ(auth_calls_, 1);
  EXPECT_EQ(role_calls_, 0);
  ExpectNoDataRead();
}

TEST_F(JobQueryRpcTest, FailedRoleLookupDoesNotGrantDetailsOrReadData) {
  role_ok_ = false;
  grant_admin_ = true;
  EXPECT_EQ(Query().error_code(), grpc::StatusCode::PERMISSION_DENIED);
  EXPECT_EQ(auth_calls_, 1);
  ExpectNoDataRead();
}

TEST_F(JobQueryRpcTest, NonTlsOwnerAndRootClaimsRemainPublic) {
  g_config.ListenConf.TlsConfig.Enabled = false;
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
  service_.certificate = MakeCertificate(1002);
  ASSERT_FALSE(service_.certificate.empty());
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

// Link-time substitutes for read dependencies only; no production test hooks.
void QueryRam(Ctld::JobScheduler*, const QueryJobsInfoRequest* request,
              JobQueryResult* jobs, size_t limit) asm(JOB_QUERY_WRAP_RAM);
void QueryRam(Ctld::JobScheduler*, const QueryJobsInfoRequest* request,
              JobQueryResult* jobs, size_t limit) {
  auto& state = *active_state;
  ++state.ram_calls_;
  state.seen_request_ = *request;
  state.ram_limit_ = limit;
  *jobs = state.live_;
}

bool QueryHistory(Ctld::MongodbClient*, const QueryJobsInfoRequest* request,
                  JobQueryResult* jobs,
                  size_t limit) asm(JOB_QUERY_WRAP_HISTORY);
bool QueryHistory(Ctld::MongodbClient*, const QueryJobsInfoRequest* request,
                  JobQueryResult* jobs, size_t limit) {
  auto& state = *active_state;
  ++state.history_calls_;
  state.seen_request_ = *request;
  state.history_limit_ = limit;
  for (const auto& [id, job] : state.history_) jobs->try_emplace(id, job);
  return state.history_ok_;
}

bool QueryHistorySteps(Ctld::MongodbClient*,
                       const QueryJobsInfoRequest* request,
                       JobQueryResult* jobs) asm(JOB_QUERY_WRAP_STEPS);
bool QueryHistorySteps(Ctld::MongodbClient*,
                       const QueryJobsInfoRequest* request,
                       JobQueryResult* jobs) {
  auto& state = *active_state;
  ++state.step_calls_;
  state.seen_request_ = *request;
  for (const auto& [id, job] : state.history_) {
    auto it = jobs->find(id);
    if (it == jobs->end()) continue;
    for (const auto& step : job.step_info_list())
      *it->second.add_step_info_list() = step;
  }
  return state.history_ok_;
}

CraneExpected<std::string> ResolveAdmin(Ctld::AccountManager*,
                                        uint32_t uid) asm(JOB_QUERY_WRAP_ADMIN);
CraneExpected<std::string> ResolveAdmin(Ctld::AccountManager*, uint32_t uid) {
  auto& state = *active_state;
  ++state.role_calls_;
  state.authenticated_uid_ = uid;
  if (!state.role_ok_)
    return std::unexpected(CraneErrCode::ERR_INVALID_OP_USER);
  if (state.grant_admin_) return std::string("admin");
  return std::unexpected(CraneErrCode::ERR_USER_NO_PRIVILEGE);
}

bool AllowCertificate(Ctld::Security::VaultClient*,
                      const std::string&) asm(JOB_QUERY_WRAP_CERTIFICATE);
bool AllowCertificate(Ctld::Security::VaultClient*, const std::string&) {
  ++active_state->auth_calls_;
  return true;
}

void SelectAllQos(Ctld::MongodbClient*,
                  std::list<Ctld::Qos>* result) asm(JOB_QUERY_WRAP_QOS);
void SelectAllQos(Ctld::MongodbClient*, std::list<Ctld::Qos>* result) {
  result->clear();
}

void SelectAllUser(Ctld::MongodbClient*,
                   std::list<Ctld::User>* result) asm(JOB_QUERY_WRAP_USERS);
void SelectAllUser(Ctld::MongodbClient*, std::list<Ctld::User>* result) {
  result->clear();
}

void SelectAllWckey(Ctld::MongodbClient*,
                    std::list<Ctld::Wckey>* result) asm(JOB_QUERY_WRAP_WCKEYS);
void SelectAllWckey(Ctld::MongodbClient*, std::list<Ctld::Wckey>* result) {
  result->clear();
}

void SelectAllAccount(
    Ctld::MongodbClient*,
    std::list<Ctld::Account>* result) asm(JOB_QUERY_WRAP_ACCOUNTS);
void SelectAllAccount(Ctld::MongodbClient*, std::list<Ctld::Account>* result) {
  result->clear();
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  char log_path[] = "/tmp/crane-job-query-test-XXXXXX";
  const int fd = mkstemp(log_path);
  if (fd < 0) return EXIT_FAILURE;
  if (close(fd) != 0) {
    std::filesystem::remove(log_path);
    return EXIT_FAILURE;
  }

  int result = EXIT_FAILURE;
  try {
    InitLogger(spdlog::level::off, log_path, false, 1024 * 1024, 1, 128, 1);
    g_config.CtldConf.LogToConsole = false;
    g_config.CraneCtldDebugLevel = "off";
    // mongocxx permits one driver instance per process, including repeated
    // test runs. Construct the client once; never connect to a database.
    g_db_client = std::make_unique<Ctld::MongodbClient>();
    result = RUN_ALL_TESTS();
  } catch (const std::exception& error) {
    std::fprintf(stderr, "Job-query test setup failed: %s\n", error.what());
  }
  g_db_client.reset();
  g_runtime_status.db_logger.reset();
  spdlog::shutdown();
  std::error_code remove_error;
  std::filesystem::remove(log_path, remove_error);
  return remove_error ? EXIT_FAILURE : result;
}

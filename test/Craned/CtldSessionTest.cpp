// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include "cri/api.pb.h"
// CRI signal enum names must precede system signal macros.

#include <gtest/gtest.h>

#include "../../src/Craned/Core/CranedServer.h"
#include "../../src/Craned/Core/CtldClient.h"
#include "../../src/Craned/Core/JobManager.h"

// Logger.h configures the active spdlog level.
#include <spdlog/sinks/null_sink.h>

namespace Craned {
namespace {

using namespace std::chrono_literals;

// The test controller issues an ordinary registration token. Keep the session
// in REGISTERING so a final status has to use the shutdown flush.
class TestController final : public CraneCtldForInternal::Service {
 public:
  grpc::Status CranedTriggerReverseConn(
      grpc::ServerContext*,
      const crane::grpc::CranedTriggerReverseConnRequest* req,
      google::protobuf::Empty*) override {
    absl::MutexLock lock(&requests_mutex);
    if (!triggered.HasBeenNotified()) {
      config.set_ok(true);
      *config.mutable_token() = req->token();
      triggered.Notify();
    } else if (!retried.HasBeenNotified()) {
      retry_config.set_ok(true);
      *retry_config.mutable_token() = req->token();
      retried.Notify();
    }
    return grpc::Status::OK;
  }

  grpc::Status CranedRegister(grpc::ServerContext*,
                              const crane::grpc::CranedRegisterRequest*,
                              crane::grpc::CranedRegisterReply*) override {
    cleaned_at_register = g_job_mgr->GetAllocatedJobSteps().empty();
    if (!registered.HasBeenNotified()) registered.Notify();
    if (hold_register_reply.load()) release_register.WaitForNotification();
    return {grpc::StatusCode::UNAVAILABLE, "registration reply unavailable"};
  }

  grpc::Status StepStatusChange(
      grpc::ServerContext*, const crane::grpc::StepStatusChangeRequest* req,
      crane::grpc::StepStatusChangeReply* reply) override {
    last_step = req->step_id();
    last_status = req->new_status();
    last_code = req->exit_code();
    reply->set_ok(true);
    return grpc::Status::OK;
  }

  ConfigureRequest config;
  absl::Mutex requests_mutex;
  ConfigureRequest retry_config;
  absl::Notification retried;
  absl::Notification triggered;
  absl::Notification registered;
  absl::Notification release_register;
  std::atomic_bool hold_register_reply{false};
  bool cleaned_at_register = false;
  std::atomic<step_id_t> last_step{0};
  std::atomic<StepStatus> last_status{StepStatus::Pending};
  std::atomic<uint32_t> last_code{0};
};

class TestSupervisor final
    : public crane::grpc::supervisor::Supervisor::Service {
 public:
  grpc::Status ShutdownSupervisor(
      grpc::ServerContext*,
      const crane::grpc::supervisor::ShutdownSupervisorRequest*,
      crane::grpc::supervisor::ShutdownSupervisorReply*) override {
    if (!shutdown.HasBeenNotified()) shutdown.Notify();
    if (hold_reply.load()) release.WaitForNotification();
    return grpc::Status::OK;
  }
  grpc::Status ExecuteStep(
      grpc::ServerContext*,
      const crane::grpc::supervisor::StepExecutionRequest*,
      crane::grpc::supervisor::StepExecutionReply*) override {
    executed = true;
    return grpc::Status::OK;
  }
  grpc::Status TerminateStep(
      grpc::ServerContext*,
      const crane::grpc::supervisor::TerminateStepRequest*,
      crane::grpc::supervisor::TerminateStepReply* reply) override {
    terminated = true;
    reply->set_ok(true);
    return grpc::Status::OK;
  }
  std::atomic_bool executed{false};
  std::atomic_bool terminated{false};
  absl::Notification shutdown;
  absl::Notification release;
  std::atomic_bool hold_reply{false};
};

class CtldSessionTest : public ::testing::Test {
 protected:
  void SetUp() override { StartSession(1, 120); }

  void StartSession(int workers, uint32_t timeout) {
    grpc::ServerBuilder builder;
    int port;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                             &port);
    builder.RegisterService(&controller);
    builder.RegisterService(&supervisor);
    server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);

    // One worker exercises the dependency between recovery and the cleanup
    // which the JobManager timer submits back to the pool.
    g_thread_pool = std::make_unique<BS::thread_pool>(workers);
    g_config.CranedConf.CtldTimeoutSec = timeout;
    g_config.CraneCtldForInternalListenPort = std::to_string(port);
    g_config.CranedIdOfThisNode = "session-test";
    g_config.CranedRes["session-test"] = std::make_shared<ResourceInNodeV3>();
    g_runtime_status.conn_logger = std::make_shared<spdlog::async_logger>(
        "session-test", std::make_shared<spdlog::sinks::null_sink_mt>(),
        logger_pool);
    char directory[] = "/tmp/craned-session-XXXXXX";
    ASSERT_NE(mkdtemp(directory), nullptr);
    socket_dir = directory;
    g_config.CranedUnixSockPath = socket_dir / "craned.sock";
    Config::CranedListenConf listen{};
    listen.CranedListenAddr = "127.0.0.1";
    listen.CranedListenPort = "0";
    listen.UnixSocketListenAddr =
        "unix:" + g_config.CranedUnixSockPath.string();
    g_server = std::make_unique<CranedServer>(listen);
    g_server->SetGrpcSrvReady(true);
    g_job_mgr = std::make_unique<JobManager>();
    g_ctld_client = std::make_unique<CtldClient>();
    g_ctld_client->SetCranedId("session-test");
    supervisor_endpoint = "127.0.0.1:" + std::to_string(port);
    g_ctld_client->Init([this](const ConfigureRequest&) { RestoreJob(42); });
    g_ctld_client->InitGrpcChannel("127.0.0.1", "localhost");
    g_ctld_client->StartGrpcCtldConnection();
    ASSERT_TRUE(
        controller.triggered.WaitForNotificationWithTimeout(absl::Seconds(10)));
  }

  void RestoreJob(job_id_t id, StepStatus status = StepStatus::Running) {
    crane::grpc::JobToD job;
    job.set_job_id(id);
    std::unordered_map<job_id_t, JobInD> jobs;
    jobs.emplace(id, JobInD(job));
    crane::grpc::StepToD step;
    step.set_job_id(id);
    step.add_gids(1000);
    step.set_step_id(kDaemonStepId);
    step.set_step_type(crane::grpc::DAEMON);
    auto stub = std::make_shared<SupervisorStub>(supervisor_endpoint);
    absl::flat_hash_map<StepKey, std::unique_ptr<StepInstance>> steps;
    steps.emplace(
        StepKey{id, kDaemonStepId},
        std::make_unique<StepInstance>(step, std::numeric_limits<pid_t>::max(),
                                       status, std::move(stub)));
    g_job_mgr->Recover(std::move(jobs), std::move(steps));
  }

  void TearDown() override {
    if (!server) return;
    if (!supervisor.release.HasBeenNotified()) supervisor.release.Notify();
    if (!controller.release_register.HasBeenNotified())
      controller.release_register.Notify();
    g_ctld_client->StopHandshakes().wait();
    g_job_mgr->DrainAndStop();
    g_ctld_client->Shutdown();
    g_server->Shutdown();
    g_server->Wait();
    g_server.reset();
    std::filesystem::remove_all(socket_dir);
    g_ctld_client.reset();
    g_job_mgr.reset();
    g_thread_pool.reset();
    g_runtime_status.conn_logger.reset();
    server->Shutdown();
    g_config.CranedRes.erase("session-test");
  }

  std::string supervisor_endpoint;
  std::filesystem::path socket_dir;
  TestSupervisor supervisor;
  TestController controller;
  std::unique_ptr<grpc::Server> server;
  std::shared_ptr<spdlog::details::thread_pool> logger_pool =
      std::make_shared<spdlog::details::thread_pool>(128, 1);
};

class ReconnectTest : public CtldSessionTest {
 protected:
  // A blocked supervisor RPC leaves a second worker for the new handshake.
  void SetUp() override { StartSession(2, 1); }
};

TEST_F(ReconnectTest, SupersededCleanupPreservesJobInNewSnapshot) {
  RestoreJob(43);
  auto old = controller.config;
  (*(*old.mutable_job_steps())[42].mutable_step_status())[1] =
      StepStatus::Running;
  supervisor.hold_reply = true;
  ASSERT_TRUE(g_ctld_client->Configure(old));
  ASSERT_TRUE(
      supervisor.shutdown.WaitForNotificationWithTimeout(absl::Seconds(5)));
  ASSERT_TRUE(
      controller.retried.WaitForNotificationWithTimeout(absl::Seconds(5)));
  auto replacement = controller.retry_config;
  (*(*replacement.mutable_job_steps())[43]
        .mutable_step_status())[kDaemonStepId] = StepStatus::Running;
  ASSERT_TRUE(g_ctld_client->Configure(replacement));
  supervisor.release.Notify();
  ASSERT_TRUE(
      controller.registered.WaitForNotificationWithTimeout(absl::Seconds(5)));
  EXPECT_TRUE(g_job_mgr->GetAllocatedJobSteps().contains(43));
}

TEST_F(ReconnectTest, FailedRefreezeRequiresNewConfiguration) {
  auto suspended = controller.config;
  (*(*suspended.mutable_job_steps())[42].mutable_step_status())[kDaemonStepId] =
      StepStatus::Suspended;
  // The recovered job has no cgroup, so the requested suspend cannot succeed.
  ASSERT_TRUE(g_ctld_client->Configure(suspended));
  ASSERT_TRUE(
      controller.retried.WaitForNotificationWithTimeout(absl::Seconds(5)));
  EXPECT_FALSE(controller.registered.HasBeenNotified());
  auto running = controller.retry_config;
  (*(*running.mutable_job_steps())[42].mutable_step_status())[kDaemonStepId] =
      StepStatus::Running;
  ASSERT_TRUE(g_ctld_client->Configure(running));
  ASSERT_TRUE(
      controller.registered.WaitForNotificationWithTimeout(absl::Seconds(5)));
}

TEST_F(CtldSessionTest, RemovesInvalidJobBeforeRegisteringWithOneWorker) {
  auto stale = controller.config;
  stale.mutable_token()->set_seconds(stale.token().seconds() + 1);
  EXPECT_FALSE(g_ctld_client->Configure(stale));
  ASSERT_TRUE(g_ctld_client->Configure(controller.config));
  ASSERT_TRUE(
      controller.registered.WaitForNotificationWithTimeout(absl::Seconds(10)));
  EXPECT_TRUE(controller.cleaned_at_register);
}

TEST_F(CtldSessionTest, JoinsRegistrationAlreadyInFlightOnStop) {
  controller.hold_register_reply = true;
  ASSERT_TRUE(g_ctld_client->Configure(controller.config));
  ASSERT_TRUE(
      controller.registered.WaitForNotificationWithTimeout(absl::Seconds(10)));
  kill(getpid(), SIGTERM);
  g_job_mgr->WaitForStopRequest();
  const auto& actions = g_ctld_client->StopHandshakes();
  EXPECT_LT(actions.ready_count(), actions.size());
  controller.release_register.Notify();
  actions.wait();
  EXPECT_FALSE(g_ctld_client->Configure(controller.config));
}

TEST_F(CtldSessionTest, DrainsAcceptedRecoveryAndSendsLastStatusOnStop) {
  absl::Notification release_worker;
  auto blocker =
      g_thread_pool->submit_task([&] { release_worker.WaitForNotification(); });
  EXPECT_TRUE(g_ctld_client->Configure(controller.config));
  kill(getpid(), SIGTERM);
  g_job_mgr->WaitForStopRequest();
  const auto& actions = g_ctld_client->StopHandshakes();
  EXPECT_LT(actions.ready_count(), actions.size());
  release_worker.Notify();
  blocker.get();
  actions.wait();
  EXPECT_FALSE(g_ctld_client->Configure(controller.config));
  EXPECT_TRUE(g_job_mgr->GetAllocatedJobSteps().empty());
  g_job_mgr->StepStatusChangeAsync(42, 7, StepStatus::Completed, 0,
                                   std::nullopt, std::nullopt, {});
  g_job_mgr->DrainAndStop();
  auto late = g_job_mgr->FreeSteps({{42, {7}}});
  EXPECT_EQ(late.at(42).at(7).get(), CraneErrCode::ERR_SHUTTING_DOWN);
  g_ctld_client->Shutdown();
  EXPECT_EQ(controller.last_step.load(), 7);
}

TEST_F(CtldSessionTest, ReceivesFinalSupervisorStatusWhileDrainingWork) {
  auto request = controller.config;
  (*(*request.mutable_job_steps())[42].mutable_step_status())[kDaemonStepId] =
      StepStatus::Running;
  ASSERT_TRUE(g_ctld_client->Configure(request));
  ASSERT_TRUE(
      controller.registered.WaitForNotificationWithTimeout(absl::Seconds(10)));
  supervisor.hold_reply = true;
  auto cleanup = g_job_mgr->FreeSteps({{42, {kDaemonStepId}}});
  ASSERT_TRUE(
      supervisor.shutdown.WaitForNotificationWithTimeout(absl::Seconds(5)));
  kill(getpid(), SIGTERM);
  g_job_mgr->WaitForStopRequest();
  g_server->CloseWorkAdmission();
  g_ctld_client->StopHandshakes().wait();
  auto drain =
      std::async(std::launch::async, [] { g_job_mgr->DrainAndStop(); });
  CranedServiceImpl receiver;
  grpc::ServerContext context;
  google::protobuf::Empty ignored;
  EXPECT_EQ(receiver.Configure(&context, &request, &ignored).error_code(),
            grpc::StatusCode::UNAVAILABLE);
  crane::grpc::StepStatusChangeRequest report;
  report.set_job_id(42);
  report.set_step_id(kDaemonStepId);
  report.set_new_status(StepStatus::Completing);
  report.set_final_status(StepStatus::Completed);
  report.set_exit_code(0);
  crane::grpc::StepStatusChangeReply ack;
  EXPECT_TRUE(receiver.StepStatusChange(&context, &report, &ack).ok());
  EXPECT_TRUE(ack.ok());
  supervisor.release.Notify();
  drain.get();
  EXPECT_EQ(cleanup.at(42).at(kDaemonStepId).get(), CraneErrCode::SUCCESS);
  EXPECT_EQ(receiver.StepStatusChange(&context, &report, &ack).error_code(),
            grpc::StatusCode::UNAVAILABLE);
  g_ctld_client->Shutdown();
  EXPECT_EQ(controller.last_status.load(), StepStatus::Completed);
  EXPECT_EQ(controller.last_code.load(), 0U);
}

TEST_F(CtldSessionTest, DrainsAcceptedExecutionAndDeferredTermination) {
  RestoreJob(44, StepStatus::Starting);
  RestoreJob(45, StepStatus::Configuring);
  ASSERT_EQ(g_job_mgr->ExecuteStepAsync({{44, {kDaemonStepId}}}),
            CraneErrCode::SUCCESS);
  g_job_mgr->TerminateStepAsync(45, kDaemonStepId,
                                crane::grpc::TERMINATE_SOURCE_USER_CANCEL);
  kill(getpid(), SIGTERM);
  g_job_mgr->WaitForStopRequest();
  g_server->CloseWorkAdmission();
  g_ctld_client->StopHandshakes().wait();
  g_job_mgr->DrainAndStop();
  EXPECT_TRUE(supervisor.executed.load());
  EXPECT_TRUE(supervisor.terminated.load());
}

TEST_F(CtldSessionTest, ProcessesSignalsWhileSupervisorRpcIsPending) {
  supervisor.hold_reply = true;
  ASSERT_TRUE(g_ctld_client->Configure(controller.config));
  ASSERT_TRUE(
      supervisor.shutdown.WaitForNotificationWithTimeout(absl::Seconds(5)));
  kill(getpid(), SIGTERM);
  auto observed =
      std::async(std::launch::async, [] { g_job_mgr->WaitForStopRequest(); });
  EXPECT_EQ(observed.wait_for(2s), std::future_status::ready);
  supervisor.release.Notify();
  observed.get();
}

}  // namespace
}  // namespace Craned

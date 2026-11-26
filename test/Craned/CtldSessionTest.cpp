// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include "cri/api.pb.h"
// CRI signal enum names must precede system signal macros.

#include <gtest/gtest.h>

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
    config.set_ok(true);
    *config.mutable_token() = req->token();
    triggered.Notify();
    return grpc::Status::OK;
  }

  grpc::Status CranedRegister(grpc::ServerContext*,
                              const crane::grpc::CranedRegisterRequest*,
                              crane::grpc::CranedRegisterReply*) override {
    cleaned_at_register = g_job_mgr->GetAllocatedJobSteps().empty();
    registered.Notify();
    if (hold_register_reply.load()) release_register.WaitForNotification();
    return {grpc::StatusCode::UNAVAILABLE, "registration reply unavailable"};
  }

  grpc::Status StepStatusChange(
      grpc::ServerContext*, const crane::grpc::StepStatusChangeRequest* req,
      crane::grpc::StepStatusChangeReply* reply) override {
    last_step = req->step_id();
    reply->set_ok(true);
    return grpc::Status::OK;
  }

  ConfigureRequest config;
  absl::Notification triggered;
  absl::Notification registered;
  absl::Notification release_register;
  std::atomic_bool hold_register_reply{false};
  bool cleaned_at_register = false;
  std::atomic<step_id_t> last_step{0};
};

class CtldSessionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    grpc::ServerBuilder builder;
    int port;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                             &port);
    builder.RegisterService(&controller);
    server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);

    // One worker exercises the dependency between recovery and the cleanup
    // which the JobManager timer submits back to the pool.
    g_thread_pool = std::make_unique<BS::thread_pool>(1);
    g_config.CranedConf.CtldTimeoutSec = 120;
    g_config.CraneCtldForInternalListenPort = std::to_string(port);
    g_config.CranedIdOfThisNode = "session-test";
    g_config.CranedRes["session-test"] = std::make_shared<ResourceInNodeV3>();
    g_runtime_status.conn_logger = std::make_shared<spdlog::async_logger>(
        "session-test", std::make_shared<spdlog::sinks::null_sink_mt>(),
        logger_pool);
    g_job_mgr = std::make_unique<JobManager>();
    g_ctld_client = std::make_unique<CtldClient>();
    g_ctld_client->SetCranedId("session-test");
    g_ctld_client->Init([](const ConfigureRequest&) {
      crane::grpc::JobToD job;
      job.set_job_id(42);
      std::unordered_map<job_id_t, JobInD> jobs;
      jobs.emplace(42, JobInD(job));
      crane::grpc::StepToD step;
      step.set_job_id(42);
      step.add_gids(1000);
      step.set_step_id(kDaemonStepId);
      step.set_step_type(crane::grpc::DAEMON);
      auto instance = std::make_unique<StepInstance>(step);
      instance->err_before_supv_start = true;
      absl::flat_hash_map<std::pair<job_id_t, step_id_t>,
                          std::unique_ptr<StepInstance>>
          steps;
      steps.emplace(std::make_pair(42, kDaemonStepId), std::move(instance));
      g_job_mgr->Recover(std::move(jobs), std::move(steps));
    });
    g_ctld_client->InitGrpcChannel("127.0.0.1", "localhost");
    g_ctld_client->StartGrpcCtldConnection();
    ASSERT_TRUE(
        controller.triggered.WaitForNotificationWithTimeout(absl::Seconds(10)));
  }

  void TearDown() override {
    if (!server) return;
    if (!controller.release_register.HasBeenNotified())
      controller.release_register.Notify();
    g_ctld_client->Quiesce();
    g_job_mgr->DrainAndStop();
    g_ctld_client->Shutdown();
    g_ctld_client.reset();
    g_job_mgr.reset();
    g_thread_pool.reset();
    g_runtime_status.conn_logger.reset();
    server->Shutdown();
    g_config.CranedRes.erase("session-test");
  }

  TestController controller;
  std::unique_ptr<grpc::Server> server;
  std::shared_ptr<spdlog::details::thread_pool> logger_pool =
      std::make_shared<spdlog::details::thread_pool>(128, 1);
};

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
  absl::Notification entering_drain;
  auto drained = std::async(std::launch::async, [&] {
    entering_drain.Notify();
    g_ctld_client->Quiesce();
  });
  entering_drain.WaitForNotification();
  EXPECT_EQ(drained.wait_for(20ms), std::future_status::timeout);
  controller.release_register.Notify();
  drained.get();
  EXPECT_FALSE(g_ctld_client->Configure(controller.config));
}

TEST_F(CtldSessionTest, DrainsAcceptedRecoveryAndSendsLastStatusOnStop) {
  absl::Notification release_worker;
  auto blocker =
      g_thread_pool->submit_task([&] { release_worker.WaitForNotification(); });
  EXPECT_TRUE(g_ctld_client->Configure(controller.config));
  kill(getpid(), SIGTERM);
  g_job_mgr->WaitForStopRequest();
  auto drained =
      std::async(std::launch::async, [] { g_ctld_client->Quiesce(); });
  EXPECT_TRUE(drained.wait_for(0s) == std::future_status::timeout);
  release_worker.Notify();
  blocker.get();
  drained.get();
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

}  // namespace
}  // namespace Craned

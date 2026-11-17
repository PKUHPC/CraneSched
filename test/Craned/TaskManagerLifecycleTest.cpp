#include "../../src/Craned/Supervisor/SupervisorPublicDefs.h"
// Precompiled header comes first.

#include <gtest/gtest.h>

#include <condition_variable>

#include "../../src/Craned/Supervisor/CranedClient.h"
#include "../../src/Craned/Supervisor/TaskManager.h"

namespace Craned::Supervisor {
namespace {
using namespace std::chrono_literals;

class TestCraned final : public crane::grpc::Craned::Service {
 public:
  grpc::Status StepStatusChange(
      grpc::ServerContext*, const crane::grpc::StepStatusChangeRequest* request,
      crane::grpc::StepStatusChangeReply* reply) override {
    {
      std::lock_guard lock(mutex);
      changes.push_back(*request);
    }
    changed.notify_all();
    reply->set_ok(true);
    return grpc::Status::OK;
  }
  std::mutex mutex;
  std::condition_variable changed;
  std::vector<crane::grpc::StepStatusChangeRequest> changes;
};
}  // namespace

class TaskManagerLifecycleTest : public testing::Test {
 protected:
  TestCraned craned;
  std::unique_ptr<grpc::Server> server;
  std::filesystem::path temp_dir;

  void SetUp() override {
    g_config.StepSpec.Clear();
    g_config.JobLifecycleHook = {};
    g_config.JobId = 123;
    g_config.StepId = 1;
    g_config.StepSpec.set_job_id(123);
    g_config.StepSpec.set_step_id(1);
    g_config.StepSpec.set_step_type(crane::grpc::COMMON);
    g_config.StepSpec.set_type(crane::grpc::Batch);
    char path[] = "/tmp/crane-task-lifecycle-XXXXXX";
    ASSERT_NE(mkdtemp(path), nullptr);
    temp_dir = path;
    const auto endpoint = "unix://" + (temp_dir / "craned.sock").string();
    grpc::ServerBuilder builder;
    builder.AddListeningPort(endpoint, grpc::InsecureServerCredentials());
    builder.RegisterService(&craned);
    server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);
    g_craned_client = std::make_unique<CranedClient>();
    g_craned_client->InitChannelAndStub(endpoint);
    g_task_mgr.reset(new TaskManager(false));
    g_task_mgr->m_step_.GotNewStatus(StepStatus::Starting);
  }

  void TearDown() override {
    if (g_task_mgr) {
      // Inspect the shutdown request without executing process-wide shutdown.
      g_task_mgr->m_uvw_loop_->walk([](auto& handle) { handle.close(); });
      g_task_mgr->m_uvw_loop_->run();
      g_task_mgr.reset();
    }
    if (g_craned_client) {
      g_craned_client->Shutdown();
      g_craned_client.reset();
    }
    if (server) server->Shutdown();
    server.reset();
    std::filesystem::remove_all(temp_dir);
  }

  void ExpectStartingTermination(TaskFinalizeCause cause, StepStatus expected) {
    g_task_mgr->TerminateStepAsync(cause);
    g_task_mgr->EvCleanTerminateStepQueueCb_();
    EXPECT_EQ(g_task_mgr->GetStepStatus(), StepStatus::Completing);
    const auto final = g_task_mgr->GetFinalTerminationStatus();
    EXPECT_EQ(final.final_status_on_termination, expected);
    EXPECT_EQ(final.max_exit_code, 0);

    std::tuple<StepStatus, uint32_t, std::string> shutdown;
    ASSERT_TRUE(g_task_mgr->m_shutdown_status_queue_.try_dequeue(shutdown));
    EXPECT_EQ(std::get<0>(shutdown), expected);
    EXPECT_EQ(std::get<1>(shutdown), 0);
    EXPECT_FALSE(g_task_mgr->m_shutdown_status_queue_.try_dequeue(shutdown));

    std::unique_lock lock(craned.mutex);
    ASSERT_TRUE(craned.changed.wait_for(
        lock, 5s, [&] { return !craned.changes.empty(); }));
    ASSERT_EQ(craned.changes.size(), 1);
    EXPECT_EQ(craned.changes.front().new_status(), StepStatus::Completing);
    EXPECT_TRUE(craned.changes.front().has_final_status());
    EXPECT_EQ(craned.changes.front().final_status(), expected);
    EXPECT_EQ(craned.changes.front().exit_code(), 0);
  }
};

TEST_F(TaskManagerLifecycleTest, FreeBeforeExecuteReportsCompletion) {
  ExpectStartingTermination(TaskFinalizeCause::NORMAL, StepStatus::Completed);
}

TEST_F(TaskManagerLifecycleTest, CancelBeforeExecuteKeepsCancelledResult) {
  ExpectStartingTermination(TaskFinalizeCause::CANCELLED_BY_USER,
                            StepStatus::Cancelled);
}
}  // namespace Craned::Supervisor

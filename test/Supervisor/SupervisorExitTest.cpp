#include <gtest/gtest.h>

#include <csignal>

#include "TaskManager.h"

namespace Craned::Supervisor {

class SupervisorExitTestPeer {
 public:
  static StepInstance& Step(TaskManager& manager) { return manager.m_step_; }

  static void Stop(CforedClient& client) {
    client.m_stopped_ = true;
    client.m_ev_thread_.join();
  }

  static void Clear(CforedClient& client) {
    absl::MutexLock lock(&client.m_mtx_);
    client.m_fwd_meta_map.clear();
  }

  static void AddTask(TaskManager& manager, std::unique_ptr<ProcInstance> task,
                      pid_t pid) {
    const auto task_id = task->task_id;
    task->m_pid_ = pid;
    auto meta = std::make_unique<CrunInstanceMeta>();
    meta->stdin_write = -1;
    task->m_meta_ = std::move(meta);
    manager.m_exec_id_task_id_map_.emplace(pid, task_id);
    manager.m_step_.AddTaskInstance(task_id, std::move(task));
    SetEof(*manager.m_step_.GetCforedClient(), task_id, false, false);
  }

  static void ProcessExit(TaskManager& manager, pid_t pid, int status) {
    manager.m_sigchld_queue_.enqueue({pid, status});
    manager.EvCleanSigchldQueueCb_();
  }

  static void PollExits(TaskManager& manager) {
    manager.EvCleanSigchldQueueCb_();
  }

  static void SetEof(CforedClient& client, task_id_t task_id, bool stdout_eof,
                     bool stderr_eof) {
    absl::MutexLock lock(&client.m_mtx_);
    auto& meta = client.m_fwd_meta_map[task_id];
    meta.task_id = task_id;
    meta.output_stopped = stdout_eof;
    meta.err_stopped = stderr_eof;
  }

  static void ProcessOutputEof(CforedClient& client, task_id_t task_id,
                               bool is_stdout) {
    client.HandleOutputStop_(task_id, is_stdout);
    client.CleanStopTaskIOQueueCb_();
  }

  static void EnqueueOutput(CforedClient& client, task_id_t task_id,
                            bool is_stdout, std::string_view output) {
    auto data = std::make_unique<char[]>(output.size());
    std::ranges::copy(output, data.get());
    if (is_stdout) {
      client.TaskOutPutForward(task_id, std::move(data), output.size());
    } else {
      client.TaskErrOutPutForward(std::move(data), output.size());
    }
  }

  static std::vector<crane::grpc::StreamStepIORequest::SupervisorRequestType>
  DrainRequests(CforedClient& client) {
    std::vector<crane::grpc::StreamStepIORequest::SupervisorRequestType> types;
    CforedClient::FwdRequest request;
    while (client.m_task_fwd_req_queue_.try_dequeue(request)) {
      types.push_back(request.type);
    }
    return types;
  }
};

namespace {

using Common::CgConstant::CgroupVersion;
using crane::grpc::StreamStepIORequest;

class RecordingProcess : public ProcInstance {
 public:
  using ProcInstance::ProcInstance;

  CraneErrCode Kill(int signal_number) override {
    signals.push_back(signal_number);
    return succeeds ? CraneErrCode::SUCCESS : CraneErrCode::ERR_GENERIC_FAILURE;
  }

  bool succeeds{true};
  std::vector<int> signals;
};

class RecordingCgroup : public Common::CgroupInterface {
 public:
  RecordingCgroup()
      : Common::CgroupInterface("supervisor-exit-test", nullptr) {}

  bool SetCpuCoreLimit(double) override { return false; }
  bool SetCpuShares(uint64_t) override { return false; }
  bool SetCpuSet(const std::unordered_set<uint32_t>&) override { return false; }
  bool SetCpusetMems(const std::string&) override { return false; }
  bool SetMemoryLimitBytes(uint64_t) override { return false; }
  bool SetMemorySwLimitBytes(uint64_t) override { return false; }
  bool SetMemorySoftLimitBytes(uint64_t) override { return false; }
  bool SetBlockioWeight(uint64_t) override { return false; }
  bool SetDeviceAccess(const std::unordered_set<SlotId>&, bool, bool,
                       bool) override {
    return false;
  }
  bool Empty() override { return false; }

  bool KillAllProcesses(int signal_number) override {
    signals.push_back(signal_number);
    return succeeds;
  }

  bool succeeds{true};
  std::vector<int> signals;
};

class SupervisorExitTest : public testing::Test {
 protected:
  void SetUp() override {
    m_previous_step_ = g_config.StepSpec;
    m_previous_version_ = CgroupManager::GetCgroupVersion();
    g_config.StepSpec.Clear();
    g_config.StepSpec.set_type(crane::grpc::Interactive);
    g_config.StepSpec.mutable_interactive_meta()->set_interactive_type(
        crane::grpc::Crun);
    g_task_mgr = std::make_unique<TaskManager>();
    m_manager_ = g_task_mgr.get();
    m_manager_->Shutdown();
    m_manager_->Wait();
    Step().InitCforedClient();
    SupervisorExitTestPeer::Stop(Client());
  }

  void TearDown() override {
    SupervisorExitTestPeer::Clear(Client());
    g_task_mgr.reset();
    m_manager_ = nullptr;
    g_config.StepSpec = m_previous_step_;
    CgroupManager::SetCgroupVersion(m_previous_version_);
  }

  StepInstance& Step() { return SupervisorExitTestPeer::Step(*m_manager_); }
  CforedClient& Client() { return *Step().GetCforedClient(); }

  RecordingProcess& AddTask(task_id_t task_id, pid_t pid) {
    auto task = std::make_unique<RecordingProcess>(&Step(), task_id);
    auto& result = *task;
    SupervisorExitTestPeer::AddTask(*m_manager_, std::move(task), pid);
    return result;
  }

  RecordingCgroup& AddCgroup() {
    auto cgroup = std::make_unique<RecordingCgroup>();
    auto& result = *cgroup;
    Step().step_user_cg = std::move(cgroup);
    CgroupManager::SetCgroupVersion(CgroupVersion::CGROUP_V2);
    return result;
  }

  TaskManager* m_manager_{nullptr};

 private:
  StepToSupv m_previous_step_;
  CgroupVersion m_previous_version_;
};

TEST_F(SupervisorExitTest, KillsStepCgroupOnceAfterLastProcessExitBeforeEof) {
  auto& first = AddTask(0, 900001);
  auto& second = AddTask(1, 900002);
  auto& cgroup = AddCgroup();

  SupervisorExitTestPeer::ProcessExit(*m_manager_, 900001, 7 << 8);
  EXPECT_FALSE(Step().AllTaskProcessesExited());
  EXPECT_TRUE(cgroup.signals.empty());

  SupervisorExitTestPeer::ProcessExit(*m_manager_, 900002, SIGTERM);
  EXPECT_TRUE(Step().AllTaskProcessesExited());
  EXPECT_FALSE(Step().AllTaskFinished());
  EXPECT_EQ(cgroup.signals, std::vector<int>{SIGKILL});
  EXPECT_TRUE(first.signals.empty());
  EXPECT_TRUE(second.signals.empty());
  EXPECT_TRUE(SupervisorExitTestPeer::DrainRequests(Client()).empty());
  ASSERT_TRUE(first.GetFinalInfo()->raw_exit.has_value());
  EXPECT_EQ(first.GetFinalInfo()->raw_exit->value, 7);
  ASSERT_TRUE(second.GetFinalInfo()->raw_exit.has_value());
  EXPECT_EQ(second.GetFinalInfo()->raw_exit->value, SIGTERM);

  SupervisorExitTestPeer::PollExits(*m_manager_);
  EXPECT_EQ(cgroup.signals.size(), 1);
}

TEST_F(SupervisorExitTest, FallsBackToTaskProcessesWhenCgroupKillFails) {
  auto& task = AddTask(0, 900001);
  auto& cgroup = AddCgroup();
  cgroup.succeeds = false;

  SupervisorExitTestPeer::ProcessExit(*m_manager_, 900001, 0);
  EXPECT_EQ(cgroup.signals, std::vector<int>{SIGKILL});
  EXPECT_EQ(task.signals, std::vector<int>{SIGKILL});
  SupervisorExitTestPeer::PollExits(*m_manager_);
  EXPECT_EQ(task.signals.size(), 1);
}

TEST_F(SupervisorExitTest, RetriesResidualCleanupAfterKillFailure) {
  auto& task = AddTask(0, 900001);
  auto& cgroup = AddCgroup();
  cgroup.succeeds = false;
  task.succeeds = false;

  SupervisorExitTestPeer::ProcessExit(*m_manager_, 900001, 0);
  EXPECT_EQ(task.signals, std::vector<int>{SIGKILL});

  task.succeeds = true;
  SupervisorExitTestPeer::PollExits(*m_manager_);
  EXPECT_EQ(task.signals, (std::vector<int>{SIGKILL, SIGKILL}));
}

TEST_F(SupervisorExitTest, UsesTaskProcessGroupsForCgroupV1) {
  auto& task = AddTask(0, 900001);
  auto& cgroup = AddCgroup();
  CgroupManager::SetCgroupVersion(CgroupVersion::CGROUP_V1);

  SupervisorExitTestPeer::ProcessExit(*m_manager_, 900001, 0);
  EXPECT_TRUE(cgroup.signals.empty());
  EXPECT_EQ(task.signals, std::vector<int>{SIGKILL});
}

TEST_F(SupervisorExitTest, UsesTaskProcessGroupsWhenStepCgroupIsMissing) {
  auto& task = AddTask(0, 900001);
  CgroupManager::SetCgroupVersion(CgroupVersion::CGROUP_V2);

  SupervisorExitTestPeer::ProcessExit(*m_manager_, 900001, 0);
  EXPECT_EQ(task.signals, std::vector<int>{SIGKILL});
}

TEST_F(SupervisorExitTest, EmptyStepDoesNotTriggerCleanup) {
  auto& cgroup = AddCgroup();
  SupervisorExitTestPeer::PollExits(*m_manager_);
  EXPECT_TRUE(cgroup.signals.empty());
}

TEST_F(SupervisorExitTest, NonCrunStepDoesNotTriggerCleanup) {
  auto& task = AddTask(0, 900001);
  auto& cgroup = AddCgroup();
  task.GetFinalInfo()->raw_exit = TaskExitInfo{};
  Step().interactive_type = crane::grpc::Calloc;

  SupervisorExitTestPeer::PollExits(*m_manager_);
  EXPECT_TRUE(cgroup.signals.empty());
  EXPECT_TRUE(task.signals.empty());
}

TEST_F(SupervisorExitTest, ProcessExitWaitsForStderrAfterStdoutEof) {
  SupervisorExitTestPeer::SetEof(Client(), 0, true, false);
  EXPECT_FALSE(Client().TaskProcessStop(0, 7, false));
  EXPECT_TRUE(SupervisorExitTestPeer::DrainRequests(Client()).empty());
}

TEST_F(SupervisorExitTest, ProcessExitWaitsForStdoutAfterStderrEof) {
  SupervisorExitTestPeer::SetEof(Client(), 0, false, true);
  EXPECT_FALSE(Client().TaskProcessStop(0, 7, false));
  EXPECT_TRUE(SupervisorExitTestPeer::DrainRequests(Client()).empty());
}

TEST_F(SupervisorExitTest, ProcessExitWaitsForBothStreams) {
  SupervisorExitTestPeer::SetEof(Client(), 0, false, false);
  EXPECT_FALSE(Client().TaskProcessStop(0, 7, false));
  EXPECT_TRUE(SupervisorExitTestPeer::DrainRequests(Client()).empty());
}

TEST_F(SupervisorExitTest, ExitStatusFollowsBufferedStdoutAndStderr) {
  SupervisorExitTestPeer::SetEof(Client(), 0, true, true);
  SupervisorExitTestPeer::EnqueueOutput(Client(), 0, true, "stdout tail");
  SupervisorExitTestPeer::EnqueueOutput(Client(), 0, false, "stderr tail");

  EXPECT_TRUE(Client().TaskProcessStop(0, 7, false));
  EXPECT_EQ(SupervisorExitTestPeer::DrainRequests(Client()),
            (std::vector{StreamStepIORequest::TASK_OUTPUT,
                         StreamStepIORequest::TASK_ERR_OUTPUT,
                         StreamStepIORequest::TASK_EXIT_STATUS}));
}

TEST_F(SupervisorExitTest, FinalEofSendsDeferredExitStatusAfterProcessExit) {
  SupervisorExitTestPeer::SetEof(Client(), 0, false, false);
  EXPECT_FALSE(Client().TaskProcessStop(0, 7, false));
  EXPECT_TRUE(SupervisorExitTestPeer::DrainRequests(Client()).empty());

  SupervisorExitTestPeer::ProcessOutputEof(Client(), 0, true);
  EXPECT_TRUE(SupervisorExitTestPeer::DrainRequests(Client()).empty());

  SupervisorExitTestPeer::ProcessOutputEof(Client(), 0, false);
  EXPECT_EQ(SupervisorExitTestPeer::DrainRequests(Client()),
            (std::vector{StreamStepIORequest::TASK_EXIT_STATUS}));
}

}  // namespace
}  // namespace Craned::Supervisor

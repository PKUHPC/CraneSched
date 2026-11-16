#include "CranedPublicDefs.h"
// Precompiled header comes first.

#include <fcntl.h>
#include <gtest/gtest.h>
#include <poll.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <fstream>

#include "CtldClient.h"
#include "JobManager.h"

namespace Craned {
namespace {
using namespace std::chrono_literals;

// Replace the controller transport and cgroup operations. Tests run the real
// JobManager queues, initialization worker, Prolog runner and cleanup path.
std::mutex reported_statuses_mutex;
std::vector<StepStatusChangeQueueElem> reported_statuses;

std::vector<StepStatusChangeQueueElem> ReportedStatuses() {
  std::lock_guard lock(reported_statuses_mutex);
  return reported_statuses;
}

class TestCgroup final : public CgroupInterface {
 public:
  explicit TestCgroup(std::function<void()> destroy = {})
      : CgroupInterface("crane_lifecycle_test_nonexistent", nullptr),
        m_destroy_(std::move(destroy)) {}
  bool SetCpuCoreLimit(double) override { return true; }
  bool SetCpuShares(uint64_t) override { return true; }
  bool SetCpuSet(const std::unordered_set<uint32_t>&) override { return true; }
  bool SetCpusetMems(const std::string&) override { return true; }
  bool SetMemoryLimitBytes(uint64_t) override { return true; }
  bool SetMemorySwLimitBytes(uint64_t) override { return true; }
  bool SetMemorySoftLimitBytes(uint64_t) override { return true; }
  bool SetBlockioWeight(uint64_t) override { return true; }
  bool SetDeviceAccess(const std::unordered_set<SlotId>&, bool, bool,
                       bool) override {
    return true;
  }
  bool KillAllProcesses(int) override { return true; }
  bool Empty() override { return true; }
  void Destroy() override {
    if (m_destroy_) m_destroy_();
  }

 private:
  std::function<void()> m_destroy_;
};
}  // namespace

// No network threads are started by this test transport.
CtldClient::CtldClient() = default;
CtldClient::~CtldClient() = default;
CtldClientStateMachine::~CtldClientStateMachine() = default;
void CtldClient::StepStatusChangeAsync(StepStatusChangeQueueElem&& change) {
  std::lock_guard lock(reported_statuses_mutex);
  reported_statuses.push_back(std::move(change));
}
void CtldClient::UpdateNodeDrainState(bool, const std::string&) {}

class JobManagerLifecycleTest : public testing::Test {
 protected:
  static constexpr job_id_t kJob = 123;
  std::unique_ptr<JobManager> manager;
  std::filesystem::path temp_dir;
  int started_fd{-1};
  int release_fd{-1};
  std::atomic_int job_destroyed{0};

  void SetUp() override {
    g_config = Config{};
    g_config.CranedIdOfThisNode = "test-node";
    reported_statuses.clear();
    g_thread_pool = std::make_unique<BS::thread_pool>(2);
    g_ctld_client = std::make_unique<CtldClient>();
    // Drive the real callbacks explicitly, so queue ordering is deterministic.
    manager.reset(new JobManager(false));
    char path[] = "/tmp/crane-step-lifecycle-XXXXXX";
    ASSERT_NE(mkdtemp(path), nullptr);
    temp_dir = path;
  }

  void TearDown() override {
    ReleaseProlog();
    EXPECT_TRUE(
        DriveUntil([] { return g_thread_pool->get_tasks_total() == 0; }));
    g_thread_pool->wait();
    manager->m_uvw_loop_->walk([](auto& handle) { handle.close(); });
    manager->m_uvw_loop_->run();
    manager.reset();
    g_ctld_client.reset();
    g_thread_pool.reset();
    if (started_fd >= 0) close(started_fd);
    if (release_fd >= 0) close(release_fd);
    std::filesystem::remove_all(temp_dir);
  }

  StepToD Spec(step_id_t id) {
    StepToD step;
    step.set_job_id(kJob);
    step.set_step_id(id);
    step.set_step_type(id == kDaemonStepId ? crane::grpc::DAEMON
                                           : crane::grpc::COMMON);
    step.set_uid(getuid());
    step.add_gids(getgid());
    step.add_nodelist("test-node");
    step.set_node_num(1);
    step.set_ntasks(1);
    step.set_ntasks_per_node(1);
    step.set_cpus_per_task(1);
    step.set_cwd(temp_dir.string());
    step.set_submit_dir(temp_dir.string());
    return step;
  }

  void AddJob(bool with_daemon = true) {
    crane::grpc::JobToD spec;
    spec.set_job_id(kJob);
    spec.set_uid(getuid());
    JobInD job(spec);
    job.cgroup = std::make_unique<TestCgroup>([this] { ++job_destroyed; });
    if (with_daemon) {
      auto daemon = std::make_unique<StepInstance>(Spec(kDaemonStepId));
      daemon->status = StepStatus::Running;
      daemon->err_before_supv_start = true;
      job.step_map.emplace(kDaemonStepId, std::move(daemon));
    }
    std::vector<JobInD> jobs;
    jobs.push_back(std::move(job));
    crane::grpc::AllocJobsReply reply;
    manager->AllocJobs(std::move(jobs), &reply);
    ASSERT_TRUE(reply.failed_job_ids().empty());
  }

  bool Allocate(step_id_t id, bool container = false) {
    std::vector<StepToD> steps{Spec(id)};
    if (container) steps.front().mutable_container_meta();
    crane::grpc::AllocStepsReply reply;
    manager->AllocSteps(std::move(steps), &reply);
    return reply.failed_job_step_ids_map().empty();
  }

  std::future<CraneErrCode> Free(step_id_t id) {
    auto futures = manager->FreeSteps({{kJob, {id}}});
    return std::move(futures.at(kJob).at(id));
  }

  bool HasStep(step_id_t id) {
    auto job = manager->QueryJob(kJob);
    return job && job->step_map.contains(id);
  }

  void Pump() {
    manager->EvSigchldCb_();
    manager->EvCleanStepStatusChangeQueueCb_();
    manager->EvCleanStepInitializationFinishedQueueCb_();
    manager->EvCleanFreeStepsQueueCb_();
    manager->EvCheckSupervisorRunning_();
  }

  bool DriveUntil(const std::function<bool()>& ready) {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    do {
      Pump();
      if (ready()) return true;
      std::this_thread::yield();
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
  }

  void StartInitialization() { manager->EvCleanGrpcAllocStepsQueueCb_(); }
  void ProcessFree() { manager->EvCleanFreeStepsQueueCb_(); }
  void ProcessCancel() { manager->EvCleanTerminateStepQueueCb_(); }
  void ProcessExecute() { manager->EvCleanGrpcExecuteStepQueueCb_(); }
  void ProcessStatus() { manager->EvCleanStepStatusChangeQueueCb_(); }
  bool InitializationPending(step_id_t id) {
    auto job = manager->QueryJob(kJob);
    return job->step_map.at(id)->initialization_pending;
  }
  void SetStatus(step_id_t id, StepStatus status) {
    auto job = manager->QueryJob(kJob);
    job->step_map.at(id)->status = status;
  }
  EnvMap QueuedEnvironment() {
    JobManager::EvQueueAllocateStepElem elem;
    if (!manager->m_grpc_alloc_step_queue_.try_dequeue(elem)) return {};
    auto env = elem.job_env;
    manager->m_grpc_alloc_step_queue_.enqueue(std::move(elem));
    return env;
  }
  std::future<CraneErrCode> QueueExecute(step_id_t id) {
    JobManager::EvQueueExecuteStepElem elem{.job_id = kJob, .step_id = id};
    auto future = elem.ok_prom.get_future();
    manager->m_grpc_execute_step_queue_.enqueue(std::move(elem));
    return future;
  }
  size_t PendingCancels() {
    return manager->m_step_terminate_queue_.size_approx();
  }

  void BlockProlog(int exit_code = 0) {
    const auto started = temp_dir / "started";
    const auto release = temp_dir / "release";
    ASSERT_EQ(mkfifo(started.c_str(), 0600), 0);
    ASSERT_EQ(mkfifo(release.c_str(), 0600), 0);
    started_fd = open(started.c_str(), O_RDWR | O_NONBLOCK | O_CLOEXEC);
    release_fd = open(release.c_str(), O_RDWR | O_NONBLOCK | O_CLOEXEC);
    ASSERT_GE(started_fd, 0);
    ASSERT_GE(release_fd, 0);
    const auto script = temp_dir / "prolog.sh";
    std::ofstream(script) << "#!/bin/sh\nprintf x > '" << started.string()
                          << "'\nread line < '" << release.string()
                          << "'\nexit " << exit_code << "\n";
    ASSERT_EQ(chmod(script.c_str(), 0700), 0);
    g_config.JobLifecycleHook.Prologs = {script.string()};
    g_config.JobLifecycleHook.PrologTimeout = 10;
  }

  bool PrologEntered() {
    pollfd fd{.fd = started_fd, .events = POLLIN};
    return poll(&fd, 1, 5000) == 1 && (fd.revents & POLLIN);
  }
  void ReleaseProlog() {
    if (release_fd >= 0) EXPECT_EQ(write(release_fd, "go\n", 3), 3);
  }
};

TEST_F(JobManagerLifecycleTest, AcceptedAllocationIsVisibleBeforeWorkerStarts) {
  AddJob();
  ASSERT_TRUE(Allocate(1));
  EXPECT_EQ(manager->GetAllocatedJobSteps().at(kJob).at(1),
            StepStatus::Configuring);
  EXPECT_FALSE(Allocate(1));
  auto freed = Free(1);
  ProcessFree();
  EXPECT_TRUE(HasStep(1));
  EXPECT_EQ(freed.wait_for(0s), std::future_status::timeout);
  StartInitialization();
  ASSERT_TRUE(DriveUntil(
      [&] { return freed.wait_for(0s) == std::future_status::ready; }));
  EXPECT_EQ(freed.get(), CraneErrCode::SUCCESS);
  EXPECT_FALSE(HasStep(1));
  ASSERT_EQ(ReportedStatuses().size(), 1);
  EXPECT_EQ(ReportedStatuses().back().new_status, StepStatus::Completed);
}

TEST_F(JobManagerLifecycleTest, FreeDuringPrologWaitsAndPreventsLaunch) {
  if (geteuid() != 0) GTEST_SKIP() << "Craned Prolog runs as root";
  AddJob();
  BlockProlog();
  ASSERT_TRUE(Allocate(1));
  StartInitialization();
  ASSERT_TRUE(PrologEntered());
  auto first = Free(1);
  auto second = Free(1);
  ProcessFree();
  EXPECT_TRUE(HasStep(1));
  EXPECT_EQ(first.wait_for(0s), std::future_status::timeout);
  EXPECT_EQ(second.wait_for(0s), std::future_status::timeout);
  EXPECT_TRUE(ReportedStatuses().empty());
  EXPECT_EQ(job_destroyed, 0);
  ReleaseProlog();
  ASSERT_TRUE(DriveUntil(
      [&] { return first.wait_for(0s) == std::future_status::ready; }));
  EXPECT_EQ(first.get(), CraneErrCode::SUCCESS);
  EXPECT_EQ(second.get(), CraneErrCode::SUCCESS);
  EXPECT_FALSE(HasStep(1));
  EXPECT_TRUE(HasStep(kDaemonStepId));
  ASSERT_EQ(ReportedStatuses().size(), 1);
  EXPECT_EQ(ReportedStatuses().back().new_status, StepStatus::Completed);
}

TEST_F(JobManagerLifecycleTest, PrologFailureJoinsAndPreservesFailure) {
  if (geteuid() != 0) GTEST_SKIP() << "Craned Prolog runs as root";
  AddJob();
  BlockProlog(3);
  ASSERT_TRUE(Allocate(1));
  StartInitialization();
  ASSERT_TRUE(PrologEntered());
  auto freed = Free(1);
  ProcessFree();
  ReleaseProlog();
  ASSERT_TRUE(DriveUntil(
      [&] { return freed.wait_for(0s) == std::future_status::ready; }));
  EXPECT_EQ(freed.get(), CraneErrCode::SUCCESS);
  EXPECT_FALSE(HasStep(1));
  ASSERT_FALSE(ReportedStatuses().empty());
  EXPECT_EQ(ReportedStatuses().back().new_status, StepStatus::Failed);
  EXPECT_EQ(ReportedStatuses().back().exit_code, ExitCode::EC_PROLOG_ERR);
}

TEST_F(JobManagerLifecycleTest, DaemonFreeWaitsForChildAndClosesAdmission) {
  if (geteuid() != 0) GTEST_SKIP() << "Craned Prolog runs as root";
  AddJob();
  BlockProlog();
  ASSERT_TRUE(Allocate(1));
  StartInitialization();
  ASSERT_TRUE(PrologEntered());
  auto daemon_freed = Free(kDaemonStepId);
  ProcessFree();
  EXPECT_FALSE(Allocate(2));
  EXPECT_TRUE(HasStep(kDaemonStepId));
  EXPECT_TRUE(HasStep(1));
  EXPECT_EQ(job_destroyed, 0);
  EXPECT_EQ(daemon_freed.wait_for(0s), std::future_status::timeout);
  ReleaseProlog();
  ASSERT_TRUE(DriveUntil(
      [&] { return daemon_freed.wait_for(0s) == std::future_status::ready; }));
  EXPECT_EQ(daemon_freed.get(), CraneErrCode::SUCCESS);
  EXPECT_FALSE(HasStep(1));
  EXPECT_FALSE(HasStep(kDaemonStepId));
  EXPECT_EQ(job_destroyed, 1);
  EXPECT_TRUE(manager->FreeJobs({kJob}));
  EXPECT_FALSE(manager->QueryJob(kJob));
}

TEST_F(JobManagerLifecycleTest, WholeJobCancelDefersUninitializedChildren) {
  AddJob();
  ASSERT_TRUE(Allocate(1));
  manager->TerminateStepAsync(kJob, kDaemonStepId,
                              crane::grpc::TERMINATE_SOURCE_USER_CANCEL);
  ProcessCancel();
  ProcessStatus();
  EXPECT_EQ(PendingCancels(), 1);
  EXPECT_TRUE(ReportedStatuses().empty());
  auto freed = Free(1);
  ProcessFree();
  StartInitialization();
  ASSERT_TRUE(DriveUntil(
      [&] { return freed.wait_for(0s) == std::future_status::ready; }));
}

TEST_F(JobManagerLifecycleTest, MissingDaemonRejectsAllocationWithoutThrowing) {
  AddJob(false);
  EXPECT_FALSE(Allocate(1));
  EXPECT_FALSE(HasStep(1));
}
TEST_F(JobManagerLifecycleTest, DaemonEnvironmentIsCapturedAtAdmission) {
  AddJob(false);
  ASSERT_TRUE(Allocate(kDaemonStepId));
  auto env = QueuedEnvironment();
  EXPECT_EQ(env.at("CRANE_JOB_ID"), std::to_string(kJob));
  EXPECT_EQ(env.at("CRANE_JOB_NODELIST"), "test-node");
  auto freed = Free(kDaemonStepId);
  ProcessFree();
  EXPECT_EQ(freed.wait_for(0s), std::future_status::timeout);
  StartInitialization();
  ASSERT_TRUE(DriveUntil(
      [&] { return freed.wait_for(0s) == std::future_status::ready; }));
  EXPECT_EQ(freed.get(), CraneErrCode::SUCCESS);
  EXPECT_EQ(job_destroyed, 1);
}

TEST_F(JobManagerLifecycleTest, ExecuteWaitsForInitializationAndYieldsToFree) {
  AddJob();
  ASSERT_TRUE(Allocate(1));
  auto executed = QueueExecute(1);
  ProcessExecute();
  EXPECT_EQ(executed.wait_for(0s), std::future_status::timeout);
  auto freed = Free(1);
  ProcessFree();
  ProcessExecute();
  ASSERT_EQ(executed.wait_for(0s), std::future_status::ready);
  EXPECT_EQ(executed.get(), CraneErrCode::SUCCESS);
  EXPECT_EQ(freed.wait_for(0s), std::future_status::timeout);
  StartInitialization();
  ASSERT_TRUE(DriveUntil(
      [&] { return freed.wait_for(0s) == std::future_status::ready; }));
}

TEST_F(JobManagerLifecycleTest, LaunchFailureJoinsWithoutPollingSupervisor) {
  AddJob();
  ASSERT_TRUE(Allocate(1, true));
  StartInitialization();
  ASSERT_TRUE(DriveUntil([&] { return !InitializationPending(1); }));
  ASSERT_FALSE(ReportedStatuses().empty());
  EXPECT_EQ(ReportedStatuses().back().new_status, StepStatus::Completing);
  auto freed = Free(1);
  ASSERT_TRUE(DriveUntil(
      [&] { return freed.wait_for(0s) == std::future_status::ready; }));
  EXPECT_EQ(freed.get(), CraneErrCode::SUCCESS);
  EXPECT_EQ(ReportedStatuses().back().new_status, StepStatus::Failed);
  EXPECT_EQ(ReportedStatuses().back().exit_code, ExitCode::EC_SPAWN_FAILED);
}

TEST_F(JobManagerLifecycleTest, StartingStatusDoesNotEndInitialization) {
  AddJob();
  ASSERT_TRUE(Allocate(1));
  // A supervisor status can reach the event loop before the worker joins.
  SetStatus(1, StepStatus::Starting);
  manager->TerminateStepAsync(kJob, 1,
                              crane::grpc::TERMINATE_SOURCE_USER_CANCEL);
  ProcessCancel();
  ProcessStatus();
  EXPECT_EQ(PendingCancels(), 1);
  EXPECT_TRUE(ReportedStatuses().empty());
  auto executed = QueueExecute(1);
  ProcessExecute();
  EXPECT_EQ(executed.wait_for(0s), std::future_status::timeout);
  auto freed = Free(1);
  ProcessFree();
  StartInitialization();
  ASSERT_TRUE(DriveUntil(
      [&] { return freed.wait_for(0s) == std::future_status::ready; }));
  ProcessExecute();
  EXPECT_EQ(executed.wait_for(0s), std::future_status::ready);
}

TEST_F(JobManagerLifecycleTest, RpcBeforeSupervisorExistsDoesNotFailStep) {
  AddJob();
  ASSERT_TRUE(Allocate(1));
  EXPECT_FALSE(manager->QuerySshStepEnvVariables(kJob, 1));
  EXPECT_FALSE(manager->ChangeStepTimeConstraint(kJob, 1, 60, std::nullopt));
  ProcessStatus();
  EXPECT_TRUE(ReportedStatuses().empty());
  EXPECT_EQ(manager->GetAllocatedJobSteps().at(kJob).at(1),
            StepStatus::Configuring);
  auto freed = Free(1);
  ProcessFree();
  StartInitialization();
  ASSERT_TRUE(DriveUntil(
      [&] { return freed.wait_for(0s) == std::future_status::ready; }));
}

TEST_F(JobManagerLifecycleTest,
       RepeatedDaemonFreeJoinsAllInitializingChildren) {
  AddJob();
  ASSERT_TRUE(Allocate(1));
  ASSERT_TRUE(Allocate(2));
  auto first = Free(kDaemonStepId);
  auto second = Free(kDaemonStepId);
  ProcessFree();
  EXPECT_EQ(first.wait_for(0s), std::future_status::timeout);
  EXPECT_EQ(second.wait_for(0s), std::future_status::timeout);
  // FreeJobs acknowledges the request even when its existing guard retains it.
  EXPECT_TRUE(manager->FreeJobs({kJob}));
  EXPECT_TRUE(static_cast<bool>(manager->QueryJob(kJob)));
  EXPECT_EQ(job_destroyed, 0);
  StartInitialization();
  ASSERT_TRUE(DriveUntil([&] {
    return first.wait_for(0s) == std::future_status::ready &&
           second.wait_for(0s) == std::future_status::ready;
  }));
  EXPECT_EQ(first.get(), CraneErrCode::SUCCESS);
  EXPECT_EQ(second.get(), CraneErrCode::SUCCESS);
  EXPECT_TRUE(manager->GetAllocatedJobSteps(kJob).empty());
  EXPECT_EQ(job_destroyed, 1);
}
}  // namespace Craned

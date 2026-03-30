/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#pragma once

#include "CranedPublicDefs.h"
// Precompiled header comes first.

#include <grp.h>

#include "StepInstance.h"
#include "crane/AtomicHashMap.h"
#include "crane/PasswordEntry.h"

namespace Craned {

constexpr int kMaxSupervisorCheckRetryCount = 10;

using StepToD = crane::grpc::StepToD;

// Job allocation info, where allocation = job spec + execution info
struct JobInD {
  JobInD() = default;

  explicit JobInD(crane::grpc::JobToD const& job_to_d)
      : job_id(job_to_d.job_id()), job_to_d(job_to_d) {
    step_map_mtx = std::make_unique<absl::Mutex>();
  }

  ~JobInD() = default;

  JobInD(const JobInD& other) = delete;
  JobInD& operator=(const JobInD& other) = delete;

  JobInD(JobInD&& other) = default;
  JobInD& operator=(JobInD&& other) noexcept = default;

  uid_t Uid() const { return job_to_d.uid(); }

  job_id_t job_id;
  crane::grpc::JobToD job_to_d;
  crane::grpc::JobStatus status{crane::grpc::JobStatus::Configuring};

  std::unique_ptr<CgroupInterface> cgroup{nullptr};

  // Cgroup path info for this job. Set when cgroup is created/recovered.
  Common::CgroupPathInfo path_info;

  std::unique_ptr<absl::Mutex> step_map_mtx;
  absl::flat_hash_map<step_id_t, std::unique_ptr<StepInstance>> step_map;

  bool is_prolog_run{false};

  EnvMap GetJobEnvMap();
};

/**
 * The class that manages all jobs and handles interrupts.
 * SIGINT and SIGCHLD are processed in JobManager.
 * Especially, outside caller can use SetSigintCallback() to
 * set the callback when SIGINT is triggered.
 */
class JobManager {
 public:
  JobManager();

  CraneErrCode Recover(
      std::unordered_map<job_id_t, JobInD>&& job_map,
      absl::flat_hash_map<std::pair<job_id_t, step_id_t>,
                          std::unique_ptr<StepInstance>>&& step_map);

  ~JobManager();

  bool AllocJobs(std::vector<JobInD>&& jobs);

  /**
   * Terminate all job steps on the node and clean up.
   * @param job_ids jobs to free
   * @return true if all job exists
   */
  bool FreeJobs(std::set<job_id_t>&& job_ids);

  void AllocSteps(std::vector<StepToD>&& steps);

  void FreeSteps(
      std::unordered_map<job_id_t, std::unordered_set<step_id_t>>&& steps);

  CraneErrCode ExecuteStepAsync(
      std::unordered_map<job_id_t, std::unordered_set<step_id_t>>&& steps);

  bool ReceivePmixPort(
      const std::vector<std::pair<CranedId, std::string>>& pmix_ports,
      job_id_t job_id, step_id_t step_id);

  CraneExpected<void> ChangeStepTimeConstraint(
      job_id_t job_id, step_id_t step_id,
      std::optional<int64_t> time_limit_seconds,
      std::optional<int64_t> deadline_time);

  CraneExpected<void> ChangeAllStepsTimelimit(job_id_t job_id,
                                              int64_t new_timelimit_sec);
  std::optional<JobInfoOfUid> QueryJobInfoOfUid(uid_t uid);

  bool MigrateProcToCgroupOfJob(pid_t pid, job_id_t job_id);

  CraneExpected<EnvMap> QuerySshStepEnvVariables(job_id_t job_id,
                                                 step_id_t step_id);

  auto QueryJob(job_id_t job_id) {
    return m_job_map_.GetValueExclusivePtr(job_id);
  }

  std::map<job_id_t, std::map<step_id_t, StepStatus>> GetAllocatedJobSteps();

  std::vector<step_id_t> GetAllocatedJobSteps(job_id_t job_id);

  CraneErrCode SuspendJobByCgroup(job_id_t job_id);

  CraneErrCode ResumeJobByCgroup(job_id_t job_id);

  std::shared_ptr<SupervisorStub> GetSupervisorStub(job_id_t job_id,
                                                    step_id_t step_id);

  uint32_t GetStepExitCode(job_id_t job_id, step_id_t step_id);
  google::protobuf::Timestamp GetStepEndTime(job_id_t job_id,
                                             step_id_t step_id);

  void TerminateStepAsync(job_id_t job_id, step_id_t step_id,
                          crane::grpc::TerminateSource terminate_source);

  void MarkStepAsOrphanedAndTerminateAsync(job_id_t job_id, step_id_t step_id);

  /**
   *
   * @param jobs completing jobs to clean up
   * @param steps completing step to clean up, for daemon steps will send
   * ShutdownSupervisor RPC.
   *
   * If a job and its steps are both provided, the ownership of its steps submit
   * in `steps` is in step_map.
   * If step is not provided with its job, the step is erased from its job's
   * step_map.
   */
  void CleanUpJobAndStepsAsync(std::vector<JobInD>&& jobs,
                               std::vector<StepInstance*>&& steps);

  void StepStatusChangeAsync(job_id_t job_id, step_id_t step_id,
                             crane::grpc::JobStatus new_status,
                             uint32_t exit_code,
                             std::optional<std::string> reason,
                             const google::protobuf::Timestamp& timestamp);

  // Wait internal libuv base loop to exit...
  void Wait();
  bool IsEnding() { return m_is_ending_now_; }

  /***
   * Set the callback function will be called when SIGINT is triggered.
   * This function is not thread-safe.
   * @param cb the callback function.
   */
  void SetSigintCallback(std::function<void()> cb);

 private:
  template <class T>
  using ConcurrentQueue = moodycamel::ConcurrentQueue<T>;
  using JobMap = util::AtomicHashMap<absl::flat_hash_map, job_id_t, JobInD>;
  using UidMap = util::AtomicHashMap<absl::flat_hash_map, uid_t /*uid*/,
                                     absl::flat_hash_set<job_id_t>>;

  struct EvQueueAllocateJobElem {
    std::promise<bool> ok_prom;
    std::vector<JobInD> job_specs;
  };

  struct EvQueueAllocateStepElem {
    std::unique_ptr<StepInstance> step_inst;
    bool need_run_prolog;
    EnvMap job_env;
    std::promise<CraneErrCode> ok_prom;
  };

  struct EvQueueExecuteStepElem {
    job_id_t job_id;
    step_id_t step_id;
    std::promise<CraneErrCode> ok_prom;
  };

  struct EvQueueQueryJobIdFromPid {
    std::promise<CraneExpected<job_id_t>> job_id_prom;
    pid_t pid;
  };

  struct StepTerminateQueueElem {
    job_id_t job_id;
    step_id_t step_id;
    crane::grpc::TerminateSource terminate_source{
        crane::grpc::TERMINATE_SOURCE_USER_CANCEL};
    bool mark_as_orphaned{false};
    std::promise<void> terminate_prom;
  };

  struct UnexpectedSupervisorExitInfo {
    uint32_t exit_code;
    std::string reason;
    int retry_count{0};
  };

  std::optional<JobInD> FreeJobInfo_(job_id_t job_id);
  std::optional<JobInD> FreeJobInfoNoLock_(
      job_id_t job_id, JobMap::MapExclusivePtr& job_map_ptr,
      UidMap::MapExclusivePtr& uid_map_ptr);

  bool FreeJobAllocation_(std::vector<JobInD>&& jobs);

  void FreeStepAllocation_(std::vector<std::unique_ptr<StepInstance>>&& steps);

  bool RunPrologWhenAllocSteps_(job_id_t job_id, step_id_t step_id,
                                const EnvMap& job_env);

  void LaunchStepMt_(std::unique_ptr<StepInstance> step);

  /**
   * Inform CraneCtld of the status change of a job.
   * This method is called when the status of a job is changed:
   * 1. A job is completed successfully. It means that this job returns
   *  normally with 0 or a non-zero code. (EvSigchldCb_)
   * 2. A job is killed by a signal. In this case, the job is considered
   *  failed. (EvSigchldCb_)
   * 3. A job cannot be created because of various reasons.
   */
  void ActivateStepStatusChangeAsync_(
      job_id_t job_id, step_id_t step_id, crane::grpc::JobStatus new_status,
      uint32_t exit_code, std::optional<std::string> reason,
      const google::protobuf::Timestamp& timestamp);

  // Contains all the jobs that are running on this Craned node.
  JobMap m_job_map_;

  UidMap m_uid_to_job_ids_map_;

  bool EvCheckSupervisorRunning_();

  void EvSigchldCb_();

  // Callback function to handle SIGINT sent by Ctrl+C
  void EvSigintCb_();

  void EvCleanGrpcAllocStepsQueueCb_();

  void EvCleanGrpcExecuteStepQueueCb_();

  void EvCleanStepStatusChangeQueueCb_();

  void EvCleanTerminateStepQueueCb_();

  void RecordUnexpectedSupervisorExit_(pid_t pid, int status);
  void HandleUnexpectedSupervisorExits_();

  std::shared_ptr<uvw::loop> m_uvw_loop_;

  std::shared_ptr<uvw::signal_handle> m_sigchld_handle_;

  // When this event is triggered, the JobManager will not accept
  // any more new jobs and quit as soon as all existing jobs end.
  std::shared_ptr<uvw::signal_handle> m_sigint_handle_;
  std::shared_ptr<uvw::signal_handle> m_sigterm_handle_;

  absl::Mutex m_free_job_step_mtx_;
  // Step may hold by a job, use raw pointer here.
  std::unordered_map<StepInstance*, int /*retry count*/>
      m_completing_step_retry_map_ ABSL_GUARDED_BY(m_free_job_step_mtx_);
  std::unordered_map<job_id_t, JobInD> m_completing_job_
      ABSL_GUARDED_BY(m_free_job_step_mtx_);

  absl::Mutex m_unexpected_supervisor_exit_mtx_;
  absl::flat_hash_map<std::pair<job_id_t, step_id_t>,
                      UnexpectedSupervisorExitInfo>
      m_unexpected_supervisor_exit_map_
          ABSL_GUARDED_BY(m_unexpected_supervisor_exit_mtx_);

  std::shared_ptr<uvw::timer_handle> m_check_supervisor_timer_handle_;

  std::shared_ptr<uvw::async_handle> m_grpc_alloc_step_async_handle_;
  ConcurrentQueue<EvQueueAllocateStepElem> m_grpc_alloc_step_queue_;

  std::shared_ptr<uvw::async_handle> m_grpc_execute_step_async_handle_;
  // A custom event that handles the ExecuteStep RPC.
  ConcurrentQueue<EvQueueExecuteStepElem> m_grpc_execute_step_queue_;

  std::shared_ptr<uvw::async_handle> m_step_status_change_async_handle_;
  ConcurrentQueue<StepStatusChangeQueueElem> m_step_status_change_queue_;

  std::shared_ptr<uvw::async_handle> m_terminate_step_async_handle_;
  std::shared_ptr<uvw::timer_handle> m_terminate_step_timer_handle_;
  ConcurrentQueue<StepTerminateQueueElem> m_step_terminate_queue_;

  // The function which will be called when SIGINT is triggered.
  std::function<void()> m_sigint_cb_;

  // When SIGINT is triggered or Shutdown() gets called, this variable is set to
  // true. Then, AddJobAsyncMethod will not accept any more new jobs and
  // ev_sigchld_cb_ will stop the event loop when there is no job running.
  std::atomic_bool m_is_ending_now_{false};

  util::mutex m_prolog_serial_mutex_;  // when prolog flags set Serial

  std::thread m_uvw_thread_;

  std::mutex m_fork_reap_mu_;
  ChildExitWatcher m_exit_watcher_;
};
}  // namespace Craned

inline std::unique_ptr<Craned::JobManager> g_job_mgr;

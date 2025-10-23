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

#include "CgroupManager.h"
#include "crane/AtomicHashMap.h"
#include "crane/PasswordEntry.h"
#include "protos/Crane.grpc.pb.h"

namespace Craned {

constexpr int kMaxSupervisorCheckRetryCount = 10;

using StepToD = crane::grpc::StepToD;

struct StepInstance {
  job_id_t job_id;
  step_id_t step_id;

  pid_t supv_pid;

  crane::grpc::StepToD step_to_d;
  StepStatus status{StepStatus::Invalid};

  explicit StepInstance(const crane::grpc::StepToD& step_to_d);
  explicit StepInstance(const crane::grpc::StepToD& step_to_d, pid_t supv_pid,
                        StepStatus status);  // Step recovery

  [[nodiscard]] bool IsDaemon() const noexcept {
    return step_to_d.step_type() == crane::grpc::StepType::DAEMON;
  }

  [[nodiscard]] bool IsContainer() const noexcept {
    return step_to_d.has_container_meta();
  }

  [[nodiscard]] std::string StepIdString() const noexcept {
    return std::format("{}.{}", job_id, step_id);
  }
};

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

  std::unique_ptr<Common::CgroupInterface> cgroup{nullptr};
  CraneErrCode err_before_supervisor_ready{CraneErrCode::SUCCESS};

  std::unique_ptr<absl::Mutex> step_map_mtx;
  absl::flat_hash_map<step_id_t, std::unique_ptr<StepInstance>> step_map;

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
      std::unordered_map<task_id_t, JobInD>&& job_map,
      absl::flat_hash_map<std::pair<job_id_t, step_id_t>,
                          std::unique_ptr<StepInstance>>&& step_map);

  ~JobManager();

  bool AllocJobs(std::vector<JobInD>&& jobs);

  /**
   * Terminate all job steps on the node and clean up.
   * @param job_ids jobs to free
   * @return true if all job exists
   */
  bool FreeJobs(std::set<task_id_t>&& job_ids);

  void AllocSteps(std::vector<StepToD>&& steps);

  void FreeSteps(
      std::unordered_map<job_id_t, std::unordered_set<step_id_t>>&& steps);

  CraneErrCode ExecuteStepAsync(
      std::unordered_map<job_id_t, std::unordered_set<step_id_t>>&& steps);

  std::optional<TaskInfoOfUid> QueryTaskInfoOfUid(uid_t uid);

  bool MigrateProcToCgroupOfJob(pid_t pid, task_id_t job_id);

  auto QueryJob(job_id_t job_id) {
    return m_job_map_.GetValueExclusivePtr(job_id);
  }

  std::map<job_id_t, std::map<step_id_t, StepStatus>> GetAllocatedJobSteps();

  void TerminateStepAsync(job_id_t job_id, step_id_t step_id);

  void MarkStepAsOrphanedAndTerminateAsync(job_id_t job_id, step_id_t step_id);

  /**
   *
   * @param jobs completing jobs to clean up
   * @param steps completing step to clean up, for daemon steps will send
   * ShutdownSupervisor
   */
  void CleanUpJobAndStepsAsync(std::vector<JobInD>&& jobs,
                               std::vector<StepInstance*>&& steps);

  void StepStatusChangeAsync(job_id_t job_id, step_id_t step_id,
                             crane::grpc::TaskStatus new_status,
                             uint32_t exit_code,
                             std::optional<std::string> reason);

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
    std::promise<CraneErrCode> ok_prom;
  };

  struct EvQueueExecuteStepElem {
    job_id_t job_id;
    step_id_t step_id;
    std::promise<CraneErrCode> ok_prom;
  };

  struct EvQueueQueryTaskIdFromPid {
    std::promise<CraneExpected<task_id_t>> task_id_prom;
    pid_t pid;
  };

  struct StepTerminateQueueElem {
    job_id_t job_id;
    step_id_t step_id;
    bool terminated_by_user{false};  // If the task is canceled by user,
                                     // task->status=Cancelled
    bool mark_as_orphaned{false};
    std::promise<void> terminate_prom;
  };

  std::optional<JobInD> FreeJobInfo_(job_id_t job_id);
  std::optional<JobInD> FreeJobInfoNoLock_(
      job_id_t job_id, JobMap::MapExclusivePtr& job_map_ptr,
      UidMap::MapExclusivePtr& uid_map_ptr);

  bool FreeJobAllocation_(std::vector<JobInD>&& jobs);

  void FreeStepAllocation_(std::vector<std::unique_ptr<StepInstance>>&& steps);

  void LaunchStepMt_(std::unique_ptr<StepInstance> step);

  CraneErrCode SpawnSupervisor_(JobInD* job, StepInstance* step);

  /**
   * Inform CraneCtld of the status change of a task.
   * This method is called when the status of a task is changed:
   * 1. A task is completed successfully. It means that this task returns
   *  normally with 0 or a non-zero code. (EvSigchldCb_)
   * 2. A task is killed by a signal. In this case, the task is considered
   *  failed. (EvSigchldCb_)
   * 3. A task cannot be created because of various reasons.
   *  (EvGrpcSpawnInteractiveTaskCb_ and EvGrpcExecuteTaskCb_)
   */
  void ActivateTaskStatusChangeAsync_(job_id_t job_id, step_id_t step_id,
                                      crane::grpc::TaskStatus new_status,
                                      uint32_t exit_code,
                                      std::optional<std::string> reason);

  /**
   * Send a signal to the process group of pid. For kill uninitialized
   * Supervisor only.
   * This function ASSUMES that ALL processes belongs to the
   * process group with the PGID set to the PID of the first process in this
   * TaskExecutionInstance.
   * @param signum the value of signal.
   * @return if the signal is sent successfully, kOk is returned.
   * otherwise, kGenericFailure is returned.
   */
  static CraneErrCode KillPid_(pid_t pid, int signum);

  // Contains all the task that is running on this Craned node.
  JobMap m_job_map_;

  UidMap m_uid_to_job_ids_map_;

  bool EvCheckSupervisorRunning_();

  void EvSigchldCb_();

  // Callback function to handle SIGINT sent by Ctrl+C
  void EvSigintCb_();

  void EvCleanGrpcAllocStepsQueueCb_();

  void EvCleanGrpcExecuteStepQueueCb_();

  void EvCleanTaskStatusChangeQueueCb_();

  void EvCleanTerminateTaskQueueCb_();

  std::shared_ptr<uvw::loop> m_uvw_loop_;

  std::shared_ptr<uvw::signal_handle> m_sigchld_handle_;

  // When this event is triggered, the JobManager will not accept
  // any more new tasks and quit as soon as all existing task end.
  std::shared_ptr<uvw::signal_handle> m_sigint_handle_;
  std::shared_ptr<uvw::signal_handle> m_sigterm_handle_;

  absl::Mutex m_free_job_step_mtx_;
  // Step may hold by a job, use raw pointer here.
  std::unordered_map<StepInstance*, int /*retry count*/>
      m_completing_step_retry_map_ ABSL_GUARDED_BY(m_free_job_step_mtx_);
  std::unordered_map<job_id_t, JobInD> m_completing_job_
      ABSL_GUARDED_BY(m_free_job_step_mtx_);

  std::shared_ptr<uvw::timer_handle> m_check_supervisor_timer_handle_;

  std::shared_ptr<uvw::async_handle> m_grpc_alloc_step_async_handle_;
  ConcurrentQueue<EvQueueAllocateStepElem> m_grpc_alloc_step_queue_;

  std::shared_ptr<uvw::async_handle> m_grpc_execute_step_async_handle_;
  // A custom event that handles the ExecuteTask RPC.
  ConcurrentQueue<EvQueueExecuteStepElem> m_grpc_execute_step_queue_;

  std::shared_ptr<uvw::async_handle> m_task_status_change_async_handle_;
  ConcurrentQueue<StepStatusChangeQueueElem> m_task_status_change_queue_;

  std::shared_ptr<uvw::async_handle> m_terminate_step_async_handle_;
  std::shared_ptr<uvw::timer_handle> m_terminate_step_timer_handle_;
  ConcurrentQueue<StepTerminateQueueElem> m_step_terminate_queue_;

  // The function which will be called when SIGINT is triggered.
  std::function<void()> m_sigint_cb_;

  // When SIGINT is triggered or Shutdown() gets called, this variable is set to
  // true. Then, AddTaskAsyncMethod will not accept any more new tasks and
  // ev_sigchld_cb_ will stop the event loop when there is no task running.
  std::atomic_bool m_is_ending_now_{false};

  std::thread m_uvw_thread_;
};
}  // namespace Craned

inline std::unique_ptr<Craned::JobManager> g_job_mgr;
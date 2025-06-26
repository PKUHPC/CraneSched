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

// TODO: Replace this with tak execution info.
using StepToD = crane::grpc::TaskToD;

struct StepInstance {
  StepToD step_to_d;
  pid_t supervisor_pid;
  static EnvMap GetStepEnvMap(const StepToD& step);
};

struct StepStatus {
  JobToD job_to_d;
  StepToD step_to_d;
  pid_t super_pid;
};

// Job allocation info
// allocation = job spec + execution info
struct JobInstance {
  // Should never use, only for AtomicHashMap.
  JobInstance() = default;
  explicit JobInstance(const JobToD& job) : job_id(job.job_id), job_to_d(job) {}

  JobInstance(const JobInstance& other) = delete;
  JobInstance(JobInstance&& other) = default;

  ~JobInstance() = default;

  JobInstance& operator=(const JobInstance& other) = delete;
  JobInstance& operator=(JobInstance&& other) noexcept = default;

  task_id_t job_id;
  JobToD job_to_d;

  std::unique_ptr<CgroupInterface> cgroup{nullptr};
  bool orphaned{false};
  CraneErrCode err_before_supervisor_ready{CraneErrCode::SUCCESS};

  absl::flat_hash_map<step_id_t, std::unique_ptr<StepInstance>> step_map;

  static EnvMap GetJobEnvMap(const JobToD& job);
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
      std::unordered_map<task_id_t, StepStatus>&& job_status_map);

  ~JobManager();

  bool AllocJobs(std::vector<JobToD>&& jobs);

  CgroupInterface* GetCgForJob(task_id_t job_id);

  bool FreeJobs(const std::set<task_id_t>& job_ids);

  CraneErrCode ExecuteStepAsync(StepToD const& step);

  std::optional<TaskInfoOfUid> QueryTaskInfoOfUid(uid_t uid);

  bool MigrateProcToCgroupOfJob(pid_t pid, task_id_t job_id);

  CraneExpected<JobToD> QueryJob(task_id_t job_id);

  std::set<task_id_t> GetAllocatedJobs();

  void TerminateStepAsync(task_id_t task_id);

  void MarkStepAsOrphanedAndTerminateAsync(task_id_t task_id);

  bool ChangeJobTimeLimitAsync(task_id_t task_id, absl::Duration time_limit);

  void StepStopAndDoStatusChangeAsync(task_id_t job_id,
                                      crane::grpc::TaskStatus new_status,
                                      uint32_t exit_code,
                                      std::optional<std::string> reason);

  // Wait internal libuv base loop to exit...
  void Wait();

  /***
   * Set the callback function will be called when SIGINT is triggered.
   * This function is not thread-safe.
   * @param cb the callback function.
   */
  void SetSigintCallback(std::function<void()> cb);

 private:
  template <class T>
  using ConcurrentQueue = moodycamel::ConcurrentQueue<T>;

  struct EvQueueAllocateJobElem {
    std::promise<bool> ok_prom;
    std::vector<JobToD> job_specs;
  };

  struct EvQueueExecuteStepElem {
    std::unique_ptr<StepInstance> step_inst;
    std::promise<CraneErrCode> ok_prom;
  };

  struct EvQueueQueryTaskIdFromPid {
    std::promise<CraneExpected<task_id_t>> task_id_prom;
    pid_t pid;
  };

  struct ChangeTaskTimeLimitQueueElem {
    task_id_t job_id;
    absl::Duration time_limit;
    std::promise<bool> ok_prom;
  };

  struct StepTerminateQueueElem {
    uint32_t step_id{0};
    bool terminated_by_user{false};  // If the task is canceled by user,
                                     // task->status=Cancelled
    bool mark_as_orphaned{false};
    std::promise<void> terminate_prom;
  };

  struct CheckTaskStatusQueueElem {
    task_id_t task_id;
    std::promise<std::pair<bool, crane::grpc::TaskStatus>> status_prom;
  };

  bool FreeJobInstanceAllocation_(const std::vector<task_id_t>& job_ids);

  void LaunchStepMt_(std::unique_ptr<StepInstance> step);

  CraneErrCode SpawnSupervisor_(JobInstance* job, StepInstance* step);

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
  void ActivateTaskStatusChangeAsync_(task_id_t task_id,
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
  util::AtomicHashMap<absl::flat_hash_map, task_id_t, JobInstance> m_job_map_;
  util::AtomicHashMap<absl::flat_hash_map, uid_t /*uid*/,
                      absl::flat_hash_set<task_id_t>>
      m_uid_to_job_ids_map_;

  void EvCleanCheckSupervisorQueueCb_();
  bool EvCheckSupervisorRunning_();

  void EvSigchldCb_();

  // Callback function to handle SIGINT sent by Ctrl+C
  void EvSigintCb_();

  void EvCleanGrpcExecuteStepQueueCb_();

  void EvCleanTaskStatusChangeQueueCb_();

  void EvCleanTerminateTaskQueueCb_();

  void EvCleanChangeTaskTimeLimitQueueCb_();

  std::shared_ptr<uvw::loop> m_uvw_loop_;

  std::shared_ptr<uvw::signal_handle> m_sigchld_handle_;

  // When this event is triggered, the JobManager will not accept
  // any more new tasks and quit as soon as all existing task end.
  std::shared_ptr<uvw::signal_handle> m_sigint_handle_;
  std::shared_ptr<uvw::signal_handle> m_sigterm_handle_;

  absl::Mutex m_release_cg_mtx_;
  std::unordered_set<task_id_t> m_release_job_req_set_
      ABSL_GUARDED_BY(m_release_cg_mtx_);
  ConcurrentQueue<std::vector<task_id_t>> m_check_supervisor_queue_;
  std::shared_ptr<uvw::async_handle> m_check_supervisor_async_handle_;
  std::shared_ptr<uvw::timer_handle> m_check_supervisor_timer_handle_;

  std::shared_ptr<uvw::async_handle> m_grpc_alloc_job_async_handle_;
  ConcurrentQueue<EvQueueAllocateJobElem> m_grpc_alloc_job_queue_;

  std::shared_ptr<uvw::async_handle> m_grpc_execute_step_async_handle_;
  // A custom event that handles the ExecuteTask RPC.
  ConcurrentQueue<EvQueueExecuteStepElem> m_grpc_execute_step_queue_;

  std::shared_ptr<uvw::async_handle> m_task_status_change_async_handle_;
  ConcurrentQueue<TaskStatusChangeQueueElem> m_task_status_change_queue_;

  std::shared_ptr<uvw::async_handle> m_change_task_time_limit_async_handle_;
  ConcurrentQueue<ChangeTaskTimeLimitQueueElem> m_task_time_limit_change_queue_;

  std::shared_ptr<uvw::async_handle> m_terminate_step_async_handle_;
  ConcurrentQueue<StepTerminateQueueElem> m_step_terminate_queue_;

  // The function which will be called when SIGINT is triggered.
  std::function<void()> m_sigint_cb_;

  // When SIGINT is triggered or Shutdown() gets called, this variable is set to
  // true. Then, AddTaskAsyncMethod will not accept any more new tasks and
  // ev_sigchld_cb_ will stop the event loop when there is no task running.
  std::atomic_bool m_is_ending_now_{false};
  util::flexible_latch m_pending_thread_pool_tasks_{0};

  std::thread m_uvw_thread_;
};
}  // namespace Craned

inline std::unique_ptr<Craned::JobManager> g_job_mgr;
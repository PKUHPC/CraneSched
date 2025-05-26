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
#include "crane/PasswordEntry.h"
#include "protos/Crane.grpc.pb.h"

namespace Craned {

struct BatchMetaInProcessInstance {
  std::string parsed_output_file_pattern;
  std::string parsed_error_file_pattern;
};

class ProcessInstance {
 public:
  ProcessInstance(std::string exec_path, std::list<std::string> arg_list)
      : m_pid_(0),
        m_executive_path_(std::move(exec_path)),
        m_arguments_(std::move(arg_list)),
        m_user_data_(nullptr) {}

  ~ProcessInstance() {
    if (m_user_data_) {
      if (m_clean_cb_) {
        CRANE_TRACE("Clean Callback for pid {} is called.", m_pid_);
        m_clean_cb_(m_user_data_);
      } else
        CRANE_ERROR(
            "user_data in ProcessInstance is set, but clean_cb is not set!");
    }
  }

  [[nodiscard]] const std::string& GetExecPath() const {
    return m_executive_path_;
  }
  [[nodiscard]] const std::list<std::string>& GetArgList() const {
    return m_arguments_;
  }

  void SetPid(pid_t pid) { m_pid_ = pid; }
  [[nodiscard]] pid_t GetPid() const { return m_pid_; }

  void SetFinishCb(std::function<void(bool, int, void*)> cb) {
    m_finish_cb_ = std::move(cb);
  }

  void Output(std::string&& buf) {
    if (m_output_cb_) m_output_cb_(std::move(buf), m_user_data_);
  }

  void Finish(bool is_killed, int val) {
    if (m_finish_cb_) m_finish_cb_(is_killed, val, m_user_data_);
  }

  void SetUserDataAndCleanCb(void* data, std::function<void(void*)> cb) {
    m_user_data_ = data;
    m_clean_cb_ = std::move(cb);
  }

  BatchMetaInProcessInstance batch_meta;

 private:
  /* ------------- Fields set by SpawnProcessInInstance_  ---------------- */
  pid_t m_pid_{-1};

  /* ------- Fields set by the caller of SpawnProcessInInstance_  -------- */
  std::string m_executive_path_;
  std::list<std::string> m_arguments_;

  /***
   * The callback function called when a task writes to stdout or stderr.
   * @param[in] buf a slice of output buffer.
   */
  std::function<void(std::string&& buf, void*)> m_output_cb_;

  /***
   * The callback function called when a task is finished.
   * @param[in] bool true if the task is terminated by a signal, false
   * otherwise.
   * @param[in] int the number of signal if bool is true, the return value
   * otherwise.
   */
  std::function<void(bool, int, void*)> m_finish_cb_;

  void* m_user_data_{nullptr};
  std::function<void(void*)> m_clean_cb_;
};

struct MetaInTaskInstance {
  std::string parsed_sh_script_path;
  virtual ~MetaInTaskInstance() = default;
};

struct BatchMetaInTaskInstance : MetaInTaskInstance {
  ~BatchMetaInTaskInstance() override = default;
};

struct CrunMetaInTaskInstance : MetaInTaskInstance {
  ~CrunMetaInTaskInstance() override = default;

  int task_input_fd;
  int task_output_fd;

  std::string x11_target;
  uint16_t x11_port;
  std::string x11_auth_path;
};

// also arg for EvSigchldTimerCb_
struct ProcSigchldInfo {
  pid_t pid{};
  bool is_terminated_by_signal{};
  int value{};

  std::shared_ptr<uvw::timer_handle> resend_timer{nullptr};
};

// Todo: Task may consists of multiple subtasks
struct TaskInstance {
  TaskInstance() = default;
  ~TaskInstance();

  bool IsCrun() const;
  bool IsCalloc() const;

  CrunMetaInTaskInstance* GetCrunMeta() const;

  EnvMap GetTaskEnvMap() const;

  crane::grpc::TaskToD task;

  PasswordEntry pwd_entry{};
  std::unique_ptr<MetaInTaskInstance> meta{};

  std::string cgroup_path;
  CgroupInterface* cgroup{};
  std::shared_ptr<uvw::timer_handle> termination_timer{nullptr};
  std::shared_ptr<uvw::timer_handle> signal_timer{nullptr};

  // Task execution results
  bool orphaned{false};
  CraneErrCode err_before_exec{CraneErrCode::SUCCESS};
  bool cancelled_by_user{false};
  bool terminated_by_timeout{false};
  ProcSigchldInfo sigchld_info{};

  absl::flat_hash_map<pid_t, std::unique_ptr<ProcessInstance>> processes{};
};

/**
 * The class that manages all tasks and handles interrupts.
 * SIGINT and SIGCHLD are processed in TaskManager.
 * Especially, outside caller can use SetSigintCallback() to
 * set the callback when SIGINT is triggered.
 */
class TaskManager {
 public:
  TaskManager();

  ~TaskManager();

  CraneErrCode ExecuteTaskAsync(crane::grpc::TaskToD const& task);

  CraneExpected<EnvMap> QueryTaskEnvMapAsync(task_id_t task_id);

  void TerminateTaskAsync(uint32_t task_id);

  std::future<void> MarkTaskAsOrphanedAndTerminateAsync(task_id_t task_id);

  std::set<task_id_t> QueryRunningTasksAsync();

  bool ChangeTaskTimeLimitAsync(task_id_t task_id, absl::Duration time_limit);

  void TaskStopAndDoStatusChangeAsync(uint32_t task_id);

  // Wait internal libevent base loop to exit...
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

  struct SavedPrivilege {
    uid_t uid;
    gid_t gid;
  };

  struct EvQueueQueryTaskEnvMap {
    std::promise<CraneExpected<EnvMap>> env_prom;
    task_id_t task_id;
  };

  struct ChangeTaskTimeLimitQueueElem {
    uint32_t task_id;
    absl::Duration time_limit;
    std::promise<bool> ok_prom;
  };

  struct TaskTerminateQueueElem {
    uint32_t task_id{0};
    bool terminated_by_user{false};     // If the task is canceled by user,
                                        // task->status=Cancelled
    bool terminated_by_timeout{false};  // If the task is canceled by user,
                                        // task->status=Timeout
    bool mark_as_orphaned{false};
    std::promise<void> termination_prom{};
  };

  struct QueryTasksStatusQueueElem {
    std::promise<std::unordered_map<task_id_t, crane::grpc::TaskStatus>>
        status_prom;
  };

  static std::string ParseFilePathPattern_(const std::string& path_pattern,
                                           const std::string& cwd,
                                           task_id_t task_id);

  void LaunchTaskInstanceMt_(TaskInstance* instance);

  CraneErrCode SpawnProcessInInstance_(TaskInstance* instance,
                                       ProcessInstance* process);

  const TaskInstance* FindInstanceByTaskId_(uint32_t task_id);

  // Ask TaskManager to stop its event loop.
  void ActivateShutdownAsync_();

  /**
   * Inform CraneCtld of the status change of a task.
   * This method is called when the status of a task is changed:
   * 1. A task is completed successfully. It means that this task returns
   *  normally with 0 or a non-zero code. (EvSigchldCb_)
   * 2. A task is killed by a signal. In this case, the task is considered
   *  failed. (EvSigchldCb_)
   * 3. A task cannot be created because of various reasons.
   *  (EvGrpcSpawnInteractiveTaskCb_ and EvGrpcExecuteTaskCb_)
   * @param release_resource If set to true, CraneCtld will release the
   *  resource (mark the task status as REQUEUE) and requeue the task.
   */
  void ActivateTaskStatusChangeAsync_(uint32_t task_id,
                                      crane::grpc::TaskStatus new_status,
                                      uint32_t exit_code,
                                      std::optional<std::string> reason);

  template <typename Duration>
  void AddTerminationTimer_(TaskInstance* instance, Duration duration) {
    auto termination_handel = m_uvw_loop_->resource<uvw::timer_handle>();
    termination_handel->on<uvw::timer_event>(
        [this, task_id = instance->task.task_id()](const uvw::timer_event&,
                                                   uvw::timer_handle& h) {
          EvTaskTimerCb_(task_id);
        });
    termination_handel->start(
        std::chrono::duration_cast<std::chrono::milliseconds>(duration),
        std::chrono::seconds(0));
    instance->termination_timer = termination_handel;
  }

  void AddTerminationTimer_(TaskInstance* instance, int64_t secs) {
    auto termination_handel = m_uvw_loop_->resource<uvw::timer_handle>();
    termination_handel->on<uvw::timer_event>(
        [this, task_id = instance->task.task_id()](const uvw::timer_event&,
                                                   uvw::timer_handle& h) {
          EvTaskTimerCb_(task_id);
        });
    termination_handel->start(std::chrono::seconds(secs),
                              std::chrono::seconds(0));
    instance->termination_timer = termination_handel;
  }

  static void DelTerminationTimer_(TaskInstance* instance) {
    // Close handle before free
    instance->termination_timer->close();
    instance->termination_timer.reset();
  }

  void AddSignalTimer_(TaskInstance* instance, int64_t secs) {
    if (instance->signal_timer) {
      instance->signal_timer->close();
      instance->signal_timer.reset();
    }
    auto signal_handle = m_uvw_loop_->resource<uvw::timer_handle>();
    signal_handle->on<uvw::timer_event>([this, instance](
                                            const uvw::timer_event&,
                                            uvw::timer_handle& h) {
      if (!instance) return;
      int signal_number = instance->task.signal_param().signal_number();
      for (const auto& [_, pr_instance] : instance->processes) {
        KillProcessInstance_(pr_instance.get(), signal_number);
      }
    });
    signal_handle->start(std::chrono::seconds(secs), std::chrono::seconds(0));
    instance->signal_timer = signal_handle;
  }

  static void DelSignalTimer_(TaskInstance* instance) {
    if (instance->signal_timer) {
      instance->signal_timer->close();
      instance->signal_timer.reset();
    }
  }

  /**
   * Send a signal to the process group to which the processes in
   *  ProcessInstance belongs.
   * This function ASSUMES that ALL processes belongs to the process group with
   *  the PGID set to the PID of the first process in this ProcessInstance.
   * @param signum the value of signal.
   * @return if the signal is sent successfully, kOk is returned.
   * if the task name doesn't exist, kNonExistent is returned.
   * if the signal is invalid, kInvalidParam is returned.
   * otherwise, kGenericFailure is returned.
   */
  static CraneErrCode KillProcessInstance_(const ProcessInstance* proc,
                                           int signum);

  // Note: the three maps below are NOT protected by any mutex.
  //  They should be modified in libev callbacks to avoid races.

  // Contains all the task that is running on this Craned node.
  absl::flat_hash_map<uint32_t /*task id*/, std::unique_ptr<TaskInstance>>
      m_task_map_;

  //  ==================================================================
  // Critical data region starts
  //
  // To improve performance, the cgroup creation and task creation
  // are parallelized,
  // which breaks the serializability guaranteed by the single event loop.
  // The data structures in this region are accessed by multiple threads.
  // The atomicity of these data structure is guaranteed by either mutex or
  // AtomicHashMap.

  // The two following maps are used as indexes
  // and doesn't have the ownership of underlying objects.
  // A TaskInstance may contain more than one ProcessInstance.
  absl::flat_hash_map<uint32_t /*pid*/, TaskInstance*> m_pid_task_map_
      ABSL_GUARDED_BY(m_mtx_);
  absl::flat_hash_map<uint32_t /*pid*/, ProcessInstance*> m_pid_proc_map_
      ABSL_GUARDED_BY(m_mtx_);

  absl::Mutex m_mtx_;

  // Critical data region ends
  // ========================================================================

  void EvSigchldCb_();

  void EvCleanSigchldQueueCb_();

  // Callback function to handle SIGINT sent by Ctrl+C
  void EvGracefulExitCb_();

  void EvCleanGrpcExecuteTaskQueueCb_();

  void EvCleanGrpcQueryTaskEnvQueueCb_();

  void EvCleanTaskStatusChangeQueueCb_();

  void EvCleanTerminateTaskQueueCb_();

  void EvCleanQueryRunningTasksQueueCb_();

  void EvCleanChangeTaskTimeLimitQueueCb_();

  void EvTaskTimerCb_(task_id_t task_id);

  void EvSigchldTimerCb_(ProcSigchldInfo* sigchld_info);

  std::shared_ptr<uvw::loop> m_uvw_loop_;

  std::shared_ptr<uvw::signal_handle> m_sigchld_handle_;

  // When this event is triggered, the TaskManager will not accept
  // any more new tasks and quit as soon as all existing task end.
  std::shared_ptr<uvw::signal_handle> m_sigint_handle_;
  std::shared_ptr<uvw::signal_handle> m_sigterm_handle_;

  std::shared_ptr<uvw::async_handle>
      m_query_task_environment_variables_async_handle_;
  ConcurrentQueue<EvQueueQueryTaskEnvMap>
      m_query_task_environment_variables_queue;

  std::shared_ptr<uvw::async_handle> m_grpc_execute_task_async_handle_;
  // A custom event that handles the ExecuteTask RPC.
  ConcurrentQueue<std::unique_ptr<TaskInstance>> m_grpc_execute_task_queue_;

  std::shared_ptr<uvw::async_handle> m_process_sigchld_async_handle_;
  ConcurrentQueue<std::unique_ptr<ProcSigchldInfo>> m_sigchld_queue_;

  std::shared_ptr<uvw::async_handle> m_task_status_change_async_handle_;
  ConcurrentQueue<TaskStatusChangeQueueElem> m_task_status_change_queue_;

  std::shared_ptr<uvw::async_handle> m_change_task_time_limit_async_handle_;
  ConcurrentQueue<ChangeTaskTimeLimitQueueElem> m_task_time_limit_change_queue_;

  std::shared_ptr<uvw::async_handle> m_terminate_task_async_handle_;
  ConcurrentQueue<TaskTerminateQueueElem> m_task_terminate_queue_;

  std::shared_ptr<uvw::async_handle> m_query_running_task_async_handle_;
  ConcurrentQueue<std::promise<std::set<task_id_t>>>
      m_query_running_task_queue_;

  // The function which will be called when SIGINT is triggered.
  std::function<void()> m_sigint_cb_;

  // When SIGINT is triggered or Shutdown() gets called, this variable is set to
  // true. Then, AddTaskAsyncMethod will not accept any more new tasks and
  // ev_sigchld_cb_ will stop the event loop when there is no task running.
  std::atomic_bool m_is_ending_now_{false};

  // After m_is_ending_now_ set to true, when all task are cleared, we can exit.
  std::atomic_bool m_task_cleared_{false};

  std::thread m_uvw_thread_;

  static inline TaskManager* s_instance_ptr_;
};
}  // namespace Craned

inline std::unique_ptr<Craned::TaskManager> g_task_mgr;
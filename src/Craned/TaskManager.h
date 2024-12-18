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
#include <sys/epoll.h>
#include <sys/eventfd.h>

#include "CgroupManager.h"
#include "crane/PasswordEntry.h"
#include "protos/Crane.grpc.pb.h"

namespace Craned {

inline const char* MemoryEvents = "memory.events";
inline const char* MemoryOomControl = "memory.oom_control";

struct BatchMetaInProcessInstance {
  std::string parsed_output_file_pattern;
  std::string parsed_error_file_pattern;
};

class ProcessInstance {
 public:
  ProcessInstance(std::string exec_path, std::list<std::string> arg_list)
      : m_executive_path_(std::move(exec_path)),
        m_arguments_(std::move(arg_list)),
        m_pid_(0),
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
  pid_t m_pid_;

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

  void* m_user_data_;
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
  int msg_fd;
  ~CrunMetaInTaskInstance() override = default;
};

// also arg for EvSigchldTimerCb_
struct ProcSigchldInfo {
  pid_t pid;
  bool is_terminated_by_signal;
  int value;

  std::shared_ptr<uvw::timer_handle> resend_timer{nullptr};
};

enum class TerminatedBy : uint64_t {
  NONE = 0,
  CANCELLED_BY_USER,
  TERMINATION_BY_TIMEOUT,
  TERMINATION_BY_OOM
};

// Todo: Task may consists of multiple subtasks
struct TaskInstance {
  ~TaskInstance() {
    if (termination_timer) {
      termination_timer->close();
    }
    if (termination_oom) {
      termination_oom->close();
    }

    if (this->IsCrun()) {
      close(dynamic_cast<CrunMetaInTaskInstance*>(meta.get())->msg_fd);
    }
  }

  bool IsCrun() const;
  bool IsCalloc() const;
  EnvMap GetTaskEnvMap() const;

  crane::grpc::TaskToD task;

  PasswordEntry pwd_entry;
  std::unique_ptr<MetaInTaskInstance> meta;

  std::string cgroup_path;
  CgroupInterface* cgroup;
  std::shared_ptr<uvw::timer_handle> termination_timer{nullptr};
  std::shared_ptr<uvw::fs_event_handle> termination_oom{nullptr};
  std::atomic<bool> monitor_thread_stop{true};

  // Task execution results
  bool orphaned{false};
  CraneErr err_before_exec{CraneErr::kOk};
  TerminatedBy termination_event{TerminatedBy::NONE};
  ProcSigchldInfo sigchld_info{};

  absl::flat_hash_map<pid_t, std::unique_ptr<ProcessInstance>> processes;
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

  CraneErr ExecuteTaskAsync(crane::grpc::TaskToD const& task);

  CraneExpected<task_id_t> QueryTaskIdFromPidAsync(pid_t pid);

  CraneExpected<EnvMap> QueryTaskEnvMapAsync(task_id_t task_id);

  void TerminateTaskAsync(uint32_t task_id);

  void MarkTaskAsOrphanedAndTerminateAsync(task_id_t task_id);

  bool CheckTaskStatusAsync(task_id_t task_id, crane::grpc::TaskStatus* status);

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

  struct EvQueueQueryTaskIdFromPid {
    std::promise<CraneExpected<task_id_t>> task_id_prom;
    pid_t pid;
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
    TerminatedBy terminated_by{TerminatedBy::NONE};
    // If the task is canceled by user,
    // task->status=Cancelled
    // If the task is canceled by user,
    // task->status=Timeout
    bool mark_as_orphaned{false};
  };

  struct CheckTaskStatusQueueElem {
    task_id_t task_id;
    std::promise<std::pair<bool, crane::grpc::TaskStatus>> status_prom;
  };

  static std::string ParseFilePathPattern_(const std::string& path_pattern,
                                           const std::string& cwd,
                                           task_id_t task_id);

  void LaunchTaskInstanceMt_(TaskInstance* instance);

  CraneErr SpawnProcessInInstance_(TaskInstance* instance,
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

  void SetCgroupV1TerminationOOM_(TaskInstance* instance) {
    using namespace CgroupConstant;
    std::string slice = "/";
    std::string oom_control_full_path;

    oom_control_full_path =
        CgroupConstant::RootCgroupFullPath + slice +
        std::string(GetControllerStringView(Controller::MEMORY_CONTROLLER)) +
        slice + instance->cgroup_path + "/" + MemoryOomControl;

    int oom_control_fd = open(oom_control_full_path.c_str(), O_RDONLY);

    int efd = eventfd(0, EFD_CLOEXEC);
    if (efd == -1) {
      CRANE_ERROR("Failed to create event fd");
      return;
    }

    int epfd = epoll_create1(EPOLL_CLOEXEC);
    if (epfd == -1) {
      CRANE_ERROR("Failed to create epoll fd");
    }

    struct epoll_event event;
    event.events = EPOLLIN | EPOLLERR | EPOLLHUP;
    event.data.fd = efd;

    if (epoll_ctl(epfd, EPOLL_CTL_ADD, efd, &event) == -1) {
      CRANE_ERROR("Failed to set epoll_stl .");
      close(oom_control_fd);
      close(efd);
      close(epfd);
      return;
    }

    std::string cgroup_event_control =
        CgroupConstant::RootCgroupFullPath + slice +
        std::string(GetControllerStringView(Controller::MEMORY_CONTROLLER)) +
        slice + instance->cgroup_path + "/" + "cgroup.event_control";
    std::ofstream eventControlFile(cgroup_event_control);
    if (!eventControlFile.is_open()) {
      CRANE_ERROR("Failed to open cgroup.event_control file.");
      close(oom_control_fd);
      close(efd);
      close(epfd);
      return;
    }

    std::stringstream ss;
    ss << efd << " " << oom_control_fd;
    eventControlFile << ss.str();
    eventControlFile.close();
    close(oom_control_fd);
    instance->monitor_thread_stop = false;
    std::thread([this, epfd, efd, oom_control_full_path, instance]() {
      struct epoll_event events[32];
      uint64_t buf = 0;
      while (!instance->monitor_thread_stop) {
        // check monitor_thread_stop every 500 ms
        int nfds = epoll_wait(epfd, events, 32, 500);
        for (int i = 0; i < nfds; i++) {
          if (events[i].events & EPOLLIN) {
            ssize_t readBytes = read(events[i].data.fd, &buf, sizeof(buf));
            if (readBytes < 0) {
              close(efd);
              close(epfd);
              return;
            }
            EvOomCb_(oom_control_full_path, this, instance->task.task_id());
            close(efd);
            close(epfd);
            return;
          }
          if (events[i].events & (EPOLLERR | EPOLLHUP)) {
            CRANE_ERROR("Error or hangup on eventfd");
          }
        }
      }
      close(efd);
      close(epfd);
    }).detach();
  }

  void SetCgroupV2TerminationOOM_(TaskInstance* instance) {
    std::string slice = "/";
    std::string memory_events_full_path = CgroupConstant::RootCgroupFullPath +
                                          slice + instance->cgroup_path +
                                          slice + MemoryEvents;

    auto ev = m_uvw_loop_->resource<uvw::fs_event_handle>();

    ev->on<uvw::fs_event_event>(
        [this, memory_events_full_path, instance](
            uvw::fs_event_event& event, uvw::fs_event_handle& handle) {
          EvOomCb_(memory_events_full_path, this, instance->task.task_id());
        });

    ev->start(memory_events_full_path,
              uvw::fs_event_handle::event_flags::RECURSIVE);

    instance->termination_oom = ev;
  }

  static void DelTerminationTimer_(TaskInstance* instance) {
    // Close handle before free
    instance->termination_timer->close();
    instance->termination_timer.reset();
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
  static CraneErr KillProcessInstance_(const ProcessInstance* proc, int signum);

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
  void EvSigintCb_();

  void EvCleanGrpcExecuteTaskQueueCb_();

  void EvCleanGrpcQueryTaskIdFromPidQueueCb_();

  void EvCleanGrpcQueryTaskEnvQueueCb_();

  void EvCleanTaskStatusChangeQueueCb_();

  void EvCleanTerminateTaskQueueCb_();

  void EvCleanCheckTaskStatusQueueCb_();

  void EvCleanChangeTaskTimeLimitQueueCb_();

  void EvTaskTimerCb_(task_id_t task_id);

  void EvSigchldTimerCb_(ProcSigchldInfo* sigchld_info);

  void EvOomCb_(std::string oom_path, TaskManager* task_manager,
                task_id_t task_id);

  std::shared_ptr<uvw::loop> m_uvw_loop_;

  std::shared_ptr<uvw::signal_handle> m_sigchld_handle_;

  // When this event is triggered, the TaskManager will not accept
  // any more new tasks and quit as soon as all existing task end.
  std::shared_ptr<uvw::signal_handle> m_sigint_handle_;

  std::shared_ptr<uvw::async_handle> m_query_task_id_from_pid_async_handle_;
  ConcurrentQueue<EvQueueQueryTaskIdFromPid> m_query_task_id_from_pid_queue_;

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

  std::shared_ptr<uvw::async_handle> m_check_task_status_async_handle_;
  ConcurrentQueue<CheckTaskStatusQueueElem> m_check_task_status_queue_;

  // The function which will be called when SIGINT is triggered.
  std::function<void()> m_sigint_cb_;

  // When SIGINT is triggered or Shutdown() gets called, this variable is set to
  // true. Then, AddTaskAsyncMethod will not accept any more new tasks and
  // ev_sigchld_cb_ will stop the event loop when there is no task running.
  std::atomic_bool m_is_ending_now_{false};

  // After m_is_ending_now_ set to true, when all task are cleared, we can exit.
  std::atomic_bool m_task_cleared_{false};

  std::thread m_uvw_thread_;

  static inline TaskManager* m_instance_ptr_;
};
}  // namespace Craned

inline std::unique_ptr<Craned::TaskManager> g_task_mgr;
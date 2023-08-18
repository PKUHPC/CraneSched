/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include "CranedPublicDefs.h"
// Precompiled header comes first.

#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/util.h>
#include <evrpc.h>
#include <grp.h>
#include <sys/eventfd.h>
#include <sys/wait.h>

#include "CtldClient.h"
#include "crane/PasswordEntry.h"
#include "crane/PublicHeader.h"
#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"

namespace Craned {

struct BatchMetaInProcessInstance {
  std::string parsed_output_file_pattern;
};

class ProcessInstance {
 public:
  ProcessInstance(std::string exec_path, std::list<std::string> arg_list)
      : m_executive_path_(std::move(exec_path)),
        m_arguments_(std::move(arg_list)),
        m_pid_(0),
        m_ev_buf_event_(nullptr),
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

    if (m_ev_buf_event_) bufferevent_free(m_ev_buf_event_);
  }

  [[nodiscard]] const std::string& GetExecPath() const {
    return m_executive_path_;
  }
  [[nodiscard]] const std::list<std::string>& GetArgList() const {
    return m_arguments_;
  }

  void SetPid(pid_t pid) { m_pid_ = pid; }
  [[nodiscard]] pid_t GetPid() const { return m_pid_; }

  void SetEvBufEvent(struct bufferevent* ev_buf_event) {
    m_ev_buf_event_ = ev_buf_event;
  }

  void SetOutputCb(std::function<void(std::string&&, void*)> cb) {
    m_output_cb_ = std::move(cb);
  }

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

  // The underlying event that handles the output of the task.
  struct bufferevent* m_ev_buf_event_;

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

struct BatchMetaInTaskInstance {
  std::string parsed_sh_script_path;
};

// Todo: Task may consists of multiple subtasks
struct TaskInstance {
  ~TaskInstance() {
    if (termination_timer) {
      event_del(termination_timer);
      event_free(termination_timer);
      termination_timer = nullptr;
    }
  }

  crane::grpc::TaskToD task;

  PasswordEntry pwd_entry;
  BatchMetaInTaskInstance batch_meta;

  bool cancelled_by_user{false};
  bool terminated_by_timeout{false};

  bool orphaned{false};

  std::string cgroup_path;
  util::Cgroup* cgroup;
  struct event* termination_timer{nullptr};

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

  [[deprecated]] CraneErr SpawnInteractiveTaskAsync(
      uint32_t task_id, std::string executive_path,
      std::list<std::string> arguments,
      std::function<void(std::string&&, void*)> output_cb,
      std::function<void(bool, int, void*)> finish_cb);

  std::optional<uint32_t> QueryTaskIdFromPidAsync(pid_t pid);

  bool QueryCgOfTaskIdAsync(uint32_t task_id, util::Cgroup** cg);

  bool QueryTaskInfoOfUidAsync(uid_t uid, TaskInfoOfUid* info);

  bool CreateCgroupsAsync(
      std::vector<std::pair<task_id_t, uid_t>>&& task_id_uid_pairs);

  bool MigrateProcToCgroupOfTask(pid_t pid, task_id_t task_id);

  bool ReleaseCgroupAsync(uint32_t task_id, uid_t uid);

  void TerminateTaskAsync(uint32_t task_id);

  void MarkTaskAsOrphanedAndTerminateAsync(task_id_t task_id);

  bool CheckTaskStatusAsync(task_id_t task_id, crane::grpc::TaskStatus* status);

  bool ChangeTaskTimeLimitAsync(task_id_t task_id, absl::Duration time_limit);

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

  struct SigchldInfo {
    pid_t pid;
    bool is_terminated_by_signal;
    int value;
  };

  struct savedPrivilege {
    uid_t uid;
    gid_t gid;
  };

  struct EvQueueGrpcInteractiveTask {
    std::promise<CraneErr> err_promise;
    uint32_t task_id;
    std::string executive_path;
    std::list<std::string> arguments;
    std::function<void(std::string&&, void*)> output_cb;
    std::function<void(bool, int, void*)> finish_cb;
  };

  struct EvQueueQueryTaskIdFromPid {
    std::promise<std::optional<uint32_t> /*task_id*/> task_id_prom;
    pid_t pid;
  };

  struct EvQueueChangeTaskTimeLimit {
    uint32_t task_id;
    absl::Duration time_limit;
    std::promise<bool> ok_prom;
  };

  struct EvQueueTaskTerminate {
    uint32_t task_id{0};
    bool terminated_by_user{false};     // If the task is canceled by user,
                                        // task->status=Cancelled
    bool terminated_by_timeout{false};  // If the task is canceled by user,
                                        // task->status=Timeout
    bool mark_as_orphaned{false};
  };

  struct EvQueueCheckTaskStatus {
    task_id_t task_id;
    std::promise<std::pair<bool, crane::grpc::TaskStatus>> status_prom;
  };

  struct EvTimerCbArg {
    TaskManager* task_manager;
    TaskInstance* task_instance;
  };

  static std::string CgroupStrByTaskId_(uint32_t task_id);

  /**
   * EvActivateTaskStatusChange_ must NOT be called in this method and should be
   *  called in the caller method after checking the return value of this
   *  method.
   * @return kSystemErr if the socket pair between the parent process and child
   *  process cannot be created, and the caller should call strerror() to check
   *  the unix error code. kLibEventError if bufferevent_socket_new() fails.
   *  kCgroupError if CgroupManager cannot move the process to the cgroup bound
   *  to the TaskInstance. kProtobufError if the communication between the
   *  parent and the child process fails.
   */
  static CraneErr SpawnProcessInInstance_(TaskInstance* instance,
                                          ProcessInstance* process);

  const TaskInstance* FindInstanceByTaskId_(uint32_t task_id);

  // Ask TaskManager to stop its event loop.
  void EvActivateShutdown_();

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
  void EvActivateTaskStatusChange_(uint32_t task_id,
                                   crane::grpc::TaskStatus new_status,
                                   uint32_t exit_code,
                                   std::optional<std::string> reason);

  template <typename Duration>
  void EvAddTerminationTimer_(TaskInstance* instance, Duration duration) {
    std::chrono::seconds const sec =
        std::chrono::duration_cast<std::chrono::seconds>(duration);

    auto* arg = new EvTimerCbArg;
    arg->task_manager = this;
    arg->task_instance = instance;

    timeval tv{
        sec.count(),
        std::chrono::duration_cast<std::chrono::microseconds>(duration - sec)
            .count()};

    struct event* ev = event_new(m_ev_base_, -1, 0, EvOnTimerCb_, arg);
    CRANE_ASSERT_MSG(ev != nullptr, "Failed to create new timer.");
    evtimer_add(ev, &tv);

    arg->task_instance->termination_timer = ev;
  }

  void EvAddTerminationTimer_(TaskInstance* instance, int64_t secs) {
    auto* arg = new EvTimerCbArg;
    arg->task_manager = this;
    arg->task_instance = instance;

    timeval tv{static_cast<__time_t>(secs), 0};

    struct event* ev = event_new(m_ev_base_, -1, 0, EvOnTimerCb_, arg);
    CRANE_ASSERT_MSG(ev != nullptr, "Failed to create new timer.");
    evtimer_add(ev, &tv);

    arg->task_instance->termination_timer = ev;
  }

  void EvDelTerminationTimer_(TaskInstance* instance) {
    evtimer_del(instance->termination_timer);
    instance->termination_timer = nullptr;
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

  // The two following maps are used as indexes and doesn't have the ownership
  // of underlying objects. A TaskInstance may contain more than one
  // ProcessInstance.
  absl::flat_hash_map<uint32_t /*pid*/, TaskInstance*> m_pid_task_map_;
  absl::flat_hash_map<uint32_t /*pid*/, ProcessInstance*> m_pid_proc_map_;

  absl::node_hash_map<uint32_t /*task id*/, std::shared_ptr<util::Cgroup>>
      m_task_id_to_cg_map_;
  absl::flat_hash_map<uid_t /*uid*/, absl::flat_hash_set<uint32_t /*task id*/>>
      m_uid_to_task_ids_map_;

  absl::Mutex m_mtx_;

  static void EvSigchldCb_(evutil_socket_t sig, short events, void* user_data);

  // Callback function to handle SIGINT sent by Ctrl+C
  static void EvSigintCb_(evutil_socket_t sig, short events, void* user_data);

  static void EvGrpcExecuteTaskCb_(evutil_socket_t efd, short events,
                                   void* user_data);

  static void EvGrpcSpawnInteractiveTaskCb_(evutil_socket_t efd, short events,
                                            void* user_data);

  static void EvGrpcQueryTaskIdFromPidCb_(evutil_socket_t efd, short events,
                                          void* user_data);

  static void EvSubprocessReadCb_(struct bufferevent* bev, void* process);

  static void EvTaskStatusChangeCb_(evutil_socket_t efd, short events,
                                    void* user_data);

  static void EvTerminateTaskCb_(evutil_socket_t efd, short events,
                                 void* user_data);

  static void EvCheckTaskStatusCb_(evutil_socket_t, short events,
                                   void* user_data);

  static void EvChangeTaskTimeLimitCb_(evutil_socket_t, short events,
                                       void* user_data);

  static void EvExitEventCb_(evutil_socket_t, short events, void* user_data);

  static void EvOnTimerCb_(evutil_socket_t, short, void* arg);

  struct event_base* m_ev_base_{};
  struct event* m_ev_sigchld_{};

  // When this event is triggered, the TaskManager will not accept
  // any more new tasks and quit as soon as all existing task end.
  struct event* m_ev_sigint_{};

  // The function which will be called when SIGINT is triggered.
  std::function<void()> m_sigint_cb_;

  // When SIGINT is triggered or Shutdown() gets called, this variable is set to
  // true. Then, AddTaskAsyncMethod will not accept any more new tasks and
  // ev_sigchld_cb_ will stop the event loop when there is no task running.
  std::atomic_bool m_is_ending_now_{false};

  // When a new task grpc message arrives, the grpc function (which
  //  runs in parallel) uses m_grpc_event_fd_ to inform the event
  //  loop thread and the event loop thread retrieves the message
  //  from m_grpc_reqs_. We use this to keep thread-safety.
  struct event* m_ev_grpc_interactive_task_{};
  ConcurrentQueue<EvQueueGrpcInteractiveTask> m_grpc_interactive_task_queue_;

  struct event* m_ev_query_task_id_from_pid_{};
  ConcurrentQueue<EvQueueQueryTaskIdFromPid> m_query_task_id_from_pid_queue_;

  // A custom event that handles the ExecuteTask RPC.
  struct event* m_ev_grpc_execute_task_{};
  ConcurrentQueue<std::unique_ptr<TaskInstance>> m_grpc_execute_task_queue_;

  // When this event is triggered, the event loop will exit.
  struct event* m_ev_exit_event_{};

  struct event* m_ev_task_status_change_{};
  ConcurrentQueue<TaskStatusChange> m_task_status_change_queue_;

  struct event* m_ev_task_time_limit_change_{};
  ConcurrentQueue<EvQueueChangeTaskTimeLimit> m_task_time_limit_change_queue_;

  struct event* m_ev_task_terminate_{};
  ConcurrentQueue<EvQueueTaskTerminate> m_task_terminate_queue_;

  struct event* m_ev_check_task_status_{};
  ConcurrentQueue<EvQueueCheckTaskStatus> m_check_task_status_queue_;

  std::thread m_ev_loop_thread_;

  static inline TaskManager* m_instance_ptr_;
};
}  // namespace Craned

inline std::unique_ptr<Craned::TaskManager> g_task_mgr;
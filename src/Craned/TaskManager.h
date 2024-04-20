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
#include <uv.h>

#include <memory>
#include <uvw.hpp>

#include "TaskExecutor.h"
#include "crane/AtomicHashMap.h"
#include "crane/PasswordEntry.h"
#include "crane/PublicHeader.h"

namespace Craned {

class TaskManager;

struct EvTimerCbArg {
  TaskManager* task_manager;
  task_id_t task_id;
};

struct BatchMetaInTaskInstance {
  std::string parsed_sh_script_path;
};

// Todo: Task may consists of multiple subtasks
struct TaskInstance {
  ~TaskInstance() {
    if (termination_timer) {
      delete static_cast<EvTimerCbArg*>(
          event_get_callback_arg(termination_timer));
      evtimer_del(termination_timer);
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

  absl::flat_hash_map<pid_t, std::unique_ptr<TaskExecutor>>
      executors;  // TODO: pid -> executor, what about containers?
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

  // TODO: Add support for container
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

  static std::string CgroupStrByTaskId_(task_id_t task_id);

  static void ParseResultPathPattern_(const task_id_t task_id,
                                      const std::string& task_name,
                                      const std::string& cwd,
                                      const PasswordEntry& pwd,
                                      std::string& stdout_pattern,
                                      std::string& stderr_pattern);

  const TaskInstance* FindInstanceByTaskId_(uint32_t task_id);

  /**
   * Generate environment varibles from TaskInstance
   */
  static TaskExecutor::EnvironVars GetEnvironVarsFromTask_(
      const TaskInstance& task);

  [[deprecated]] CraneErr SpawnProcessInInstance_(TaskInstance* instance,
                                                  ProcessInstance* process);

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
    arg->task_id = instance->task.task_id();

    timeval tv{
        sec.count(),
        std::chrono::duration_cast<std::chrono::microseconds>(duration - sec)
            .count()};

    struct event* ev = event_new(m_ev_base_, -1, 0, EvOnTimerCb_, arg);
    CRANE_ASSERT_MSG(ev != nullptr, "Failed to create new timer.");
    evtimer_add(ev, &tv);

    instance->termination_timer = ev;
  }

  void EvAddTerminationTimer_(TaskInstance* instance, int64_t secs) {
    auto* arg = new EvTimerCbArg;
    arg->task_manager = this;
    arg->task_id = instance->task.task_id();

    timeval tv{static_cast<__time_t>(secs), 0};

    struct event* ev = event_new(m_ev_base_, -1, 0, EvOnTimerCb_, arg);
    CRANE_ASSERT_MSG(ev != nullptr, "Failed to create new timer.");
    evtimer_add(ev, &tv);

    instance->termination_timer = ev;
  }

  static void EvDelTerminationTimer_(TaskInstance* instance) {
    delete static_cast<EvTimerCbArg*>(
        event_get_callback_arg(instance->termination_timer));
    evtimer_del(instance->termination_timer);
    event_free(instance->termination_timer);
    instance->termination_timer = nullptr;
  }

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
      GUARDED_BY(m_mtx_);
  absl::flat_hash_map<uint32_t /*pid*/, TaskExecutor*> m_pid_exec_map_
      GUARDED_BY(m_mtx_);

  absl::Mutex m_mtx_;

  util::AtomicHashMap<absl::flat_hash_map, task_id_t, uid_t>
      m_task_id_to_uid_map_;

  util::AtomicHashMap<absl::flat_hash_map, task_id_t,
                      std::unique_ptr<util::Cgroup>>
      m_task_id_to_cg_map_;

  util::AtomicHashMap<absl::flat_hash_map, uid_t /*uid*/,
                      absl::flat_hash_set<task_id_t>>
      m_uid_to_task_ids_map_;

  // Critical data region ends
  // ========================================================================

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
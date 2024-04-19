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

#include "TaskManager.h"

#include <fcntl.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/util/delimited_message_util.h>
#include <sys/stat.h>

#include <filesystem>

#include "ResourceAllocators.h"
#include "TaskExecutor.h"
#include "crane/FdFunctions.h"
#include "protos/CraneSubprocess.pb.h"

namespace Craned {

TaskManager::TaskManager() {
  // Only called once. Guaranteed by singleton pattern.
  m_instance_ptr_ = this;

  m_ev_base_ = event_base_new();
  if (m_ev_base_ == nullptr) {
    CRANE_ERROR("Could not initialize libevent!");
    std::terminate();
  }
  {  // SIGCHLD
    m_ev_sigchld_ = evsignal_new(m_ev_base_, SIGCHLD, EvSigchldCb_, this);
    if (!m_ev_sigchld_) {
      CRANE_ERROR("Failed to create the SIGCHLD event!");
      std::terminate();
    }

    if (event_add(m_ev_sigchld_, nullptr) < 0) {
      CRANE_ERROR("Could not add the SIGCHLD event to base!");
      std::terminate();
    }
  }
  {  // SIGINT
    m_ev_sigint_ = evsignal_new(m_ev_base_, SIGINT, EvSigintCb_, this);
    if (!m_ev_sigint_) {
      CRANE_ERROR("Failed to create the SIGCHLD event!");
      std::terminate();
    }

    if (event_add(m_ev_sigint_, nullptr) < 0) {
      CRANE_ERROR("Could not add the SIGINT event to base!");
      std::terminate();
    }
  }
  {  // gRPC: SpawnInteractiveTask
    m_ev_grpc_interactive_task_ =
        event_new(m_ev_base_, -1, EV_PERSIST | EV_READ,
                  EvGrpcSpawnInteractiveTaskCb_, this);
    if (!m_ev_grpc_interactive_task_) {
      CRANE_ERROR("Failed to create the grpc event!");
      std::terminate();
    }

    if (event_add(m_ev_grpc_interactive_task_, nullptr) < 0) {
      CRANE_ERROR("Could not add the grpc event to base!");
      std::terminate();
    }
  }
  {  // gRPC: QueryTaskIdFromPid
    m_ev_query_task_id_from_pid_ =
        event_new(m_ev_base_, -1, EV_PERSIST | EV_READ,
                  EvGrpcQueryTaskIdFromPidCb_, this);
    if (!m_ev_query_task_id_from_pid_) {
      CRANE_ERROR("Failed to create the query task id event!");
      std::terminate();
    }

    if (event_add(m_ev_query_task_id_from_pid_, nullptr) < 0) {
      CRANE_ERROR("Could not add the query task id event to base!");
      std::terminate();
    }
  }
  {  // Exit Event
    m_ev_exit_event_ =
        event_new(m_ev_base_, -1, EV_PERSIST | EV_READ, EvExitEventCb_, this);
    if (!m_ev_exit_event_) {
      CRANE_ERROR("Failed to create the exit event!");
      std::terminate();
    }

    if (event_add(m_ev_exit_event_, nullptr) < 0) {
      CRANE_ERROR("Could not add the exit event to base!");
      std::terminate();
    }
  }
  {  // Grpc Execute Task Event
    m_ev_grpc_execute_task_ = event_new(m_ev_base_, -1, EV_READ | EV_PERSIST,
                                        EvGrpcExecuteTaskCb_, this);
    if (!m_ev_grpc_execute_task_) {
      CRANE_ERROR("Failed to create the grpc_execute_task event!");
      std::terminate();
    }
    if (event_add(m_ev_grpc_execute_task_, nullptr) < 0) {
      CRANE_ERROR("Could not add the m_ev_grpc_execute_task_ to base!");
      std::terminate();
    }
  }
  {  // Task Status Change Event
    m_ev_task_status_change_ = event_new(m_ev_base_, -1, EV_READ | EV_PERSIST,
                                         EvTaskStatusChangeCb_, this);
    if (!m_ev_task_status_change_) {
      CRANE_ERROR("Failed to create the task_status_change event!");
      std::terminate();
    }
    if (event_add(m_ev_task_status_change_, nullptr) < 0) {
      CRANE_ERROR("Could not add the m_ev_task_status_change_event_ to base!");
      std::terminate();
    }
  }
  {
    m_ev_task_time_limit_change_ = event_new(
        m_ev_base_, -1, EV_READ | EV_PERSIST, EvChangeTaskTimeLimitCb_, this);
    if (!m_ev_task_time_limit_change_) {
      CRANE_ERROR("Failed to create the task_time_limit_change event!");
      std::terminate();
    }
    if (event_add(m_ev_task_time_limit_change_, nullptr) < 0) {
      CRANE_ERROR("Could not add the m_ev_task_time_limit_change_ to base!");
      std::terminate();
    }
  }
  {
    m_ev_task_terminate_ = event_new(m_ev_base_, -1, EV_READ | EV_PERSIST,
                                     EvTerminateTaskCb_, this);
    if (!m_ev_task_terminate_) {
      CRANE_ERROR("Failed to create the task_terminate event!");
      std::terminate();
    }
    if (event_add(m_ev_task_terminate_, nullptr) < 0) {
      CRANE_ERROR("Could not add the m_ev_task_terminate_ to base!");
      std::terminate();
    }
  }
  {
    m_ev_check_task_status_ = event_new(m_ev_base_, -1, EV_READ | EV_PERSIST,
                                        EvCheckTaskStatusCb_, this);
    if (!m_ev_check_task_status_) {
      CRANE_ERROR("Failed to create the check_task_status event!");
      std::terminate();
    }
    if (event_add(m_ev_check_task_status_, nullptr) < 0) {
      CRANE_ERROR("Could not add the m_ev_check_task_status_ to base!");
      std::terminate();
    }
  }

  m_ev_loop_thread_ =
      std::thread([this]() { event_base_dispatch(m_ev_base_); });
}

TaskManager::~TaskManager() {
  if (m_ev_loop_thread_.joinable()) m_ev_loop_thread_.join();

  if (m_ev_sigchld_) event_free(m_ev_sigchld_);
  if (m_ev_sigint_) event_free(m_ev_sigint_);

  if (m_ev_grpc_interactive_task_) event_free(m_ev_grpc_interactive_task_);
  if (m_ev_query_task_id_from_pid_) event_free(m_ev_query_task_id_from_pid_);
  if (m_ev_grpc_execute_task_) event_free(m_ev_grpc_execute_task_);
  if (m_ev_exit_event_) event_free(m_ev_exit_event_);
  if (m_ev_task_status_change_) event_free(m_ev_task_status_change_);
  if (m_ev_task_time_limit_change_) event_free(m_ev_task_time_limit_change_);
  if (m_ev_task_terminate_) event_free(m_ev_task_terminate_);
  if (m_ev_check_task_status_) event_free(m_ev_check_task_status_);

  if (m_ev_base_) event_base_free(m_ev_base_);
}

const TaskInstance* TaskManager::FindInstanceByTaskId_(uint32_t task_id) {
  auto iter = m_task_map_.find(task_id);
  if (iter == m_task_map_.end()) return nullptr;
  return iter->second.get();
}

std::string TaskManager::CgroupStrByTaskId_(uint32_t task_id) {
  return fmt::format("Crane_Task_{}", task_id);
}

void TaskManager::EvSigchldCb_(evutil_socket_t sig, short events,
                               void* user_data) {
  assert(m_instance_ptr_->m_instance_ptr_ != nullptr);
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  SigchldInfo sigchld_info{};

  int status;
  pid_t pid;
  while (true) {
    pid = waitpid(-1, &status, WNOHANG
                  /* TODO(More status tracing): | WUNTRACED | WCONTINUED */);

    if (pid > 0) {
      if (WIFEXITED(status)) {
        // Exited with status WEXITSTATUS(status)
        sigchld_info = {pid, false, WEXITSTATUS(status)};
        CRANE_TRACE("Receiving SIGCHLD for pid {}. Signaled: false, Status: {}",
                    pid, WEXITSTATUS(status));
      } else if (WIFSIGNALED(status)) {
        // Killed by signal WTERMSIG(status)
        sigchld_info = {pid, true, WTERMSIG(status)};
        CRANE_TRACE("Receiving SIGCHLD for pid {}. Signaled: true, Signal: {}",
                    pid, WTERMSIG(status));
      }
      /* Todo(More status tracing):
       else if (WIFSTOPPED(status)) {
        printf("stopped by signal %d\n", WSTOPSIG(status));
      } else if (WIFCONTINUED(status)) {
        printf("continued\n");
      } */

      this_->m_mtx_.Lock();

      auto task_iter = this_->m_pid_task_map_.find(pid);
      auto exec_iter = this_->m_pid_exec_map_.find(pid);
      if (task_iter == this_->m_pid_task_map_.end() ||
          exec_iter == this_->m_pid_exec_map_.end()) {
        CRANE_WARN("Failed to find task id for pid {}.", pid);
        this_->m_mtx_.Unlock();
      } else {
        TaskInstance* instance = task_iter->second;
        TaskExecutor* exec = exec_iter->second;
        uint32_t task_id = instance->task.task_id();

        // Remove indexes from pid to ProcessInstance*
        this_->m_pid_exec_map_.erase(exec_iter);
        this_->m_pid_task_map_.erase(task_iter);

        this_->m_mtx_.Unlock();

        exec->Finish(sigchld_info.is_terminated_by_signal, sigchld_info.value);

        // Free the ProcessInstance. ITask struct is not freed here because
        // the ITask for an Interactive task can have no ProcessInstance.
        auto pr_it = instance->executors.find(pid);
        if (pr_it == instance->executors.end()) {
          CRANE_ERROR("Failed to find pid {} in task #{}'s ProcessInstances",
                      task_id, pid);
        } else {
          instance->executors.erase(pr_it);

          if (!instance->executors.empty()) {
            if (sigchld_info.is_terminated_by_signal) {
              // If a task is terminated by a signal and there are other
              //  running processes belonging to this task, kill them.
              this_->TerminateTaskAsync(task_id);
            }
          } else {
            if (!instance->orphaned) {
              // If the ProcessInstance has no process left and the task was not
              // marked as an orphaned task, send TaskStatusChange for this
              // task. See the comment of EvActivateTaskStatusChange_.
              if (instance->task.type() == crane::grpc::Batch) {
                // For a Batch task, the end of the process means it is done.
                if (sigchld_info.is_terminated_by_signal) {
                  if (instance->cancelled_by_user)
                    this_->EvActivateTaskStatusChange_(
                        task_id, crane::grpc::TaskStatus::Cancelled,
                        sigchld_info.value + ExitCode::kTerminationSignalBase,
                        std::nullopt);
                  else if (instance->terminated_by_timeout)
                    this_->EvActivateTaskStatusChange_(
                        task_id, crane::grpc::TaskStatus::ExceedTimeLimit,
                        sigchld_info.value + ExitCode::kTerminationSignalBase,
                        std::nullopt);
                  else
                    this_->EvActivateTaskStatusChange_(
                        task_id, crane::grpc::TaskStatus::Failed,
                        sigchld_info.value + ExitCode::kTerminationSignalBase,
                        std::nullopt);
                } else
                  this_->EvActivateTaskStatusChange_(
                      task_id, crane::grpc::TaskStatus::Completed,
                      sigchld_info.value, std::nullopt);
              } else {
                // For a COMPLETING Interactive task with a process running, the
                // end of this process means that this task is done.
                if (sigchld_info.is_terminated_by_signal) {
                  this_->EvActivateTaskStatusChange_(
                      task_id, crane::grpc::TaskStatus::Completed,
                      sigchld_info.value + ExitCode::kTerminationSignalBase,
                      std::nullopt);
                } else {
                  this_->EvActivateTaskStatusChange_(
                      task_id, crane::grpc::TaskStatus::Completed,
                      sigchld_info.value, std::nullopt);
                }
              }
            }
          }
        }
      }
    } else if (pid == 0) {
      // There's no child that needs reaping.
      // If Craned is exiting, check if there's any task remaining.
      // If there's no task running, just stop the loop of TaskManager.
      if (this_->m_is_ending_now_) {
        if (this_->m_task_map_.empty()) {
          this_->EvActivateShutdown_();
        }
      }
      break;
    } else if (pid < 0) {
      if (errno != ECHILD)
        CRANE_DEBUG("waitpid() error: {}, {}", errno, strerror(errno));
      break;
    }
  }
}

void TaskManager::EvSubprocessReadCb_(struct bufferevent* bev, void* process) {
  auto* proc = reinterpret_cast<ProcessInstance*>(process);

  size_t buf_len = evbuffer_get_length(bev->input);

  std::string str;
  str.resize(buf_len);
  int n_copy = evbuffer_remove(bev->input, str.data(), buf_len);

  CRANE_TRACE("Read {:>4} bytes from subprocess (pid: {}): {}", n_copy,
              proc->GetPid(), str);

  proc->Output(std::move(str));
}

void TaskManager::EvSigintCb_(int sig, short events, void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  if (!this_->m_is_ending_now_) {
    // SIGINT has been sent once. If SIGINT are captured twice, it indicates
    // the signal sender can't wait to stop Craned and Craned just send SIGTERM
    // to all tasks to kill them immediately.

    CRANE_INFO("Caught SIGINT. Send SIGTERM to all running tasks...");

    this_->m_is_ending_now_ = true;

    if (this_->m_sigint_cb_) this_->m_sigint_cb_();

    for (auto task_it = this_->m_task_map_.begin();
         task_it != this_->m_task_map_.end();) {
      task_id_t task_id = task_it->first;
      TaskInstance* task_instance = task_it->second.get();

      if (task_instance->task.type() == crane::grpc::Batch) {
        for (auto&& [pid, pr_instance] : task_instance->executors) {
          CRANE_INFO(
              "Sending SIGINT to the process group of task #{} with root "
              "process pid {}",
              task_id, pr_instance->GetPid());
          pr_instance->Kill(SIGKILL);
        }
        task_it++;
      } else {
        // Kill all process in an interactive task and just remove it from the
        // task map.
        CRANE_DEBUG("Cleaning interactive task #{}...",
                    task_instance->task.task_id());
        task_instance->cgroup->KillAllProcesses();

        auto to_remove_it = task_it++;
        this_->m_task_map_.erase(to_remove_it);
      }
    }

    if (this_->m_task_map_.empty()) {
      // If there is not any batch task to wait for, stop the loop directly.
      this_->EvActivateShutdown_();
    }
  } else {
    CRANE_INFO(
        "SIGINT has been triggered already. Sending SIGKILL to all process "
        "groups instead.");
    if (this_->m_task_map_.empty()) {
      // If there is no task to kill, stop the loop directly.
      this_->EvActivateShutdown_();
    } else {
      for (auto&& [task_id, task_instance] : this_->m_task_map_) {
        for (auto&& [pid, pr_instance] : task_instance->executors) {
          CRANE_INFO(
              "Sending SIGKILL to the process group of task #{} with root "
              "process pid {}",
              task_id, pr_instance->GetPid());
          pr_instance->Kill(SIGKILL);
        }
      }
    }
  }
}

void TaskManager::EvExitEventCb_(int efd, short events, void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  CRANE_TRACE("Exit event triggered. Stop event loop.");

  struct timeval delay = {0, 0};
  event_base_loopexit(this_->m_ev_base_, &delay);
}

void TaskManager::EvActivateShutdown_() {
  CRANE_TRACE("Triggering exit event...");
  m_is_ending_now_ = true;
  event_active(m_ev_exit_event_, 0, 0);
}

void TaskManager::Wait() {
  if (m_ev_loop_thread_.joinable()) m_ev_loop_thread_.join();
}

void TaskManager::SetSigintCallback(std::function<void()> cb) {
  m_sigint_cb_ = std::move(cb);
}

CraneErr TaskManager::ExecuteTaskAsync(crane::grpc::TaskToD const& task) {
  CRANE_INFO("Executing task #{}", task.task_id());

  auto instance = std::make_unique<TaskInstance>();

  // Simply wrap the Task structure within a TaskInstance structure and
  // pass it to the event loop. The cgroup field of this task is initialized
  // in the corresponding handler (EvGrpcExecuteTaskCb_).
  instance->task = task;

  m_grpc_execute_task_queue_.enqueue(std::move(instance));
  event_active(m_ev_grpc_execute_task_, 0, 0);

  return CraneErr::kOk;
}

void TaskManager::EvGrpcExecuteTaskCb_(int, short events, void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);
  std::unique_ptr<TaskInstance> popped_instance;

  while (this_->m_grpc_execute_task_queue_.try_dequeue(popped_instance)) {
    // Once ExecuteTask RPC is processed, the TaskInstance goes into
    // m_task_map_.
    TaskInstance* instance = popped_instance.get();
    task_id_t task_id = instance->task.task_id();

    auto [iter, _] =
        this_->m_task_map_.emplace(task_id, std::move(popped_instance));

    // Add a timer to limit the execution time of a task.
    // Note: event_new and event_add in this function is not thread safe,
    //       so we move it outside the multithreading part.
    this_->EvAddTerminationTimer_(instance,
                                  instance->task.time_limit().seconds());

    g_thread_pool->detach_task([this_, instance, task_id]() {
      if (!this_->m_task_id_to_cg_map_.Contains(task_id)) {
        CRANE_ERROR("Failed to find created cgroup for task #{}", task_id);
        this_->EvActivateTaskStatusChange_(
            task_id, crane::grpc::TaskStatus::Failed,
            ExitCode::kExitCodeCgroupError,
            fmt::format("Failed to find created cgroup for task #{}", task_id));
        return;
      }

      {
        auto cg_it = this_->m_task_id_to_cg_map_[task_id];
        auto& cg_unique_ptr = *cg_it;
        if (!cg_unique_ptr) {
          instance->cgroup_path = CgroupStrByTaskId_(task_id);
          cg_unique_ptr = util::CgroupUtil::CreateOrOpen(
              instance->cgroup_path,
              util::NO_CONTROLLER_FLAG |
                  util::CgroupConstant::Controller::CPU_CONTROLLER |
                  util::CgroupConstant::Controller::MEMORY_CONTROLLER,
              util::NO_CONTROLLER_FLAG, false);

          if (!cg_unique_ptr) {
            CRANE_ERROR("Failed to created cgroup for task #{}", task_id);
            this_->EvActivateTaskStatusChange_(
                task_id, crane::grpc::TaskStatus::Failed,
                ExitCode::kExitCodeCgroupError,
                fmt::format("Failed to create cgroup for task #{}", task_id));
            return;
          }
        }
        instance->cgroup = cg_unique_ptr.get();
      }

      instance->pwd_entry.Init(instance->task.uid());
      if (!instance->pwd_entry.Valid()) {
        CRANE_DEBUG("Failed to look up password entry for uid {} of task #{}",
                    instance->task.uid(), task_id);
        this_->EvActivateTaskStatusChange_(
            task_id, crane::grpc::TaskStatus::Failed,
            ExitCode::kExitCodePermissionDenied,
            fmt::format(
                "Failed to look up password entry for uid {} of task #{}",
                instance->task.uid(), task_id));
        return;
      }

      bool ok = AllocatableResourceAllocator::Allocate(
          instance->task.resources().allocatable_resource(), instance->cgroup);

      if (!ok) {
        CRANE_ERROR(
            "Failed to allocate allocatable resource in cgroup for task #{}",
            task_id);
        this_->EvActivateTaskStatusChange_(
            instance->task.task_id(), crane::grpc::TaskStatus::Failed,
            ExitCode::kExitCodeCgroupError,
            fmt::format(
                "Cannot allocate resources for the instance of task #{}",
                task_id));
        return;
      }
      // If this is a batch task, run it now.
      if (instance->task.type() == crane::grpc::Batch) {
        std::unique_ptr<TaskExecutor> executor = nullptr;

        // Store meta data in executor
        auto meta = TaskMetaInExecutor{
            .pwd = instance->pwd_entry,
            .id = task_id,
            .name = instance->task.name(),
        };

        // Instantiate ProcessInstance/ContainerInstance
        if (instance->task.container().empty()) {
          // use ProcessInstance
          executor = std::make_unique<ProcessInstance>(
              meta, instance->task.cwd(), std::list<std::string>());
        } else {
          // use ContainerInstance
          auto bundle_path = std::filesystem::path(instance->task.container());
          executor =
              std::make_unique<ContainerInstance>(meta, bundle_path.string());
        }

        if (executor == nullptr) {
          CRANE_ERROR("Failed to create executor for task #{}", task_id);
          this_->EvActivateTaskStatusChange_(
              task_id, crane::grpc::TaskStatus::Failed,
              ExitCode::kExitCodeSpawnProcessFail,
              fmt::format("Failed to create executor for task #{}", task_id));
          return;
        }

        // Write the script to the file
        instance->batch_meta.parsed_sh_script_path =
            executor->WriteBatchScript(instance->task.batch_meta().sh_script());
        if (instance->batch_meta.parsed_sh_script_path.empty()) {
          CRANE_ERROR("Cannot write shell script for task #{}", task_id);
          this_->EvActivateTaskStatusChange_(
              task_id, crane::grpc::TaskStatus::Failed,
              ExitCode::kExitCodeFileNotFound,
              fmt::format("Cannot write shell script for task #{}", task_id));
          return;
        }

        /* Perform file name substitutions
         * %j - Job ID
         * %u - Username
         * %x - Job name
         */
        CraneErr err = CraneErr::kOk;
        std::string parsed_output_file_pattern{};
        if (instance->task.batch_meta().output_file_pattern().empty()) {
          // If output file path is not specified, first set it to cwd.
          parsed_output_file_pattern = fmt::format("{}/", instance->task.cwd());
        } else {
          if (instance->task.batch_meta().output_file_pattern()[0] == '/') {
            // If output file path is an absolute path, do nothing.
            parsed_output_file_pattern =
                instance->task.batch_meta().output_file_pattern();
          } else {
            // If output file path is a relative path, prepend cwd to the path.
            parsed_output_file_pattern =
                fmt::format("{}/{}", instance->task.cwd(),
                            instance->task.batch_meta().output_file_pattern());
          }
        }

        // Path ends with a directory, append default output file name
        // `Crane-<Job ID>.out` to the path.
        if (absl::EndsWith(parsed_output_file_pattern, "/")) {
          parsed_output_file_pattern +=
              fmt::format("Crane-{}.out", g_ctld_client->GetCranedId());
        }

        // Replace the format strings.
        absl::StrReplaceAll({{"%j", std::to_string(task_id)},
                             {"%u", instance->pwd_entry.Username()},
                             {"%x", instance->task.name()}},
                            &parsed_output_file_pattern);

        // Set the parsed output file pattern to batch_meta in executor.
        executor->SetBatchMeta({
            .parsed_output_file_pattern = std::move(parsed_output_file_pattern),
        });

        // auto output_cb = [](std::string&& buf, void* data) {
        //   CRANE_TRACE("Read output from subprocess: {}", buf);
        // };
        //
        // process->SetOutputCb(std::move(output_cb));

        // TODO: Modify this for ContainerInstance
        err = executor->Spawn(instance->cgroup,
                              std::move(GetEnvironVarsFromTask_(*instance)));

        if (err == CraneErr::kOk) {
          this_->m_mtx_.Lock();

          // Child process may finish or abort before we put its pid into maps.
          // However, it doesn't matter because SIGCHLD will be handled after
          // this function or event ends.
          // Add indexes from pid to TaskInstance*, TaskExecutor*
          this_->m_pid_task_map_.emplace(executor->GetPid(), instance);
          this_->m_pid_exec_map_.emplace(executor->GetPid(), executor.get());

          this_->m_mtx_.Unlock();

          // Move the ownership of TaskExecutor into the TaskInstance.
          instance->executors.emplace(executor->GetPid(), std::move(executor));
        } else {
          this_->EvActivateTaskStatusChange_(
              task_id, crane::grpc::TaskStatus::Failed,
              ExitCode::kExitCodeSpawnProcessFail,
              fmt::format(
                  "Cannot spawn an executor inside the instance of task #{}",
                  task_id));
        }
      }
    });
  }
}

void TaskManager::EvTaskStatusChangeCb_(int efd, short events,
                                        void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  TaskStatusChange status_change;
  while (this_->m_task_status_change_queue_.try_dequeue(status_change)) {
    auto iter = this_->m_task_map_.find(status_change.task_id);
    CRANE_ASSERT_MSG(iter != this_->m_task_map_.end(),
                     "Task should be found here.");

    // Free the TaskInstance structure
    this_->m_task_map_.erase(status_change.task_id);
    g_ctld_client->TaskStatusChangeAsync(std::move(status_change));
  }

  // Todo: Add additional timer to check periodically whether all children
  //  have exited.
  if (this_->m_is_ending_now_ && this_->m_task_map_.empty()) {
    CRANE_TRACE(
        "Craned is ending and all tasks have been reaped. "
        "Stop event loop.");
    this_->EvActivateShutdown_();
  }
}

void TaskManager::EvActivateTaskStatusChange_(
    uint32_t task_id, crane::grpc::TaskStatus new_status, uint32_t exit_code,
    std::optional<std::string> reason) {
  TaskStatusChange status_change{task_id, new_status, exit_code};
  if (reason.has_value()) status_change.reason = std::move(reason);

  m_task_status_change_queue_.enqueue(std::move(status_change));
  event_active(m_ev_task_status_change_, 0, 0);
}

TaskExecutor::EnvironVars TaskManager::GetEnvironVarsFromTask_(
    const TaskInstance& instance) {
  TaskExecutor::EnvironVars env_vec{};
  env_vec.emplace_back("CRANE_JOB_NODELIST",
                       absl::StrJoin(instance.task.allocated_nodes(), ";"));
  env_vec.emplace_back("CRANE_EXCLUDES",
                       absl::StrJoin(instance.task.excludes(), ";"));
  env_vec.emplace_back("CRANE_JOB_NAME", instance.task.name());
  env_vec.emplace_back("CRANE_ACCOUNT", instance.task.account());
  env_vec.emplace_back("CRANE_PARTITION", instance.task.partition());
  env_vec.emplace_back("CRANE_QOS", instance.task.qos());
  env_vec.emplace_back("CRANE_MEM_PER_NODE",
                       std::to_string(instance.task.resources()
                                          .allocatable_resource()
                                          .memory_limit_bytes() /
                                      (1024 * 1024)));
  env_vec.emplace_back("CRANE_JOB_ID", std::to_string(instance.task.task_id()));

  int64_t time_limit_sec = instance.task.time_limit().seconds();
  int hours = time_limit_sec / 3600;
  int minutes = (time_limit_sec % 3600) / 60;
  int seconds = time_limit_sec % 60;
  std::string time_limit =
      fmt::format("{:0>2}:{:0>2}:{:0>2}", hours, minutes, seconds);
  env_vec.emplace_back("CRANE_TIMELIMIT", time_limit);
  return env_vec;
}

CraneErr TaskManager::SpawnInteractiveTaskAsync(
    uint32_t task_id, std::string executive_path,
    std::list<std::string> arguments,
    std::function<void(std::string&&, void*)> output_cb,
    std::function<void(bool, int, void*)> finish_cb) {
  EvQueueGrpcInteractiveTask elem{
      .task_id = task_id,
      .executive_path = std::move(executive_path),
      .arguments = std::move(arguments),
      .output_cb = std::move(output_cb),
      .finish_cb = std::move(finish_cb),
  };
  std::future<CraneErr> err_future = elem.err_promise.get_future();

  m_grpc_interactive_task_queue_.enqueue(std::move(elem));
  event_active(m_ev_grpc_interactive_task_, 0, 0);

  return err_future.get();
}

std::optional<uint32_t> TaskManager::QueryTaskIdFromPidAsync(pid_t pid) {
  EvQueueQueryTaskIdFromPid elem{.pid = pid};
  std::future<std::optional<uint32_t>> task_id_opt_future =
      elem.task_id_prom.get_future();
  m_query_task_id_from_pid_queue_.enqueue(std::move(elem));
  event_active(m_ev_query_task_id_from_pid_, 0, 0);

  return task_id_opt_future.get();
}

void TaskManager::EvGrpcSpawnInteractiveTaskCb_(int efd, short events,
                                                void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  EvQueueGrpcInteractiveTask elem;
  while (this_->m_grpc_interactive_task_queue_.try_dequeue(elem)) {
    CRANE_TRACE("Receive one GrpcSpawnInteractiveTask for task #{}",
                elem.task_id);

    auto task_iter = this_->m_task_map_.find(elem.task_id);
    if (task_iter == this_->m_task_map_.end()) {
      CRANE_ERROR("Cannot find task #{}", elem.task_id);
      elem.err_promise.set_value(CraneErr::kNonExistent);
      return;
    }

    if (task_iter->second->task.type() != crane::grpc::Interactive) {
      CRANE_ERROR("Try spawning a new process in non-interactive task #{}!",
                  elem.task_id);
      elem.err_promise.set_value(CraneErr::kInvalidParam);
      return;
    }

    // TODO: Add container support
    // FIXME: NOT passing executive path
    auto process = std::make_unique<ProcessInstance>(
        TaskMetaInExecutor{
            .pwd = task_iter->second->pwd_entry,
            .id = task_iter->second->task.task_id(),
            .name = task_iter->second->task.name(),
        },
        task_iter->second->task.cwd(), std::move(elem.arguments));

    process->SetOutputCb(std::move(elem.output_cb));
    process->SetFinishCb(std::move(elem.finish_cb));

    CraneErr err;
    err = process->Spawn(task_iter->second->cgroup,
                         GetEnvironVarsFromTask_(*task_iter->second));
    elem.err_promise.set_value(err);

    if (err != CraneErr::kOk)
      this_->EvActivateTaskStatusChange_(elem.task_id, crane::grpc::Failed,
                                         ExitCode::kExitCodeSpawnProcessFail,
                                         std::string(CraneErrStr(err)));
  }
}

void TaskManager::EvGrpcQueryTaskIdFromPidCb_(int efd, short events,
                                              void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  EvQueueQueryTaskIdFromPid elem;
  while (this_->m_query_task_id_from_pid_queue_.try_dequeue(elem)) {
    this_->m_mtx_.Lock();

    auto task_iter = this_->m_pid_task_map_.find(elem.pid);
    if (task_iter == this_->m_pid_task_map_.end())
      elem.task_id_prom.set_value(std::nullopt);
    else {
      TaskInstance* instance = task_iter->second;
      uint32_t task_id = instance->task.task_id();
      elem.task_id_prom.set_value(task_id);
    }

    this_->m_mtx_.Unlock();
  }
}

void TaskManager::EvOnTimerCb_(int, short, void* arg_) {
  auto* arg = reinterpret_cast<EvTimerCbArg*>(arg_);
  TaskManager* this_ = arg->task_manager;
  task_id_t task_id = arg->task_id;

  CRANE_TRACE("Task #{} exceeded its time limit. Terminating it...", task_id);

  // Sometimes, task finishes just before time limit.
  // After the execution of SIGCHLD callback where the task has been erased,
  // the timer is triggered immediately.
  // That's why we need to check the existence of the task again in timer
  // callback, otherwise a segmentation fault will occur.
  auto task_it = this_->m_task_map_.find(task_id);
  if (task_it == this_->m_task_map_.end()) {
    CRANE_TRACE("Task #{} has already been removed.");
    return;
  }

  TaskInstance* task_instance = task_it->second.get();
  this_->EvDelTerminationTimer_(task_instance);

  if (task_instance->task.type() == crane::grpc::Batch) {
    EvQueueTaskTerminate ev_task_terminate{
        .task_id = task_id,
        .terminated_by_timeout = true,
    };
    this_->m_task_terminate_queue_.enqueue(ev_task_terminate);
    event_active(this_->m_ev_task_terminate_, 0, 0);
  } else {
    this_->EvActivateTaskStatusChange_(
        task_id, crane::grpc::TaskStatus::ExceedTimeLimit,
        ExitCode::kExitCodeExceedTimeLimit, std::nullopt);
  }
}

void TaskManager::EvTerminateTaskCb_(int efd, short events, void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  EvQueueTaskTerminate elem;  // NOLINT(cppcoreguidelines-pro-type-member-init)
  while (this_->m_task_terminate_queue_.try_dequeue(elem)) {
    CRANE_TRACE(
        "Receive TerminateRunningTask Request from internal queue. "
        "Task id: {}",
        elem.task_id);

    auto iter = this_->m_task_map_.find(elem.task_id);
    if (iter == this_->m_task_map_.end()) {
      CRANE_DEBUG("Trying terminating unknown task #{}", elem.task_id);
      return;
    }

    const auto& task_instance = iter->second;

    if (elem.terminated_by_user) task_instance->cancelled_by_user = true;
    if (elem.mark_as_orphaned) task_instance->orphaned = true;
    if (elem.terminated_by_timeout) task_instance->terminated_by_timeout = true;

    int sig = SIGTERM;  // For BatchTask
    if (task_instance->task.type() == crane::grpc::Interactive) sig = SIGHUP;

    if (!task_instance->executors.empty()) {
      // For an Interactive task with a process running or a Batch task, we just
      // send a kill signal here.
      for (auto&& [pid, pr_instance] : task_instance->executors)
        pr_instance->Kill(sig);
    } else if (task_instance->task.type() == crane::grpc::Interactive) {
      // For an Interactive task with no process running, it ends immediately.
      this_->EvActivateTaskStatusChange_(elem.task_id, crane::grpc::Completed,
                                         ExitCode::kExitCodeTerminated,
                                         std::nullopt);
    }
  }
}

void TaskManager::TerminateTaskAsync(uint32_t task_id) {
  EvQueueTaskTerminate elem{.task_id = task_id, .terminated_by_user = true};
  m_task_terminate_queue_.enqueue(elem);
  event_active(m_ev_task_terminate_, 0, 0);
}

void TaskManager::MarkTaskAsOrphanedAndTerminateAsync(task_id_t task_id) {
  EvQueueTaskTerminate elem{.task_id = task_id, .mark_as_orphaned = true};
  m_task_terminate_queue_.enqueue(elem);
  event_active(m_ev_task_terminate_, 0, 0);
}

bool TaskManager::CreateCgroupsAsync(
    std::vector<std::pair<task_id_t, uid_t>>&& task_id_uid_pairs) {
  std::chrono::steady_clock::time_point begin;
  std::chrono::steady_clock::time_point end;

  CRANE_DEBUG("Creating cgroups for {} tasks", task_id_uid_pairs.size());

  begin = std::chrono::steady_clock::now();

  for (int i = 0; i < task_id_uid_pairs.size(); i++) {
    auto [task_id, uid] = task_id_uid_pairs[i];

    CRANE_TRACE("Create lazily allocated cgroups for task #{}, uid {}", task_id,
                uid);

    this->m_task_id_to_cg_map_.Emplace(task_id, nullptr);
    if (!this->m_uid_to_task_ids_map_.Contains(uid))
      this->m_uid_to_task_ids_map_.Emplace(
          uid, absl::flat_hash_set<uint32_t>{task_id});
    else
      this->m_uid_to_task_ids_map_[uid]->emplace(task_id);
  }

  end = std::chrono::steady_clock::now();
  CRANE_TRACE("Create cgroups costed {} ms",
              std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
                  .count());

  return true;
}

bool TaskManager::ReleaseCgroupAsync(uint32_t task_id, uid_t uid) {
  this->m_uid_to_task_ids_map_[uid]->erase(task_id);
  if (this->m_uid_to_task_ids_map_[uid]->empty()) {
    this->m_uid_to_task_ids_map_.Erase(uid);
  }

  if (!this->m_task_id_to_cg_map_.Contains(task_id)) {
    CRANE_DEBUG(
        "Trying to release a non-existent cgroup for task #{}. Ignoring it...",
        task_id);

    return false;
  } else {
    // The termination of all processes in a cgroup is a time-consuming work.
    // Therefore, once we are sure that the cgroup for this task exists, we
    // let gRPC call return and put the termination work into the thread pool
    // to avoid blocking the event loop of TaskManager.
    // Kind of async behavior.

    // avoid deadlock by Erase at next line
    util::Cgroup* cgroup = this->m_task_id_to_cg_map_[task_id]->release();
    this->m_task_id_to_cg_map_.Erase(task_id);

    if (cgroup != nullptr) {
      g_thread_pool->detach_task([cgroup]() {
        bool rc;
        int cnt = 0;

        while (true) {
          if (cgroup->Empty()) break;

          if (cnt >= 5) {
            CRANE_ERROR(
                "Couldn't kill the processes in cgroup {} after {} times. "
                "Skipping it.",
                cgroup->GetCgroupString(), cnt);
            break;
          }

          cgroup->KillAllProcesses();
          ++cnt;
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        delete cgroup;
      });
    }
    return true;
  }
}

bool TaskManager::QueryTaskInfoOfUidAsync(uid_t uid, TaskInfoOfUid* info) {
  CRANE_DEBUG("Query task info for uid {}", uid);

  info->job_cnt = 0;
  info->cgroup_exists = false;

  if (this->m_uid_to_task_ids_map_.Contains(uid)) {
    auto task_ids = this->m_uid_to_task_ids_map_[uid];
    info->job_cnt = task_ids->size();
    info->first_task_id = *task_ids->begin();
  }
  return info->job_cnt > 0;
}

bool TaskManager::CheckTaskStatusAsync(task_id_t task_id,
                                       crane::grpc::TaskStatus* status) {
  EvQueueCheckTaskStatus elem{.task_id = task_id};

  std::future<std::pair<bool, crane::grpc::TaskStatus>> res{
      elem.status_prom.get_future()};

  m_check_task_status_queue_.enqueue(std::move(elem));
  event_active(m_ev_check_task_status_, 0, 0);

  auto [ok, task_status] = res.get();
  if (!ok) return false;

  *status = task_status;
  return true;
}

void TaskManager::EvCheckTaskStatusCb_(int, short events, void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  EvQueueCheckTaskStatus elem;
  while (this_->m_check_task_status_queue_.try_dequeue(elem)) {
    task_id_t task_id = elem.task_id;
    if (this_->m_task_map_.contains(task_id)) {
      // Found in task map. The task must be running.
      elem.status_prom.set_value({true, crane::grpc::TaskStatus::Running});
      continue;
    }

    // If a task id can be found in g_ctld_client, the task has ended.
    //  Now if CraneCtld check the status of these tasks, there is no need to
    //  send to TaskStatusChange again. Just cancel them.
    crane::grpc::TaskStatus status;
    bool exist =
        g_ctld_client->CancelTaskStatusChangeByTaskId(task_id, &status);
    if (exist) {
      elem.status_prom.set_value({true, status});
      continue;
    }

    elem.status_prom.set_value(
        {false, /* Invalid Value*/ crane::grpc::Pending});
  }
}

bool TaskManager::ChangeTaskTimeLimitAsync(task_id_t task_id,
                                           absl::Duration time_limit) {
  EvQueueChangeTaskTimeLimit elem{.task_id = task_id, .time_limit = time_limit};

  std::future<bool> ok_fut = elem.ok_prom.get_future();
  m_task_time_limit_change_queue_.enqueue(std::move(elem));
  event_active(m_ev_task_time_limit_change_, 0, 0);
  return ok_fut.get();
}

void TaskManager::EvChangeTaskTimeLimitCb_(int, short events, void* user_data) {
  auto* this_ = reinterpret_cast<TaskManager*>(user_data);

  EvQueueChangeTaskTimeLimit elem;
  while (this_->m_task_time_limit_change_queue_.try_dequeue(elem)) {
    auto iter = this_->m_task_map_.find(elem.task_id);
    if (iter != this_->m_task_map_.end()) {
      TaskInstance* task_instance = iter->second.get();
      this_->EvDelTerminationTimer_(task_instance);

      absl::Time start_time =
          absl::FromUnixSeconds(task_instance->task.start_time().seconds());
      absl::Duration const& new_time_limit = elem.time_limit;

      if (absl::Now() - start_time >= new_time_limit) {
        // If the task times out, terminate it.
        EvQueueTaskTerminate ev_task_terminate{elem.task_id};
        this_->m_task_terminate_queue_.enqueue(ev_task_terminate);
        event_active(this_->m_ev_task_terminate_, 0, 0);

      } else {
        // If the task haven't timed out, set up a new timer.
        this_->EvAddTerminationTimer_(
            task_instance,
            ToInt64Seconds((new_time_limit - (absl::Now() - start_time))));
      }
      elem.ok_prom.set_value(true);
    } else {
      CRANE_ERROR("Try to update the time limit of a non-existent task #{}.",
                  elem.task_id);
      elem.ok_prom.set_value(false);
    }
  }
}

bool TaskManager::MigrateProcToCgroupOfTask(pid_t pid, task_id_t task_id) {
  if (!this->m_task_id_to_cg_map_.Contains(task_id)) {
    return false;
  }

  auto cg_it = this->m_task_id_to_cg_map_[task_id];
  auto& cg_ptr = *cg_it;
  if (!cg_ptr) {
    auto cgroup_path = CgroupStrByTaskId_(task_id);
    cg_ptr = util::CgroupUtil::CreateOrOpen(
        cgroup_path,
        util::NO_CONTROLLER_FLAG |
            util::CgroupConstant::Controller::CPU_CONTROLLER |
            util::CgroupConstant::Controller::MEMORY_CONTROLLER |
            util::CgroupConstant::Controller::DEVICES_CONTROLLER,
        util::NO_CONTROLLER_FLAG, false);
  }

  return cg_ptr->MigrateProcIn(pid);
}

}  // namespace Craned

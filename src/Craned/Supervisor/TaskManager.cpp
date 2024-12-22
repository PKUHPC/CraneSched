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

#include "TaskManager.h"

#include <sys/wait.h>

#include "CforedClient.h"
#include "CranedClient.h"
#include "crane/String.h"

namespace Supervisor {

bool TaskInstance::IsCrun() const {
  return task.type() == crane::grpc::Interactive &&
         task.interactive_meta().interactive_type() == crane::grpc::Crun;
}

bool TaskInstance::IsCalloc() const {
  return task.type() == crane::grpc::Interactive &&
         task.interactive_meta().interactive_type() == crane::grpc::Calloc;
}

TaskManager::TaskManager() : m_supervisor_exit_(false) {
  m_uvw_loop_ = uvw::loop::create();

  m_sigchld_handle_ = m_uvw_loop_->resource<uvw::signal_handle>();
  m_sigchld_handle_->on<uvw::signal_event>(
      [this](const uvw::signal_event&, uvw::signal_handle&) {
        EvSigchldCb_();
      });
  if (m_sigchld_handle_->start(SIGCHLD) != 0) {
    CRANE_ERROR("Failed to start the SIGCHLD handle");
  }

  m_uvw_thread_ = std::thread([this]() {
    util::SetCurrentThreadName("TaskMgrLoopThr");
    auto idle_handle = m_uvw_loop_->resource<uvw::idle_handle>();
    idle_handle->on<uvw::idle_event>(
        [this](const uvw::idle_event&, uvw::idle_handle& h) {
          if (m_supervisor_exit_) {
            h.parent().walk([](auto&& h) { h.close(); });
            h.parent().stop();
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
        });
    if (idle_handle->start() != 0) {
      CRANE_ERROR("Failed to start the idle event in TaskManager loop.");
    }
    m_uvw_loop_->run();
  });
}

TaskManager::~TaskManager() {
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}
void TaskManager::Wait() {
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}

void TaskManager::TaskStopAndDoStatusChange() {
  CRANE_INFO("Task #{} stopped and is doing TaskStatusChange...",
             m_process_->task.task_id());

  switch (m_process_->err_before_exec) {
  case CraneErr::kProtobufError:
    ActivateTaskStatusChange_(crane::grpc::TaskStatus::Failed,
                              ExitCode::kExitCodeSpawnProcessFail,
                              std::nullopt);
    break;

  case CraneErr::kCgroupError:
    ActivateTaskStatusChange_(crane::grpc::TaskStatus::Failed,
                              ExitCode::kExitCodeCgroupError, std::nullopt);
    break;

  default:
    break;
  }

  ProcSigchldInfo& sigchld_info = m_process_->sigchld_info;
  if (m_process_->task.type() == crane::grpc::Batch || m_process_->IsCrun()) {
    // For a Batch task, the end of the process means it is done.
    if (sigchld_info.is_terminated_by_signal) {
      if (m_process_->cancelled_by_user)
        ActivateTaskStatusChange_(
            crane::grpc::TaskStatus::Cancelled,
            sigchld_info.value + ExitCode::kTerminationSignalBase,
            std::nullopt);
      else if (m_process_->terminated_by_timeout)
        ActivateTaskStatusChange_(
            crane::grpc::TaskStatus::ExceedTimeLimit,
            sigchld_info.value + ExitCode::kTerminationSignalBase,
            std::nullopt);
      else
        ActivateTaskStatusChange_(
            crane::grpc::TaskStatus::Failed,
            sigchld_info.value + ExitCode::kTerminationSignalBase,
            std::nullopt);
    } else
      ActivateTaskStatusChange_(crane::grpc::TaskStatus::Completed,
                                sigchld_info.value, std::nullopt);
  } else /* Calloc */ {
    // For a COMPLETING Calloc task with a process running,
    // the end of this process means that this task is done.
    if (sigchld_info.is_terminated_by_signal)
      ActivateTaskStatusChange_(
          crane::grpc::TaskStatus::Completed,
          sigchld_info.value + ExitCode::kTerminationSignalBase, std::nullopt);
    else
      ActivateTaskStatusChange_(crane::grpc::TaskStatus::Completed,
                                sigchld_info.value, std::nullopt);
  }
}

void TaskManager::ActivateTaskStatusChange_(crane::grpc::TaskStatus new_status,
                                            uint32_t exit_code,
                                            std::optional<std::string> reason) {
  TaskInstance* instance = m_process_.get();
  if (instance->task.type() == crane::grpc::Batch || instance->IsCrun()) {
    const std::string& path = instance->meta->parsed_sh_script_path;
    if (!path.empty())
      g_thread_pool->detach_task([p = path]() { util::os::DeleteFile(p); });
  }

  bool orphaned = instance->orphaned;
  // Free the TaskInstance structure
  m_process_.release();
  if (!orphaned)
    g_craned_client->TaskStatusChange(m_process_->task.task_id(), new_status,
                                      exit_code, reason);
}

void TaskManager::EvSigchldCb_() {
  int status;
  pid_t pid;
  while (true) {
    pid = waitpid(-1, &status, WNOHANG
                  /* TODO(More status tracing): | WUNTRACED | WCONTINUED */);

    if (pid > 0) {
      auto sigchld_info = std::make_unique<ProcSigchldInfo>();

      if (WIFEXITED(status)) {
        // Exited with status WEXITSTATUS(status)
        sigchld_info->pid = pid;
        sigchld_info->is_terminated_by_signal = false;
        sigchld_info->value = WEXITSTATUS(status);

        CRANE_TRACE("Receiving SIGCHLD for pid {}. Signaled: false, Status: {}",
                    pid, WEXITSTATUS(status));
      } else if (WIFSIGNALED(status)) {
        // Killed by signal WTERMSIG(status)
        sigchld_info->pid = pid;
        sigchld_info->is_terminated_by_signal = true;
        sigchld_info->value = WTERMSIG(status);

        CRANE_TRACE("Receiving SIGCHLD for pid {}. Signaled: true, Signal: {}",
                    pid, WTERMSIG(status));
      }
      /* Todo(More status tracing):
       else if (WIFSTOPPED(status)) {
        printf("stopped by signal %d\n", WSTOPSIG(status));
      } else if (WIFCONTINUED(status)) {
        printf("continued\n");
      } */

      m_mtx_.Lock();
      ABSL_ASSERT(m_process_);
      m_process_->sigchld_info = *sigchld_info;

      if (m_process_->IsCrun())
        // TaskStatusChange of a crun task is triggered in
        // CforedManager.
        g_cfored_manager->TaskProcStopped();
      else /* Batch / Calloc */ {
        // If the TaskInstance has no process left,
        // send TaskStatusChange for this task.
        // See the comment of EvActivateTaskStatusChange_.
        TaskStopAndDoStatusChange();
      }
      m_mtx_.Unlock();
    } else if (pid == 0) {
      break;
    } else if (pid < 0) {
      if (errno != ECHILD)
        CRANE_DEBUG("waitpid() error: {}, {}", errno, strerror(errno));
      break;
    }
  }
}

}  // namespace Supervisor
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

#include "crane/PrologEpilogExecutor.h"

#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <climits>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <expected>
#include <utility>
#include <vector>

#include "crane/Logger.h"
#include "crane/String.h"

namespace util::os {

namespace {

std::pair<int, int> DecodeWaitStatus(int status) {
  if (WIFEXITED(status)) return {WEXITSTATUS(status), 0};
  if (WIFSIGNALED(status)) return {0, WTERMSIG(status)};
  return {status, 0};
}

}  // namespace

PrologEpilogExecutor::PrologEpilogExecutor(
    std::chrono::milliseconds poll_interval)
    : m_poll_interval_(poll_interval), m_loop_(uvw::loop::create()) {
  m_submit_async_ = m_loop_->resource<uvw::async_handle>();
  m_submit_async_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        DrainPendingRuns_();
      });

  m_stop_async_ = m_loop_->resource<uvw::async_handle>();
  m_stop_async_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) { StopInLoop_(); });

  m_poll_timer_ = m_loop_->resource<uvw::timer_handle>();
  m_poll_timer_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle&) { PollChildren_(); });

  m_loop_thread_ = std::thread([this] { LoopThread_(); });

  std::unique_lock lk(m_start_mtx_);
  m_start_cv_.wait(lk, [this] { return m_loop_ready_; });
}

PrologEpilogExecutor::~PrologEpilogExecutor() { Shutdown(); }

void PrologEpilogExecutor::Submit(RunPrologEpilogArgs args, Callback callback) {
  std::scoped_lock lk(m_pending_mtx_);
  if (m_shutdown_requested_.load(std::memory_order_acquire)) {
    CRANE_TRACE("Dropping prolog/epilog after executor shutdown.");
    return;
  }
  if (!m_submit_async_ || m_submit_async_->closing()) {
    CRANE_TRACE("Dropping prolog/epilog because executor is closing.");
    return;
  }
  m_pending_runs_.push_back(
      PendingRun{.args = std::move(args), .callback = std::move(callback)});
  m_submit_async_->send();
}

void PrologEpilogExecutor::Shutdown() {
  bool expected = false;
  if (!m_shutdown_requested_.compare_exchange_strong(expected, true)) return;

  if (m_stop_async_) m_stop_async_->send();
  if (m_loop_thread_.joinable()) m_loop_thread_.join();
}

void PrologEpilogExecutor::LoopThread_() {
  util::SetCurrentThreadName("PrologEpiExec");
  {
    std::scoped_lock lk(m_start_mtx_);
    m_loop_ready_ = true;
  }
  m_start_cv_.notify_all();

  m_poll_timer_->start(m_poll_interval_, m_poll_interval_);
  m_loop_->run();
}

void PrologEpilogExecutor::DrainPendingRuns_() {
  std::deque<PendingRun> runs;
  {
    std::scoped_lock lk(m_pending_mtx_);
    runs.swap(m_pending_runs_);
  }

  if (m_shutdown_requested_.load(std::memory_order_acquire)) return;

  while (!runs.empty()) {
    StartRun_(std::move(runs.front()));
    runs.pop_front();
  }
}

void PrologEpilogExecutor::StartRun_(PendingRun run) {
  const uint64_t run_id = m_next_run_id_++;
  ActiveRun active{
      .id = run_id,
      .args = std::move(run.args),
      .callback = std::move(run.callback),
      .start_time = std::chrono::steady_clock::now(),
  };
  m_active_runs_.emplace(run_id, std::move(active));
  StartNextScript_(run_id);
}

void PrologEpilogExecutor::StartNextScript_(uint64_t run_id) {
  auto it = m_active_runs_.find(run_id);
  if (it == m_active_runs_.end()) return;
  ActiveRun& run = it->second;

  CloseScriptHandles_(&run);

  if (run.script_index >= run.args.scripts.size()) {
    CompleteRun_(run_id, PrologEpilogResult{
                             .ok = true,
                             .output = std::move(run.output),
                         });
    return;
  }

  const std::string script = run.args.scripts[run.script_index];
  if (!util::os::IsAbsolutePath(script)) {
    CRANE_ERROR("Script path '{}' is not absolute.", script);
    FailRun_(run_id, 1, 0, false, script);
    return;
  }

  const auto timeout = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::seconds(run.args.timeout_sec));
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - run.start_time);
  if (elapsed >= timeout) {
    CRANE_TRACE("Script '{}' timed out before it started.", script);
    FailRun_(run_id, 1, SIGKILL, true, script);
    return;
  }

  int stdout_pipe[2];
  if (pipe(stdout_pipe) != 0) {
    CRANE_ERROR("Failed to create pipe for script '{}': {}", script,
                strerror(errno));
    FailRun_(run_id, 1, 0, false, script);
    return;
  }

  std::vector<std::string> env_storage;
  std::vector<char*> envp;
  env_storage.reserve(run.args.envs.size());
  envp.reserve(run.args.envs.size() + 1);
  for (const auto& [name, value] : run.args.envs) {
    env_storage.emplace_back(name + "=" + value);
    envp.emplace_back(env_storage.back().data());
  }
  envp.emplace_back(nullptr);

  const char* exec_argv[] = {script.c_str(), nullptr};
  pid_t pid = fork();
  if (pid == -1) {
    CRANE_ERROR("Failed to fork for script '{}': {}", script, strerror(errno));
    close(stdout_pipe[0]);
    close(stdout_pipe[1]);
    FailRun_(run_id, 1, 0, false, script);
    return;
  }

  if (pid == 0) {
    close(stdout_pipe[0]);
    if (dup2(stdout_pipe[1], STDOUT_FILENO) == -1 ||
        dup2(stdout_pipe[1], STDERR_FILENO) == -1) {
      close(stdout_pipe[1]);
      const char msg[] = "[Subprocess] dup2 failed\n";
      write(STDERR_FILENO, msg, sizeof(msg) - 1);
    }

    CloseFdInChild_();
    setpgid(0, 0);

    if (run.args.at_child_setup_cb) {
      if (!run.args.at_child_setup_cb(getpid())) {
        const char msg[] = "[Subprocess] child setup callback failed\n";
        write(STDERR_FILENO, msg, sizeof(msg) - 1);
        _exit(EXIT_FAILURE);
      }
    }

    if (setgid(run.args.run_gid) != 0) {
      const char msg[] = "[Subprocess] setgid failed\n";
      write(STDERR_FILENO, msg, sizeof(msg) - 1);
      _exit(EXIT_FAILURE);
    }
    if (setuid(run.args.run_uid) != 0) {
      const char msg[] = "[Subprocess] setuid failed\n";
      write(STDERR_FILENO, msg, sizeof(msg) - 1);
      _exit(EXIT_FAILURE);
    }

    execvpe(exec_argv[0], const_cast<char* const*>(exec_argv), envp.data());
    const char msg[] = "[Subprocess] execvp failed\n";
    write(STDERR_FILENO, msg, sizeof(msg) - 1);
    _exit(EXIT_FAILURE);
  }

  close(stdout_pipe[1]);

  run.pid = pid;
  run.child_exited = false;
  run.pipe_ended = false;
  run.has_error = false;
  run.timed_out = false;
  run.exit_status = 0;
  run.script_output.clear();
  run.current_script = script;
  m_pid_to_run_id_[pid] = run_id;

  run.pipe_handle = m_loop_->uninitialized_resource<uvw::pipe_handle>(false);
  if (int err = run.pipe_handle->init(); err != 0) {
    CRANE_ERROR("Failed to init pipe_handle for '{}': {}", script,
                uv_strerror(err));
    close(stdout_pipe[0]);
    run.pipe_handle.reset();
    run.pipe_ended = true;
    run.has_error = true;
    util::os::KillPg(pid);
    return;
  }
  if (int err = run.pipe_handle->open(stdout_pipe[0]); err != 0) {
    CRANE_ERROR("Failed to open pipe_handle for '{}': {}", script,
                uv_strerror(err));
    close(stdout_pipe[0]);
    if (!run.pipe_handle->closing()) run.pipe_handle->close();
    run.pipe_handle.reset();
    run.pipe_ended = true;
    run.has_error = true;
    util::os::KillPg(pid);
    return;
  }

  run.pipe_handle->on<uvw::data_event>(
      [this, run_id](const uvw::data_event& event, uvw::pipe_handle&) {
        auto it = m_active_runs_.find(run_id);
        if (it == m_active_runs_.end()) return;
        ActiveRun& run = it->second;
        if (event.length <= 0) return;
        const size_t remain =
            run.args.output_size > run.script_output.size()
                ? run.args.output_size - run.script_output.size()
                : 0;
        if (remain > 0) {
          run.script_output.append(event.data.get(),
                                   std::min<size_t>(event.length, remain));
        }
      });

  run.pipe_handle->on<uvw::end_event>(
      [this, run_id](const uvw::end_event&, uvw::pipe_handle& handle) {
        auto it = m_active_runs_.find(run_id);
        if (it == m_active_runs_.end()) return;
        if (!handle.closing()) handle.close();
        it->second.pipe_handle.reset();
        it->second.pipe_ended = true;
        TryFinishScript_(run_id);
      });

  run.pipe_handle->on<uvw::error_event>(
      [this, run_id](uvw::error_event& event, uvw::pipe_handle& handle) {
        auto it = m_active_runs_.find(run_id);
        if (it == m_active_runs_.end()) return;
        ActiveRun& run = it->second;
        CRANE_WARN("Pipe error for script '{}'({}): {}.", run.current_script,
                   run.pid, event.what());
        if (!handle.closing()) handle.close();
        run.pipe_handle.reset();
        run.pipe_ended = true;
        run.has_error = true;
        if (run.pid > 0) util::os::KillPg(run.pid);
        TryFinishScript_(run_id);
      });
  run.pipe_handle->read();

  run.timeout_timer = m_loop_->resource<uvw::timer_handle>();
  run.timeout_timer->on<uvw::timer_event>(
      [this, run_id](const uvw::timer_event&, uvw::timer_handle& handle) {
        auto it = m_active_runs_.find(run_id);
        if (it == m_active_runs_.end()) return;
        ActiveRun& run = it->second;
        CRANE_TRACE("Script '{}' timed out; killing process group {}.",
                    run.current_script, run.pid);
        run.timed_out = true;
        if (run.pid > 0) util::os::KillPg(run.pid);
        if (!handle.closing()) handle.close();
        run.timeout_timer.reset();
      });
  run.timeout_timer->start(timeout - elapsed, std::chrono::milliseconds(0));
}

void PrologEpilogExecutor::PollChildren_() {
  std::vector<pid_t> pids;
  pids.reserve(m_pid_to_run_id_.size());
  for (const auto& [pid, _] : m_pid_to_run_id_) pids.emplace_back(pid);

  for (pid_t pid : pids) {
    auto pid_it = m_pid_to_run_id_.find(pid);
    if (pid_it == m_pid_to_run_id_.end()) continue;

    int status = 0;
    pid_t rc = waitpid(pid, &status, WNOHANG);
    if (rc == pid) {
      const uint64_t run_id = pid_it->second;
      m_pid_to_run_id_.erase(pid_it);
      auto run_it = m_active_runs_.find(run_id);
      if (run_it == m_active_runs_.end()) continue;
      run_it->second.exit_status = status;
      run_it->second.child_exited = true;
      TryFinishScript_(run_id);
    } else if (rc == -1) {
      if (errno != ECHILD) {
        CRANE_ERROR("waitpid failed for pid={}: {}", pid, strerror(errno));
      }
      const uint64_t run_id = pid_it->second;
      m_pid_to_run_id_.erase(pid_it);
      auto run_it = m_active_runs_.find(run_id);
      if (run_it == m_active_runs_.end()) continue;
      run_it->second.exit_status = 1;
      run_it->second.has_error = true;
      run_it->second.child_exited = true;
      TryFinishScript_(run_id);
    }
  }
}

void PrologEpilogExecutor::TryFinishScript_(uint64_t run_id) {
  auto it = m_active_runs_.find(run_id);
  if (it == m_active_runs_.end()) return;
  ActiveRun& run = it->second;
  if (!run.child_exited || !run.pipe_ended) return;

  if (run.timeout_timer) {
    if (!run.timeout_timer->closing()) run.timeout_timer->close();
    run.timeout_timer.reset();
  }

  if (run.has_error) {
    FailRun_(run_id, 1, 0, run.timed_out, run.current_script);
    return;
  }

  auto [exit_code, signal_num] = DecodeWaitStatus(run.exit_status);
  if (exit_code != 0 || signal_num != 0) {
    CRANE_TRACE("Script '{}' failed (exit_code={}, signal={}), output: {}.",
                run.current_script, exit_code, signal_num, run.script_output);
    FailRun_(run_id, exit_code, signal_num, run.timed_out, run.current_script);
    return;
  }

  run.output += run.script_output;
  run.script_index++;
  StartNextScript_(run_id);
}

void PrologEpilogExecutor::CompleteRun_(uint64_t run_id,
                                        PrologEpilogResult result) {
  auto it = m_active_runs_.find(run_id);
  if (it == m_active_runs_.end()) return;

  CloseScriptHandles_(&it->second);
  Callback callback = std::move(it->second.callback);
  m_active_runs_.erase(it);

  if (!m_shutdown_requested_.load(std::memory_order_acquire) && callback) {
    callback(std::move(result));
  }
}

void PrologEpilogExecutor::FailRun_(uint64_t run_id, int exit_code,
                                    int signal_num, bool timed_out,
                                    std::string failed_script) {
  auto it = m_active_runs_.find(run_id);
  if (it == m_active_runs_.end()) return;
  CompleteRun_(run_id, PrologEpilogResult{
                           .ok = false,
                           .exit_code = exit_code,
                           .signal_num = signal_num,
                           .timed_out = timed_out,
                           .failed_script = std::move(failed_script),
                           .output = std::move(it->second.output),
                       });
}

void PrologEpilogExecutor::StopInLoop_() {
  std::deque<PendingRun> pending;
  {
    std::scoped_lock lk(m_pending_mtx_);
    pending.swap(m_pending_runs_);
  }

  for (auto& [_, run] : m_active_runs_) {
    if (run.pid > 0) {
      util::os::KillPg(run.pid);
      int status = 0;
      waitpid(run.pid, &status, WNOHANG);
    }
    CloseScriptHandles_(&run);
  }
  m_active_runs_.clear();
  m_pid_to_run_id_.clear();

  if (m_poll_timer_ && !m_poll_timer_->closing()) m_poll_timer_->close();
  if (m_submit_async_ && !m_submit_async_->closing()) m_submit_async_->close();
  if (m_stop_async_ && !m_stop_async_->closing()) m_stop_async_->close();
  m_loop_->walk([](auto&& handle) {
    if (!handle.closing()) handle.close();
  });
}

void PrologEpilogExecutor::CloseScriptHandles_(ActiveRun* run) {
  if (run == nullptr) return;
  if (run->pipe_handle) {
    if (!run->pipe_handle->closing()) run->pipe_handle->close();
    run->pipe_handle.reset();
  }
  if (run->timeout_timer) {
    if (!run->timeout_timer->closing()) run->timeout_timer->close();
    run->timeout_timer.reset();
  }
  if (run->pid > 0) {
    m_pid_to_run_id_.erase(run->pid);
    run->pid = -1;
  }
}

void PrologEpilogExecutor::CloseFdInChild_() {
#if defined(__linux__) && defined(SYS_close_range)
  syscall(SYS_close_range, 3, UINT_MAX, 0);
#else
  util::os::CloseFdFrom(3);
#endif
}

}  // namespace util::os

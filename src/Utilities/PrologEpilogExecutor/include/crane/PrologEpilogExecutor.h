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

#include <sys/types.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <uvw.hpp>

#include "crane/OS.h"

namespace util::os {

struct PrologEpilogResult {
  bool ok{false};
  int exit_code{0};
  int signal_num{0};
  bool timed_out{false};
  std::string failed_script;
  std::string output;
};

class PrologEpilogExecutor {
 public:
  using Callback = std::function<void(PrologEpilogResult)>;

  explicit PrologEpilogExecutor(
      std::chrono::milliseconds poll_interval = std::chrono::milliseconds(50));
  ~PrologEpilogExecutor();

  PrologEpilogExecutor(const PrologEpilogExecutor&) = delete;
  PrologEpilogExecutor& operator=(const PrologEpilogExecutor&) = delete;

  void Submit(RunPrologEpilogArgs args, Callback callback);
  void Shutdown();

 private:
  struct PendingRun {
    RunPrologEpilogArgs args;
    Callback callback;
  };

  struct ActiveRun {
    uint64_t id;
    RunPrologEpilogArgs args;
    Callback callback;
    std::chrono::steady_clock::time_point start_time;
    size_t script_index{0};
    pid_t pid{-1};
    bool child_exited{false};
    bool pipe_ended{false};
    bool has_error{false};
    bool timed_out{false};
    int exit_status{0};
    std::string output;
    std::string script_output;
    std::string current_script;
    std::shared_ptr<uvw::pipe_handle> pipe_handle;
    std::shared_ptr<uvw::timer_handle> timeout_timer;
  };

  void LoopThread_();
  void DrainPendingRuns_();
  void StartRun_(PendingRun run);
  void StartNextScript_(uint64_t run_id);
  void PollChildren_();
  void TryFinishScript_(uint64_t run_id);
  void CompleteRun_(uint64_t run_id, PrologEpilogResult result);
  void FailRun_(uint64_t run_id, int exit_code, int signal_num, bool timed_out,
                std::string failed_script);
  void StopInLoop_();
  void CloseScriptHandles_(ActiveRun* run);
  void CloseFdInChild_();

  const std::chrono::milliseconds m_poll_interval_;
  std::shared_ptr<uvw::loop> m_loop_;
  std::shared_ptr<uvw::async_handle> m_submit_async_;
  std::shared_ptr<uvw::async_handle> m_stop_async_;
  std::shared_ptr<uvw::timer_handle> m_poll_timer_;

  std::mutex m_pending_mtx_;
  std::deque<PendingRun> m_pending_runs_;

  std::mutex m_start_mtx_;
  std::condition_variable m_start_cv_;
  bool m_loop_ready_{false};

  std::atomic_bool m_shutdown_requested_{false};
  std::thread m_loop_thread_;
  uint64_t m_next_run_id_{1};
  std::unordered_map<uint64_t, ActiveRun> m_active_runs_;
  std::unordered_map<pid_t, uint64_t> m_pid_to_run_id_;
};

}  // namespace util::os

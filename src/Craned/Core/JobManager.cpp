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

#include "JobManager.h"

#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/util/delimited_message_util.h>
#include <pty.h>
#include <sys/wait.h>

#include "CranedPublicDefs.h"
#include "CtldClient.h"
#include "SupervisorKeeper.h"
#include "crane/PluginClient.h"
#include "crane/String.h"
#include "protos/PublicDefs.pb.h"
#include "protos/Supervisor.pb.h"

namespace Craned {
using Common::kStepRequestCheckIntervalMs;
StepInstance::StepInstance(const crane::grpc::StepToD& step_to_d)
    : job_id(step_to_d.job_id()),
      step_id(step_to_d.step_id()),
      step_to_d(step_to_d),
      status(StepStatus::Configuring) {}

StepInstance::StepInstance(const crane::grpc::StepToD& step_to_d,
                           pid_t supv_pid)
    : job_id(step_to_d.job_id()),
      step_id(step_to_d.step_id()),
      step_to_d(step_to_d),
      supv_pid(supv_pid),
      status(StepStatus::Running) {}

bool StepInstance::IsDaemon() const {
  return step_to_d.step_type() == crane::grpc::StepType::DAEMON;
}

bool StepInstance::CanOperate() const { return status != StepStatus::Running; }

EnvMap JobInD::GetJobEnvMap() {
  auto env_map = CgroupManager::GetResourceEnvMapByResInNode(job_to_d.res());

  // TODO: Move all job level env to here.
  env_map.emplace("CRANE_JOB_ID", std::to_string(job_to_d.job_id()));
  auto& daemon_step_to_d = step_map.at(kDaemonStepId)->step_to_d;
  env_map.emplace("CRANE_JOB_ACCOUNT", job_to_d.account());

  auto time_limit_dur =
      std::chrono::seconds(daemon_step_to_d.time_limit().seconds()) +
      std::chrono::nanoseconds(daemon_step_to_d.time_limit().nanos());

  env_map.emplace(
      "CRANE_JOB_END_TIME",
      std::to_string((std::chrono::system_clock::now() + time_limit_dur)
                         .time_since_epoch()
                         .count()));
  env_map.emplace("CRANE_JOB_ID", std::to_string(job_id));
  env_map.emplace("CRANE_JOB_NAME", daemon_step_to_d.name());
  env_map.emplace("CRANE_JOB_NODELIST",
                  absl::StrJoin(daemon_step_to_d.nodelist(), ";"));
  env_map.emplace("CRANE_JOB_NUM_NODES",
                  std::to_string(daemon_step_to_d.node_num()));
  env_map.emplace("CRANE_JOB_PARTITION", job_to_d.partition());
  env_map.emplace("CRANE_JOB_QOS", job_to_d.qos());

  return env_map;
}

JobManager::JobManager() {
  m_uvw_loop_ = uvw::loop::create();

  m_sigchld_handle_ = m_uvw_loop_->resource<uvw::signal_handle>();
  m_sigchld_handle_->on<uvw::signal_event>(
      [this](const uvw::signal_event&, uvw::signal_handle&) {
        EvSigchldCb_();
      });

  if (int rc = m_sigchld_handle_->start(SIGCHLD); rc != 0) {
    CRANE_ERROR("Failed to start the SIGCHLD handle: {}", uv_err_name(rc));
  }

  m_sigint_handle_ = m_uvw_loop_->resource<uvw::signal_handle>();
  m_sigint_handle_->on<uvw::signal_event>(
      [this](const uvw::signal_event&, uvw::signal_handle&) { EvSigintCb_(); });
  if (int rc = m_sigint_handle_->start(SIGINT); rc != 0) {
    CRANE_ERROR("Failed to start the SIGINT handle: {}", uv_err_name(rc));
  }

  m_sigterm_handle_ = m_uvw_loop_->resource<uvw::signal_handle>();
  m_sigterm_handle_->on<uvw::signal_event>(
      [this](const uvw::signal_event&, uvw::signal_handle&) { EvSigintCb_(); });
  if (int rc = m_sigterm_handle_->start(SIGTERM); rc != 0) {
    CRANE_ERROR("Failed to start the SIGTERM handle: {}", uv_err_name(rc));
  }

  m_check_supervisor_timer_handle_ = m_uvw_loop_->resource<uvw::timer_handle>();
  m_check_supervisor_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle& handle) {
        EvCheckSupervisorRunning_();
      });
  m_check_supervisor_timer_handle_->start(std::chrono::milliseconds{0},
                                          std::chrono::milliseconds{500});

  // gRPC Alloc step Event
  m_grpc_alloc_step_async_handle_ = m_uvw_loop_->resource<uvw::async_handle>();
  m_grpc_alloc_step_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanGrpcAllocStepsQueueCb_();
      });

  // gRPC Execute step Event
  m_grpc_execute_step_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_grpc_execute_step_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanGrpcExecuteStepQueueCb_();
      });

  // Task Status Change Event
  m_task_status_change_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_task_status_change_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanTaskStatusChangeQueueCb_();
      });

  m_change_task_time_limit_async_handle_ =
      m_uvw_loop_->resource<uvw::async_handle>();
  m_change_task_time_limit_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanChangeTaskTimeLimitQueueCb_();
      });
  m_change_task_time_limit_timer_handle_ =
      m_uvw_loop_->resource<uvw::timer_handle>();
  m_change_task_time_limit_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle& handle) {
        m_change_task_time_limit_async_handle_->send();
      });
  m_change_task_time_limit_timer_handle_->start(
      std::chrono::milliseconds{kStepRequestCheckIntervalMs * 3},
      std::chrono::milliseconds{kStepRequestCheckIntervalMs});

  m_terminate_step_async_handle_ = m_uvw_loop_->resource<uvw::async_handle>();
  m_terminate_step_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanTerminateTaskQueueCb_();
      });
  m_terminate_step_timer_handle_ = m_uvw_loop_->resource<uvw::timer_handle>();
  m_terminate_step_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle& handle) {
        m_terminate_step_async_handle_->send();
      });
  m_terminate_step_timer_handle_->start(
      std::chrono::milliseconds{kStepRequestCheckIntervalMs * 3},
      std::chrono::milliseconds{kStepRequestCheckIntervalMs});

  m_uvw_thread_ = std::thread([this]() {
    util::SetCurrentThreadName("JobMgrLoopThr");
    auto idle_handle = m_uvw_loop_->resource<uvw::idle_handle>();
    idle_handle->on<uvw::idle_event>(
        [this](const uvw::idle_event&, uvw::idle_handle& h) {
          if (m_is_ending_now_) {
            h.parent().walk([](auto&& h) { h.close(); });
            h.parent().stop();
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
        });
    if (idle_handle->start() != 0) {
      CRANE_ERROR("Failed to start the idle event in JobManager loop.");
    }
    m_uvw_loop_->run();
  });
}

CraneErrCode JobManager::Recover(
    std::unordered_map<task_id_t, JobInD>&& job_map,
    absl::flat_hash_map<std::pair<job_id_t, step_id_t>,
                        std::unique_ptr<StepInstance>>&& step_map) {
  CRANE_INFO("Job allocation [{}] recovered.",
             absl::StrJoin(job_map | std::views::keys, ","));

  for (auto&& job : job_map | std::views::values) {
    job_id_t job_id = job.job_id;
    uid_t uid = job.Uid();
    m_job_map_.Emplace(job_id, std::move(job));

    auto uid_map = m_uid_to_job_ids_map_.GetMapExclusivePtr();
    if (uid_map->contains(uid)) {
      uid_map->at(uid).RawPtr()->emplace(job_id);
    } else {
      uid_map->emplace(uid, absl::flat_hash_set<task_id_t>({job_id}));
    }
  }

  for (auto&& elem : step_map) {
    job_id_t job_id = elem.first.first;
    step_id_t step_id = elem.first.second;
    auto job = m_job_map_.GetValueExclusivePtr(job_id);
    CRANE_ASSERT(job);
    absl::MutexLock lk(job->step_map_mtx.get());
    job->step_map.emplace(step_id, std::move(elem.second));
    CRANE_TRACE("[Step #{}.{}] was recovered.", job_id, step_id);
  }
  return CraneErrCode::SUCCESS;
}

JobManager::~JobManager() {
  CRANE_DEBUG("JobManager is being destroyed.");
  m_is_ending_now_ = true;
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}

bool JobManager::AllocJobs(std::vector<JobInD>&& jobs) {
  CRANE_DEBUG("Allocating {} job", jobs.size());

  auto begin = std::chrono::steady_clock::now();

  {
    auto job_map_ptr = m_job_map_.GetMapExclusivePtr();
    auto uid_map_ptr = m_uid_to_job_ids_map_.GetMapExclusivePtr();
    for (auto& job : jobs) {
      task_id_t job_id = job.job_id;
      uid_t uid = job.Uid();
      CRANE_TRACE("Create lazily allocated cgroups for job #{}, uid {}", job_id,
                  uid);
      job_map_ptr->emplace(job_id, std::move(job));

      if (uid_map_ptr->contains(uid)) {
        uid_map_ptr->at(uid).RawPtr()->emplace(job_id);
      } else {
        uid_map_ptr->emplace(uid, absl::flat_hash_set<task_id_t>({job_id}));
      }
    }
  }

  auto end = std::chrono::steady_clock::now();
  CRANE_TRACE("Create cgroups costed {} ms",
              std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
                  .count());
  return true;
}

bool JobManager::FreeJobs(std::set<task_id_t>&& job_ids) {
  std::unordered_map<job_id_t, std::unordered_set<step_id_t>> job_steps;
  std::vector<uid_t> uid_vec;
  {
    auto map_ptr = m_job_map_.GetMapExclusivePtr();
    for (auto job_id : job_ids) {
      if (!map_ptr->contains(job_id)) {
        CRANE_WARN("Try to free nonexistent job#{}", job_ids);
        return false;
      }
      auto* job = map_ptr->at(job_id).RawPtr();
      uid_vec.push_back(job->Uid());
      absl::MutexLock lk(job->step_map_mtx.get());
      job_steps[job_id] = job->step_map | std::views::keys |
                          std::ranges::to<std::unordered_set>();
    }
  }
  {
    auto uid_map = m_uid_to_job_ids_map_.GetMapExclusivePtr();
    for (auto [idx, job_id] : job_ids | std::ranges::views::enumerate) {
      bool erase{false};
      {
        auto& value = uid_map->at(uid_vec[idx]);
        value.RawPtr()->erase(job_id);
        erase = value.RawPtr()->empty();
      }
      if (erase) {
        uid_map->erase(uid_vec[idx]);
      }
    }
  }

  g_thread_pool->detach_task([this, steps = std::move(job_steps)] {
    this->CleanUpJobAndStepsAsync(std::move(steps));
  });
  return true;
}

void JobManager::AllocSteps(std::vector<StepToD>&& steps) {
  if (m_is_ending_now_.load(std::memory_order_acquire)) {
    CRANE_TRACE("JobManager is ending now, ignoring the request.");
    return;
  }

  CRANE_TRACE("Allocating step [{}].",
              absl::StrJoin(steps | std::views::transform(GetStepIdStr), ","));

  auto job_map_ptr = m_job_map_.GetMapExclusivePtr();
  for (const auto& step : steps) {
    if (!job_map_ptr->contains(step.job_id())) {
      CRANE_WARN("Try to allocate step for nonexistent job#{}, ignoring it.",
                 step.job_id());
    } else {
      // Simply wrap the Task structure within an Execution structure and
      // pass it to the event loop. The cgroup field of this task is initialized
      // in the corresponding handler (EvGrpcExecuteTaskCb_).
      auto step_inst = std::make_unique<StepInstance>(step);
      step_inst->step_to_d = std::move(step);
      EvQueueAllocateStepElem elem{.step_inst = std::move(step_inst)};
      m_grpc_alloc_step_queue_.enqueue(std::move(elem));
    }
  }
  m_grpc_alloc_step_async_handle_->send();
}

bool JobManager::EvCheckSupervisorRunning_() {
  std::vector<JobInD> jobs_to_clean;
  std::vector<StepInstance*> steps_to_clean;
  {
    absl::MutexLock lk(&m_free_job_step_mtx_);
    for (auto& [step, retry_count] : m_completing_step_retry_map_) {
      job_id_t job_id = step->job_id;
      step_id_t step_id = step->step_id;
      auto exists = kill(step->supv_pid, 0) == 0;

      if (exists) {
        retry_count++;
      }
      if (retry_count >= kMaxSupervisorCheckRetryCount) {
        CRANE_WARN(
            "[Step #{}.{}] Supervisor is still running after {} checks, will "
            "clean up now!",
            job_id, step_id, kMaxSupervisorCheckRetryCount);
        // TODO: Send status change for dead step
      } else {
        if (exists) continue;
      }
      steps_to_clean.push_back(step);
    }
    for (auto* step : steps_to_clean) {
      m_completing_step_retry_map_.erase(step);
      if (!m_completing_job_.contains(step->job_id)) continue;
      m_completing_job_.at(step->job_id).step_map.erase(step->step_id);
      if (m_completing_job_.at(step->job_id).step_map.empty()) {
        jobs_to_clean.emplace_back(
            std::move(m_completing_job_.at(step->job_id)));
        m_completing_job_.erase(step->job_id);
      }
    }
  }

  if (!steps_to_clean.empty()) {
    CRANE_TRACE(
        "Supervisor for Step [{}] found to be exited",
        absl::StrJoin(steps_to_clean | std::views::transform([](auto* step) {
                        return std::make_pair(step->job_id, step->step_id);
                      }) | std::views::transform(util::StepIdPairToString),
                      ","));
    FreeStepAllocation_(std::move(steps_to_clean));
    FreeJobAllocation_(std::move(jobs_to_clean));
  }

  return m_completing_step_retry_map_.empty();
}

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
void JobManager::EvSigchldCb_() {
  int status;
  pid_t pid;
  while (true) {
    pid = waitpid(-1, &status, WNOHANG
                  /* TODO(More status tracing): | WUNTRACED | WCONTINUED */);

    if (pid > 0) {
      CRANE_TRACE("Child pid {} exit", pid);
      // We do nothing now
    } else if (pid == 0) {
      // There's no child that needs reaping.
      break;
    } else if (pid < 0) {
      if (errno != ECHILD)
        CRANE_DEBUG("waitpid() error: {}, {}", errno, strerror(errno));
      break;
    }
  }
}

void JobManager::EvSigintCb_() {
  CRANE_TRACE("Triggering exit event...");
  if (m_sigint_cb_) m_sigint_cb_();
  m_is_ending_now_ = true;
}

void JobManager::EvCleanGrpcAllocStepsQueueCb_() {
  EvQueueAllocateStepElem elem;

  while (m_grpc_alloc_step_queue_.try_dequeue(elem)) {
    // Once ExecuteTask RPC is processed, the Execution goes into m_job_map_.

    std::unique_ptr step_inst = std::move(elem.step_inst);

    if (!m_job_map_.Contains(step_inst->job_id)) {
      CRANE_ERROR("[Job #{}.{}]Failed to find allocation", step_inst->job_id,
                  step_inst->step_id);
      elem.ok_prom.set_value(CraneErrCode::ERR_CGROUP);
      continue;
    }
    elem.ok_prom.set_value(CraneErrCode::SUCCESS);

    g_thread_pool->detach_task([this, execution = step_inst.release()] {
      LaunchStepMt_(std::unique_ptr<StepInstance>(execution));
    });
  }
}

void JobManager::Wait() {
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}

CraneErrCode JobManager::KillPid_(pid_t pid, int signum) {
  // TODO: Add timer which sends SIGTERM for those tasks who
  //  will not quit when receiving SIGINT.
  CRANE_TRACE("Killing pid {} with signal {}", pid, signum);

  // Send the signal to the whole process group.
  int err = kill(-pid, signum);

  if (err == 0)
    return CraneErrCode::SUCCESS;
  else {
    std::error_code ec(errno, std::system_category());
    CRANE_TRACE("kill failed. error: {}", ec.message());
    return CraneErrCode::ERR_GENERIC_FAILURE;
  }
}

void JobManager::SetSigintCallback(std::function<void()> cb) {
  m_sigint_cb_ = std::move(cb);
}

CraneErrCode JobManager::SpawnSupervisor_(JobInD* job, StepInstance* step) {
  using google::protobuf::io::FileInputStream;
  using google::protobuf::io::FileOutputStream;
  using google::protobuf::util::ParseDelimitedFromZeroCopyStream;
  using google::protobuf::util::SerializeDelimitedToZeroCopyStream;

  using crane::grpc::supervisor::CanStartMessage;
  using crane::grpc::supervisor::ChildProcessReady;

  job_id_t job_id = step->job_id;
  step_id_t step_id = step->step_id;

  std::array<int, 2> supervisor_craned_pipe{};
  std::array<int, 2> craned_supervisor_pipe{};

  if (pipe(supervisor_craned_pipe.data()) == -1) {
    CRANE_ERROR("Pipe creation failed!");
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (pipe(craned_supervisor_pipe.data()) == -1) {
    close(supervisor_craned_pipe[0]);
    close(supervisor_craned_pipe[1]);
    CRANE_ERROR("Pipe creation failed!");
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  // The ResourceInNode structure should be copied here if being accessed in
  // the child process.
  // Note that CgroupManager acquires a lock for this.
  // If the lock is held in the parent process during fork, the forked thread
  // in the child proc will block forever.
  // auto res_in_node = job->job_spec.cgroup_spec.res_in_node;

  pid_t child_pid = fork();

  if (child_pid == -1) {
    CRANE_ERROR("[Step #{}.{}] fork() failed: {}", job_id, step_id,
                strerror(errno));

    close(craned_supervisor_pipe[0]);
    close(craned_supervisor_pipe[1]);
    close(supervisor_craned_pipe[0]);
    close(supervisor_craned_pipe[1]);
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (child_pid > 0) {  // Parent proc
    CRANE_DEBUG("[Step #{}.{}] Subprocess was created, pid: {}", job_id,
                step_id, child_pid);

    bool ok;
    CanStartMessage msg;
    ChildProcessReady child_process_ready;

    int craned_supervisor_fd = craned_supervisor_pipe[1];
    close(craned_supervisor_pipe[0]);
    int supervisor_craned_fd = supervisor_craned_pipe[0];
    close(supervisor_craned_pipe[1]);

    FileInputStream istream(supervisor_craned_fd);
    FileOutputStream ostream(craned_supervisor_fd);

    // Before exec, we need to make sure that the cgroup is ready.
    if (!job->cgroup->MigrateProcIn(child_pid)) {
      CRANE_ERROR(
          "[Step #{}.{}] Terminate the subprocess due to failure of cgroup "
          "migration.",
          job_id, step->step_id);

      job->err_before_supervisor_ready = CraneErrCode::ERR_CGROUP;

      KillPid_(child_pid, SIGKILL);

      close(craned_supervisor_fd);
      close(supervisor_craned_fd);
      return CraneErrCode::ERR_CGROUP;
    }

    // Do Supervisor Init
    crane::grpc::supervisor::InitSupervisorRequest init_req;
    init_req.set_job_id(job_id);
    init_req.set_step_id(step_id);
    init_req.set_debug_level(g_config.Supervisor.DebugLevel);
    init_req.set_craned_id(g_config.CranedIdOfThisNode);
    init_req.set_craned_unix_socket_path(g_config.CranedUnixSockPath);
    init_req.set_crane_base_dir(g_config.CraneBaseDir);
    init_req.set_crane_script_dir(g_config.CranedScriptDir);
    init_req.mutable_step_spec()->CopyFrom(step->step_to_d);
    init_req.set_log_dir(g_config.Supervisor.LogDir);
    auto* cfored_listen_conf = init_req.mutable_cfored_listen_conf();
    cfored_listen_conf->set_use_tls(g_config.ListenConf.TlsConfig.Enabled);
    cfored_listen_conf->set_domain_suffix(
        g_config.ListenConf.TlsConfig.DomainSuffix);
    auto* tls_certs = cfored_listen_conf->mutable_tls_certs();
    tls_certs->set_cert_content(
        g_config.ListenConf.TlsConfig.TlsCerts.CertContent);
    tls_certs->set_ca_content(g_config.ListenConf.TlsConfig.TlsCerts.CaContent);
    tls_certs->set_key_content(
        g_config.ListenConf.TlsConfig.TlsCerts.KeyContent);

    // Pass job env to supervisor
    EnvMap res_env_map = job->GetJobEnvMap();
    init_req.mutable_env()->clear();
    init_req.mutable_env()->insert(res_env_map.begin(), res_env_map.end());

    if (g_config.Container.Enabled) {
      auto* container_conf = init_req.mutable_container_config();
      container_conf->set_temp_dir(g_config.Container.TempDir);
      container_conf->set_runtime_bin(g_config.Container.RuntimeBin);
      container_conf->set_state_cmd(g_config.Container.RuntimeState);
      container_conf->set_run_cmd(g_config.Container.RuntimeRun);
      container_conf->set_kill_cmd(g_config.Container.RuntimeKill);
      container_conf->set_delete_cmd(g_config.Container.RuntimeDelete);
    }

    if (g_config.Plugin.Enabled) {
      auto* plugin_conf = init_req.mutable_plugin_config();
      plugin_conf->set_socket_path(g_config.Plugin.PlugindSockPath);
    }

    ok = SerializeDelimitedToZeroCopyStream(init_req, &ostream);
    if (!ok) {
      CRANE_ERROR("[Step #{}.{}] Failed to serialize msg to ostream: {}",
                  job_id, step_id, strerror(ostream.GetErrno()));
    }

    if (ok) ok &= ostream.Flush();
    if (!ok) {
      CRANE_ERROR("[Step #{}.{}] Failed to send init msg to supervisor: {}",
                  job_id, step_id, strerror(ostream.GetErrno()));

      job->err_before_supervisor_ready = CraneErrCode::ERR_PROTOBUF;
      KillPid_(child_pid, SIGKILL);

      close(craned_supervisor_fd);
      close(supervisor_craned_fd);
      return CraneErrCode::ERR_PROTOBUF;
    }

    CRANE_TRACE("[Step #{}.{}] Supervisor init msg send.", job_id, step_id);

    crane::grpc::supervisor::SupervisorReady supervisor_ready;
    bool clean_eof{false};
    ok = ParseDelimitedFromZeroCopyStream(&supervisor_ready, &istream,
                                          &clean_eof);
    if (!ok || !supervisor_ready.ok()) {
      if (!ok)
        CRANE_ERROR("[Step #{}.{}] Pipe child endpoint failed: {},{}", job_id,
                    step_id,
                    std::error_code(istream.GetErrno(), std::generic_category())
                        .message(),
                    clean_eof);
      if (!supervisor_ready.ok())
        CRANE_ERROR("[Step #{}.{}] False from subprocess {}.", job_id, step_id,
                    child_pid);

      job->err_before_supervisor_ready = CraneErrCode::ERR_PROTOBUF;
      KillPid_(child_pid, SIGKILL);

      close(craned_supervisor_fd);
      close(supervisor_craned_fd);
      return CraneErrCode::ERR_PROTOBUF;
    }

    close(craned_supervisor_fd);
    close(supervisor_craned_fd);

    CRANE_TRACE("[Step #{}.{}] Supervisor init msg received.", job_id, step_id);
    g_supervisor_keeper->AddSupervisor(job_id, step_id);

    step->supv_pid = child_pid;
    return CraneErrCode::SUCCESS;
  } else {  // Child proc, NOLINT(readability-else-after-return)
    // Disable SIGABRT backtrace from child processes.
    signal(SIGABRT, SIG_DFL);

    int craned_supervisor_fd = craned_supervisor_pipe[0];
    close(craned_supervisor_pipe[1]);
    int supervisor_craned_fd = supervisor_craned_pipe[1];
    close(supervisor_craned_pipe[0]);

    // Message will send to stdin of Supervisor for its init.
    for (int retry_count{0}; retry_count < 5; --retry_count) {
      if (-1 == dup2(craned_supervisor_fd, STDIN_FILENO)) {
        if (errno == EINTR) {
          fmt::print(
              stderr,
              "[Step #{}.{}] Retrying dup2 stdin: interrupted system call, "
              "retry count: {}",
              job_id, step_id, retry_count);
          continue;
        }
        std::error_code ec(errno, std::generic_category());
        fmt::print("[Step #{}.{}] Failed to dup2 stdin: {}.", job_id, step_id,
                   ec.message());
        std::abort();
      }

      break;
    }

    for (int retry_count{0}; retry_count < 5; --retry_count) {
      if (-1 == dup2(supervisor_craned_fd, STDOUT_FILENO)) {
        if (errno == EINTR) {
          fmt::print(
              stderr,
              "[Step #{}.{}] Retrying dup2 stdout: interrupted system call, "
              "retry count: {}",
              job_id, step_id, retry_count);
          continue;
        }
        std::error_code ec(errno, std::generic_category());
        fmt::print("[Step #{}.{}] Failed to dup2 stdout: {}.", job_id, step_id,
                   ec.message());
        std::abort();
      }

      break;
    }
    close(craned_supervisor_fd);
    close(supervisor_craned_fd);

    // Close stdin for batch tasks.
    // If these file descriptors are not closed, a program like mpirun may
    // keep waiting for the input from stdin or other fds and will never end.
    util::os::CloseFdFrom(3);

    // Prepare the command line arguments.
    std::vector<const char*> argv;

    // Argv[0] is the program name which can be anything.
    auto supervisor_name = fmt::format("csupervisor: [{}.{}]", job_id, step_id);
    argv.emplace_back(supervisor_name.c_str());

    argv.push_back(nullptr);

    // Use execvp to search the kSupervisorPath in the PATH.
    execvp(g_config.Supervisor.Path.c_str(),
           const_cast<char* const*>(argv.data()));

    // Error occurred since execvp returned. At this point, errno is set.
    // Ctld use SIGABRT to inform the client of this failure.
    fmt::print(stderr, "[Craned Subprocess] Failed to execvp {}. Error: {}\n",
               g_config.Supervisor.Path.c_str(), strerror(errno));

    // TODO: See https://tldp.org/LDP/abs/html/exitcodes.html, return
    // standard
    //  exit codes
    abort();
  }
}

CraneErrCode JobManager::ExecuteStepAsync(
    std::unordered_map<job_id_t, std::unordered_set<step_id_t>>&& steps) {
  if (m_is_ending_now_.load(std::memory_order_acquire)) {
    return CraneErrCode::ERR_SHUTTING_DOWN;
  }

  for (auto& [job_id, step_ids] : steps) {
    for (auto step_id : step_ids) {
      auto job = m_job_map_[job_id];
      if (!job) {
        CRANE_DEBUG("[Step #{}.{}] without job allocation. Ignoring it.",
                    job_id, step_id);
        return CraneErrCode::ERR_CGROUP;
      }

      EvQueueExecuteStepElem elem{.job_id = job_id,
                                  .step_id = step_id,
                                  .ok_prom = std::promise<CraneErrCode>{}};

      m_grpc_execute_step_queue_.enqueue(std::move(elem));
    }
  }
  m_grpc_execute_step_async_handle_->send();

  return CraneErrCode::SUCCESS;
}

void JobManager::EvCleanGrpcExecuteStepQueueCb_() {
  EvQueueExecuteStepElem elem;

  while (m_grpc_execute_step_queue_.try_dequeue(elem)) {
    // Once ExecuteTask RPC is processed, the Execution goes into m_job_map_.
    auto& [job_id, step_id, ok_prom] = elem;
    if (!m_job_map_.Contains(job_id)) {
      CRANE_ERROR("[Job #{}.{}]Failed to find allocation", job_id, step_id);
      elem.ok_prom.set_value(CraneErrCode::ERR_CGROUP);
      continue;
    }
    elem.ok_prom.set_value(CraneErrCode::SUCCESS);

    g_thread_pool->detach_task([job_id, step_id] {
      auto stub = g_supervisor_keeper->GetStub(job_id, step_id);
      if (!stub) {
        CRANE_ERROR("[Step #{}.{}] Failed to find supervisor stub.", job_id,
                    step_id);
      }
      auto code = stub->ExecuteStep();
      if (code != CraneErrCode::SUCCESS) {
        CRANE_ERROR("[Step #{}.{}] Supervisor failed to execute task, code:{}.",
                    job_id, step_id, static_cast<int>(code));
        // Ctld will send ShutdownSupervisor after status change from
        // supervisor.
      }
    });
  }
}

bool JobManager::FreeJobAllocation_(std::vector<JobInD>&& jobs) {
  CRANE_DEBUG("Freeing job [{}] allocation.",
              absl::StrJoin(jobs | std::views::transform([](auto& job) {
                              return job.job_id;
                            }) | std::views::common,
                            ","));
  std::unordered_map<task_id_t, CgroupInterface*> job_cg_map;
  std::vector<uid_t> uid_vec;

  for (auto& job : jobs) {
    job_cg_map[job.job_id] = job.cgroup.release();
  }

  for (auto [job_id, cgroup] : job_cg_map) {
    if (cgroup == nullptr) continue;

    if (g_config.Plugin.Enabled && g_plugin_client) {
      g_plugin_client->DestroyCgroupHookAsync(job_id, cgroup->CgroupName());
    }

    g_thread_pool->detach_task([cgroup]() {
      int cnt = 0;

      while (true) {
        if (cgroup->Empty()) break;

        if (cnt >= 5) {
          CRANE_ERROR(
              "Couldn't kill the processes in cgroup {} after {} times. "
              "Skipping it.",
              cgroup->CgroupName(), cnt);
          break;
        }

        cgroup->KillAllProcesses();
        ++cnt;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
      cgroup->Destroy();

      delete cgroup;
    });
  }
  return true;
}

void JobManager::FreeStepAllocation_(std::vector<StepInstance*>&& steps) {
  for (auto* step : steps) {
    delete step;
  }
  // TODO: delete cgroup
}

void JobManager::LaunchStepMt_(std::unique_ptr<StepInstance> step) {
  // This function runs in a multi-threading manner. Take care of thread
  // safety. JobInstance will not be free during this function. Take care of
  // data race for job instance.

  job_id_t job_id = step->job_id;
  step_id_t step_id = step->step_id;
  auto job_ptr = m_job_map_.GetValueExclusivePtr(job_id);
  if (!job_ptr) {
    CRANE_ERROR("[Job #{}.{}]Failed to find job allocation", job_id, step_id);
    ActivateTaskStatusChangeAsync_(
        job_id, step_id, crane::grpc::TaskStatus::Failed,
        ExitCode::kExitCodeCgroupError,
        fmt::format("Failed to get the allocation for job#{} ", job_id));
    return;
  }
  auto* job = job_ptr.get();

  // Check if the step is acceptable.
  if (step->IsDaemon()) {
    if (!step->step_to_d.container().empty() && !g_config.Container.Enabled) {
      CRANE_ERROR("Container support is disabled but job #{} requires it.",
                  job_id);
      ActivateTaskStatusChangeAsync_(
          job_id, step_id, crane::grpc::TaskStatus::Failed,
          ExitCode::kExitCodeSpawnProcessFail,
          "Container is not enabled in this craned.");
      return;
    }
  }

  if (!job->cgroup) {
    auto cg_expt = CgroupManager::AllocateAndGetCgroup(
        CgroupManager::CgroupStrByJobId(job->job_id), job->job_to_d.res(),
        false);
    if (cg_expt.has_value()) {
      job->cgroup = std::move(cg_expt.value());
    } else {
      CRANE_ERROR("Failed to get cgroup for job#{}", job_id);
      ActivateTaskStatusChangeAsync_(
          job_id, step_id, crane::grpc::TaskStatus::Failed,
          ExitCode::kExitCodeCgroupError,
          fmt::format("Failed to get cgroup for job#{} ", job_id));
      return;
    }
  }
  auto* step_ptr = step.get();
  {
    absl::MutexLock lk(job->step_map_mtx.get());
    job->step_map.emplace(step->step_id, std::move(step));
  }
  // err will NOT be kOk ONLY if fork() is not called due to some failure
  // or fork() fails.
  // In this case, SIGCHLD will NOT be received for this task, and
  // we should send TaskStatusChange manually.
  CraneErrCode err = SpawnSupervisor_(job, step_ptr);
  if (err != CraneErrCode::SUCCESS) {
    ActivateTaskStatusChangeAsync_(
        job_id, step_id, crane::grpc::TaskStatus::Failed,
        ExitCode::kExitCodeSpawnProcessFail,
        fmt::format("Cannot spawn a new process inside the instance of job #{}",
                    job_id));
  } else {
    // kOk means that SpawnSupervisor_ has successfully forked a child
    // process. Now we put the child pid into index maps. SIGCHLD sent just
    // after fork() and before putting pid into maps will repeatedly be sent
    // by timer and eventually be handled once the SIGCHLD processing callback
    // sees the pid in index maps. Now we do not support launch multiple tasks
    // in a job.
    CRANE_TRACE("[Step #{}.{}] Supervisor spawned successfully.", job_id,
                step_id);
  }
}

void JobManager::EvCleanTaskStatusChangeQueueCb_() {
  StepStatusChangeQueueElem status_change;
  while (m_task_status_change_queue_.try_dequeue(status_change)) {
    auto job_ptr = m_job_map_.GetValueExclusivePtr(status_change.job_id);
    if (!job_ptr) {
      CRANE_ERROR("[Job #{}] Job allocation not found for StepStatusChange.",
                  status_change.job_id);
      continue;
    }
    absl::MutexLock lk(job_ptr->step_map_mtx.get());
    if (!job_ptr->step_map.contains(status_change.step_id)) {
      CRANE_ERROR(
          "[Step #{}.{}] Step allocation not found for StepStatusChange.",
          status_change.job_id, status_change.step_id);
      continue;
    }
    auto* step = job_ptr->step_map.at(status_change.step_id).get();
    step->status = status_change.new_status;
    bool orphaned = job_ptr->orphaned;
    if (!orphaned)
      g_ctld_client->StepStatusChangeAsync(std::move(status_change));
    else {
      CRANE_DEBUG("[Step #{}.{}] Step status change not send: orphaned.",
                  status_change.job_id, status_change.step_id);
    }
  }
}

void JobManager::ActivateTaskStatusChangeAsync_(
    job_id_t job_id, step_id_t step_id, crane::grpc::TaskStatus new_status,
    uint32_t exit_code, std::optional<std::string> reason) {
  StepStatusChangeQueueElem status_change{.job_id = job_id,
                                          .step_id = step_id,
                                          .new_status = new_status,
                                          .exit_code = exit_code};
  if (reason.has_value()) status_change.reason = std::move(reason);

  m_task_status_change_queue_.enqueue(std::move(status_change));
  m_task_status_change_async_handle_->send();
}

/**
 * @brief Move process on craned to specify job's cgroup. Called from
 * CranedServer
 * @param pid process pid
 * @param job_id
 * @return True if job and cgroup exists
 */
bool JobManager::MigrateProcToCgroupOfJob(pid_t pid, task_id_t job_id) {
  auto job = m_job_map_.GetValueExclusivePtr(job_id);
  if (!job) {
    CRANE_TRACE("Job #{} does not exist when querying its cgroup.", job_id);
    return false;
  }
  if (job->cgroup) {
    return job->cgroup->MigrateProcIn(pid);
  }

  auto cg_expt = CgroupManager::AllocateAndGetCgroup(
      CgroupManager::CgroupStrByJobId(job->job_id), job->job_to_d.res(), false);
  if (cg_expt.has_value()) {
    job->cgroup = std::move(cg_expt.value());
    return job->cgroup->MigrateProcIn(pid);
  }

  CRANE_ERROR("Failed to get cgroup for job#{}", job_id);
  return false;
}

std::map<job_id_t, std::set<step_id_t>> JobManager::GetAllocatedJobSteps() {
  auto job_map_ptr = m_job_map_.GetMapExclusivePtr();
  std::map<job_id_t, std::set<step_id_t>> job_steps;
  for (auto& [job_id, job] : *job_map_ptr) {
    auto job_ptr = job.GetExclusivePtr();
    absl::MutexLock lk(job_ptr->step_map_mtx.get());
    job_steps[job_id] =
        job_ptr->step_map | std::views::keys | std::ranges::to<std::set>();
  }
  return job_steps;
}

std::optional<TaskInfoOfUid> JobManager::QueryTaskInfoOfUid(uid_t uid) {
  CRANE_DEBUG("Query task info for uid {}", uid);

  TaskInfoOfUid info;
  info.job_cnt = 0;
  info.cgroup_exists = false;

  if (auto task_ids = this->m_uid_to_job_ids_map_[uid]; task_ids) {
    info.cgroup_exists = true;
    info.job_cnt = task_ids->size();
    info.first_task_id = *task_ids->begin();
  } else {
    CRANE_WARN("Uid {} not found in uid_to_task_ids_map", uid);
    return std::nullopt;
  }
  return info;
}

void JobManager::EvCleanTerminateTaskQueueCb_() {
  StepTerminateQueueElem elem;
  std::vector<StepTerminateQueueElem> not_ready_elems;
  while (m_step_terminate_queue_.try_dequeue(elem)) {
    CRANE_TRACE(
        "Receive TerminateRunningTask Request from internal queue. "
        "Step: {}.{}",
        elem.job_id, elem.step_id);

    auto job_instance = m_job_map_.GetValueExclusivePtr(elem.job_id);

    if (!job_instance) {
      CRANE_DEBUG("[Step #{}.{}] Terminating a non-existent step.", elem.job_id,
                  elem.step_id);

      // if (!elem.mark_as_orphaned)
      //   StepStopAndDoStatusChangeAsync(
      //       elem.job_id, elem.step_id, crane::grpc::TaskStatus::Cancelled,
      //       ExitCode::kExitCodeTerminated, "Step not found.");
      continue;
    }
    absl::MutexLock lk(job_instance->step_map_mtx.get());
    if (!job_instance->step_map.contains(elem.step_id)) {
      CRANE_DEBUG("[Step #{}.{}] Terminating a non-existent step.", elem.job_id,
                  elem.step_id);

      // if (!elem.mark_as_orphaned)
      //   g_ctld_client->StepStatusChangeAsync(
      //       {.job_id = elem.job_id,
      //        .step_id = elem.step_id,
      //        .new_status = crane::grpc::TaskStatus::Cancelled,
      //        .exit_code = ExitCode::kExitCodeTerminated,
      //        .reason = "Step not found."});
      continue;
    }

    auto* instance = job_instance.get();
    instance->orphaned = elem.mark_as_orphaned;
    std::set<step_id_t> terminate_step_ids;
    if (elem.step_id == kDaemonStepId) {
      terminate_step_ids =
          instance->step_map | std::views::keys | std::ranges::to<std::set>();
    } else {
      terminate_step_ids = {elem.step_id};
    }
    bool can_terminate_now = true;

    for (auto step_id : terminate_step_ids) {
      auto* step = job_instance->step_map.at(step_id).get();
      if (step->CanOperate()) {
        CRANE_DEBUG(
            "[Step #{}.{}] Terminating a non-running step, will do it later.",
            elem.job_id, elem.step_id);
        not_ready_elems.emplace_back(std::move(elem));
        can_terminate_now = false;
        break;
      }
    }
    if (!can_terminate_now) continue;

    for (auto step_id : terminate_step_ids) {
      auto stub = g_supervisor_keeper->GetStub(elem.job_id, step_id);
      if (!stub) {
        CRANE_ERROR("[Step #{}.{}] Supervisor not found", elem.job_id, step_id);
        continue;
      }

      auto err =
          stub->TerminateTask(elem.mark_as_orphaned, elem.terminated_by_user);
      if (err != CraneErrCode::SUCCESS) {
        // Supervisor dead for some reason.
        CRANE_ERROR("[Step #{}.{}] Failed to terminate.", elem.job_id, step_id);
        if (!elem.mark_as_orphaned)
          StepStatusChangeAsync(
              elem.job_id, step_id, crane::grpc::TaskStatus::Cancelled,
              ExitCode::kExitCodeTerminated, "Terminated failed.");
      }
      CRANE_TRACE("[Step #{}.{}] Terminated.", elem.job_id, step_id);
    }
  }
  for (auto& not_ready_elem : not_ready_elems) {
    m_step_terminate_queue_.enqueue(std::move(not_ready_elem));
  }
}

void JobManager::TerminateStepAsync(job_id_t job_id, step_id_t step_id) {
  StepTerminateQueueElem elem{.job_id = job_id,
                              .step_id = step_id,
                              .terminated_by_user = true};
  m_step_terminate_queue_.enqueue(std::move(elem));
  m_terminate_step_async_handle_->send();
}

void JobManager::MarkStepAsOrphanedAndTerminateAsync(job_id_t job_id,
                                                     step_id_t step_id) {
  StepTerminateQueueElem elem{.job_id = job_id,
                              .step_id = step_id,
                              .mark_as_orphaned = true};
  m_step_terminate_queue_.enqueue(std::move(elem));
  m_terminate_step_async_handle_->send();
}

bool JobManager::ChangeJobTimeLimitAsync(job_id_t job_id,
                                         absl::Duration time_limit) {
  if (m_is_ending_now_.load(std::memory_order_acquire)) {
    return false;
  }
  ChangeTaskTimeLimitQueueElem elem{.job_id = job_id, .time_limit = time_limit};

  std::future<bool> ok_fut = elem.ok_prom.get_future();
  m_task_time_limit_change_queue_.enqueue(std::move(elem));
  m_change_task_time_limit_async_handle_->send();
  return ok_fut.get();
}

void JobManager::CleanUpJobAndStepsAsync(
    const std::unordered_map<job_id_t, std::unordered_set<step_id_t>>& steps) {
  {
    std::latch shutdown_daemon_latch{static_cast<std::ptrdiff_t>(steps.size())};
    for (auto job_id : steps | std::views::keys) {
      g_thread_pool->detach_task([&shutdown_daemon_latch, &steps, job_id] {
        for (const auto step_id : steps.at(job_id)) {
          auto stub = g_supervisor_keeper->GetStub(job_id, step_id);
          if (!stub) {
            CRANE_ERROR("[Step #{}.{}] Failed to get stub.", job_id, step_id);
          } else {
            stub->ShutdownSupervisor();
          }
          g_supervisor_keeper->RemoveSupervisor(job_id, step_id);
        }
        shutdown_daemon_latch.count_down();
      });
    }
    shutdown_daemon_latch.wait();
  }

  std::vector<JobInD> jobs_to_clean;
  std::vector<StepInstance*> step_instances;
  {
    auto map_ptr = m_job_map_.GetMapExclusivePtr();
    for (const auto [job_id, step_ids] : steps) {
      if (!map_ptr->contains(job_id)) {
        CRANE_TRACE("[Job #{}] not found in job_map");
        continue;
      }
      auto* job = map_ptr->at(job_id).RawPtr();
      absl::MutexLock lock(job->step_map_mtx.get());
      for (const auto step_id : step_ids) {
        if (!job->step_map.contains(step_id)) {
          CRANE_TRACE("[Step #{}.{}] not found in step_map", job_id, step_id);
          continue;
        }
        step_instances.push_back(job->step_map.at(step_id).get());
      }
      if (step_ids.size() == job->step_map.size() &&
          std::ranges::equal(step_ids, job->step_map | std::views::keys)) {
        jobs_to_clean.emplace_back(std::move(*job));
        map_ptr->erase(job_id);
      } else {
        for (auto step_id : step_ids) {
          job->step_map.erase(step_id);
        }
      }
    }
  }

  absl::MutexLock lk(&m_free_job_step_mtx_);
  for (auto* step : step_instances) {
    if (m_completing_step_retry_map_.contains(step)) {
      CRANE_DEBUG("[Step #{}.{}] is already completing, ignore clean up.",
                  step->job_id, step->step_id);
      continue;
    }
    m_completing_step_retry_map_[step] = 0;
  }
  for (auto&& job : jobs_to_clean) {
    job_id_t job_id = job.job_id;
    if (m_completing_job_.contains(job_id)) {
      CRANE_DEBUG("[Job #{}] is already completing, ignore clean up.", job_id);
      continue;
    }
    m_completing_job_.emplace(job_id, std::move(job));
  }
}

void JobManager::StepStatusChangeAsync(job_id_t job_id, step_id_t step_id,
                                       crane::grpc::TaskStatus new_status,
                                       uint32_t exit_code,
                                       std::optional<std::string> reason) {
  CRANE_INFO("[Step #{}.{}] is doing StepStatusChange, new status: {}", job_id,
             step_id, util::StepStatusToString(new_status));
  ActivateTaskStatusChangeAsync_(job_id, step_id, new_status, exit_code,
                                 std::move(reason));
}

void JobManager::EvCleanChangeTaskTimeLimitQueueCb_() {
  ChangeTaskTimeLimitQueueElem elem;
  std::vector<ChangeTaskTimeLimitQueueElem> not_ready_elems;
  while (m_task_time_limit_change_queue_.try_dequeue(elem)) {
    if (auto job_ptr = m_job_map_.GetValueExclusivePtr(elem.job_id); job_ptr) {
      absl::MutexLock lk(job_ptr->step_map_mtx.get());
      if (!job_ptr->step_map.contains(elem.step_id)) {
        CRANE_DEBUG("[Step #{}.{}] Terminating a non-existent step.",
                    elem.job_id, elem.step_id);
        continue;
      }
      auto* step = job_ptr->step_map.at(elem.step_id).get();
      if (step->CanOperate()) {
        CRANE_DEBUG(
            "[Step #{}.{}] Terminating a non-running step, will do it later.",
            elem.job_id, elem.step_id);
        not_ready_elems.emplace_back(std::move(elem));
        continue;
      }
      auto stub = g_supervisor_keeper->GetStub(elem.job_id, elem.step_id);
      if (!stub) {
        CRANE_ERROR("Supervisor for task #{} not found", elem.job_id);
        continue;
      }
      auto err = stub->ChangeTaskTimeLimit(elem.time_limit);
      if (err != CraneErrCode::SUCCESS) {
        CRANE_ERROR("Failed to change task time limit for task #{}",
                    elem.job_id);
        continue;
      }
      elem.ok_prom.set_value(true);
    } else {
      CRANE_ERROR("Try to update the time limit of a non-existent task #{}.",
                  elem.job_id);
      elem.ok_prom.set_value(false);
    }
  }
  for (auto& not_ready_elem : not_ready_elems) {
    m_task_time_limit_change_queue_.enqueue(std::move(not_ready_elem));
  }
}

}  // namespace Craned

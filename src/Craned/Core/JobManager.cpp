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
using namespace std::chrono_literals;

StepInstance::StepInstance(const crane::grpc::StepToD& step_to_d)
    : job_id(step_to_d.job_id()),
      step_id(step_to_d.step_id()),
      step_to_d(step_to_d),
      supv_pid(0),
      status(StepStatus::Configuring) {}

StepInstance::StepInstance(const crane::grpc::StepToD& step_to_d,
                           pid_t supv_pid, StepStatus status)
    : job_id(step_to_d.job_id()),
      step_id(step_to_d.step_id()),
      step_to_d(step_to_d),
      supv_pid(supv_pid),
      status(status) {}

EnvMap JobInD::GetJobEnvMap() {
  auto env_map = CgroupManager::GetResourceEnvMapByResInNode(job_to_d.res());

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
  m_check_supervisor_timer_handle_->start(std::chrono::milliseconds{0ms},
                                          std::chrono::milliseconds{500ms});

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
  std::vector<job_id_t> invalid_jobs;
  for (auto& [job_id, job] : *m_job_map_.GetMapExclusivePtr()) {
    if (job.RawPtr()->step_map.empty()) {
      CRANE_WARN("Job #{} has no step after recovery.", job_id);
      invalid_jobs.push_back(job_id);
    }
  }
  for (auto job_id : invalid_jobs) {
    m_job_map_.Erase(job_id);
  }
  return CraneErrCode::SUCCESS;
}

JobManager::~JobManager() {
  CRANE_DEBUG("JobManager is being destroyed.");
  m_is_ending_now_ = true;
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}

bool JobManager::AllocJobs(std::vector<JobInD>&& jobs) {
  // NOTE: Cgroup is lazily created when the first step is arrived.
  // Only maintain the job info here.
  auto job_map_ptr = m_job_map_.GetMapExclusivePtr();
  auto uid_map_ptr = m_uid_to_job_ids_map_.GetMapExclusivePtr();

  for (auto& job : jobs) {
    task_id_t job_id = job.job_id;
    uid_t uid = job.Uid();

    job_map_ptr->emplace(job_id, std::move(job));
    if (uid_map_ptr->contains(uid)) {
      uid_map_ptr->at(uid).RawPtr()->emplace(job_id);
    } else {
      uid_map_ptr->emplace(uid, absl::flat_hash_set<task_id_t>({job_id}));
    }
  }

  return true;
}

bool JobManager::FreeJobs(std::set<task_id_t>&& job_ids) {
  std::vector<JobInD> jobs_to_free;
  std::vector<StepInstance*> steps_to_free;

  for (job_id_t job_id : job_ids) {
    auto job = FreeJobInfo_(job_id);
    if (!job.has_value()) {
      CRANE_INFO("Try to free non-existent job #{}.", job_id);
      continue;
    }

    for (auto& step : job->step_map | std::views::values) {
      steps_to_free.emplace_back(step.get());
    }

    if (job->step_map.size() != 1)
      CRANE_DEBUG("Job #{} to free has more than one step.", job_id);

    jobs_to_free.emplace_back(std::move(job.value()));
  }

  CRANE_DEBUG(
      "Free jobs [{}], steps [{}].",
      absl::StrJoin(jobs_to_free | std::views::transform([](const auto& job) {
                      return job.job_id;
                    }) | std::views::common,
                    ","),
      absl::StrJoin(steps_to_free | std::views::transform([](const auto* step) {
                      return step->StepIdString();
                    }) | std::views::common,
                    ","));

  CleanUpJobAndStepsAsync(std::move(jobs_to_free), std::move(steps_to_free));

  return true;
}

void JobManager::AllocSteps(std::vector<StepToD>&& steps) {
  if (m_is_ending_now_.load(std::memory_order_acquire)) {
    CRANE_TRACE("JobManager is ending now, ignoring the request.");
    return;
  }

  CRANE_TRACE("Allocating step [{}].",
              absl::StrJoin(steps | std::views::transform(GetStepIdStr), ","));

  for (const auto& step : steps) {
    auto job_ptr = m_job_map_.GetValueExclusivePtr(step.job_id());
    if (!job_ptr) {
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

void JobManager::FreeSteps(
    std::unordered_map<job_id_t, std::unordered_set<step_id_t>>&& steps) {
  std::vector<StepInstance*> steps_to_free;
  for (auto& [job_id, step_ids] : steps) {
    auto job = m_job_map_.GetValueExclusivePtr(job_id);
    if (!job) {
      CRANE_WARN("Try to free step [{}] for nonexistent job #{}.",
                 absl::StrJoin(step_ids, ","), job_id);
      continue;
    }
    for (step_id_t step_id : step_ids) {
      absl::MutexLock lk(job->step_map_mtx.get());
      if (!job->step_map.contains(step_id)) {
        CRANE_WARN("[Step #{}.{}] Try to free nonexistent step, ignoring it.",
                   job_id, step_id);
        continue;
      }
      auto& step = job->step_map.at(step_id);
      if (step->IsDaemonStep()) {
        CRANE_ERROR("Not allowed to free daemon step #{}.{}", job_id, step_id);
        continue;
      }
      steps_to_free.emplace_back(job->step_map.at(step_id).release());
      job->step_map.erase(step_id);
    }
  }
  CleanUpJobAndStepsAsync({}, std::move(steps_to_free));
}

bool JobManager::EvCheckSupervisorRunning_() {
  std::vector<JobInD> jobs_to_clean;
  // Step is completing, get ownership here.
  std::vector<std::unique_ptr<StepInstance>> steps_to_clean;
  {
    std::vector<StepInstance*> exit_steps;
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
      exit_steps.push_back(step);
    }
    for (auto* step : exit_steps) {
      m_completing_step_retry_map_.erase(step);
      if (!m_completing_job_.contains(step->job_id)) {
        // Only the step is completing
        steps_to_clean.emplace_back(step);
        continue;
      }
      auto& job = m_completing_job_.at(step->job_id);
      if (!job.step_map.contains(step->step_id)) {
        // The step is considered completing before the job considered
        // completing, already remove form step map.
        steps_to_clean.emplace_back(std::move(step));
        continue;
      }
      steps_to_clean.emplace_back(std::move(job.step_map.at(step->step_id)));
      job.step_map.erase(step->step_id);
      if (job.step_map.empty()) {
        jobs_to_clean.emplace_back(std::move(job));
        m_completing_job_.erase(step->job_id);
      }
    }
  }

  if (!steps_to_clean.empty()) {
    CRANE_TRACE(
        "Supervisor for Step [{}] found to be exited",
        absl::StrJoin(steps_to_clean | std::views::transform([](auto& step) {
                        return std::make_pair(step->job_id, step->step_id);
                      }) | std::views::transform(util::StepIdPairToString),
                      ","));
    FreeStepAllocation_(std::move(steps_to_clean));
    if (!jobs_to_clean.empty()) FreeJobAllocation_(std::move(jobs_to_clean));
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
    std::unique_ptr step_inst = std::move(elem.step_inst);

    if (!m_job_map_.Contains(step_inst->job_id)) {
      CRANE_ERROR("[Step #{}.{}] Failed to find a job allocation for the step",
                  step_inst->job_id, step_inst->step_id);
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
    init_req.set_max_log_file_size(g_config.Supervisor.MaxLogFileSize);
    init_req.set_max_log_file_num(g_config.Supervisor.MaxLogFileNum);
    auto* cfored_listen_conf = init_req.mutable_cfored_listen_conf();
    cfored_listen_conf->set_use_tls(g_config.ListenConf.TlsConfig.Enabled);
    cfored_listen_conf->set_domain_suffix(
        g_config.ListenConf.TlsConfig.DomainSuffix);
    auto* tls_certs = cfored_listen_conf->mutable_tls_certs();
    tls_certs->set_cert_content(
        g_config.ListenConf.TlsConfig.TlsCerts.CertContent);
    tls_certs->set_ca_content(g_config.ListenConf.TlsConfig.CaContent);
    tls_certs->set_key_content(
        g_config.ListenConf.TlsConfig.TlsCerts.KeyContent);

    // Pass job env to supervisor
    EnvMap res_env_map = job->GetJobEnvMap();
    init_req.mutable_env()->clear();
    init_req.mutable_env()->insert(res_env_map.begin(), res_env_map.end());

    std::string cgroup_path_str = job->cgroup->CgroupPath().string();
    init_req.set_cgroup_path(cgroup_path_str);
    CRANE_TRACE("[Step #{}.{}] Setting cgroup path: {}", job_id, step_id,
                cgroup_path_str);

    if (g_config.Container.Enabled) {
      auto* container_conf = init_req.mutable_container_config();
      container_conf->set_temp_dir(g_config.Container.TempDir);
      container_conf->set_runtime_endpoint(g_config.Container.RuntimeEndpoint);
      container_conf->set_image_endpoint(g_config.Container.ImageEndpoint);
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
    for (int retry_count{0}; retry_count < 5; ++retry_count) {
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

    for (int retry_count{0}; retry_count < 5; ++retry_count) {
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
    auto supv_name = fmt::format("csupervisor: [{}.{}]", job_id, step_id);
    argv.emplace_back(supv_name.c_str());
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
    auto job = m_job_map_.GetValueExclusivePtr(job_id);

    if (!job) {
      CRANE_ERROR("[Step #{}.{}] Failed to find job allocation", job_id,
                  step_id);
      elem.ok_prom.set_value(CraneErrCode::ERR_NON_EXISTENT);
      continue;
    }
    absl::MutexLock lk(job->step_map_mtx.get());
    auto step_it = job->step_map.find(step_id);
    if (step_it == job->step_map.end()) {
      CRANE_ERROR("[Step #{}.{}] Failed to find step allocation", job_id,
                  step_id);
      elem.ok_prom.set_value(CraneErrCode::ERR_NON_EXISTENT);
      continue;
    }
    if (step_it->second->status != StepStatus::Configured) {
      CRANE_WARN(
          "[Step #{}.{}] Step status is not 'Configured' when executing step, "
          "current status: {}.",
          job_id, step_id, static_cast<int>(step_it->second->status));
    }
    step_it->second->status = StepStatus::Running;
    elem.ok_prom.set_value(CraneErrCode::SUCCESS);

    g_thread_pool->detach_task([job_id, step_id] {
      auto stub = g_supervisor_keeper->GetStub(job_id, step_id);
      if (!stub) {
        CRANE_ERROR("[Step #{}.{}] Failed to find supervisor stub.", job_id,
                    step_id);
        return;
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

std::optional<JobInD> JobManager::FreeJobInfo_(job_id_t job_id) {
  auto map_ptr = m_job_map_.GetMapExclusivePtr();
  auto uid_map = m_uid_to_job_ids_map_.GetMapExclusivePtr();
  return FreeJobInfoNoLock_(job_id, map_ptr, uid_map);
}

std::optional<JobInD> JobManager::FreeJobInfoNoLock_(
    job_id_t job_id, JobMap::MapExclusivePtr& job_map_ptr,
    UidMap::MapExclusivePtr& uid_map_ptr) {
  std::optional<JobInD> job_opt{std::nullopt};
  if (!job_map_ptr->contains(job_id)) {
    return job_opt;
  }
  job_opt = std::move(*job_map_ptr->at(job_id).RawPtr());
  job_map_ptr->erase(job_id);
  bool erase{false};
  {
    auto& value = uid_map_ptr->at(job_opt.value().Uid());
    value.RawPtr()->erase(job_id);
    erase = value.RawPtr()->empty();
  }
  if (erase) {
    uid_map_ptr->erase(job_opt.value().Uid());
  }

  return job_opt;
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
        std::this_thread::sleep_for(std::chrono::milliseconds{100ms});
      }
      cgroup->Destroy();

      delete cgroup;
    });
  }
  return true;
}

void JobManager::FreeStepAllocation_(
    std::vector<std::unique_ptr<StepInstance>>&& steps) {
  for (auto& step : steps) {
    step.reset();
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
    CRANE_ERROR("[Step #{}.{}] Failed to find job allocation", job_id, step_id);
    ActivateTaskStatusChangeAsync_(
        job_id, step_id, crane::grpc::TaskStatus::Failed,
        ExitCode::EC_CGROUP_ERR,
        fmt::format("Failed to get the allocation for job#{} ", job_id),
        google::protobuf::util::TimeUtil::GetCurrentTime());
    return;
  }
  auto* job = job_ptr.get();

  // Check if the step is acceptable.
  // TODO: Is this check necessary?
  if (step->IsContainer() && !g_config.Container.Enabled) {
    CRANE_ERROR("Container support is disabled but job #{} requires it.",
                job_id);
    ActivateTaskStatusChangeAsync_(
        job_id, step_id, crane::grpc::TaskStatus::Failed,
        ExitCode::EC_SPAWN_FAILED, "Container is not enabled in this craned.",
        google::protobuf::util::TimeUtil::GetCurrentTime());
    return;
  }

  // Create job cgroup first.
  // TODO: Create step cgroup later.
  if (!job->cgroup) {
    auto cg_expt = CgroupManager::AllocateAndGetCgroup(
        CgroupManager::CgroupStrByJobId(job->job_id), job->job_to_d.res(),
        false, Common::CgConstant::kCgMinMem);
    if (cg_expt.has_value()) {
      job->cgroup = std::move(cg_expt.value());
    } else {
      CRANE_ERROR("Failed to get cgroup for job#{}", job_id);
      ActivateTaskStatusChangeAsync_(
          job_id, step_id, crane::grpc::TaskStatus::Failed,
          ExitCode::EC_CGROUP_ERR,
          fmt::format("Failed to get cgroup for job#{} ", job_id),
          google::protobuf::util::TimeUtil::GetCurrentTime());
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
        ExitCode::EC_SPAWN_FAILED,
        fmt::format("Cannot spawn a new process inside the instance of job #{}",
                    job_id),
        google::protobuf::util::TimeUtil::GetCurrentTime());
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
    {
      auto job_ptr = m_job_map_.GetValueExclusivePtr(status_change.job_id);
      if (job_ptr) {
        absl::MutexLock lk(job_ptr->step_map_mtx.get());
        if (auto step_it = job_ptr->step_map.find(status_change.step_id);
            step_it != job_ptr->step_map.end()) {
          step_it->second->status = status_change.new_status;
        }
      }
    }

    CRANE_TRACE("[Step #{}.{}] StepStatusChange status: {}.",
                status_change.job_id, status_change.step_id,
                status_change.new_status);
    g_ctld_client->StepStatusChangeAsync(std::move(status_change));
  }
}

void JobManager::ActivateTaskStatusChangeAsync_(
    job_id_t job_id, step_id_t step_id, crane::grpc::TaskStatus new_status,
    uint32_t exit_code, std::optional<std::string> reason,
    google::protobuf::Timestamp timestamp) {
  StepStatusChangeQueueElem status_change{.job_id = job_id,
                                          .step_id = step_id,
                                          .new_status = new_status,
                                          .exit_code = exit_code,
                                          .timestamp = timestamp};
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

std::map<job_id_t, std::map<step_id_t, StepStatus>>
JobManager::GetAllocatedJobSteps() {
  auto job_map_ptr = m_job_map_.GetMapExclusivePtr();
  std::map<job_id_t, std::map<step_id_t, StepStatus>> job_steps;
  for (auto& [job_id, job] : *job_map_ptr) {
    auto job_ptr = job.GetExclusivePtr();
    absl::MutexLock lk(job_ptr->step_map_mtx.get());
    std::map<step_id_t, StepStatus> step_status_map;
    ;
    for (auto& [step_id, step] : job_ptr->step_map) {
      step_status_map[step_id] = step->status;
    }
    job_steps[job_id] = std::move(step_status_map);
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

  std::unordered_map<job_id_t, std::unordered_set<step_id_t>> job_step_ids;

  while (m_step_terminate_queue_.try_dequeue(elem)) {
    CRANE_TRACE(
        "Receive TerminateRunningTask Request from internal queue. "
        "Step: {}.{}",
        elem.job_id, elem.step_id);

    std::vector<StepInstance*> steps_to_clean;
    std::vector<JobInD> job_to_clean;
    {
      bool terminate_job = elem.step_id == kDaemonStepId;
      auto map_ptr = m_job_map_.GetMapExclusivePtr();
      if (!map_ptr->contains(elem.job_id)) {
        CRANE_DEBUG("[Step #{}.{}] Terminating a non-existent job.",
                    elem.job_id, elem.step_id);

        continue;
      }
      auto job_instance = map_ptr->at(elem.job_id).RawPtr();

      absl::MutexLock lk(job_instance->step_map_mtx.get());
      if (!job_instance->step_map.contains(elem.step_id)) {
        CRANE_DEBUG("[Step #{}.{}] Terminating a non-existent step.",
                    elem.job_id, elem.step_id);

        continue;
      }

      std::set<step_id_t> terminate_step_ids;
      if (terminate_job) {
        terminate_step_ids = job_instance->step_map | std::views::keys |
                             std::ranges::to<std::set>();
      } else {
        terminate_step_ids = {elem.step_id};
      }

      for (auto step_id : terminate_step_ids) {
        auto stub = g_supervisor_keeper->GetStub(elem.job_id, step_id);
        if (!stub) {
          CRANE_ERROR("[Step #{}.{}] Supervisor not found", elem.job_id,
                      step_id);
          continue;
        }

        auto err =
            stub->TerminateTask(elem.mark_as_orphaned, elem.terminated_by_user);
        if (err != CraneErrCode::SUCCESS) {
          // Supervisor dead for some reason.
          CRANE_ERROR("[Step #{}.{}] Failed to terminate.", elem.job_id,
                      step_id);
          if (!elem.mark_as_orphaned)
            g_ctld_client->StepStatusChangeAsync(
                {.job_id = elem.job_id,
                 .step_id = step_id,
                 .new_status = crane::grpc::TaskStatus::Cancelled,
                 .exit_code = ExitCode::EC_TERMINATED,
                 .reason = "Terminated failed."});
        }
        CRANE_TRACE("[Step #{}.{}] Terminated.", elem.job_id, step_id);
      }

      if (elem.mark_as_orphaned) {
        for (auto step_id : terminate_step_ids) {
          CRANE_DEBUG("[Step #{}.{}] Removed orphaned step.", elem.job_id,
                      step_id);
          auto* step = job_instance->step_map.at(step_id).release();
          steps_to_clean.push_back(step);
          job_instance->step_map.erase(step_id);
        }
      }
      if (terminate_job) {
        bool job_empty{false};
        {
          if (!map_ptr->contains(elem.job_id)) {
            CRANE_ERROR(
                "Job #{} not found when trying to remove orphaned step.",
                elem.job_id);
          }
          auto job_ptr = map_ptr->at(elem.job_id).RawPtr();
          job_empty = job_ptr->step_map.empty();
        }
        if (job_empty) {
          auto uid_map_ptr = m_uid_to_job_ids_map_.GetMapExclusivePtr();
          auto job_opt = FreeJobInfoNoLock_(elem.job_id, map_ptr, uid_map_ptr);
          if (job_opt.has_value()) {
            job_to_clean.emplace_back(std::move(job_opt.value()));
          } else {
            CRANE_DEBUG(
                "Job #{} not found when trying to remove orphaned step.",
                elem.job_id);
          }
        }
      }
    }
    CleanUpJobAndStepsAsync(std::move(job_to_clean), std::move(steps_to_clean));
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

void JobManager::CleanUpJobAndStepsAsync(std::vector<JobInD>&& jobs,
                                         std::vector<StepInstance*>&& steps) {
  std::latch shutdown_step_latch(steps.size());
  for (auto* step : steps) {
    if (!step->IsDaemonStep()) {
      g_supervisor_keeper->RemoveSupervisor(step->job_id, step->step_id);
      shutdown_step_latch.count_down();
      continue;
    }

    g_thread_pool->detach_task([&shutdown_step_latch, step] {
      auto stub = g_supervisor_keeper->GetStub(step->job_id, step->step_id);
      if (!stub) {
        CRANE_ERROR("[Step #{}.{}] Failed to get stub.", step->job_id,
                    step->step_id);
      } else {
        CRANE_TRACE("[Step #{}.{}] Shutting down supervisor.", step->job_id,
                    step->step_id);
        auto err = stub->ShutdownSupervisor();
        if (err != CraneErrCode::SUCCESS) {
          CRANE_ERROR("[Step #{}.{}] Failed to shutdown supervisor.",
                      step->job_id, step->step_id);
        }
      }
      g_supervisor_keeper->RemoveSupervisor(step->job_id, step->step_id);

      shutdown_step_latch.count_down();
    });
  }
  shutdown_step_latch.wait();

  absl::MutexLock lk(&m_free_job_step_mtx_);
  for (auto* step : steps) {
    if (m_completing_step_retry_map_.contains(step)) {
      CRANE_DEBUG("[Step #{}.{}] is already completing, ignore clean up.",
                  step->job_id, step->step_id);
      continue;
    }
    m_completing_step_retry_map_[step] = 0;
  }

  for (auto&& job : jobs) {
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
                                       std::optional<std::string> reason,
                                       google::protobuf::Timestamp timestamp) {
  CRANE_INFO("[Step #{}.{}] is doing StepStatusChange, new status: {}", job_id,
             step_id, new_status);
  ActivateTaskStatusChangeAsync_(job_id, step_id, new_status, exit_code,
                                 std::move(reason), timestamp);
}

}  // namespace Craned

/**
 * Copyright (c) 2025 Peking University and Peking University
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

#include "StepInstance.h"

#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/util/delimited_message_util.h>

#include <algorithm>

#include "CtldClient.h"
#include "DeviceManager.h"
#include "JobManager.h"
#include "crane/CriClient.h"
#include "crane/ExecutionFlow.h"
#include "crane/Tracing.h"

namespace Craned {
using namespace std::literals::chrono_literals;

namespace {

void CleanUpStepCgroupWithoutFlow(bool async, job_id_t job_id,
                                  step_id_t step_id, CgroupInterface* cgroup,
                                  std::string step_cg_str) {
  if (cgroup == nullptr) return;

  auto clean_step_cgroup = [job_id, step_id, cgroup,
                            step_cg_str = std::move(step_cg_str)] {
    auto remove_step_directory = [job_id, step_id,
                                  step_cg_str = std::move(step_cg_str)] {
      auto step_cg_path =
          (std::filesystem::path{Common::CgConstant::kSystemCgPathPrefix} /
           Common::CgConstant::kRootCgNamePrefix / step_cg_str)
              .parent_path();

      std::error_code ec;
      if (std::filesystem::exists(step_cg_path, ec)) {
        std::filesystem::remove(step_cg_path, ec);
        if (ec) {
          CRANE_ERROR("[Step #{}.{}] Failed to remove step cgroup dir {}: {}",
                      job_id, step_id, step_cg_path, ec.message());
        } else {
          CRANE_DEBUG("[Step #{}.{}] Step cgroup dir {} removed.", job_id,
                      step_id, step_cg_path);
        }
      } else if (ec) {
        CRANE_ERROR(
            "[Step #{}.{}] Failed to check existence of step cgroup dir {}: {}",
            job_id, step_id, step_cg_path, ec.message());
      } else {
        CRANE_DEBUG(
            "[Step #{}.{}] Step cgroup dir {} does not exist, skip clean.",
            job_id, step_id, step_cg_path);
      }
    };

#ifdef CRANE_ENABLE_EXECUTION_FLOW
    CgroupManager::KillAndDestroyCgroup(
        std::unique_ptr<CgroupInterface>{cgroup},
        [remove_step_directory = std::move(remove_step_directory)](
            CgroupManager::CgroupCleanupResult) mutable {
          remove_step_directory();
        });
#else
    CgroupManager::KillAndDestroyCgroup(
        std::unique_ptr<CgroupInterface>{cgroup});
    remove_step_directory();
#endif
  };

  if (async) {
    g_thread_pool->detach_task(std::move(clean_step_cgroup));
  } else {
    clean_step_cgroup();
  }
}

}  // namespace

StepInstance::StepInstance(const crane::grpc::StepToD& step_to_d)
    : job_id(step_to_d.job_id()),
      step_id(step_to_d.step_id()),
      supv_pid(0),
      step_to_d(step_to_d),
      status(StepStatus::Configuring) {}

StepInstance::StepInstance(const crane::grpc::StepToD& step_to_d,
                           pid_t supv_pid, StepStatus status,
                           std::shared_ptr<SupervisorStub> supervisor_stub)
    : job_id(step_to_d.job_id()),
      step_id(step_to_d.step_id()),
      supv_pid(supv_pid),
      step_to_d(step_to_d),
      status(status),
      supervisor_stub(supervisor_stub) {}

#ifdef CRANE_ENABLE_EXECUTION_FLOW
std::string StepInstance::ExecutionFlowId() const {
  if (step_to_d.type() != crane::grpc::JobType::Batch || IsContainer() ||
      step_to_d.has_array_task() || step_to_d.requeue_count() != 0)
    return {};
  auto parsed = crane::ParseExecutionFlowId(step_to_d.execution_flow_id());
  return parsed.value_or(std::string{});
}
#endif

#ifdef CRANE_ENABLE_EXECUTION_FLOW
void StepInstance::CleanUp(bool async, CleanupCompletion completion) {
  if (this->status != StepStatus::Completing &&
      !IsFinishedStepStatus(this->status)) {
    CRANE_WARN(
        "[Step #{}.{}] Cleaning up a step which is not in finished status, "
        "current status: {}.",
        job_id, step_id, static_cast<int>(this->status));
  }

  auto* cgroup = crane_cgroup.release();
  if (!crane::ExecutionFlowEnabled() || !completion ||
      ExecutionFlowId().empty()) {
    CleanUpStepCgroupWithoutFlow(async, job_id, step_id, cgroup, cg_str);
    return;
  }

  auto clean_step_cgroup = [job_id = job_id, step_id = step_id, cgroup,
                            step_cg_str = this->cg_str,
                            completion = std::move(completion)]() mutable {
    if (cgroup == nullptr) {
      CleanupResult result;
      result.cgroup_present = false;
      if (completion) completion(result);
      return;
    }

    CgroupManager::KillAndDestroyCgroup(
        std::unique_ptr<CgroupInterface>{cgroup},
        [job_id, step_id, step_cg_str = std::move(step_cg_str),
         completion = std::move(completion)](
            CgroupManager::CgroupCleanupResult cgroup_result) mutable {
          CleanupResult result;
          result.processes_drained = cgroup_result.processes_drained;
          result.cgroup_destroyed = cgroup_result.cgroup_destroyed;

          // step_cg_str is e.g. "overflow/job_1/step_0/system". Remove
          // step_N only after the asynchronous cgroup backend has finished.
          if (step_cg_str.empty()) {
            if (completion) completion(result);
            return;
          }
          auto step_cg_path =
              (std::filesystem::path{Common::CgConstant::kSystemCgPathPrefix} /
               Common::CgConstant::kRootCgNamePrefix / step_cg_str)
                  .parent_path();

          std::error_code ec;
          if (std::filesystem::exists(step_cg_path, ec)) {
            const bool removed = std::filesystem::remove(step_cg_path, ec);
            result.step_directory_removed = removed && !ec;
            if (!result.step_directory_removed) {
              CRANE_ERROR(
                  "[Step #{}.{}] Failed to remove step cgroup dir {}: {}",
                  job_id, step_id, step_cg_path,
                  ec ? ec.message() : "directory is not empty");
            } else {
              CRANE_DEBUG("[Step #{}.{}] Step cgroup dir {} removed.", job_id,
                          step_id, step_cg_path);
            }
          } else if (ec) {
            result.step_directory_removed = false;
            CRANE_ERROR(
                "[Step #{}.{}] Failed to check existence of step cgroup dir "
                "{}: {}",
                job_id, step_id, step_cg_path, ec.message());
          } else {
            CRANE_DEBUG(
                "[Step #{}.{}] Step cgroup dir {} does not exist, skip clean.",
                job_id, step_id, step_cg_path);
          }
          if (completion) completion(result);
        });
  };

  if (async) {
    g_thread_pool->detach_task(std::move(clean_step_cgroup));
  } else {
    clean_step_cgroup();
  }
}
#else
void StepInstance::CleanUp(bool async) {
  if (this->status != StepStatus::Completing &&
      !IsFinishedStepStatus(this->status)) {
    CRANE_WARN(
        "[Step #{}.{}] Cleaning up a step which is not in finished status, "
        "current status: {}.",
        job_id, step_id, static_cast<int>(this->status));
  }

  auto* cgroup = crane_cgroup.release();
  CleanUpStepCgroupWithoutFlow(async, job_id, step_id, cgroup, cg_str);
}
#endif

CraneErrCode StepInstance::Prepare(const Common::CgroupPathInfo& path_info) {
  job_path_info = path_info;
  cg_str = CgroupManager::CgroupStrByStepId(path_info.cg_str, step_id, true);

  auto cg_expt =
      CgroupManager::AllocateAndGetCgroup(cg_str, step_to_d.res(), false);
  if (!cg_expt) return cg_expt.error();

  this->crane_cgroup = std::move(cg_expt.value());
  auto* cg = this->crane_cgroup.get();
  return CraneErrCode::SUCCESS;
}

CraneErrCode StepInstance::SpawnSupervisor(const EnvMap& job_env_map) {
  CRANE_TRACE_SCOPE_FROM_REMOTE(spawn_span, "step/supervisor_spawn",
                                this->traceparent);
  spawn_span.SetAttribute("job_id", job_id);
  spawn_span.SetAttribute("step_id", step_id);
  spawn_span.SetAttribute("step_type",
                          static_cast<int64_t>(step_to_d.step_type()));

  using google::protobuf::io::FileInputStream;
  using google::protobuf::io::FileOutputStream;
  using google::protobuf::util::ParseDelimitedFromZeroCopyStream;
  using google::protobuf::util::SerializeDelimitedToZeroCopyStream;

  using crane::grpc::supervisor::CanStartMessage;
  using crane::grpc::supervisor::ChildProcessReady;

  std::array<int, 2> supervisor_craned_pipe{};
  std::array<int, 2> craned_supervisor_pipe{};

  if (pipe(supervisor_craned_pipe.data()) == -1) {
    CRANE_ERROR("Pipe creation failed!");
    spawn_span.SetStatus(crane::StatusCode::kError, "pipe_failed");
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (pipe(craned_supervisor_pipe.data()) == -1) {
    close(supervisor_craned_pipe[0]);
    close(supervisor_craned_pipe[1]);
    CRANE_ERROR("Pipe creation failed!");
    spawn_span.SetStatus(crane::StatusCode::kError, "pipe_failed");
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
    spawn_span.SetStatus(crane::StatusCode::kError, "fork_failed");

    close(craned_supervisor_pipe[0]);
    close(craned_supervisor_pipe[1]);
    close(supervisor_craned_pipe[0]);
    close(supervisor_craned_pipe[1]);
    return CraneErrCode::ERR_SYSTEM_ERR;
  }

  if (child_pid > 0) {  // Parent proc
    CRANE_DEBUG("[Step #{}.{}] Subprocess was created, pid: {}", job_id,
                step_id, child_pid);
    CRANE_FLOW_POINT(
        "craned/supervisor/forked", ExecutionFlowId(), traceparent,
        CRANE_FLOW_SET_ATTR("job_id", job_id);
        CRANE_FLOW_SET_ATTR("step_id", step_id);
        CRANE_FLOW_SET_ATTR("node_id", std::string{g_config.Hostname});
        CRANE_FLOW_SET_ATTR("operation", "fork-supervisor");
        CRANE_FLOW_SET_ATTR("outcome", "success"););

    bool ok;
    CanStartMessage msg;
    ChildProcessReady child_process_ready;

    int craned_supervisor_fd = craned_supervisor_pipe[1];
    close(craned_supervisor_pipe[0]);
    int supervisor_craned_fd = supervisor_craned_pipe[0];
    close(supervisor_craned_pipe[1]);

    FileInputStream istream(supervisor_craned_fd);
    FileOutputStream ostream(craned_supervisor_fd);

    // Do Supervisor Init
    CRANE_TRACE_CHILD_NAMED(init_span, spawn_span, "step/send_init");
    crane::grpc::supervisor::InitSupervisorRequest init_req;
    init_req.set_job_id(job_id);
    init_req.set_job_name(step_to_d.name());
    init_req.set_step_id(step_id);
    init_req.set_debug_level(g_config.Supervisor.DebugLevel);
    init_req.set_craned_id(g_config.CranedIdOfThisNode);
    init_req.set_craned_unix_socket_path(g_config.CranedUnixSockPath);
    init_req.set_crane_base_dir(g_config.CraneBaseDir);
    init_req.set_crane_script_dir(g_config.CranedScriptDir);
    init_req.mutable_step_spec()->CopyFrom(step_to_d);
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
    init_req.mutable_env()->clear();
    init_req.mutable_env()->insert(job_env_map.begin(), job_env_map.end());

    std::string cgroup_path_str = this->crane_cgroup->CgroupPath().string();
    init_req.set_supv_cgroup_path(cgroup_path_str);
    init_req.set_job_cg_str(this->job_path_info.cg_str);
    init_req.set_cpuset_cg_str(this->job_path_info.cpuset_cg_str);
    CRANE_TRACE("[Step #{}.{}] Setting cgroup path: {}, job_cg_str: {}", job_id,
                step_id, cgroup_path_str, this->job_path_info.cg_str);

    if (g_config.Container.Enabled) {
      auto* container_conf = init_req.mutable_container_config();
      container_conf->set_runtime_endpoint(g_config.Container.RuntimeEndpoint);
      container_conf->set_image_endpoint(g_config.Container.ImageEndpoint);
      auto* dns_conf = container_conf->mutable_dns_config();
      dns_conf->set_cluster_domain(g_config.Container.Dns.ClusterDomain);
      for (const auto& s : g_config.Container.Dns.Servers)
        dns_conf->add_servers(s);
      for (const auto& s : g_config.Container.Dns.Searches)
        dns_conf->add_searches(s);
      for (const auto& s : g_config.Container.Dns.Options)
        dns_conf->add_options(s);
      if (g_config.Container.BindFs.Enabled) {
        auto* bindfs_conf = container_conf->mutable_bindfs();
        bindfs_conf->set_bindfs_binary(
            g_config.Container.BindFs.BindfsBinary.string());
        bindfs_conf->set_fusermount_binary(
            g_config.Container.BindFs.FusermountBinary.string());
        bindfs_conf->set_mount_base_dir(
            g_config.Container.BindFs.MountBaseDir.string());
      }
      auto* subid_conf = container_conf->mutable_subid();
      subid_conf->set_managed(g_config.Container.SubId.Managed);
      for (const auto& mapping : g_config.Container.SubId.UidMappings) {
        auto* uid_mapping = subid_conf->add_uid_mappings();
        uid_mapping->set_id(mapping.Id);
        uid_mapping->set_id_count(mapping.IdCount);
        uid_mapping->set_subid_start(mapping.SubIdStart);
        uid_mapping->set_subid_size(mapping.SubIdSize);
      }
      for (const auto& mapping : g_config.Container.SubId.GidMappings) {
        auto* gid_mapping = subid_conf->add_gid_mappings();
        gid_mapping->set_id(mapping.Id);
        gid_mapping->set_id_count(mapping.IdCount);
        gid_mapping->set_subid_start(mapping.SubIdStart);
        gid_mapping->set_subid_size(mapping.SubIdSize);
      }
    }

    if (g_config.Plugin.Enabled) {
      auto* plugin_conf = init_req.mutable_plugin_config();
      plugin_conf->set_socket_path(g_config.Plugin.PlugindSockPath);
    }

    if (g_config.JobLifecycleHook.PrologFlags & PrologFlagEnum::RunInJob ||
        !g_config.JobLifecycleHook.TaskPrologs.empty() ||
        !g_config.JobLifecycleHook.TaskEpilogs.empty()) {
      auto* job_lifecycle_hook_conf =
          init_req.mutable_job_lifecycle_hook_config();

      for (const auto& prolog : g_config.JobLifecycleHook.TaskPrologs) {
        job_lifecycle_hook_conf->add_task_prologs(prolog);
      }
      for (const auto& epilog : g_config.JobLifecycleHook.TaskEpilogs) {
        job_lifecycle_hook_conf->add_task_epilogs(epilog);
      }

      if (g_config.JobLifecycleHook.PrologFlags & PrologFlagEnum::RunInJob &&
          IsDaemonStep()) {
        for (const auto& prolog : g_config.JobLifecycleHook.Prologs) {
          job_lifecycle_hook_conf->add_prologs(prolog);
        }
        for (const auto& epilog : g_config.JobLifecycleHook.Epilogs) {
          job_lifecycle_hook_conf->add_epilogs(epilog);
        }
      }

      job_lifecycle_hook_conf->set_prolog_timeout(
          g_config.JobLifecycleHook.PrologTimeout);
      job_lifecycle_hook_conf->set_epilog_timeout(
          g_config.JobLifecycleHook.EpilogTimeout);
      job_lifecycle_hook_conf->set_prolog_epilog_timeout(
          g_config.JobLifecycleHook.PrologEpilogTimeout);
      job_lifecycle_hook_conf->set_max_output_size(
          g_config.JobLifecycleHook.MaxOutputSize);
    }

    // Populate CDI devices into container_meta for container jobs.
    // CDI consistency (all-or-none per name/type) is validated at config
    // parse time in Craned.cpp, so here we only need to fill the list.
    if (init_req.step_spec().has_container_meta()) {
      auto* cm = init_req.mutable_step_spec()->mutable_container_meta();
      const auto& dedicated_res = this->step_to_d.res().gres();
      for (const auto& [dev_name, type_slots_map] :
           dedicated_res.name_type_map()) {
        for (const auto& [dev_type, slots] : type_slots_map.type_slots_map()) {
          for (const auto& slot_id : slots.slots()) {
            auto dev_it = Common::g_this_node_device.find(slot_id);
            if (dev_it != Common::g_this_node_device.end() &&
                dev_it->second->cdi_name.has_value()) {
              cm->add_cdi_devices(dev_it->second->cdi_name.value());
              CRANE_TRACE("[Step #{}.{}] CDI device: {} (slot: {})", job_id,
                          step_id, dev_it->second->cdi_name.value(), slot_id);
            }
          }
        }
      }
    }

    // CNI is pod-level, so GRES annotations apply to daemon pod steps even
    // though they do not carry container_meta.
    if (this->IsDaemonStep() && init_req.step_spec().has_pod_meta()) {
      const auto& dedicated_res = this->step_to_d.res().gres();
      auto cni_annos =
          Common::DeviceManager::GetCniGresAnnotations(dedicated_res);
      if (!cni_annos.has_value()) {
        CRANE_ERROR("[Step #{}.{}] {}", job_id, step_id, cni_annos.error());
        crane_cgroup->KillAllProcesses(SIGKILL);
        close(craned_supervisor_fd);
        close(supervisor_craned_fd);
        return CraneErrCode::ERR_INVALID_PARAM;
      }
      if (!cni_annos->empty()) {
        auto* annotations = init_req.mutable_step_spec()
                                ->mutable_pod_meta()
                                ->mutable_annotations();
        for (auto& [key, value] : cni_annos.value()) {
          std::string prefixed_key =
              fmt::format("{}{}", cri::kCriAnnotationPrefix, key);
          CRANE_TRACE("[Step #{}.{}] CNI GRES annotation: {}={}", job_id,
                      step_id, prefixed_key, value);
          (*annotations)[std::move(prefixed_key)] = std::move(value);
        }
      }
    }

    init_req.set_enable_slurm_compatible_env(g_config.EnableSlurmCompatibleEnv);
    init_req.set_thread_pool_size(g_config.Supervisor.ThreadPoolSize);

    init_req.set_tracing_enabled(g_config.Tracing.Enabled);
    init_req.set_trace_level(
        std::string{crane::TraceLevelToString(g_config.Tracing.Level)});
#ifdef CRANE_ENABLE_EXECUTION_FLOW
    if (crane::ExecutionFlowEnabled() && !ExecutionFlowId().empty()) {
      init_req.set_execution_flow_enabled(true);
      init_req.set_execution_flow_heartbeat_interval_seconds(std::max<uint32_t>(
          1, g_config.Tracing.ExecutionFlow.HeartbeatIntervalSeconds));
    }
#endif
    // Pass spawn span's context so step/execute becomes child of
    // step/supervisor_spawn, not just child of job/lifecycle.
    auto spawn_tp = crane::SerializeTraceParent(spawn_span.GetContext());
    if (!spawn_tp.empty())
      init_req.set_traceparent(spawn_tp);
    else if (!this->traceparent.empty())
      init_req.set_traceparent(this->traceparent);

    ok = SerializeDelimitedToZeroCopyStream(init_req, &ostream);
    if (!ok) {
      CRANE_ERROR("[Step #{}.{}] Failed to serialize msg to ostream: {}",
                  job_id, step_id, strerror(ostream.GetErrno()));
    }

    if (ok) ok &= ostream.Flush();
    if (!ok) {
      CRANE_ERROR("[Step #{}.{}] Failed to send init msg to supervisor: {}",
                  job_id, step_id, strerror(ostream.GetErrno()));
      init_span.SetStatus(crane::StatusCode::kError, "init_send_failed");
      spawn_span.SetStatus(crane::StatusCode::kError, "init_send_failed");

      crane_cgroup->KillAllProcesses(SIGKILL);

      close(craned_supervisor_fd);
      close(supervisor_craned_fd);
      return CraneErrCode::ERR_PROTOBUF;
    }

    CRANE_TRACE("[Step #{}.{}] Supervisor init msg send.", job_id, step_id);
    init_span.End();

    CRANE_TRACE_CHILD_NAMED(ready_span, spawn_span, "step/supervisor_ready");
    ready_span.SetAttribute("job_id", job_id);
    ready_span.SetAttribute("step_id", step_id);
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

      ready_span.SetStatus(crane::StatusCode::kError, "supervisor_not_ready");
      spawn_span.SetStatus(crane::StatusCode::kError, "supervisor_not_ready");
      crane_cgroup->KillAllProcesses(SIGKILL);

      close(craned_supervisor_fd);
      close(supervisor_craned_fd);
      return CraneErrCode::ERR_PROTOBUF;
    }

    CRANE_FLOW_POINT(
        "craned/supervisor/ready", ExecutionFlowId(), traceparent,
        CRANE_FLOW_SET_ATTR("job_id", job_id);
        CRANE_FLOW_SET_ATTR("step_id", step_id);
        CRANE_FLOW_SET_ATTR("node_id", std::string{g_config.Hostname});
        CRANE_FLOW_SET_ATTR("operation", "initialize-supervisor");
        CRANE_FLOW_SET_ATTR("outcome", "success"););

    close(craned_supervisor_fd);
    close(supervisor_craned_fd);

    // Migrate supervisor into the job's cgroup after it has finished
    // initialization. This avoids throttling supervisor startup by the
    // job's CPU quota.
    if (!this->crane_cgroup->MigrateProcIn(child_pid)) {
      CRANE_ERROR(
          "[Step #{}.{}] Failed to migrate supervisor pid {} into cgroup.",
          job_id, step_id, child_pid);
    }
    if (!CgroupManager::MigrateToCpuset(child_pid,
                                        this->job_path_info.cpuset_cg_str)) {
      CRANE_WARN("[Step #{}.{}] Failed to migrate supervisor to cpuset.",
                 job_id, step_id);
    }

    this->supervisor_stub = std::make_shared<SupervisorStub>(job_id, step_id);

    this->supv_pid = child_pid;
    return CraneErrCode::SUCCESS;
  } else {  // Child proc, NOLINT(readability-else-after-return)
    // Disable SIGABRT backtrace from child processes.
    signal(SIGABRT, SIG_DFL);

    if (setpgid(0, 0) == -1) {
      fmt::print(
          stderr,
          "[Step #{}.{}] Failed to isolate supervisor process group: {}\n",
          job_id, step_id, strerror(errno));
      _exit(1);
    }

    // Cgroup migration is deferred to csupervisor (after SupervisorReady)
    // so the supervisor init is not throttled by the job's CPU quota.
    int craned_supervisor_fd = craned_supervisor_pipe[0];
    close(craned_supervisor_pipe[1]);
    int supervisor_craned_fd = supervisor_craned_pipe[1];
    close(supervisor_craned_pipe[0]);

    if (!util::os::CloseFdFromExcept(
            3, {craned_supervisor_fd, supervisor_craned_fd})) {
      fmt::print(stderr,
                 "[Step #{}.{}] Failed to read /proc/self/fd, aborting.\n",
                 job_id, step_id);
      _exit(1);
    }

    std::vector<std::string> string_argv;
    std::vector<const char*> argv;

    auto supv_name = fmt::format("csupervisor: [{}.{}]", job_id, step_id);
    string_argv.emplace_back(supv_name.c_str());
    string_argv.push_back("--input-fd");
    string_argv.push_back(std::to_string(craned_supervisor_fd));
    string_argv.push_back("--output-fd");
    string_argv.push_back(std::to_string(supervisor_craned_fd));
    argv.reserve(string_argv.size());
    for (auto& arg : string_argv) {
      argv.push_back(arg.c_str());
    }
    argv.push_back(nullptr);  // argv must be null-terminated.
    fmt::print(stderr,
               "[{:%Y-%m-%d %H:%M:%S}] [Step #{}.{}]: Executing supervisor\n",
               std::chrono::system_clock::now(), job_id, step_id);

    // Use execvp to search the kSupervisorPath in the PATH.
    execvp(g_config.Supervisor.Path.c_str(),
           const_cast<char* const*>(argv.data()));

    // Error occurred since execvp returned. At this point, errno is set.
    // Ctld use SIGABRT to inform the client of this failure.
    fmt::print(stderr, "[Craned Subprocess] Failed to execvp {}. Error: {}\n",
               g_config.Supervisor.Path.c_str(), strerror(errno));

    // TODO: See https://tldp.org/LDP/abs/html/exitcodes.html, return standard
    // exit codes
    abort();
  }
}

void StepInstance::GotNewStatus(const StepStatus& new_status) {
  if (IsFinishedStepStatus(new_status)) {
    if (status != StepStatus::Running && status != StepStatus::Completing &&
        status != StepStatus::Starting && status != StepStatus::Configuring) {
      CRANE_WARN(
          "[Step {}.{}] Step status is not "
          "Running/Completing/Starting/Configuring when receiving new finished "
          "status {}, current status: {}.",
          job_id, step_id, new_status, this->status);
    }

    status = new_status;
    return;
  }

  switch (new_status) {
  case StepStatus::Configuring:
  case StepStatus::Pending:
  case StepStatus::Invalid: {
    CRANE_ERROR("[Step #{}.{}] Invalid new status received: {}, ignored.",
                job_id, step_id, new_status);
    return;
  }

  case StepStatus::Running: {
    if (this->IsDaemonStep()) {
      if (status != StepStatus::Configuring)
        CRANE_WARN(
            "[Step {}.{}] Daemon step status is not 'Configuring' when "
            "receiving new status 'Running', current status: {}.",
            job_id, step_id, this->status);
    } else {
      if (status != StepStatus::Starting)
        CRANE_WARN(
            "[Step {}.{}] Step status is not 'Starting' when receiving new "
            "status 'Running', current status: {}.",
            job_id, step_id, this->status);
    }
    break;
  }
  case StepStatus::Starting: {
    if (this->IsDaemonStep()) {
      CRANE_WARN(
          "[Step {}.{}] Daemon step got invalid status 'Starting' current "
          "status: {}.",
          job_id, step_id, this->status);
    } else {
      if (status != StepStatus::Configuring)
        CRANE_WARN(
            "[Step {}.{}] Step status is not 'Configuring' when "
            "receiving new status 'Starting', current status: {}.",
            job_id, step_id, this->status);
    }
    break;
  }

  case StepStatus::Completing: {
    // Starting -> Completing is used when a termination request reaches the
    // supervisor before ExecuteStep gets a chance to launch tasks.
    if (status != StepStatus::Running && status != StepStatus::Starting)
      CRANE_WARN(
          "[Step {}.{}] Step status is not 'Running/Starting' when receiving "
          "new status 'Completing', current status: {}.",
          job_id, step_id, this->status);
    break;
  }
  default: {
    std::unreachable();
  }
  }

  status = new_status;
}

void StepInstance::ExecuteStepAsync() {
  this->GotNewStatus(StepStatus::Running);

  g_thread_pool->detach_task([job_id = job_id, step_id = step_id,
                              stub = supervisor_stub] {
    auto result = stub->ExecuteStepWithStatus();
    if (result.Ok()) return;

    if (result.grpc_status == grpc::StatusCode::DEADLINE_EXCEEDED) {
      CRANE_WARN(
          "[Step #{}.{}] Supervisor ExecuteStep ack deadline exceeded; "
          "checking supervisor status before marking RPC failure.",
          job_id, step_id);

      for (int attempt = 1; attempt <= kCranedRpcTimeoutSeconds; ++attempt) {
        std::this_thread::sleep_for(1s);
        auto status = stub->CheckStatus();
        if (status.has_value()) {
          auto step_status = std::get<3>(*status);
          if (step_status == StepStatus::Running ||
              step_status == StepStatus::Completing ||
              IsFinishedStepStatus(step_status)) {
            CRANE_WARN(
                "[Step #{}.{}] ExecuteStep ack deadline treated as accepted; "
                "supervisor status is {}.",
                job_id, step_id, step_status);
            return;
          }
          CRANE_WARN(
              "[Step #{}.{}] ExecuteStep ack still unknown after attempt "
              "{}/{}; "
              "supervisor status is {}.",
              job_id, step_id, attempt, kCranedRpcTimeoutSeconds, step_status);
        } else {
          CRANE_WARN(
              "[Step #{}.{}] ExecuteStep ack still unknown after attempt "
              "{}/{}; "
              "supervisor status query failed.",
              job_id, step_id, attempt, kCranedRpcTimeoutSeconds);
        }
      }

      CRANE_ERROR(
          "[Step #{}.{}] ExecuteStep ack was not confirmed within {}s grace, "
          "marking step failed.",
          job_id, step_id, kCranedRpcTimeoutSeconds);
    } else {
      CRANE_ERROR(
          "[Step #{}.{}] Supervisor failed to accept ExecuteStep, code:{}, "
          "grpc_status:{}, error:{}.",
          job_id, step_id, static_cast<int>(result.code),
          static_cast<int>(result.grpc_status), result.error_message);
    }

    if (result.code != CraneErrCode::SUCCESS ||
        result.grpc_status != grpc::StatusCode::OK) {
      g_job_mgr->SendCompletingAndTerminal_(
          job_id, step_id, StepStatus::Failed, ExitCode::EC_RPC_ERR,
          "Supervisor not responding when execute step");
      // Ctld will send ShutdownSupervisor after status change from
      // daemon supervisor, for common step, will shut down itself when all
      // steps finished locally.
    }
  });
}

}  // namespace Craned

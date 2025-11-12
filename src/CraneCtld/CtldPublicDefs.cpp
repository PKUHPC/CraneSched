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

#include "CtldPublicDefs.h"

#include "EmbeddedDbClient.h"

namespace Ctld {

CranedRemoteMeta::CranedRemoteMeta(
    const crane::grpc::CranedRemoteMeta& grpc_meta)
    : dres_in_node(grpc_meta.dres_in_node()) {
  this->sys_rel_info.name = grpc_meta.sys_rel_info().name();
  this->sys_rel_info.release = grpc_meta.sys_rel_info().release();
  this->sys_rel_info.version = grpc_meta.sys_rel_info().version();
  this->craned_start_time =
      absl::FromUnixSeconds(grpc_meta.craned_start_time().seconds());
  this->system_boot_time =
      absl::FromUnixSeconds(grpc_meta.system_boot_time().seconds());

  this->network_interfaces.clear();
  for (const auto& interface : grpc_meta.network_interfaces()) {
    this->network_interfaces.emplace_back(interface);
  }
}

ContainerMetaInTask::ContainerMetaInTask(
    const crane::grpc::ContainerTaskAdditionalMeta& rhs)
    : image_info{.image = rhs.image().image(),
                 .username = rhs.image().username(),
                 .password = rhs.image().password(),
                 .server_address = rhs.image().server_address(),
                 .pull_policy = rhs.image().pull_policy()},
      name(rhs.name()),
      labels(rhs.labels().begin(), rhs.labels().end()),
      annotations(rhs.annotations().begin(), rhs.annotations().end()),
      command(rhs.command()),
      args(rhs.args().begin(), rhs.args().end()),
      workdir(rhs.workdir()),
      env(rhs.env().begin(), rhs.env().end()),
      detached(rhs.detached()),
      tty(rhs.tty()),
      stdin(rhs.stdin()),
      stdin_once(rhs.stdin_once()),
      userns(rhs.userns()),
      run_as_user(rhs.run_as_user()),
      run_as_group(rhs.run_as_group()),
      mounts(rhs.mounts().begin(), rhs.mounts().end()),
      port_mappings(rhs.ports().begin(), rhs.ports().end()) {}

ContainerMetaInTask::operator crane::grpc::ContainerTaskAdditionalMeta() const {
  crane::grpc::ContainerTaskAdditionalMeta result;

  auto* image = result.mutable_image();
  image->set_image(this->image_info.image);
  image->set_username(this->image_info.username);
  image->set_password(this->image_info.password);
  image->set_server_address(this->image_info.server_address);
  image->set_pull_policy(this->image_info.pull_policy);

  result.set_name(this->name);

  auto* labels_map = result.mutable_labels();
  for (const auto& label : this->labels) {
    (*labels_map)[label.first] = label.second;
  }

  auto* annotations_map = result.mutable_annotations();
  for (const auto& annotation : this->annotations) {
    (*annotations_map)[annotation.first] = annotation.second;
  }

  result.set_command(this->command);
  for (const auto& arg : this->args) {
    result.add_args(arg);
  }
  result.set_workdir(this->workdir);

  auto* env_map = result.mutable_env();
  for (const auto& env_var : this->env) {
    (*env_map)[env_var.first] = env_var.second;
  }

  result.set_detached(this->detached);
  result.set_tty(this->tty);
  result.set_stdin(this->stdin);
  result.set_stdin_once(this->stdin_once);

  result.set_userns(this->userns);
  result.set_run_as_user(this->run_as_user);
  result.set_run_as_group(this->run_as_group);

  auto* mounts_map = result.mutable_mounts();
  for (const auto& mount : this->mounts) {
    (*mounts_map)[mount.first] = mount.second;
  }

  auto* ports_map = result.mutable_ports();
  for (const auto& port : this->port_mappings) {
    (*ports_map)[port.first] = port.second;
  }

  return result;
}

void StepInCtld::SetStepType(crane::grpc::StepType type) {
  step_type = type;
  this->m_runtime_attr_.set_step_type(type);
}

crane::grpc::StepType StepInCtld::StepType() const { return step_type; }

const crane::grpc::StepToCtld& StepInCtld::StepToCtld() const {
  return m_step_to_ctld_;
}
crane::grpc::StepToCtld* StepInCtld::MutableStepToCtld() {
  return &m_step_to_ctld_;
}

void StepInCtld::SetStepId(step_id_t id) {
  m_step_id_ = id;
  this->m_runtime_attr_.set_step_id(id);
}
void StepInCtld::SetStepDbId(step_db_id_t id) {
  m_step_db_id_ = id;
  this->m_runtime_attr_.set_step_db_id(id);
}

void StepInCtld::SetRequeueCount(std::int32_t count) {
  this->m_requeue_count_ = count;
  this->m_runtime_attr_.set_requeue_count(count);
}

void StepInCtld::SetAllocatedRes(const ResourceV2& res) {
  this->m_allocated_res_ = res;
  *this->m_runtime_attr_.mutable_allocated_res() =
      static_cast<crane::grpc::ResourceV2>(res);
}

void StepInCtld::SetCranedIds(const std::unordered_set<CranedId>& craned_list) {
  this->m_craned_ids_ = craned_list;
  this->m_runtime_attr_.mutable_craned_ids()->Assign(craned_list.begin(),
                                                     craned_list.end());
}

void StepInCtld::SetExecutionNodes(const std::unordered_set<CranedId>& nodes) {
  this->m_execute_nodes_ = nodes;
  this->m_runtime_attr_.mutable_execution_nodes()->Assign(nodes.begin(),
                                                          nodes.end());
}

void StepInCtld::SetConfiguringNodes(
    const std::unordered_set<CranedId>& nodes) {
  this->m_configuring_nodes_ = nodes;
  this->m_runtime_attr_.mutable_configuring_nodes()->Assign(nodes.begin(),
                                                            nodes.end());
}

void StepInCtld::NodeConfigured(const CranedId& node) {
  this->m_configuring_nodes_.erase(node);
  this->m_runtime_attr_.mutable_configuring_nodes()->Assign(
      m_configuring_nodes_.begin(), m_configuring_nodes_.end());
}

void StepInCtld::SetRunningNodes(const std::unordered_set<CranedId>& nodes) {
  this->m_running_nodes_ = nodes;
  this->m_runtime_attr_.mutable_running_nodes()->Assign(nodes.begin(),
                                                        nodes.end());
}

void StepInCtld::StepOnNodeFinish(const CranedId& node) {
  this->m_running_nodes_.erase(node);
  this->m_runtime_attr_.mutable_running_nodes()->Assign(
      m_running_nodes_.begin(), m_running_nodes_.end());
}

void StepInCtld::SetSubmitTime(absl::Time submit_time) {
  m_submit_time_ = submit_time;
  this->m_runtime_attr_.mutable_submit_time()->set_seconds(
      ToUnixSeconds(submit_time));
}
void StepInCtld::SetStartTime(absl::Time start_time) {
  m_start_time_ = start_time;
  this->m_runtime_attr_.mutable_start_time()->set_seconds(
      ToUnixSeconds(start_time));
}
void StepInCtld::SetEndTime(absl::Time end_time) {
  m_end_time_ = end_time;
  this->m_runtime_attr_.mutable_end_time()->set_seconds(
      ToUnixSeconds(end_time));
}

void StepInCtld::SetErrorStatus(crane::grpc::TaskStatus failed_status) {
  m_error_status = failed_status;
  this->m_runtime_attr_.set_error_status(failed_status);
}

void StepInCtld::SetErrorExitCode(uint32_t exit_code) {
  m_error_exit_code_ = exit_code;
  this->m_runtime_attr_.set_error_exit_code(exit_code);
}

void StepInCtld::SetStatus(crane::grpc::TaskStatus new_status) {
  this->m_status_ = new_status;
  this->m_runtime_attr_.set_status(new_status);
}

void StepInCtld::SetExitCode(uint32_t exit_code) {
  this->m_exit_code_ = exit_code;
  this->m_runtime_attr_.set_exit_code(exit_code);
}

void StepInCtld::SetHeld(bool held) {
  this->m_held_ = held;
  this->m_runtime_attr_.set_held(held);
}

void StepInCtld::RecoverFromDb(
    const TaskInCtld& job, crane::grpc::StepInEmbeddedDb const& step_in_db) {
  const auto& step_to_ctld = step_in_db.step_to_ctld();
  const auto& runtime_attr = step_in_db.runtime_attr();
  type = step_to_ctld.type();
  this->job = const_cast<TaskInCtld*>(&job);
  job_id = step_to_ctld.job_id();
  uid = step_to_ctld.uid();
  gids = {step_to_ctld.gid().begin(), step_to_ctld.gid().end()};
  name = step_to_ctld.name();
  ntasks_per_node = step_to_ctld.ntasks_per_node();
  cpus_per_task = cpu_t(step_to_ctld.cpus_per_task());

  requeue_if_failed = step_to_ctld.requeue_if_failed();
  get_user_env = step_to_ctld.get_user_env();
  env = step_to_ctld.env() | std::ranges::to<std::unordered_map>();

  if (job.IsContainer() && step_to_ctld.has_container_meta())
    container_meta =
        static_cast<ContainerMetaInTask>(step_to_ctld.container_meta());

  time_limit = absl::Seconds(step_to_ctld.time_limit().seconds());
  requested_node_res_view =
      static_cast<ResourceView>(step_to_ctld.req_resources());
  node_num = step_to_ctld.node_num();

  SetStepDbId(runtime_attr.step_db_id());
  SetStepId(runtime_attr.step_id());
  SetStepType(runtime_attr.step_type());

  SetRequeueCount(runtime_attr.requeue_count());
  SetAllocatedRes(static_cast<ResourceV2>(runtime_attr.allocated_res()));

  SetCranedIds(
      {runtime_attr.craned_ids().begin(), runtime_attr.craned_ids().end()});
  SetExecutionNodes({runtime_attr.execution_nodes().begin(),
                     runtime_attr.execution_nodes().end()});
  SetConfiguringNodes({runtime_attr.configuring_nodes().begin(),
                       runtime_attr.configuring_nodes().end()});
  SetRunningNodes({runtime_attr.running_nodes().begin(),
                   runtime_attr.running_nodes().end()});

  SetSubmitTime(absl::FromUnixSeconds(runtime_attr.submit_time().seconds()));
  SetStartTime(absl::FromUnixSeconds(runtime_attr.start_time().seconds()));
  SetEndTime(absl::FromUnixSeconds(runtime_attr.end_time().seconds()));

  SetErrorStatus(runtime_attr.error_status());
  SetErrorExitCode(runtime_attr.error_exit_code());
  SetStatus(runtime_attr.status());
  SetExitCode(runtime_attr.exit_code());
  SetHeld(runtime_attr.held());

  m_step_to_ctld_ = step_to_ctld;
  m_runtime_attr_ = runtime_attr;
}

void StepInCtld::SetFieldsOfStepInfo(
    crane::grpc::StepInfo* step_info) const noexcept {
  step_info->set_type(type);
  step_info->set_step_type(step_type);
  step_info->set_job_id(job_id);
  step_info->set_step_id(m_step_id_);
  step_info->set_name(name);
  step_info->set_uid(uid);
  step_info->mutable_gid()->Assign(gids.begin(), gids.end());

  step_info->mutable_time_limit()->set_seconds(ToInt64Seconds(time_limit));
  step_info->mutable_submit_time()->CopyFrom(m_runtime_attr_.submit_time());
  step_info->mutable_start_time()->CopyFrom(m_runtime_attr_.start_time());
  step_info->mutable_end_time()->CopyFrom(m_runtime_attr_.end_time());

  step_info->set_node_num(node_num);
  // string cmd_line = 13;
  // string cwd = 14;

  *step_info->mutable_req_res_view() =
      static_cast<crane::grpc::ResourceView>(requested_node_res_view);
  step_info->mutable_req_nodes()->Assign(included_nodes.begin(),
                                         included_nodes.end());
  step_info->mutable_exclude_nodes()->Assign(excluded_nodes.begin(),
                                             excluded_nodes.end());

  // string extra_attr = 21;
  if (container_meta.has_value()) {
    *step_info->mutable_container_meta() =
        std::move(static_cast<crane::grpc::ContainerTaskAdditionalMeta>(
            container_meta.value()));
  }
  step_info->set_held(m_held_);
  step_info->set_status(m_status_);
  step_info->set_exit_code(m_exit_code_);
  // oneof pending_reason_or_craned_list {
  // string pending_reason = 35;
  // string craned_list = 36;

  step_info->mutable_execution_node()->Assign(m_execute_nodes_.begin(),
                                              m_execute_nodes_.end());
  // ResourceView allocated_res_view = 40;
}

void DaemonStepInCtld::InitFromJob(const TaskInCtld& job) {
  /*Fields in StepInCtld*/
  this->job = const_cast<TaskInCtld*>(&job);
  type = job.type;
  job_id = job.TaskId();
  uid = job.uid;
  gids = {job.gid};
  name = job.name;

  ntasks_per_node = job.ntasks_per_node;
  cpus_per_task = job.cpus_per_task;

  requeue_if_failed = job.requeue_if_failed;
  get_user_env = job.get_user_env;
  env = job.env;
  extra_attr = job.extra_attr;

  time_limit = job.time_limit;
  requested_node_res_view = job.requested_node_res_view;
  node_num = job.node_num;
  included_nodes = job.included_nodes;
  excluded_nodes = job.excluded_nodes;

  if (job.IsContainer()) {
    container_meta = std::get<ContainerMetaInTask>(job.meta);
  }

  SetStepType(crane::grpc::StepType::DAEMON);

  SetRequeueCount(0);
  SetAllocatedRes(job.AllocatedRes());

  SetCranedIds({job.CranedIds().begin(), job.CranedIds().end()});
  SetExecutionNodes(CranedIds());
  SetConfiguringNodes(CranedIds());
  SetRunningNodes(CranedIds());

  SetSubmitTime(job.SubmitTime());
  SetStartTime(job.StartTime());
  SetEndTime(job.EndTime());

  SetErrorStatus(crane::grpc::TaskStatus::Invalid);
  SetErrorExitCode(0u);
  SetStatus(crane::grpc::TaskStatus::Configuring);
  SetHeld(false);

  /* Fields in DaemonStepInCtld */
  partition = job.partition_id;
  account = job.account;
  qos = job.qos;

  crane::grpc::StepToCtld step;
  step.mutable_time_limit()->CopyFrom(
      google::protobuf::util::TimeUtil::MillisecondsToDuration(
          ToInt64Milliseconds(time_limit)));
  step.set_job_id(job.TaskId());
  *step.mutable_req_resources() =
      static_cast<crane::grpc::ResourceView>(requested_node_res_view);
  step.set_type(job.type);
  step.set_uid(uid);
  step.set_name(name);
  step.set_node_num(node_num);
  step.set_ntasks_per_node(ntasks_per_node);
  step.set_cpus_per_task(static_cast<double>(cpus_per_task));

  step.set_requeue_if_failed(requeue_if_failed);
  step.set_get_user_env(get_user_env);
  step.mutable_gid()->Assign(gids.begin(), gids.end());
  // No batch or ia meta need to set
  step.set_extra_attr(job.extra_attr);
  // step.set_cmd_line(job.cmd_line);
  // step.set_cwd();
  step.mutable_env()->insert(env.begin(), env.end());
  step.set_excludes(job.TaskToCtld().excludes());
  step.set_nodelist(job.TaskToCtld().nodelist());

  if (job.IsContainer()) {
    step.mutable_container_meta()->CopyFrom(
        crane::grpc::ContainerTaskAdditionalMeta(container_meta.value()));
  }

  *MutableStepToCtld() = std::move(step);
}

crane::grpc::JobToD DaemonStepInCtld::GetJobToD(
    const CranedId& craned_id) const {
  crane::grpc::JobToD job_to_d;
  job_to_d.set_job_id(job_id);
  job_to_d.set_uid(uid);
  *job_to_d.mutable_res() =
      crane::grpc::ResourceInNode(m_allocated_res_.at(craned_id));
  return job_to_d;
}

crane::grpc::StepToD DaemonStepInCtld::GetStepToD(
    const CranedId& craned_id) const {
  crane::grpc::StepToD step_to_d;
  auto* mutable_res_in_node = step_to_d.mutable_res();
  *mutable_res_in_node =
      static_cast<crane::grpc::ResourceInNode>(m_allocated_res_.at(craned_id));

  // Set type
  step_to_d.set_type(this->type);
  step_to_d.set_step_type(this->step_type);

  step_to_d.set_job_id(this->job_id);
  step_to_d.set_step_id(this->m_step_id_);
  step_to_d.set_name(this->name);

  step_to_d.set_node_num(this->node_num);

  step_to_d.set_uid(uid);
  step_to_d.mutable_gid()->Assign(this->gids.begin(), this->gids.end());
  step_to_d.mutable_env()->insert(this->env.begin(), this->env.end());
  step_to_d.set_get_user_env(this->get_user_env);
  step_to_d.set_extra_attr(extra_attr);

  for (const auto& hostname : this->m_craned_ids_)
    step_to_d.mutable_nodelist()->Add()->assign(hostname);

  step_to_d.mutable_start_time()->set_seconds(
      ToUnixSeconds(this->m_start_time_));
  step_to_d.mutable_submit_time()->set_seconds(
      ToUnixSeconds(this->m_submit_time_));
  step_to_d.mutable_time_limit()->set_seconds(ToInt64Seconds(this->time_limit));

  if (this->container_meta.has_value())
    step_to_d.mutable_container_meta()->CopyFrom(
        crane::grpc::ContainerTaskAdditionalMeta(container_meta.value()));

  return step_to_d;
}

std::optional<std::pair<crane::grpc::TaskStatus, uint32_t>>
DaemonStepInCtld::StepStatusChange(crane::grpc::TaskStatus new_status,
                                   uint32_t exit_code,
                                   const std::string& reason,
                                   const CranedId& craned_id,
                                   google::protobuf::Timestamp timestamp,
                                   StepStatusChangeContext* context) {
  bool job_finished{false};

  CRANE_TRACE("[Step #{}.{}] current status {}, got new status {} from {}",
              job_id, this->StepId(), this->Status(), new_status, craned_id);
  switch (this->Status()) {
  case crane::grpc::TaskStatus::Configuring:
    // Configuring -> Failed / Running
    this->NodeConfigured(craned_id);
    if (new_status != crane::grpc::TaskStatus::Running) {
      this->SetErrorStatus(new_status);
      this->SetErrorExitCode(exit_code);
    }
    if (this->AllNodesConfigured()) {
      if (this->PrevErrorStatus())
        job_finished = true;
      else {
        CRANE_TRACE("[Step #{}.{}] CONFIGURING->Running", job_id,
                    this->StepId());
        this->SetStatus(crane::grpc::TaskStatus::Running);
        this->SetErrorStatus(crane::grpc::TaskStatus::Invalid);
        this->SetErrorExitCode(0u);

        context->rn_step_raw_ptrs.insert(this);
        std::unique_ptr primary_step = std::make_unique<CommonStepInCtld>();
        primary_step->InitPrimaryStepFromJob(*job);
        // TODO: Aggregate this operation
        if (!g_embedded_db_client->AppendSteps({primary_step.get()})) {
          this->SetStatus(crane::grpc::TaskStatus::Failed);
          job_finished = true;
          CRANE_ERROR("[Job #{}] Failed to append a step to embedded db.",
                      job->TaskId());
          context->rn_step_raw_ptrs.erase(this);
          break;
        }
        if (primary_step->type == crane::grpc::Interactive) {
          const auto& meta = primary_step->ia_meta.value();
          meta.cb_step_res_allocated(StepInteractiveMeta::StepResAllocArgs{
              .job_id = primary_step->job_id,
              .step_id = primary_step->StepId(),
              .allocated_nodes{std::make_pair(
                  job->allocated_craneds_regex,
                  job->CranedIds() | std::ranges::to<std::unordered_set>())}});
        }
        job->SetPrimaryStep(std::move(primary_step));
        for (auto& node_id : job->PrimaryStep()->ExecutionNodes()) {
          context->craned_step_alloc_map[node_id].emplace_back(
              job->PrimaryStep()->GetStepToD(node_id));
        }
      }
    }
    break;

  case crane::grpc::TaskStatus::Running:
  case crane::grpc::TaskStatus::Completing:
    // Completing -> Completed / Failed

    this->StepOnNodeFinish(craned_id);
    if (new_status != crane::grpc::TaskStatus::Completed) {
      this->SetErrorStatus(new_status);
      this->SetErrorExitCode(exit_code);
    }
    job_finished = this->AllNodesFinished();
    break;

  default: {
    CRANE_ASSERT_MSG(
        false, fmt::format("Invalid step status, current: {}, new status: {}",
                           StepStatusToString(this->Status()),
                           StepStatusToString(new_status)));
    std::unreachable();
  }
  }

  if (job_finished) {
    context->step_raw_ptrs.insert(this);
    context->step_ptrs.emplace(job->ReleaseDaemonStep());
    this->SetEndTime(absl::FromUnixSeconds(timestamp.seconds()) +
                     absl::Nanoseconds(timestamp.nanos()));
    if (this->Status() == crane::grpc::TaskStatus::Configuring) {
      this->SetStatus(this->PrevErrorStatus().value());
      this->SetExitCode(this->PrevErrorExitCode());
      CRANE_INFO("[Step #{}.{}] ConfigureFailed with status {}.", job_id,
                 this->StepId(), this->Status());
      // Daemon step failed to configure, terminate all daemon step,
      for (const auto& node : this->CranedIds()) {
        if (node == craned_id) continue;
        context->craned_orphaned_steps[node][job_id].emplace(this->StepId());
      }

      context->craned_jobs_to_free[craned_id].emplace_back(job->TaskId());
      return std::pair{this->Status(), this->ExitCode()};
    } else {
      if (std::optional error_status = this->PrevErrorStatus(); error_status) {
        this->SetStatus(error_status.value());
        this->SetExitCode(this->PrevErrorExitCode());
      } else {
        this->SetStatus(crane::grpc::TaskStatus::Completed);
        this->SetExitCode(0U);
      }
      CRANE_INFO("[Step #{}.{}] FINISHED with status {}.", job_id,
                 this->StepId(), this->Status());
      return std::pair{job->PrimaryStepStatus(), job->PrimaryStepExitCode()};
    }
  }
  return std::nullopt;
}

void DaemonStepInCtld::RecoverFromDb(
    const TaskInCtld& job, const crane::grpc::StepInEmbeddedDb& step_in_db) {
  StepInCtld::RecoverFromDb(job, step_in_db);
  partition = job.partition_id;
  account = job.account;
  qos = job.qos;
}

void DaemonStepInCtld::SetFieldsOfStepInfo(
    crane::grpc::StepInfo* step_info) const noexcept {
  StepInCtld::SetFieldsOfStepInfo(step_info);
  step_info->set_cwd("");
  step_info->set_cmd_line("");
  step_info->set_extra_attr(extra_attr);
  step_info->set_craned_list(job->allocated_craneds_regex);
  *step_info->mutable_allocated_res_view() =
      static_cast<crane::grpc::ResourceView>(job->allocated_res_view);
}

void CommonStepInCtld::InitPrimaryStepFromJob(const TaskInCtld& job) {
  /*Fields in StepInCtld*/

  this->job = const_cast<TaskInCtld*>(&job);
  type = job.type;

  job_id = job.TaskId();
  uid = job.uid;
  gids = {job.gid};
  name = job.name;

  ntasks_per_node = job.ntasks_per_node;
  cpus_per_task = job.cpus_per_task;

  requeue_if_failed = job.requeue_if_failed;
  get_user_env = job.get_user_env;
  env = job.env;

  time_limit = job.time_limit;
  requested_node_res_view = job.requested_node_res_view;
  node_num = job.node_num;
  included_nodes = job.included_nodes;
  excluded_nodes = job.excluded_nodes;

  if (job.IsContainer())
    container_meta = std::get<ContainerMetaInTask>(job.meta);

  SetStepType(crane::grpc::StepType::PRIMARY);

  SetRequeueCount(0);
  SetAllocatedRes(job.AllocatedRes());

  SetCranedIds({job.CranedIds().begin(), job.CranedIds().end()});
  SetExecutionNodes(job.executing_craned_ids |
                    std::ranges::to<std::unordered_set>());
  SetConfiguringNodes(ExecutionNodes());
  SetRunningNodes(ExecutionNodes());

  SetSubmitTime(job.SubmitTime());
  SetStartTime(job.StartTime());
  SetEndTime(job.EndTime());

  SetErrorStatus(crane::grpc::TaskStatus::Invalid);
  SetErrorExitCode(0u);
  SetStatus(crane::grpc::TaskStatus::Configuring);
  SetHeld(false);

  /*Fields in CommonStepInCtld*/
  cmd_line = job.cmd_line;
  cwd = job.cwd;
  extra_attr = job.extra_attr;
  if (job.IsInteractive()) {
    ia_meta = std::get<InteractiveMeta>(std::move(job.meta));
  }

  allocated_craneds_regex = job.allocated_craneds_regex;
  pending_reason = "";

  crane::grpc::StepToCtld step;

  step.mutable_time_limit()->CopyFrom(
      google::protobuf::util::TimeUtil::MillisecondsToDuration(
          ToInt64Milliseconds(time_limit)));
  step.set_job_id(job.TaskId());
  *step.mutable_req_resources() =
      static_cast<crane::grpc::ResourceView>(requested_node_res_view);
  step.set_type(job.type);
  step.set_uid(uid);
  step.set_name(name);
  step.set_node_num(node_num);
  step.set_ntasks_per_node(ntasks_per_node);
  step.set_cpus_per_task(static_cast<double>(cpus_per_task));

  step.set_requeue_if_failed(requeue_if_failed);
  step.set_get_user_env(get_user_env);
  step.mutable_gid()->Assign(gids.begin(), gids.end());

  if (job.type == crane::grpc::Batch) {
    step.mutable_batch_meta()->CopyFrom(job.TaskToCtld().batch_meta());
  } else if (job.IsInteractive()) {
    step.mutable_interactive_meta()->CopyFrom(
        job.TaskToCtld().interactive_meta());
  } else if (job.IsContainer()) {
    step.mutable_container_meta()->CopyFrom(job.TaskToCtld().container_meta());
  }

  step.set_extra_attr(job.extra_attr);
  step.set_cmd_line(job.cmd_line);
  step.set_cwd(job.cwd);
  step.mutable_env()->insert(env.begin(), env.end());
  step.set_excludes(job.TaskToCtld().excludes());
  step.set_nodelist(job.TaskToCtld().nodelist());

  *MutableStepToCtld() = std::move(step);
}

bool CommonStepInCtld::IsPrimaryStep() const noexcept {
  return step_type == crane::grpc::StepType::PRIMARY;
}

void CommonStepInCtld::SetFieldsByStepToCtld(
    const crane::grpc::StepToCtld& step_to_ctld) {
  /*Fields in StepInCtld*/
  type = step_to_ctld.type();
  job_id = step_to_ctld.job_id();
  uid = step_to_ctld.uid();
  gids = step_to_ctld.gid() | std::ranges::to<std::vector>();
  name = step_to_ctld.name();

  ntasks_per_node = step_to_ctld.ntasks_per_node();
  cpus_per_task = static_cast<cpu_t>(step_to_ctld.cpus_per_task());

  requeue_if_failed = step_to_ctld.requeue_if_failed();
  get_user_env = step_to_ctld.get_user_env();
  env = step_to_ctld.env() | std::ranges::to<std::unordered_map>();
  extra_attr = step_to_ctld.extra_attr();

  time_limit = absl::Seconds(step_to_ctld.time_limit().seconds());
  requested_node_res_view = step_to_ctld.req_resources();
  node_num = step_to_ctld.node_num();

  {
    std::list<std::string> included_list{};
    util::ParseHostList(step_to_ctld.nodelist(), &included_list);
    included_nodes = included_list | std::ranges::to<std::unordered_set>();
  }

  {
    std::list<std::string> excluded_list{};
    util::ParseHostList(step_to_ctld.excludes(), &excluded_list);
    excluded_nodes = excluded_list | std::ranges::to<std::unordered_set>();
  }
  if (step_to_ctld.type() == crane::grpc::TaskType::Container)
    container_meta =
        static_cast<ContainerMetaInTask>(step_to_ctld.container_meta());

  cmd_line = step_to_ctld.cmd_line();
  cwd = step_to_ctld.cwd();

  SetStepType(crane::grpc::StepType::COMMON);

  SetRequeueCount(0);
  SetErrorStatus(crane::grpc::TaskStatus::Invalid);
  SetErrorExitCode(0u);
  SetStatus(crane::grpc::TaskStatus::Pending);
  SetHeld(false);
  *MutableStepToCtld() = step_to_ctld;
}

crane::grpc::StepToD CommonStepInCtld::GetStepToD(
    const CranedId& craned_id) const {
  crane::grpc::StepToD step_to_d;

  auto* mutable_res_in_node = step_to_d.mutable_res();
  *mutable_res_in_node =
      static_cast<crane::grpc::ResourceInNode>(m_allocated_res_.at(craned_id));

  // Set type
  step_to_d.set_type(this->type);
  step_to_d.set_step_type(this->step_type);

  step_to_d.set_job_id(this->job_id);
  step_to_d.set_step_id(m_step_id_);
  step_to_d.set_name(this->name);

  step_to_d.set_node_num(this->node_num);
  step_to_d.set_ntasks_per_node(this->ntasks_per_node);
  step_to_d.set_cpus_per_task(static_cast<double>(this->cpus_per_task));

  step_to_d.set_uid(uid);
  step_to_d.mutable_gid()->Assign(this->gids.begin(), this->gids.end());
  step_to_d.mutable_env()->insert(this->env.begin(), this->env.end());

  step_to_d.set_cwd(this->cwd);
  step_to_d.set_get_user_env(this->get_user_env);
  step_to_d.set_extra_attr(extra_attr);

  for (const auto& hostname : this->m_craned_ids_)
    step_to_d.mutable_nodelist()->Add()->assign(hostname);

  step_to_d.mutable_start_time()->set_seconds(
      ToUnixSeconds(this->m_start_time_));
  step_to_d.mutable_submit_time()->set_seconds(
      ToUnixSeconds(this->m_submit_time_));
  step_to_d.mutable_time_limit()->set_seconds(ToInt64Seconds(this->time_limit));

  if (this->type == crane::grpc::Batch) {
    auto* mutable_meta = step_to_d.mutable_batch_meta();
    mutable_meta->CopyFrom(StepToCtld().batch_meta());
  } else if (this->type == crane::grpc::Interactive) {
    auto* mutable_meta = step_to_d.mutable_interactive_meta();
    mutable_meta->CopyFrom(StepToCtld().interactive_meta());
  } else if (this->type == crane::grpc::Container) {
    auto* mutable_meta = step_to_d.mutable_container_meta();
    mutable_meta->CopyFrom(StepToCtld().container_meta());
  }

  return step_to_d;
}

void CommonStepInCtld::StepStatusChange(crane::grpc::TaskStatus new_status,
                                        uint32_t exit_code,
                                        const std::string& reason,
                                        const CranedId& craned_id,
                                        google::protobuf::Timestamp timestamp,
                                        StepStatusChangeContext* context) {
  /**
   * Step final status
   * finished: step configured successfully, got all step execution status
   * change
   * configure_failed: step failed to configure, terminate the step
   * For both status, job finish if primary step.
   */
  auto step_id = this->StepId();

  bool step_finished{false};
  // Step failed to configure, terminate step
  bool step_configure_failed{false};

  CRANE_TRACE("[Step #{}.{}] current status {}, got new status {} from {}",
              job_id, step_id, this->Status(), new_status, craned_id);
  if (this->Status() == crane::grpc::TaskStatus::Configuring) {
    // Configuring -> Configured / Failed / Cancelled,
    this->NodeConfigured(craned_id);
    if (new_status != crane::grpc::TaskStatus::Configured) {
      this->SetErrorStatus(new_status);
      this->SetErrorExitCode(exit_code);
    }
    if (this->AllNodesConfigured()) {
      if (this->PrevErrorStatus().has_value()) {
        // Configuring -> Failed
        step_configure_failed = true;
      } else {
        // Configuring -> Running
        // All supervisor ready without failure, start execution.
        CRANE_INFO("[Step #{}.{}] is ready to run.", job_id, step_id);
        // No need to set to Configured, make it running and process failed
        // cases by step status change
        this->SetStatus(crane::grpc::TaskStatus::Running);
        this->SetErrorStatus(crane::grpc::TaskStatus::Invalid);
        this->SetErrorExitCode(0u);
        this->SetRunningNodes(this->ExecutionNodes());

        // Primary:Update job status when primary step is Running.
        if (this->IsPrimaryStep()) {
          job->SetStatus(crane::grpc::TaskStatus::Running);
          context->rn_job_raw_ptrs.insert(job);
        }

        // Launch step execution
        for (auto& node : this->ExecutionNodes()) {
          context->craned_step_exec_map[node][job_id].insert(step_id);
        }
        context->rn_step_raw_ptrs.insert(this);
      }
    }
  } else if (this->Status() == crane::grpc::TaskStatus::Running) {
    // Running -> Completed / Failed / Cancelled,
    // Primary: the job is completed.

    this->StepOnNodeFinish(craned_id);
    if (new_status != crane::grpc::TaskStatus::Completed) {
      this->SetErrorStatus(new_status);
      this->SetErrorExitCode(exit_code);
    }
    CRANE_DEBUG(
        "[Step #{}.{}] got a finish status, waiting for {} status change.",
        job_id, step_id, this->RunningNodes().size());
    step_finished = this->AllNodesFinished();

  } else {
    CRANE_ASSERT_MSG(
        false, fmt::format("Invalid step status, current: {}, new status: {}",
                           StepStatusToString(Status()),
                           StepStatusToString(new_status)));
    std::unreachable();
  }

  // Step finish: configure failed or execution status change
  if (step_finished || step_configure_failed) {
    if (this->ia_meta.has_value()) {
      auto& meta = this->ia_meta.value();
      if (!meta.has_been_cancelled_on_front_end) {
        meta.has_been_cancelled_on_front_end = true;
        meta.cb_step_cancel({.job_id = job_id, .step_id = step_id});
        // Completion ack will send in grpc server triggered by task complete
        // req
        meta.cb_step_completed({.job_id = job_id,
                                .step_id = step_id,
                                .send_completion_ack = false,
                                .cfored_name = meta.cfored_name});
      } else {
        // Send Completion Ack to frontend now.
        meta.cb_step_completed({.job_id = job_id,
                                .step_id = step_id,
                                .send_completion_ack = true,
                                .cfored_name = meta.cfored_name});
      }
    }
    this->SetEndTime(absl::FromUnixSeconds(timestamp.seconds()) +
                     absl::Nanoseconds(timestamp.nanos()));
    if (this->Status() == crane::grpc::TaskStatus::Configuring) {
      CRANE_INFO("[Step #{}.{}] CONFIGURE_FAILED.", job_id, step_id);
      // CONFIGURE_FAILED
      this->SetStatus(this->PrevErrorStatus().value());
      this->SetExitCode(this->PrevErrorExitCode());
      // Step failed to configure, terminate this step
      for (const auto& node : this->ExecutionNodes()) {
        if (node != craned_id)
          context->craned_orphaned_steps[node][job->TaskId()].emplace(step_id);
      }

      if (this->IsPrimaryStep()) {
        job->DaemonStep()->SetStatus(crane::grpc::Completing);
        context->rn_step_raw_ptrs.emplace(job->DaemonStep());
        // Primary step CONFIGURE_FAILED, free daemon step, will send status
        // change.
        for (const auto& node : job->DaemonStep()->CranedIds()) {
          context->craned_jobs_to_free[node].emplace_back(job->TaskId());
        }
      }
    } else {
      // Step COMPLETED
      if (this->IsPrimaryStep()) {
        job->DaemonStep()->SetStatus(crane::grpc::Completing);
        context->rn_step_raw_ptrs.emplace(job->DaemonStep());
        // Primary step finish, free daemon step, will send status change.
        for (const auto& node : job->DaemonStep()->CranedIds()) {
          context->craned_jobs_to_free[node].emplace_back(job->TaskId());
        }

        std::unordered_set<step_id_t> pd_steps;
        // Cancel all other step with CANCELED status
        for (const auto& comm_step : job->Steps() | std::views::values) {
          // All pending steps are crun steps, just set status to cancelled
          if (comm_step->Status() == crane::grpc::TaskStatus::Pending) {
            comm_step->SetStatus(crane::grpc::Cancelled);
            comm_step->SetEndTime(absl::Now());
            auto& meta = comm_step->ia_meta.value();
            if (!meta.has_been_cancelled_on_front_end) {
              meta.has_been_cancelled_on_front_end = true;
              meta.cb_step_cancel({.job_id = comm_step->job_id,
                                   .step_id = comm_step->StepId()});
            }
            // Send Completion Ack to frontend now.
            meta.cb_step_completed({.job_id = comm_step->job_id,
                                    .step_id = comm_step->StepId(),
                                    .send_completion_ack = true,
                                    .cfored_name = meta.cfored_name});
            pd_steps.insert(comm_step->StepId());
            continue;
          }
          for (const auto& node : comm_step->ExecutionNodes()) {
            context->craned_cancel_steps[node][comm_step->job_id].emplace(
                comm_step->StepId());
          }
        }
        for (const auto& pd_step_id : pd_steps) {
          auto pd_step = job->EraseStep(pd_step_id);
          context->step_raw_ptrs.insert(pd_step.get());
          context->step_ptrs.insert(std::move(pd_step));
        }
      } else {
        for (const auto& node : this->ExecutionNodes()) {
          context->craned_step_free_map[node][job_id].insert(step_id);
        }
      }
      if (this->PrevErrorStatus().has_value()) {
        this->SetStatus(this->PrevErrorStatus().value());
        this->SetExitCode(this->PrevErrorExitCode());
      } else {
        this->SetStatus(crane::grpc::TaskStatus::Completed);
        this->SetExitCode(exit_code);
      }

      CRANE_INFO("[Step #{}.{}] FINISHED with status {}.", job_id, step_id,
                 this->Status());
    }

    context->step_raw_ptrs.insert(this);
    if (this->IsPrimaryStep()) {
      job->SetPrimaryStepStatus(this->Status());
      job->SetPrimaryStepExitCode(exit_code);
      context->rn_job_raw_ptrs.insert(job);
      context->step_ptrs.emplace(job->ReleasePrimaryStep());
    } else {
      context->step_ptrs.insert(job->EraseStep(step_id));
    }
  }
}

void CommonStepInCtld::RecoverFromDb(
    const TaskInCtld& job, const crane::grpc::StepInEmbeddedDb& step_in_db) {
  StepInCtld::RecoverFromDb(job, step_in_db);

  cmd_line = StepToCtld().cmd_line();
  cwd = StepToCtld().cwd();
  extra_attr = StepToCtld().extra_attr();

  allocated_craneds_regex = job.allocated_craneds_regex;
  pending_reason = "";
}

void CommonStepInCtld::SetFieldsOfStepInfo(
    crane::grpc::StepInfo* step_info) const noexcept {
  StepInCtld::SetFieldsOfStepInfo(step_info);
  step_info->set_cwd(cwd);
  step_info->set_cmd_line(cmd_line);
  step_info->set_extra_attr(extra_attr);
  step_info->set_craned_list(allocated_craneds_regex);
  *step_info->mutable_allocated_res_view() =
      static_cast<crane::grpc::ResourceView>(m_allocated_res_.View());
}

bool TaskInCtld::IsX11() const {
  if (!IsInteractive()) return false;
  auto const& ia_meta = this->task_to_ctld.interactive_meta();
  return ia_meta.x11();
}

bool TaskInCtld::IsX11WithPty() const {
  if (!IsX11()) return false;
  auto const& ia_meta = this->task_to_ctld.interactive_meta();
  return ia_meta.pty();
}

bool TaskInCtld::ShouldLaunchOnAllNodes() const {
  // For cbatch tasks whose --node > 1,
  // only execute the command at the first allocated node.
  if (type != crane::grpc::Interactive) return false;

  const auto& ia_meta = TaskToCtld().interactive_meta();
  // For calloc tasks we still need to execute a dummy empty task to
  // set up a timer.
  if (ia_meta.interactive_type() == crane::grpc::Calloc) return false;

  // Crun task with pty only launch on first node
  if (ia_meta.pty()) return false;

  // For crun tasks with regular I/O, execute tasks on all allocated nodes.
  return true;
}

void TaskInCtld::SetTaskId(task_id_t id) {
  task_id = id;
  runtime_attr.set_task_id(id);
}

void TaskInCtld::SetTaskDbId(task_db_id_t id) {
  task_db_id = id;
  runtime_attr.set_task_db_id(id);
}

void TaskInCtld::SetUsername(std::string const& val) {
  username = val;
  runtime_attr.set_username(val);
}

void TaskInCtld::SetCranedIds(std::vector<CranedId>&& val) {
  runtime_attr.mutable_craned_ids()->Assign(val.begin(), val.end());
  craned_ids = std::move(val);
}

void TaskInCtld::CranedIdsClear() {
  craned_ids.clear();
  runtime_attr.mutable_craned_ids()->Clear();
}

void TaskInCtld::CranedIdsAdd(CranedId const& i) {
  craned_ids.emplace_back(i);
  *runtime_attr.mutable_craned_ids()->Add() = i;
}

void TaskInCtld::SetPrimaryStepStatus(crane::grpc::TaskStatus val) {
  primary_status = val;
  runtime_attr.set_primary_step_status(val);
}

void TaskInCtld::SetStatus(crane::grpc::TaskStatus val) {
  status = val;
  runtime_attr.set_status(val);
}

void TaskInCtld::SetPrimaryStepExitCode(uint32_t val) {
  primary_exit_code = val;
  runtime_attr.set_primary_step_exit_code(val);
}

void TaskInCtld::SetExitCode(uint32_t val) {
  exit_code = val;
  runtime_attr.set_exit_code(val);
}

void TaskInCtld::SetSubmitTime(absl::Time const& val) {
  submit_time = val;
  runtime_attr.mutable_submit_time()->set_seconds(ToUnixSeconds(submit_time));
}

void TaskInCtld::SetSubmitTimeByUnixSecond(uint64_t val) {
  submit_time = absl::FromUnixSeconds(val);
  runtime_attr.mutable_submit_time()->set_seconds(val);
}

void TaskInCtld::SetStartTime(absl::Time const& val) {
  start_time = val;
  runtime_attr.mutable_start_time()->set_seconds(ToUnixSeconds(start_time));
}

void TaskInCtld::SetStartTimeByUnixSecond(uint64_t val) {
  start_time = absl::FromUnixSeconds(val);
  runtime_attr.mutable_start_time()->set_seconds(val);
}

void TaskInCtld::SetEndTime(absl::Time const& val) {
  SetEndTimeByUnixSecond(ToUnixSeconds(val));
}

void TaskInCtld::SetEndTimeByUnixSecond(uint64_t val) {
  val = std::min<uint64_t>(val, kTaskMaxTimeStampSec);
  end_time = absl::FromUnixSeconds(val);
  runtime_attr.mutable_end_time()->set_seconds(val);
}

void TaskInCtld::SetHeld(bool val) {
  held = val;
  runtime_attr.set_held(val);
}

void TaskInCtld::SetCachedPriority(const double val) {
  cached_priority = val;
  runtime_attr.set_cached_priority(val);
}

void TaskInCtld::SetAllocatedRes(ResourceV2&& val) {
  *runtime_attr.mutable_allocated_res() =
      static_cast<crane::grpc::ResourceV2>(val);
  allocated_res = std::move(val);
}

void TaskInCtld::SetFieldsByTaskToCtld(crane::grpc::TaskToCtld const& val) {
  task_to_ctld = val;

  partition_id = (val.partition_name().empty()) ? g_config.DefaultPartition
                                                : val.partition_name();
  requested_node_res_view = static_cast<ResourceView>(val.req_resources());

  time_limit = absl::Seconds(val.time_limit().seconds());

  type = val.type();

  if (IsContainer()) {
    meta.emplace<ContainerMetaInTask>(
        static_cast<ContainerMetaInTask>(val.container_meta()));
  }

  node_num = val.node_num();
  ntasks_per_node = val.ntasks_per_node();
  cpus_per_task = cpu_t(val.cpus_per_task());

  uid = val.uid();
  password_entry = std::make_unique<PasswordEntry>(uid);

  // Note: gid is egid, which may be different from the
  // primary group of the user in `password_entry`.
  gid = val.gid();

  account = val.account();
  name = val.name();
  qos = val.qos();

  cmd_line = val.cmd_line();
  cwd = val.cwd();

  for (const auto& [k, v] : val.env()) env[k] = v;

  get_user_env = val.get_user_env();

  extra_attr = val.extra_attr();

  reservation = val.reservation();
  if (val.has_begin_time()) {
    begin_time = absl::FromUnixSeconds(val.begin_time().seconds());
  }

  exclusive = val.exclusive();

  SetHeld(val.hold());
}

void TaskInCtld::SetFieldsByRuntimeAttr(
    crane::grpc::RuntimeAttrOfTask const& val) {
  runtime_attr = val;

  task_id = runtime_attr.task_id();
  task_db_id = runtime_attr.task_db_id();
  username = runtime_attr.username();

  requeue_count = runtime_attr.requeue_count();

  primary_status = runtime_attr.primary_step_status();
  status = runtime_attr.status();
  primary_exit_code = runtime_attr.primary_step_exit_code();
  exit_code = runtime_attr.exit_code();

  held = runtime_attr.held();
  cached_priority = runtime_attr.cached_priority();

  if (status != crane::grpc::TaskStatus::Pending) {
    craned_ids.assign(runtime_attr.craned_ids().begin(),
                      runtime_attr.craned_ids().end());
    allocated_craneds_regex = util::HostNameListToStr(craned_ids);

    if (type == crane::grpc::Batch || type == crane::grpc::Container) {
      executing_craned_ids.emplace_back(craned_ids.front());
    } else if (type == crane::grpc::Interactive) {
      if (task_to_ctld.interactive_meta().interactive_type() ==
          crane::grpc::Calloc)
        // For calloc tasks we still need to execute a dummy empty task to
        // set up a timer.
        executing_craned_ids.emplace_back(CranedIds().front());
      else
        // For crun tasks we need to execute tasks on all allocated nodes.
        for (auto const& craned_id : craned_ids)
          executing_craned_ids.emplace_back(craned_id);
    } else {
      std::unreachable();
    }

    allocated_res = static_cast<ResourceV2>(runtime_attr.allocated_res());
    allocated_res_view.SetToZero();
    allocated_res_view += allocated_res;
  }

  nodes_alloc = craned_ids.size();
  start_time = absl::FromUnixSeconds(runtime_attr.start_time().seconds());
  end_time = absl::FromUnixSeconds(runtime_attr.end_time().seconds());
  submit_time = absl::FromUnixSeconds(runtime_attr.submit_time().seconds());
}

void TaskInCtld::SetFieldsOfTaskInfo(crane::grpc::TaskInfo* task_info) {
  task_info->set_type(type);
  task_info->set_task_id(task_id);
  task_info->set_name(name);

  task_info->set_account(account);
  task_info->set_partition(partition_id);
  task_info->set_qos(qos);

  task_info->mutable_time_limit()->set_seconds(ToInt64Seconds(time_limit));
  task_info->mutable_submit_time()->CopyFrom(runtime_attr.submit_time());
  task_info->mutable_start_time()->CopyFrom(runtime_attr.start_time());
  task_info->mutable_end_time()->CopyFrom(runtime_attr.end_time());

  task_info->set_uid(uid);
  task_info->set_gid(gid);
  task_info->set_username(username);
  task_info->set_node_num(node_num);
  task_info->set_cmd_line(cmd_line);
  task_info->set_cwd(cwd);
  task_info->mutable_req_nodes()->Assign(included_nodes.begin(),
                                         included_nodes.end());
  task_info->mutable_exclude_nodes()->Assign(excluded_nodes.begin(),
                                             excluded_nodes.end());

  task_info->set_extra_attr(extra_attr);
  task_info->set_reservation(reservation);

  // Only pass container meta if it's a container task
  // This is because ccon command requires more info than cqueue/cacct.
  if (IsContainer()) {
    task_info->mutable_container_meta()->CopyFrom(
        static_cast<crane::grpc::ContainerTaskAdditionalMeta>(
            std::get<ContainerMetaInTask>(meta)));
    // Remove sensitive info
    task_info->mutable_container_meta()->mutable_image()->clear_username();
    task_info->mutable_container_meta()->mutable_image()->clear_password();
  }

  // Dynamic fields
  task_info->set_held(held);
  task_info->mutable_execution_node()->Assign(executing_craned_ids.begin(),
                                              executing_craned_ids.end());

  *task_info->mutable_req_res_view() =
      static_cast<crane::grpc::ResourceView>(requested_node_res_view);

  task_info->set_exit_code(runtime_attr.exit_code());
  task_info->set_priority(cached_priority);  // FIXME: A BUG?

  task_info->set_status(status);
  if (Status() == crane::grpc::Pending) {
    task_info->set_pending_reason(pending_reason);
  } else {
    task_info->set_craned_list(allocated_craneds_regex);
  }
  task_info->set_exclusive(task_to_ctld.exclusive());

  *task_info->mutable_allocated_res_view() =
      static_cast<crane::grpc::ResourceView>(allocated_res_view);
}

}  // namespace Ctld
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

void StepInCtld::SetSubmitTime(absl::Time submit_time) {
  m_submit_time_ = submit_time;
  this->runtime_attr.mutable_submit_time()->set_seconds(
      ToUnixSeconds(submit_time));
}
void StepInCtld::SetStartTime(absl::Time start_time) {
  m_start_time_ = start_time;
  this->runtime_attr.mutable_start_time()->set_seconds(
      ToUnixSeconds(start_time));
}
void StepInCtld::SetEndTime(absl::Time end_time) {
  m_end_time_ = end_time;
  this->runtime_attr.mutable_end_time()->set_seconds(ToUnixSeconds(end_time));
}

void StepInCtld::SetAllocatedRes(const ResourceV2& res) {
  this->m_allocated_res_ = res;
  *this->runtime_attr.mutable_allocated_res() =
      static_cast<crane::grpc::ResourceV2>(res);
}

void StepInCtld::SetCranedIds(const std::list<CranedId>& craned_list) {
  this->craned_ids = craned_list;
  this->runtime_attr.mutable_craned_ids()->Assign(craned_list.begin(),
                                                  craned_list.end());
}

void StepInCtld::SetStatus(crane::grpc::TaskStatus new_status) {
  this->m_status_ = new_status;
  this->runtime_attr.set_status(new_status);
}

void StepInCtld::SetExitCode(uint32_t exit_code) {
  this->m_exit_code_ = exit_code;
  this->runtime_attr.set_exit_code(exit_code);
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
  // Set time_limit
  step_to_d.mutable_time_limit()->CopyFrom(
      google::protobuf::util::TimeUtil::MillisecondsToDuration(
          ToInt64Milliseconds(this->time_limit)));
  auto* mutable_res_in_node = step_to_d.mutable_res();
  *mutable_res_in_node =
      static_cast<crane::grpc::ResourceInNode>(m_allocated_res_.at(craned_id));

  // Set type
  step_to_d.set_type(this->type);

  step_to_d.set_job_id(this->job_id);
  step_to_d.set_step_id(this->step_id);
  step_to_d.set_name(this->name);

  step_to_d.set_node_num(this->node_num);

  step_to_d.set_uid(uid);
  step_to_d.mutable_gid()->Assign(this->gids.begin(), this->gids.end());
  step_to_d.mutable_env()->insert(this->env.begin(), this->env.end());

  step_to_d.set_container(this->container);
  step_to_d.set_get_user_env(this->get_user_env);

  for (const auto& hostname : this->craned_ids)
    step_to_d.mutable_nodelist()->Add()->assign(hostname);

  step_to_d.mutable_start_time()->set_seconds(
      ToUnixSeconds(this->m_start_time_));
  step_to_d.mutable_time_limit()->set_seconds(ToInt64Seconds(this->time_limit));

  return step_to_d;
}

crane::grpc::StepToD CommonStepInCtld::GetStepToD(
    const CranedId& craned_id) const {
  crane::grpc::StepToD step_to_d;
  // Set time_limit
  step_to_d.mutable_time_limit()->CopyFrom(
      google::protobuf::util::TimeUtil::MillisecondsToDuration(
          ToInt64Milliseconds(this->time_limit)));
  auto* mutable_res_in_node = step_to_d.mutable_res();
  *mutable_res_in_node =
      static_cast<crane::grpc::ResourceInNode>(m_allocated_res_.at(craned_id));

  // Set type
  step_to_d.set_type(this->type);

  step_to_d.set_job_id(this->job_id);
  step_to_d.set_step_id(1);
  step_to_d.set_name(this->name);

  step_to_d.set_node_num(this->node_num);
  step_to_d.set_ntasks_per_node(this->ntasks_per_node);
  step_to_d.set_cpus_per_task(static_cast<double>(this->cpus_per_task));

  step_to_d.set_uid(uid);
  step_to_d.mutable_gid()->Assign(this->gids.begin(), this->gids.end());
  step_to_d.mutable_env()->insert(this->env.begin(), this->env.end());

  step_to_d.set_cwd(this->cwd);
  step_to_d.set_container(this->container);
  step_to_d.set_get_user_env(this->get_user_env);

  for (const auto& hostname : this->craned_ids)
    step_to_d.mutable_nodelist()->Add()->assign(hostname);

  step_to_d.mutable_start_time()->set_seconds(
      ToUnixSeconds(this->m_start_time_));
  step_to_d.mutable_time_limit()->set_seconds(ToInt64Seconds(this->time_limit));

  if (this->type == crane::grpc::Batch) {
    auto* mutable_meta = step_to_d.mutable_batch_meta();
    mutable_meta->CopyFrom(this->task_to_ctld.batch_meta());
  } else {
    const auto& proto_ia_meta = this->task_to_ctld.interactive_meta();
    auto* mutable_meta = step_to_d.mutable_interactive_meta();
    mutable_meta->CopyFrom(proto_ia_meta);
  }
  return step_to_d;
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
  if (type == crane::grpc::Batch) return false;

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

void TaskInCtld::SetCranedIds(std::list<CranedId>&& val) {
  runtime_attr.mutable_craned_ids()->Assign(val.begin(), val.end());
  craned_ids = val;
}

void TaskInCtld::CranedIdsClear() {
  craned_ids.clear();
  runtime_attr.mutable_craned_ids()->Clear();
}

void TaskInCtld::CranedIdsAdd(CranedId const& i) {
  craned_ids.emplace_back(i);
  *runtime_attr.mutable_craned_ids()->Add() = i;
}

void TaskInCtld::SetStatus(crane::grpc::TaskStatus val) {
  status = val;
  runtime_attr.set_status(val);
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
  if (val > kTaskMaxTimeStampSec) val = kTaskMaxTimeStampSec;
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

  if (type == crane::grpc::Batch) {
    meta.emplace<BatchMetaInTask>(BatchMetaInTask{
        .sh_script = val.batch_meta().sh_script(),
        .interpreter = val.batch_meta().interpreter(),
        .output_file_pattern = val.batch_meta().output_file_pattern(),
        .error_file_pattern = val.batch_meta().error_file_pattern(),
    });
  } else {
    auto& ia_meta = std::get<InteractiveMetaInTask>(meta);
    ia_meta.interactive_type = val.interactive_meta().interactive_type();
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
  container = val.container();

  for (const auto& [k, v] : val.env()) env[k] = v;

  get_user_env = val.get_user_env();

  extra_attr = val.extra_attr();

  reservation = val.reservation();

  SetHeld(val.hold());
}

void TaskInCtld::SetFieldsByRuntimeAttr(
    crane::grpc::RuntimeAttrOfTask const& val) {
  runtime_attr = val;

  task_id = runtime_attr.task_id();
  task_db_id = runtime_attr.task_db_id();
  username = runtime_attr.username();

  exit_code = runtime_attr.exit_code();

  status = runtime_attr.status();
  held = runtime_attr.held();
  cached_priority = runtime_attr.cached_priority();

  if (status != crane::grpc::TaskStatus::Pending) {
    craned_ids.assign(runtime_attr.craned_ids().begin(),
                      runtime_attr.craned_ids().end());
    allocated_craneds_regex = util::HostNameListToStr(craned_ids);

    if (type == crane::grpc::Batch)
      executing_craned_ids.emplace_back(craned_ids.front());
    else {
      const auto& int_meta = std::get<InteractiveMetaInTask>(meta);
      if (int_meta.interactive_type == crane::grpc::Calloc)
        // For calloc tasks we still need to execute a dummy empty task to
        // set up a timer.
        executing_craned_ids.emplace_back(CranedIds().front());
      else
        // For crun tasks we need to execute tasks on all allocated nodes.
        for (auto const& craned_id : craned_ids)
          executing_craned_ids.emplace_back(craned_id);
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

  task_info->set_container(container);
  task_info->set_extra_attr(extra_attr);

  task_info->set_held(held);
  task_info->mutable_execution_node()->Assign(executing_craned_ids.begin(),
                                              executing_craned_ids.end());

  *task_info->mutable_req_res_view() =
      static_cast<crane::grpc::ResourceView>(requested_node_res_view);

  task_info->set_exit_code(runtime_attr.exit_code());
  task_info->set_priority(cached_priority);

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

void TaskInCtld::InitDaemonStepInCtld() {
  m_daemon_step_ = std::make_unique<DaemonStepInCtld>();
  /*Fields in StepInCtld*/
  m_daemon_step_->type = type;
  m_daemon_step_->step_type = crane::grpc::StepType::DAEMON;
  m_daemon_step_->job_id = task_id;
  m_daemon_step_->step_id = kDaemonStepId;
  m_daemon_step_->uid = uid;
  m_daemon_step_->gids = {task_to_ctld.gid()};
  m_daemon_step_->name = name;
  m_daemon_step_->username = username;
  m_daemon_step_->get_user_env = get_user_env;
  m_daemon_step_->env = env;
  m_daemon_step_->container = container;

  m_daemon_step_->time_limit = time_limit;
  m_daemon_step_->requested_node_res_view = requested_node_res_view;
  m_daemon_step_->node_num = node_num;
  m_daemon_step_->included_nodes = included_nodes;
  m_daemon_step_->excluded_nodes = excluded_nodes;
  m_daemon_step_->configuring_nodes =
      CranedIds() | std::ranges::to<std::unordered_set<CranedId>>();
  m_daemon_step_->running_nodes =
      CranedIds() | std::ranges::to<std::unordered_set<CranedId>>();
  m_daemon_step_->SetSubmitTime(submit_time);
  m_daemon_step_->SetCranedIds(craned_ids);
  m_daemon_step_->SetStatus(crane::grpc::TaskStatus::Configuring);

  m_daemon_step_->SetSubmitTime(submit_time);
  m_daemon_step_->SetStartTime(start_time);
  m_daemon_step_->SetEndTime(end_time);
  m_daemon_step_->SetAllocatedRes(allocated_res);

  /*Fields in DaemonStepInCtld*/
  m_daemon_step_->partition = partition_id;
  m_daemon_step_->account = account;
  m_daemon_step_->qos = qos;
}

void TaskInCtld::InitPrimaryStepInCtld() {
  m_primary_step_ = std::make_unique<CommonStepInCtld>();
  /*Fields in StepInCtld*/
  m_primary_step_->step_type = crane::grpc::StepType::PRIMARY;
  m_primary_step_->type = type;
  m_primary_step_->job_id = task_id;
  m_primary_step_->step_id = kDaemonStepId + 1;
  m_primary_step_->uid = uid;
  m_primary_step_->gids = {task_to_ctld.gid()};

  m_primary_step_->name = name;
  m_primary_step_->username = username;
  m_primary_step_->get_user_env = get_user_env;
  m_primary_step_->env = env;
  m_primary_step_->container = container;

  m_primary_step_->time_limit = time_limit;
  m_primary_step_->requested_node_res_view = requested_node_res_view;
  m_primary_step_->node_num = node_num;
  m_primary_step_->included_nodes = included_nodes;
  m_primary_step_->excluded_nodes = excluded_nodes;

  m_primary_step_->SetSubmitTime(submit_time);
  m_primary_step_->SetCranedIds(craned_ids);
  m_primary_step_->SetStatus(crane::grpc::TaskStatus::Configuring);
  m_primary_step_->configuring_nodes =
      CranedIds() | std::ranges::to<std::unordered_set<CranedId>>();
  m_primary_step_->running_nodes =
      CranedIds() | std::ranges::to<std::unordered_set<CranedId>>();
  m_primary_step_->SetSubmitTime(submit_time);
  m_primary_step_->SetStartTime(start_time);
  m_primary_step_->SetEndTime(end_time);
  m_primary_step_->SetAllocatedRes(allocated_res);

  /*Fields in CommonStepInCtld*/
  m_primary_step_->ntasks_per_node = ntasks_per_node;
  m_primary_step_->cpus_per_task = cpus_per_task;
  m_primary_step_->requeue_if_failed = requeue_if_failed;
  m_primary_step_->cmd_line = cmd_line;

  m_primary_step_->cwd = cwd;
  m_primary_step_->container = container;
  m_primary_step_->extra_attr = extra_attr;

  m_primary_step_->executing_craned_ids = executing_craned_ids;
  m_primary_step_->allocated_craneds_regex = allocated_craneds_regex;
  m_primary_step_->pending_reason = pending_reason;
}

}  // namespace Ctld
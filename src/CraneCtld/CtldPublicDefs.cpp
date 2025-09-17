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

ContainerMetaInTask::ContainerMetaInTask(
    const crane::grpc::ContainerTaskAdditionalMeta& rhs)
    : image_info{.image = rhs.image().image(),
                 .username = rhs.image().username(),
                 .password = rhs.image().password(),
                 .server_address = rhs.image().server_address()},
      name(rhs.name()),
      labels(rhs.labels().begin(), rhs.labels().end()),
      annotations(rhs.annotations().begin(), rhs.annotations().end()),
      command(rhs.command()),
      args(rhs.args().begin(), rhs.args().end()),
      workdir(rhs.workdir()),
      env(rhs.env().begin(), rhs.env().end()),
      detached(rhs.detached()),
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

  if (IsBatch()) {
    meta.emplace<BatchMetaInTask>(BatchMetaInTask{
        .sh_script = val.batch_meta().sh_script(),
        .interpreter = val.batch_meta().interpreter(),
        .output_file_pattern = val.batch_meta().output_file_pattern(),
        .error_file_pattern = val.batch_meta().error_file_pattern(),
    });
  } else if (IsContainer()) {
    meta.emplace<ContainerMetaInTask>(
        static_cast<ContainerMetaInTask>(val.container_meta()));
  } else if (IsInteractive()) {
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

  for (const auto& [k, v] : val.env()) env[k] = v;

  get_user_env = val.get_user_env();

  extra_attr = val.extra_attr();

  reservation = val.reservation();
  if (val.has_begin_time()) {
    begin_time = absl::FromUnixSeconds(val.begin_time().seconds());
  }
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

    if (type == crane::grpc::Batch || type == crane::grpc::Container) {
      executing_craned_ids.emplace_back(craned_ids.front());
    } else if (type == crane::grpc::Interactive) {
      const auto& int_meta = std::get<InteractiveMetaInTask>(meta);
      if (int_meta.interactive_type == crane::grpc::Calloc)
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
    auto* meta_info = task_info->mutable_container_meta();
    meta_info->mutable_image()->set_image(
        std::get<ContainerMetaInTask>(meta).image_info.image);
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

crane::grpc::TaskToD TaskInCtld::GetTaskToD(const CranedId& craned_id) const {
  crane::grpc::TaskToD task_to_d;
  // Set time_limit
  task_to_d.mutable_time_limit()->CopyFrom(
      google::protobuf::util::TimeUtil::MillisecondsToDuration(
          ToInt64Milliseconds(this->time_limit)));

  // TODO: remove this field
  //  Set resources
  auto* mutable_res_in_node = task_to_d.mutable_resources();
  *mutable_res_in_node = static_cast<crane::grpc::ResourceInNode>(
      this->AllocatedRes().at(craned_id));

  // Set type
  task_to_d.set_type(this->type);

  task_to_d.set_task_id(this->TaskId());
  task_to_d.set_name(this->name);
  task_to_d.set_account(this->account);
  task_to_d.set_qos(this->qos);
  task_to_d.set_partition(this->partition_id);

  for (auto&& node : this->included_nodes) {
    task_to_d.mutable_nodelist()->Add()->assign(node);
  }

  for (auto&& node : this->excluded_nodes) {
    task_to_d.mutable_excludes()->Add()->assign(node);
  }

  task_to_d.set_node_num(this->node_num);
  task_to_d.set_ntasks_per_node(this->ntasks_per_node);
  task_to_d.set_cpus_per_task(static_cast<double>(this->cpus_per_task));

  task_to_d.set_uid(this->uid);
  task_to_d.set_gid(this->gid);
  task_to_d.mutable_env()->insert(this->env.begin(), this->env.end());

  task_to_d.set_cwd(this->cwd);
  task_to_d.set_get_user_env(this->get_user_env);

  for (const auto& hostname : this->CranedIds())
    task_to_d.mutable_allocated_nodes()->Add()->assign(hostname);

  task_to_d.mutable_start_time()->set_seconds(this->StartTimeInUnixSecond());
  task_to_d.mutable_time_limit()->set_seconds(ToInt64Seconds(this->time_limit));

  if (this->type == crane::grpc::Batch) {
    auto* mutable_meta = task_to_d.mutable_batch_meta();
    mutable_meta->CopyFrom(this->task_to_ctld.batch_meta());
  } else if (this->type == crane::grpc::Interactive) {
    const auto& proto_ia_meta = this->task_to_ctld.interactive_meta();
    auto* mutable_meta = task_to_d.mutable_interactive_meta();
    mutable_meta->CopyFrom(proto_ia_meta);
  } else if (this->type == crane::grpc::Container) {
    auto* mutable_meta = task_to_d.mutable_container_meta();
    mutable_meta->CopyFrom(this->task_to_ctld.container_meta());
  }

  return task_to_d;
}

crane::grpc::JobToD TaskInCtld::GetJobToD(const CranedId& craned_id) const {
  crane::grpc::JobToD spec;
  spec.set_job_id(task_id);
  spec.set_uid(uid);
  *spec.mutable_res() =
      crane::grpc::ResourceInNode(allocated_res.at(craned_id));
  return spec;
}

}  // namespace Ctld
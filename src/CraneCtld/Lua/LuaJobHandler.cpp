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

#include "LuaJobHandler.h"

#include "CranedMetaContainer.h"
#include "TaskScheduler.h"

namespace Ctld {

#ifdef HAVE_LUA

const std::vector<std::string> LuaJobHandler::kReqFxns = {"crane_job_submit",
                                                          "crane_job_modify"};
#endif

CraneRichError LuaJobHandler::JobSubmit(const std::string& lua_script,
                                        TaskInCtld* task) {
  CraneRichError result = FormatRichErr(CraneErrCode::SUCCESS, "");
#ifdef HAVE_LUA
  auto lua_env = std::make_unique<crane::LuaEnvironment>();
  if (!lua_env->Init(lua_script))
    return FormatRichErr(CraneErrCode::ERR_LUA_FAILED,
                         "Failed to init lua environment");

  RegisterGlobalFunctions_(*lua_env);
  RegisterTypes_(*lua_env);
  // crane.jobs
  sol::table jobs_table = lua_env->GetLuaState().create_table();
  jobs_table[sol::metatable_key] = lua_env->GetLuaState().create_table_with(
      "__pairs", [](sol::this_state ts, sol::table self) {
        sol::state_view lua(ts);
        sol::object next = lua["next"];

        crane::grpc::QueryTasksInfoRequest req;
        std::unordered_map<job_id_t, crane::grpc::TaskInfo> job_info_map;
        g_task_scheduler->QueryTasksInRam(&req, &job_info_map);

        sol::table all_jobs = lua.create_table();
        for (auto& [job_id, job_info] : job_info_map) {
          all_jobs[job_id] = job_info;
        }

        return std::make_tuple(next, all_jobs, sol::nil);
      });
  lua_env->GetCraneTable()["jobs"] = jobs_table;

  RegisterGlobalVariables_(*lua_env);

  /*
   *  All lua script functions should have been verified during
   *   initialization:
   */
  if (!lua_env->LoadLuaScript(kReqFxns))
    return FormatRichErr(CraneErrCode::ERR_LUA_FAILED,
                         "Failed to load lua script");

  std::list<crane::grpc::PartitionInfo> part_list;
  PushPartitionList_(task->name, task->account, part_list);

  auto& lua_state = lua_env->GetLuaState();
  sol::function submit = lua_state["crane_job_submit"];
  sol::protected_function_result lua_result =
      submit(task, part_list, task->uid);

  if (!lua_result.valid()) {
    sol::error err = lua_result;
    CRANE_ERROR("{}", err.what());
    return FormatRichErr(CraneErrCode::ERR_LUA_FAILED, err.what());
  }

  int rc = lua_result.get<int>();

  if (rc != 0)
    return FormatRichErr(static_cast<CraneErrCode>(rc), lua_env->GetUserMsg());
#endif

  return result;
}

CraneRichError LuaJobHandler::JobModify(const std::string& lua_script,
                                        TaskInCtld* task) {
  CraneRichError result = FormatRichErr(CraneErrCode::SUCCESS, "");
#ifdef HAVE_LUA
  auto lua_env = std::make_unique<crane::LuaEnvironment>();
  if (!lua_env->Init(lua_script))
    return FormatRichErr(CraneErrCode::ERR_LUA_FAILED,
                         "Failed to init lua environment");

  RegisterGlobalFunctions_(*lua_env);
  RegisterTypes_(*lua_env);
  RegisterGlobalVariables_(*lua_env);

  /*
   *  All lua script functions should have been verified during
   *   initialization:
   */
  if (!lua_env->LoadLuaScript(kReqFxns))
    return FormatRichErr(CraneErrCode::ERR_LUA_FAILED,
                         "Failed to load lua script");

  crane::grpc::TaskInfo task_info;
  task->SetFieldsOfTaskInfo(&task_info);
  std::list<crane::grpc::PartitionInfo> part_list;
  PushPartitionList_(task->name, task->account, part_list);

  auto& lua_state = lua_env->GetLuaState();
  sol::function modify = lua_state["crane_job_modify"];
  sol::protected_function_result lua_result =
      modify(task, task_info, part_list, task->uid);

  if (!lua_result.valid()) {
    sol::error err = lua_result;
    CRANE_ERROR("{}", err.what());
    return FormatRichErr(CraneErrCode::ERR_LUA_FAILED, err.what());
  }

  int rc = lua_result.get<int>();

  if (rc != 0)
    return FormatRichErr(static_cast<CraneErrCode>(rc), lua_env->GetUserMsg());
#endif

  return result;
}

#ifdef HAVE_LUA

void LuaJobHandler::RegisterGlobalFunctions_(
    const crane::LuaEnvironment& lua_env) {
  lua_env.GetCraneTable().set_function(
      "get_qos_priority",
      [](const std::string& qos_name) -> std::optional<uint32_t> {
        auto qos = g_account_manager->GetExistedQosInfo(qos_name);
        if (!qos) {
          CRANE_ERROR("Invalid QOS name: {}", qos_name);
          return std::nullopt;
        }
        return qos->priority;
      });

  lua_env.GetCraneTable().set_function(
      "get_job_env_field",
      [](TaskInCtld& job, const std::string& env_name) -> std::string {
        return job.env[env_name];
      });

  lua_env.GetCraneTable().set_function(
      "set_job_env_field",
      [](const std::string& name, const std::string& value, TaskInCtld* job) {
        job->env.emplace(name, value);
      });
}

void LuaJobHandler::RegisterTypes_(const crane::LuaEnvironment& lua_env) {
  // clang-format off

  lua_env.GetLuaState().new_usertype<ResourceView>("ResourceView",
    "cpu_count", &ResourceView::CpuCount,
    "memory_bytes", &ResourceView::MemoryBytes,
    "device_map", sol::property(
      [&](const ResourceView& rv) {
        sol::table tbl = lua_env.GetLuaState().create_table();
        for (const auto& [dev_name, pair] : rv.GetDeviceMap()) {
          sol::table entry = lua_env.GetLuaState().create_table();
          entry["untyped_count"] = pair.first;

          sol::table typed = lua_env.GetLuaState().create_table();
          for (const auto& [typed_name, typed_count] : pair.second) {
            typed[typed_name] = typed_count;
          }
          entry["typed"] = typed;
          tbl[dev_name] = entry;
        }
        return tbl;
      })
  );

  // job_desc
  lua_env.GetLuaState().new_usertype<TaskInCtld>("TaskInCtld",
    "time_limit", &TaskInCtld::time_limit,
    "partition_id", &TaskInCtld::partition_id,
    "requested_node_res_view", &TaskInCtld::requested_node_res_view,
    "type", &TaskInCtld::type, "uid", &TaskInCtld::uid,
    "gid", &TaskInCtld::gid, "account", &TaskInCtld::account,
    "name", &TaskInCtld::name, "qos", &TaskInCtld::qos,
    "node_num", &TaskInCtld::node_num,
    "ntasks_per_node", &TaskInCtld::ntasks_per_node,
    "cpus_per_task", &TaskInCtld::cpus_per_task,
    "included_nodes", sol::property(
          [](const TaskInCtld& t) {
            return std::vector<std::string>(t.included_nodes.begin(),
                                            t.included_nodes.end());
          },
          [](TaskInCtld& t, const std::vector<std::string>& nodes) {
            t.included_nodes.clear();
            t.included_nodes.insert(nodes.begin(), nodes.end());
          }),
      "excluded_nodes", sol::property(
          [](const TaskInCtld& t) {
            return std::vector<std::string>(t.excluded_nodes.begin(),
                                            t.excluded_nodes.end());
          },
          [](TaskInCtld& t, const std::vector<std::string>& nodes) {
            t.excluded_nodes.clear();
            t.excluded_nodes.insert(nodes.begin(), nodes.end());
          }),
      "requeue_if_failed", &TaskInCtld::requeue_if_failed,
      "get_user_env", &TaskInCtld::get_user_env,
      "cmd_line", &TaskInCtld::cmd_line,
      "env", sol::property(
          [&](const TaskInCtld& t) {
            sol::table tbl = lua_env.GetLuaState().create_table();
            for (const auto& [name, value] : t.env) tbl[name] = value;
            return tbl;
          },
          [](TaskInCtld& t, const sol::table& tbl) {
            t.env.clear();
            for (const auto& [name, value] : tbl) {
              t.env[name.as<std::string>()] = value.as<std::string>();
            }
          }),
      "cwd", &TaskInCtld::cwd, "extra_attr", &TaskInCtld::extra_attr,
      // "meta", &TaskInCtld::meta,
      "reservation", &TaskInCtld::reservation,
      "begin_time", &TaskInCtld::begin_time, "exclusive", &TaskInCtld::exclusive,
      "licenses_count", sol::property(
          [&](const TaskInCtld& t) {
            sol::table tbl = lua_env.GetLuaState().create_table();
            for (const auto& [name, value] : t.licenses_count) {
              tbl[name] = value;
            }
            return tbl;
          },
          [](TaskInCtld& t, const sol::table& tbl) {
            t.licenses_count.clear();
            for (const auto& [name, value] : tbl) {
              t.licenses_count.emplace(name.as<std::string>(),
                                       value.as<uint32_t>());
            }
        }));

  // crane.jobs
  using TaskInfo = crane::grpc::TaskInfo;
  lua_env.GetLuaState().new_usertype<TaskInfo>("TaskInfo",
    "type", sol::property([](const TaskInfo& t) {
      return static_cast<int>(t.type());
    }),
    "task_id", sol::property([](const TaskInfo& t) {
      return t.task_id();
    }),
    "name", sol::property([](TaskInfo& t) {
      return t.name();
    }),
    "partition", sol::property([](const TaskInfo& t) {
      return t.partition();
    }),
    "uid", sol::property([](const TaskInfo& t) {
      return t.uid();
    }),
    "time_limit", sol::property([](const TaskInfo& t) {
      return t.time_limit();
    }),
    "end_time", &TaskInfo::end_time,
    "submit_time", &TaskInfo::submit_time,
    "account", &TaskInfo::account,
    "node_num", &TaskInfo::node_num,
    "cmd_line", &TaskInfo::cmd_line, "cwd", &TaskInfo::cwd,
    "username", sol::property([](const TaskInfo& t) {
      return t.username();
    }),
    "qos", sol::property([](const TaskInfo& t) {
      return t.qos();
    }),
    // "req_res_view", &TaskInfo::req_res_view,
    // "licenses_count", &TaskInfo::licenses_count,
    // "req_nodes", &TaskInfo::req_nodes,
    // "exclude_nodes", &TaskInfo::exclude_nodes,
    "extra_attr", &TaskInfo::extra_attr,
    "reservation", &TaskInfo::reservation,
    // "container_meta", &TaskInfo::container_meta,
    // "step_info_list", &TaskInfo::step_info_list,
    "held", &TaskInfo::held, "status", &TaskInfo::status,
    "exit_code", &TaskInfo::exit_code, "priority", &TaskInfo::priority,
    "pending_reason", &TaskInfo::pending_reason,
    "craned_list", &TaskInfo::craned_list,
    "elapsed_time", &TaskInfo::elapsed_time,
    // "execution_node", &TaskInfo::execution_node,
    "exclusive", &TaskInfo::exclusive
    // "allocated_res_view", &TaskInfo::allocated_res_view,
    // "env", &TaskInfo::env
  );

  using PartitionInfo = crane::grpc::PartitionInfo;
  lua_env.GetLuaState().new_usertype<PartitionInfo>("PartitionInfo",
    "hostlist", &PartitionInfo::hostlist,
    "state", &PartitionInfo::state,
    "name", &PartitionInfo::name,
    "total_nodes", &PartitionInfo::total_nodes,
    "alive_nodes", &PartitionInfo::alive_nodes,
    // "res_total", &PartitionInfo::res_total,
    // "res_avail", &PartitionInfo::res_avail,
    // "res_alloc", &PartitionInfo::res_alloc,
    "allowed_accounts", sol::property(
      [](const PartitionInfo& partition_info) {
        return std::vector<std::string>(partition_info.allowed_accounts().begin(),
          partition_info.allowed_accounts().end());
      }),
    "denied_accounts", sol::property(
      [](const PartitionInfo& partition_info) {
        return std::vector<std::string>(partition_info.denied_accounts().begin(),
          partition_info.denied_accounts().end());
      }),
    "default_mem_per_cpu", &PartitionInfo::default_mem_per_cpu,
    "max_mem_per_cpu", &PartitionInfo::max_mem_per_cpu
  );

  // clang-format on
}

void LuaJobHandler::RegisterGlobalVariables_(
    const crane::LuaEnvironment& lua_env) {
  // crane.reservations
  sol::table resv_table = lua_env.GetLuaState().create_table();
  resv_table[sol::metatable_key] = lua_env.GetLuaState().create_table_with(
      "__pairs", [](sol::this_state ts, sol::table self) {
        sol::state_view lua(ts);
        sol::object next = lua["next"];

        auto reply = g_meta_container->QueryAllResvInfo();
        sol::table reservations_tbl = lua.create_table();
        for (const auto& resv : reply.reservation_info_list()) {
          reservations_tbl[resv.reservation_name()] = resv;
        }

        return std::make_tuple(next, reservations_tbl, sol::nil);
      });
  lua_env.GetCraneTable()["reservations"] = resv_table;

}

void LuaJobHandler::PushPartitionList_(
    const std::string& user_name, const std::string& account,
    std::list<crane::grpc::PartitionInfo> part_list) {
  auto user = g_account_manager->GetExistedUserInfo(user_name);
  if (!user) {
    CRANE_ERROR("username is null");
    return;
  }
  std::string actual_account = account;
  if (actual_account.empty()) actual_account = user->default_account;

  auto partition_info_reply = g_meta_container->QueryAllPartitionInfo();
  for (const auto& partition : partition_info_reply.partition_info_list()) {
    if (!user->account_to_attrs_map.at(actual_account)
             .allowed_partition_qos_map.contains(partition.name()))
      continue;
    if (partition.allowed_accounts_size() > 0 &&
        !std::ranges::contains(partition.allowed_accounts(), actual_account))
      continue;

    part_list.emplace_back(partition);
  }
}
#endif

}  // namespace Ctld

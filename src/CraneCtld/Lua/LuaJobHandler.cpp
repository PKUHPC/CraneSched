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
  lua_env->GetCraneTable()["jobs"] = lua_env->GetLuaState().create_table();
  lua_env->GetCraneTable()["jobs"][sol::metatable_key] = lua_env->GetLuaState().create_table_with(
    "__index", [&](const sol::this_state& ts, const sol::object& key) -> sol::object {
      CRANE_TRACE("index");
      sol::state_view lua(ts);

      crane::grpc::QueryTasksInfoRequest req;
      std::unordered_map<job_id_t, crane::grpc::TaskInfo> job_info_map;
      g_task_scheduler->QueryTasksInRam(&req, &job_info_map);

      sol::table all_jobs = lua.create_table();
      for (auto& [job_id, job_info] : job_info_map) {
        all_jobs[job_id] = job_info;
      }
      if (!key.valid() || key == sol::nil) {
        return all_jobs;
      }
      sol::object job = all_jobs[key];
        if (job.valid())
          return job;

      return sol::nil;
    }
  );
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
                                        TaskInCtld* task_in_ctld) {
  CraneRichError result = FormatRichErr(CraneErrCode::SUCCESS, "");
  // #ifdef HAVE_LUA
  //   crane::grpc::TaskInfo task_info;
  //   task_in_ctld->SetFieldsOfTaskInfo(&task_info);
  //
  //   auto lua_env = std::make_unique<crane::LuaEnvironment>();
  //   if (!lua_env->Init(lua_script))
  //     return FormatRichErr(CraneErrCode::ERR_LUA_FAILED,
  //                          "Failed to init lua environment");
  //
  //   lua_env->LuaTableRegister(kCraneFunctions);
  //
  //   lua_env->RegisterLuaCraneStructFunctions(kGlobalFunctions);
  //
  //   if (!lua_env->LoadLuaScript(kReqFxns))
  //     return FormatRichErr(CraneErrCode::ERR_LUA_FAILED,
  //                          "Failed to load lua script");
  //
  //   /*
  //    *  All lua script functions should have been verified during
  //    *   initialization:
  //    */
  //   lua_getglobal(lua_env->GetLuaState(), "crane_job_modify");
  //   if (lua_isnil(lua_env->GetLuaState(), -1))
  //     return FormatRichErr(CraneErrCode::ERR_LUA_FAILED,
  //                          "lua environment is nil");
  //
  //   crane::grpc::QueryPartitionInfoReply partition_info_reply;
  //
  //   UpdateResvGloabl_(*lua_env);
  //
  //   PushJobDesc_(task_in_ctld, *lua_env);
  //   PushJobRec_(*lua_env, &task_info);
  //   PushPartitionList_(*lua_env, task_in_ctld->Username(),
  //   task_in_ctld->account,
  //                      &partition_info_reply);
  //   lua_pushnumber(lua_env->GetLuaState(), task_in_ctld->uid);
  //
  //   int rc = CraneErrCode::ERR_LUA_FAILED;
  //   if (lua_pcall(lua_env->GetLuaState(), 4, 1, 0) != 0) {
  //     CRANE_ERROR("{}", lua_tostring(lua_env->GetLuaState(), -1));
  //   } else {
  //     if (lua_isnumber(lua_env->GetLuaState(), -1)) {
  //       rc = lua_tonumber(lua_env->GetLuaState(), -1);
  //     } else {
  //       CRANE_ERROR("{}/lua: non-numeric return code", __func__);
  //     }
  //   }
  //   lua_pop(lua_env->GetLuaState(), 1);
  //
  //   std::string user_msg;
  //   if (!lua_env->GetUserMsg().empty()) {
  //     CRANE_TRACE("lua user_msg: {}", lua_env->GetUserMsg());
  //     user_msg = lua_env->GetUserMsg();
  //     lua_env->ResetUserMsg();
  //   }
  //
  //   if (rc != 0) return FormatRichErr(static_cast<CraneErrCode>(rc),
  //   user_msg);
  // #endif
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
    "type", &TaskInfo::type, "task_id", &TaskInfo::task_id,
    "name", &TaskInfo::name, "partition", &TaskInfo::partition,
    "uid", &TaskInfo::uid, "gid", &TaskInfo::gid,
    "time_limit", &TaskInfo::time_limit, "start_time", &TaskInfo::start_time,
    "end_time", &TaskInfo::end_time, "submit_time", &TaskInfo::submit_time,
    "account", &TaskInfo::account, "node_num", &TaskInfo::node_num,
    "cmd_line", &TaskInfo::cmd_line, "cwd", &TaskInfo::cwd,
    "username", &TaskInfo::username, "qos", &TaskInfo::qos,
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
  lua_env.GetCraneTable()["reservations"] = sol::property([&]() {
    auto reply = g_meta_container->QueryAllResvInfo();

    sol::table reservations_tbl = lua_env.GetLuaState().create_table();
    for (const auto& resv : reply.reservation_info_list()) {
      reservations_tbl[resv.reservation_name()] = resv;
    }
    return reservations_tbl;
  });
}

// int LuaJobHandler::GetJobEnvFieldNameCb_(lua_State* lua_state) {
//   const auto* job_desc =
//       static_cast<const TaskInCtld*>(lua_touserdata(lua_state, 1));
//   if (job_desc == nullptr) {
//     CRANE_ERROR("job_desc is nullptr");
//     lua_pushnil(lua_state);
//     return 1;
//   }
//   const std::string& name = luaL_checkstring(lua_state, 2);
//
//   return GetJobEnvField_(*job_desc, name, lua_state);
// }
//
// int LuaJobHandler::GetJobReqFieldNameCb_(lua_State* lua_state) {
//   const auto* job_desc =
//       static_cast<const TaskInCtld*>(lua_touserdata(lua_state, 1));
//   if (job_desc == nullptr) {
//     CRANE_ERROR("job_desc is nullptr");
//     lua_pushnil(lua_state);
//     return 1;
//   }
//   const std::string& name = luaL_checkstring(lua_state, 2);
//
//   return GetJobReqField_(*job_desc, name, lua_state);
// }
//
// int LuaJobHandler::SetJobEnvFieldCb_(lua_State* lua_state) {
//   const auto* name = luaL_checkstring(lua_state, 2);
//   lua_getmetatable(lua_state, -3);
//   lua_getfield(lua_state, -1, "_job_desc");
//
//   auto* job_desc = static_cast<TaskInCtld*>(lua_touserdata(lua_state, -1));
//   if (job_desc == nullptr) {
//     CRANE_ERROR("job_desc is nullptr");
//     lua_pushnil(lua_state);
//     return 0;
//   }
//
//   if (!job_desc->IsBatch()) {
//     CRANE_INFO("job_desc->environment only accessible for batch jobs.");
//     lua_pushnil(lua_state);
//     return 0;
//   }
//
//   const auto* value_str = luaL_checkstring(lua_state, 3);
//   job_desc->env[name] = value_str;
//
//   return 0;
// }
//
// int LuaJobHandler::SetJobReqFieldCb_(lua_State* lua_state) {
//   const std::string name = luaL_checkstring(lua_state, 2);
//   lua_getmetatable(lua_state, -3);
//   lua_getfield(lua_state, -1, "_job_desc");
//   auto* job_desc = static_cast<TaskInCtld*>(lua_touserdata(lua_state, -1));
//   if (job_desc == nullptr) {
//     CRANE_ERROR("job_desc is nullptr");
//     lua_pushnil(lua_state);
//     return 0;
//   }
//
//   if (name == "time_limit") {
//     uint64_t value = (uint64_t)luaL_checknumber(lua_state, 3);
//     job_desc->time_limit = absl::Seconds(value);
//   } else if (name == "partition") {
//     const std::string& value_str = luaL_checkstring(lua_state, 3);
//     job_desc->partition_id = value_str;
//   } else if (name == "account") {
//     const std::string& value_str = luaL_checkstring(lua_state, 3);
//     job_desc->account = value_str;
//   } else if (name == "name") {
//     const std::string& value_str = luaL_checkstring(lua_state, 3);
//     job_desc->name = value_str;
//   } else if (name == "qos") {
//     const std::string& value_str = luaL_checkstring(lua_state, 3);
//     job_desc->qos = value_str;
//   } else if (name == "node_num") {
//     job_desc->node_num = static_cast<uint32_t>(luaL_checknumber(lua_state,
//     3));
//   } else if (name == "ntasks_per_node") {
//     job_desc->ntasks_per_node =
//         static_cast<int>(luaL_checknumber(lua_state, 3));
//   } else if (name == "cpus_per_task") {
//     job_desc->cpus_per_task =
//         static_cast<cpu_t>(luaL_checknumber(lua_state, 3));
//   } else if (name == "requeue_if_failed") {
//     job_desc->requeue_if_failed = lua_toboolean(lua_state, 3);
//   } else if (name == "extra_attr") {
//     const std::string& value_str = luaL_checkstring(lua_state, 3);
//     job_desc->extra_attr = value_str;
//   } else if (name == "cmd_line") {
//     const std::string& value_str = luaL_checkstring(lua_state, 3);
//     job_desc->cmd_line = value_str;
//   } else if (name == "cwd") {
//     const std::string& value_str = luaL_checkstring(lua_state, 3);
//     job_desc->cwd = value_str;
//   } else if (name == "excludes") {
//     const std::string& value_str = luaL_checkstring(lua_state, 3);
//     for (const auto& node_name : absl::StrSplit(value_str, ',')) {
//       job_desc->excluded_nodes.emplace(node_name);
//     }
//   } else if (name == "nodelist") {
//     const std::string& value_str = luaL_checkstring(lua_state, 3);
//     for (const auto& node_name : absl::StrSplit(value_str, ',')) {
//       job_desc->included_nodes.emplace(node_name);
//     }
//   } else if (name == "reservation") {
//     const std::string& value_str = luaL_checkstring(lua_state, 3);
//     job_desc->reservation = value_str;
//   } else if (name == "begin_time") {
//     uint64_t value = (uint64_t)luaL_checknumber(lua_state, 3);
//     job_desc->begin_time = absl::FromUnixSeconds(value);
//   } else if (name == "exclusive") {
//     job_desc->exclusive = lua_toboolean(lua_state, 3);
//   }
//
//   return 0;
// }
//
// // partition_record
// int LuaJobHandler::GetPartRecFieldNameCb_(lua_State* lua_state) {
//   const auto* partition_meta = static_cast<const
//   crane::grpc::PartitionInfo*>(
//       lua_touserdata(lua_state, 1));
//   const std::string& name = luaL_checkstring(lua_state, 2);
//   if (partition_meta == nullptr) {
//     CRANE_ERROR("partition_meta is nil");
//     lua_pushnil(lua_state);
//     return 1;
//   }
//
//   return GetPartRecField_(*partition_meta, name, lua_state);
// }
//
// int LuaJobHandler::GetJobEnvField_(const TaskInCtld& job_desc,
//                                    const std::string& name,
//                                    lua_State* lua_state) {
//   if (job_desc.env.empty()) {
//     if (job_desc.type == crane::grpc::TaskType::Batch) {
//       CRANE_ERROR("job_desc->environment is nullptr.");
//     } else {
//       CRANE_INFO("job_desc->environment only accessible for batch jobs.");
//     }
//     lua_pushnil(lua_state);
//   } else {
//     auto iter = job_desc.env.find(name);
//     if (iter != job_desc.env.end()) {
//       lua_pushstring(lua_state, iter->second.data());
//     } else {
//       lua_pushnil(lua_state);
//     }
//   }
//
//   return 1;
// }
//
// int LuaJobHandler::GetJobReqField_(const TaskInCtld& job_desc,
//                                    const std::string& name,
//                                    lua_State* lua_state) {
//   using Handler = std::function<void(lua_State*, const TaskInCtld&)>;
//
//   static const std::unordered_map<std::string, Handler> handlers = {
//       {"time_limit",
//        [](lua_State* L, const TaskInCtld& t) {
//          lua_pushnumber(L, absl::ToDoubleSeconds(t.time_limit));
//        }},
//       {"partition",
//        [](lua_State* L, const TaskInCtld& t) {
//          lua_pushstring(L, t.partition_id.data());
//        }},
//       {"requested_node_res_view",
//        [](lua_State* L, const TaskInCtld& t) {
//          PushResourceView_(L, t.requested_node_res_view);
//        }},
//       {"type",
//        [](lua_State* L, const TaskInCtld& t) { lua_pushnumber(L, t.type); }},
//       {"uid",
//        [](lua_State* L, const TaskInCtld& t) { lua_pushnumber(L, t.uid); }},
//       {"account",
//        [](lua_State* L, const TaskInCtld& t) {
//          lua_pushstring(L, t.account.data());
//        }},
//       {"name", [](lua_State* L,
//                   const TaskInCtld& t) { lua_pushstring(L, t.name.data());
//                   }},
//       {"qos", [](lua_State* L,
//                  const TaskInCtld& t) { lua_pushstring(L, t.qos.data()); }},
//       {"node_num", [](lua_State* L,
//                       const TaskInCtld& t) { lua_pushnumber(L, t.node_num);
//                       }},
//       {"ntasks_per_node",
//        [](lua_State* L, const TaskInCtld& t) {
//          lua_pushnumber(L, t.ntasks_per_node);
//        }},
//       {"cpus_per_task",
//        [](lua_State* L, const TaskInCtld& t) {
//          lua_pushnumber(L, static_cast<double>(t.cpus_per_task));
//        }},
//       {"requeue_if_failed",
//        [](lua_State* L, const TaskInCtld& t) {
//          lua_pushboolean(L, t.requeue_if_failed);
//        }},
//       {"get_user_env",
//        [](lua_State* L, const TaskInCtld& t) {
//          lua_pushboolean(L, t.get_user_env);
//        }},
//       {"gid",
//        [](lua_State* L, const TaskInCtld& t) { lua_pushnumber(L, t.gid); }},
//       {"batch_meta",
//        [](lua_State* L, const TaskInCtld& t) {
//          if (t.TaskToCtld().has_batch_meta()) {
//            lua_newtable(L);
//            auto* ptr = &t.TaskToCtld().batch_meta();
//            lua_pushstring(L, ptr->sh_script().data());
//            lua_setfield(L, -2, "sh_script");
//
//            lua_pushstring(L, ptr->output_file_pattern().data());
//            lua_setfield(L, -2, "output_file_pattern");
//
//            lua_pushstring(L, ptr->error_file_pattern().data());
//            lua_setfield(L, -2, "error_file_pattern");
//
//            lua_pushstring(L, ptr->interpreter().data());
//            lua_setfield(L, -2, "interpreter");
//          } else {
//            lua_pushnil(L);
//          }
//        }},
//       {"interactive_meta",
//        [](lua_State* L, const TaskInCtld& t) {
//          if (t.TaskToCtld().has_interactive_meta()) {
//            const auto* ptr = &t.TaskToCtld().interactive_meta();
//            lua_newtable(L);
//
//            lua_pushstring(L, ptr->cfored_name().data());
//            lua_setfield(L, -2, "cfored_name");
//            lua_pushstring(L, ptr->sh_script().data());
//            lua_setfield(L, -2, "sh_script");
//            lua_pushstring(L, ptr->term_env().data());
//            lua_setfield(L, -2, "term_env");
//            lua_pushnumber(L, ptr->interactive_type());
//            lua_setfield(L, -2, "interactive_type");
//            lua_pushboolean(L, ptr->pty());
//            lua_setfield(L, -2, "pty");
//            lua_pushboolean(L, ptr->x11());
//            lua_setfield(L, -2, "x11");
//          } else {
//            lua_pushnil(L);
//          }
//        }},
//       {"extra_attr",
//        [](lua_State* L, const TaskInCtld& t) {
//          lua_pushstring(L, t.extra_attr.data());
//        }},
//       {"cmd_line",
//        [](lua_State* L, const TaskInCtld& t) {
//          lua_pushstring(L, t.cmd_line.data());
//        }},
//       {"cwd", [](lua_State* L,
//                  const TaskInCtld& t) { lua_pushstring(L, t.cwd.data()); }},
//       {"env",
//        [](lua_State* L, const TaskInCtld& t) {
//          lua_newtable(L);
//          const auto& env_map = t.env;
//          for (const auto& kv : env_map) {
//            lua_pushstring(L, kv.first.c_str());
//            lua_pushstring(L, kv.second.c_str());
//            lua_settable(L, -3);
//          }
//        }},
//       {"excludes",
//        [](lua_State* L, const TaskInCtld& t) {
//          lua_pushstring(L, t.TaskToCtld().excludes().data());
//        }},
//       {"nodelist",
//        [](lua_State* L, const TaskInCtld& t) {
//          lua_pushstring(L, t.TaskToCtld().nodelist().data());
//        }},
//       {"reservation",
//        [](lua_State* L, const TaskInCtld& t) {
//          lua_pushstring(L, t.reservation.data());
//        }},
//       {"begin_time",
//        [](lua_State* L, const TaskInCtld& t) {
//          lua_pushnumber(L, absl::ToUnixSeconds(t.begin_time));
//        }},
//       {"exclusive",
//        [](lua_State* L, const TaskInCtld& t) {
//          lua_pushboolean(L, t.exclusive);
//        }},
//       {"hold",
//        [](lua_State* L, const TaskInCtld& t) {
//          lua_pushboolean(L, t.TaskToCtld().hold());
//        }},
//   };
//
//   auto it = handlers.find(name);
//   if (it != handlers.end()) {
//     it->second(lua_state, job_desc);
//   } else {
//     lua_pushnil(lua_state);
//   }
//   return 1;
// }
//
// int LuaJobHandler::GetPartRecField_(
//     const crane::grpc::PartitionInfo& partition_meta, const std::string&
//     name, lua_State* lua_state) {
//   using Handler =
//       std::function<void(lua_State*, const crane::grpc::PartitionInfo&)>;
//   static const std::unordered_map<std::string, Handler> handlers = {
//       {"name",
//        [](lua_State* L, const crane::grpc::PartitionInfo& p) {
//          lua_pushstring(L, p.name().c_str());
//        }},
//       {"nodelist",
//        [](lua_State* L, const crane::grpc::PartitionInfo& p) {
//          lua_pushstring(L, p.hostlist().data());
//        }},
//       {"total_nodes",
//        [](lua_State* L, const crane::grpc::PartitionInfo& p) {
//          lua_pushinteger(L, p.total_nodes());
//        }},
//       {"alive_nodes",
//        [](lua_State* L, const crane::grpc::PartitionInfo& p) {
//          lua_pushinteger(L, p.alive_nodes());
//        }},
//       {"state",
//        [](lua_State* L, const crane::grpc::PartitionInfo& p) {
//          lua_pushinteger(L, p.state());
//        }},
//       {"default_mem_per_cpu",
//        [](lua_State* L, const crane::grpc::PartitionInfo& p) {
//          lua_pushinteger(L, p.default_mem_per_cpu());
//        }},
//       {"max_mem_per_cpu",
//        [](lua_State* L, const crane::grpc::PartitionInfo& p) {
//          lua_pushinteger(L, p.max_mem_per_cpu());
//        }},
//       {"allowed_accounts",
//        [](lua_State* L, const crane::grpc::PartitionInfo& p) {
//          lua_newtable(L);
//          int idx = 1;
//          for (const auto& acc : p.allowed_accounts()) {
//            lua_pushstring(L, acc.c_str());
//            lua_seti(L, -2, idx++);
//          }
//        }},
//       {"denied_accounts",
//        [](lua_State* L, const crane::grpc::PartitionInfo& p) {
//          lua_newtable(L);
//          int idx = 1;
//          for (const auto& acc : p.denied_accounts()) {
//            lua_pushstring(L, acc.c_str());
//            lua_seti(L, -2, idx++);
//          }
//        }},
//       {"res_total",
//        [](lua_State* L, const crane::grpc::PartitionInfo& p) {
//          PushResourceView_(L, static_cast<ResourceView>(p.res_total()));
//        }},
//       {"res_avail",
//        [](lua_State* L, const crane::grpc::PartitionInfo& p) {
//          PushResourceView_(L, static_cast<ResourceView>(p.res_avail()));
//        }},
//       {"res_in_use",
//        [](lua_State* L, const crane::grpc::PartitionInfo& p) {
//          PushResourceView_(L, static_cast<ResourceView>(p.res_alloc()));
//        }},
//   };
//
//   auto it = handlers.find(name);
//   if (it != handlers.end()) {
//     it->second(lua_state, partition_meta);
//   } else {
//     lua_pushnil(lua_state);
//   }
//   return 1;
// }
//
// void LuaJobHandler::UpdateJobGloabl_(const crane::LuaEnvironment& lua_env) {
//   lua_State* lua_state = lua_env.GetLuaState();
//
//   lua_getglobal(lua_state, "crane");
//   lua_newtable(lua_state);  // jobs
//
//   // jobs:iter()
//   lua_pushcfunction(lua_state, JobsIterCb_);
//   lua_setfield(lua_state, -2, "iter");
//
//   lua_setfield(lua_state, -2, "jobs");
//   lua_pop(lua_state, 1);
// }
//
// int LuaJobHandler::JobsIterCb_(lua_State* lua_state) {
//   CRANE_TRACE("JobsIterCb called");
//
//   auto* state = static_cast<JobsIterState*>(
//       lua_newuserdata(lua_state, sizeof(JobsIterState)));
//   new (state) JobsIterState();
//
//   crane::grpc::QueryTasksInfoRequest req;
//   std::unordered_map<job_id_t, crane::grpc::TaskInfo> job_info_map;
//   g_task_scheduler->QueryTasksInRam(&req, &job_info_map);
//
//   for (auto& [_, task] : job_info_map) {
//     state->tasks.emplace_back(task);
//   }
//
//   if (luaL_newmetatable(lua_state, "JobsIterState")) {
//     lua_pushcfunction(lua_state, JobsIterGcCb_);
//     lua_setfield(lua_state, -2, "__gc");
//   }
//   lua_setmetatable(lua_state, -2);
//
//   lua_pushvalue(lua_state, -1);
//   lua_pushcclosure(lua_state, JobsIterNextCb_, 1);
//
//   lua_insert(lua_state, -2);
//
//   lua_pushnil(lua_state);
//   return 3;
// }
//
// int LuaJobHandler::JobsIterNextCb_(lua_State* lua_state) {
//   CRANE_TRACE("JobsIterNextCb called");
//
//   auto* state = static_cast<JobsIterState*>(
//       lua_touserdata(lua_state, lua_upvalueindex(1)));
//
//   if (state->index >= state->tasks.size()) return 0;
//
//   const auto& task = state->tasks[state->index++];
//   std::string id = std::to_string(task.task_id());
//
//   lua_pushstring(lua_state, id.data());
//
//   lua_newtable(lua_state);
//
//   lua_newtable(lua_state);
//   lua_pushcfunction(lua_state, JobRecFieldIndexCb_);
//   lua_setfield(lua_state, -2, "__index");
//
//   lua_pushlightuserdata(lua_state, (void*)&task);
//   lua_setfield(lua_state, -2, "__job_rec_ptr");
//   lua_setmetatable(lua_state, -2);
//
//   return 2;
// }
//
// int LuaJobHandler::JobsIterGcCb_(lua_State* lua_state) {
//   CRANE_TRACE("JobsIterGC called");
//   auto* state = static_cast<JobsIterState*>(lua_touserdata(lua_state, 1));
//   state->~JobsIterState();
//   return 0;
// }
//
// void LuaJobHandler::UpdateResvGloabl_(const crane::LuaEnvironment& lua_env) {
//   lua_State* lua_state = lua_env.GetLuaState();
//
//   lua_getglobal(lua_state, "crane");
//   lua_newtable(lua_state);  // reservations
//
//   // reservations:iter()
//   lua_pushcfunction(lua_state, ResvsIterCb_);
//   lua_setfield(lua_state, -2, "iter");
//
//   // reservations:get()
//   lua_pushcfunction(lua_state, ResvsGetCb_);
//   lua_setfield(lua_state, -2, "get");
//
//   lua_setfield(lua_state, -2, "reservations");
//   lua_pop(lua_state, 1);
// }
//
// int LuaJobHandler::ResvsIterCb_(lua_State* lua_state) {
//   CRANE_TRACE("ResvsIterCb_ called");
//
//   auto* state = static_cast<ResvsIterState*>(
//       lua_newuserdata(lua_state, sizeof(ResvsIterState)));
//   new (state) ResvsIterState();
//
//   state->reply = g_meta_container->QueryAllResvInfo();
//
//   if (luaL_newmetatable(lua_state, "ResvsIterState")) {
//     lua_pushcfunction(lua_state, ResvsIterGcCb_);
//     lua_setfield(lua_state, -2, "__gc");
//   }
//   lua_setmetatable(lua_state, -2);
//
//   lua_pushvalue(lua_state, -1);
//   lua_pushcclosure(lua_state, ResvsIterNextCb_, 1);
//
//   lua_insert(lua_state, -2);
//
//   lua_pushnil(lua_state);
//   return 3;
// }
//
// int LuaJobHandler::ResvsIterNextCb_(lua_State* lua_state) {
//   CRANE_TRACE("ResvsIterNextCb_ called");
//   auto* state = static_cast<ResvsIterState*>(
//       lua_touserdata(lua_state, lua_upvalueindex(1)));
//
//   if (state->index >= state->reply.reservation_info_list_size()) return 0;
//
//   const auto& resv = state->reply.reservation_info_list()[state->index++];
//
//   lua_pushstring(lua_state, resv.reservation_name().data());
//
//   void* ud = lua_newuserdata(lua_state,
//   sizeof(crane::grpc::ReservationInfo)); new (ud)
//   crane::grpc::ReservationInfo(resv);
//
//   if (luaL_newmetatable(lua_state, "ReservationInfoMT")) {
//     lua_pushcfunction(lua_state, ResvGcCb_);
//     lua_setfield(lua_state, -2, "__gc");
//     lua_pushcfunction(lua_state, ResvFieldIndexCb_);
//     lua_setfield(lua_state, -2, "__index");
//   }
//   lua_setmetatable(lua_state, -2);
//
//   return 2;
// }
//
// int LuaJobHandler::ResvsGetCb_(lua_State* lua_state) {
//   CRANE_TRACE("ResvsGetCb_ called");
//   std::string name = luaL_checkstring(lua_state, 2);
//
//   auto reply = g_meta_container->QueryResvInfo(name);
//   if (reply.reservation_info_list().empty()) {
//     lua_pushnil(lua_state);
//     return 1;
//   }
//
//   void* ud = lua_newuserdata(lua_state,
//   sizeof(crane::grpc::ReservationInfo)); new (ud)
//   crane::grpc::ReservationInfo(reply.reservation_info_list()[0]);
//
//   if (luaL_newmetatable(lua_state, "ReservationInfoMT")) {
//     lua_pushcfunction(lua_state, ResvGcCb_);
//     lua_setfield(lua_state, -2, "__gc");
//     lua_pushcfunction(lua_state, ResvFieldIndexCb_);
//     lua_setfield(lua_state, -2, "__index");
//   }
//   lua_setmetatable(lua_state, -2);
//
//   CRANE_TRACE("ResvsGetCb_ end");
//   return 1;
// }
//
// int LuaJobHandler::ResvsIterGcCb_(lua_State* lua_state) {
//   CRANE_TRACE("ResvsIterGcCb_ called");
//   auto* state = static_cast<ResvsIterState*>(lua_touserdata(lua_state, 1));
//   state->~ResvsIterState();
//   return 0;
// }
//
// int LuaJobHandler::ResvGcCb_(lua_State* lua_state) {
//   CRANE_TRACE("ResvGcCb_ called");
//   void* ud = lua_touserdata(lua_state, 1);
//   if (ud) static_cast<crane::grpc::ReservationInfo*>(ud)->~ReservationInfo();
//   return 0;
// }
//
// void LuaJobHandler::PushJobDesc_(TaskInCtld* task,
//                                  const crane::LuaEnvironment& lua_env) {
//   lua_env.GetLuaState().new_usertype<ResourceView>("ResourceView",
//     "cpu_count", &ResourceView::CpuCount,
//     "memory_bytes", &ResourceView::MemoryBytes);
//
//   lua_env.GetLuaState().new_usertype<TaskInCtld>("TaskInCtld",
//       // [1] Fields
//       "time_limit", &TaskInCtld::time_limit, "partition_id",
//       &TaskInCtld::partition_id, "requested_node_res_view",
//       &TaskInCtld::requested_node_res_view, "type", &TaskInCtld::type, "uid",
//       &TaskInCtld::uid, "gid", &TaskInCtld::gid, "account",
//       &TaskInCtld::account, "name", &TaskInCtld::name, "qos",
//       &TaskInCtld::qos, "node_num", &TaskInCtld::node_num, "ntasks_per_node",
//       &TaskInCtld::ntasks_per_node, "cpus_per_task",
//       &TaskInCtld::cpus_per_task, "included_nodes",
//       &TaskInCtld::included_nodes, "excluded_nodes",
//       &TaskInCtld::excluded_nodes, "requeue_if_failed",
//       &TaskInCtld::requeue_if_failed, "get_user_env",
//       &TaskInCtld::get_user_env, "cmd_line", &TaskInCtld::cmd_line, "env",
//       &TaskInCtld::env, "cwd", &TaskInCtld::cwd, "extra_attr",
//       &TaskInCtld::extra_attr, "meta", &TaskInCtld::meta, "reservation",
//       &TaskInCtld::reservation, "begin_time", &TaskInCtld::begin_time,
//       "exclusive", &TaskInCtld::exclusive, "licenses_count",
//       &TaskInCtld::licenses_count);
// }
//
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
//
// void LuaJobHandler::PushJobRec_(const crane::LuaEnvironment& lua_env,
//                                 crane::grpc::TaskInfo* task) {
//   lua_newtable(lua_env.GetLuaState());
//
//   lua_newtable(lua_env.GetLuaState());
//   lua_pushcfunction(lua_env.GetLuaState(), JobRecFieldIndexCb_);
//   lua_setfield(lua_env.GetLuaState(), -2, "__index");
//   /* Store the job_ptr in the metatable, so the index
//    * function knows which struct it's getting data for.
//    */
//   lua_pushlightuserdata(lua_env.GetLuaState(), task);
//   lua_setfield(lua_env.GetLuaState(), -2, "_job_rec_ptr");
//   lua_setmetatable(lua_env.GetLuaState(), -2);
// }
//
// int LuaJobHandler::GetJobReqFieldIndexCb_(lua_State* lua_state) {
//   const std::string& name = luaL_checkstring(lua_state, 2);
//   lua_getmetatable(lua_state, -2);
//   lua_getfield(lua_state, -1, "_job_desc");
//   const auto* job_desc =
//       static_cast<const TaskInCtld*>(lua_touserdata(lua_state, -1));
//   if (job_desc == nullptr) {
//     CRANE_ERROR("job_desc is nullptr");
//     lua_pushnil(lua_state);
//     return 1;
//   }
//
//   return GetJobReqField_(*job_desc, name, lua_state);
// }
//
// int LuaJobHandler::JobRecFieldIndexCb_(lua_State* lua_state) {
//   CRANE_TRACE("JobRecFieldIndex_ called");
//
//   const std::string& name = luaL_checkstring(lua_state, 2);
//   crane::grpc::TaskInfo* job_ptr = nullptr;
//
//   if (lua_getmetatable(lua_state, 1)) {
//     lua_getfield(lua_state, -1, "__job_rec_ptr");
//     job_ptr =
//         static_cast<crane::grpc::TaskInfo*>(lua_touserdata(lua_state, -1));
//     lua_pop(lua_state, 2);  // metatable + job_ptr
//   }
//
//   return LuaJobRecordField_(lua_state, job_ptr, name);
// }
//
// int LuaJobHandler::PartitionRecFieldIndexCb_(lua_State* lua_state) {
//   const std::string& name = luaL_checkstring(lua_state, 2);
//   crane::grpc::PartitionInfo* part_ptr;
//
//   lua_getmetatable(lua_state, -2);
//   lua_getfield(lua_state, -1, "__part_rec_ptr");
//   part_ptr =
//       static_cast<crane::grpc::PartitionInfo*>(lua_touserdata(lua_state,
//       -1));
//   if (part_ptr == nullptr) {
//     CRANE_ERROR("part_ptr is nullptr");
//     lua_pushnil(lua_state);
//     return 1;
//   }
//
//   return GetPartRecField_(*part_ptr, name, lua_state);
// }
//
// int LuaJobHandler::ResvFieldIndexCb_(lua_State* lua_state) {
//   CRANE_TRACE("ResvFieldIndexCb_ called");
//
//   const std::string& name = luaL_checkstring(lua_state, 2);
//   auto* resv_ptr =
//       static_cast<crane::grpc::ReservationInfo*>(lua_touserdata(lua_state,
//       1));
//
//   return ResvField_(lua_state, resv_ptr, name);
// }
//
// int LuaJobHandler::LuaJobRecordField_(lua_State* lua_state,
//                                       crane::grpc::TaskInfo* job_ptr,
//                                       const std::string& name) {
//   if (!job_ptr) {
//     CRANE_ERROR("_job_rec_field: job_ptr is NULL");
//     lua_pushnil(lua_state);
//     return 1;
//   }
//
//   using Handler = std::function<void(lua_State*, const
//   crane::grpc::TaskInfo*)>; static const std::
//       unordered_map<std::string, Handler>
//           handlers =
//               {
//                   {"type",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushnumber(L, j->type());
//                    }},
//                   {"job_id",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushnumber(L, j->task_id());
//                    }},
//                   {"job_name",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushstring(L, j->name().data());
//                    }},
//                   {"partition",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushstring(L, j->partition().data());
//                    }},
//                   {"uid",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushnumber(L, j->uid());
//                    }},
//                   {"gid",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushnumber(L, j->gid());
//                    }},
//                   {"time_limit",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushnumber(L, static_cast<uint32_t>(j->time_limit().seconds()));
//                    }},
//                   {"start_time",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushnumber(L, j->start_time().seconds());
//                    }},
//                   {"end_time",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushnumber(L, j->end_time().seconds());
//                    }},
//                   {"submit_time",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushnumber(L, j->submit_time().seconds());
//                    }},
//                   {"account",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushstring(L, j->account().data());
//                    }},
//                   {"node_num",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushnumber(L, j->node_num());
//                    }},
//                   {"cmd_line",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushstring(L, j->cmd_line().data());
//                    }},
//                   {"cwd",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushstring(L, j->cwd().data());
//                    }},
//                   {"username",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushstring(L, j->username().data());
//                    }},
//                   {"qos",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushstring(L, j->qos().data());
//                    }},
//     {"req_res_view", [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          PushResourceView_(L, static_cast<ResourceView>(j->req_res_view()));
//           }},
//                   {"req_nodes",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushstring(L, absl::StrJoin(j->req_nodes(), ",").data());
//                    }},
//                   {"exclude_nodes",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushstring(L, absl::StrJoin(j->exclude_nodes(), ",").data());
//                    }},
//                   {"extra_attr",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushstring(L, j->extra_attr().data());
//                    }},
//                   {"reservation",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushstring(L, j->reservation().data());
//                    }},
//                   {"held", [](lua_State* L,
//                               const crane::grpc::TaskInfo*
//                                   j) {
//          lua_pushboolean(L, j->held()); }},
//                   {"status",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//                      lua_pushnumber(L, j->status());
//                    }},
//                   {"exit_code",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushnumber(L, j->exit_code());
//                    }},
//                   {"priority",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushnumber(L, j->priority());
//                    }},
//                   {"execution_node",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushstring(L, absl::StrJoin(j->execution_node(), ",").data());
//                    }},
//                   {"exclusive",
//                    [](lua_State* L, const crane::grpc::TaskInfo* j) {
//          lua_pushboolean(L, j->exclusive());
//                    }},
//               };
//
//   auto it = handlers.find(name);
//   if (it != handlers.end()) {
//     it->second(lua_state, job_ptr);
//   } else {
//     lua_pushnil(lua_state);
//   }
//   return 1;
// }
//
// int LuaJobHandler::ResvField_(lua_State* lua_state,
//                               crane::grpc::ReservationInfo* resv_ptr,
//                               const std::string& name) {
//   if (!resv_ptr) {
//     CRANE_ERROR("_resv_rec_field: resv_ptr is NULL");
//     lua_pushnil(lua_state);
//     return 1;
//   }
//
//   using Handler =
//       std::function<void(lua_State*, const crane::grpc::ReservationInfo*)>;
//   static const std::unordered_map<std::string, Handler> handlers = {
//       // reservation_name
//       {"reservation_name",
//        [](lua_State* L, const crane::grpc::ReservationInfo* r) {
//          lua_pushstring(L, r->reservation_name().data());
//        }},
//       // start_time (protobuf Timestamp)
//       {"start_time",
//        [](lua_State* L, const crane::grpc::ReservationInfo* r) {
//          lua_pushnumber(L, r->start_time().seconds());
//        }},
//       // duration (protobuf Duration)
//       {"duration",
//        [](lua_State* L, const crane::grpc::ReservationInfo* r) {
//          lua_pushnumber(L, r->duration().seconds());
//        }},
//       // partition
//       {"partition",
//        [](lua_State* L, const crane::grpc::ReservationInfo* r) {
//          lua_pushstring(L, r->partition().c_str());
//        }},
//       // craned_regex
//       {"craned_regex",
//        [](lua_State* L, const crane::grpc::ReservationInfo* r) {
//          lua_pushstring(L, r->craned_regex().c_str());
//        }},
//       // res_total (ResourceView)
//       {"res_total",
//        [](lua_State* L, const crane::grpc::ReservationInfo* r) {
//          PushResourceView_(L, static_cast<ResourceView>(r->res_total()));
//        }},
//       // res_avail (ResourceView)
//       {"res_avail",
//        [](lua_State* L, const crane::grpc::ReservationInfo* r) {
//          PushResourceView_(L, static_cast<ResourceView>(r->res_avail()));
//        }},
//       // res_alloc (ResourceView)
//       {"res_alloc",
//        [](lua_State* L, const crane::grpc::ReservationInfo* r) {
//          PushResourceView_(L, static_cast<ResourceView>(r->res_alloc()));
//        }},
//       // allowed_accounts
//       {"allowed_accounts",
//        [](lua_State* L, const crane::grpc::ReservationInfo* r) {
//          lua_newtable(L);
//          int idx = 1;
//          for (const auto& acc : r->allowed_accounts()) {
//            lua_pushstring(L, acc.c_str());
//            lua_seti(L, -2, idx++);
//          }
//        }},
//       // denied_accounts
//       {"denied_accounts",
//        [](lua_State* L, const crane::grpc::ReservationInfo* r) {
//          lua_newtable(L);
//          int idx = 1;
//          for (const auto& acc : r->denied_accounts()) {
//            lua_pushstring(L, acc.c_str());
//            lua_seti(L, -2, idx++);
//          }
//        }},
//       // allowed_users
//       {"allowed_users",
//        [](lua_State* L, const crane::grpc::ReservationInfo* r) {
//          lua_newtable(L);
//          int idx = 1;
//          for (const auto& user : r->allowed_users()) {
//            lua_pushstring(L, user.c_str());
//            lua_seti(L, -2, idx++);
//          }
//        }},
//       // denied_users
//       {"denied_users",
//        [](lua_State* L, const crane::grpc::ReservationInfo* r) {
//          lua_newtable(L);
//          int idx = 1;
//          for (const auto& user : r->denied_users()) {
//            lua_pushstring(L, user.c_str());
//            lua_seti(L, -2, idx++);
//          }
//        }},
//   };
//
//   auto it = handlers.find(name);
//   if (it != handlers.end()) {
//     it->second(lua_state, resv_ptr);
//   } else {
//     lua_pushnil(lua_state);
//   }
//   return 1;
// }
//
// void LuaJobHandler::PushResourceView_(lua_State* L, const ResourceView& res)
// {
//   lua_newtable(L);  // ResourceView table
//
//   // allocatable_res
//   lua_newtable(L);
//   const auto& ar = res.GetAllocatableRes();
//   lua_pushnumber(L, ar.CpuCount());
//   lua_setfield(L, -2, "cpu_core_limit");
//   lua_pushnumber(L, ar.memory_bytes);
//   lua_setfield(L, -2, "memory_limit_bytes");
//   lua_pushnumber(L, ar.memory_sw_bytes);
//   lua_setfield(L, -2, "memory_sw_limit_bytes");
//   lua_setfield(L, -2,
//                "allocatable_res");  // ResourceView.allocatable_res = table
//
//   // device_map
//   lua_newtable(L);
//   for (const auto& dev_pair : res.GetDeviceMap()) {
//     const std::string& device_name = dev_pair.first;
//     const auto& untyped_and_type_map = dev_pair.second;
//     uint64_t untyped_req_count = untyped_and_type_map.first;
//     const auto& type_count_map = untyped_and_type_map.second;
//
//     lua_pushstring(L, device_name.c_str());
//     lua_newtable(L);  // device entry table
//
//     // untyped_req_count
//     lua_pushnumber(L, static_cast<lua_Number>(untyped_req_count));
//     lua_setfield(L, -2, "untyped_req_count");
//
//     // type_count_map
//     lua_newtable(L);
//     for (const auto& type_pair : type_count_map) {
//       lua_pushstring(L, type_pair.first.c_str());
//       lua_pushnumber(L, static_cast<lua_Number>(type_pair.second));
//       lua_settable(L, -3);
//     }
//     lua_setfield(L, -2, "type_count_map");
//
//     lua_settable(L, -3);  // device_map[device_name] = device entry table
//   }
//
//   //  device_map table
//   lua_setfield(L, -2, "device_map");  // parent_table.device_map = device_map
// }
#endif

}  // namespace Ctld

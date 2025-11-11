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

#include "LuaJobHandler.h"

#include "CranedMetaContainer.h"
#include "TaskScheduler.h"

namespace Ctld {

CraneRichError LuaJobHandler::JobSubmit(TaskInCtld& task,
                                        const std::string& lua_script) {
  CraneRichError result = FormatRichErr(CraneErrCode::SUCCESS, "");
#ifdef HAVE_LUA
  auto lua_env = std::make_unique<crane::LuaEnvironment>();
  if (!lua_env->Init(lua_script))
    return FormatRichErr(CraneErrCode::ERR_LUA_FAILED,
                         "Failed to init lua environment");

  lua_env->LuaTableRegister(kCraneFunctions);

  lua_env->RegisterLuaCraneStructFunctions(kGlobalFunctions);

  if (!lua_env->LoadLuaScript(kReqFxns))
    return FormatRichErr(CraneErrCode::ERR_LUA_FAILED,
                         "Failed to load lua script");

  /*
   *  All lua script functions should have been verified during
   *   initialization:
   */
  lua_getglobal(lua_env->GetLuaState(), "crane_job_submit");
  if (lua_isnil(lua_env->GetLuaState(), -1))
    return FormatRichErr(CraneErrCode::ERR_LUA_FAILED,
                         "lua environment is nil");

  crane::grpc::QueryTasksInfoReply task_info_reply;
  crane::grpc::QueryReservationInfoReply resv_info_reply;
  crane::grpc::QueryPartitionInfoReply partition_info_reply;

  UpdateJobGloable_(*lua_env, &task_info_reply);
  UpdateJobResvGloable_(*lua_env, &resv_info_reply);
  PushJobDesc_(&task, *lua_env);
  PushPartitionList_(*lua_env, task.Username(), task.account,
                     &partition_info_reply);
  lua_pushnumber(lua_env->GetLuaState(), task.uid);

  int rc = CraneErrCode::ERR_LUA_FAILED;
  if (lua_pcall(lua_env->GetLuaState(), 3, 1, 0) != 0) {
    CRANE_ERROR("{}", lua_tostring(lua_env->GetLuaState(), -1));
  } else {
    if (lua_isnumber(lua_env->GetLuaState(), -1)) {
      rc = lua_tonumber(lua_env->GetLuaState(), -1);
    } else {
      CRANE_INFO("{}/lua: non-numeric return code", __func__);
    }
  }
  lua_pop(lua_env->GetLuaState(), 1);

  std::string user_msg;
  if (!lua_env->GetUserMsg().empty()) {
    CRANE_TRACE("lua user_msg: {}", lua_env->GetUserMsg());
    user_msg = lua_env->GetUserMsg();
    lua_env->ResetUserMsg();
  }

  if (rc != 0) return FormatRichErr(static_cast<CraneErrCode>(rc), user_msg);
#endif

  return result;
}

CraneRichError LuaJobHandler::JobModify(TaskInCtld& task_in_ctld,
                                        const std::string& lua_script) {
  CraneRichError result = FormatRichErr(CraneErrCode::SUCCESS, "");
#ifdef HAVE_LUA
  crane::grpc::TaskInfo task_info;
  task_in_ctld.SetFieldsOfTaskInfo(&task_info);

  auto lua_env = std::make_unique<crane::LuaEnvironment>();
  if (!lua_env->Init(lua_script))
    return FormatRichErr(CraneErrCode::ERR_LUA_FAILED,
                         "Failed to init lua environment");

  lua_env->LuaTableRegister(kCraneFunctions);

  lua_env->RegisterLuaCraneStructFunctions(kGlobalFunctions);

  if (!lua_env->LoadLuaScript(kReqFxns))
    return FormatRichErr(CraneErrCode::ERR_LUA_FAILED,
                         "Failed to load lua script");

  /*
   *  All lua script functions should have been verified during
   *   initialization:
   */
  lua_getglobal(lua_env->GetLuaState(), "crane_job_modify");
  if (lua_isnil(lua_env->GetLuaState(), -1))
    return FormatRichErr(CraneErrCode::ERR_LUA_FAILED,
                         "lua environment is nil");

  crane::grpc::QueryReservationInfoReply resv_info_reply;
  crane::grpc::QueryPartitionInfoReply partition_info_reply;

  // dead lock
  // UpdateJobGloable_();
  UpdateJobResvGloable_(*lua_env, &resv_info_reply);

  PushJobDesc_(&task_in_ctld, *lua_env);
  PushJobRec_(*lua_env, &task_info);
  PushPartitionList_(*lua_env, task_in_ctld.Username(), task_in_ctld.account,
                     &partition_info_reply);
  lua_pushnumber(lua_env->GetLuaState(), task_in_ctld.uid);

  int rc = CraneErrCode::ERR_LUA_FAILED;
  if (lua_pcall(lua_env->GetLuaState(), 4, 1, 0) != 0 != 0) {
    CRANE_ERROR("{}", lua_tostring(lua_env->GetLuaState(), -1));
  } else {
    if (lua_isnumber(lua_env->GetLuaState(), -1)) {
      rc = lua_tonumber(lua_env->GetLuaState(), -1);
    } else {
      CRANE_INFO("{}/lua: non-numeric return code", __func__);
    }
  }
  lua_pop(lua_env->GetLuaState(), 1);

  std::string user_msg;
  if (!lua_env->GetUserMsg().empty()) {
    CRANE_TRACE("lua user_msg: {}", lua_env->GetUserMsg());
    user_msg = lua_env->GetUserMsg();
    lua_env->ResetUserMsg();
  }

  if (rc != 0) return FormatRichErr(static_cast<CraneErrCode>(rc), user_msg);
#endif
  return result;
}
#ifdef HAVE_LUA

const luaL_Reg LuaJobHandler::kGlobalFunctions[] = {
    {"_get_job_env_field_name", LuaJobHandler::GetJobEnvFieldNameCb_},
    {"_get_job_req_field_name", LuaJobHandler::GetJobReqFieldNameCb_},
    {"_set_job_env_field", LuaJobHandler::SetJobEnvFieldCb_},
    {"_set_job_req_field", LuaJobHandler::SetJobReqFieldCb_},
    {"_get_part_rec_field", LuaJobHandler::GetPartRecFieldNameCb_},
    {nullptr, nullptr}};

const luaL_Reg LuaJobHandler::kCraneFunctions[] = {
    {"get_qos_priority", GetQosPriorityCb_}, {nullptr, nullptr}};

const char* LuaJobHandler::kReqFxns[] = {"crane_job_submit", "crane_job_modify",
                                         nullptr};

int LuaJobHandler::GetQosPriorityCb_(lua_State* lua_state) {
  std::string qos_name = lua_tostring(lua_state, -1);

  auto qos = g_account_manager->GetExistedQosInfo(qos_name);
  if (!qos) {
    CRANE_ERROR("Invalid QOS name: {}", qos_name);
    return 0;
  }

  lua_pushnumber(lua_state, qos->priority);
  return 1;
}

int LuaJobHandler::GetJobEnvFieldNameCb_(lua_State* lua_state) {
  const auto* job_desc =
      static_cast<const TaskInCtld*>(lua_touserdata(lua_state, 1));
  if (job_desc == nullptr) {
    CRANE_ERROR("job_desc is nullptr");
    lua_pushnil(lua_state);
    return 1;
  }
  const char* name = luaL_checkstring(lua_state, 2);
  return GetJobEnvField_(*job_desc, name, lua_state);
}

int LuaJobHandler::GetJobReqFieldNameCb_(lua_State* lua_state) {
  const auto* job_desc =
      static_cast<const TaskInCtld*>(lua_touserdata(lua_state, 1));
  if (job_desc == nullptr) {
    CRANE_ERROR("job_desc is nullptr");
    lua_pushnil(lua_state);
    return 1;
  }
  const char* name = luaL_checkstring(lua_state, 2);

  return GetJobReqField_(*job_desc, name, lua_state);
}

int LuaJobHandler::SetJobEnvFieldCb_(lua_State* lua_state) {
  const auto* name = luaL_checkstring(lua_state, 2);
  lua_getmetatable(lua_state, -3);
  lua_getfield(lua_state, -1, "_job_desc");

  auto* job_desc = static_cast<TaskInCtld*>(lua_touserdata(lua_state, -1));
  if (job_desc == nullptr) {
    CRANE_ERROR("job_desc is nullptr");
    lua_pushnil(lua_state);
    return 0;
  }

  if (!job_desc->IsBatch()) {
    CRANE_INFO("job_desc->environment only accessible for batch jobs.");
    lua_pushnil(lua_state);
    return 0;
  }

  const auto* value_str = luaL_checkstring(lua_state, 3);
  job_desc->env[name] = value_str;

  return 0;
}

int LuaJobHandler::SetJobReqFieldCb_(lua_State* lua_state) {
  const std::string name = luaL_checkstring(lua_state, 2);
  lua_getmetatable(lua_state, -3);
  lua_getfield(lua_state, -1, "_job_desc");
  auto* job_desc = static_cast<TaskInCtld*>(lua_touserdata(lua_state, -1));
  if (job_desc == nullptr) {
    CRANE_ERROR("job_desc is nullptr");
    lua_pushnil(lua_state);
    return 0;
  }

  if (name == "partition") {
    const char* value_str = luaL_checkstring(lua_state, 3);
    job_desc->partition_id = value_str;
  }
  if (name == "name") {
    const char* value_str = luaL_checkstring(lua_state, 3);
    job_desc->name = value_str;
  }
  if (name == "account") {
    const char* value_str = luaL_checkstring(lua_state, 3);
    job_desc->account = value_str;
  }
  if (name == "qos") {
    const char* value_str = luaL_checkstring(lua_state, 3);
    job_desc->qos = value_str;
  }
  if (name == "cmd_line") {
    const char* value_str = luaL_checkstring(lua_state, 3);
    job_desc->cmd_line = value_str;
  }
  if (name == "cwd") {
    const char* value_str = luaL_checkstring(lua_state, 3);
    job_desc->cwd = value_str;
  }
  if (name == "reservation") {
    const char* value_str = luaL_checkstring(lua_state, 3);
    job_desc->reservation = value_str;
  }
  if (name == "extra_attr") {
    const char* value_str = luaL_checkstring(lua_state, 3);
    job_desc->extra_attr = value_str;
  }
  if (name == "type") {
    job_desc->type =
        static_cast<crane::grpc::TaskType>(luaL_checknumber(lua_state, 3));
  }
  if (name == "uid") {
    job_desc->uid = static_cast<uid_t>(luaL_checknumber(lua_state, 3));
  }
  if (name == "gid") {
    job_desc->gid = static_cast<gid_t>(luaL_checknumber(lua_state, 3));
  }
  if (name == "node_num") {
    job_desc->node_num = static_cast<int>(luaL_checknumber(lua_state, 3));
  }
  if (name == "ntasks_per_node") {
    job_desc->ntasks_per_node =
        static_cast<int>(luaL_checknumber(lua_state, 3));
  }
  if (name == "cpus_per_task") {
    job_desc->cpus_per_task =
        static_cast<cpu_t>(luaL_checknumber(lua_state, 3));
  }
  if (name == "requeue_if_failed") {
    job_desc->requeue_if_failed = lua_toboolean(lua_state, 3);
  }
  if (name == "get_user_env") {
    job_desc->get_user_env = lua_toboolean(lua_state, 3);
  }

  return 0;
}

// partition_record
int LuaJobHandler::GetPartRecFieldNameCb_(lua_State* lua_state) {
  const auto* partition_meta = static_cast<const crane::grpc::PartitionInfo*>(
      lua_touserdata(lua_state, 1));
  const char* name = luaL_checkstring(lua_state, 2);
  if (partition_meta == nullptr) {
    CRANE_ERROR("partition_meta is nil");
    lua_pushnil(lua_state);
    return 1;
  }

  return GetPartRecField_(*partition_meta, name, lua_state);
}

int LuaJobHandler::GetJobEnvField_(const TaskInCtld& job_desc, const char* name,
                                   lua_State* lua_state) {
  if (job_desc.env.empty()) {
    if (job_desc.type == crane::grpc::TaskType::Batch) {
      CRANE_ERROR("job_desc->environment is nullptr.");
    } else {
      CRANE_INFO("job_desc->environment only accessible for batch jobs.");
    }
    lua_pushnil(lua_state);
  } else {
    auto iter = job_desc.env.find(name);
    if (iter != job_desc.env.end()) {
      lua_pushstring(lua_state, iter->second.data());
    } else {
      lua_pushnil(lua_state);
    }
  }

  return 1;
}

int LuaJobHandler::GetJobReqField_(const TaskInCtld& job_desc, const char* name,
                                   lua_State* lua_state) {
  using Handler = std::function<void(lua_State*, const TaskInCtld&)>;

  static const std::unordered_map<std::string, Handler> handlers = {
      {"time_limit",
       [](lua_State* L, const TaskInCtld& t) {
         lua_pushnumber(L, absl::ToDoubleSeconds(t.time_limit));
       }},
      {"partition",
       [](lua_State* L, const TaskInCtld& t) {
         lua_pushstring(L, t.partition_id.data());
       }},
      {"requested_node_res_view",
       [](lua_State* L, const TaskInCtld& t) {
         PushResourceView_(L, t.requested_node_res_view);
       }},
      {"type",
       [](lua_State* L, const TaskInCtld& t) { lua_pushnumber(L, t.type); }},
      {"uid",
       [](lua_State* L, const TaskInCtld& t) { lua_pushnumber(L, t.uid); }},
      {"account",
       [](lua_State* L, const TaskInCtld& t) {
         lua_pushstring(L, t.account.data());
       }},
      {"name", [](lua_State* L,
                  const TaskInCtld& t) { lua_pushstring(L, t.name.data()); }},
      {"qos", [](lua_State* L,
                 const TaskInCtld& t) { lua_pushstring(L, t.qos.data()); }},
      {"node_num", [](lua_State* L,
                      const TaskInCtld& t) { lua_pushnumber(L, t.node_num); }},
      {"ntasks_per_node",
       [](lua_State* L, const TaskInCtld& t) {
         lua_pushnumber(L, t.ntasks_per_node);
       }},
      {"cpus_per_task",
       [](lua_State* L, const TaskInCtld& t) {
         lua_pushnumber(L, static_cast<double>(t.cpus_per_task));
       }},
      {"requeue_if_failed",
       [](lua_State* L, const TaskInCtld& t) {
         lua_pushboolean(L, t.requeue_if_failed);
       }},
      {"get_user_env",
       [](lua_State* L, const TaskInCtld& t) {
         lua_pushboolean(L, t.get_user_env);
       }},
      {"gid",
       [](lua_State* L, const TaskInCtld& t) { lua_pushnumber(L, t.gid); }},
      {"extra_attr",
       [](lua_State* L, const TaskInCtld& t) {
         lua_pushstring(L, t.extra_attr.data());
       }},
      {"cmd_line",
       [](lua_State* L, const TaskInCtld& t) {
         lua_pushstring(L, t.cmd_line.data());
       }},
      {"cwd", [](lua_State* L,
                 const TaskInCtld& t) { lua_pushstring(L, t.cwd.data()); }},
      {"env",
       [](lua_State* L, const TaskInCtld& t) {
         lua_newtable(L);
         const auto& env_map = t.env;
         for (const auto& kv : env_map) {
           lua_pushstring(L, kv.first.c_str());
           lua_pushstring(L, kv.second.c_str());
           lua_settable(L, -3);
         }
       }},
      {"reservation",
       [](lua_State* L, const TaskInCtld& t) {
         lua_pushstring(L, t.reservation.data());
       }},
      // {"begin_time",
      //  [](lua_State* L, const TaskInCtld& t) {
      //    lua_pushnumber(L, absl::ToDoubleSeconds(t.begin_time));
      //  }},
      {"bash_meta",
       [](lua_State* L, const TaskInCtld& t) {
         if (auto* ptr = std::get_if<BatchMetaInTask>(&t.meta)) {
           const auto& meta = ptr;
           lua_newtable(L);

           lua_pushstring(L, ptr->sh_script.c_str());
           lua_setfield(L, -2, "sh_script");

           lua_pushstring(L, ptr->output_file_pattern.c_str());
           lua_setfield(L, -2, "output_file_pattern");

           lua_pushstring(L, ptr->error_file_pattern.c_str());
           lua_setfield(L, -2, "error_file_pattern");

           lua_pushstring(L, ptr->interpreter.c_str());
           lua_setfield(L, -2, "interpreter");
         } else {
           lua_pushnil(L);
         }
       }},
      {"interactive_meta",
       [](lua_State* L, const TaskInCtld& t) {
         if (auto* ptr = std::get_if<InteractiveMetaInTask>(&t.meta)) {
           lua_newtable(L);
           lua_pushnumber(L, ptr->interactive_type);
           lua_setfield(L, -2, "interactive_type");
           // TODO: Add more fields.
         } else {
           lua_pushnil(L);
         }
       }},
  };

  auto it = handlers.find(name);
  if (it != handlers.end()) {
    it->second(lua_state, job_desc);
  } else {
    lua_pushnil(lua_state);
  }
  return 1;
}

int LuaJobHandler::GetPartRecField_(
    const crane::grpc::PartitionInfo& partition_meta, const char* name,
    lua_State* lua_state) {
  using Handler =
      std::function<void(lua_State*, const crane::grpc::PartitionInfo&)>;
  static const std::unordered_map<std::string, Handler> handlers = {
      {"name",
       [](lua_State* L, const crane::grpc::PartitionInfo& p) {
         lua_pushstring(L, p.name().c_str());
       }},
      {"nodelist",
       [](lua_State* L, const crane::grpc::PartitionInfo& p) {
         lua_pushstring(L, p.hostlist().data());
       }},
      {"total_nodes",
       [](lua_State* L, const crane::grpc::PartitionInfo& p) {
         lua_pushinteger(L, p.total_nodes());
       }},
      {"alive_nodes",
       [](lua_State* L, const crane::grpc::PartitionInfo& p) {
         lua_pushinteger(L, p.alive_nodes());
       }},
      {"state",
       [](lua_State* L, const crane::grpc::PartitionInfo& p) {
         lua_pushinteger(L, p.state());
       }},
      {"default_mem_per_cpu",
       [](lua_State* L, const crane::grpc::PartitionInfo& p) {
         lua_pushinteger(L, p.default_mem_per_cpu());
       }},
      {"max_mem_per_cpu",
       [](lua_State* L, const crane::grpc::PartitionInfo& p) {
         lua_pushinteger(L, p.max_mem_per_cpu());
       }},
      {"allowed_accounts",
       [](lua_State* L, const crane::grpc::PartitionInfo& p) {
         lua_newtable(L);
         int idx = 1;
         for (const auto& acc : p.allowed_accounts()) {
           lua_pushstring(L, acc.c_str());
           lua_seti(L, -2, idx++);
         }
       }},
      {"denied_accounts",
       [](lua_State* L, const crane::grpc::PartitionInfo& p) {
         lua_newtable(L);
         int idx = 1;
         for (const auto& acc : p.denied_accounts()) {
           lua_pushstring(L, acc.c_str());
           lua_seti(L, -2, idx++);
         }
       }},
      {"res_total",
       [](lua_State* L, const crane::grpc::PartitionInfo& p) {
         PushResourceView_(L, static_cast<ResourceView>(p.res_total()));
       }},
      {"res_avail",
       [](lua_State* L, const crane::grpc::PartitionInfo& p) {
         PushResourceView_(L, static_cast<ResourceView>(p.res_avail()));
       }},
      {"res_in_use",
       [](lua_State* L, const crane::grpc::PartitionInfo& p) {
         PushResourceView_(L, static_cast<ResourceView>(p.res_alloc()));
       }},
  };

  auto it = handlers.find(name);
  if (it != handlers.end()) {
    it->second(lua_state, partition_meta);
  } else {
    lua_pushnil(lua_state);
  }
  return 1;
}

void LuaJobHandler::UpdateJobGloable_(
    const crane::LuaEnvironment& lua_env,
    crane::grpc::QueryTasksInfoReply* task_info_reply) {
  lua_getglobal(lua_env.GetLuaState(), "crane");
  lua_newtable(lua_env.GetLuaState());

  crane::grpc::QueryTasksInfoRequest request;
  g_task_scheduler->QueryTasksInRam(&request, task_info_reply);
  for (const auto& task : task_info_reply->task_info_list()) {
    /*
     * Create an empty table, with a metatable that looks up the
     * data for the individual job.
     */
    lua_newtable(lua_env.GetLuaState());

    lua_newtable(lua_env.GetLuaState());
    lua_pushcfunction(lua_env.GetLuaState(), JobRecFieldIndex_);
    lua_setfield(lua_env.GetLuaState(), -2, "__index");
    /*
     * Store the job_record in the metatable, so the index
     * function knows which job it's getting data for.
     */
    lua_pushlightuserdata(lua_env.GetLuaState(), (void*)&task);
    lua_setfield(lua_env.GetLuaState(), -2, "_job_rec_ptr");
    lua_setmetatable(lua_env.GetLuaState(), -2);

    /* Lua copies passed strings, so we can reuse the buffer. */
    lua_setfield(lua_env.GetLuaState(), -2,
                 std::to_string(task.task_id()).data());
  }

  lua_setfield(lua_env.GetLuaState(), -2, "jobs");
  lua_pop(lua_env.GetLuaState(), 1);
}

void LuaJobHandler::UpdateJobResvGloable_(
    const crane::LuaEnvironment& lua_env,
    crane::grpc::QueryReservationInfoReply* resv_info_reply) {
  lua_getglobal(lua_env.GetLuaState(), "crane");
  lua_newtable(lua_env.GetLuaState());

  *resv_info_reply = g_meta_container->QueryAllResvInfo();
  for (const auto& resv : resv_info_reply->reservation_info_list()) {
    /*
     * Create an empty table, with a metatable that looks up the
     * data for the individual reservation.
     */
    lua_newtable(lua_env.GetLuaState());

    lua_newtable(lua_env.GetLuaState());
    lua_pushcfunction(lua_env.GetLuaState(), ResvFieldIndex_);
    lua_setfield(lua_env.GetLuaState(), -2, "__index");
    /*
     * Store the slurmctld_resv_t in the metatable, so the index
     * function knows which reservation it's getting data for.
     */
    lua_pushlightuserdata(lua_env.GetLuaState(), (void*)&resv);
    lua_setfield(lua_env.GetLuaState(), -2, "_resv_ptr");
    lua_setmetatable(lua_env.GetLuaState(), -2);

    lua_setfield(lua_env.GetLuaState(), -2, resv.reservation_name().data());
  }

  lua_setfield(lua_env.GetLuaState(), -2, "reservations");
  lua_pop(lua_env.GetLuaState(), 1);
}

void LuaJobHandler::PushJobDesc_(TaskInCtld* task,
                                 const crane::LuaEnvironment& lua_env) {
  lua_newtable(lua_env.GetLuaState());

  lua_newtable(lua_env.GetLuaState());
  lua_pushcfunction(lua_env.GetLuaState(), GetJobReqFieldIndex_);
  lua_setfield(lua_env.GetLuaState(), -2, "__index");
  lua_pushcfunction(lua_env.GetLuaState(), SetJobReqFieldCb_);
  lua_setfield(lua_env.GetLuaState(), -2, "__newindex");
  /* Store the job descriptor in the metatable, so the index
   * function knows which struct it's getting data for.
   */
  lua_pushlightuserdata(lua_env.GetLuaState(), task);
  lua_setfield(lua_env.GetLuaState(), -2, "_job_desc");
  lua_setmetatable(lua_env.GetLuaState(), -2);
}

void LuaJobHandler::PushPartitionList_(
    const crane::LuaEnvironment& lua_env, const std::string& user_name,
    const std::string& account,
    crane::grpc::QueryPartitionInfoReply* partition_info_reply) {
  lua_newtable(lua_env.GetLuaState());

  auto user = g_account_manager->GetExistedUserInfo(user_name);
  if (!user) {
    CRANE_ERROR("username is null");
    return;
  }
  std::string actual_account = account;
  if (actual_account.empty()) actual_account = user->default_account;

  *partition_info_reply = g_meta_container->QueryAllPartitionInfo();
  for (const auto& partition : partition_info_reply->partition_info_list()) {
    if (!user->account_to_attrs_map.at(actual_account)
             .allowed_partition_qos_map.contains(partition.name()))
      continue;
    if (partition.allowed_accounts_size() > 0 &&
        !std::ranges::contains(partition.allowed_accounts(), actual_account))
      continue;

    /*
     * Create an empty table, with a metatable that looks up the
     * data for the partition.
     */
    lua_newtable(lua_env.GetLuaState());

    lua_newtable(lua_env.GetLuaState());

    lua_pushcfunction(lua_env.GetLuaState(), PartitionRecFieldIndex_);
    lua_setfield(lua_env.GetLuaState(), -2, "__index");
    /*
     * Store the part_record in the metatable, so the index
     * function knows which job it's getting data for.
     */
    lua_pushlightuserdata(lua_env.GetLuaState(), (void*)&partition);
    lua_setfield(lua_env.GetLuaState(), -2, "_part_rec_ptr");
    lua_setmetatable(lua_env.GetLuaState(), -2);

    lua_setfield(lua_env.GetLuaState(), -2, partition.name().data());
  }
}

void LuaJobHandler::PushJobRec_(const crane::LuaEnvironment& lua_env,
                                crane::grpc::TaskInfo* task) {
  lua_newtable(lua_env.GetLuaState());

  lua_newtable(lua_env.GetLuaState());
  lua_pushcfunction(lua_env.GetLuaState(), JobRecFieldIndex_);
  lua_setfield(lua_env.GetLuaState(), -2, "__index");
  /* Store the job_ptr in the metatable, so the index
   * function knows which struct it's getting data for.
   */
  lua_pushlightuserdata(lua_env.GetLuaState(), task);
  lua_setfield(lua_env.GetLuaState(), -2, "_job_rec_ptr");
  lua_setmetatable(lua_env.GetLuaState(), -2);
}

int LuaJobHandler::GetJobReqFieldIndex_(lua_State* lua_state) {
  const char* name = luaL_checkstring(lua_state, 2);
  lua_getmetatable(lua_state, -2);
  lua_getfield(lua_state, -1, "_job_desc");
  const auto* job_desc =
      static_cast<const TaskInCtld*>(lua_touserdata(lua_state, -1));
  if (job_desc == nullptr) {
    CRANE_ERROR("job_desc is nullptr");
    lua_pushnil(lua_state);
    return 1;
  }

  return GetJobReqField_(*job_desc, name, lua_state);
}

int LuaJobHandler::JobRecFieldIndex_(lua_State* lua_state) {
  const char* name = luaL_checkstring(lua_state, 2);
  crane::grpc::TaskInfo* job_ptr;

  lua_getmetatable(lua_state, -2);
  lua_getfield(lua_state, -1, "_job_rec_ptr");
  job_ptr = static_cast<crane::grpc::TaskInfo*>(lua_touserdata(lua_state, -1));

  return LuaJobRecordField_(lua_state, job_ptr, name);
}

int LuaJobHandler::PartitionRecFieldIndex_(lua_State* lua_state) {
  const char* name = luaL_checkstring(lua_state, 2);
  crane::grpc::PartitionInfo* part_ptr;

  lua_getmetatable(lua_state, -2);
  lua_getfield(lua_state, -1, "_part_rec_ptr");
  part_ptr =
      static_cast<crane::grpc::PartitionInfo*>(lua_touserdata(lua_state, -1));
  if (part_ptr == nullptr) {
    CRANE_ERROR("part_ptr is nullptr");
    lua_pushnil(lua_state);
    return 1;
  }

  return GetPartRecField_(*part_ptr, name, lua_state);
}
int LuaJobHandler::ResvFieldIndex_(lua_State* lua_state) {
  const std::string name = luaL_checkstring(lua_state, 2);
  crane::grpc::ReservationInfo* resv_ptr;

  lua_getmetatable(lua_state, -2);
  lua_getfield(lua_state, -1, "_resv_ptr");
  resv_ptr =
      static_cast<crane::grpc::ReservationInfo*>(lua_touserdata(lua_state, -1));

  return ResvField_(lua_state, resv_ptr, name.data());
}

int LuaJobHandler::LuaJobRecordField_(lua_State* lua_state,
                                      crane::grpc::TaskInfo* job_ptr,
                                      const char* name) {
  if (!job_ptr) {
    CRANE_ERROR("_job_rec_field: job_ptr is NULL");
    lua_pushnil(lua_state);
    return 1;
  }

  using Handler = std::function<void(lua_State*, const crane::grpc::TaskInfo*)>;
  static const std::
      unordered_map<std::string, Handler>
          handlers =
              {
                  {"type",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushnumber(L, j->type());
                   }},
                  {"job_id",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushnumber(L, j->task_id());
                   }},
                  {"job_name",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushstring(L, j->name().data());
                   }},
                  {"partition",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushstring(L, j->partition().data());
                   }},
                  {"uid",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushnumber(L, j->uid());
                   }},
                  {"gid",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushnumber(L, j->gid());
                   }},
                  {"time_limit",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushnumber(L, static_cast<uint32_t>(j->time_limit().seconds()));
                   }},
                  {"start_time",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushnumber(L, j->start_time().seconds());
                   }},
                  {"end_time",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushnumber(L, j->end_time().seconds());
                   }},
                  {"submit_time",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushnumber(L, j->submit_time().seconds());
                   }},
                  {"account",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushstring(L, j->account().data());
                   }},
                  {"node_num",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushnumber(L, j->node_num());
                   }},
                  {"cmd_line",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushstring(L, j->cmd_line().data());
                   }},
                  {"cwd",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushstring(L, j->cwd().data());
                   }},
                  {"username",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushstring(L, j->username().data());
                   }},
                  {"qos",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushstring(L, j->qos().data());
                   }},
    {"req_res_view ", [](lua_State* L, const crane::grpc::TaskInfo* j) {
         PushResourceView_(L, static_cast<ResourceView>(j->req_res_view()));
          }},
                  {"req_nodes",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushstring(L, absl::StrJoin(j->req_nodes(), ",").data());
                   }},
                  {"exclude_nodes",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushstring(L, absl::StrJoin(j->exclude_nodes(), ",").data());
                   }},
                  {"extra_attr",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushstring(L, j->extra_attr().data());
                   }},
                  {"reservation",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushstring(L, j->reservation().data());
                   }},
                  {"held", [](lua_State* L,
                              const crane::grpc::TaskInfo*
                                  j) {
         lua_pushboolean(L, j->held()); }},
                  {"status",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {}},
                  {"exit_code",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushnumber(L, j->exit_code());
                   }},
                  {"priority",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushnumber(L, j->priority());
                   }},
                  {"execution_node",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushstring(L, absl::StrJoin(j->execution_node(), ",").data());
                   }},
                  {"exclusive",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushboolean(L, j->exclusive());
                   }},
              };

  auto it = handlers.find(name);
  if (it != handlers.end()) {
    it->second(lua_state, job_ptr);
  } else {
    lua_pushnil(lua_state);
  }
  return 1;
}

int LuaJobHandler::ResvField_(lua_State* lua_state,
                              crane::grpc::ReservationInfo* resv_ptr,
                              const char* name) {
  if (!resv_ptr) {
    CRANE_ERROR("_resv_rec_field: resv_ptr is NULL");
    lua_pushnil(lua_state);
    return 1;
  }

  using Handler =
      std::function<void(lua_State*, const crane::grpc::ReservationInfo*)>;
  static const std::unordered_map<std::string, Handler> handlers = {
      // reservation_name
      {"reservation_name",
       [](lua_State* L, const crane::grpc::ReservationInfo* r) {
         lua_pushstring(L, r->reservation_name().c_str());
       }},
      // start_time (protobuf Timestamp)
      {"start_time",
       [](lua_State* L, const crane::grpc::ReservationInfo* r) {
         lua_pushnumber(L, r->start_time().seconds());
       }},
      // duration (protobuf Duration)
      {"duration",
       [](lua_State* L, const crane::grpc::ReservationInfo* r) {
         lua_pushnumber(L, r->duration().seconds());
       }},
      // partition
      {"partition",
       [](lua_State* L, const crane::grpc::ReservationInfo* r) {
         lua_pushstring(L, r->partition().c_str());
       }},
      // craned_regex
      {"craned_regex",
       [](lua_State* L, const crane::grpc::ReservationInfo* r) {
         lua_pushstring(L, r->craned_regex().c_str());
       }},
      // res_total (ResourceView)
      {"res_total",
       [](lua_State* L, const crane::grpc::ReservationInfo* r) {
         PushResourceView_(L, static_cast<ResourceView>(r->res_total()));
       }},
      // res_avail (ResourceView)
      {"res_avail",
       [](lua_State* L, const crane::grpc::ReservationInfo* r) {
         PushResourceView_(L, static_cast<ResourceView>(r->res_avail()));
       }},
      // res_alloc (ResourceView)
      {"res_alloc",
       [](lua_State* L, const crane::grpc::ReservationInfo* r) {
         PushResourceView_(L, static_cast<ResourceView>(r->res_alloc()));
       }},
      // allowed_accounts
      {"allowed_accounts",
       [](lua_State* L, const crane::grpc::ReservationInfo* r) {
         lua_newtable(L);
         int idx = 1;
         for (const auto& acc : r->allowed_accounts()) {
           lua_pushstring(L, acc.c_str());
           lua_seti(L, -2, idx++);
         }
       }},
      // denied_accounts
      {"denied_accounts",
       [](lua_State* L, const crane::grpc::ReservationInfo* r) {
         lua_newtable(L);
         int idx = 1;
         for (const auto& acc : r->denied_accounts()) {
           lua_pushstring(L, acc.c_str());
           lua_seti(L, -2, idx++);
         }
       }},
      // allowed_users
      {"allowed_users",
       [](lua_State* L, const crane::grpc::ReservationInfo* r) {
         lua_newtable(L);
         int idx = 1;
         for (const auto& user : r->allowed_users()) {
           lua_pushstring(L, user.c_str());
           lua_seti(L, -2, idx++);
         }
       }},
      // denied_users
      {"denied_users",
       [](lua_State* L, const crane::grpc::ReservationInfo* r) {
         lua_newtable(L);
         int idx = 1;
         for (const auto& user : r->denied_users()) {
           lua_pushstring(L, user.c_str());
           lua_seti(L, -2, idx++);
         }
       }},
  };

  auto it = handlers.find(name);
  if (it != handlers.end()) {
    it->second(lua_state, resv_ptr);
  } else {
    lua_pushnil(lua_state);
  }
  return 1;
}

void LuaJobHandler::PushResourceView_(lua_State* L, const ResourceView& res) {
  lua_newtable(L);  // ResourceView table

  // allocatable_res
  lua_newtable(L);
  const auto& ar = res.GetAllocatableRes();
  lua_pushnumber(L, ar.CpuCount());
  lua_setfield(L, -2, "cpu_core_limit");
  lua_pushnumber(L, ar.memory_bytes);
  lua_setfield(L, -2, "memory_limit_bytes");
  lua_pushnumber(L, ar.memory_sw_bytes);
  lua_setfield(L, -2, "memory_sw_limit_bytes");
  lua_setfield(L, -2,
               "allocatable_res");  // ResourceView.allocatable_res = table

  // device_map
  lua_newtable(L);
  for (const auto& dev_pair : res.GetDeviceMap()) {
    const std::string& device_name = dev_pair.first;
    const auto& untyped_and_type_map = dev_pair.second;
    uint64_t untyped_req_count = untyped_and_type_map.first;
    const auto& type_count_map = untyped_and_type_map.second;

    lua_pushstring(L, device_name.c_str());
    lua_newtable(L);  // device entry table

    // untyped_req_count
    lua_pushnumber(L, static_cast<lua_Number>(untyped_req_count));
    lua_setfield(L, -2, "untyped_req_count");

    // type_count_map
    lua_newtable(L);
    for (const auto& type_pair : type_count_map) {
      lua_pushstring(L, type_pair.first.c_str());
      lua_pushnumber(L, static_cast<lua_Number>(type_pair.second));
      lua_settable(L, -3);
    }
    lua_setfield(L, -2, "type_count_map");

    lua_settable(L, -3);  // device_map[device_name] = device entry table
  }

  //  device_map table
  lua_setfield(L, -2, "device_map");  // parent_table.device_map = device_map
}
#endif

}  // namespace Ctld

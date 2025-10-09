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

#include "JobSubmitLua.h"

#include "TaskScheduler.h"

namespace Ctld {

int LogLuaMsg(lua_State* lua_state) {
  std::string prefix = "lua";
  int level = 0;
  std::string msg;

  /*
   *  Optional numeric prefix indicating the log level
   *  of the message.
   */

  /* Pop message off the lua stack */
  msg = lua_tostring(lua_state, -1);
  lua_pop(lua_state, 1);

  /* Pop level off stack: */
  level = static_cast<int>(lua_tonumber(lua_state, -1));
  lua_pop(lua_state, 1);

  /* Call appropriate slurm log function based on log-level argument */
  if (level > 4)
    CRANE_TRACE("{}: {}", prefix, msg);
  else if (level == 4)
    CRANE_TRACE("{}: {}", prefix, msg);
  else if (level == 3)
    CRANE_DEBUG("{}: {}", prefix, msg);
  else if (level == 2)
    CRANE_DEBUG("{}: {}", prefix, msg);
  else if (level == 1)
    CRANE_DEBUG("{}: {}", prefix, msg);
  else if (level == 0)
    CRANE_INFO("{}: {}", prefix, msg);

  return (0);
}

int LogLuaError(lua_State* lua_state) {
  std::string prefix = "lua";
  std::string msg = lua_tostring(lua_state, -1);
  CRANE_ERROR("{}: {}", prefix, msg);

  return (0);
}

int TimeStr2Mins(lua_State* lua_state) {
  std::string time = lua_tostring(lua_state, -1);
  int minutes = 0;
  // TODO: TimeStr2Mins
  // int minutes = crane::TimeStr2Mins(time);
  lua_pushnumber(lua_state, minutes);
  return 1;
}

int GetQosPriority(lua_State* lua_state) {
  std::string qos_name = lua_tostring(lua_state, -1);

  auto qos = g_account_manager->GetExistedQosInfo(qos_name);
  if (!qos) {
    CRANE_ERROR("Invalid QOS name:", qos_name);
    return 0;
  }

  lua_pushnumber(lua_state, qos->priority);
  return 1;
}

CraneExpectedRich<void> JobSubmitLua::JobSubmit(TaskInCtld& task) {
  CraneExpectedRich<void> result{};

  if (!LoadLuaScript_())
    return std::unexpected(FormatRichErr(CraneErrCode::ERR_SYSTEM_ERR, ""));

  /*
   *  All lua script functions should have been verified during
   *   initialization:
   */
  lua_getglobal(m_lua_state_, "crane_job_submit");
  if (lua_isnil(m_lua_state_, -1))
    return std::unexpected(FormatRichErr(CraneErrCode::ERR_SYSTEM_ERR, ""));

  UpdateJobGloable_();
  UpdateJobResvGloable_();
  PushJobDesc_(&task);
  PushPartitionList_(task.Username(), task.account);
  lua_pushnumber(m_lua_state_, task.uid);

  int rc = CraneErrCode::ERR_LUA_FAILED;
  if (lua_pcall(m_lua_state_, 3, 1, 0) != 0) {
    CRANE_ERROR("{}: {}", m_lua_script_, lua_tostring(m_lua_state_, -1));
  } else {
    if (lua_isnumber(m_lua_state_, -1)) {
      rc = lua_tonumber(m_lua_state_, -1);
    } else {
      CRANE_INFO("{}/lua: {}: non-numeric return code", __func__,
                 m_lua_script_);
    }
    lua_pop(m_lua_state_, 1);
  }
  std::string user_msg;
  if (!m_user_msg_.empty()) {
    CRANE_TRACE("lua user_msg: {}", m_user_msg_);
    user_msg = m_user_msg_;
    m_user_msg_.clear();
  }

  if (rc != 0)
    return std::unexpected(
        FormatRichErr(static_cast<CraneErrCode>(rc), user_msg));

  return result;
}

CraneExpectedRich<void> JobSubmitLua::JobModify(TaskInCtld& task_in_ctld) {
  CraneExpectedRich<void> result;

  crane::grpc::TaskInfo task_info;
  task_in_ctld.SetFieldsOfTaskInfo(&task_info);

  if (!LoadLuaScript_())
    return std::unexpected(FormatRichErr(CraneErrCode::ERR_SYSTEM_ERR, ""));

  /*
   *  All lua script functions should have been verified during
   *   initialization:
   */
  lua_getglobal(m_lua_state_, "slurm_job_modify");
  if (lua_isnil(m_lua_state_, -1))
    return std::unexpected(FormatRichErr(CraneErrCode::ERR_SYSTEM_ERR, ""));

  UpdateJobGloable_();
  UpdateJobResvGloable_();

  PushJobDesc_(&task_in_ctld);
  PushJobRec(&task_info);
  PushPartitionList_(task_in_ctld.Username(), task_in_ctld.account);
  lua_pushnumber(m_lua_state_, task_in_ctld.uid);

  int rc = CraneErrCode::ERR_LUA_FAILED;
  if (lua_pcall(m_lua_state_, 4, 1, 0) != 0 != 0) {
    CRANE_ERROR("{}: {}", m_lua_script_, lua_tostring(m_lua_state_, -1));
  } else {
    if (lua_isnumber(m_lua_state_, -1)) {
      rc = lua_tonumber(m_lua_state_, -1);
    } else {
      CRANE_INFO("{}/lua: {}: non-numeric return code", __func__,
                 m_lua_script_);
    }
    lua_pop(m_lua_state_, 1);
  }
  std::string user_msg;
  if (!m_user_msg_.empty()) {
    CRANE_TRACE("lua user_msg: {}", m_user_msg_);
    user_msg = m_user_msg_;
    m_user_msg_.clear();
  }

  if (rc != 0)
    return std::unexpected(
        FormatRichErr(static_cast<CraneErrCode>(rc), user_msg));

  return result;
}

void JobSubmitLua::RegisterOutputFunctions_() {
  const char* unpack_str;

#if LUA_VERSION_NUM == 501
  unpack_str = "unpack";
#else
  unpack_str = "table.unpack";
#endif

  lua_newtable(m_lua_state_);
  LuaTableRegister_(kCraneFunctions);

  lua_pushlightuserdata(m_lua_state_, this);
  lua_pushcclosure(m_lua_state_, LogLuaUserMsgStatic_, 1);
  lua_setfield(m_lua_state_, -2, "user_msg");

  /*
   *  Create more user-friendly lua versions of Crane log functions.
   */
  std::pair<std::string, std::string> log_funcs[] = {
      {"log_error", "crane.error (string.format({}({{...}})))"},
      {"log_info", "crane.log (0, string.format({}({{...}})))"},
      {"log_verbose", "crane.log (1, string.format({}({{...}})))"},
      {"log_debug", "crane.log (2, string.format({}({{...}})))"},
      {"log_debug2", "crane.log (3, string.format({}({{...}})))"},
      {"log_debug3", "crane.log (4, string.format({}({{...}})))"},
      {"log_debug4", "crane.log (5, string.format({}({{...}})))"},
      {"log_user", "crane.user_msg (string.format({}({{...}})))"}};

  for (const auto& lf : log_funcs) {
    std::string lua_code = fmt::format(fmt::runtime(lf.second), unpack_str);
    if (luaL_loadstring(m_lua_state_, lua_code.data()) != LUA_OK) {
      CRANE_ERROR("{} load failed", lf.first);
      lua_pop(m_lua_state_, 1);
      continue;
    }
    lua_setfield(m_lua_state_, -2, lf.first.data());
  }

  RegisterOutputErrTab_();
  lua_pushnumber(m_lua_state_, CraneErrCode::ERR_LUA_FAILED);
  lua_setfield(m_lua_state_, -2, "ERROR");
  lua_pushnumber(m_lua_state_, CraneErrCode::SUCCESS);
  lua_setfield(m_lua_state_, -2, "SUCCESS");

  lua_pushnumber(m_lua_state_, crane::grpc::TaskStatus::Pending);
  lua_setfield(m_lua_state_, -2, "Pending");
  lua_pushnumber(m_lua_state_, crane::grpc::TaskStatus::Running);
  lua_setfield(m_lua_state_, -2, "Running");
  lua_pushnumber(m_lua_state_, crane::grpc::TaskStatus::Completed);
  lua_setfield(m_lua_state_, -2, "Completed");
  lua_pushnumber(m_lua_state_, crane::grpc::TaskStatus::Failed);
  lua_setfield(m_lua_state_, -2, "Failed");
  lua_pushnumber(m_lua_state_, crane::grpc::TaskStatus::Cancelled);
  lua_setfield(m_lua_state_, -2, "Cancelled");
  lua_pushnumber(m_lua_state_, crane::grpc::TaskStatus::OutOfMemory);
  lua_setfield(m_lua_state_, -2, "OutOfMemory");

  lua_pushnumber(m_lua_state_, crane::grpc::TaskType::Batch);
  lua_setfield(m_lua_state_, -2, "Batch");
  lua_pushnumber(m_lua_state_, crane::grpc::TaskType::Interactive);
  lua_setfield(m_lua_state_, -2, "Interactive");

  // TODO: all used flags

  lua_setglobal(m_lua_state_, "crane");
}

void JobSubmitLua::LuaTableRegister_(const luaL_Reg* l) {
#if LUA_VERSION_NUM == 501
  luaL_register(m_lua_state_, NULL, l);
#else
  luaL_setfuncs(m_lua_state_, l, 0);
#endif
}

void JobSubmitLua::RegisterOutputErrTab_() {
  const google::protobuf::EnumDescriptor* desc =
      crane::grpc::ErrCode_descriptor();
  for (int i = 0; i < desc->value_count(); ++i) {
    const google::protobuf::EnumValueDescriptor* vdesc = desc->value(i);
    lua_pushnumber(m_lua_state_, vdesc->number());
    lua_setfield(m_lua_state_, -2, vdesc->name().c_str());
  }
}

int JobSubmitLua::LogLuaUserMsgStatic_(lua_State* lua_state) {
  auto* self = static_cast<JobSubmitLua*>(
      lua_touserdata(lua_state, lua_upvalueindex(1)));
  return self->LogLuaUserMsg_(lua_state);
}

int JobSubmitLua::LogLuaUserMsg_(lua_State* lua_state) {
  std::string msg = lua_tostring(lua_state, -1);

  if (!m_user_msg_.empty()) {
    m_user_msg_ = fmt::format("{}\n{}", m_user_msg_, msg);
  } else {
    m_user_msg_ = msg;
  }

  return (0);
}

void JobSubmitLua::RegisterLuaCraneStructFunctions_(lua_State* lua_state) {
  lua_pushcfunction(lua_state, GetJobEnvFieldName_);
  lua_setglobal(lua_state, "_get_job_env_field_name");
  lua_pushcfunction(lua_state, GetJobReqFieldName_);
  lua_setglobal(lua_state, "_get_job_req_field_name");
  lua_pushcfunction(lua_state, SetJobEnvField_);
  lua_setglobal(lua_state, "_set_job_env_field");
  lua_pushcfunction(lua_state, SetJobReqField_);
  lua_setglobal(lua_state, "_set_job_req_field");
  lua_pushcfunction(lua_state, GetPartRecFieldName_);
  lua_setglobal(lua_state, "_get_part_rec_field");
}

int JobSubmitLua::GetJobEnvFieldName_(lua_State* lua_state) {
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

int JobSubmitLua::GetJobReqFieldName_(lua_State* lua_state) {
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

int JobSubmitLua::SetJobEnvField_(lua_State* lua_state) {
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

int JobSubmitLua::SetJobReqField_(lua_State* lua_state) {
  const auto* name = luaL_checkstring(lua_state, 2);
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
  if (name == "container") {
    const char* value_str = luaL_checkstring(lua_state, 3);
    job_desc->container = value_str;
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

  // if (name == "env") {
  //   if (lua_istable(lua_state, 3)) {
  //     lua_pushnil(lua_state);
  //     while (lua_next(lua_state, 3) != 0) {
  //       const char* key = luaL_checkstring(lua_state, -2);
  //       const char* value = luaL_checkstring(lua_state, -1);
  //       job_desc->env[key] = value;
  //       lua_pop(lua_state, 1);
  //     }
  //   }
  // }

  return 0;
}

// partition_record
int JobSubmitLua::GetPartRecFieldName_(lua_State* lua_state) {
  const auto* partition_meta =
      static_cast<const PartitionMeta*>(lua_touserdata(lua_state, 1));
  const char* name = luaL_checkstring(lua_state, 2);
  if (partition_meta == nullptr) {
    CRANE_ERROR("partition_meta is nill");
    lua_pushnil(lua_state);
    return 1;
  }

  return GetPartRecField_(*partition_meta, name, lua_state);
}

int JobSubmitLua::GetJobEnvField_(const TaskInCtld& job_desc, const char* name,
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

int JobSubmitLua::GetJobReqField_(const TaskInCtld& job_desc, const char* name,
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
      {"container",
       [](lua_State* L, const TaskInCtld& t) {
         lua_pushstring(L, t.container.data());
       }},
      {"reservation",
       [](lua_State* L, const TaskInCtld& t) {
         lua_pushstring(L, t.reservation.data());
       }},
      // {"begin_time",
      //  [](lua_State* L, const TaskInCtld& t) {
      //    lua_pushnumber(L, absl::ToDoubleSeconds(t.begin_time));
      //  }},
      {"meta",
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
         } else if (auto* ptr = std::get_if<InteractiveMetaInTask>(&t.meta)) {
           lua_newtable(L);

           lua_pushnumber(L, ptr->interactive_type);
           lua_setfield(L, -2, "interactive_type");

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

int JobSubmitLua::GetPartRecField_(const PartitionMeta& partition_meta,
                                   const char* name, lua_State* lua_state) {
  using Handler = std::function<void(lua_State*, const PartitionMeta&)>;
  static const std::unordered_map<std::string, Handler> handlers = {
      {"name",
       [](lua_State* L, const PartitionMeta& p) {
         lua_pushstring(L, p.partition_global_meta.name.c_str());
       }},
      {"nodelist_str",
       [](lua_State* L, const PartitionMeta& p) {
         lua_pushstring(L, p.partition_global_meta.nodelist_str.c_str());
       }},
      {"node_cnt",
       [](lua_State* L, const PartitionMeta& p) {
         lua_pushinteger(L, p.partition_global_meta.node_cnt);
       }},
      {"alive_craned_cnt",
       [](lua_State* L, const PartitionMeta& p) {
         lua_pushinteger(L, p.partition_global_meta.alive_craned_cnt);
       }},
      {"allowed_accounts",
       [](lua_State* L, const PartitionMeta& p) {
         lua_newtable(L);
         int idx = 1;
         for (const auto& acc : p.partition_global_meta.allowed_accounts) {
           lua_pushstring(L, acc.c_str());
           lua_seti(L, -2, idx++);
         }
       }},
      {"denied_accounts",
       [](lua_State* L, const PartitionMeta& p) {
         lua_newtable(L);
         int idx = 1;
         for (const auto& acc : p.partition_global_meta.denied_accounts) {
           lua_pushstring(L, acc.c_str());
           lua_seti(L, -2, idx++);
         }
       }},
      {"craned_ids",
       [](lua_State* L, const PartitionMeta& p) {
         lua_newtable(L);
         int idx = 1;
         for (const auto& id : p.craned_ids) {
           lua_pushstring(L, id.c_str());  // 假设 CranedId 可转为 string
           lua_seti(L, -2, idx++);
         }
       }},
      {"res_total",
       [](lua_State* L, const PartitionMeta& p) {
         PushResourceView_(L, p.partition_global_meta.res_total);
       }},
      {"res_avail",
       [](lua_State* L, const PartitionMeta& p) {
         PushResourceView_(L, p.partition_global_meta.res_avail);
       }},
      {"res_in_use",
       [](lua_State* L, const PartitionMeta& p) {
         PushResourceView_(L, p.partition_global_meta.res_in_use);
       }},
      {"res_total_inc_dead",
       [](lua_State* L, const PartitionMeta& p) {
         PushResourceView_(L, p.partition_global_meta.res_total_inc_dead);
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

bool JobSubmitLua::LoadLuaScript_() {
  if (!m_lua_state_) {
    CRANE_ERROR(
        "Lua state (m_lua_state_) is null when loading script '{}'. "
        "This usually indicates Lua VM initialization failed.",
        m_lua_script_);
    return false;
  }

  if (luaL_loadfile(m_lua_state_, m_lua_script_.data())) {
    CRANE_ERROR("luaL_loadfile failed.");
    lua_close(m_lua_state_);
    return false;
  }

  if (lua_pcall(m_lua_state_, 0, 1, 0)) {
    CRANE_ERROR("{}:{}", m_lua_script_, lua_tostring(m_lua_state_, -1));
    return false;
  }
  int rc = (int)lua_tonumber(m_lua_state_, -1);
  if (rc) {
    CRANE_ERROR("{}: returned {} on load", m_lua_script_, rc);
    return false;
  }

  if (!CheckLuaScriptFunctions_(m_lua_state_, m_lua_script_, g_req_fxns)) {
    CRANE_ERROR("{}: required function(s) not present", m_lua_script_);
    return false;
  }

  return true;
}

bool JobSubmitLua::CheckLuaScriptFunction_(lua_State* lua_state,
                                           const char* name) {
  bool result = true;
  lua_getglobal(lua_state, name);
  if (!lua_isfunction(lua_state, -1)) result = false;
  lua_pop(lua_state, -1);
  return result;
}

bool JobSubmitLua::CheckLuaScriptFunctions_(lua_State* lua_state,
                                            const std::string& script_pash,
                                            const char** req_fxns) {
  bool result = true;
  const char** ptr = nullptr;
  for (ptr = req_fxns; ptr && *ptr; ptr++) {
    if (!CheckLuaScriptFunction_(lua_state, *ptr)) {
      CRANE_ERROR("{}: missing required function {}", script_pash, *ptr);
      result = false;
    }
  }
  return result;
}

void JobSubmitLua::UpdateJobGloable_() {
  lua_getglobal(m_lua_state_, "crane");
  lua_newtable(m_lua_state_);

  crane::grpc::QueryTasksInfoRequest request;
  crane::grpc::QueryTasksInfoReply response;
  g_task_scheduler->QueryTasksInRam(&request, &response);
  m_job_info_list_.clear();
  m_job_info_list_.reserve(response.task_info_list_size());
  for (const auto& task : response.task_info_list()) {
    auto task_ptr = std::make_shared<crane::grpc::TaskInfo>(task);
    m_job_info_list_.emplace_back(task_ptr);
    /*
     * Create an empty table, with a metatable that looks up the
     * data for the individual job.
     */
    lua_newtable(m_lua_state_);

    lua_newtable(m_lua_state_);
    lua_pushcfunction(m_lua_state_, JobRecFieldIndex_);
    lua_setfield(m_lua_state_, -2, "__index");
    /*
     * Store the job_record in the metatable, so the index
     * function knows which job it's getting data for.
     */
    lua_pushlightuserdata(m_lua_state_, task_ptr.get());
    lua_setfield(m_lua_state_, -2, "_job_rec_ptr");
    lua_setmetatable(m_lua_state_, -2);

    /* Lua copies passed strings, so we can reuse the buffer. */
    lua_setfield(m_lua_state_, -2, std::to_string(task.task_id()).data());
  }

  lua_setfield(m_lua_state_, -2, "jobs");
  lua_pop(m_lua_state_, 1);
}

void JobSubmitLua::UpdateJobResvGloable_() {
  lua_getglobal(m_lua_state_, "crane");
  lua_newtable(m_lua_state_);

  // TODO: push all reservation info

  lua_setfield(m_lua_state_, -2, "reservations");
  lua_pop(m_lua_state_, 1);
}

void JobSubmitLua::PushJobDesc_(TaskInCtld* task) {
  lua_newtable(m_lua_state_);

  lua_newtable(m_lua_state_);
  lua_pushcfunction(m_lua_state_, GetJobReqFieldIndex_);
  lua_setfield(m_lua_state_, -2, "__index");
  lua_pushcfunction(m_lua_state_, SetJobReqField_);
  lua_setfield(m_lua_state_, -2, "__newindex");
  /* Store the job descriptor in the metatable, so the index
   * function knows which struct it's getting data for.
   */
  lua_pushlightuserdata(m_lua_state_, task);
  lua_setfield(m_lua_state_, -2, "_job_desc");
  lua_setmetatable(m_lua_state_, -2);
}

// TODO: all is lock ?
void JobSubmitLua::PushPartitionList_(const std::string& user_name,
                                      const std::string& account) {
  lua_newtable(m_lua_state_);

  std::string actual_account = account;
  if (actual_account.empty()) {
    auto user = g_account_manager->GetExistedUserInfo(user_name);
    if (!user) {
      CRANE_ERROR("username is null");
      return;
    }
    actual_account = user->default_account;
  }

  auto partition_map = g_meta_container->GetAllPartitionsMetaMapConstPtr();
  for (const auto& [partition_name, partition_meta] : *partition_map) {
    auto partition = partition_meta.GetExclusivePtr();
    if (!partition->partition_global_meta.allowed_accounts.empty() &&
        !partition->partition_global_meta.allowed_accounts.contains(
            actual_account))
      continue;
    lua_newtable(m_lua_state_);

    lua_newtable(m_lua_state_);

    lua_pushcfunction(m_lua_state_, PartitionRecFieldIndex_);
    lua_setfield(m_lua_state_, -2, "__index");
    /*
     * Store the part_record in the metatable, so the index
     * function knows which job it's getting data for.
     */
    lua_pushlightuserdata(m_lua_state_,
                          const_cast<PartitionMeta*>(partition.get()));
    lua_setfield(m_lua_state_, -2, "_part_rec_ptr");
    lua_setmetatable(m_lua_state_, -2);

    lua_setfield(m_lua_state_, -2, partition_name.data());
  }
}

void JobSubmitLua::PushJobRec(crane::grpc::TaskInfo* task) {
  lua_newtable(m_lua_state_);

  lua_newtable(m_lua_state_);
  lua_pushcfunction(m_lua_state_, JobRecFieldIndex_);
  lua_setfield(m_lua_state_, -2, "__index");
  /* Store the job_ptr in the metatable, so the index
   * function knows which struct it's getting data for.
   */
  lua_pushlightuserdata(m_lua_state_, task);
  lua_setfield(m_lua_state_, -2, "_job_rec_ptr");
  lua_setmetatable(m_lua_state_, -2);
}

int JobSubmitLua::GetJobReqFieldIndex_(lua_State* lua_state) {
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

int JobSubmitLua::JobRecFieldIndex_(lua_State* lua_state) {
  const char* name = luaL_checkstring(lua_state, 2);
  crane::grpc::TaskInfo* job_ptr;

  lua_getmetatable(lua_state, -2);
  lua_getfield(lua_state, -1, "_job_rec_ptr");
  job_ptr = static_cast<crane::grpc::TaskInfo*>(lua_touserdata(lua_state, -1));

  return luaJobRecordField_(lua_state, job_ptr, name);
}

int JobSubmitLua::PartitionRecFieldIndex_(lua_State* lua_state) {
  const char* name = luaL_checkstring(lua_state, 2);
  PartitionMeta* part_ptr;

  lua_getmetatable(lua_state, -2);
  lua_getfield(lua_state, -1, "_part_rec_ptr");
  part_ptr = static_cast<PartitionMeta*>(lua_touserdata(lua_state, -1));
  if (!part_ptr) {
    CRANE_ERROR("part_ptr is NULL");
    lua_pushnil(lua_state);
    return 1;
  }

  return GetPartRecField_(*part_ptr, name, lua_state);
}

int JobSubmitLua::luaJobRecordField_(lua_State* lua_state,
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
         const auto& res = j->req_res_view();
         lua_newtable(L);  // ResourceView table

         // allocatable_res
         lua_newtable(L);
         const auto& ar = res.allocatable_res();
         lua_pushnumber(L, ar.cpu_core_limit());
         lua_setfield(L, -2, "cpu_core_limit");
         lua_pushnumber(L, ar.memory_limit_bytes());
         lua_setfield(L, -2, "memory_limit_bytes");
         lua_pushnumber(L, ar.memory_sw_limit_bytes());
         lua_setfield(L, -2, "memory_sw_limit_bytes");
         lua_setfield(L, -2, "allocatable_res");

         // device_map
         lua_newtable(L);
         const auto& dm = res.device_map();

         // name_type_map
         lua_newtable(L);
         const auto& name_type_map = dm.name_type_map();
         for (const auto& dev_pair : name_type_map) {
           const std::string& device_name = dev_pair.first;
           const auto& type_count_map = dev_pair.second;

           lua_pushstring(L, device_name.c_str());
           lua_newtable(L);  // TypeCountMap table

           // type_count_map: map<string, uint64>
           const auto& tcm = type_count_map.type_count_map();
           for (const auto& type_pair : tcm) {
             lua_pushstring(L, type_pair.first.c_str());  // type
             lua_pushnumber(
                 L, static_cast<lua_Number>(type_pair.second));  // count
             lua_settable(L, -3);
           }
           // total
           lua_pushnumber(L, static_cast<lua_Number>(type_count_map.total()));
           lua_setfield(L, -2, "total");

           lua_settable(L,
                        -3);  // name_type_map[device_name] = TypeCountMap table
         }
         lua_setfield(L, -2, "name_type_map");
         lua_setfield(L, -2, "device_map");
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
                  {"container",
                   [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushstring(L, j->container().data());
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

void JobSubmitLua::PushResourceView_(lua_State* L, const ResourceView& res) {
  lua_newtable(L);  // 创建 ResourceView table

  // allocatable_res 字段
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

  // device_map 字段
  lua_newtable(L);
  const auto& dm = ToGrpcDeviceMap(res.GetDeviceMap());

  // name_type_map 字段
  lua_newtable(L);
  const auto& name_type_map = dm.name_type_map();
  for (const auto& dev_pair : name_type_map) {
    const std::string& device_name = dev_pair.first;
    const auto& type_count_map = dev_pair.second;

    lua_pushstring(L, device_name.c_str());
    lua_newtable(L);  // TypeCountMap table

    // type_count_map: map<string, uint64>
    const auto& tcm = type_count_map.type_count_map();
    for (const auto& type_pair : tcm) {
      lua_pushstring(L, type_pair.first.c_str());                    // type
      lua_pushnumber(L, static_cast<lua_Number>(type_pair.second));  // count
      lua_settable(L, -3);
    }
    // total
    lua_pushnumber(L, static_cast<lua_Number>(type_count_map.total()));
    lua_setfield(L, -2, "total");

    lua_settable(L, -3);  // name_type_map[device_name] = TypeCountMap table
  }
  lua_setfield(L, -2, "name_type_map");
  lua_setfield(L, -2, "device_map");  // ResourceView.device_map = table
}

}  // namespace Ctld

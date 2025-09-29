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
  level = (int)lua_tonumber(lua_state, -1);
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
  // TODO:
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

  crane::grpc::TaskInfo task_info;
  task.SetFieldsOfTaskInfo(&task_info);

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
  PushJobInfo_(&task_info);
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

  PushJobInfo_(&task_info);
  // PushJobRec(&task_info);
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

  // TODO: all used flags
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
      static_cast<const crane::grpc::TaskInfo*>(lua_touserdata(lua_state, 1));
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
      static_cast<const crane::grpc::TaskInfo*>(lua_touserdata(lua_state, 1));
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

  // TODO: set task map job field

  return 0;
}

int JobSubmitLua::GetPartRecFieldName_(lua_State* lua_state) {
  const auto* partition_meta =
      static_cast<PartitionMeta*>(lua_touserdata(lua_state, 1));
  const char* name = luaL_checkstring(lua_state, 2);
  if (partition_meta == nullptr) {
    CRANE_ERROR("partition_meta is nill");
    lua_pushnil(lua_state);
    return 1;
  }

  return GetPratRecField_(*partition_meta, name, lua_state);
}

int JobSubmitLua::GetJobEnvField_(const crane::grpc::TaskInfo& job_desc, const char* name,
                                  lua_State* lua_state) {
  if (job_desc.env().empty()) {
    if (job_desc.type() == crane::grpc::TaskType::Batch) {
      CRANE_ERROR("job_desc->environment is nullptr.");
    } else {
      CRANE_INFO("job_desc->environment only accessible for batch jobs.");
    }
    lua_pushnil(lua_state);
  } else {
    auto iter = job_desc.env().find(name);
    if (iter != job_desc.env().end()) {
      lua_pushstring(lua_state, iter->second.data());
    } else {
      lua_pushnil(lua_state);
    }
  }

  return 1;
}

int JobSubmitLua::GetJobReqField_(const crane::grpc::TaskInfo& job_desc, const char* name,
                                  lua_State* lua_state) {
  using Handler = std::function<void(lua_State*, const crane::grpc::TaskInfo&)>;

  static const std::unordered_map<std::string, Handler> handlers = {
      // ----------- [1] Fields set at submission time ----------
      {"time_limit",
       [](lua_State* L, const crane::grpc::TaskInfo& t) {
         // lua_pushnumber(L, absl::ToInt64Seconds(t.time_limit()));
       }},
      {"partition_id",
       [](lua_State* L, const crane::grpc::TaskInfo& t) {
         lua_pushstring(L, t.partition().data());
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

int JobSubmitLua::GetPratRecField_(const PartitionMeta& partition_meta,
                                   const char* name, lua_State* lua_state) {
  // TODO: lua_pushstring partition_meta field

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
  const char** ptr = NULL;
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
void JobSubmitLua::PushJobInfo_(crane::grpc::TaskInfo* task) {
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

void JobSubmitLua::PushPartitionList_(const std::string& user_name, const std::string& account) {
  lua_newtable(m_lua_state_);

  std::string actual_account = account;
  if (actual_account.empty()) {
    auto user = g_account_manager->GetExistedUserInfo(user_name);
    actual_account = user->default_account;
  }

  auto partition_map = g_meta_container->GetAllPartitionsMetaMapConstPtr();
  for (const auto& [partition_name, partition_meta] : *partition_map) {
    auto partition = partition_meta.GetExclusivePtr();
    if (!partition->partition_global_meta.allowed_accounts.empty() &&
      !partition->partition_global_meta.allowed_accounts.contains(actual_account))
      continue;
    lua_newtable(m_lua_state_);

    lua_newtable(m_lua_state_);

    lua_pushcfunction(m_lua_state_, PartitionRecFieldIndex_);
    lua_setfield(m_lua_state_, -2, "__index");
    /*
     * Store the part_record in the metatable, so the index
     * function knows which job it's getting data for.
     */
    lua_pushlightuserdata(m_lua_state_, const_cast<PartitionMeta*>(partition.get()));
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
  crane::grpc::TaskInfo *job_desc = static_cast<crane::grpc::TaskInfo*>(lua_touserdata(lua_state, -1));

  return luaJobRecordField_(lua_state, job_desc, name);
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
  const char *name = luaL_checkstring(lua_state, 2);
  PartitionMeta *part_ptr;

  lua_getmetatable(lua_state, -2);
  lua_getfield(lua_state, -1, "_part_rec_ptr");
  part_ptr = static_cast<PartitionMeta*>(lua_touserdata(lua_state, -1));
  if (!part_ptr) {
    CRANE_ERROR("part_ptr is NULL");
    lua_pushnil(lua_state);
    return 1;
  }

  return PartitionRecField_(lua_state, *part_ptr, name);
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
  static const std::unordered_map<std::string, Handler> handlers = {
      {"account",
       [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushstring(L, j->account().data());
       }},
      {"alloc_node",
       [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushstring(L, j->req_nodes(0).data());
       }},
      {"container",
       [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushstring(L, j->container().data());
       }},
      {"cpus_per_tres",
       [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushstring(
             L, std::to_string(
                    j->req_res_view().allocatable_res().cpu_core_limit())
                    .data());
       }},
      {"exit_code",
       [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushnumber(L, j->exit_code());
       }},
      {"extra",
       [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushstring(L, j->extra_attr().data());
       }},
      {"group_id",
       [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushnumber(L, j->gid());
       }},
      {"job_id",
       [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushnumber(L, j->task_id());
       }},
      {"job_state",
       [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushnumber(L, j->status());
       }},
      {"mem_per_tres",
       [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushnumber(
             L, j->req_res_view().allocatable_res().memory_limit_bytes());
       }},
      {"name",
       [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushstring(L, j->name().data());
       }},
      {"nodes",
       [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushstring(L, j->craned_list().data());
       }},
      {"partition",
       [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushstring(L, j->partition().data());
       }},
      {"priority",
       [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushnumber(L, j->priority());
       }},
      {"qos",
       [](lua_State* L, const crane::grpc::TaskInfo* j) {
         lua_pushstring(L, j->qos().data());
       }},

      // {"admin_comment", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->admin_comment().data()); }},
      // {"details", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnil(L); /* 复杂结构，建议后续补充 */ }},
      // {"array_job_id", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->array_job_id()); }},
      // {"array_recs", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnil(L); }},
      // {"array_task_id", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->array_task_id()); }},
      // {"batch_features", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->batch_features().data()); }},
      // {"batch_host", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->batch_host().data()); }},
      // {"best_switch", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->best_switch()); }},
      // {"burst_buffer", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->burst_buffer().data()); }},
      // {"comment", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->comment().data()); }},
      // {"delay_boot", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->delay_boot()); }},
      // {"curr_dependency", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->curr_dependency().data()); }},
      // {"orig_dependency", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->orig_dependency().data()); }},
      // {"derived_ec", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->derived_ec()); }},
      // {"direct_set_prio", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->direct_set_prio()); }},
      // {"end_time", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->end_time()); }},
      // {"features", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->features().data()); }},
      // {"gres", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->req_res_view().device_map()); }},
      // {"gres_req", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->gres_req().data()); }},
      // {"gres_used", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->gres_used().data()); }},
      // {"licenses", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->licenses().data()); }},
      // {"max_cpus", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->max_cpus()); }},
      // {"max_nodes", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->max_nodes()); }},
      // {"mcs_label", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->mcs_label().data()); }},
      // {"min_cpus", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->min_cpus()); }},
      // {"min_mem_per_node", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->min_mem_per_node()); }},
      // {"min_mem_per_cpu", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->min_mem_per_cpu()); }},
      // {"min_nodes", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->min_nodes()); }},
      // {"nice", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->nice()); }},
      // {"origin_cluster", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->origin_cluster().data()); }},
      // {"pack_job_id", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->pack_job_id()); }},
      // {"het_job_id", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->het_job_id()); }},
      // {"pack_job_id_set", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->pack_job_id_set().data()); }},
      // {"het_job_id_set", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->het_job_id_set().data()); }},
      // {"het_job_offset", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->het_job_offset()); }},
      // {"pn_min_cpus", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->pn_min_cpus()); }},
      // {"reboot", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->reboot()); }},
      // {"req_switch", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->req_switch()); }},
      // {"resizing", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->resizing()); }},
      // {"restart_cnt", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->restart_cnt()); }},
      // {"resv_name", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->resv_name().data()); }},
      // {"script", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->script().data()); }},
      // {"selinux_context", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->selinux_context().data()); }},
      // {"site_factor", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->site_factor()); }},
      // {"spank_job_env", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnil(L); }},
      // {"spank_job_env_size", [](lua_State* L, const crane::grpc::TaskInfo* j)
      // { lua_pushnumber(L, j->spank_job_env_size()); }},
      // {"start_time", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->start_time()); }},
      // {"std_err", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->std_err().data()); }},
      // {"std_in", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->std_in().data()); }},
      // {"std_out", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->std_out().data()); }},
      // {"submit_time", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->submit_time()); }},
      // {"time_limit", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->time_limit()); }},
      // {"time_min", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->time_min()); }},
      // {"total_cpus", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->total_cpus()); }},
      // {"total_nodes", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->total_nodes()); }},
      // {"tres_alloc_str", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->tres_alloc_str().data()); }},
      // {"tres_bind", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->tres_bind().data()); }},
      // {"tres_fmt_alloc_str", [](lua_State* L, const crane::grpc::TaskInfo* j)
      // { lua_pushstring(L, j->tres_fmt_alloc_str().data()); }},
      // {"tres_fmt_req_str", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->tres_fmt_req_str().data()); }},
      // {"tres_freq", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->tres_freq().data()); }},
      // {"tres_per_job", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->tres_per_job().data()); }},
      // {"tres_per_node", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->tres_per_node().data()); }},
      // {"tres_per_socket", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->tres_per_socket().data()); }},
      // {"tres_per_task", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->tres_per_task().data()); }},
      // {"tres_req_str", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->tres_req_str().data()); }},
      // {"user_id", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->uid()); }},
      // {"user_name", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->username().data()); }},
      // {"wait4switch", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushnumber(L, j->wait4switch()); }},
      // {"wait4switch_start", [](lua_State* L, const crane::grpc::TaskInfo* j)
      // { lua_pushnumber(L, j->wait4switch_start()); }},
      // {"wckey", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->wckey().data()); }},
      // {"work_dir", [](lua_State* L, const crane::grpc::TaskInfo* j) {
      // lua_pushstring(L, j->work_dir().data()); }},
  };

  auto it = handlers.find(name);
  if (it != handlers.end()) {
    it->second(lua_state, job_ptr);
  } else {
    lua_pushnil(lua_state);
  }
  return 1;
}

int JobSubmitLua::PartitionRecField_(lua_State* lua_state,
                                     const PartitionMeta& partition_meta,
                                     const char* name) {

}

}  // namespace Ctld

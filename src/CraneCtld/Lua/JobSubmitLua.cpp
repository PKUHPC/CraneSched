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

namespace Ctld {

bool JobSubmitLua::Init(const std::string& lua_script) {
  if (!(m_lua_state_ = luaL_newstate())) {
    CRANE_ERROR("luaL_newstate() failed to allocate");
    return false;
  }
  luaL_openlibs(m_lua_state_);

  if (luaL_loadfile(m_lua_state_, lua_script.data())) {
    CRANE_ERROR("luaL_loadfile failed.");
    lua_close(m_lua_state_);
    return false;
  }

  RegisterOutputFunctions_();
  RegisterLuaCraneStructFunctions_(m_lua_state_);

  return true;
}

std::optional<std::string> JobSubmitLua::JobSubmit(
    const TaskInCtld &task_in_ctld) {

  /*
   *  All lua script functions should have been verified during
   *   initialization:
  */
  lua_getglobal(m_lua_state_, "crane_job_submit");
  if (lua_isnil(m_lua_state_, -1))
    return "";

  // _update_jobs_global(m_lua_state_);
  // _update_resvs_global(m_lua_state_);
  //
  // _push_job_desc(job_desc);
  // _push_partition_list(job_desc->user_id, submit_uid);
  // lua_pushnumber(m_lua_state_, submit_uid);
  // slurm_lua_stack_dump(
  //         "job_submit/lua", "job_submit, before lua_pcall", L);
  // if (lua_pcall(L, 3, 1, 0) != 0) {
  //   error("%s/lua: %s: %s",
  //         __func__, lua_script_path, lua_tostring(L, -1));
  // } else {
  //   if (lua_isnumber(L, -1)) {
  //     rc = lua_tonumber(L, -1);
  //   } else {
  //     info("%s/lua: %s: non-numeric return code",
  //          __func__, lua_script_path);
  //     rc = SLURM_SUCCESS;
  //   }
  //   lua_pop(L, 1);
  // }
  // slurm_lua_stack_dump(
  //         "job_submit/lua", "job_submit, after lua_pcall", L);
  // if (user_msg) {
  //   *err_msg = user_msg;
  //   user_msg = NULL;
  // }
}

void JobSubmitLua::RegisterOutputFunctions_() {
  const char*unpack_str;

#if LUA_VERSION_NUM == 501
  unpack_str = "unpack";
#else
  unpack_str = "table.unpack";
#endif

  lua_newtable(m_lua_state_);
  LuaTableRegister_(crane_functions);

  lua_pushlightuserdata(m_lua_state_, this);
  lua_pushcclosure(m_lua_state_, LogLuaUserMsgStatic_, 1);
  lua_setfield(m_lua_state_, -2, "user_msg");

  /*
   *  Create more user-friendly lua versions of Crane log functions.
   */
  std::pair<std::string, std::string> log_funcs[] = {
    {"log_error",   "crane.error (string.format(%s({...})))"},
    {"log_info",    "crane.log (0, string.format(%s({...})))"},
    {"log_verbose", "crane.log (1, string.format(%s({...})))"},
    {"log_debug",   "crane.log (2, string.format(%s({...})))"},
    {"log_debug2",  "crane.log (3, string.format(%s({...})))"},
    {"log_debug3",  "crane.log (4, string.format(%s({...})))"},
    {"log_debug4",  "crane.log (5, string.format(%s({...})))"},
    {"log_user",    "crane.user_msg (string.format(%s({...})))"}
  };

  for (const auto& lf : log_funcs) {
    std::string lua_code = fmt::format("{}{}", lf.second, unpack_str);
    if (luaL_loadstring(m_lua_state_, lua_code.data()) != LUA_OK) {
      lua_pop(m_lua_state_, 1);
      continue;
    }
    lua_setfield(m_lua_state_, -2, lf.first.data());
  }

  /*
 * TODO: Error codes: true, false, CraneErrCode etc.
 */
  lua_pushnumber(m_lua_state_, false);
  lua_setfield(m_lua_state_, -2, "ERROR");
  lua_pushnumber(m_lua_state_, true);
  lua_setfield(m_lua_state_, -2, "SUCCESS");
  RegisterOutputErrTab_();

  // TODO: all used flags
  /*
   * Other definitions needed to interpret data
   * slurm.MEM_PER_CPU, slurm.NO_VAL, etc.
   */

  /*
   * job_desc bitflags
   */


  lua_setglobal(m_lua_state_, "crane");
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
  lua_pushcfunction(lua_state, GetPartRecField_);
  lua_setglobal(lua_state, "_get_part_rec_field");
}

int LogLuaMsg(lua_State *lua_state) {
  std::string prefix  = "lua";
  int        level    = 0;
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
    CRANE_TRACE ("{}: {}", prefix, msg);
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

int LogLuaError(lua_State *lua_state) {
  std::string prefix  = "lua";
  std::string msg     = lua_tostring(lua_state, -1);
  CRANE_ERROR("{}: {}", prefix, msg);

  return (0);
}

int TimeStr2Mins(lua_State *lua_state) {
  std::string time = lua_tostring(lua_state, -1);
  int minutes = 0;
  // int minutes = crane::TimeStr2Mins(time);
  lua_pushnumber(lua_state, minutes);
  return 1;
}

int GetQosPriority(lua_State *lua_state) {
  std::string qos_name = lua_tostring(lua_state, -1);

  auto qos = g_account_manager->GetExistedQosInfo(qos_name);
  if (!qos) {
    CRANE_ERROR("Invalid QOS name:", qos_name);
    return 0;
  }

  lua_pushnumber(lua_state, qos->priority);
  return 1;
}


int JobSubmitLua::LogLuaUserMsgStatic_(lua_State *lua_state) {
  auto* self = static_cast<JobSubmitLua*>(lua_touserdata(lua_state, lua_upvalueindex(1)));
  return self->LogLuaUserMsg_(lua_state);
}

int JobSubmitLua::LogLuaUserMsg_(lua_State *lua_state) {
  std::string msg = lua_tostring(lua_state, -1);

  if (!m_user_msg_.empty()) {
    m_user_msg_ = fmt::format("{}\n{}", m_user_msg_, msg);
  } else {
    m_user_msg_ = msg;
  }

  return (0);
}

void JobSubmitLua::LuaTableRegister_(const luaL_Reg* l) {
#if LUA_VERSION_NUM == 501
  luaL_register(m_lua_state_, NULL, l);
#else
  luaL_setfuncs(m_lua_state_, l, 0);
#endif
}

void JobSubmitLua::RegisterOutputErrTab_() {
  // TODO: 注册所有错误码
  // for (int i = 0; i<crane::grpc::ErrCode_ARRAYSIZE; i++) {
  //   lua_pushnumber(m_lua_state_, slurm_errtab[i].xe_number);
  //   lua_setfield(L, -2, slurm_errtab[i].xe_name);
  // }
}

bool JobSubmitLua::RunScript_() {
  if (lua_pcall(m_lua_state_, 0, 1, 0)) {
    CRANE_ERROR("{}:{}", m_lua_script_, lua_tostring(m_lua_state_, -1));
    return false;
  }
  int rc = (int)lua_tonumber(m_lua_state_, -1);
  if (rc) {
    CRANE_ERROR("{}: returned {} on load", m_lua_script_, rc);
    return false;
  }

  // rc = _check_lua_script_functions(new, plugin, script_path, req_fxns);
  // if (rc != SLURM_SUCCESS) {
  //   err_str = xstrdup_printf("%s: required function(s) not present",
  //                            script_path);
  //   goto fini_error;
  // }
  return true;
}

} // namespace Ctld


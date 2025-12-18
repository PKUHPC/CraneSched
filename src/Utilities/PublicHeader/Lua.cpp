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

#include "crane/Lua.h"

#include "crane/String.h"

namespace crane {

#ifdef HAVE_LUA

const luaL_Reg LuaEnvironment::kCraneFunctions[] = {
  {"log", LogLuaMsg_},
  {"error", LogLuaError_},
  {"time_str2mins", TimeStr2Mins_},
  {nullptr, nullptr}};

bool LuaEnvironment::Init(const std::string& script) {
  m_lua_script_ = script;
  if ((m_lua_state_ = luaL_newstate()) == nullptr) {
    CRANE_ERROR("luaL_newstate() failed to allocate");
    return false;
  }

  luaL_openlibs(m_lua_state_);

  RegisterFunctions_();
  return true;
}

void LuaEnvironment::LuaTableRegister(const luaL_Reg* l) {
  lua_getglobal(m_lua_state_, "crane");
#  if LUA_VERSION_NUM == 501
  luaL_register(m_lua_state_, NULL, l);
#  else
  luaL_setfuncs(m_lua_state_, l, 0);
#  endif
  lua_pop(m_lua_state_, 1);
}

void LuaEnvironment::RegisterLuaCraneStructFunctions(
    const luaL_Reg* global_funcs) {
  for (const luaL_Reg* reg = global_funcs; reg->name != nullptr; ++reg) {
    lua_pushcfunction(m_lua_state_, reg->func);
    lua_setglobal(m_lua_state_, reg->name);
  }
}

bool LuaEnvironment::LoadLuaScript(const std::vector<std::string> req_funcs) {
  if (m_lua_state_ == nullptr) {
    CRANE_DEBUG(
        "Lua state (m_lua_state_) is null when loading script '{}'. "
        "This usually indicates Lua VM initialization failed.",
        m_lua_script_);

    if ((m_lua_state_ = luaL_newstate()) == nullptr) {
      CRANE_ERROR("luaL_newstate() failed to allocate");
      return false;
    }
    luaL_openlibs(m_lua_state_);

    RegisterFunctions_();

    return false;
  }

  if (luaL_loadfile(m_lua_state_, m_lua_script_.data())) {
    CRANE_ERROR("luaL_loadfile failed.");
    lua_pop(m_lua_state_, 1);
    return false;
  }

  if (lua_pcall(m_lua_state_, 0, 1, 0)) {
    CRANE_ERROR("{}:{}", m_lua_script_, lua_tostring(m_lua_state_, -1));
    lua_pop(m_lua_state_, 1);
    return false;
  }
  int rc = static_cast<int>(lua_tonumber(m_lua_state_, -1));
  lua_pop(m_lua_state_, 1);
  if (rc) {
    CRANE_ERROR("{}: returned {} on load", m_lua_script_, rc);
    return false;
  }

  if (!CheckLuaScriptFunctions_(m_lua_state_, m_lua_script_, req_funcs)) {
    CRANE_ERROR("{}: required function(s) not present", m_lua_script_);
    return false;
  }

  return true;
}

int LuaEnvironment::LogLuaMsg_(lua_State* lua_state) {
  std::string prefix = "[lua]";
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

int LuaEnvironment::LogLuaError_(lua_State* lua_state) {
  std::string prefix = "[lua]";
  std::string msg = lua_tostring(lua_state, -1);
  CRANE_ERROR("{}: {}", prefix, msg);

  return (0);
}

int LuaEnvironment::TimeStr2Mins_(lua_State* lua_state) {
  std::string_view time = lua_tostring(lua_state, -1);
  int minutes = util::TimeStr2Mins(time);
  lua_pushnumber(lua_state, minutes);
  return 1;
}

void LuaEnvironment::RegisterFunctions_() {
  const char* unpack_str;

#  if LUA_VERSION_NUM == 501
  unpack_str = "unpack";
#  else
  unpack_str = "table.unpack";
#  endif

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

  // all used flags

  lua_setglobal(m_lua_state_, "crane");
}

void LuaEnvironment::LuaTableRegister_(const luaL_Reg* l) {
#  if LUA_VERSION_NUM == 501
  luaL_register(m_lua_state_, NULL, l);
#  else
  luaL_setfuncs(m_lua_state_, l, 0);
#  endif
}

void LuaEnvironment::RegisterOutputErrTab_() {
  const google::protobuf::EnumDescriptor* desc =
      crane::grpc::ErrCode_descriptor();
  for (int i = 0; i < desc->value_count(); ++i) {
    const google::protobuf::EnumValueDescriptor* vdesc = desc->value(i);
    lua_pushnumber(m_lua_state_, vdesc->number());
    lua_setfield(m_lua_state_, -2, vdesc->name().data());
  }
}

int LuaEnvironment::LogLuaUserMsg_(lua_State* lua_state) {
  std::string msg = lua_tostring(lua_state, -1);

  if (!m_user_msg_.empty()) {
    m_user_msg_ = fmt::format("{}\n{}", m_user_msg_, msg);
  } else {
    m_user_msg_ = msg;
  }

  return (0);
}

int LuaEnvironment::LogLuaUserMsgStatic_(lua_State* lua_state) {
  auto* self = static_cast<LuaEnvironment*>(
      lua_touserdata(lua_state, lua_upvalueindex(1)));
  return self->LogLuaUserMsg_(lua_state);
}

bool LuaEnvironment::CheckLuaScriptFunction_(lua_State* lua_state,
                                             const std::string& name) {
  bool result = true;
  lua_getglobal(lua_state, name.data());
  if (!lua_isfunction(lua_state, -1)) result = false;
  lua_pop(lua_state, -1);
  return result;
}

bool LuaEnvironment::CheckLuaScriptFunctions_(lua_State* lua_state,
                                              const std::string& script_path,
                                              const std::vector<std::string>& req_fxns) {
  bool result = true;
  for (const auto& req_fxn : req_fxns) {
    if (!CheckLuaScriptFunction_(lua_state, req_fxn)) {
      CRANE_ERROR("{}: missing required function {}", script_path, req_fxn);
      result = false;
    }
  }
  return result;
}

#endif

}  // namespace crane
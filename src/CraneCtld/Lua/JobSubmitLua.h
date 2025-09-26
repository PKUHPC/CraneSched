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
#pragma once

#include "CtldPublicDefs.h"
// Precompiled header comes first!

#include "AccountManager.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

namespace Ctld {

int LogLuaMsg(lua_State *lua_state);
int LogLuaError(lua_State *lua_state);
int TimeStr2Mins(lua_State *lua_state);
int GetQosPriority(lua_State *lua_state);

static const struct luaL_Reg crane_functions [] = {
  { "log", LogLuaMsg},
  { "error", LogLuaError },
  { "time_str2mins", TimeStr2Mins},
  { "get_qos_priority", GetQosPriority },
  { NULL, NULL }
};

class JobSubmitLua {
public:
  JobSubmitLua() = default;

  ~JobSubmitLua() {
    if (m_lua_state_) {
      lua_close(m_lua_state_);
    }
  }

  bool Init(const std::string& lua_script);

  std::optional<std::string> JobSubmit(const TaskInCtld& task_in_ctld);

private:

  void RegisterOutputFunctions_();

  static void RegisterLuaCraneStructFunctions_(lua_State *lua_state);

  void LuaTableRegister_(const luaL_Reg* l);

  void RegisterOutputErrTab_();

  bool RunScript_();

  static int GetJobEnvFieldName_(lua_State* lua_state);
  static int GetJobReqFieldName_(lua_State* lua_state);
  static int SetJobEnvField_(lua_State* lua_state);
  static int SetJobReqField_(lua_State* lua_state);
  static int GetPartRecField_(lua_State* lua_state);

  static int LogLuaUserMsgStatic_(lua_State *lua_state);
  int LogLuaUserMsg_(lua_State *lua_state);

  void UpdateJobGloable_();
  void UpdateJobResvGloable_();
  void PushJobInfo_();
  void PushPartitionList_();

  std::string m_lua_script_;
  lua_State* m_lua_state_;
  std::string m_user_msg_;
};

} // namespace Ctld
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
#pragma once

#include "CtldPublicDefs.h"
// Precompiled header comes first!

#ifdef HAVE_LUA
#  include "crane/Lua.h"
#endif

namespace Ctld {

class LuaJobHandler {
 public:
  explicit LuaJobHandler() = default;
  ~LuaJobHandler() = default;

  LuaJobHandler(const LuaJobHandler&) = delete;
  LuaJobHandler& operator=(const LuaJobHandler&) = delete;
  LuaJobHandler(LuaJobHandler&&) = delete;
  LuaJobHandler& operator=(LuaJobHandler&&) = delete;

  static CraneRichError JobSubmit(const std::string& lua_script,
                                  TaskInCtld* task);
  static CraneRichError JobModify(const std::string& lua_script,
                                  TaskInCtld* task);

 private:
#ifdef HAVE_LUA
  static const std::vector<std::string> kReqFxns;

  static void RegisterGlobalFunctions_(const crane::LuaEnvironment& lua_env);
  static void RegisterTypes_(const crane::LuaEnvironment& lua_env);
  static void RegisterGlobalVariables_(const crane::LuaEnvironment& lua_env);

  // part_list
  static void PushPartitionList_(
      const std::string& user_name, const std::string& account,
      std::vector<crane::grpc::PartitionInfo>* part_list);
#endif
};

}  // namespace Ctld

#ifdef HAVE_LUA
inline std::unique_ptr<crane::LuaPool> g_lua_pool;
#endif
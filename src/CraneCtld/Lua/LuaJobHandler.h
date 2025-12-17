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
#  include <lua.hpp>
#endif

#include "AccountManager.h"
#include "crane/Lua.h"

namespace Ctld {

class LuaJobHandler {
 public:
  explicit LuaJobHandler() = default;
  ~LuaJobHandler() = default;

  LuaJobHandler(const LuaJobHandler&) = delete;
  LuaJobHandler& operator=(const LuaJobHandler&) = delete;
  LuaJobHandler(LuaJobHandler&&) = delete;
  LuaJobHandler& operator=(LuaJobHandler&&) = delete;

  static CraneRichError JobSubmit(const std::string& lua_script, TaskInCtld* task);
  static CraneRichError JobModify(const std::string& lua_script, TaskInCtld* task_in_ctld);

 private:
#ifdef HAVE_LUA
  static const luaL_Reg kGlobalFunctions[];
  static const luaL_Reg kCraneFunctions[];
  static const std::vector<std::string> kReqFxns;

  static int GetQosPriorityCb_(lua_State* lua_state);

  static int GetJobEnvFieldNameCb_(lua_State* lua_state);
  static int GetJobReqFieldNameCb_(lua_State* lua_state);
  static int SetJobEnvFieldCb_(lua_State* lua_state);
  static int SetJobReqFieldCb_(lua_State* lua_state);
  static int GetPartRecFieldNameCb_(lua_State* lua_state);
  static int GetJobEnvField_(const TaskInCtld& job_desc, const std::string& name,
                             lua_State* lua_state);
  static int GetJobReqField_(const TaskInCtld& job_desc, const std::string& name,
                             lua_State* lua_state);
  static int GetPartRecField_(const crane::grpc::PartitionInfo& partition_meta,
                              const std::string& name, lua_State* lua_state);

  static void UpdateJobGloable_(
      const crane::LuaEnvironment& lua_env,
      std::unordered_map<job_id_t, crane::grpc::TaskInfo>* job_info_map);
  // TODO: rename *Cb_
  static int JobsIter_(lua_State* lua_state);
  static int JobsIterNext_(lua_State* lua_state);
  static int JobsIterGC_(lua_State* lua_state);
  static int JobsGet_(lua_State* lua_state);

  static void UpdateJobResvGloable_(
      const crane::LuaEnvironment& lua_env,
      crane::grpc::QueryReservationInfoReply* resv_info_reply);
  static void PushJobDesc_(TaskInCtld* task,
                           const crane::LuaEnvironment& lua_env);
  static void PushPartitionList_(
      const crane::LuaEnvironment& lua_env, const std::string& user_name,
      const std::string& account,
      crane::grpc::QueryPartitionInfoReply* partition_info_reply);
  static void PushJobRec_(const crane::LuaEnvironment& lua_env,
                          crane::grpc::TaskInfo* task);
  static int GetJobReqFieldIndex_(lua_State* lua_state);
  static int JobRecFieldIndex_(lua_State* lua_state);
  static int PartitionRecFieldIndex_(lua_State* lua_state);
  static int ResvFieldIndex_(lua_State* lua_state);
  static int LuaJobRecordField_(lua_State* lua_state,
                                crane::grpc::TaskInfo* job_ptr,
                                const std::string& name);
  static int ResvField_(lua_State* lua_state,
                        crane::grpc::ReservationInfo* resv_ptr,
                        const std::string& name);

  static void PushResourceView_(lua_State* L, const ResourceView& res);
#endif
};

}  // namespace Ctld

inline std::unique_ptr<crane::LuaPool> g_lua_pool;
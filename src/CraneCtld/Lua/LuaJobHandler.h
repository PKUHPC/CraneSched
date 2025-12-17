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

  static CraneRichError JobSubmit(const std::string& lua_script,
                                  TaskInCtld* task);
  static CraneRichError JobModify(const std::string& lua_script,
                                  TaskInCtld* task_in_ctld);

 private:
#ifdef HAVE_LUA
  static const luaL_Reg kGlobalFunctions[];
  static const luaL_Reg kCraneFunctions[];
  static const std::vector<std::string> kReqFxns;

  struct JobsIterState {
    std::vector<crane::grpc::TaskInfo> tasks;
    size_t index = 0;
  };

  struct ResvsIterState {
    crane::grpc::QueryReservationInfoReply reply;
    size_t index = 0;
  };

  // global function cb
  static int GetQosPriorityCb_(lua_State* lua_state);
  static int GetJobEnvFieldNameCb_(lua_State* lua_state);
  static int GetJobReqFieldNameCb_(lua_State* lua_state);
  static int SetJobEnvFieldCb_(lua_State* lua_state);
  static int SetJobReqFieldCb_(lua_State* lua_state);
  static int GetPartRecFieldNameCb_(lua_State* lua_state);

  // crane.jobs table
  static void UpdateJobGloable_(const crane::LuaEnvironment& lua_env);
  static int JobsIterCb_(lua_State* lua_state);
  static int JobsIterNextCb_(lua_State* lua_state);
  static int JobsIterGcCb_(lua_State* lua_state);

  // crane.reservations table
  static void UpdateResvGloable_(const crane::LuaEnvironment& lua_env);
  static int ResvsIterCb_(lua_State* lua_state);
  static int ResvsIterNextCb_(lua_State* lua_state);
  static int ResvsIterGcCb_(lua_State* lua_state);
  static int ResvsGetCb_(lua_State* lua_state);
  static int ResvGcCb_(lua_State* lua_state);

  // job_desc
  static void PushJobDesc_(TaskInCtld* task,
                           const crane::LuaEnvironment& lua_env);
  // part_list
  static void PushPartitionList_(
      const crane::LuaEnvironment& lua_env, const std::string& user_name,
      const std::string& account,
      crane::grpc::QueryPartitionInfoReply* partition_info_reply);
  // job_rec
  static void PushJobRec_(const crane::LuaEnvironment& lua_env,
                          crane::grpc::TaskInfo* task);
  // resource_view
  static void PushResourceView_(lua_State* L, const ResourceView& res);

  static int GetJobReqFieldIndexCb_(lua_State* lua_state);
  static int JobRecFieldIndexCb_(lua_State* lua_state);
  static int PartitionRecFieldIndexCb_(lua_State* lua_state);
  static int ResvFieldIndexCb_(lua_State* lua_state);

  /* ---------------------------------------------------------------------------
   * Field getter
   * ---------------------------------------------------------------------------
   */
  static int GetJobEnvField_(const TaskInCtld& job_desc,
                             const std::string& name, lua_State* lua_state);
  static int GetJobReqField_(const TaskInCtld& job_desc,
                             const std::string& name, lua_State* lua_state);
  static int GetPartRecField_(const crane::grpc::PartitionInfo& partition_meta,
                              const std::string& name, lua_State* lua_state);
  static int LuaJobRecordField_(lua_State* lua_state,
                                crane::grpc::TaskInfo* job_ptr,
                                const std::string& name);
  static int ResvField_(lua_State* lua_state,
                        crane::grpc::ReservationInfo* resv_ptr,
                        const std::string& name);

#endif
};

}  // namespace Ctld

inline std::unique_ptr<crane::LuaPool> g_lua_pool;
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

#ifdef HAVE_LUA
#  include <lua.hpp>
#endif

#include "AccountManager.h"
#include "crane/Lua.h"

namespace Ctld {

class JobSubmitLua {
 public:
  explicit JobSubmitLua() = default;
  ~JobSubmitLua() = default;

  JobSubmitLua(const JobSubmitLua&) = delete;
  JobSubmitLua& operator=(const JobSubmitLua&) = delete;
  JobSubmitLua(JobSubmitLua&&) = delete;
  JobSubmitLua& operator=(JobSubmitLua&&) = delete;

  bool Init(const std::string& lua_script);
  CraneExpectedRich<void> JobSubmit(TaskInCtld& task_in_ctld);
  CraneExpectedRich<void> JobModify(TaskInCtld& task_in_ctld);

 private:
#ifdef HAVE_LUA
  static const luaL_Reg kGlobalFunctions[];
  static const luaL_Reg kCraneFunctions[];
  static const char* kReqFxns[];

  static int GetQosPriority_(lua_State* lua_state);

  static int GetJobEnvFieldName_(lua_State* lua_state);
  static int GetJobReqFieldName_(lua_State* lua_state);
  static int SetJobEnvField_(lua_State* lua_state);
  static int SetJobReqField_(lua_State* lua_state);
  static int GetPartRecFieldName_(lua_State* lua_state);
  static int GetJobEnvField_(const TaskInCtld& job_desc, const char* name,
                             lua_State* lua_state);
  static int GetJobReqField_(const TaskInCtld& job_desc, const char* name,
                             lua_State* lua_state);
  static int GetPartRecField_(const crane::grpc::PartitionInfo& partition_meta,
                              const char* name, lua_State* lua_state);

  void UpdateJobGloable_();
  void UpdateJobResvGloable_();
  void PushJobDesc_(TaskInCtld* task);
  void PushPartitionList_(const std::string& user_name,
                          const std::string& account);
  void PushJobRec(crane::grpc::TaskInfo* task);
  static int GetJobReqFieldIndex_(lua_State* lua_state);
  static int JobRecFieldIndex_(lua_State* lua_state);
  static int PartitionRecFieldIndex_(lua_State* lua_state);
  static int ResvFieldIndex_(lua_State* lua_state);
  static int LuaJobRecordField_(lua_State* lua_state,
                                crane::grpc::TaskInfo* job_ptr,
                                const char* name);
  static int ResvField_(lua_State* lua_state,
                        crane::grpc::ReservationInfo* resv_ptr,
                        const char* name);

  static void PushResourceView_(lua_State* L, const ResourceView& res);

  std::unique_ptr<crane::LuaEnvironment> m_lua_env_;
  crane::grpc::QueryTasksInfoReply m_task_info_reply_;
  crane::grpc::QueryReservationInfoReply m_resv_info_reply_;
  crane::grpc::QueryPartitionInfoReply m_partition_info_reply_;
#endif
};

}  // namespace Ctld

inline std::unique_ptr<crane::LuaPool<Ctld::JobSubmitLua>> g_lua_pool;
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
#include "crane/Lock.h"

#include <lua.hpp>

namespace Ctld {

int LogLuaMsg(lua_State *lua_state);
int LogLuaError(lua_State *lua_state);
int TimeStr2Mins(lua_State *lua_state);
int GetQosPriority(lua_State *lua_state);

static const luaL_Reg kCraneFunctions[] = {
  { "log", LogLuaMsg },
  { "error", LogLuaError },
  { "time_str2mins", TimeStr2Mins },
  { "get_qos_priority", GetQosPriority },
  { nullptr, nullptr }
};

static const char* g_req_fxns[] = {
  "crane_job_submit",
  "crane_job_modify",
  nullptr
};

class JobSubmitLua {
public:
  explicit JobSubmitLua(const std::string& lua_script)
      : m_lua_script_(lua_script) {
    if ((m_lua_state_ = luaL_newstate()) == nullptr) {
      CRANE_ERROR("luaL_newstate() failed to allocate");
    }

    luaL_openlibs(m_lua_state_);

    RegisterOutputFunctions_();
    RegisterLuaCraneStructFunctions_(m_lua_state_);
  }

  JobSubmitLua(const JobSubmitLua&) = delete;
  JobSubmitLua& operator=(const JobSubmitLua&) = delete;
  JobSubmitLua(JobSubmitLua&&) = delete;
  JobSubmitLua& operator=(JobSubmitLua&&) = delete;

  ~JobSubmitLua() {
    if (m_lua_state_ != nullptr) {
      lua_close(m_lua_state_);
    }
  }

  CraneExpectedRich<void> JobSubmit(TaskInCtld& task_in_ctld);

  CraneExpectedRich<void> JobModify(TaskInCtld& task_in_ctld);

private:
  void RegisterOutputFunctions_();
  void LuaTableRegister_(const luaL_Reg* l);
  void RegisterOutputErrTab_();
  static int LogLuaUserMsgStatic_(lua_State *lua_state);
  int LogLuaUserMsg_(lua_State *lua_state);

  static void RegisterLuaCraneStructFunctions_(lua_State *lua_state);
  static int GetJobEnvFieldName_(lua_State* lua_state);
  static int GetJobReqFieldName_(lua_State* lua_state);
  static int SetJobEnvField_(lua_State* lua_state);
  static int SetJobReqField_(lua_State* lua_state);
  static int GetPartRecFieldName_(lua_State* lua_state);
  static int GetJobEnvField_(const crane::grpc::TaskInfo& job_desc, const char *name, lua_State *lua_state);
  static int GetJobReqField_(const crane::grpc::TaskToCtld& job_desc,
                             const char* name, lua_State* lua_state);
  static int GetPartRecField_(const PartitionMeta& partition_meta, const char *name, lua_State *lua_state);

  bool LoadLuaScript_();
  static bool CheckLuaScriptFunction_(lua_State *lua_state, const char *name);
  static bool CheckLuaScriptFunctions_(lua_State *lua_state, const std::string& script_pash, const char **req_fxns);

  void UpdateJobGloable_();
  void UpdateJobResvGloable_();
  void PushJobDesc_(crane::grpc::TaskToCtld* task);
  void PushPartitionList_(const std::string& user_name,
                          const std::string& account);
  void PushJobRec(crane::grpc::TaskInfo* task);
  static int GetJobReqFieldIndex_(lua_State *lua_state);
  static int JobRecFieldIndex_(lua_State *lua_state);
  static int PartitionRecFieldIndex_(lua_State* lua_state);
  static int luaJobRecordField_(lua_State *lua_state,
    crane::grpc::TaskInfo* job_ptr,
    const char *name);
  static int PartitionRecField_(lua_State *lua_state,
    const PartitionMeta& partition_meta,
    const char *name);

  std::string m_lua_script_;
  lua_State* m_lua_state_;
  std::string m_user_msg_;
  std::vector<std::shared_ptr<crane::grpc::TaskInfo>> m_job_info_list_;
};

class LuaPool {
public:
  class Handle {
  public:
    Handle(LuaPool* pool, std::unique_ptr<JobSubmitLua> obj)
        : m_pool_(pool), m_lua_(std::move(obj)) {}

    Handle(Handle&& other) noexcept
        : m_pool_(other.m_pool_), m_lua_(std::move(other.m_lua_)) { other.m_pool_ = nullptr; }

    Handle& operator=(Handle&& other) noexcept {
      if (this != &other) {
        m_pool_ = other.m_pool_;
        m_lua_ = std::move(other.m_lua_);
        other.m_pool_ = nullptr;
      }
      return *this;
    }

    JobSubmitLua* operator->() const { return m_lua_.get(); }
    JobSubmitLua& operator*() const { return *m_lua_; }
    JobSubmitLua* get() const { return m_lua_.get(); }

    ~Handle() {
      if ((m_pool_ != nullptr) && m_lua_) m_pool_->Release_(std::move(m_lua_));
    }
    Handle(const Handle&) = delete;
    Handle& operator=(const Handle&) = delete;
  private:
    LuaPool* m_pool_;
    std::unique_ptr<JobSubmitLua> m_lua_;
  };

  LuaPool(size_t pool_size, const std::string& lua_script) {
    for (size_t i = 0; i < pool_size; ++i)
      m_pool_.push(std::make_unique<JobSubmitLua>(lua_script));
  }

  Handle Acquire() {
    std::unique_lock<std::mutex> lock(m_mutex_);
    m_cv_.wait(lock, [this] { return !m_pool_.empty(); });
    auto obj = std::move(m_pool_.front());
    m_pool_.pop();
    return Handle(this, std::move(obj));
  }
private:
  void Release_(std::unique_ptr<JobSubmitLua> obj) {
    {
      std::unique_lock<std::mutex> lock(m_mutex_);
      m_pool_.push(std::move(obj));
    }
    m_cv_.notify_one();
  }
  std::mutex m_mutex_;
  std::queue<std::unique_ptr<JobSubmitLua>> m_pool_;
  std::condition_variable m_cv_;
};

} // namespace Ctld

inline std::unique_ptr<Ctld::LuaPool> g_lua_pool;
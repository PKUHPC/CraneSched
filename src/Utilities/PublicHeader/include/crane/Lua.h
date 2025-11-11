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

#ifdef HAVE_LUA
#  include <lua.hpp>
#endif

#include <BS_thread_pool.hpp>
#include <memory>

#include "crane/Logger.h"

namespace crane {
#ifdef HAVE_LUA

class LuaEnvironment {
 public:
  LuaEnvironment() = default;
  ~LuaEnvironment() {
    if (m_lua_state_ != nullptr) lua_close(m_lua_state_);
  }

  bool Init(const std::string& script);

  void LuaTableRegister(const luaL_Reg* l);

  void RegisterLuaCraneStructFunctions(const luaL_Reg* global_funcs);

  bool LoadLuaScript(const char* req_funcs[]);

  lua_State* GetLuaState() const { return m_lua_state_; }
  std::string GetUserMsg() const { return m_user_msg_; }
  void ResetUserMsg() { m_user_msg_.clear(); }

 private:
  static const luaL_Reg kCraneFunctions[];

  static int LogLuaMsg_(lua_State* lua_state);
  static int LogLuaError_(lua_State* lua_state);
  static int TimeStr2Mins_(lua_State* lua_state);

  void RegisterFunctions_();

  void LuaTableRegister_(const luaL_Reg* l);

  void RegisterOutputErrTab_();

  int LogLuaUserMsg_(lua_State* lua_state);
  static int LogLuaUserMsgStatic_(lua_State* lua_state);
  static bool CheckLuaScriptFunction_(lua_State* lua_state, const char* name);
  static bool CheckLuaScriptFunctions_(lua_State* lua_state,
                                       const std::string& script_pash,
                                       const char** req_fxns);

  std::string m_lua_script_;
  lua_State* m_lua_state_{};
  std::string m_user_msg_;
};
#endif

class LuaPool {
 public:
  LuaPool() {
    m_thread_pool_ = std::make_unique<BS::thread_pool>(
        std::thread::hardware_concurrency(),
        [] { util::SetCurrentThreadName("LuaThreadPool"); });
  }

  std::future<CraneRichError> ExecuteLuaScript(const std::string& lua_script) {
    auto promise = std::make_shared<std::promise<CraneRichError>>();
    std::future<CraneRichError> fut = promise->get_future();

    m_thread_pool_->detach_task([lua_script, promise]() {
      CraneRichError result;
      auto lua_env = std::make_unique<crane::LuaEnvironment>();
      if (!lua_env->Init(lua_script))
        result = FormatRichErr(CraneErrCode::ERR_LUA_FAILED,
                               "Failed to init lua environment");

      if (!lua_env->LoadLuaScript({}))
        result = FormatRichErr(CraneErrCode::ERR_LUA_FAILED,
                               "Failed to load lua script");

      promise->set_value(result);
    });

    return fut;
  }

  template <typename Callback, typename... Args>
  std::future<CraneRichError> ExecuteLuaScript(Callback&& callback,
                                               Args&&... args) {
    auto promise = std::make_shared<std::promise<CraneRichError>>();
    std::future<CraneRichError> fut = promise->get_future();
    auto packed_args = std::make_tuple(std::forward<Args>(args)...);

    m_thread_pool_->detach_task([callback = std::forward<Callback>(callback),
                                 packed_args = std::move(packed_args),
                                 promise]() {
      CraneRichError result;

      result = std::apply(callback, packed_args);

      promise->set_value(result);
    });

    return fut;
  }

 private:
  std::unique_ptr<BS::thread_pool> m_thread_pool_;
};

}  // namespace crane

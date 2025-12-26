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

#ifdef HAVE_LUA
#  include <sol/sol.hpp>
#endif

#include <BS_thread_pool.hpp>
#include <memory>

#include "crane/Logger.h"

namespace crane {
class LuaEnvironment {
 public:
  LuaEnvironment() = default;
  ~LuaEnvironment() = default;

  bool Init(const std::string& script);

  bool LoadLuaScript(const std::vector<std::string>& req_funcs);
#ifdef HAVE_LUA
  sol::state& GetLuaState() const { return *m_lua_state_ptr_; }
  sol::table GetCraneTable() const { return m_crane_table_; }
  std::string GetUserMsg() const { return m_user_msg_; }

 private:
  void RegisterFunctions_();

  void RegisterOutputErrTab_();

  bool CheckLuaScriptFunctions_(const std::vector<std::string>& req_fxns);

  std::string m_lua_script_;
  std::unique_ptr<sol::state> m_lua_state_ptr_;
  sol::table m_crane_table_;
  std::string m_user_msg_;
#endif
};

class LuaPool {
 public:
  LuaPool() = default;

  bool Init() {
#ifndef HAVE_LUA
    CRANE_ERROR("Lua is not enable");
    return false;
#endif
    m_thread_pool_ = std::make_unique<BS::thread_pool>(
        std::thread::hardware_concurrency(),
        [] { util::SetCurrentThreadName("LuaThreadPool"); });
    return true;
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

  template <typename Callback>
    requires std::invocable<Callback> &&
             std::same_as<std::invoke_result_t<Callback>, CraneRichError>
  std::future<CraneRichError> ExecuteLuaScript(Callback callback) {
    auto promise = std::make_shared<std::promise<CraneRichError>>();
    std::future<CraneRichError> fut = promise->get_future();

    m_thread_pool_->detach_task(
        [callback = std::forward<Callback>(callback), promise]() {
          promise->set_value(callback());
        });

    return fut;
  }

 private:
  std::unique_ptr<BS::thread_pool> m_thread_pool_;
};

}  // namespace crane

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

#include "crane/PublicHeader.h"
#include "crane/String.h"

namespace crane {
class LuaEnvironment {
 public:
  LuaEnvironment() = default;
  ~LuaEnvironment() = default;

  bool Init(const std::string& script);

  bool LoadLuaScript(const std::vector<std::string>& req_funcs);
#ifdef HAVE_LUA
  sol::state& GetLuaState() const;
  sol::table GetCraneTable() const;
  std::string GetUserMsg() const;

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

  bool Init();

  std::future<CraneRichError> ExecuteLuaScript(const std::string& lua_script);

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

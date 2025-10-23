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
#include <lua.hpp>
#endif

#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>

#include "crane/Logger.h"

namespace crane {
#ifdef HAVE_LUA
int LogLuaMsg(lua_State *lua_state);
int LogLuaError(lua_State *lua_state);
int TimeStr2Mins(lua_State *lua_state);

static const luaL_Reg kCraneFunctions[] = {
  { "log", LogLuaMsg },
  { "error", LogLuaError },
  { "time_str2mins", TimeStr2Mins },
  { nullptr, nullptr }
};

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
  void ResetUserMsg() {
    m_user_msg_.clear();
  }

private:
  void LuaTableRegister_(const luaL_Reg* l);

  void RegisterOutputErrTab_();

  int LogLuaUserMsg_(lua_State *lua_state);
  static int LogLuaUserMsgStatic_(lua_State *lua_state);
  static bool CheckLuaScriptFunction_(lua_State *lua_state, const char *name);
  static bool CheckLuaScriptFunctions_(lua_State *lua_state, const std::string& script_pash, const char **req_fxns);

  std::string m_lua_script_;
  lua_State* m_lua_state_{};
  std::string m_user_msg_;
};
#endif

template <typename T>
class LuaPool {
public:
  class Handle {
  public:
    Handle(LuaPool* pool, std::unique_ptr<T> obj)
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

    T* operator->() const { return m_lua_.get(); }
    T& operator*() const { return *m_lua_; }
    T* get() const { return m_lua_.get(); }

    ~Handle() {
      if ((m_pool_ != nullptr) && m_lua_) m_pool_->Release_(std::move(m_lua_));
    }
    Handle(const Handle&) = delete;
    Handle& operator=(const Handle&) = delete;
  private:
    LuaPool* m_pool_;
    std::unique_ptr<T> m_lua_;
  };

  LuaPool() = default;

  bool Init(size_t pool_size, const std::string& lua_script) {
#ifdef HAVE_LUA
    for (size_t i = 0; i < pool_size; ++i) {
      auto lua = std::make_unique<T>();
      if (!lua->Init(lua_script)) {
        CRANE_ERROR("Lua env init failed");
        continue;
      }
      m_pool_.push(std::move(lua));
    }
    return true;
#endif

    return false;
  }

  Handle Acquire() {
    std::unique_lock<std::mutex> lock(m_mutex_);
    m_cv_.wait(lock, [this] { return !m_pool_.empty(); });
    auto obj = std::move(m_pool_.front());
    m_pool_.pop();
    return Handle(this, std::move(obj));
  }
private:
  void Release_(std::unique_ptr<T> obj) {
    {
      std::unique_lock<std::mutex> lock(m_mutex_);
      m_pool_.push(std::move(obj));
    }
    m_cv_.notify_one();
  }
  std::mutex m_mutex_;
  std::queue<std::unique_ptr<T>> m_pool_;
  std::condition_variable m_cv_;
};

} // namespace crane

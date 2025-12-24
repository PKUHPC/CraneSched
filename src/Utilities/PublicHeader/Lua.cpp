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

#include "crane/Lua.h"

#include "crane/String.h"

namespace crane {



bool LuaEnvironment::Init(const std::string& script) {
#ifdef HAVE_LUA
  m_lua_script_ = script;
  m_lua_state_ptr_ = std::make_unique<sol::state>(sol::state());
  m_lua_state_ptr_->open_libraries(sol::lib::base, sol::lib::package,
                                   sol::lib::table, sol::lib::string);
  m_crane_table_ = (*m_lua_state_ptr_)["crane"].valid()
                       ? (*m_lua_state_ptr_)["crane"]
                       : (*m_lua_state_ptr_).create_named_table("crane");
  RegisterFunctions_();
#endif
  return true;
}

bool LuaEnvironment::LoadLuaScript(const std::vector<std::string>& req_funcs) {
#ifdef HAVE_LUA
  if (m_lua_state_ptr_ == nullptr) {
    CRANE_DEBUG(
        "Lua state (m_lua_state_) is null when loading script '{}'. "
        "This usually indicates Lua VM initialization failed.",
        m_lua_script_);
    m_lua_state_ptr_ = std::make_unique<sol::state>(sol::state());
    m_lua_state_ptr_->open_libraries(sol::lib::base, sol::lib::package,
                                     sol::lib::table, sol::lib::string);
    m_crane_table_ = (*m_lua_state_ptr_)["crane"].valid()
                         ? (*m_lua_state_ptr_)["crane"]
                         : (*m_lua_state_ptr_).create_named_table("crane");
    RegisterFunctions_();
  }

  sol::load_result loaded = m_lua_state_ptr_->load_file(m_lua_script_);
  if (!loaded.valid()) {
    sol::error err = loaded;
    CRANE_ERROR("luaL_loadfile failed: {}", err.what());
    return false;
  }

  sol::protected_function script_func = loaded;
  sol::protected_function_result result = script_func();
  if (!result.valid()) {
    sol::error err = result;
    CRANE_ERROR("{}:{}", m_lua_script_, err.what());
    return false;
  }

  int rc = 0;
  if (result.return_count() > 0) {
    sol::object ret = result.get<sol::object>();
    if (ret.is<int>()) {
      rc = ret.as<int>();
    } else if (ret.is<double>()) {
      rc = static_cast<int>(ret.as<double>());
    } else {
      CRANE_ERROR("{}/lua: non-numeric return code", __func__);
    }
  }
  if (rc) {
    CRANE_ERROR("{}: returned {} on load", m_lua_script_, rc);
    return false;
  }

  if (!CheckLuaScriptFunctions_(req_funcs)) {
    CRANE_ERROR("{}: required function(s) not present", m_lua_script_);
    return false;
  }
#endif
  return true;
}

#ifdef HAVE_LUA
void LuaEnvironment::RegisterFunctions_() {
  // crane.log_info()
  m_crane_table_.set_function("log_info", [](const sol::variadic_args& va) {
    sol::state_view lua(va.lua_state());
    sol::function string_format = lua["string"]["format"];
    sol::object result = string_format.call(
        va[0], sol::as_args(std::vector<sol::object>{va.begin()+1, va.end()}));
    CRANE_INFO("[lua]: {}", result.as<std::string>());
  });

  // crane.log_debug()
  m_crane_table_.set_function("log_debug", [](const sol::variadic_args& va) {
    sol::state_view lua(va.lua_state());
    sol::function string_format = lua["string"]["format"];
    sol::object result = string_format.call(
        va[0], sol::as_args(std::vector<sol::object>{va.begin()+1, va.end()}));
    CRANE_DEBUG("[lua]: {}", result.as<std::string>());
  });

  // crane.log_trace()
  m_crane_table_.set_function("log_trace", [](const sol::variadic_args& va) {
    sol::state_view lua(va.lua_state());
    sol::function string_format = lua["string"]["format"];
    sol::object result = string_format.call(
        va[0], sol::as_args(std::vector<sol::object>{va.begin()+1, va.end()}));
    CRANE_TRACE("[lua]: {}", result.as<std::string>());
  });

  // crane.log_error()
  m_crane_table_.set_function("log_error", [](const sol::variadic_args& va) {
    sol::state_view lua(va.lua_state());
    sol::function string_format = lua["string"]["format"];
    sol::object result = string_format.call(
        va[0], sol::as_args(std::vector<sol::object>{va.begin()+1, va.end()}));
    CRANE_ERROR("[lua]: {}", result.as<std::string>());
  });

  // crane.log_user()
  m_crane_table_.set_function("log_user", [this](const sol::variadic_args& va) {
    sol::state_view lua(va.lua_state());
    sol::function string_format = lua["string"]["format"];
    sol::object result = string_format.call(
        va[0], sol::as_args(std::vector<sol::object>{va.begin()+1, va.end()}));
    m_user_msg_ = result.as<std::string>();
  });

  // crane.time_str2mins()
  m_crane_table_.set_function("time_str2mins", [](const std::string& time) {
    return util::TimeStr2Mins(time);
  });


  RegisterOutputErrTab_();
  m_crane_table_["ERROR"] = static_cast<int>(CraneErrCode::ERR_LUA_FAILED);
  m_crane_table_["SUCCESS"] = static_cast<int>(CraneErrCode::SUCCESS);

  m_crane_table_["Pending"] =
      static_cast<int>(crane::grpc::TaskStatus::Pending);
  m_crane_table_["Running"] =
      static_cast<int>(crane::grpc::TaskStatus::Running);
  m_crane_table_["Completed"] =
      static_cast<int>(crane::grpc::TaskStatus::Completed);
  m_crane_table_["Failed"] = static_cast<int>(crane::grpc::TaskStatus::Failed);
  m_crane_table_["Cancelled"] =
      static_cast<int>(crane::grpc::TaskStatus::Cancelled);
  m_crane_table_["OutOfMemory"] =
      static_cast<int>(crane::grpc::TaskStatus::OutOfMemory);

  m_crane_table_["Batch"] = static_cast<int>(crane::grpc::TaskType::Batch);
  m_crane_table_["Interactive"] =
      static_cast<int>(crane::grpc::TaskType::Interactive);

  // other used flags
}

void LuaEnvironment::RegisterOutputErrTab_() {
  const google::protobuf::EnumDescriptor* desc =
      crane::grpc::ErrCode_descriptor();
  for (int i = 0; i < desc->value_count(); ++i) {
    const google::protobuf::EnumValueDescriptor* vdesc = desc->value(i);
    m_crane_table_[vdesc->name()] = vdesc->number();
  }
}

bool LuaEnvironment::CheckLuaScriptFunctions_(
    const std::vector<std::string>& req_fxns) {
  for (const auto& name : req_fxns) {
    sol::object f = (*m_lua_state_ptr_)[name];
    if (!f.valid() || f.get_type() != sol::type::function) {
      CRANE_ERROR("{}: missing required function {}", m_lua_script_, name);
      return false;
    }
  }
  return true;
}

#endif

}  // namespace crane
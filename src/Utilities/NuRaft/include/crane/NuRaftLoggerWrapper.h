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

#include <libnuraft/logger.hxx>

#include "crane/Logger.h"

namespace crane {
namespace Internal {

class NuRaftLoggerWrapper : public nuraft::logger {
 public:
  void SetLevel(int level) {
    m_level_ = static_cast<spdlog::level::level_enum>(level);
  }

  int get_level() override {
    int res = 6 - m_level_;
    return res;
  }

  /**
   * @param level Level of given log.
   * @param source_file Name of file where the log is located.
   * @param func_name Name of function where the log is located.
   * @param line_number Line number of the log.
   * @param log_line Contents of the log.
   */
  void put_details(int level, const char* source_file, const char* func_name,
                   size_t line_number, const std::string& log_line) override {
    int rev_level = 6 - level;
    auto spd_level = spdlog::level::level_enum(rev_level);
    switch (spd_level) {
    case spdlog::level::trace:
      RAFT_TRACE("[{}:{}] {}", func_name, line_number, log_line);
      break;
    case spdlog::level::debug:
      RAFT_DEBUG("[{}:{}] {}", func_name, line_number, log_line);
      break;
    case spdlog::level::info:
      RAFT_INFO("[{}:{}] {}", func_name, line_number, log_line);
      break;
    case spdlog::level::warn:
      RAFT_WARN("[{}:{}] {}", func_name, line_number, log_line);
      break;
    case spdlog::level::err:
      RAFT_ERROR("[{}:{}] {}", func_name, line_number, log_line);
      break;
    case spdlog::level::critical:
      RAFT_CRITICAL("[{}:{}] {}", func_name, line_number, log_line);
      break;
    default:
      break;
    }
  }

 private:
  spdlog::level::level_enum m_level_ = spdlog::level::info;
};

}  // namespace Internal
}  // namespace crane
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

#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

namespace crane {

namespace Internal {

class FileLoggerStaticInitializer {
 public:
  FileLoggerStaticInitializer() noexcept { spdlog::init_thread_pool(8192, 1); }
};

}  // namespace Internal

class FileLogger {
 public:
  FileLogger(std::string name, std::string file_path)
      : m_name_(std::move(name)), m_file_path_(std::move(file_path)) {
    m_logger_ =
        spdlog::basic_logger_st<spdlog::async_factory>(m_name_, m_file_path_);
    m_logger_->set_pattern("%v");
    m_logger_->set_level(spdlog::level::info);
  }

  ~FileLogger() { spdlog::drop(m_name_); }

  void Output(const std::string& buf) { m_logger_->info(buf); }

 private:
  std::string m_name_;
  std::string m_file_path_;
  std::shared_ptr<spdlog::logger> m_logger_;

  [[maybe_unused]] static inline Internal::FileLoggerStaticInitializer
      _s_initializer_{};
};

}  // namespace crane
/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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
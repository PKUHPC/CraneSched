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

#include "crane/Logger.h"

void LoggerFactory::InitAndSetDefaultLogger(spdlog::level::level_enum level,
                                            const std::string& log_file_path) {
  spdlog::init_thread_pool(256, 1);
  spdlog::set_pattern("[%^%L%$ %C-%m-%d %s:%#] %v");

  s_file_sink_ = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
      log_file_path, 1048576 * 50 /*MB*/, 3);
  s_file_sink_->set_level(level);

  s_console_sink_ = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
  s_console_sink_->set_level(level);

  s_sinks_init_list_ = {s_file_sink_, s_console_sink_};

  std::vector<std::shared_ptr<spdlog::logger>> loggers;
  for (auto [name, level] : s_static_registered_logger_level_map_) {
    auto logger = std::make_shared<spdlog::async_logger>(
        name.data(), s_sinks_init_list_, spdlog::thread_pool(),
        s_overflow_policy_);
    logger->set_level(level);

    loggers.push_back(logger);
  }

  s_logger_map_mtx_.lock();
  for (auto& logger : loggers) s_logger_map_.emplace(logger->name(), logger);
  spdlog::set_default_logger(s_logger_map_.at("default"));
  s_logger_map_mtx_.unlock();

  spdlog::flush_on(spdlog::level::err);
  spdlog::flush_every(std::chrono::seconds(1));

  spdlog::set_level(level);
}

std::shared_ptr<spdlog::logger> LoggerFactory::RegisterLogger(
    const std::string& name, const spdlog::level::level_enum level) {
  auto logger = std::make_shared<spdlog::async_logger>(
      name, s_sinks_init_list_, spdlog::thread_pool(), s_overflow_policy_);
  logger->set_level(level);

  s_logger_map_mtx_.lock();
  s_logger_map_.emplace(name, logger);
  s_logger_map_mtx_.unlock();

  return logger;
}

void LoggerFactory::SetLoggerLevel(const std::string& name,
                                   spdlog::level::level_enum level) {
  std::lock_guard lock(s_logger_map_mtx_);

  auto it = s_logger_map_.find(name);
  if (it == s_logger_map_.end()) return;

  it->second->set_level(level);
}
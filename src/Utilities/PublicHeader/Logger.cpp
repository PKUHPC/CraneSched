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

void InitLogger(spdlog::level::level_enum level,
                const std::string& log_file_path) {
  auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
      log_file_path, 1048576 * 50 /*MB*/, 3);

  auto console_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();

  spdlog::init_thread_pool(256, 1);
  auto default_logger = std::make_shared<spdlog::async_logger>(
      "default", spdlog::sinks_init_list{file_sink, console_sink},
      spdlog::thread_pool(), spdlog::async_overflow_policy::block);

  auto taskscheduler_logger = std::make_shared<spdlog::async_logger>(
      "taskscheduler", spdlog::sinks_init_list{file_sink, console_sink},
      spdlog::thread_pool(), spdlog::async_overflow_policy::block);

  auto cranedkeeper_logger = std::make_shared<spdlog::async_logger>(
      "cranedkeeper", spdlog::sinks_init_list{file_sink, console_sink},
      spdlog::thread_pool(), spdlog::async_overflow_policy::block);

  file_sink->set_level(level);
  console_sink->set_level(level);

  default_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%f] [%n] [%^%l%$] [%s:%#] %v");
  taskscheduler_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%f] [%n] [%^%l%$] [%s:%#] %v");
  cranedkeeper_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%f] [%n] [%^%l%$] [%s:%#] %v");

  spdlog::register_logger(default_logger);
  spdlog::register_logger(taskscheduler_logger);
  spdlog::register_logger(cranedkeeper_logger);

  spdlog::flush_on(spdlog::level::err);
  spdlog::flush_every(std::chrono::seconds(1));

  spdlog::set_level(level);
}
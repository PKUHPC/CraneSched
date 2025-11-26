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

static LoggerSinks default_sinks{};

std::optional<spdlog::level::level_enum> StrToLogLevel(
    const std::string& level) {
  if (level == "trace") {
    return spdlog::level::trace;
  } else if (level == "debug") {
    return spdlog::level::debug;
  } else if (level == "info") {
    return spdlog::level::info;
  } else if (level == "warn") {
    return spdlog::level::warn;
  } else if (level == "error") {
    return spdlog::level::err;
  } else if (level == "off") {
    return spdlog::level::off;
  } else {
    return std::nullopt;
  }
}

void InitLogger(spdlog::level::level_enum level,
                const std::string& log_file_path, bool enable_console,
                uint64_t max_file_size, uint64_t max_file_num) {
  std::vector<spdlog::sink_ptr> sinks;
  auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
      log_file_path, max_file_size, max_file_num);
  file_sink->set_level(level);
  file_sink->set_pattern(kLogPattern);
  default_sinks.file_sink = file_sink;
  sinks.push_back(file_sink);

  if (enable_console) {
    auto console_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
    console_sink->set_level(level);
    console_sink->set_pattern(kLogPattern);
    sinks.push_back(console_sink);

    default_sinks.console_sink = console_sink;
  }

  spdlog::init_thread_pool(256, 1);
  auto logger = std::make_shared<spdlog::async_logger>(
      "default", sinks.begin(), sinks.end(), spdlog::thread_pool(),
      spdlog::async_overflow_policy::block);
  spdlog::set_default_logger(logger);

  spdlog::flush_on(spdlog::level::err);
  spdlog::flush_every(std::chrono::seconds(1));

  spdlog::set_level(level);
}

std::shared_ptr<spdlog::async_logger> AddLogger(
    const std::string& name, spdlog::level::level_enum level,
    const std::filesystem::path& log_file_path, bool enable_console,
    uint64_t max_file_size, uint64_t max_file_num) {
  std::vector<spdlog::sink_ptr> sinks;
  auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
      log_file_path, max_file_size, max_file_num);
  file_sink->set_level(level);
  file_sink->set_pattern(kLogPattern);
  sinks.push_back(file_sink);

  if (enable_console) {
    if (!default_sinks.console_sink) {
      default_sinks.console_sink =
          std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
      default_sinks.console_sink->set_level(level);

      default_sinks.console_sink->set_pattern(kLogPattern);
    }
    sinks.push_back(default_sinks.console_sink);
  }
  auto logger = std::make_shared<spdlog::async_logger>(
      name, sinks.begin(), sinks.end(), spdlog::thread_pool(),
      spdlog::async_overflow_policy::block);
  logger->set_level(level);
  spdlog::register_logger(logger);
  return logger;
}

std::shared_ptr<spdlog::async_logger> AddLogger(const std::string& name,
                                                spdlog::level::level_enum level,
                                                bool enable_console) {
  std::vector<spdlog::sink_ptr> sinks;
  sinks.push_back(default_sinks.file_sink);

  if (enable_console) {
    if (!default_sinks.console_sink) {
      default_sinks.console_sink =
          std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
      default_sinks.console_sink->set_level(level);

      default_sinks.console_sink->set_pattern(kLogPattern);
    }
    sinks.push_back(default_sinks.console_sink);
  }

  auto logger = std::make_shared<spdlog::async_logger>(
      name, sinks.begin(), sinks.end(), spdlog::thread_pool(),
      spdlog::async_overflow_policy::block);
  logger->set_level(level);
  spdlog::register_logger(logger);
  return logger;
}
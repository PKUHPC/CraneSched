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
                const std::string& log_file_path, bool console) {
  spdlog::set_pattern("[%^%L%$ %C-%m-%d %s:%#] %v");

  std::vector<spdlog::sink_ptr> sinks;
  auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
      log_file_path, 1048576 * 50 /*MB*/, 3);
  file_sink->set_level(level);
  sinks.push_back(file_sink);

  if (console) {
    auto console_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
    console_sink->set_level(level);
    sinks.push_back(console_sink);
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
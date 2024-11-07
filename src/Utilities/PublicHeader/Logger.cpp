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

void InitLogger(const std::map<std::string, spdlog::level::level_enum>& logLevels,
                const std::string& log_file_path,
                const bool cranectld_flag) {
  auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
      log_file_path, 1048576 * 50 /*MB*/, 3);

  auto console_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();

  spdlog::init_thread_pool(256, 1);
    auto create_logger = [&](const std::string& name) {
        auto logger = std::make_shared<spdlog::async_logger>(
            name, spdlog::sinks_init_list{file_sink, console_sink},
            spdlog::thread_pool(), spdlog::async_overflow_policy::block);
        logger->set_pattern("[%Y-%m-%d %H:%M:%S.%f] [%n] [%^%l%$] [%s:%#] %v");
        return logger;
    };

  spdlog::level::level_enum level = spdlog::level::trace;
  auto default_logger = create_logger("Default");
  FindLoggerValidLevel(logLevels, "Default", &level);
  default_logger->set_level(level);
  spdlog::set_level(level);
  spdlog::register_logger(default_logger);

  if (cranectld_flag) {
    auto cranectld_taskscheduler_logger = create_logger("TaskScheduler");
    auto cranectld_cranedkeeper_logger = create_logger("CranedKeeper");

    FindLoggerValidLevel(logLevels, "TaskScheduler", &level);
    cranectld_taskscheduler_logger->set_level(level);

    FindLoggerValidLevel(logLevels, "CranedKeeper", &level);
    cranectld_cranedkeeper_logger->set_level(level);

    spdlog::register_logger(cranectld_taskscheduler_logger);
    spdlog::register_logger(cranectld_cranedkeeper_logger);
  }

  spdlog::flush_on(spdlog::level::err);
  spdlog::flush_every(std::chrono::seconds(1));
}

void FindLoggerValidLevel(const std::map<std::string, spdlog::level::level_enum>& logLevels,
                          const std::string& loggerName,
                          spdlog::level::level_enum *out_level) {
    if (out_level == nullptr) {
        fmt::print("Logger map empty.\n");
        return;
    }
    auto it = logLevels.find(loggerName);
    if (it != logLevels.end()) {
        *out_level = it->second;  // 使用迭代器访问值
    } else {
        *out_level = spdlog::level::trace;  // 默认级别
    }
}

Result SetLoggerLogLevel(const std::string& logger_name, spdlog::level::level_enum level) {
 std::map<std::string, bool> loggers_to_set;
    if (logger_name == "All") {
        loggers_to_set = {{"Default", false}, {"TaskScheduler", false}, {"CranedKeeper", false}};
    } else if (logger_name == "Other") {
        loggers_to_set = {{"Default", false}};
    } else if (logger_name == "TaskScheduler" || logger_name == "CranedKeeper") {
        loggers_to_set = {{logger_name, false}};
    } else {
        return Result{false, fmt::format("logger {} not found\n", logger_name)};
    }

    for (auto& [name, result] : loggers_to_set) {
        result = SetSingleLoggerLevel(name, level);
    }

    std::string success_loggers;
    std::string failed_loggers;
    for (const auto& [name, result] : loggers_to_set) {
        if (result) {
            if (!success_loggers.empty()) {
                success_loggers += ", ";
            }
            success_loggers += name;
        } else {
            if (!failed_loggers.empty()) {
                failed_loggers += ", ";
            }
            failed_loggers += name;
        }
    }
    std::string final_message;
    if (!success_loggers.empty()) {
        final_message += fmt::format("Loggers {} set successfully.\n", success_loggers);
    }
    if (!failed_loggers.empty()) {
        final_message += fmt::format("Loggers {} set failed.\n", failed_loggers);
    }

    bool overall_success = failed_loggers.empty();
    return Result{overall_success, final_message};

}

bool StrToLogLevel(const std::string& str_level, spdlog::level::level_enum *out_Level) {
    if (str_level == "trace") {
        *out_Level = spdlog::level::trace;
    } else if (str_level == "debug") {
        *out_Level = spdlog::level::debug;
    } else if (str_level == "info") {
        *out_Level = spdlog::level::info;
    } else if (str_level == "warn") {
        *out_Level = spdlog::level::warn;
    } else if (str_level == "error") {
        *out_Level = spdlog::level::err;
    } else {
        return false;
    }

    return true;
}

bool SetSingleLoggerLevel(const std::string& logger_name, spdlog::level::level_enum level) {
    auto logger = spdlog::get(logger_name);
    if (logger == nullptr) {
        return false;
    }
    logger->set_level(level);
    return true;
}

std::shared_ptr<spdlog::logger> GetDefaultLogger() {
    static auto logger = spdlog::get("Default");
    return logger;
}

std::shared_ptr<spdlog::logger> GetCtldTaskSchedulerLogger() {
    static auto logger = spdlog::get("TaskScheduler");
    return logger;
}

std::shared_ptr<spdlog::logger> GetCtldCranedKeeperLogger() {
    static auto logger = spdlog::get("CranedKeeper");
    return logger;
}
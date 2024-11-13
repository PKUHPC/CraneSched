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
#include <unordered_map>

std::shared_ptr<spdlog::sinks::rotating_file_sink_mt> BasicLogger::file_sink = nullptr;
std::shared_ptr<spdlog::sinks::stderr_color_sink_mt> BasicLogger::console_sink = nullptr;

BasicLogger* Logger::CreateLogger() {
     return new Logger();
}

void Logger::Init(const std::string& log_file_path, const std::string& name) {
   static std::once_flag init_flag;
    std::call_once(init_flag, [&]() {
        // init spdlog
        spdlog::init_thread_pool(256, 1);
        spdlog::flush_on(spdlog::level::err);
        spdlog::flush_every(std::chrono::seconds(1));
        spdlog::set_level(spdlog::level::trace);

        BasicLogger::file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
            log_file_path, 1048576 * 50 /*MB*/, 3);
        BasicLogger::console_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
    });
    real_logger = std::make_shared<spdlog::async_logger>(
        name, spdlog::sinks_init_list{BasicLogger::file_sink, BasicLogger::console_sink},
        spdlog::thread_pool(), spdlog::async_overflow_policy::block);
    real_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%f] [%n] [%^%l%$] [%s:%#] %v");
    real_logger->set_level(spdlog::level::trace);
    spdlog::register_logger(real_logger);
}

void InitLogger(const std::unordered_map<std::string, spdlog::level::level_enum>& log_levels,
                const std::string& log_file_path,
                const bool cranectld_flag) {
    //resister logger
    for (const auto& [name, level] : log_levels) {
        REGISTER_LOGGER(name, Logger::CreateLogger);
    }

    //get logger
    for (const auto& [name, level] : log_levels) {
        auto logger = LoggerRegistry<BasicLogger>::Create(name);
        if (!logger) {
            fmt::print("Logger {} install failed.\n",name);
            continue;
        }
        logger->Init(log_file_path, name);
        logger->real_logger->set_level(level);
    }
}

Result SetLoggerLogLevel(const std::string& logger_name, spdlog::level::level_enum level) {
 std::unordered_map<std::string, bool> loggers_to_set;
    if (logger_name == "all") {
        loggers_to_set = {{"Default", false}, {"TaskScheduler", false}, {"CranedKeeper", false}};
    } else if (logger_name == "default") {
        loggers_to_set = {{"Default", false}};
    } else if (logger_name == "taskScheduler") {
        loggers_to_set = {{"TaskScheduler", false}};
    } else if (logger_name == "cranedkeeper") {
         loggers_to_set = {{"CranedKeeper", false}};
    } else {
        return Result{false, fmt::format("logger {} not found\n", logger_name)};
    }

    for (auto& [name, result] : loggers_to_set) {
        auto logger = spdlog::get(name);
        if (logger == nullptr) {
            result = false;
        } else {
            logger->set_level(level);
            result = true;
        }
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
    } else if (str_level == "critical") {
        *out_Level = spdlog::level::critical;
    } else {
        return false;
    }

    return true;
}

std::shared_ptr<spdlog::logger> GetLoggerByName(const std::string& logger_name) {
    static std::shared_ptr<spdlog::logger> default_logger = spdlog::get("Default");
    static std::shared_ptr<spdlog::logger> cranedkeeper_logger = spdlog::get("CranedKeeper");
    static std::shared_ptr<spdlog::logger> taskscheduler_logger = spdlog::get("TaskScheduler");

    if (logger_name == "Default") {
        return default_logger;
    } else if (logger_name == "CranedKeeper") {
        return cranedkeeper_logger;
    } else if (logger_name == "TaskScheduler") {
        return taskscheduler_logger;
    } else {
        return nullptr;
    }
}
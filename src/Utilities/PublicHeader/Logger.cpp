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
static std::unordered_set<std::string> g_logger_array;

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
                const std::string& log_file_path) {
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
        g_logger_array.insert(name);
    }
}

Result SetLoggerLogLevel(const std::string& logger_name, spdlog::level::level_enum level) {
    std::unordered_set<std::string> loggers_to_set;
    if (logger_name == "all") {
        loggers_to_set = g_logger_array;
    } else if (g_logger_array.find(logger_name) != g_logger_array.end()) {
        loggers_to_set.insert(logger_name);
    } else {
        return Result{false, fmt::format("logger {} not found", logger_name)};
    }

    std::unordered_set<std::string> success_loggers;
    std::unordered_set<std::string> failed_loggers;
    for (const auto& name : loggers_to_set) {
        auto logger = spdlog::get(name);
        if (logger) {
            logger->set_level(level);
            success_loggers.insert(name);
        } else {
            failed_loggers.insert(name);
        }
    }
    std::string final_message;
    if (!success_loggers.empty()) {
        final_message += fmt::format("\nLoggers {} set successfully",
         fmt::join(success_loggers, ", "));
    }
    if (!failed_loggers.empty()) {
        final_message += fmt::format("\nLoggers {} set failed",
         fmt::join(failed_loggers, ", "));
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
    static std::shared_ptr<spdlog::logger> default_logger = spdlog::get("default");
    static std::shared_ptr<spdlog::logger> cranedkeeper_logger = spdlog::get("cranedkeeper");
    static std::shared_ptr<spdlog::logger> taskscheduler_logger = spdlog::get("taskscheduler");

    if (logger_name == "default") {
        return default_logger;
    } else if (logger_name == "cranedkeeper") {
        return cranedkeeper_logger;
    } else if (logger_name == "taskscheduler") {
        return taskscheduler_logger;
    } else {
        return nullptr;
    }
}
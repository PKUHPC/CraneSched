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

#include <spdlog/fmt/bundled/format.h>

#include <source_location>

// For better logging inside lambda functions
#if defined(__clang__) || defined(__GNUC__) || defined(__GNUG__)
#  define __FUNCTION__ __PRETTY_FUNCTION__
#endif

#include "PublicHeader.h"

#define CRANE_LOG_LEVEL_TRACE 0
#define CRANE_LOG_LEVEL_DEBUG 1
#define CRANE_LOG_LEVEL_INFO 2
#define CRANE_LOG_LEVEL_WARN 3
#define CRANE_LOG_LEVEL_ERROR 4
#define CRANE_LOG_LEVEL_CRITICAL 5
#define CRANE_LOG_LEVEL_OFF 6

#if !defined(CRANE_LOG_LEVEL)
#  if defined(NDEBUG)
#    define CRANE_LOG_LEVEL CRANE_LOG_LEVEL_INFO
#  else
#    define CRANE_LOG_LEVEL CRANE_LOG_LEVEL_TRACE
#  endif
#endif

#define SPDLOG_ACTIVE_LEVEL CRANE_LOG_LEVEL

#include <spdlog/fmt/fmt.h>
#include <spdlog/fmt/ranges.h>
#include <spdlog/spdlog.h>

// Must be after the static log level definition
#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

//cranectld spdlog sink
std::shared_ptr<spdlog::logger> GetCtldDefaultLogger();
std::shared_ptr<spdlog::logger> GetCtldTaskSchedulerLogger();
std::shared_ptr<spdlog::logger> GetCtldCranedKeeperLogger();

//craned spdlog sink
std::shared_ptr<spdlog::logger> GetCranedDefaultLogger();

// default cranetld log
#define CRANECTLD_TRACE(...) SPDLOG_LOGGER_TRACE(GetCtldDefaultLogger(), __VA_ARGS__);
#define CRANECTLD_DEBUG(...) SPDLOG_LOGGER_DEBUG(GetCtldDefaultLogger(), __VA_ARGS__);
#define CRANECTLD_INFO(...) SPDLOG_LOGGER_INFO(GetCtldDefaultLogger(), __VA_ARGS__);
#define CRANECTLD_WARN(...) SPDLOG_LOGGER_WARN(GetCtldDefaultLogger(), __VA_ARGS__);
#define CRANECTLD_ERROR(...) SPDLOG_LOGGER_ERROR(GetCtldDefaultLogger(), __VA_ARGS__);
#define CRANECTLD_CRITICAL(...) SPDLOG_LOGGER_CRITICAL(GetCtldDefaultLogger(), __VA_ARGS__);

// default craned log
#define CRANED_TRACE(...) \
  if (auto logger = GetCranedDefaultLogger()) { \
    SPDLOG_LOGGER_TRACE(logger, __VA_ARGS__); \
  }

#define CRANED_DEBUG(...) \
  if (auto logger = GetCranedDefaultLogger()) { \
    SPDLOG_LOGGER_DEBUG(logger, __VA_ARGS__); \
  }

#define CRANED_INFO(...) \
  if (auto logger = GetCranedDefaultLogger()) { \
    SPDLOG_LOGGER_INFO(logger, __VA_ARGS__); \
  }

#define CRANED_WARN(...) \
  if (auto logger = GetCranedDefaultLogger()) { \
    SPDLOG_LOGGER_WARN(logger, __VA_ARGS__); \
  }

#define CRANED_ERROR(...) \
  if (auto logger = GetCranedDefaultLogger()) { \
    SPDLOG_LOGGER_ERROR(logger, __VA_ARGS__); \
  }

#define CRANED_CRITICAL(...) \
  if (auto logger = GetCranedDefaultLogger()) { \
    SPDLOG_LOGGER_CRITICAL(logger, __VA_ARGS__); \
  }

#define CRANE_LOG_LOC_CALL(loc, level, ...)                             \
  spdlog::default_logger_raw()->log(                                    \
      spdlog::source_loc{loc.file_name(), static_cast<int>(loc.line()), \
                         loc.function_name()},                          \
      level, __VA_ARGS__)

#if CRANE_ACTIVE_LEVEL <= CRANE_LEVEL_TRACE
#  define CRANE_TRACE_LOC(loc, ...) \
    CRANE_LOG_LOC_CALL(loc, spdlog::level::trace, __VA_ARGS__)
#else
#  define CRANE_TRACE_LOC(loc, ...) (void)0
#endif

#if CRANE_ACTIVE_LEVEL <= CRANE_LEVEL_DEBUG
#  define CRANE_DEBUG_LOC(loc, ...) \
    CRANE_LOG_LOC_CALL(loc, spdlog::level::debug, __VA_ARGS__)
#else
#  define CRANE_DEBUG_LOC(loc, ...) (void)0
#endif

#if CRANE_ACTIVE_LEVEL <= CRANE_LEVEL_INFO
#  define CRANE_INFO_LOC(loc, ...) \
    CRANE_LOG_LOC_CALL(loc, spdlog::level::info, __VA_ARGS__)
#else
#  define CRANE_INFO_LOC(loc, ...) (void)0
#endif

#if CRANE_ACTIVE_LEVEL <= CRANE_LEVEL_WARN
#  define CRANE_WARN_LOC(loc, ...) \
    CRANE_LOG_LOC_CALL(loc, spdlog::level::warn, __VA_ARGS__)
#else
#  define CRANE_WARN_LOC(loc, ...) (void)0
#endif

#if CRANE_ACTIVE_LEVEL <= CRANE_LEVEL_ERROR
#  define CRANE_ERROR_LOC(loc, ...) \
    CRANE_LOG_LOC_CALL(loc, spdlog::level::err, __VA_ARGS__)
#else
#  define CRANE_ERROR_LOC(loc, ...) (void)0
#endif

#if CRANE_ACTIVE_LEVEL <= CRANE_LEVEL_CRITICAL
#  define CRANE_CRITICAL_LOC(loc, ...) \
    CRANE_LOG_LOC_CALL(loc, spdlog::level::critical, __VA_ARGS__)
#else
#  define CRANE_CRITICAL_LOC(loc, ...) (void)0
#endif

#ifndef NDEBUG
#  define CRANE_ASSERT_MSG_VA(condition, message, ...)                    \
    do {                                                                  \
      if (!(condition)) {                                                 \
        CRANECTLD_CRITICAL("Assertion failed: \"" #condition "\": " #message, \
                       __VA_ARGS__);                                      \
        std::terminate();                                                 \
      }                                                                   \
    } while (false)

#  define CRANE_ASSERT_MSG(condition, message)                             \
    do {                                                                   \
      if (!(condition)) {                                                  \
        CRANECTLD_CRITICAL("Assertion failed: \"" #condition "\": " #message); \
        std::terminate();                                                  \
      }                                                                    \
    } while (false)

#  define CRANE_ASSERT(condition)                               \
    do {                                                        \
      if (!(condition)) {                                       \
        CRANECTLD_CRITICAL("Assertion failed: \"" #condition "\""); \
        std::terminate();                                       \
      }                                                         \
    } while (false)
#else
#  define CRANE_ASSERT_MSG_VA(condition, message, ...) \
    do {                                               \
    } while (false)
#  define CRANE_ASSERT_MSG(condition, message) \
    do {                                       \
    } while (false)

#  define CRANE_ASSERT(condition) \
    do {                          \
    } while (false)
#endif

void InitLogger(const std::map<std::string, spdlog::level::level_enum>& logLevels,
                const std::string& log_file_path,
                const bool cranectld_flag);

void FindLoggerValidLevel(const std::map<std::string, spdlog::level::level_enum>& logLevels, 
                          const std::string& loggerName,
                          spdlog::level::level_enum *out_level);

bool SetLoggerLogLevel(const std::string& logger_name, spdlog::level::level_enum level);

bool StrToLogLevel(const std::string& str_level, spdlog::level::level_enum *out_Level);



// Custom type formatting
namespace fmt {

template <>
struct formatter<cpu_t> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext &ctx) {
    return ctx.begin();
  };

  template <typename FormatContext>
  auto format(const cpu_t &v, FormatContext &ctx) {
    return fmt::format_to(ctx.out(), "{:.2f}", static_cast<double>(v));
  }
};

}  // namespace fmt

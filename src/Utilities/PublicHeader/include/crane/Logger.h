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

constexpr static const char *kDefaultLoggerName = "Default";

#define CRANE_DEFAULT_TRACE(...) SPDLOG_TRACE(__VA_ARGS__)
#define CRANE_DEFAULT_DEBUG(...) SPDLOG_DEBUG(__VA_ARGS__)
#define CRANE_DEFAULT_INFO(...) SPDLOG_INFO(__VA_ARGS__)
#define CRANE_DEFAULT_WARN(...) SPDLOG_WARN(__VA_ARGS__)
#define CRANE_DEFAULT_ERROR(...) SPDLOG_ERROR(__VA_ARGS__)
#define CRANE_DEFAULT_CRITICAL(...) SPDLOG_CRITICAL(__VA_ARGS__)

#define CRANE_LOGGER_TRACE(logger, ...) SPDLOG_LOGGER_TRACE(logger, __VA_ARGS__)
#define CRANE_LOGGER_DEBUG(logger, ...) SPDLOG_LOGGER_DEBUG(logger, __VA_ARGS__)
#define CRANE_LOGGER_INFO(logger, ...) SPDLOG_LOGGER_INFO(logger, __VA_ARGS__)
#define CRANE_LOGGER_WARN(logger, ...) SPDLOG_LOGGER_WARN(logger, __VA_ARGS__)
#define CRANE_LOGGER_ERROR(logger, ...) SPDLOG_LOGGER_ERROR(logger, __VA_ARGS__)
#define CRANE_LOGGER_CRITICAL(logger, ...) \
  SPDLOG_LOGGER_CRITICAL(logger, __VA_ARGS__)

#define CRANE_TRACE(...) CRANE_DEFAULT_TRACE(__VA_ARGS__)
#define CRANE_DEBUG(...) CRANE_DEFAULT_DEBUG(__VA_ARGS__)
#define CRANE_INFO(...) CRANE_DEFAULT_INFO(__VA_ARGS__)
#define CRANE_WARN(...) CRANE_DEFAULT_WARN(__VA_ARGS__)
#define CRANE_ERROR(...) CRANE_DEFAULT_ERROR(__VA_ARGS__)
#define CRANE_CRITICAL(...) CRANE_DEFAULT_CRITICAL(__VA_ARGS__)

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
        CRANE_CRITICAL("Assertion failed: \"" #condition "\": " #message, \
                       __VA_ARGS__);                                      \
        std::terminate();                                                 \
      }                                                                   \
    } while (false)

#  define CRANE_ASSERT_MSG(condition, message)                             \
    do {                                                                   \
      if (!(condition)) {                                                  \
        CRANE_CRITICAL("Assertion failed: \"" #condition "\": " #message); \
        std::terminate();                                                  \
      }                                                                    \
    } while (false)

#  define CRANE_ASSERT(condition)                               \
    do {                                                        \
      if (!(condition)) {                                       \
        CRANE_CRITICAL("Assertion failed: \"" #condition "\""); \
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

struct StaticLoggerRegister;

class LoggerFactory {
  friend struct StaticLoggerRegister;

 private:
  constinit inline static spdlog::async_overflow_policy s_overflow_policy_{
      spdlog::async_overflow_policy::overrun_oldest};

 public:
  static void InitAndSetDefaultLogger(spdlog::level::level_enum level,
                                      const std::string &log_file_path);

  static std::shared_ptr<spdlog::logger> RegisterLogger(
      const std::string &name, spdlog::level::level_enum level);

  static void SetLoggerLevel(const std::string &name,
                             spdlog::level::level_enum level);

 private:
  static inline std::unordered_map<std::string_view, spdlog::level::level_enum>
      s_static_registered_logger_level_map_;

  static inline std::mutex s_logger_map_mtx_;
  static inline std::unordered_map<std::string, std::shared_ptr<spdlog::logger>>
      s_logger_map_ ABSL_GUARDED_BY(s_logger_map_mtx_);

  static inline spdlog::sinks_init_list s_sinks_init_list_;

  static inline std::shared_ptr<spdlog::sinks::sink> s_file_sink_;
  static inline std::shared_ptr<spdlog::sinks::sink> s_console_sink_;
};

struct StaticLoggerRegister {
  StaticLoggerRegister(constexpr std::string_view name,
                       spdlog::level::level_enum level = spdlog::level::info) {
    s_logger_name_ = name;
    LoggerFactory::s_static_registered_logger_level_map_.emplace(name, level);
  }

  static spdlog::logger *GetLogger() {
    static spdlog::logger *logger = GetLoggerFromFactory_();
    return logger;
  }

 private:
  static inline std::string_view s_logger_name_;

  static spdlog::logger *GetLoggerFromFactory_() {
    std::lock_guard lock(LoggerFactory::s_logger_map_mtx_);
    spdlog::logger *logger =
        LoggerFactory::s_logger_map_.at(s_logger_name_.data()).get();
    ABSL_ASSERT(logger != nullptr);
    return logger;
  }
};

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

namespace internal {
inline StaticLoggerRegister g_default_logger_register(kDefaultLoggerName);
}
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

#define CRANE_TRACE(...) SPDLOG_TRACE(__VA_ARGS__)
#define CRANE_DEBUG(...) SPDLOG_DEBUG(__VA_ARGS__)
#define CRANE_INFO(...) SPDLOG_INFO(__VA_ARGS__)
#define CRANE_WARN(...) SPDLOG_WARN(__VA_ARGS__)
#define CRANE_ERROR(...) SPDLOG_ERROR(__VA_ARGS__)
#define CRANE_CRITICAL(...) SPDLOG_CRITICAL(__VA_ARGS__)

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

void InitLogger(spdlog::level::level_enum level,
                const std::string &log_file_path);

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

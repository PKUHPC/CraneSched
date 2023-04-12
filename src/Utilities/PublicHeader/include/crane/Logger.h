#pragma once

#include <source_location>

// For better logging inside lambda functions
#if defined(__clang__) || defined(__GNUC__) || defined(__GNUG__)
#  define __FUNCTION__ __PRETTY_FUNCTION__
#endif

#define SPDLOG_ACTIVE_LEVEL CRANE_LOG_LEVEL

#include <spdlog/spdlog.h>

#include "PublicHeader.h"

#define CRANE_LOG_LEVEL_TRACE SPDLOG_LEVEL_TRACE
#define CRANE_LOG_LEVEL_DEBUG SPDLOG_LEVEL_DEBUG
#define CRANE_LOG_LEVEL_INFO SPDLOG_LEVEL_INFO
#define CRANE_LOG_LEVEL_WARN SPDLOG_LEVEL_WARN
#define CRANE_LOG_LEVEL_ERROR SPDLOG_LEVEL_ERROR
#define CRANE_LOG_LEVEL_CRITICAL SPDLOG_LEVEL_CRITICAL
#define CRANE_LOG_LEVEL_OFF SPDLOG_LEVEL_OFF

#if !defined(CRANE_LOG_LEVEL)
#  define CRANE_LOG_LEVEL CRANE_LOG_LEVEL_INFO
#endif

#define SPDLOG_ACTIVE_LEVEL CRANE_LOG_LEVEL

#include <spdlog/fmt/fmt.h>
#include <spdlog/fmt/ranges.h>
#include <spdlog/spdlog.h>

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
#  define CRANE_ASSERT_MSG(condition, message) \
    do {                                       \
    } while (false)

#  define CRANE_ASSERT(condition) \
    do {                          \
    } while (false)
#endif

namespace Internal {

struct StaticLogFormatSetter {
  StaticLogFormatSetter() { spdlog::set_pattern("[%^%L%$ %C-%m-%d %s:%#] %v"); }
};

// Set the global spdlog pattern in global variable initialization.
[[maybe_unused]] inline StaticLogFormatSetter _static_formatter_setter;

}  // namespace Internal

#pragma once

#include <spdlog/spdlog.h>

#include "PublicHeader.h"

#define CRANE_TRACE(...) SPDLOG_TRACE(__VA_ARGS__)
#define CRANE_DEBUG(...) SPDLOG_DEBUG(__VA_ARGS__)
#define CRANE_INFO(...) SPDLOG_INFO(__VA_ARGS__)
#define CRANE_WARN(...) SPDLOG_WARN(__VA_ARGS__)
#define CRANE_ERROR(...) SPDLOG_ERROR(__VA_ARGS__)
#define CRANE_CRITICAL(...) SPDLOG_CRITICAL(__VA_ARGS__)

#ifndef NDEBUG
#define CRANE_ASSERT_MSG_VA(condition, message, ...)                    \
  do {                                                                  \
    if (!(condition)) {                                                 \
      CRANE_CRITICAL("Assertion failed: \"" #condition "\": " #message, \
                     __VA_ARGS__);                                      \
      std::terminate();                                                 \
    }                                                                   \
  } while (false)

#define CRANE_ASSERT_MSG(condition, message)                             \
  do {                                                                   \
    if (!(condition)) {                                                  \
      CRANE_CRITICAL("Assertion failed: \"" #condition "\": " #message); \
      std::terminate();                                                  \
    }                                                                    \
  } while (false)

#define CRANE_ASSERT(condition)                               \
  do {                                                        \
    if (!(condition)) {                                       \
      CRANE_CRITICAL("Assertion failed: \"" #condition "\""); \
      std::terminate();                                       \
    }                                                         \
  } while (false)
#else
#define CRANE_ASSERT_MSG(condition, message) \
  do {                                       \
  } while (false)

#define CRANE_ASSERT(condition) \
  do {                          \
  } while (false)
#endif

/**
 * Custom formatter for CranedId in fmt.
 */
template <>
struct fmt::formatter<CranedId> {
  constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const CranedId& id, FormatContext& ctx) -> decltype(ctx.out()) {
    // ctx.out() is an output iterator to write to.
    return format_to(ctx.out(), "({}, {})", id.partition_id, id.craned_index);
  }
};

namespace Internal {

struct StaticLogFormatSetter {
  StaticLogFormatSetter() { spdlog::set_pattern("[%^%L%$ %C-%m-%d %s:%#] %v"); }
};

// Set the global spdlog pattern in global variable initialization.
[[maybe_unused]] inline StaticLogFormatSetter _static_formatter_setter;

}  // namespace Internal

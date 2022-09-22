#pragma once

#include <spdlog/spdlog.h>

namespace Internal {

class EnableSpdlogTraceLevel {
 public:
  EnableSpdlogTraceLevel() { spdlog::set_level(spdlog::level::trace); }
};

[[maybe_unused]] inline EnableSpdlogTraceLevel enableSpdlogTraceLevel;

}  // namespace Internal
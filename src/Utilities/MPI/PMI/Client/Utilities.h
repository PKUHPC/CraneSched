/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 */
#pragma once
#include <charconv>
#include <optional>
#include <string_view>

namespace crane::pmi {
inline std::optional<int> ParseInt(std::string_view value) {
  int result;
  auto [end, error] =
      std::from_chars(value.data(), value.data() + value.size(), result);
  if (error != std::errc{} || end != value.data() + value.size())
    return std::nullopt;
  return result;
}
}  // namespace crane::pmi

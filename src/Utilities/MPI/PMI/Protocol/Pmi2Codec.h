/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 */

#pragma once

#include <map>
#include <optional>
#include <string>
#include <string_view>

namespace crane::pmi2 {

using Fields = std::map<std::string, std::string>;

class Codec final {
 public:
  static constexpr size_t kMaxFrame = 64 * 1024;
  static constexpr size_t kMaxKey = 64;
  static constexpr size_t kMaxValue = 1024;

  static std::optional<Fields> ParseInit(std::string_view line);
  static std::optional<Fields> ParseFrame(std::string_view body);
  static std::string EncodeInit(const Fields& fields);
  static std::optional<std::string> EncodeFrame(const Fields& fields);
};

}  // namespace crane::pmi2

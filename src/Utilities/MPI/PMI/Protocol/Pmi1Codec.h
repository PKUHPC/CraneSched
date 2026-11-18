/**
 * Copyright (c) 2026 Peking University and Peking University
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

#include <map>
#include <optional>
#include <string>
#include <string_view>

namespace pmi::pmi1 {

using Fields = std::map<std::string, std::string>;

class Codec {
 public:
  static constexpr size_t kMaxLine = 64 * 1024;
  static constexpr size_t kMaxKey = 64;
  static constexpr size_t kMaxValue = 1024;

  static std::optional<Fields> ParseLine(std::string_view line);
  static std::optional<std::string> EncodeLine(std::string_view command,
                                               const Fields& fields);
};

}  // namespace pmi::pmi1

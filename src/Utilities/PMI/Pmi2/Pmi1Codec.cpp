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

#include "Pmi1Codec.h"

namespace pmi2::pmi1 {

std::optional<Fields> Codec::ParseLine(std::string_view line) {
  if (line.empty() || line.size() > kMaxLine || line.back() != '\n' ||
      line.find('\0') != std::string_view::npos ||
      line.find('\n') != line.size() - 1)
    return std::nullopt;
  line.remove_suffix(1);
  if (!line.empty() && line.back() == '\r') line.remove_suffix(1);

  Fields fields;
  while (!line.empty()) {
    while (!line.empty() && line.front() == ' ') line.remove_prefix(1);
    if (line.empty()) break;
    auto end = line.find(' ');
    auto token = line.substr(0, end);
    auto equal = token.find('=');
    if (equal == std::string_view::npos || equal == 0 || equal >= kMaxKey ||
        token.size() - equal - 1 >= kMaxValue)
      return std::nullopt;
    if (!fields
             .emplace(std::string(token.substr(0, equal)),
                      std::string(token.substr(equal + 1)))
             .second)
      return std::nullopt;
    if (end == std::string_view::npos) break;
    line.remove_prefix(end + 1);
  }
  if (!fields.contains("cmd")) return std::nullopt;
  return fields;
}

std::optional<std::string> Codec::EncodeLine(std::string_view command,
                                             const Fields& fields) {
  if (command.empty() || command.size() >= kMaxValue ||
      command.find_first_of(" =\t\r\n") != std::string_view::npos ||
      command.find('\0') != std::string_view::npos)
    return std::nullopt;

  std::string line = "cmd=";
  line += command;
  for (const auto& [key, value] : fields) {
    if (key.empty() || key.size() >= kMaxKey || value.size() >= kMaxValue ||
        key.find_first_of(" =\t\r\n") != std::string::npos ||
        key.find('\0') != std::string::npos ||
        value.find_first_of(" \t\r\n") != std::string::npos ||
        value.find('\0') != std::string::npos)
      return std::nullopt;
    line.push_back(' ');
    line += key;
    line.push_back('=');
    line += value;
  }
  line.push_back('\n');
  if (line.size() > kMaxLine) return std::nullopt;
  return line;
}

}  // namespace pmi2::pmi1

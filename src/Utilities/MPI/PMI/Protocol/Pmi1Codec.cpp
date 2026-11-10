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

#include "Protocol/Pmi1Codec.h"

namespace pmi::pmi1 {

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
    auto equal = line.find('=');
    auto key = line.substr(0, equal);
    if (equal == std::string_view::npos || key.empty() ||
        key.size() >= kMaxKey ||
        key.find_first_of(" \t\r") != std::string_view::npos)
      return std::nullopt;
    line.remove_prefix(equal + 1);
    std::string value;
    size_t end = 0;
    // PMI KVS puts carry value last. Open MPI s1 uses literal spaces for
    // base64 padding; retain the remainder instead of splitting that payload.
    // MPICH wire clients also accept backslash-escaped spaces in replies.
    while (end < line.size() && (key == "value" || line[end] != ' ')) {
      char ch = line[end++];
      if (ch == '\\') {
        if (end == line.size()) return std::nullopt;
        ch = line[end++];
      }
      if (ch == '\t' || ch == '\r' || value.size() + 1 >= kMaxValue)
        return std::nullopt;
      value.push_back(ch);
    }
    if (!fields.emplace(std::string(key), std::move(value)).second)
      return std::nullopt;
    line.remove_prefix(end);
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
        value.find_first_of("\t\r\n") != std::string::npos ||
        value.find('\0') != std::string::npos)
      return std::nullopt;
    line.push_back(' ');
    line += key;
    line.push_back('=');
    for (char ch : value) {
      if (ch == ' ' || ch == '\\') line.push_back('\\');
      line.push_back(ch);
    }
  }
  line.push_back('\n');
  if (line.size() > kMaxLine) return std::nullopt;
  return line;
}

}  // namespace pmi::pmi1

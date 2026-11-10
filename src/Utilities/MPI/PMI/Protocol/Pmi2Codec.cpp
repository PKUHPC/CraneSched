/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 */

#include "Pmi2Codec.h"

#include <charconv>

namespace crane::pmi2 {
namespace {

std::optional<Fields> Parse(std::string_view input, char delimiter) {
  Fields fields;
  if (input.find('\0') != std::string_view::npos) return std::nullopt;
  size_t offset = 0;
  while (offset < input.size()) {
    while (offset < input.size() && input[offset] == delimiter) ++offset;
    if (offset == input.size()) break;

    const size_t key_begin = offset;
    while (offset < input.size() && input[offset] != '=' &&
           input[offset] != delimiter)
      ++offset;
    if (offset == key_begin || offset == input.size() || input[offset] != '=')
      return std::nullopt;
    std::string key(input.substr(key_begin, offset - key_begin));
    if (key.size() >= Codec::kMaxKey) return std::nullopt;
    ++offset;

    std::string value;
    while (offset < input.size()) {
      if (input[offset] != delimiter) {
        value.push_back(input[offset++]);
        continue;
      }
      if (delimiter == ';' && offset + 1 < input.size() &&
          input[offset + 1] == ';') {
        value.push_back(';');
        offset += 2;
        continue;
      }
      ++offset;
      break;
    }
    if (value.size() >= Codec::kMaxValue ||
        !fields.emplace(std::move(key), std::move(value)).second)
      return std::nullopt;
  }
  if (!fields.contains("cmd")) return std::nullopt;
  return fields;
}

}  // namespace

std::optional<Fields> Codec::ParseInit(std::string_view line) {
  if (line.ends_with('\n')) line.remove_suffix(1);
  return Parse(line, ' ');
}

std::optional<Fields> Codec::ParseFrame(std::string_view body) {
  return Parse(body, ';');
}

std::string Codec::EncodeInit(const Fields& fields) {
  std::string result;
  for (const auto& [key, value] : fields) {
    if (!result.empty()) result.push_back(' ');
    result += key;
    result.push_back('=');
    result += value;
  }
  result.push_back('\n');
  return result;
}

std::optional<std::string> Codec::EncodeFrame(const Fields& fields) {
  auto command = fields.find("cmd");
  if (command == fields.end() || command->second.empty() ||
      command->second.size() >= kMaxValue)
    return std::nullopt;
  std::string body = "cmd=" + command->second + ';';
  for (const auto& [key, value] : fields) {
    if (key == "cmd") continue;
    if (key.empty() || key.size() >= kMaxKey || value.size() >= kMaxValue ||
        key.find_first_of("=;") != std::string::npos)
      return std::nullopt;
    body += key;
    body.push_back('=');
    for (char ch : value) {
      body.push_back(ch);
      if (ch == ';') body.push_back(';');
    }
    body.push_back(';');
  }
  if (body.size() > kMaxFrame) return std::nullopt;
  return body;
}

}  // namespace crane::pmi2

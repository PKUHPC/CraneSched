/**
 * Copyright (c) 2024 Peking University and Peking University
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

#include <absl/strings/ascii.h>
#include <absl/strings/str_join.h>
#include <re2/re2.h>
#include <spdlog/fmt/fmt.h>

#include <charconv>
#include <filesystem>
#include <fstream>
#include <list>
#include <ranges>
#include <string>

#include "crane/PublicHeader.h"

namespace util {

template <typename D = std::string, typename T1, typename T2>
requires requires(const T1 &node) {
  { node.template as<D>() } -> std::convertible_to<D>;
} && std::convertible_to<T2, D>
D value_or(const T1 &node, const T2 &default_value) {
  return node ? node.template as<D>() : default_value;
}

std::string ReadFileIntoString(std::filesystem::path const &p);

std::string ReadableMemory(uint64_t memory_bytes);

bool ParseHostList(const std::string &host_str,
                   std::list<std::string> *host_list);

bool FoundFirstNumberWithoutBrackets(const std::string &input, int *start,
                                     int *end);

std::string RemoveBracketsWithoutDashOrComma(const std::string &input);

bool HostNameListToStr_(std::list<std::string> &host_list,
                        std::list<std::string> *res_list);

template <std::ranges::range T>
std::string HostNameListToStr(T const &host_list)
  requires std::same_as<std::ranges::range_value_t<T>, std::string>
{
  std::list<std::string> source_list{host_list.begin(), host_list.end()};
  while (true) {
    std::list<std::string> res_list;
    if (HostNameListToStr_(source_list, &res_list)) {
      res_list.sort();
      std::string host_name_str{absl::StrJoin(res_list, ",")};
      // Remove brackets containing single numbers
      // Temporary "[]" can be used to mark that the number has been processed,
      // so "[]" cannot be removed during the processing and can only be deleted
      // at the end
      return RemoveBracketsWithoutDashOrComma(host_name_str);
    }
    source_list = res_list;
  }
}

void SetCurrentThreadName(const std::string &name);

bool ConvertStringToInt64(const std::string &s, int64_t *val);

std::string ReadableTypedDeviceMap(const DeviceMap &dedicated_resource);
std::string ReadableDresInNode(const ResourceInNode &dedicated_resource);

std::string ReadableGrpcDresInNode(
    const crane::grpc::DedicatedResourceInNode &dres_in_node);

std::string GenerateCommaSeparatedString(const int val);

}  // namespace util
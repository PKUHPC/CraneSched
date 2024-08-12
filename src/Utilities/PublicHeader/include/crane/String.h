/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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
#include <vector>

#include "crane/PublicHeader.h"

namespace util {

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

std::string ReadableDres(
    const crane::grpc::DedicatedResourceInNode &dres_in_node);

std::string GenerateCommaSeparatedString(const int val);

}  // namespace util
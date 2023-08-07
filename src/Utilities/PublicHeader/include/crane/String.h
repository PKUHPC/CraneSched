#pragma once

#include <absl/strings/ascii.h>
#include <spdlog/fmt/fmt.h>

#include <boost/algorithm/string.hpp>
#include <filesystem>
#include <fstream>
#include <list>
#include <queue>
#include <ranges>
#include <regex>
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
      std::string host_name_str{boost::join(res_list, ",")};
      return host_name_str;
    }
    source_list = res_list;
  }
}


std::string GenerateCudaVisiableDeviceStr(const uint64_t count);

}  // namespace util
#pragma once

#include <absl/strings/ascii.h>
#include <spdlog/fmt/fmt.h>

#include <boost/algorithm/string.hpp>
#include <list>
#include <queue>
#include <ranges>
#include <regex>
#include <string>
#include <vector>

#include "crane/PublicHeader.h"

namespace util {

std::string ReadableMemory(uint64_t memory_bytes);

bool ParseHostList(const std::string &host_str,
                   std::list<std::string> *host_list);

template <std::ranges::range T>
std::string HostNameListToStr(T const &host_list)
  requires std::same_as<std::ranges::range_value_t<T>, std::string>
{
  std::map<std::string, std::list<std::string>> host_map;
  std::string host_name_str;

  size_t sz = std::ranges::size(host_list);
  if (sz == 0)
    return host_name_str;
  else if (sz == 1)
    return *std::ranges::begin(host_list);

  for (const auto &host : host_list) {
    if (host.empty()) continue;
    std::regex regex(R"(\d+$)");
    std::smatch match;
    if (std::regex_search(host, match, regex)) {
      std::string num_str = match[0],
                  head_str = host.substr(0, match.position(0));
      auto iter = host_map.find(head_str);
      if (iter == host_map.end()) {
        std::list<std::string> list;
        host_map[head_str] = list;
      }
      host_map[head_str].push_back(num_str);
    } else {
      host_name_str += host;
      host_name_str += ",";
    }
  }

  for (auto &&iter : host_map) {
    if (iter.second.size() == 1) {
      host_name_str += iter.first;
      host_name_str += iter.second.front();
      host_name_str += ",";
      continue;
    }
    host_name_str += iter.first;
    host_name_str += "[";
    iter.second.sort();
    iter.second.unique();
    int first = -1, last = -1, num;
    std::string first_str, last_str;
    for (const auto &num_str : iter.second) {
      num = stoi(num_str);
      if (first < 0) {  // init the head
        first = last = num;
        first_str = last_str = num_str;
        continue;
      }
      if (num == last + 1) {  // update the tail
        last++;
        last_str = num_str;
      } else {
        if (first == last) {
          host_name_str += first_str;
        } else {
          host_name_str += first_str;
          host_name_str += "-";
          host_name_str += last_str;
        }
        host_name_str += ",";
        first = last = num;
        first_str = last_str = num_str;
      }
    }
    if (first == last) {
      host_name_str += first_str;
    } else {
      host_name_str += first_str;
      host_name_str += "-";
      host_name_str += last_str;
    }
    host_name_str += "],";
  }
  host_name_str.pop_back();
  return host_name_str;
}

}  // namespace util
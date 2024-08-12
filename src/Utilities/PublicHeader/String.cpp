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

#include "crane/String.h"

#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>
#include <pthread.h>

#include "crane/Logger.h"

namespace util {

std::string ReadFileIntoString(std::filesystem::path const &p) {
  std::ifstream file(p, std::ios::in | std::ios::binary);
  size_t sz = std::filesystem::file_size(p);
  std::string str(sz, '\0');
  file.read(&str[0], sz);
  return str;
}

std::string ReadableMemory(uint64_t memory_bytes) {
  if (memory_bytes < 1024)
    return fmt::format("{}B", memory_bytes);
  else if (memory_bytes < 1024 * 1024)
    return fmt::format("{}K", memory_bytes / 1024);
  else if (memory_bytes < 1024 * 1024 * 1024)
    return fmt::format("{}M", memory_bytes / 1024 / 1024);
  else
    return fmt::format("{}G", memory_bytes / 1024 / 1024 / 1024);
}

bool ParseNodeList(const std::string &node_str,
                   std::list<std::string> *nodelist) {
  static const LazyRE2 brackets_regex = {R"(.*\[(.*)\])"};
  if (!RE2::PartialMatch(node_str, *brackets_regex)) {
    return false;
  }

  std::vector<std::string_view> unit_str_list = absl::StrSplit(node_str, ']');
  std::string_view end_str = unit_str_list[unit_str_list.size() - 1];
  unit_str_list.pop_back();
  std::list<std::string> res_list{""};
  static const LazyRE2 num_regex = {R"(\d+)"};
  static const LazyRE2 scope_regex = {R"(\d+-\d+)"};

  for (const auto &str : unit_str_list) {
    std::vector<std::string_view> node_num =
        absl::StrSplit(str, absl::ByAnyChar("[,"));
    std::list<std::string> unit_list;
    std::string_view head_str = node_num.front();
    size_t len = node_num.size();

    for (size_t i = 1; i < len; i++) {
      if (RE2::FullMatch(node_num[i], *num_regex)) {
        unit_list.emplace_back(fmt::format("{}{}", head_str, node_num[i]));
      } else if (RE2::FullMatch(node_num[i], *scope_regex)) {
        std::vector<std::string_view> loc_index =
            absl::StrSplit(node_num[i], '-');

        size_t l = loc_index[0].length();

        std::from_chars_result convert_result{};
        size_t start;
        convert_result =
            std::from_chars(loc_index[0].data(),
                            loc_index[0].data() + loc_index[0].size(), start);
        if (convert_result.ec != std::errc()) return false;

        size_t end;
        convert_result =
            std::from_chars(loc_index[1].data(),
                            loc_index[1].data() + loc_index[1].size(), end);
        if (convert_result.ec != std::errc()) return false;

        for (size_t j = start; j <= end; j++) {
          std::string s_num = fmt::format("{:0>{}}", std::to_string(j), l);
          unit_list.emplace_back(fmt::format("{}{}", head_str, s_num));
        }
      } else {
        // Format error
        return false;
      }
    }

    std::list<std::string> temp_list;
    for (const auto &left : res_list) {
      for (const auto &right : unit_list) {
        temp_list.emplace_back(fmt::format("{}{}", left, right));
      }
    }
    res_list.assign(temp_list.begin(), temp_list.end());
  }

  if (!end_str.empty()) {
    for (auto &str : res_list) {
      str += end_str;
    }
  }
  nodelist->insert(nodelist->end(), res_list.begin(), res_list.end());

  return true;
}

bool ParseHostList(const std::string &host_str,
                   std::list<std::string> *host_list) {
  std::string name_str;
  name_str.reserve(host_str.size());

  for (auto &i : host_str)
    if (i != ' ') name_str += i;  // remove all spaces

  name_str += ',';  // uniform end format
  std::string name_meta;
  std::list<std::string> str_list;

  std::string char_queue;
  for (const auto &c : name_str) {
    if (c == '[') {
      if (char_queue.empty()) {
        char_queue += c;
      } else {
        CRANE_ERROR("Illegal node name string format: duplicate brackets");
        return false;
      }
    } else if (c == ']') {
      if (char_queue.empty()) {
        CRANE_ERROR("Illegal node name string format: isolated bracket");
        return false;
      } else {
        name_meta += char_queue;
        name_meta += c;
        char_queue.clear();
      }
    } else if (c == ',') {
      if (char_queue.empty()) {
        str_list.emplace_back(name_meta);
        name_meta.clear();
      } else {
        char_queue += c;
      }
    } else {
      if (char_queue.empty()) {
        name_meta += c;
      } else {
        char_queue += c;
      }
    }
  }
  if (!char_queue.empty()) {
    CRANE_ERROR("Illegal node name string format: isolated bracket");
    return false;
  }

  static const LazyRE2 regex = {R"(.*\[(.*)\](\..*)*$)"};
  for (auto &&str : str_list) {
    std::string str_s{absl::StripAsciiWhitespace(str)};
    if (!RE2::FullMatch(str_s, *regex)) {
      host_list->emplace_back(str_s);
    } else {
      if (!ParseNodeList(str_s, host_list)) return false;
    }
  }
  return true;
}

bool HostNameListToStr_(std::list<std::string> &host_list,
                        std::list<std::string> *res_list) {
  std::unordered_map<std::string, std::vector<std::string>> host_map;
  bool res = true;

  size_t sz = host_list.size();
  if (sz == 0)
    return true;
  else if (sz == 1) {
    res_list->emplace_back(host_list.front());
    return true;
  }

  for (const auto &host : host_list) {
    if (host.empty()) continue;

    int start, end;
    if (FoundFirstNumberWithoutBrackets(host, &start, &end)) {
      res = false;
      std::string num_str = host.substr(start, end - start),
                  head_str = host.substr(0, start),
                  tail_str = host.substr(end, host.size());
      std::string key_str = fmt::format("{}<{}", head_str, tail_str);
      if (!host_map.contains(key_str)) {
        std::vector<std::string> list;
        host_map[key_str] = list;
      }
      host_map[key_str].emplace_back(num_str);
    } else {
      res_list->emplace_back(host);
    }
  }

  if (res) return true;

  for (auto &&iter : host_map) {
    std::string host_name_str;

    int delimiter_pos = 0;
    for (const auto &c : iter.first) {
      if (c == '<') {
        break;
      }
      delimiter_pos++;
    }
    host_name_str += iter.first.substr(0, delimiter_pos);  // head
    host_name_str += "[";

    std::sort(iter.second.begin(), iter.second.end(),
              [](std::string &a, std::string &b) {
                if (a.length() != b.length()) {
                  return a.length() < b.length();
                } else {
                  return stoi(a) < stoi(b);
                }
              });
    // delete duplicate elements
    auto new_end = std::unique(iter.second.begin(), iter.second.end());
    iter.second.erase(new_end, iter.second.end());

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
    host_name_str += fmt::format(
        "]{}",
        iter.first.substr(delimiter_pos + 1, iter.first.size()));  // tail
    res_list->emplace_back(host_name_str);
  }

  return res;
}

bool FoundFirstNumberWithoutBrackets(const std::string &input, int *start,
                                     int *end) {
  *start = *end = 0;
  size_t opens = 0;
  int i = 0;

  for (const auto &c : input) {
    if (c == '[') {
      opens++;
    } else if (c == ']') {
      opens--;
    } else {
      if (!opens) {
        if (!(*start) && c >= '0' && c <= '9') {
          *start = i;
        } else if (*start && (c < '0' || c > '9')) {
          *end = i;
          return true;
        }
      }
    }
    i++;
  }

  if (*start) {
    *end = i;
    return true;
  } else {
    return false;
  }
}

std::string RemoveBracketsWithoutDashOrComma(const std::string &input) {
  std::string output = input;
  std::size_t leftBracketPos = 0;
  while ((leftBracketPos = output.find('[', leftBracketPos)) !=
         std::string::npos) {
    std::size_t rightBracketPos = output.find(']', leftBracketPos);
    if (rightBracketPos == std::string::npos) {
      break;
    }
    std::string betweenBrackets =
        output.substr(leftBracketPos + 1, rightBracketPos - leftBracketPos - 1);
    if (betweenBrackets.find('-') == std::string::npos &&
        betweenBrackets.find(',') == std::string::npos) {
      output.erase(rightBracketPos, 1);
      output.erase(leftBracketPos, 1);
    } else {
      leftBracketPos = rightBracketPos + 1;
    }
  }
  return output;
}

void SetCurrentThreadName(const std::string &name) {
  // The thread name is not allowed to exceed 16 characters including '\0'.
  if (name.size() >= 16) {
    CRANE_ERROR("Thread name cannot exceed 16 character!");
    return;
  }

  pthread_setname_np(pthread_self(), name.c_str());
}

bool ConvertStringToInt64(const std::string &s, int64_t *val) {
  std::from_chars_result convert_result{};
  convert_result = std::from_chars(s.data(), s.data() + s.size(), *val);
  return convert_result.ec == std::errc();
}

std::string ReadableTypedDeviceMap(const DeviceMap &device_map) {
  if (device_map.empty()) return "None";

  std::vector<std::string> typed_device_str_vec;
  for (const auto &[dev_name, p] : device_map) {
    const auto &type_size_map = p.second;

    for (const auto &[dev_type, size] : type_size_map) {
      typed_device_str_vec.push_back(
          fmt::format("{}:{}:{}", dev_name, dev_type, size));
    }
  }

  return absl::StrJoin(typed_device_str_vec, ",");
}

std::string ReadableDresInNode(const ResourceInNode &resource_in_node) {
  const DedicatedResourceInNode &dedicated_resource_in_node =
      resource_in_node.dedicated_res;

  if (dedicated_resource_in_node.IsZero()) {
    return "0";
  }
  std::vector<std::string> node_gres_string_vector;
  for (const auto &[device_name, type_slots_map] :
       dedicated_resource_in_node.name_type_slots_map) {
    for (const auto &[device_type, slots] : type_slots_map.type_slots_map) {
      // name:type:count
      node_gres_string_vector.emplace_back(
          fmt::format("{}:{}:{}", device_name, device_type, slots.size()));
    }
  }

  return absl::StrJoin(node_gres_string_vector, ",");
}

std::string ReadableGrpcDresInNode(
    const crane::grpc::DedicatedResourceInNode &dres_in_node) {
  if (dres_in_node.name_type_map_size() == 0) return "None";

  std::vector<std::string> typed_device_str_vec;
  for (const auto &[dev_name, p] : dres_in_node.name_type_map()) {
    const auto &type_size_map = p.type_slots_map();

    for (const auto &[dev_type, slots] : type_size_map)
      typed_device_str_vec.push_back(
          fmt::format("{}:{}:{}", dev_name, dev_type, slots.slots_size()));
  }

  return absl::StrJoin(typed_device_str_vec, ",");
}

std::string GenerateCommaSeparatedString(const int val) {
  std::vector<int> val_vec;
  val_vec.reserve(val);
  for (int i = 0; i < val; ++i) {
    val_vec.push_back(i);
  }
  return absl::StrJoin(val_vec, ",");
}

}  // namespace util

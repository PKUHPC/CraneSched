#include "crane/String.h"

#include <absl/strings/strip.h>

#include <queue>

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
  std::regex brackets_regex(R"(.*\[(.*)\])");
  std::smatch match;
  if (!std::regex_search(node_str, match, brackets_regex)) {
    return false;
  }

  std::vector<std::string> unit_str_list;
  boost::split(unit_str_list, node_str, boost::is_any_of("]"));
  std::string end_str = unit_str_list[unit_str_list.size() - 1];
  unit_str_list.pop_back();
  std::list<std::string> res_list{""};
  std::regex num_regex(R"(\d+)"), scope_regex(R"(\d+-\d+)");

  for (const auto &str : unit_str_list) {
    std::vector<std::string> node_num;
    boost::split(node_num, str, boost::is_any_of("[,"));
    std::list<std::string> unit_list;
    std::string head_str = node_num.front();
    size_t len = node_num.size();

    for (size_t i = 1; i < len; i++) {
      if (std::regex_match(node_num[i], num_regex)) {
        unit_list.emplace_back(fmt::format("{}{}", head_str, node_num[i]));
      } else if (std::regex_match(node_num[i], scope_regex)) {
        std::vector<std::string> loc_index;
        boost::split(loc_index, node_num[i], boost::is_any_of("-"));

        size_t l = loc_index[0].length(), end = std::stoi(loc_index[1]);
        for (size_t j = std::stoi(loc_index[0]); j <= end; j++) {
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
  std::string name_str(host_str);
  name_str += ',';  // uniform end format
  std::string name_meta;
  std::list<std::string> str_list;

  std::queue<char> char_queue;
  for (const auto &c : name_str) {
    if (c == '[') {
      if (char_queue.empty()) {
        char_queue.push(c);
      } else {
        CRANE_ERROR("Illegal node name string format: duplicate brackets");
        return false;
      }
    } else if (c == ']') {
      if (char_queue.empty()) {
        CRANE_ERROR("Illegal node name string format: isolated bracket");
        return false;
      } else {
        while (!char_queue.empty()) {
          name_meta += char_queue.front();
          char_queue.pop();
        }
        name_meta += c;
      }
    } else if (c == ',') {
      if (char_queue.empty()) {
        str_list.emplace_back(name_meta);
        name_meta.clear();
      } else {
        char_queue.push(c);
      }
    } else {
      if (char_queue.empty()) {
        name_meta += c;
      } else {
        char_queue.push(c);
      }
    }
  }

  std::regex regex(R"(.*\[(.*)\](\..*)*$)");
  for (auto &&str : str_list) {
    std::string str_s{absl::StripAsciiWhitespace(str)};
    if (!std::regex_match(str_s, regex)) {
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

    if (iter.second.size() <= 1) {  // Special handling for sizes 0 and 1
      iter.second.emplace_back("");
      host_name_str += iter.second.front();
      res_list->emplace_back(host_name_str);
      continue;
    }

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

}  // namespace util

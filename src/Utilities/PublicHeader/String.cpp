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
                   std::list<std::string> *nodelist,
                   const std::string &end_str) {
  std::regex brackets_regex(R"(.*\[(.*)\])");
  std::smatch match;
  if (std::regex_search(node_str, match, brackets_regex)) {
    std::vector<std::string> node_num;
    std::string match_str = match[1],
                head_str = node_str.substr(0, match.position(1) - 1),
                end_ =
                    node_str.substr(match.position(1) + match_str.size() + 1);

    boost::split(node_num, match_str, boost::is_any_of(","));
    size_t len = node_num.size();
    std::regex num_regex(R"(\d+)"), scope_regex(R"(\d+-\d+)");
    for (size_t i = 0; i < len; i++) {
      if (std::regex_match(node_num[i], num_regex)) {
        if (!ParseNodeList(head_str, nodelist,
                           fmt::format("{}{}{}", node_num[i], end_, end_str)))
          nodelist->emplace_back(
              fmt::format("{}{}{}{}", head_str, node_num[i], end_, end_str));
      } else if (std::regex_match(node_num[i], scope_regex)) {
        std::vector<std::string> loc_index;
        boost::split(loc_index, node_num[i], boost::is_any_of("-"));
        size_t l = loc_index[0].length();
        for (size_t j = std::stoi(loc_index[0]); j <= std::stoi(loc_index[1]);
             j++) {
          std::string s_num = fmt::format("{:0>{}}", std::to_string(j), l);
          if (!ParseNodeList(head_str, nodelist,
                             fmt::format("{}{}{}", s_num, end_, end_str))) {
            nodelist->emplace_back(
                fmt::format("{}{}{}{}", head_str, s_num, end_, end_str));
          }
        }
      } else {
        return false;
      }
    }
    return true;
  }
  return false;
}

bool ParseHostList(const std::string &host_str,
                   std::list<std::string> *host_list) {
  std::string name_str(host_str);
  name_str += ',';  // uniform end format
  std::string name_meta;
  std::list<std::string> str_list;

  std::queue<char> char_queue;
  for (auto c : name_str) {
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
      if (!ParseNodeList(str_s, host_list, "")) return false;
    }
  }
  return true;
}

std::string RegexReplaceWithIsometricSpaceString(const std::regex &pattern,
                                                 const std::string &input) {
  std::string output{input};
  std::smatch matches;

  std::string::const_iterator iterStart = input.begin();
  while (regex_search(iterStart, input.end(), matches, pattern)) {
    output.replace(matches.position(0) + iterStart - input.begin(),
                   matches[0].length(), std::string(matches[0].length(), ' '));
    iterStart = matches[0].second;
  }
  return output;
}

bool HostNameListToStr_(std::list<std::string> &host_list,
                        std::list<std::string> *res_list) {
  std::unordered_map<std::string, std::list<std::string>> host_map;
  bool res = true;

  size_t sz = host_list.size();
  if (sz == 0)
    return true;
  else if (sz == 1) {
    res_list->emplace_back(host_list.front());
    return true;
  }

  std::regex regex(R"(\d+)");
  std::regex pattern(R"(\[.*?\])");

  for (const auto &host : host_list) {
    if (host.empty()) continue;

    std::string host_raw = RegexReplaceWithIsometricSpaceString(pattern, host);

    std::smatch match;
    if (std::regex_search(host_raw, match, regex)) {
      res = false;
      std::string num_str = match[0],
                  head_str = host.substr(0, match.position()),
                  tail_str = host.substr(match.position() + match[0].length(),
                                         host.size());
      std::string key_str = fmt::format("{}<=!>{}", head_str, tail_str);
      auto iter = host_map.find(key_str);
      if (iter == host_map.end()) {
        std::list<std::string> list;
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
    std::regex body_regex(R"(<=!>)");
    std::smatch match;
    std::regex_search(iter.first, match, body_regex);

    host_name_str += match.prefix();
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
    host_name_str += fmt::format("]{}", std::string(match.suffix()));
    res_list->emplace_back(host_name_str);
  }

  return res;
}

}  // namespace util

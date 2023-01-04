#include "crane/String.h"

#include <absl/strings/strip.h>

#include <queue>

#include "crane/Logger.h"
#include "crane/PublicHeader.h"

namespace util {

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
  std::regex regex(R"(.*\[(.*)\])");
  std::smatch match;
  if (std::regex_search(node_str, match, regex)) {
    std::vector<std::string> node_num;
    std::string match_str = match[1],
                head_str = node_str.substr(0, match.position(1) - 1),
                end_ =
                    node_str.substr(match.position(1) + match_str.size() + 1);

    boost::split(node_num, match_str, boost::is_any_of(","));
    size_t len = node_num.size();
    for (size_t i = 0; i < len; i++) {
      if (std::regex_match(node_num[i], std::regex(R"(^\d+$)"))) {
        if (!ParseNodeList(head_str, nodelist,
                           fmt::format("{}{}{}", node_num[i], end_, end_str)))
          nodelist->push_back(
              fmt::format("{}{}{}{}", head_str, node_num[i], end_, end_str));
      } else if (std::regex_match(node_num[i], std::regex(R"(^\d+-\d+$)"))) {
        std::vector<std::string> loc_index;
        boost::split(loc_index, node_num[i], boost::is_any_of("-"));
        size_t l = loc_index[0].length();
        for (size_t j = std::stoi(loc_index[0]); j <= std::stoi(loc_index[1]);
             j++) {
          std::string s_num = fmt::format("{:0>{}}", std::to_string(j), l);
          if (!ParseNodeList(head_str, nodelist,
                             fmt::format("{}{}{}", s_num, end_, end_str))) {
            nodelist->push_back(
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
                   std::list<std::string> *hostlist) {
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

  for (auto &&str : str_list) {
    std::string str_s{absl::StripAsciiWhitespace(str)};
    std::regex regex(R"(.*\[(.*)\](\..*)*$)");
    if (!std::regex_match(str_s, regex)) {
      hostlist->emplace_back(str_s);
    } else {
      if (!ParseNodeList(str_s, hostlist, "")) return false;
    }
  }
  return true;
}

}  // namespace util

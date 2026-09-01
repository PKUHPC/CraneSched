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

#include <absl/strings/str_join.h>
#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include "crane/String.h"

TEST(String, ParseNodeList) {
  using util::ParseHostList;

  std::string host_list{"aaa[1-2,3],bbb"};
  std::list<std::string> parsed_list;

  bool ok = ParseHostList(host_list, &parsed_list);
  GTEST_LOG_(INFO) << "Parsing result: " << ok;
  EXPECT_TRUE(ok);

  if (ok) {
    std::string parsed_list_str = absl::StrJoin(parsed_list, " ");
    GTEST_LOG_(INFO) << "Parsed List: " << parsed_list_str;
    EXPECT_EQ(parsed_list_str, "aaa1 aaa2 aaa3 bbb");
  }
}

TEST(String, ParseHostListKeepsDeterministicOrder) {
  using util::ParseHostList;

  std::list<std::string> parsed_list;
  ASSERT_TRUE(ParseHostList("node[02,01],node03", &parsed_list));
  EXPECT_EQ(absl::StrJoin(parsed_list, " "), "node02 node01 node03");

  parsed_list.clear();
  ASSERT_TRUE(ParseHostList("rack[1-2]n[01-02]", &parsed_list));
  EXPECT_EQ(absl::StrJoin(parsed_list, " "),
            "rack1n01 rack1n02 rack2n01 rack2n02");
}

TEST(String, ParseHostListSupportsSharedNonDotSuffix) {
  using util::ParseHostList;

  std::list<std::string> parsed_list;
  ASSERT_TRUE(ParseHostList(
      "b1u01n1,b2u[05,02]n3,b3u03n4", &parsed_list));
  EXPECT_EQ(absl::StrJoin(parsed_list, " "),
            "b1u01n1 b2u05n3 b2u02n3 b3u03n4");

  parsed_list.clear();
  ASSERT_TRUE(ParseHostList("node[01-02]-gpu", &parsed_list));
  EXPECT_EQ(absl::StrJoin(parsed_list, " "), "node01-gpu node02-gpu");
}

TEST(String, ParseHostListSupportsSlurmHostlistForms) {
  using util::ParseHostList;

  const std::vector<std::pair<std::string, std::string>> test_cases = {
      {"node[01,03-04]", "node01 node03 node04"},
      {"node[01-02].example.com", "node01.example.com node02.example.com"},
      {"node[08,10-11],login", "node08 node10 node11 login"},
      {"gpu[01-02]a[1-2]-x", "gpu01a1-x gpu01a2-x gpu02a1-x gpu02a2-x"},
      {"node[01, 03], node05", "node01 node03 node05"},
      {"node[01]foo[02]", "node01foo02"},
  };

  for (const auto& [host_string, expected] : test_cases) {
    SCOPED_TRACE(host_string);
    std::list<std::string> parsed_list;
    ASSERT_TRUE(ParseHostList(host_string, &parsed_list));
    EXPECT_EQ(absl::StrJoin(parsed_list, " "), expected);
  }
}

TEST(String, ParseHostListRejectsMalformedExpressions) {
  using util::ParseHostList;

  const std::vector<std::string> malformed_host_lists = {
      "node[01",   "node01]",   "node[[01]]",   "node[]",
      "node[01,]", "node[,01]", "node[01,,02]", "node[foo]",
      "node[01-]", "node[-02]", "node[01-02",   "node[01]tail]",
  };

  for (const auto& host_string : malformed_host_lists) {
    SCOPED_TRACE(host_string);
    std::list<std::string> parsed_list;
    EXPECT_FALSE(ParseHostList(host_string, &parsed_list));
  }
}

TEST(String, HostNameListToStr) {
  using util::HostNameListToStr;

  clock_t start, end;
  start = clock();
  std::string host_list{
      "a[01-99]s[01-05]c[001-100],a[30-40,501-600]s[03-07]c[201-300]"};
  std::list<std::string> parsed_list;

  bool ok = util::ParseHostList(host_list, &parsed_list);
  EXPECT_TRUE(ok);
  GTEST_LOG_(INFO) << "n: " << parsed_list.size();

  end = clock();
  double elapsedTime = static_cast<double>(end - start) / CLOCKS_PER_SEC;
  GTEST_LOG_(INFO) << "ParseHostList time: " << elapsedTime << "s";

  if (ok) {
    start = end;
    std::string res = util::HostNameListToStr(parsed_list);

    end = clock();
    elapsedTime = static_cast<double>(end - start) / CLOCKS_PER_SEC;
    GTEST_LOG_(INFO) << "HostNameListToStr time: " << elapsedTime << "s";

    EXPECT_EQ(res, host_list);
    GTEST_LOG_(INFO) << "Parsing result: " << res;
  }
}

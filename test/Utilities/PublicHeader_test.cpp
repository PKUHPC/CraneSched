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

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

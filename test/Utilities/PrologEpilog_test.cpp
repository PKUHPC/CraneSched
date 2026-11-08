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

#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iterator>
#include <utility>
#include <vector>

#include "crane/PrologEpilogExecutor.h"

namespace {

using namespace std::chrono_literals;

class PrologEpilogRunnerTest : public testing::TestWithParam<bool> {
 protected:
  void SetUp() override {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "crane-prolog-XXXXXX")
            .string();
    ASSERT_NE(mkdtemp(pattern.data()), nullptr);
    m_dir_ = pattern;
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(m_dir_, ec);
  }

  std::string Script(std::string_view name, std::string_view body) {
    auto path = m_dir_ / name;
    std::ofstream(path) << "#!/bin/sh\n" << body << '\n';
    std::filesystem::permissions(path, std::filesystem::perms::owner_all);
    return path.string();
  }

  RunPrologEpilogArgs Args(std::vector<std::string> scripts) {
    return {.scripts = std::move(scripts),
            .timeout_sec = 3,
            .run_uid = getuid(),
            .run_gid = getgid(),
            .output_size = 4096};
  }

  util::os::PrologEpilogResult Run(RunPrologEpilogArgs args) {
    if (!GetParam()) {
      auto result = util::os::RunPrologOrEpiLog(args);
      if (result) return {.ok = true, .output = *result};
      return {.exit_code = result.error().exit_code,
              .signal_num = result.error().signal_num};
    }

    std::promise<util::os::PrologEpilogResult> promise;
    auto future = promise.get_future();
    util::os::PrologEpilogExecutor executor(5ms);
    executor.Submit(std::move(args),
                    [&](auto result) { promise.set_value(std::move(result)); });
    return future.get();
  }

  std::filesystem::path m_dir_;
};

TEST_P(PrologEpilogRunnerTest, InheritsCallerCgroup) {
  std::ifstream cgroup_file("/proc/self/cgroup");
  ASSERT_TRUE(cgroup_file.is_open());
  std::string caller_cgroup{std::istreambuf_iterator<char>(cgroup_file),
                            std::istreambuf_iterator<char>()};
  auto args = Args({Script("cgroup.sh", "exec /bin/cat /proc/self/cgroup")});

  auto result = Run(std::move(args));

  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.output, caller_cgroup);
}

INSTANTIATE_TEST_SUITE_P(Runners, PrologEpilogRunnerTest, testing::Bool(),
                         [](const testing::TestParamInfo<bool>& info) {
                           return info.param ? "Async" : "Sync";
                         });

}  // namespace

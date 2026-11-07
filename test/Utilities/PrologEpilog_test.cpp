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
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <mutex>
#include <thread>

#include "crane/PrologEpilogExecutor.h"

namespace {

using namespace std::chrono_literals;

class PrologEpilogTest : public testing::Test {
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

  std::filesystem::path m_dir_;
};

class PrologEpilogRunnerTest : public PrologEpilogTest,
                               public testing::WithParamInterface<bool> {
 protected:
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
};

TEST_P(PrologEpilogRunnerTest, ParentSetupCompletesBeforeEveryScript) {
  auto script = Script("check.sh",
                       "read ready < \"$MARKER\"\n"
                       "[ \"$ready\" = \"$$\" ] || exit 7\n"
                       "printf 'ready\\n'");
  auto args = Args({script, script});
  auto marker = m_dir_ / "ready";
  args.envs.emplace("MARKER", marker.string());
  const pid_t parent = getpid();
  std::vector<pid_t> children;
  args.at_parent_setup_cb = [&](pid_t pid) {
    // This fails promptly with the old child-side callback, without using
    // the child's inherited C++ runtime or requiring a running cluster.
    if (getpid() != parent || pid == parent) return false;
    children.push_back(pid);
    std::this_thread::sleep_for(30ms);
    std::ofstream file(marker);
    file << pid << '\n';
    file.close();
    return file.good();
  };

  auto result = Run(std::move(args));

  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.output, "ready\nready\n");
  ASSERT_EQ(children.size(), 2);
  for (pid_t pid : children) {
    int status;
    EXPECT_EQ(waitpid(pid, &status, WNOHANG), -1);
    EXPECT_EQ(errno, ECHILD);
  }
}

TEST_P(PrologEpilogRunnerTest, SetupFailurePreventsExecAndRemainingScripts) {
  auto marker = m_dir_ / "executed";
  auto script = Script("record.sh", ": > \"$MARKER\"");
  auto args = Args({script, script});
  args.envs.emplace("MARKER", marker.string());
  int calls = 0;
  args.at_parent_setup_cb = [&](pid_t) {
    ++calls;
    return false;
  };

  auto result = Run(std::move(args));

  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.exit_code, EXIT_FAILURE);
  EXPECT_EQ(calls, 1);
  EXPECT_FALSE(std::filesystem::exists(marker));
}

TEST_P(PrologEpilogRunnerTest, SetupConsumesTimeoutBudgetAndPreventsLateExec) {
  auto marker = m_dir_ / "executed";
  auto args = Args({Script("record.sh", ": > \"$MARKER\"")});
  args.envs.emplace("MARKER", marker.string());
  args.timeout_sec = 1;
  args.at_parent_setup_cb = [](pid_t) {
    std::this_thread::sleep_for(1100ms);
    return true;
  };

  auto result = Run(std::move(args));

  EXPECT_FALSE(result.ok);
  EXPECT_FALSE(std::filesystem::exists(marker));
}

TEST_P(PrologEpilogRunnerTest, NoSetupPreservesEnvironmentAndOutput) {
  auto args = Args({Script("output.sh",
                           "printf '%s' \"$VALUE\"\n"
                           "printf ' stderr' >&2")});
  args.envs.emplace("VALUE", "prolog-output");

  auto result = Run(std::move(args));

  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.output, "prolog-output stderr");
}

TEST_P(PrologEpilogRunnerTest, ScriptFailurePreservesExitStatus) {
  auto args = Args({Script("failure.sh", "exit 7")});
  args.at_parent_setup_cb = [](pid_t) { return true; };

  auto result = Run(std::move(args));

  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.exit_code, 7);
  EXPECT_EQ(result.signal_num, 0);
}

TEST_P(PrologEpilogRunnerTest, ScriptTimeoutStillReapsChild) {
  auto args = Args({Script("wait.sh", "exec /bin/sleep 10")});
  args.timeout_sec = 1;
  pid_t child = -1;
  args.at_parent_setup_cb = [&](pid_t pid) {
    child = pid;
    return true;
  };

  auto result = Run(std::move(args));

  EXPECT_FALSE(result.ok);
  EXPECT_NE(result.signal_num, 0);
  ASSERT_GT(child, 0);
  int status;
  EXPECT_EQ(waitpid(child, &status, WNOHANG), -1);
  EXPECT_EQ(errno, ECHILD);
}

TEST_P(PrologEpilogRunnerTest, ChildExitDuringSetupDoesNotRaiseSigpipe) {
  auto marker = m_dir_ / "executed";
  auto args = Args({Script("record.sh", ": > \"$MARKER\"")});
  args.envs.emplace("MARKER", marker.string());
  args.at_parent_setup_cb = [](pid_t pid) {
    kill(pid, SIGKILL);
    // Let the child close its socket before the parent sends the reply.
    std::this_thread::sleep_for(50ms);
    return true;
  };

  auto result = Run(std::move(args));

  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.signal_num, SIGKILL);
  EXPECT_FALSE(std::filesystem::exists(marker));
}

TEST_F(PrologEpilogTest, CranedWatcherDeliversExitAfterParentSetup) {
  auto args = Args({Script("success.sh", "printf 'done'")});
  ChildExitWatcher watcher;
  std::mutex fork_reap_mutex;
  std::atomic<pid_t> child{-1};
  std::atomic_bool stop{false};
  bool delivered = false;
  args.fork_and_watch_fn = [&](std::function<pid_t()> do_fork)
      -> std::optional<std::pair<pid_t, std::future<int>>> {
    std::unique_lock lock(fork_reap_mutex);
    pid_t pid = do_fork();
    if (pid < 0) return std::nullopt;
    if (pid == 0) return std::make_pair(pid, std::future<int>{});
    auto future = watcher.Watch(pid);
    child.store(pid);
    return std::make_pair(pid, std::move(future));
  };
  args.at_parent_setup_cb = [&](pid_t pid) {
    // Setup must execute after fork-and-watch releases the reaper's lock.
    std::unique_lock lock(fork_reap_mutex);
    return pid == child.load();
  };
  std::thread reaper([&] {
    while (!stop.load()) {
      {
        std::unique_lock lock(fork_reap_mutex);
        pid_t pid = child.load();
        int status;
        if (pid > 0 && waitpid(pid, &status, WNOHANG) == pid) {
          delivered = watcher.TryDeliver(pid, status);
          break;
        }
      }
      std::this_thread::sleep_for(1ms);
    }
  });

  auto result = util::os::RunPrologOrEpiLog(args);
  stop.store(true);
  reaper.join();

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "done");
  EXPECT_TRUE(delivered);
}

INSTANTIATE_TEST_SUITE_P(Runners, PrologEpilogRunnerTest, testing::Bool(),
                         [](const testing::TestParamInfo<bool>& info) {
                           return info.param ? "Async" : "Sync";
                         });

}  // namespace

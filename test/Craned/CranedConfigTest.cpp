// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include "cri/api.pb.h"
// CRI signal enums must be included before system signal macros.

#include <absl/cleanup/cleanup.h>
#include <gtest/gtest.h>

#include <fstream>

// Use the real config parser without running the daemon entry point.
#define main CranedMainForConfigTest
#include "../../src/Craned/Core/Craned.cpp"
#undef main

namespace {

class CranedCompletionConfigTest : public ::testing::Test {
 protected:
  void SetUp() override { original_ = g_config.CranedConf; }
  void TearDown() override { g_config.CranedConf = original_; }

 private:
  Craned::Config::CranedConfig original_{};
};

TEST_F(CranedCompletionConfigTest, DefaultsAndAcceptsExplicitTimeout) {
  ParseCranedConfig(YAML::Load("Craned: {}"));
  EXPECT_EQ(g_config.CranedConf.SupervisorCompletionTimeoutSec, 30U);

  ParseCranedConfig(YAML::Load("Craned: {SupervisorCompletionTimeout: 60}"));
  EXPECT_EQ(g_config.CranedConf.SupervisorCompletionTimeoutSec, 60U);
}

TEST_F(CranedCompletionConfigTest, RejectsInvalidTimeoutWithFieldName) {
  for (const char* value : {"0", "-1", "4294967296", "1.5"}) {
    SCOPED_TRACE(value);
    EXPECT_EXIT(
        {
          spdlog::set_default_logger(spdlog::stderr_color_mt("config-test"));
          ParseCranedConfig(
              YAML::Load(std::string("Craned: {SupervisorCompletionTimeout: ") +
                         value + "}"));
        },
        ::testing::ExitedWithCode(1),
        "Craned[.]SupervisorCompletionTimeout must be a positive integer");
  }
}

TEST(CranedStartupTest,
     RelativeLogPathSurvivesDirectorySetupAndWorkingDirectoryChange) {
  std::string pattern = "/tmp/crane-log-startup-XXXXXX";
  ASSERT_NE(mkdtemp(pattern.data()), nullptr);
  const std::filesystem::path root{pattern};
  absl::Cleanup cleanup = [&] { std::filesystem::remove_all(root); };
  std::array<char, HOST_NAME_MAX + 1> hostname{};
  ASSERT_EQ(gethostname(hostname.data(), hostname.size()), 0);
  const auto config = root / "config.yaml";
  const auto plugin = root / "plugin.yaml";
  std::ofstream(plugin) << "Enabled: false\n";
  std::ofstream(config)
      << "CraneBaseDir: " << (root / "state").string() << "\n"
      << "ClusterName: log-test\nControlMachine: localhost\n"
      << "Supervisor:\n  Path: /proc/self/exe\n"
      << "Craned:\n  MaxLogFileSize: 1K\n  MaxLogFileNum: 2\n"
      << "Nodes:\n  - name: " << hostname.data()
      << "\n    NodeAddr: 127.0.0.1\n    cpu: 1\n    memory: 1G\n"
      << "Partitions:\n  - name: test\n    nodes: " << hostname.data() << "\n";
  std::vector<std::string> args{
      "craned",        "--config-file", config.string(), "--plugin-config",
      plugin.string(), "--log-file",    "craned.log"};
  std::vector<char*> argv;
  for (auto& arg : args) argv.push_back(arg.data());
  argv.push_back(nullptr);

  EXPECT_EXIT(
      {
        std::filesystem::current_path(root);
        ParseConfig(static_cast<int>(args.size()), argv.data());
        if (g_config.CranedLogFile != root / "craned.log") _exit(2);
        CreateRequiredDirectories();
        // Daemonization changes cwd before startup and later log rotations.
        std::filesystem::current_path(root / "state");
        CreateRequiredDirectories();
        spdlog::logger logger("rotation-test",
                              spdlog::default_logger()->sinks().front());
        logger.info("{}", std::string(2048, 'x'));
        logger.flush();
        if (!std::filesystem::exists(root / "craned.1.log") ||
            std::filesystem::exists(root / "state" / "craned.log"))
          _exit(3);
        spdlog::shutdown();
        _exit(0);
      },
      ::testing::ExitedWithCode(0), "");
}

}  // namespace

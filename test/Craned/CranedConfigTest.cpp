// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include "cri/api.pb.h"
// CRI signal enums must be included before system signal macros.

#include <gtest/gtest.h>

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

TEST_F(CranedCompletionConfigTest, DefaultsToThirtySeconds) {
  for (const char* config : {"{}", "Craned: {}"}) {
    ParseCranedConfig(YAML::Load(config));
    EXPECT_EQ(g_config.CranedConf.SupervisorCompletionTimeoutSec, 30U);
  }
}

TEST_F(CranedCompletionConfigTest, AcceptsPositiveTimeout) {
  ParseCranedConfig(YAML::Load("Craned: {SupervisorCompletionTimeout: 60}"));
  EXPECT_EQ(g_config.CranedConf.SupervisorCompletionTimeoutSec, 60U);
}

TEST_F(CranedCompletionConfigTest, RejectsZeroTimeout) {
  EXPECT_EXIT(
      ParseCranedConfig(YAML::Load("Craned: {SupervisorCompletionTimeout: 0}")),
      ::testing::ExitedWithCode(1), "");
}

TEST_F(CranedCompletionConfigTest, RejectsNegativeOrMalformedTimeout) {
  for (const char* value : {"-1", "invalid", "4294967296"}) {
    EXPECT_THROW(ParseCranedConfig(YAML::Load(
                     std::string("Craned: {SupervisorCompletionTimeout: ") +
                     value + "}")),
                 YAML::BadConversion);
  }
}

}  // namespace

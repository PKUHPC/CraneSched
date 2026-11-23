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
        ParseCranedConfig(
            YAML::Load(std::string("Craned: {SupervisorCompletionTimeout: ") +
                       value + "}")),
        ::testing::ExitedWithCode(1),
        "Craned[.]SupervisorCompletionTimeout must be a positive integer");
  }
}

}  // namespace

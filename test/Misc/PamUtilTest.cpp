/**
 * Copyright (c) 2026 Peking University and Peking University
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

#include <utility>

#include "PamUtil.h"

TEST(PamUtilTest, FiltersSshX11SessionVariables) {
  EnvMap env{{"DISPLAY", "localhost:16.0"},
             {"XAUTHORITY", "/tmp/stale-xauthority"},
             {"PATH", "/usr/bin"},
             {"XMODIFIERS", "@im=ibus"},
             {"USER_SETTING", "value"}};

  auto filtered = FilterJobEnvForSshSession(std::move(env));

  EXPECT_FALSE(filtered.contains("DISPLAY"));
  EXPECT_FALSE(filtered.contains("XAUTHORITY"));
  EXPECT_EQ(filtered.at("PATH"), "/usr/bin");
  EXPECT_EQ(filtered.at("XMODIFIERS"), "@im=ibus");
  EXPECT_EQ(filtered.at("USER_SETTING"), "value");
}

TEST(PamUtilTest, PreservesUnrelatedAndCaseDistinctVariables) {
  EnvMap env{{"display", "custom"}, {"PATH", "/custom/bin"}};

  auto filtered = FilterJobEnvForSshSession(env);

  EXPECT_EQ(filtered, env);
}

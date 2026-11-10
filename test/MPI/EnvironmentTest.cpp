/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
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

#include <unordered_map>

#include "Environment.h"
#include "Runtime.h"

namespace {

TEST(MpiEnvironmentTest, ReplacesRuntimeIdentityAndPreservesModuleSettings) {
  std::unordered_map<std::string, std::string> environment{
      {"PMI_FD", "old-fd"},
      {"PMI_RANK", "old-rank"},
      {"PMI_ID", "old-id"},
      {"PMI_FANOUT", "64"},
      {"PMIX_NAMESPACE", "old-namespace"},
      {"PMIX_SERVER_URI21", "old-uri"},
      {"PMIX_MCA_gds", "hash"},
      {"I_MPI_PMI2", "1"},
      {"OMPI_MCA_ess", "pmi"}};
  mpi::LaunchInfo launch;
  launch.env = {{"PMI_FD", "new-fd"},
                {"PMI_RANK", "2"},
                {"PMI_SIZE", "4"},
                {"PMI_JOBID", "42.3"}};

  mpi::MergeLaunchEnvironment(environment, launch);

  EXPECT_EQ(environment.at("PMI_FD"), "new-fd");
  EXPECT_EQ(environment.at("PMI_RANK"), "2");
  EXPECT_EQ(environment.at("PMI_SIZE"), "4");
  EXPECT_EQ(environment.at("PMI_JOBID"), "42.3");
  EXPECT_EQ(environment.at("PMI_FANOUT"), "64");
  EXPECT_EQ(environment.at("PMIX_MCA_gds"), "hash");
  EXPECT_EQ(environment.at("I_MPI_PMI2"), "1");
  EXPECT_EQ(environment.at("OMPI_MCA_ess"), "pmi");
  EXPECT_FALSE(environment.contains("PMIX_NAMESPACE"));
  EXPECT_FALSE(environment.contains("PMIX_SERVER_URI21"));
  EXPECT_FALSE(environment.contains("PMI_ID"));
}

TEST(MpiEnvironmentTest, DoesNotInventVendorSelectors) {
  std::unordered_map<std::string, std::string> environment;
  mpi::LaunchInfo launch;
  launch.env = {{"PMI_FD", "7"}};

  mpi::MergeLaunchEnvironment(environment, launch);

  EXPECT_FALSE(environment.contains("I_MPI_PMI2"));
  EXPECT_FALSE(environment.contains("OMPI_MCA_ess"));
}

}  // namespace

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

#include <string>
#include <unordered_set>
#include <vector>

#include "Node/NodeListValidation.h"

TEST(NodeListValidation, ReturnsNoNodesWhenAllAreInPartition) {
  const std::unordered_set<std::string> requested{"node01", "node02"};
  const std::unordered_set<std::string> partition{"node01", "node02", "node03"};

  EXPECT_TRUE(Ctld::FindNodesNotInPartition(requested, partition).empty());
}

TEST(NodeListValidation, ReturnsAllUnknownNodesInStableOrder) {
  const std::unordered_set<std::string> requested{"node999", "node997",
                                                  "node998", "node997"};
  const std::unordered_set<std::string> partition{"node001", "node998"};

  EXPECT_EQ(Ctld::FindNodesNotInPartition(requested, partition),
            (std::vector<std::string>{"node997", "node999"}));
}

TEST(NodeListValidation, ConfiguredDownNodesAreStillValid) {
  const std::unordered_set<std::string> requested{"node-down"};
  const std::unordered_set<std::string> partition{"node-down"};

  EXPECT_TRUE(Ctld::FindNodesNotInPartition(requested, partition).empty());
}

TEST(NodeListValidation, ReturnsAllRequestedNodesForEmptyPartition) {
  const std::unordered_set<std::string> requested{"node02", "node01"};
  const std::unordered_set<std::string> partition;

  EXPECT_EQ(Ctld::FindNodesNotInPartition(requested, partition),
            (std::vector<std::string>{"node01", "node02"}));
}

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

#pragma once

#include <algorithm>
#include <string>
#include <unordered_set>
#include <vector>

namespace Ctld {

inline std::vector<std::string> FindNodesNotInPartition(
    const std::unordered_set<std::string>& requested_nodes,
    const std::unordered_set<std::string>& partition_nodes) {
  std::vector<std::string> invalid_nodes;
  invalid_nodes.reserve(requested_nodes.size());
  for (const auto& node : requested_nodes) {
    if (!partition_nodes.contains(node)) invalid_nodes.emplace_back(node);
  }

  std::sort(invalid_nodes.begin(), invalid_nodes.end());
  return invalid_nodes;
}

}  // namespace Ctld

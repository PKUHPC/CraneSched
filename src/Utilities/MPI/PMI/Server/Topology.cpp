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

#include "Server/Utilities.h"
#include "fmt/format.h"
namespace pmi {
std::string BuildProcessMapping(const crane::grpc::StepToD& step) {
  struct Block {
    uint32_t first_node;
    uint32_t node_count;
    int ranks_per_node;
  };
  std::vector<Block> blocks;
  // Consecutive equally sized nodes share one tuple, keeping large regular
  // allocations within PMI2_MAX_VALLEN. Repeated runs still encode cyclic or
  // nonuniform layouts without assuming a particular scheduler distribution.
  for (int rank = 0; rank < step.task_node_list_size();) {
    auto node = step.task_node_list(rank);
    int begin = rank++;
    while (rank < step.task_node_list_size() &&
           step.task_node_list(rank) == node)
      ++rank;
    int count = rank - begin;
    if (!blocks.empty() &&
        blocks.back().first_node + blocks.back().node_count == node &&
        blocks.back().ranks_per_node == count)
      ++blocks.back().node_count;
    else
      blocks.push_back({node, 1, count});
  }
  std::string result = "(vector";
  for (const auto& block : blocks)
    result += fmt::format(",({},{},{})", block.first_node, block.node_count,
                          block.ranks_per_node);
  result += ')';
  return result;
}
}  // namespace pmi

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

#include <inttypes.h>
#include <list>

#define REVERSE_TREE_WIDTH 7
#define REVERSE_TREE_CHILDREN_TIMEOUT 60 /* seconds */
#define REVERSE_TREE_PARENT_RETRY 5 /* count, 1 sec per attempt */

namespace pmix {

void ReverseTreeInfo(int rank, int num_nodes, int width, int *parent, int *num_children, int *depth, int *total_depth);

int ReverseTreeDirectChildren(int rank, int num_nodes, int width, int depth, std::vector<int>* children);

}


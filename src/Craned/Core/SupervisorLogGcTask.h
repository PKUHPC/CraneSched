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

#include "NodeGarbageCollectionService.h"
// Precompiled header comes first.

namespace Craned {

class SupervisorLogGcTask final : public INodeGcTask {
 public:
  SupervisorLogGcTask() = default;
  ~SupervisorLogGcTask() override = default;

  SupervisorLogGcTask(const SupervisorLogGcTask&) = delete;
  SupervisorLogGcTask(SupervisorLogGcTask&&) = delete;
  SupervisorLogGcTask& operator=(const SupervisorLogGcTask&) = delete;
  SupervisorLogGcTask& operator=(SupervisorLogGcTask&&) = delete;

  std::string_view Name() const override { return "supervisor_log"; }
  NodeGcTaskRunStats Run(const NodeGcContext& ctx) override;
};

}  // namespace Craned

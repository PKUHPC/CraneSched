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
#include "SupervisorPublicDefs.h"
// Precompiled header comes first.

namespace Supervisor {

class TaskManager {
 public:
  explicit TaskManager(task_id_t task_id);
  task_id_t task_id;

 private:
  std::thread m_uvw_thread_;
};
}  // namespace Supervisor
inline std::unique_ptr<Supervisor::TaskManager> g_task_mgr;
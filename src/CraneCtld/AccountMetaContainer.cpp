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

#include "AccountMetaContainer.h"

#include "AccountManager.h"

namespace Ctld {

bool AccountMetaContainer::CheckAndMallocQosResourceFromUser(
    const std::string& username, const TaskInCtld& task, const Qos& qos) {
  if (static_cast<double>(task.cpus_per_task) > qos.max_cpus_per_user ||
      qos.max_jobs_per_user == 0)
    return false;

  bool result = true;

  ResourceView resource_view{};
  resource_view.GetAllocatableRes().cpu_count = task.cpus_per_task;

  user_meta_map_[username].qos_resource_in_use.try_emplace_l(
      task.qos,
      [&](std::pair<const std::string, QosResource>& pair) {
        auto& val = pair.second;
        if (val.resource.CpuCount() + static_cast<double>(task.cpus_per_task) >
                qos.max_cpus_per_user ||
            val.jobs_per_user >= qos.max_jobs_per_user) {
          result = false;
          return;
        }

        val.resource.GetAllocatableRes().cpu_count += task.cpus_per_task;
        val.jobs_per_user++;
      },
      QosResource{resource_view, 1});

  return result;
}

void AccountMetaContainer::FreeQosResource(const std::string& username,
                                           const TaskInCtld& task) {
  user_meta_map_[username].qos_resource_in_use.modify_if(
      task.qos, [&](std::pair<const std::string, QosResource>& pair) {
        auto& val = pair.second;
        val.resource.GetAllocatableRes().cpu_count -= task.cpus_per_task;
        val.jobs_per_user--;
      });
}

}  // namespace Ctld
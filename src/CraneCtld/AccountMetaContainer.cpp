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

  ResourceView resource_view{task.requested_node_res_view * task.node_num};

  user_meta_map_.try_emplace_l(
    username,
  [&](std::pair<const std::string, QosToResourceMap>& pair) {
      util::write_lock_guard qos_res_guard(m_rw_mutex_);
      auto& qos_to_resource_map = pair.second;
      auto iter = qos_to_resource_map.find(task.qos);
      if (iter == qos_to_resource_map.end()) {
        qos_to_resource_map.emplace(task.qos, QosResource{std::move(resource_view), 1});
        return ;
      }

      auto& val = iter->second;
      if (val.resource.CpuCount() + static_cast<double>(task.cpus_per_task) >
      qos.max_cpus_per_user || val.jobs_per_user >= qos.max_jobs_per_user) {
        result = false;
        return;
      }
      val.resource.GetAllocatableRes() +=
        (task.requested_node_res_view * task.node_num).GetAllocatableRes();
      val.jobs_per_user++;
    },
    QosToResourceMap{{task.qos, QosResource{std::move(resource_view), 1}}});

  return result;
}

void AccountMetaContainer::FreeQosResource(const std::string& username,
                                           const TaskInCtld& task) {
  user_meta_map_.modify_if(username, [&](std::pair<const std::string, QosToResourceMap>& pair) {
    util::write_lock_guard qos_res_guard(m_rw_mutex_);
    auto& val = pair.second[task.qos];
    val.resource.GetAllocatableRes() -=
            (task.requested_node_res_view * task.node_num).GetAllocatableRes();
        val.jobs_per_user--;
  });
}

void AccountMetaContainer::DeleteUserResource(const std::string& username) {
  user_meta_map_.erase(username);
}

}  // namespace Ctld
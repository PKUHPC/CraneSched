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

#include <cstdint>

#include "CtldPublicDefs.h"

namespace Ctld {

AccountMetaContainer::UserResourceMetaPtr
AccountMetaContainer::GetUserResourceMetaPtr(const std::string& username) {
  return user_mate_map_.GetValueExclusivePtr(username);
}

AccountMetaContainer::UserResourceMetaMapConstPtr
AccountMetaContainer::GetUserResourceMetaMapConstPtr() {
  return user_mate_map_.GetMapConstSharedPtr();
}

void AccountMetaContainer::MallocQosResourceToUser(
    const std::string& username, const std::string& qos_name,
    const QosResource& qos_resource) {
  // auto user_meta_map_ptr = user_mate_map_.GetMapExclusivePtr();
  // if (!user_meta_map_ptr->contains(username)) {
  //   user_meta_map_ptr->emplace(username, UserResourceMeta{});
  // }
  auto user_meta = user_mate_map_[username];
  user_meta->qos_to_resource_map[qos_name] =
      QosResourceLimit{qos_resource, QosResource{}, QosResource{}};
}

void AccountMetaContainer::FreeQosResourceToUser(const std::string& username,
                                                 const TaskInCtld& task) {
  auto user_meta = user_mate_map_[username];
  auto qos_resource = user_meta->qos_to_resource_map[task.qos];

  uint32_t cpus_per_task = static_cast<uint32_t>(task.cpus_per_task);
  qos_resource.res_avail.cpus_per_user += cpus_per_task;
  qos_resource.res_avail.jobs_per_user += 1;

  qos_resource.res_in_use.cpus_per_user -= cpus_per_task;
  qos_resource.res_in_use.jobs_per_user -= 1;
}

bool AccountMetaContainer::CheckAndApplyQosLimitOnUser(
    const std::string& username, const TaskInCtld& task) {
  auto user_meta = user_mate_map_[username];
  uint32_t cpus_per_task = static_cast<uint32_t>(task.cpus_per_task);
  auto qos_resource = user_meta->qos_to_resource_map[task.qos];
  if (qos_resource.res_avail.jobs_per_user == 0 ||
      qos_resource.res_avail.cpus_per_user < cpus_per_task)
    return false;

  qos_resource.res_avail.cpus_per_user -= cpus_per_task;
  qos_resource.res_avail.jobs_per_user -= 1;

  qos_resource.res_in_use.cpus_per_user += cpus_per_task;
  qos_resource.res_in_use.jobs_per_user += 1;

  return true;
}

}  // namespace Ctld
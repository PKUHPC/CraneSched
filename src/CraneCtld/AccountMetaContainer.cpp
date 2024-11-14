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
#include <string>

#include "AccountManager.h"
#include "CtldPublicDefs.h"

namespace Ctld {

AccountMetaContainer::UserResourceMetaPtr
AccountMetaContainer::GetUserResourceMetaPtr(const std::string& username) {
  return user_meta_map_.GetValueExclusivePtr(username);
}

AccountMetaContainer::UserResourceMetaMapConstPtr
AccountMetaContainer::GetUserResourceMetaMapConstPtr() {
  return user_meta_map_.GetMapConstSharedPtr();
}

void AccountMetaContainer::InitFromDB() {
  HashMap<std::string, UserResourceMeta> user_meta_map;

  AccountManager::UserMapMutexSharedPtr all_user =
      g_account_manager->GetAllUserInfo();
  for (const auto& [username, user] : *all_user) {
    UserResourceMeta::QosToQosResourceMap qos_resource_map;
    for (const auto& [account, attrs_in_account] : user->account_to_attrs_map) {
      for (const auto& [part, qos_list] :
           attrs_in_account.allowed_partition_qos_map) {
        // TODO:初始化
        for (const auto& qos_name : qos_list.second) {
          AccountManager::QosMutexSharedPtr qos =
              g_account_manager->GetExistedQosInfo(qos_name);
          QosResource qos_resource =
              QosResource{qos->max_cpus_per_user, qos->max_jobs_per_user};
          qos_resource_map.emplace(
              qos_name,
              QosResourceLimit{qos_resource, qos_resource, QosResource{}});
        }
      }
    }
    user_meta_map.emplace(username, qos_resource_map);
  }
  user_meta_map_.InitFromMap(std::move(user_meta_map));
}

// TODO: 优化 传入resourcelist，只加一次锁
void AccountMetaContainer::MallocQosResourceToUser(
    const std::string& username, const std::string& qos_name,
    const QosResource& qos_resource) {
  {
    auto user_meta_map_ptr = user_meta_map_.GetMapExclusivePtr();
    if (!user_meta_map_ptr->contains(username)) {
      user_meta_map_ptr->emplace(username, UserResourceMeta{});
    }
  }

  auto user_meta = user_meta_map_[username];
  user_meta->qos_to_resource_map[qos_name] =
      QosResourceLimit{qos_resource, qos_resource, QosResource{}};
}

void AccountMetaContainer::FreeQosResourceOnUser(
    const std::string& username, const std::list<std::string>& qos_list) {
  auto user_meta = user_meta_map_[username];

  for (const auto& qos_name : qos_list)
    user_meta->qos_to_resource_map.erase(qos_name);
}

void AccountMetaContainer::EraseQosResourceOnUser(const std::string& username) {
  user_meta_map_.Erase(username);
}

void AccountMetaContainer::FreeQosLimitOnUser(const std::string& username,
                                              const TaskInCtld& task) {
  auto user_meta = user_meta_map_[username];
  auto qos_resource = user_meta->qos_to_resource_map[task.qos];

  uint32_t cpus_per_task = static_cast<uint32_t>(task.cpus_per_task);
  qos_resource.res_avail.cpus_per_user += cpus_per_task;
  qos_resource.res_avail.jobs_per_user += 1;

  qos_resource.res_in_use.cpus_per_user -= cpus_per_task;
  qos_resource.res_in_use.jobs_per_user -= 1;
}

bool AccountMetaContainer::CheckAndApplyQosLimitOnUser(
    const std::string& username, const TaskInCtld& task) {
  auto user_meta = user_meta_map_[username];
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
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

AccountMetaContainer::AccountMetaContainer() { InitFromDB_(); }

AccountMetaContainer::UserResourceMetaPtr
AccountMetaContainer::GetUserResourceMetaPtr(const std::string& username) {
  return user_meta_map_.GetValueExclusivePtr(username);
}

AccountMetaContainer::UserResourceMetaMapConstPtr
AccountMetaContainer::GetUserResourceMetaMapConstPtr() {
  return user_meta_map_.GetMapConstSharedPtr();
}

AccountMetaContainer::UserResourceMetaMapExclusivePtr
AccountMetaContainer::GetUserResourceMetaMapExclusivePtr() {
  return user_meta_map_.GetMapExclusivePtr();
}

void AccountMetaContainer::AddQosResourceToUser(
    const std::string& username, const QosResourceList& qos_resource_list) {
  auto user_meta_map_ptr = user_meta_map_.GetMapExclusivePtr();
  if (!user_meta_map_ptr->contains(username))
    user_meta_map_ptr->emplace(username, UserResourceMeta{});

  auto& qos_resource_map =
      user_meta_map_ptr->find(username)->second.RawPtr()->qos_to_resource_map;
  for (const auto& pair : qos_resource_list)
    qos_resource_map.emplace(
        pair.first, QosResourceLimit{pair.second, pair.second, QosResource{}});
}

void AccountMetaContainer::EraseQosResourceOnUser(const std::string& username,
                                                  const std::string& qos_name) {
  auto user_meta = user_meta_map_[username];
  user_meta->qos_to_resource_map.erase(qos_name);
}

void AccountMetaContainer::EraseUserResource(const std::string& username) {
  user_meta_map_.Erase(username);
}

// Modify QosResource when max_jobs_per_user or max_cpus_per_user is changed.
void AccountMetaContainer::ModifyQosResourceOnUser(
    const std::string& qos_name, const QosResource& qos_resource) {
  auto user_meta_map_ptr = user_meta_map_.GetMapExclusivePtr();
  for (auto iter = user_meta_map_ptr->begin(); iter != user_meta_map_ptr->end();
       iter++) {
    auto& qos_resource_map = iter->second.RawPtr()->qos_to_resource_map;
    auto qos_map_iter = qos_resource_map.find(qos_name);
    if (qos_map_iter == qos_resource_map.end()) continue;
    // Total is changed to the new QoS resources.
    qos_map_iter->second.res_total = qos_resource;

    auto& avail = qos_map_iter->second.res_avail;
    auto& in_use = qos_map_iter->second.res_in_use;

    if (qos_resource.cpus_per_user >= in_use.cpus_per_user)
      avail.cpus_per_user = qos_resource.cpus_per_user - in_use.cpus_per_user;
    else
      avail.cpus_per_user = 0;

    if (qos_resource.jobs_per_user >= in_use.jobs_per_user)
      avail.jobs_per_user = qos_resource.jobs_per_user - in_use.jobs_per_user;
    else
      avail.jobs_per_user = 0;
  }
}

void AccountMetaContainer::FreeQosResource(const std::string& username,
                                           const TaskInCtld& task) {
  if (!user_meta_map_.Contains(username)) {
    CRANE_ERROR("Try to free resource from an unknown user {}", username);
    return;
  }

  auto user_meta = user_meta_map_[username];
  auto& qos_resource_map = user_meta->qos_to_resource_map;
  auto iter = qos_resource_map.find(task.qos);
  if (iter == qos_resource_map.end()) return;

  uint32_t cpus_per_task = static_cast<uint32_t>(task.cpus_per_task);
  const auto& total = iter->second.res_total;
  auto& avail = iter->second.res_avail;
  auto& in_use = iter->second.res_in_use;

  avail.cpus_per_user += cpus_per_task;
  avail.jobs_per_user += 1;
  // The QoS resources may change during the execution of the job.
  if (avail.cpus_per_user > total.cpus_per_user)
    avail.cpus_per_user = total.cpus_per_user;
  if (avail.jobs_per_user > total.jobs_per_user)
    avail.jobs_per_user = total.jobs_per_user;

  in_use.cpus_per_user -= cpus_per_task;
  in_use.jobs_per_user -= 1;
}

bool AccountMetaContainer::CheckQosLimitOnUser(const std::string& username,
                                               const TaskInCtld& task) {
  if (!user_meta_map_.Contains(username)) {
    CRANE_ERROR("Try to check resource to an unknown user {}", username);
    return false;
  }

  auto user_meta = user_meta_map_[username];
  uint32_t cpus_per_task = static_cast<uint32_t>(task.cpus_per_task);

  const auto& qos_resource_map = user_meta->qos_to_resource_map;
  auto iter = qos_resource_map.find(task.qos);
  if (iter == qos_resource_map.end()) return false;

  if (iter->second.res_avail.jobs_per_user == 0 ||
      iter->second.res_avail.cpus_per_user < cpus_per_task)
    return false;

  return true;
}

void AccountMetaContainer::MallocQosResourceFromUser(
    const std::string& username, const TaskInCtld& task) {
  if (!user_meta_map_.Contains(username)) {
    CRANE_ERROR("Try to apply resource to an unknown user {}", username);
    return;
  }

  auto user_meta = user_meta_map_[username];
  uint32_t cpus_per_task = static_cast<uint32_t>(task.cpus_per_task);

  auto& qos_resource_map = user_meta->qos_to_resource_map;
  auto iter = qos_resource_map.find(task.qos);
  if (iter == qos_resource_map.end()) return;

  iter->second.res_avail.cpus_per_user -= cpus_per_task;
  iter->second.res_avail.jobs_per_user -= 1;

  iter->second.res_in_use.cpus_per_user += cpus_per_task;
  iter->second.res_in_use.jobs_per_user += 1;
}

void AccountMetaContainer::InitFromDB_() {
  HashMap<std::string, UserResourceMeta> user_meta_map;

  AccountManager::UserMapMutexSharedPtr all_user =
      g_account_manager->GetAllUserInfo();
  // all users in user_map
  for (const auto& [username, user] : *all_user) {
    UserResourceMeta::QosToQosResourceMap qos_resource_map;
    // query all qos in user  account->partitioin->qos
    for (const auto& [account, attrs_in_account] : user->account_to_attrs_map) {
      for (const auto& [part, qos_list] :
           attrs_in_account.allowed_partition_qos_map) {
        // user qos list
        for (const auto& qos_name : qos_list.second) {
          // initialize
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

}  // namespace Ctld
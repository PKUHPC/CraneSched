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

void AccountMetaContainer::AddQosResourceToUser(
    const std::string& username, const std::string& qos_name,
    const QosResource& qos_resource) {
  if (!user_meta_map_.contains(username))
    user_meta_map_.emplace(username, QosToQosResourceMap{});

  TryEmplace_(username, qos_name, qos_resource);
}

void AccountMetaContainer::ModifyQosResourceOnUser(
    const std::string& qos_name, const QosResource& qos_resource) {
  for (const auto& [username, _] : user_meta_map_) {
    TryEmplace_(username, qos_name, qos_resource);
  }
}

void AccountMetaContainer::FreeQosResource(const std::string& username,
                                           const TaskInCtld& task) {
  uint32_t cpus_per_task = static_cast<uint32_t>(task.cpus_per_task);
  user_meta_map_[username].modify_if(
      task.qos, [&](std::pair<const std::string, QosResourceLimit>& pair) {
        auto& val = pair.second;
        val.res_avail.cpus_per_user += cpus_per_task;
        val.res_avail.jobs_per_user += 1;
        val.res_in_use.cpus_per_user -= cpus_per_task;
        val.res_in_use.jobs_per_user -= 1;

        if (val.res_avail.cpus_per_user > val.res_total.cpus_per_user)
          val.res_avail.cpus_per_user = val.res_total.cpus_per_user;
        if (val.res_avail.jobs_per_user > val.res_total.jobs_per_user)
          val.res_avail.jobs_per_user = val.res_total.jobs_per_user;
      });
}

bool AccountMetaContainer::CheckQosLimitOnUser(const std::string& username,
                                               const TaskInCtld& task) {
  uint32_t cpus_per_task = static_cast<uint32_t>(task.cpus_per_task);

  bool result = false;
  user_meta_map_[username].if_contains(
      task.qos, [&](const std::pair<std::string, QosResourceLimit>& pair) {
        const auto& val = pair.second;
        if (val.res_avail.cpus_per_user >= cpus_per_task &&
            val.res_avail.jobs_per_user > 0)
          result = true;
      });

  return result;
}

void AccountMetaContainer::MallocQosResourceFromUser(
    const std::string& username, const TaskInCtld& task) {
  uint32_t cpus_per_task = static_cast<uint32_t>(task.cpus_per_task);
  user_meta_map_[username].modify_if(
      task.qos, [&](std::pair<const std::string, QosResourceLimit>& pair) {
        auto& val = pair.second;
        val.res_avail.cpus_per_user -= cpus_per_task;
        val.res_avail.jobs_per_user--;
        val.res_in_use.cpus_per_user += cpus_per_task;
        val.res_in_use.jobs_per_user++;
      });
}

void AccountMetaContainer::InitFromDB_() {
  AccountManager::UserMapMutexSharedPtr all_user =
      g_account_manager->GetAllUserInfo();
  // all users in user_map
  for (const auto& [username, user] : *all_user) {
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
          TryEmplace_(username, qos_name, qos_resource);
        }
      }
    }
  }
}

void AccountMetaContainer::TryEmplace_(const std::string& username,
                                       const std::string& qos_name,
                                       const QosResource& qos_resource) {
  user_meta_map_[username].try_emplace_l(
      qos_name,
      [&](std::pair<const std::string, QosResourceLimit>& pair) {
        QosResourceLimit& val = pair.second;
        if (val.res_total.cpus_per_user == qos_resource.cpus_per_user &&
            val.res_total.jobs_per_user == qos_resource.jobs_per_user)
          return;
        auto& avail = val.res_avail;
        auto& total = val.res_total;
        auto& in_use = val.res_in_use;

        if (qos_resource.cpus_per_user >= in_use.cpus_per_user)
          avail.cpus_per_user =
              qos_resource.cpus_per_user - in_use.cpus_per_user;
        else
          avail.cpus_per_user = 0;

        if (qos_resource.jobs_per_user >= in_use.jobs_per_user)
          avail.jobs_per_user =
              qos_resource.jobs_per_user - in_use.jobs_per_user;
        else
          avail.jobs_per_user = 0;

        total = qos_resource;
      },
      QosResourceLimit{qos_resource, qos_resource, QosResource{}});
}

}  // namespace Ctld
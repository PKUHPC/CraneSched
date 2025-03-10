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

CraneErrCode AccountMetaContainer::CheckAndMallocQosResourceFromUser(
    const std::string& username, const TaskInCtld& task, const Qos& qos) {
  if (static_cast<double>(task.cpus_per_task) > qos.max_cpus_per_user)
    return CraneErrCode::ERR_CPUS_PER_TASK_BEYOND;

  if (qos.max_submit_jobs_per_user == 0 || qos.max_submit_jobs_per_account == 0)
    return CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER;

  CraneErrCode result = CraneErrCode::SUCCESS;

  QosResource qos_resource{ResourceView{}, 0, 1};

  user_meta_map_.try_emplace_l(
      username,
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& qos_to_resource_map = pair.second;
        auto iter = qos_to_resource_map.find(task.qos);
        if (iter == qos_to_resource_map.end()) {
          qos_to_resource_map.emplace(task.qos, std::move(qos_resource));
          return;
        }

        auto& val = iter->second;
        if (val.submit_jobs_count >= qos.max_submit_jobs_per_user) {
          result = CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER;
          return;
        }
        val.submit_jobs_count++;
      },
      QosToResourceMap{{task.qos, std::move(qos_resource)}});

  return result;
}

bool AccountMetaContainer::CheckQosResource(const std::string& username,
                                            const TaskInCtld& task) {
  bool result = true;

  const auto qos_ptr = g_account_manager->GetExistedQosInfo(task.qos);

  user_meta_map_.modify_if(
      username, [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second[task.qos];
        if (val.jobs_count >= qos_ptr->max_jobs_per_user) {
          CRANE_DEBUG(
              "User {} has reached the maximum number of jobs per user, task "
              "{} continues pending.",
              username, task.TaskId());
          result = false;
          return;
          if (val.resource.CpuCount() +
                  static_cast<double>(task.cpus_per_task) >
              qos_ptr->max_cpus_per_user) {
            CRANE_DEBUG(
                "User {} has reached the maximum number of cpus per user, task "
                "{} continues pending.",
                username, task.TaskId());
            result = false;
            return;
          }
        }
      });

  return result;
}

void AccountMetaContainer::MallocQosResource(const std::string& username,
                                             const TaskInCtld& task) {
  const auto qos_ptr = g_account_manager->GetExistedQosInfo(task.qos);

  user_meta_map_.modify_if(
      username, [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second[task.qos];
        val.jobs_count++;
        val.resource.GetAllocatableRes() +=
            (task.requested_node_res_view * task.node_num).GetAllocatableRes();
      });
}

void AccountMetaContainer::FreeQosSubmitResource(const std::string& username,
                                                 const TaskInCtld& task) {
  user_meta_map_.modify_if(
      username, [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second[task.qos];
        val.submit_jobs_count--;
      });
}

void AccountMetaContainer::FreeQosResource(const std::string& username,
                                           const TaskInCtld& task) {
  user_meta_map_.modify_if(
      username, [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second[task.qos];
        val.resource.GetAllocatableRes() -=
            (task.requested_node_res_view * task.node_num).GetAllocatableRes();
        val.jobs_count--;
        val.submit_jobs_count--;
      });
}

void AccountMetaContainer::DeleteUserResource(const std::string& username) {
  user_meta_map_.erase(username);
}

void AccountMetaContainer::DeleteAccountResource(const std::string& account) {
  account_meta_map_.erase(account);
}

CraneErrCode AccountMetaContainer::CheckAndMallocQosResourceFromAccount_(
    const TaskInCtld& task, const Qos& qos) {
  CraneErrCode result = CraneErrCode::SUCCESS;

  return result;
}

}  // namespace Ctld
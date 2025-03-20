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

CraneErrCode AccountMetaContainer::TryMallocQosResource(TaskInCtld& task) {
  auto qos = g_account_manager->GetExistedQosInfo(task.qos);
  if (!qos) {
    CRANE_ERROR("Unknown QOS '{}'", task.qos);
    return CraneErrCode::ERR_INVALID_QOS;
  }

  if (static_cast<double>(task.cpus_per_task) * task.node_num >
      qos->max_cpus_per_user)
    return CraneErrCode::ERR_CPUS_PER_TASK_BEYOND;

  if (qos->max_jobs_per_user == 0)
    return CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER;

  task.qos_priority = qos->priority;

  if (task.time_limit >= absl::Seconds(kTaskMaxTimeLimitSec)) {
    task.time_limit = qos->max_time_limit_per_task;
  } else if (task.time_limit > qos->max_time_limit_per_task) {
    CRANE_WARN("time-limit beyond the user's limit");
    return CraneErrCode::ERR_TIME_TIMIT_BEYOND;
  }

  CraneErrCode result = CraneErrCode::SUCCESS;

  ResourceView resource_view{task.allocated_node_res_view * task.node_num};

  user_meta_map_.try_emplace_l(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& qos_to_resource_map = pair.second;
        auto iter = qos_to_resource_map.find(task.qos);
        if (iter == qos_to_resource_map.end()) {
          qos_to_resource_map.emplace(task.qos,
                                      QosResource{std::move(resource_view), 1});
          return;
        }

        auto& val = iter->second;
        if (val.resource.CpuCount() + resource_view.CpuCount() >
            qos->max_cpus_per_user) {
          result = CraneErrCode::ERR_CPUS_PER_TASK_BEYOND;
          return;
        }
        if (val.jobs_per_user + 1 > qos->max_jobs_per_user) {
          result = CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER;
          return;
        }
        val.resource.GetAllocatableRes() += resource_view.GetAllocatableRes();
        val.jobs_per_user++;
      },
      QosToResourceMap{{task.qos, QosResource{std::move(resource_view), 1}}});

  return result;
}

void AccountMetaContainer::FreeQosResource(const TaskInCtld& task) {
  ResourceView resource_view{task.requested_node_res_view * task.node_num};

  user_meta_map_.modify_if(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second[task.qos];
        CRANE_ASSERT(val.jobs_per_user > 0);
        CRANE_ASSERT(resource_view <= val.resource);
        val.resource.GetAllocatableRes() -= (resource_view).GetAllocatableRes();
        val.jobs_per_user--;
      });
}

void AccountMetaContainer::DeleteUserResource(const std::string& username) {
  user_meta_map_.erase(username);
}

}  // namespace Ctld
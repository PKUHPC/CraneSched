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

  if (qos.max_submit_jobs_per_user == 0)
    return CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER;

  CraneErrCode result = CraneErrCode::SUCCESS;

  ResourceView total_resource{};
  total_resource.GetAllocatableRes().cpu_count = (cpu_t)qos.max_cpus_per_user;
  QosResource qos_resource{
      ResourceInQos{total_resource, qos.max_jobs_per_user},  // res_total
      ResourceInQos{ResourceView{}, 0},                      // res_in_use
      1};

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

  user_meta_map_.modify_if(
      username, [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second[task.qos];
        if (val.res_in_use.jobs_count >= val.res_total.jobs_count) {
          CRANE_TRACE(
              "User {} has reached the maximum number of jobs per user, task "
              "{} continues pending.",
              username, task.TaskId());
          result = false;
          return;
          if (val.res_in_use.resource.CpuCount() +
                  static_cast<double>(task.cpus_per_task) >
              val.res_total.resource.CpuCount()) {
            CRANE_TRACE(
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
  user_meta_map_.modify_if(
      username, [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second[task.qos];
        val.res_in_use.jobs_count++;
        val.res_in_use.resource.GetAllocatableRes() +=
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
        val.res_in_use.resource.GetAllocatableRes() -=
            (task.requested_node_res_view * task.node_num).GetAllocatableRes();
        val.res_in_use.jobs_count--;
        val.submit_jobs_count--;
      });
}

void AccountMetaContainer::DeleteUserResource(const std::string& username) {
  user_meta_map_.erase(username);
}

}  // namespace Ctld
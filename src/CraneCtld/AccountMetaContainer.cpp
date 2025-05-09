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

CraneErrCode AccountMetaContainer::TryMallocQosSubmitResource(
    TaskInCtld& task) {
  auto qos = g_account_manager->GetExistedQosInfo(task.qos);
  if (!qos) {
    CRANE_ERROR("Unknown QOS '{}'", task.qos);
    return CraneErrCode::ERR_INVALID_QOS;
  }

  if (static_cast<double>(task.cpus_per_task) * task.node_num >
      qos->max_cpus_per_user)
    return CraneErrCode::ERR_CPUS_PER_TASK_BEYOND;

  if (qos->max_submit_jobs_per_user == 0)
    return CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER;

  if (qos->max_submit_jobs_per_account == 0)
    return CraneErrCode::ERR_MAX_JOB_COUNT_PER_ACCOUNT;

  task.qos_priority = qos->priority;

  if (task.time_limit >= absl::Seconds(kTaskMaxTimeLimitSec)) {
    task.time_limit = qos->max_time_limit_per_task;
  } else if (task.time_limit > qos->max_time_limit_per_task) {
    CRANE_WARN("time-limit beyond the user's limit");
    return CraneErrCode::ERR_TIME_TIMIT_BEYOND;
  }

  const auto account_map_ptr = g_account_manager->GetAllAccountInfo();

  CraneErrCode result = CraneErrCode::SUCCESS;

  result = CheckQosSubmitResourceForUser_(task, *qos);
  if (!result) return result;

  result = CheckQosSubmitResourceForAccount_(task, *qos, account_map_ptr);
  if (!result) return result;

  ResourceView resource_view{task.requested_node_res_view * task.node_num};

  m_user_meta_map_.try_emplace_l(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& qos_to_resource_map = pair.second;
        auto iter = qos_to_resource_map.find(task.qos);
        if (iter == qos_to_resource_map.end()) {
          qos_to_resource_map.emplace(task.qos,
                                      QosResource{std::move(resource_view), 0, 1});
          return;
        }

        auto& val = iter->second;
        val.resource.GetAllocatableRes() += resource_view.GetAllocatableRes();
        val.submit_jobs_count++;
      },
      QosToResourceMap{{task.qos, QosResource{std::move(resource_view), 0, 1}}});


  std::string account_name = task.account;

  do {
    m_account_meta_map_.try_emplace_l(account_name, [&](std::pair<const std::string, QosToResourceMap>& pair) {
      auto& qos_to_resource_map = pair.second;
      auto iter = qos_to_resource_map.find(task.qos);
      if (iter == qos_to_resource_map.end()) {
        qos_to_resource_map.emplace(task.qos,
                                    QosResource{std::move(resource_view), 0, 1});
        return ;
      }

      auto& val = iter->second;
      val.submit_jobs_count++;
    },
    QosToResourceMap{{task.qos, QosResource{std::move(resource_view), 0, 1}}});

    account_name = account_map_ptr->at(account_name)->parent_account;
  } while (!account_name.empty());

  CRANE_DEBUG("Malloc QOS resource {} for user {}. Ok: {}",
              util::ReadableResourceView(resource_view), task.Username(),
              result == CraneErrCode::SUCCESS);

  return result;
}

bool AccountMetaContainer::CheckQosResource(const TaskInCtld& task) {
  auto qos = g_account_manager->GetExistedQosInfo(task.qos);
  if (!qos) {
    CRANE_ERROR("Unknown QOS '{}'", task.qos);
    return CraneErrCode::ERR_INVALID_QOS;
  }

  // TODO: Delete a user while jobs are in the queue?
  CRANE_ASSERT(m_user_meta_map_.contains(task.Username()));

  bool result = true;

  m_user_meta_map_.try_emplace_l(task.Username(), [&](std::pair<const std::string, QosToResourceMap>& pair) {
      auto& qos_to_resource_map = pair.second;
      auto iter = qos_to_resource_map.find(task.qos);

      CRANE_ASSERT(iter != qos_to_resource_map.end());

      auto& val = iter->second;
      if (val.jobs_count + 1 > qos->max_jobs_per_user) {
        result = false;
      }
  });

  return result;
}

void AccountMetaContainer::MallocQosResource(const TaskInCtld& task) {

  CRANE_ASSERT(m_user_meta_map_.contains(task.Username()));

  m_user_meta_map_.try_emplace_l(task.Username(), [&](std::pair<const std::string, QosToResourceMap>& pair) {
    auto& qos_to_resource_map = pair.second;
    auto iter = qos_to_resource_map.find(task.qos);

    CRANE_ASSERT(iter != qos_to_resource_map.end());

    auto& val = iter->second;
    val.jobs_count++;
  });
}

void AccountMetaContainer::FreeQosSubmitResource(const TaskInCtld& task) {
  ResourceView resource_view{task.requested_node_res_view * task.node_num};

  m_user_meta_map_.modify_if(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second[task.qos];
        CRANE_ASSERT(val.submit_jobs_count > 0);
        CRANE_ASSERT(resource_view <= val.resource);
        val.resource.GetAllocatableRes() -= (resource_view).GetAllocatableRes();
        val.submit_jobs_count--;
      });
}

void AccountMetaContainer::FreeQosResource(const TaskInCtld& task) {
  ResourceView resource_view{task.requested_node_res_view * task.node_num};
  CRANE_DEBUG("Free QOS resource {} for task {} of user {}",
              util::ReadableResourceView(resource_view), task.TaskId(),
              task.Username());

  m_user_meta_map_.modify_if(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second[task.qos];
        CRANE_ASSERT(val.jobs_count > 0);
        CRANE_ASSERT(resource_view.GetAllocatableRes() <=
                     val.resource.GetAllocatableRes());
        val.resource.GetAllocatableRes() -= (resource_view).GetAllocatableRes();
        val.jobs_count--;
        val.resource.GetAllocatableRes() -= (resource_view).GetAllocatableRes();
        val.submit_jobs_count--;
      });
}

void AccountMetaContainer::DeleteUserResource(const std::string& username) {
  m_user_meta_map_.erase(username);
}

CraneErrCode AccountMetaContainer::CheckQosSubmitResourceForUser_(
    const TaskInCtld& task, const Qos& qos) {
  auto result = CraneErrCode::SUCCESS;

  ResourceView resource_view{task.requested_node_res_view * task.node_num};

  m_user_meta_map_.modify_if(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& qos_to_resource_map = pair.second;
        auto iter = qos_to_resource_map.find(task.qos);
        if (iter == qos_to_resource_map.end())
          return;

        auto& val = iter->second;
        if (val.resource.CpuCount() + resource_view.CpuCount() >
            qos.max_cpus_per_user) {
          result = CraneErrCode::ERR_CPUS_PER_TASK_BEYOND;
          return;
        }

        if (val.submit_jobs_count + 1 > qos.max_submit_jobs_per_user)
          result = CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER;
      });

  return result;
}

CraneErrCode AccountMetaContainer::CheckQosSubmitResourceForAccount_(
    const TaskInCtld& task, const Qos& qos, const AccountManager::AccountMapMutexSharedPtr& account_map_ptr) {

  auto result = CraneErrCode::SUCCESS;
  std::string account_name = task.account;

  do {
    m_account_meta_map_.modify_if(account_name, [&](std::pair<const std::string, QosToResourceMap>& pair) {
      auto& qos_to_resource_map = pair.second;
      auto iter = qos_to_resource_map.find(task.qos);
      if (iter == qos_to_resource_map.end())
        return;

      auto& val = iter->second;
      if (val.submit_jobs_count + 1 > qos.max_submit_jobs_per_account)
        result = CraneErrCode::ERR_MAX_JOB_COUNT_PER_ACCOUNT;
    });
    account_name = account_map_ptr->at(account_name)->parent_account;
  } while (!account_name.empty());

  return result;
}

}  // namespace Ctld
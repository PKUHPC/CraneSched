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
  CraneErrCode result = CraneErrCode::SUCCESS;

  {
    const auto& user_ptr =
        g_account_manager->GetExistedUserInfo(task.Username());
    UserAddTask(task.Username());
    if (!user_ptr) return CraneErrCode::ERR_INVALID_USER;
  }

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

  std::set<int> account_stripes;
  for (const auto& account_name : task.account_chain) {
    account_stripes.insert(StripeForKey_(account_name));
  }

  // Lock the specified user/account to minimize the impact on other users and
  // accounts.
  std::lock_guard user_lock(m_user_stripes_[StripeForKey_(task.Username())]);
  std::vector<std::unique_lock<std::mutex>> account_locks;
  account_locks.reserve(account_stripes.size());
  for (const auto account_stripe : account_stripes) {
    account_locks.emplace_back(m_account_stripes_[account_stripe]);
  }

  result = CheckQosSubmitResourceForUser_(task, *qos);
  if (result != CraneErrCode::SUCCESS) return result;

  result = CheckQosSubmitResourceForAccount_(task, *qos);
  if (result != CraneErrCode::SUCCESS) return result;

  MallocQosSubmitResource(task);

  CRANE_DEBUG("Malloc QOS resource {} for user {}. Ok: {}",
              util::ReadableResourceView(resource_view), task.Username(),
              result == CraneErrCode::SUCCESS);

  return result;
}

void AccountMetaContainer::MallocQosSubmitResource(const TaskInCtld& task) {
  CRANE_DEBUG(
      "Malloc QOS {} submit resource for task of user {} and account {}.",
      task.qos, task.Username(), task.account);

  m_user_meta_map_.try_emplace_l(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& qos_to_resource_map = pair.second;
        auto iter = qos_to_resource_map.find(task.qos);
        if (iter == qos_to_resource_map.end()) {
          qos_to_resource_map.emplace(task.qos,
                                      QosResource{.resource = ResourceView{},
                                                  .jobs_count = 0,
                                                  .submit_jobs_count = 1});
          return;
        }

        auto& val = iter->second;
        val.submit_jobs_count++;
      },
      QosToResourceMap{{task.qos, QosResource{.resource = ResourceView{},
                                              .jobs_count = 0,
                                              .submit_jobs_count = 1}}});

  for (const auto& account_name : task.account_chain) {
    m_account_meta_map_.try_emplace_l(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto& qos_to_resource_map = pair.second;
          auto iter = qos_to_resource_map.find(task.qos);
          if (iter == qos_to_resource_map.end()) {
            qos_to_resource_map.emplace(task.qos,
                                        QosResource{.resource = ResourceView{},
                                                    .jobs_count = 0,
                                                    .submit_jobs_count = 1});
            return;
          }

          auto& val = iter->second;
          val.submit_jobs_count++;
        },
        QosToResourceMap{{task.qos, QosResource{.resource = ResourceView{},
                                                .jobs_count = 0,
                                                .submit_jobs_count = 1}}});
  }
}

void AccountMetaContainer::MallocQosResourceToRecoveredRunningTask(
    TaskInCtld& task) {
  auto qos = g_account_manager->GetExistedQosInfo(task.qos);
  // Under normal circumstances, QoS must exist.
  CRANE_ASSERT(qos);

  CRANE_DEBUG(
      "Malloc QOS {} resource for recover task {} of user {} and account {}.",
      task.qos, task.TaskId(), task.Username(), task.account);

  m_user_meta_map_.try_emplace_l(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& qos_to_resource_map = pair.second;
        auto iter = qos_to_resource_map.find(task.qos);
        if (iter == qos_to_resource_map.end()) {
          qos_to_resource_map.emplace(
              task.qos, QosResource{.resource = task.allocated_res_view,
                                    .jobs_count = 1,
                                    .submit_jobs_count = 1});
          return;
        }

        auto& val = iter->second;
        val.resource.GetAllocatableRes() +=
            task.allocated_res_view.GetAllocatableRes();
        val.submit_jobs_count++;
        val.jobs_count++;
      },
      QosToResourceMap{
          {task.qos, QosResource{.resource = task.allocated_res_view,
                                 .jobs_count = 1,
                                 .submit_jobs_count = 1}}});

  for (const auto& account_name : task.account_chain) {
    m_account_meta_map_.try_emplace_l(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto& qos_to_resource_map = pair.second;
          auto iter = qos_to_resource_map.find(task.qos);
          if (iter == qos_to_resource_map.end()) {
            qos_to_resource_map.emplace(
                task.qos, QosResource{.resource = task.allocated_res_view,
                                      .jobs_count = 1,
                                      .submit_jobs_count = 1});
            return;
          }

          auto& val = iter->second;
          val.submit_jobs_count++;
          val.jobs_count++;
        },
        QosToResourceMap{
            {task.qos, QosResource{.resource = task.allocated_res_view,
                                   .jobs_count = 1,
                                   .submit_jobs_count = 1}}});
  }
}

std::optional<std::string> AccountMetaContainer::CheckQosResource(
    const TaskInCtld& task) {
  auto qos = g_account_manager->GetExistedQosInfo(task.qos);
  if (!qos) return "InvalidQOS";

  std::lock_guard user_lock(m_user_stripes_[StripeForKey_(task.Username())]);

  std::set<int> account_stripes;
  for (const auto& account : task.account_chain) {
    account_stripes.insert(StripeForKey_(account));
  }

  std::vector<std::unique_lock<std::mutex>> account_locks;
  account_locks.reserve(account_stripes.size());
  for (const auto account_stripe : account_stripes) {
    account_locks.emplace_back(m_account_stripes_[account_stripe]);
  }

  CRANE_ASSERT(m_user_meta_map_.contains(task.Username()));

  bool result = true;

  m_user_meta_map_.if_contains(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second.at(task.qos);
        if (val.jobs_count + 1 > qos->max_jobs_per_user) result = false;

        ResourceView resource_use{task.requested_node_res_view * task.node_num};
        if (val.resource.CpuCount() + resource_use.CpuCount() >
            qos->max_cpus_per_user)
          result = false;
      });

  if (!result) return "QOSResourceLimit";

  for (const auto& account_name : task.account_chain) {
    CRANE_ASSERT(m_account_meta_map_.contains(account_name));
    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto& val = pair.second.at(task.qos);
          if (val.jobs_count + 1 > qos->max_jobs_per_account) result = false;
        });
    if (!result) break;
  }

  if (!result) return "QOSResourceLimit";

  return std::nullopt;
}

void AccountMetaContainer::MallocQosResource(const TaskInCtld& task) {
  CRANE_DEBUG("Malloc QOS {} resource for task {} of user {} and account {}.",
              task.qos, task.TaskId(), task.Username(), task.account);

  CRANE_ASSERT(m_user_meta_map_.contains(task.Username()));

  m_user_meta_map_.if_contains(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second.at(task.qos);
        val.jobs_count++;
        val.resource.GetAllocatableRes() +=
            task.allocated_res_view.GetAllocatableRes();
      });

  for (const auto& account_name : task.account_chain) {
    CRANE_ASSERT(m_account_meta_map_.contains(account_name));
    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto& val = pair.second.at(task.qos);
          val.jobs_count++;
        });
  }
}

void AccountMetaContainer::FreeQosSubmitResource(const TaskInCtld& task) {
  CRANE_DEBUG(
      "Free QOS {} submit resource for task {} of user {} and account {}.",
      task.qos, task.TaskId(), task.Username(), task.account);

  m_user_meta_map_.if_contains(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second.at(task.qos);
        CRANE_ASSERT(val.submit_jobs_count > 0);
        val.submit_jobs_count--;
      });

  for (const auto& account_name : task.account_chain) {
    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto& val = pair.second.at(task.qos);
          CRANE_ASSERT(val.submit_jobs_count > 0);
          val.submit_jobs_count--;
        });
  }

  UserReduceTask(task.Username());
}

void AccountMetaContainer::FreeQosResource(const TaskInCtld& task) {
  CRANE_DEBUG(
      "Free QOS {} submit resource for task {} of user {} and account {}.",
      task.qos, task.TaskId(), task.Username(), task.account);

  m_user_meta_map_.if_contains(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second.at(task.qos);
        CRANE_ASSERT(val.jobs_count > 0);
        CRANE_ASSERT(task.allocated_res_view.GetAllocatableRes() <=
                     val.resource.GetAllocatableRes());
        val.jobs_count--;
        val.resource.GetAllocatableRes() -=
            task.allocated_res_view.GetAllocatableRes();
        val.submit_jobs_count--;
      });

  for (const auto& account_name : task.account_chain) {
    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto& val = pair.second.at(task.qos);
          CRANE_ASSERT(val.submit_jobs_count > 0);
          CRANE_ASSERT(val.jobs_count > 0);
          val.jobs_count--;
          val.submit_jobs_count--;
        });
  }
  UserReduceTask(task.Username());
}

void AccountMetaContainer::DeleteUserMeta(const std::string& username) {
  m_user_meta_map_.erase(username);
}

void AccountMetaContainer::DeleteAccountMeta(const std::string& account) {
  m_account_meta_map_.erase(account);
}

void AccountMetaContainer::UserAddTask(const std::string& username) {
  m_user_to_task_map_.try_emplace_l(
      username,
      [&](std::pair<const std::string, uint32_t>& pair) { ++pair.second; }, 1);
}

void AccountMetaContainer::UserReduceTask(const std::string& username) {
  CRANE_ASSERT(m_user_to_task_map_.contains(username));

  m_user_to_task_map_.if_contains(
      username,
      [&](std::pair<const std::string, uint32_t>& pair) { --pair.second; });
}

bool AccountMetaContainer::UserHasTask(const std::string& username) {
  bool result = false;

  m_user_to_task_map_.if_contains(
      username, [&](std::pair<const std::string, uint32_t>& pair) {
        if (pair.second > 0) result = true;
      });

  return result;
}

CraneErrCode AccountMetaContainer::CheckQosSubmitResourceForUser_(
    const TaskInCtld& task, const Qos& qos) {
  auto result = CraneErrCode::SUCCESS;

  ResourceView resource_view{task.requested_node_res_view * task.node_num};

  m_user_meta_map_.if_contains(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& qos_to_resource_map = pair.second;
        auto iter = qos_to_resource_map.find(task.qos);
        if (iter == qos_to_resource_map.end()) return;

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
    const TaskInCtld& task, const Qos& qos) {
  auto result = CraneErrCode::SUCCESS;

  for (const auto& account_name : task.account_chain) {
    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto& qos_to_resource_map = pair.second;
          auto iter = qos_to_resource_map.find(task.qos);
          if (iter == qos_to_resource_map.end()) return;

          auto& val = iter->second;
          if (val.submit_jobs_count + 1 > qos.max_submit_jobs_per_account)
            result = CraneErrCode::ERR_MAX_JOB_COUNT_PER_ACCOUNT;
        });
  }

  return result;
}

}  // namespace Ctld
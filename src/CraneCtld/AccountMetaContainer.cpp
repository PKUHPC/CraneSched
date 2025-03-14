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

namespace Ctld {

CraneExpected<void> AccountMetaContainer::CheckQosSubmitResource(
    const TaskInCtld& task, const Qos& qos,
    const std::unordered_map<std::string, std::unique_ptr<Account>>&
        account_map) {
  if (static_cast<double>(task.cpus_per_task) > qos.max_cpus_per_user)
    return std::unexpected(CraneErrCode::ERR_CPUS_PER_TASK_BEYOND);

  if (qos.max_submit_jobs_per_user == 0)
    return std::unexpected(CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER);

  if (qos.max_submit_jobs_per_account == 0)
    return std::unexpected(CraneErrCode::ERR_MAX_JOB_COUNT_PER_ACCOUNT);

  CraneExpected<void> result;

  result = CheckQosSubmitResourceForUser_(task, qos);
  if (!result) return result;

  result = CheckQosSubmitResourceForAccount_(task, qos, account_map);
  if (!result) return result;

  return result;
}

void AccountMetaContainer::MallocQosSubmitResource(
    const TaskInCtld& task,
    const std::unordered_map<std::string, std::unique_ptr<Account>>&
        account_map) {
  MallocQosSubmitResourceForUser_(task);
  MallocQosSubmitResourceForAccount_(task, account_map);
}

void AccountMetaContainer::FreeQosSubmitResource(const TaskInCtld& task) {
  FreeQosSubmitResourceForAccount_(task);
  FreeQosSubmitResourceForUser_(task);
}

bool AccountMetaContainer::CheckQosResource(const TaskInCtld& task) {
  bool result = true;
  Qos qos;

  {
    const auto qos_ptr = g_account_manager->GetExistedQosInfo(task.qos);
    qos = *qos_ptr;
  }

  return CheckQosResourceForUser_(task, qos) &&
         CheckQosResourceForAccount_(task, qos);
}

void AccountMetaContainer::MallocQosResource(const TaskInCtld& task) {
  MallocQosResourceForUser_(task);
  MallocQosResourceForAccount_(task);
}

void AccountMetaContainer::FreeQosResource(const TaskInCtld& task) {
  FreeQosResourceForAccount_(task);
  FreeQosResourceForUser_(task);
}

void AccountMetaContainer::DeleteUserResource(const std::string& username) {
  user_meta_map_.erase(username);
}

void AccountMetaContainer::DeleteAccountResource(const std::string& account) {
  account_meta_map_.erase(account);
}

CraneExpected<void> AccountMetaContainer::CheckQosSubmitResourceForUser_(
    const TaskInCtld& task, const Qos& qos) {
  CraneExpected<void> result{};

  QosResource qos_resource{ResourceView{}, 0, 0};

  user_meta_map_.try_emplace_l(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& qos_to_resource_map = pair.second;
        auto iter = qos_to_resource_map.find(task.qos);
        if (iter == qos_to_resource_map.end()) {
          qos_to_resource_map.emplace(task.qos, std::move(qos_resource));
          return;
        }

        auto& val = iter->second;
        if (val.submit_jobs_count >= qos.max_submit_jobs_per_user) {
          result = std::unexpected(CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER);
          return;
        }
      },
      QosToResourceMap{{task.qos, std::move(qos_resource)}});

  return result;
}

CraneExpected<void> AccountMetaContainer::CheckQosSubmitResourceForAccount_(
    const TaskInCtld& task, const Qos& qos,
    const std::unordered_map<std::string, std::unique_ptr<Account>>&
        account_map) {
  CraneExpected<void> result{};
  std::string account_name = task.account;

  QosResource qos_resource{ResourceView{}, 0, 0};

  do {
    account_meta_map_.try_emplace_l(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto& qos_to_resource_map = pair.second;
          auto iter = qos_to_resource_map.find(task.qos);
          if (iter == qos_to_resource_map.end()) {
            qos_to_resource_map.emplace(task.qos, std::move(qos_resource));
            return;
          }

          auto& val = iter->second;
          if (val.submit_jobs_count >= qos.max_submit_jobs_per_account) {
            result =
                std::unexpected(CraneErrCode::ERR_MAX_JOB_COUNT_PER_ACCOUNT);
            return;
          }
        },
        QosToResourceMap{{task.qos, std::move(qos_resource)}});

    if (!result) break;
    account_name = account_map.at(account_name)->parent_account;
  } while (!account_name.empty());

  return result;
}

void AccountMetaContainer::MallocQosSubmitResourceForUser_(
    const TaskInCtld& task) {
  user_meta_map_.modify_if(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second[task.qos];
        val.submit_jobs_count++;
      });
}

void AccountMetaContainer::MallocQosSubmitResourceForAccount_(
    const TaskInCtld& task,
    const std::unordered_map<std::string, std::unique_ptr<Account>>&
        account_map) {
  std::string account_name = task.account;

  do {
    account_meta_map_.modify_if(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto& val = pair.second[task.qos];
          val.submit_jobs_count++;
        });
    account_name = account_map.at(account_name)->parent_account;
  } while (!account_name.empty());
}

void AccountMetaContainer::FreeQosSubmitResourceForUser_(
    const TaskInCtld& task) {
  user_meta_map_.modify_if(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second[task.qos];
        val.submit_jobs_count--;
      });
}

void AccountMetaContainer::FreeQosSubmitResourceForAccount_(
    const TaskInCtld& task) {
  std::string account_name = task.account;
  const auto account_map_ptr = g_account_manager->GetAllAccountInfo();

  do {
    account_meta_map_.modify_if(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto& val = pair.second[task.qos];
          val.submit_jobs_count--;
        });
    account_name = account_map_ptr->at(account_name)->parent_account;
  } while (!account_name.empty());
}

bool AccountMetaContainer::CheckQosResourceForUser_(const TaskInCtld& task,
                                                    const Qos& qos) {
  bool result = true;

  user_meta_map_.modify_if(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second[task.qos];
        if (val.jobs_count >= qos.max_jobs_per_user) {
          CRANE_DEBUG(
              "User {} has reached the maximum number of jobs per user, task "
              "{} continues pending.",
              task.Username(), task.TaskId());
          result = false;
          return;
          if (val.resource.CpuCount() +
                  static_cast<double>(task.cpus_per_task) >
              qos.max_cpus_per_user) {
            CRANE_DEBUG(
                "User {} has reached the maximum number of cpus per user, task "
                "{} continues pending.",
                task.Username(), task.TaskId());
            result = false;
            return;
          }
        }
      });

  return result;
}

bool AccountMetaContainer::CheckQosResourceForAccount_(const TaskInCtld& task,
                                                       const Qos& qos) {
  bool result = true;
  std::string account_name = task.account;

  const auto account_map_ptr = g_account_manager->GetAllAccountInfo();

  do {
    account_meta_map_.modify_if(account_name, [&](std::pair<const std::string,
                                                            QosToResourceMap>&
                                                      pair) {
      auto& val = pair.second[task.qos];
      if (val.jobs_count >= qos.max_jobs_per_account) {
        CRANE_DEBUG(
            "Account {} has reached the maximum number of jobs per account, "
            "task {} continues pending.",
            account_name, task.TaskId());
        result = false;
      }
    });

    if (!result) break;

    account_name = account_map_ptr->at(account_name)->parent_account;

  } while (!account_name.empty());

  return result;
}

void AccountMetaContainer::MallocQosResourceForUser_(const TaskInCtld& task) {
  user_meta_map_.modify_if(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second[task.qos];
        val.jobs_count++;
        val.resource.GetAllocatableRes() +=
            (task.requested_node_res_view * task.node_num).GetAllocatableRes();
      });
}

void AccountMetaContainer::MallocQosResourceForAccount_(
    const TaskInCtld& task) {
  std::string account_name = task.account;
  const auto account_map_ptr = g_account_manager->GetAllAccountInfo();

  do {
    account_meta_map_.modify_if(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto& val = pair.second[task.qos];
          val.jobs_count++;
          val.resource.GetAllocatableRes() +=
              (task.requested_node_res_view * task.node_num)
                  .GetAllocatableRes();
        });

    account_name = account_map_ptr->at(account_name)->parent_account;
  } while (!account_name.empty());
}

void AccountMetaContainer::FreeQosResourceForUser_(const TaskInCtld& task) {
  user_meta_map_.modify_if(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second[task.qos];
        val.resource.GetAllocatableRes() -=
            (task.requested_node_res_view * task.node_num).GetAllocatableRes();
        val.jobs_count--;
        val.submit_jobs_count--;
      });
}

void AccountMetaContainer::FreeQosResourceForAccount_(const TaskInCtld& task) {
  std::string account_name = task.account;
  const auto account_map_ptr = g_account_manager->GetAllAccountInfo();

  do {
    account_meta_map_.modify_if(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto& val = pair.second[task.qos];
          val.resource.GetAllocatableRes() -=
              (task.requested_node_res_view * task.node_num)
                  .GetAllocatableRes();
          val.jobs_count--;
          val.submit_jobs_count--;
        });
    account_name = account_map_ptr->at(account_name)->parent_account;
  } while (!account_name.empty());
}

}  // namespace Ctld
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

  CRANE_TRACE(
      "TryMallocQosSubmitResource for job of user {} and account {}.....",
      task.Username(), task.account);

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

  ResourceView resource_use{task.requested_node_res_view * task.node_num};

  if (qos->max_submit_jobs_per_user == 0)
    return CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER;

  if (!CheckTres_(resource_use, qos->max_tres_per_user))
    return CraneErrCode::ERR_MAX_TRES_PER_USER_BEYOND;

  if (qos->max_submit_jobs_per_account == 0)
    return CraneErrCode::ERR_MAX_JOB_COUNT_PER_ACCOUNT;

  if (!CheckTres_(resource_use, qos->max_tres_per_account))
    return CraneErrCode::ERR_MAX_TRES_PER_ACCOUNT_BEYOND;

  if (qos->max_submit_jobs == 0)
    return CraneErrCode::ERR_MAX_JOB_COUNT_PER_QOS;

  if (!CheckTres_(resource_use, qos->max_tres))
    return CraneErrCode::ERR_TRES_PER_TASK_BEYOND;

  task.qos_priority = qos->priority;

  if (task.time_limit >= absl::Seconds(kTaskMaxTimeLimitSec)) {
    task.time_limit = qos->max_time_limit_per_task;
  } else if (task.time_limit > qos->max_time_limit_per_task) {
    CRANE_TRACE("time-limit beyond the user's limit");
    return CraneErrCode::ERR_TIME_TIMIT_BEYOND;
  }

  // Lock the specified user/account to minimize the impact on other users and
  // accounts.
  std::lock_guard user_lock(m_user_stripes_[StripeForKey_(task.Username())]);
  auto account_locks = LockAccountStripes_(task.account_chain);

  result = CheckQosSubmitResourceForUser_(task, *qos);
  if (result != CraneErrCode::SUCCESS) return result;

  result = CheckQosSubmitResourceForAccount_(task, *qos);
  if (result != CraneErrCode::SUCCESS) return result;

  MallocQosSubmitResource(task);

  return result;
}

void AccountMetaContainer::MallocQosSubmitResource(const TaskInCtld& task) {
  CRANE_DEBUG(
      "Malloc QOS {} submit resource for job of user {} and account {}.",
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

  m_qos_meta_map_.try_emplace_l(
    task.qos,
    [&](std::pair<const std::string, QosResource>& pair) {
      auto& val = pair.second;
      val.submit_jobs_count++;
    },
    QosResource{resource_view, 0, 1});
}

void AccountMetaContainer::MallocQosResourceToRecoveredRunningTask(
    TaskInCtld& task) {
  auto qos = g_account_manager->GetExistedQosInfo(task.qos);
  // Under normal circumstances, QoS must exist.
  if (!qos) {
    CRANE_ERROR("Try to malloc resource from an unknown qos {}", task.qos);
    return;
  }

  // Lock the specified user/account to minimize the impact on other users and
  // accounts.
  std::lock_guard user_lock(m_user_stripes_[StripeForKey_(task.Username())]);
  auto account_locks = LockAccountStripes_(task.account_chain);

  CRANE_DEBUG(
      "Malloc QOS {} resource for recover job {} of user {} and account {}.",
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
          val.resource += task.allocated_res_view;
        },
        QosToResourceMap{
            {task.qos, QosResource{.resource = task.allocated_res_view,
                                   .jobs_count = 1,
                                   .submit_jobs_count = 1}}});
  }

  m_qos_meta_map_.try_emplace_l(
    task.qos,
    [&](std::pair<const std::string, QosResource>& pair) {
      auto& val = pair.second;
      val.submit_jobs_count++;
      val.jobs_count++;
      val.resource += task.allocated_res_view;
    },
    QosResource{task.allocated_res_view, 1, 1});
}

std::expected<void, std::string>
AccountMetaContainer::CheckAndMallocQosResource(const PdJobInScheduler& job) {
  CRANE_DEBUG("Malloc QOS {} resource for job {} of user {} and account {}.",
              job.qos, job.job_id, job.username, job.account);

  auto qos = g_account_manager->GetExistedQosInfo(job.qos);
  if (!qos) return std::unexpected("InvalidQOS");

  // lock
  std::lock_guard user_lock(m_user_stripes_[StripeForKey_(job.username)]);
  auto account_locks = LockAccountStripes_(job.account_chain);

  if (!CheckQosResource_(*qos, job, job.allocated_res.View()))
    return std::unexpected("QOSResourceLimit");

  m_user_meta_map_.if_contains(
      job.username, [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second.at(job.qos);
        val.jobs_count++;
        val.resource.GetAllocatableRes() +=
            job.allocated_res.View().GetAllocatableRes();
      });

  for (const auto& account_name : job.account_chain) {
    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto& val = pair.second.at(job.qos);
          val.jobs_count++;
          val.wall_time += task.time_limit;
        });
  }

  return {};
}

void AccountMetaContainer::FreeQosSubmitResource(const TaskInCtld& task) {
  CRANE_DEBUG(
      "Free QOS {} submit resource for job {} of user {} and account {}.",
      task.qos, task.TaskId(), task.Username(), task.account);

  m_user_meta_map_.if_contains(task.Username(), [&](std::pair<const std::string,
                                                              QosToResourceMap>&
                                                        pair) {
    auto iter = pair.second.find(task.qos);
    if (iter == pair.second.end()) {
      CRANE_ERROR(
          "Qos '{}' not found for user '{}', cannot free resource for task {}.",
          task.qos, task.Username(), task.TaskId());
      return;
    }
    auto& val = iter->second;
    CheckAndSubResource_(val.submit_jobs_count, static_cast<uint32_t>(1),
                         "submit_jobs_count", task.Username(), task.qos,
                         task.TaskId());
  });

  for (const auto& account_name : task.account_chain) {
    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto iter = pair.second.find(task.qos);
          if (iter == pair.second.end()) {
            CRANE_ERROR(
                "Qos '{}' not found for account '{}', cannot free resource for "
                "task {}.",
                task.qos, account_name, task.TaskId());
            return;
          }
          auto& val = iter->second;
          CheckAndSubResource_(val.submit_jobs_count, static_cast<uint32_t>(1),
                               "submit_jobs_count", account_name, task.qos,
                               task.TaskId());
        });
  }

  UserReduceTask(task.Username());
}

void AccountMetaContainer::FreeQosResource(const TaskInCtld& task) {
  CRANE_DEBUG(
      "Free QOS {} submit resource for job {} of user {} and account {}.",
      task.qos, task.TaskId(), task.Username(), task.account);

  m_user_meta_map_.if_contains(task.Username(), [&](std::pair<const std::string,
                                                              QosToResourceMap>&
                                                        pair) {
    auto iter = pair.second.find(task.qos);
    if (iter == pair.second.end()) {
      CRANE_ERROR(
          "Qos '{}' not found for user '{}', cannot free resource for job {}.",
          task.qos, task.Username(), task.TaskId());
      return;
    }
    auto& val = iter->second;
    CheckAndSubResource_(val.jobs_count, static_cast<uint32_t>(1), "jobs_count",
                         task.Username(), task.qos, task.TaskId());
    CheckAndSubResource_(val.resource.GetAllocatableRes(),
                         task.allocated_res_view.GetAllocatableRes(),
                         "AllocatableRes", task.Username(), task.qos,
                         task.TaskId());
    CheckAndSubResource_(val.submit_jobs_count, static_cast<uint32_t>(1),
                         "submit_jobs_count", task.Username(), task.qos,
                         task.TaskId());
  });

  for (const auto& account_name : task.account_chain) {
    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto iter = pair.second.find(task.qos);
          if (iter == pair.second.end()) {
            CRANE_ERROR(
                "Qos '{}' not found for account '{}', cannot free resource for "
                "job {}.",
                task.qos, account_name, task.TaskId());
            return;
          }
          auto& val = iter->second;
          CheckAndSubResource_(val.jobs_count, static_cast<uint32_t>(1),
                               "jobs_count", account_name, task.qos,
                               task.TaskId());
          CheckAndSubResource_(val.submit_jobs_count, static_cast<uint32_t>(1),
                               "submit_jobs_count", account_name, task.qos,
                               task.TaskId());
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
  if (!m_user_to_task_map_.contains(username)) {
    CRANE_ERROR("User '{}' not found in m_user_to_task_map_.", username);
    return;
  }

  m_user_to_task_map_.if_contains(
      username, [&](std::pair<const std::string, uint32_t>& pair) {
        if (pair.second == 0) {
          CRANE_ERROR("job_num == 0 when reduce user {} job", username);
          return;
        }
        --pair.second;
      });
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

  m_user_meta_map_.if_contains(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& qos_to_resource_map = pair.second;
        auto iter = qos_to_resource_map.find(task.qos);
        if (iter == qos_to_resource_map.end()) return;

        auto& val = iter->second;

        if (val.submit_jobs_count + 1 > qos.max_submit_jobs_per_user) {
          result = CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER;
          return ;
        }

        if (qos.flags & QosFlags::DenyOnLimit) {
          if (val.jobs_count + 1 > qos.max_jobs_per_user) {
            result = CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER;
            return ;
          }
          ResourceView resource_use{task.requested_node_res_view * task.node_num};
          resource_use += val.resource;
          // Compatible with the max_cpu_per_user parameter.
          if (resource_use.CpuCount() > qos.max_cpus_per_user) {
            result = CraneErrCode::ERR_MAX_TRES_PER_USER_BEYOND;
            return ;
          }
          if (!CheckTres_(resource_use, qos.max_tres_per_user))
            result = CraneErrCode::ERR_MAX_TRES_PER_USER_BEYOND;
        }
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

          if (val.submit_jobs_count + 1 > qos.max_submit_jobs_per_account) {
            result = CraneErrCode::ERR_MAX_JOB_COUNT_PER_ACCOUNT;
            return ;
          }

          if (qos.flags & QosFlags::DenyOnLimit) {
            if (val.jobs_count + 1 > qos.max_jobs_per_account) {
              result = CraneErrCode::ERR_MAX_JOB_COUNT_PER_ACCOUNT;
              return ;
            }
            ResourceView resource_use{task.requested_node_res_view *
                                      task.node_num};
            resource_use += val.resource;
            if (!CheckTres_(resource_use, qos.max_tres_per_account))
              result = CraneErrCode::ERR_MAX_TRES_PER_ACCOUNT_BEYOND;
          }
        });
  }

  return result;
}

CraneErrCode AccountMetaContainer::CheckQosSubmitResourceForQos_(
    const TaskInCtld& task, const Qos& qos) {

  auto result = CraneErrCode::SUCCESS;
  m_qos_meta_map_.if_contains(
    task.qos,
    [&](std::pair<const std::string, QosResource>& pair) {
      auto& val = pair.second;
      if (val.submit_jobs_count + 1 > qos.max_submit_jobs) {
        result = CraneErrCode::ERR_MAX_JOB_COUNT_PER_QOS;
        return;
      }
      if (qos.flags & QosFlags::DenyOnLimit) {
        if (val.jobs_count + 1 > qos.max_jobs) {
          result = CraneErrCode::ERR_MAX_JOB_COUNT_PER_QOS;
          return ;
        }

        // When max wall = 0, it means unlimited.
        if (qos.max_wall > absl::ZeroDuration()) {
          if (val.wall_time + task.time_limit > qos.max_wall) {
            result = CraneErrCode::ERR_TIME_TIMIT_BEYOND;
            return ;
          }
        }

        ResourceView resource_use{task.requested_node_res_view * task.node_num};
        resource_use += val.resource;
        if (!CheckTres_(resource_use, qos.max_tres))
          result = CraneErrCode::ERR_TRES_PER_TASK_BEYOND;
      }
    });

  return result;
}

bool AccountMetaContainer::CheckQosResource_(
    const Qos& qos, const PdJobInScheduler& job,
    const ResourceView& resource_view) {
  if (!m_user_meta_map_.contains(job.username)) {
    CRANE_ERROR("[job #{}]: User '{}' not found in m_user_meta_map_.",
                job.job_id, job.username);
    return false;
  }

  for (const auto& account_name : job.account_chain) {
    if (!m_account_meta_map_.contains(account_name)) {
      CRANE_ERROR("[job #{}]: Account '{}' not found in m_account_meta_map_.",
                  job.job_id, account_name);
      return false;
    }
  }

  bool result = true;

  m_user_meta_map_.if_contains(
      job.username, [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second.at(job.qos);
        if (val.jobs_count + 1 > qos.max_jobs_per_user) result = false;

        if (val.resource.CpuCount() + resource_view.CpuCount() >
            qos.max_cpus_per_user)
          result = false;
      });

  if (!result) return false;

  for (const auto& account_name : job.account_chain) {
    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto& val = pair.second.at(job.qos);
          if (val.jobs_count + 1 > qos.max_jobs_per_account) result = false;
        });
    if (!result) break;
  }

  return result;
}

std::vector<std::unique_lock<std::mutex>>
AccountMetaContainer::LockAccountStripes_(
    const std::list<std::string>& account_chain) {
  std::set<int> stripes;
  for (const auto& account_name : account_chain) {
    stripes.insert(StripeForKey_(account_name));
  }
  std::vector<std::unique_lock<std::mutex>> locks;
  locks.reserve(stripes.size());
  for (int stripe : stripes) {
    locks.emplace_back(m_account_stripes_[stripe]);
  }
  return locks;
}

}  // namespace Ctld
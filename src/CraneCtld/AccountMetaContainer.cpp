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
    CRANE_WARN("time-limit beyond the user's limit");
    return CraneErrCode::ERR_TIME_TIMIT_BEYOND;
  }

  CraneErrCode result = CraneErrCode::SUCCESS;

  std::set<int> account_stripes;
  for (const auto& account_name : task.account_chain) {
    account_stripes.insert(StripeForKey_(account_name));
  }

  // Lock the specified user/account to minimize the impact on other users and
  // accounts.
  std::lock_guard user_lock(m_user_stripes_[StripeForKey_(task.Username())]);
  std::list<std::unique_lock<std::mutex>> account_locks;
  for (const auto account_stripe : account_stripes) {
    account_locks.emplace_back(m_account_stripes_[account_stripe]);
  }

  std::lock_guard qos_lock(m_qos_stripes_[StripeForKey_(task.qos)]);

  result = CheckQosSubmitResourceForUser_(task, *qos);
  if (result != CraneErrCode::SUCCESS) return result;

  result = CheckQosSubmitResourceForAccount_(task, *qos);
  if (result != CraneErrCode::SUCCESS) return result;

  result = CheckQosSubmitResourceForQos_(task, *qos);
  if (result != CraneErrCode::SUCCESS) return result;

  m_user_meta_map_.try_emplace_l(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& qos_to_resource_map = pair.second;
        auto iter = qos_to_resource_map.find(task.qos);
        if (iter == qos_to_resource_map.end()) {
          qos_to_resource_map.emplace(task.qos,
                                      QosResource{ResourceView{}, 0, 1});
          return;
        }
        auto& val = iter->second;
        val.submit_jobs_count++;
      },
      QosToResourceMap{{task.qos, QosResource{ResourceView{}, 0, 1}}});

  for (const auto& account_name : task.account_chain) {
    m_account_meta_map_.try_emplace_l(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto& qos_to_resource_map = pair.second;
          auto iter = qos_to_resource_map.find(task.qos);
          if (iter == qos_to_resource_map.end()) {
            qos_to_resource_map.emplace(task.qos,
                                        QosResource{ResourceView{}, 0, 1});
            return;
          }

          auto& val = iter->second;
          val.submit_jobs_count++;
        },
        QosToResourceMap{{task.qos, QosResource{ResourceView{}, 0, 1}}});
  }

  m_qos_meta_map_.try_emplace_l(
    task.qos,
    [&](std::pair<const std::string, QosResource>& pair) {
      auto& val = pair.second;
      val.submit_jobs_count++;
    },
    QosResource{ResourceView{}, 0, 1});

  return result;
}

void AccountMetaContainer::MallocQosResourceToRecoveredPendingTask(
    TaskInCtld& task) {
  auto qos = g_account_manager->GetExistedQosInfo(task.qos);
  // Under normal circumstances, QoS must exist.
  CRANE_ASSERT(qos);

  std::set<int> account_stripes;
  for (const auto& account_name : task.account_chain) {
    account_stripes.insert(StripeForKey_(account_name));
  }

  // Lock the specified user/account to minimize the impact on other users and
  // accounts.
  std::lock_guard user_lock(m_user_stripes_[StripeForKey_(task.Username())]);
  std::list<std::unique_lock<std::mutex>> account_locks;
  for (const auto account_stripe : account_stripes) {
    account_locks.emplace_back(m_account_stripes_[account_stripe]);
  }

  ResourceView resource_view{task.requested_node_res_view * task.node_num};

  m_user_meta_map_.try_emplace_l(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& qos_to_resource_map = pair.second;
        auto iter = qos_to_resource_map.find(task.qos);
        if (iter == qos_to_resource_map.end()) {
          qos_to_resource_map.emplace(task.qos,
                                      QosResource{resource_view, 0, 1});
          return;
        }

        auto& val = iter->second;
        val.resource.GetAllocatableRes() += resource_view.GetAllocatableRes();
        val.submit_jobs_count++;
      },
      QosToResourceMap{{task.qos, QosResource{resource_view, 0, 1}}});

  for (const auto& account_name : task.account_chain) {
    m_account_meta_map_.try_emplace_l(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto& qos_to_resource_map = pair.second;
          auto iter = qos_to_resource_map.find(task.qos);
          if (iter == qos_to_resource_map.end()) {
            qos_to_resource_map.emplace(task.qos,
                                        QosResource{resource_view, 0, 1});
            return;
          }

          auto& val = iter->second;
          val.submit_jobs_count++;
        },
        QosToResourceMap{{task.qos, QosResource{resource_view, 0, 1}}});
  }
}

void AccountMetaContainer::MallocQosResourceToRecoveredRunningTask(
    TaskInCtld& task) {
  auto qos = g_account_manager->GetExistedQosInfo(task.qos);
  // Under normal circumstances, QoS must exist.
  CRANE_ASSERT(qos);

  std::set<int> account_stripes;
  for (const auto& account_name : task.account_chain) {
    account_stripes.insert(StripeForKey_(account_name));
  }

  // Lock the specified user/account to minimize the impact on other users and
  // accounts.
  std::lock_guard user_lock(m_user_stripes_[StripeForKey_(task.Username())]);
  std::list<std::unique_lock<std::mutex>> account_locks;
  for (const auto account_stripe : account_stripes) {
    account_locks.emplace_back(m_account_stripes_[account_stripe]);
  }

  ResourceView resource_view{task.requested_node_res_view * task.node_num};

  m_user_meta_map_.try_emplace_l(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& qos_to_resource_map = pair.second;
        auto iter = qos_to_resource_map.find(task.qos);
        if (iter == qos_to_resource_map.end()) {
          qos_to_resource_map.emplace(task.qos,
                                      QosResource{resource_view, 1, 1});
          return;
        }

        auto& val = iter->second;
        val.resource.GetAllocatableRes() += resource_view.GetAllocatableRes();
        val.submit_jobs_count++;
        val.jobs_count++;
      },
      QosToResourceMap{{task.qos, QosResource{resource_view, 1, 1}}});

  for (const auto& account_name : task.account_chain) {
    m_account_meta_map_.try_emplace_l(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto& qos_to_resource_map = pair.second;
          auto iter = qos_to_resource_map.find(task.qos);
          if (iter == qos_to_resource_map.end()) {
            qos_to_resource_map.emplace(task.qos,
                                        QosResource{resource_view, 1, 1});
            return;
          }

          auto& val = iter->second;
          val.submit_jobs_count++;
          val.jobs_count++;
        },
        QosToResourceMap{{task.qos, QosResource{resource_view, 1, 1}}});
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

  std::list<std::unique_lock<std::mutex>> account_locks;
  for (const auto account_stripe : account_stripes) {
    account_locks.emplace_back(m_account_stripes_[account_stripe]);
  }

  CRANE_ASSERT(m_user_meta_map_.contains(task.Username()));

  bool result = true;

  m_user_meta_map_.if_contains(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second[task.qos];

        if (val.jobs_count + 1 > qos->max_jobs_per_user) {
          result = false;
          return ;
        }

        ResourceView resource_use{task.requested_node_res_view * task.node_num};
        resource_use += val.resource;
        if (resource_use.CpuCount() > qos->max_cpus_per_user) {
          result = false;
          return ;
        }
        if (!CheckTres_(resource_use, qos->max_tres_per_user))
          result = false;
      });

  if (!result) return "QOSResourceLimit";

  for (const auto& account_name : task.account_chain) {
    CRANE_ASSERT(m_account_meta_map_.contains(account_name));
    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto& val = pair.second[task.qos];

          if (val.jobs_count + 1 > qos->max_jobs_per_account) {
            result = false;
            return ;
          }

          ResourceView resource_use{task.requested_node_res_view * task.node_num};
            resource_use += val.resource;
          if (!CheckTres_(resource_use, qos->max_tres_per_account))
            result = false;
        });
    if (!result) break;
  }

  if (!result) return "QOSResourceLimit";

  CRANE_ASSERT(m_qos_meta_map_.contains(task.qos));

  m_qos_meta_map_.if_contains(
    task.qos,
    [&](std::pair<const std::string, QosResource>& pair) {
      auto& val = pair.second;

      if (val.jobs_count + 1 > qos->max_jobs) {
        result = false;
        return ;
      }

      if (qos->max_wall > absl::ZeroDuration()) {
        if (val.wall_time + task.time_limit > qos->max_wall) {
          result = false;
          return ;
        }
      }

      ResourceView resource_use{task.requested_node_res_view * task.node_num};
      resource_use += val.resource;
      if (!CheckTres_(resource_use, qos->max_tres))
        result = false;
    });

  if (!result) return "QOSResourceLimit";

  return std::nullopt;
}

void AccountMetaContainer::MallocQosResource(const TaskInCtld& task) {

  CRANE_ASSERT(m_user_meta_map_.contains(task.Username()));
  ResourceView resource_use{task.requested_node_res_view * task.node_num};

  m_user_meta_map_.if_contains(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second[task.qos];
        val.resource += resource_use;
        val.jobs_count++;
      });

  for (const auto& account_name : task.account_chain) {
    CRANE_ASSERT(m_account_meta_map_.contains(account_name));
    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto& val = pair.second[task.qos];
          val.resource += resource_use;
          val.jobs_count++;
        });
  }

  CRANE_ASSERT(m_qos_meta_map_.contains(task.qos));
  m_qos_meta_map_.if_contains(
    task.qos,
    [&](std::pair<const std::string, QosResource>& pair) {
      auto& val = pair.second;
      val.resource += resource_use;
      val.jobs_count++;
      val.wall_time += task.time_limit;
    });

}

void AccountMetaContainer::FreeQosSubmitResource(const TaskInCtld& task) {
  CRANE_ASSERT(m_user_meta_map_.contains(task.Username()));
  m_user_meta_map_.if_contains(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second[task.qos];
        CRANE_ASSERT(val.submit_jobs_count > 0);
        val.submit_jobs_count--;
      });

  for (const auto& account_name : task.account_chain) {
    CRANE_ASSERT(m_account_meta_map_.contains(account_name));
    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto& val = pair.second[task.qos];
          CRANE_ASSERT(val.submit_jobs_count > 0);
          val.submit_jobs_count--;
        });
  }

  CRANE_ASSERT(m_qos_meta_map_.contains(task.qos));
  m_qos_meta_map_.if_contains(
  task.qos,
  [&](std::pair<const std::string, QosResource>& pair) {
    auto& val = pair.second;
    CRANE_ASSERT(val.submit_jobs_count > 0);
    val.submit_jobs_count--;
  });
}

void AccountMetaContainer::FreeQosResource(const TaskInCtld& task) {
  ResourceView resource_view{task.requested_node_res_view * task.node_num};

  CRANE_ASSERT(m_user_meta_map_.contains(task.Username()));
  m_user_meta_map_.if_contains(
      task.Username(),
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& val = pair.second[task.qos];
        CRANE_ASSERT(val.jobs_count > 0);
        CRANE_ASSERT(resource_view <= val.resource);
        CRANE_ASSERT(val.submit_jobs_count > 0);
        val.jobs_count--;
        val.resource -= resource_view;
        val.submit_jobs_count--;
        val.wall_time -= task.time_limit;
      });

  for (const auto& account_name : task.account_chain) {
    CRANE_ASSERT(m_account_meta_map_.contains(account_name));
    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto& val = pair.second[task.qos];
          CRANE_ASSERT(val.submit_jobs_count > 0);
          CRANE_ASSERT(val.jobs_count > 0);
          CRANE_ASSERT(resource_view <= val.resource);
          val.resource -= resource_view;
          val.jobs_count--;
          val.submit_jobs_count--;
        });
  }

  CRANE_ASSERT(m_qos_meta_map_.contains(task.qos));
  m_qos_meta_map_.if_contains(
    task.qos,
    [&](std::pair<const std::string, QosResource>& pair) {
      auto& val = pair.second;
      CRANE_ASSERT(val.jobs_count > 0);
      CRANE_ASSERT(val.submit_jobs_count > 0);
      CRANE_ASSERT(resource_view <= val.resource);
      val.resource -= resource_view;
      val.wall_time -= task.time_limit;
      val.jobs_count--;
      val.submit_jobs_count--;
    });
}

void AccountMetaContainer::DeleteUserMeta(const std::string& username) {
  m_user_meta_map_.erase(username);
}

void AccountMetaContainer::DeleteAccountMeta(const std::string& account) {
  m_account_meta_map_.erase(account);
}

void AccountMetaContainer::DeleteQosMeta(const std::string& qos) {
  m_qos_meta_map_.erase(qos);
}

bool AccountMetaContainer::CheckTres_(const ResourceView& resource_req,
                                     const ResourceView& resource_total) {
  if (!(resource_req.GetAllocatableRes() <= resource_total.GetAllocatableRes()))
    return false;

  const auto& device_req = resource_req.GetDeviceMap();
  const auto& device_total = resource_total.GetDeviceMap();

  for (const auto& [lhs_name, lhs_cnt] : device_req) {
    auto rhs_it = device_total.find(lhs_name);
    // Requests for unrecorded devices should not be restricted.
    if (rhs_it == device_total.end()) continue;

    const auto& [lhs_untyped_cnt, lhs_typed_cnt_map] = lhs_cnt;
    const auto& [rhs_untyped_cnt, rhs_typed_cnt_map] = rhs_it->second;

    if (lhs_untyped_cnt > rhs_untyped_cnt) return false;

    for (const auto& [lhs_type, lhs_type_cnt] : lhs_typed_cnt_map) {
      auto rhs_type_it = rhs_typed_cnt_map.find(lhs_type);
      if (rhs_type_it == rhs_typed_cnt_map.end()) continue;

      if (lhs_type_cnt > rhs_type_it->second) return false;
    }
  }

  return true;
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

}  // namespace Ctld
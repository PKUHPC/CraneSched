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

  const auto& user_ptr = g_account_manager->GetExistedUserInfo(task.Username());
  UserAddTask(task.Username());
  if (!user_ptr) return CraneErrCode::ERR_INVALID_USER;

  const auto& account_map = g_account_manager->GetAllAccountInfo();
  if (!account_map) return CraneErrCode::ERR_INVALID_ACCOUNT;

  const auto qos = g_account_manager->GetExistedQosInfo(task.qos);
  if (!qos) {
    CRANE_ERROR("Unknown QOS '{}'", task.qos);
    return CraneErrCode::ERR_INVALID_QOS;
  }

  ResourceView resource_use{task.requested_node_res_view * task.node_num};

  if (qos->max_submit_jobs_per_user == 0)
    return CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER;

  if (qos->max_submit_jobs_per_account == 0)
    return CraneErrCode::ERR_MAX_JOB_COUNT_PER_ACCOUNT;

  if (qos->max_submit_jobs == 0) return CraneErrCode::ERR_MAX_JOB_COUNT_PER_QOS;

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
  std::lock_guard qos_lock(m_qos_stripes_[StripeForKey_(task.qos)]);

  // TODO: CheckQosSubmitResource and CheckPartitionSubmitResource
  result = CheckQosSubmitResourceForUser_(task, *qos);
  if (result != CraneErrCode::SUCCESS) return result;

  result = CheckQosSubmitResourceForAccount_(task, *qos);
  if (result != CraneErrCode::SUCCESS) return result;

  result = CheckQosSubmitResourceForQos_(task, *qos);
  if (result != CraneErrCode::SUCCESS) return result;

  result = CheckPartitionSubmitResourceForUser_(task, *qos, *user_ptr);
  if (result != CraneErrCode::SUCCESS) return result;

  result = CheckPartitionSubmitResourceForAccount_(task, *qos, account_map);
  if (result != CraneErrCode::SUCCESS) return result;

  // TODO: MallocSubmitResource
  MallocQosSubmitResource(task);

  return result;
}

void AccountMetaContainer::MallocQosSubmitResource(const TaskInCtld& task) {
  CRANE_DEBUG(
      "Malloc QOS {} submit resource for task of user {} and account {}.",
      task.qos, task.Username(), task.account);

  MetaResource meta_resource{.resource = ResourceView{},
                             .jobs_count = 0,
                             .submit_jobs_count = 1};

  // malloc user resource
  m_user_meta_map_.try_emplace_l(
      task.Username(),
      [&](std::pair<const std::string, MetaResourceStat>& pair) {
        auto& qos_to_resource_map = pair.second.qos_to_resource_map;
        auto qos_iter = qos_to_resource_map.find(task.qos);
        if (qos_iter == qos_to_resource_map.end()) {
          qos_to_resource_map.emplace(task.qos, meta_resource);
        } else {
          auto& val = qos_iter->second;
          val.submit_jobs_count++;
        }

        auto& partition_to_resource_map = pair.second.partition_to_resource_map;
        auto partition_iter = partition_to_resource_map.find(task.partition_id);
        if (partition_iter == partition_to_resource_map.end()) {
          partition_to_resource_map.emplace(task.partition_id, meta_resource);
        } else {
          auto& val = partition_iter->second;
          val.submit_jobs_count++;
        }
      },
      MetaResourceStat{
          // none exist
          .qos_to_resource_map = {{task.qos, meta_resource}},
          .partition_to_resource_map = {{task.partition_id, meta_resource}}});

  // malloc account resource
  for (const auto& account_name : task.account_chain) {
    m_account_meta_map_.try_emplace_l(
        account_name,
        [&](std::pair<const std::string, MetaResourceStat>& pair) {
          auto& qos_to_resource_map = pair.second.qos_to_resource_map;
          auto qos_iter = qos_to_resource_map.find(task.qos);
          if (qos_iter == qos_to_resource_map.end()) {
            qos_to_resource_map.emplace(task.qos, meta_resource);
          } else {
            auto& val = qos_iter->second;
            val.submit_jobs_count++;
          }

          auto& partition_to_resource_map =
              pair.second.partition_to_resource_map;
          auto partition_iter =
              partition_to_resource_map.find(task.partition_id);
          if (partition_iter == partition_to_resource_map.end()) {
            partition_to_resource_map.emplace(task.partition_id, meta_resource);
          } else {
            auto& val = partition_iter->second;
            val.submit_jobs_count++;
          }
        },
        MetaResourceStat{
            .qos_to_resource_map = {{task.qos, meta_resource}},
            .partition_to_resource_map = {{task.partition_id, meta_resource}}});
  }

  // malloc qos resource
  m_qos_meta_map_.try_emplace_l(
      task.qos,
      [&](std::pair<const std::string, MetaResource>& pair) {
        auto& val = pair.second;
        val.submit_jobs_count++;
      },
      meta_resource);
}

void AccountMetaContainer::MallocQosResourceToRecoveredRunningTask(
    TaskInCtld& task) {
  CRANE_DEBUG(
      "Malloc QOS {} resource for recover task {} of user {} and account {}.",
      task.qos, task.TaskId(), task.Username(), task.account);

  MetaResource meta_resource{.resource = task.allocated_res_view,
                             .jobs_count = 1,
                             .submit_jobs_count = 1,
                             .wall_time = task.time_limit};

  m_user_meta_map_.try_emplace_l(
      task.Username(),
      [&](std::pair<const std::string, MetaResourceStat>& pair) {
        auto& qos_to_resource_map = pair.second.qos_to_resource_map;
        auto qos_iter = qos_to_resource_map.find(task.qos);
        if (qos_iter == qos_to_resource_map.end()) {
          qos_to_resource_map.emplace(task.qos, meta_resource);
        } else {
          auto& val = qos_iter->second;
          val.resource += task.allocated_res_view;
          val.submit_jobs_count++;
          val.jobs_count++;
        }

        auto& partition_to_resource_map = pair.second.partition_to_resource_map;
        auto partition_iter = partition_to_resource_map.find(task.partition_id);
        if (partition_iter == partition_to_resource_map.end()) {
          partition_to_resource_map.emplace(task.partition_id, meta_resource);
        } else {
          auto& val = partition_iter->second;
          val.resource += task.allocated_res_view;
          val.submit_jobs_count++;
          val.jobs_count++;
        }
      },
      MetaResourceStat{
            .qos_to_resource_map = {{task.qos, meta_resource}},
            .partition_to_resource_map = {{task.partition_id, meta_resource}}});

  for (const auto& account_name : task.account_chain) {
    m_account_meta_map_.try_emplace_l(
        account_name,
        [&](std::pair<const std::string, MetaResourceStat>& pair) {
          auto& qos_to_resource_map = pair.second.qos_to_resource_map;
          auto qos_iter = qos_to_resource_map.find(task.qos);
          if (qos_iter == qos_to_resource_map.end()) {
            qos_to_resource_map.emplace(task.qos, meta_resource);
          } else {
            auto& val = qos_iter->second;
            val.submit_jobs_count++;
            val.jobs_count++;
            val.resource += task.allocated_res_view;
          }

          auto& partition_to_resource_map =
              pair.second.partition_to_resource_map;
          auto partition_iter =
              partition_to_resource_map.find(task.partition_id);
          if (partition_iter == partition_to_resource_map.end()) {
            partition_to_resource_map.emplace(task.partition_id, meta_resource);
          } else {
            auto& val = partition_iter->second;
            val.submit_jobs_count++;
            val.jobs_count++;
            val.resource += task.allocated_res_view;
          }
        },
        MetaResourceStat{
            .qos_to_resource_map = {{task.qos, meta_resource}},
            .partition_to_resource_map = {{task.partition_id, meta_resource}}});
  }

  m_qos_meta_map_.try_emplace_l(
      task.qos,
      [&](std::pair<const std::string, MetaResource>& pair) {
        auto& val = pair.second;
        val.submit_jobs_count++;
        val.jobs_count++;
        val.resource += task.allocated_res_view;
      },
      meta_resource);
}

std::optional<std::string> AccountMetaContainer::CheckQosResource(
    const TaskInCtld& task) {
  const auto& user_ptr = g_account_manager->GetExistedUserInfo(task.Username());
  const auto& account_map = g_account_manager->GetAllAccountInfo();
  const auto qos = g_account_manager->GetExistedQosInfo(task.qos);
  if (!qos) return "InvalidQOS";

  std::set<int> account_stripes;
  for (const auto& account : task.account_chain) {
    account_stripes.insert(StripeForKey_(account));
  }
  std::lock_guard user_lock(m_user_stripes_[StripeForKey_(task.Username())]);
  std::list<std::unique_lock<std::mutex>> account_locks;
  for (const auto account_stripe : account_stripes) {
    account_locks.emplace_back(m_account_stripes_[account_stripe]);
  }
  std::lock_guard qos_lock(m_qos_stripes_[StripeForKey_(task.qos)]);

  CRANE_ASSERT(m_user_meta_map_.contains(task.Username()));

  auto partition_iter =
      user_ptr->partition_to_resource_limit_map.find(task.partition_id);
  if (partition_iter == user_ptr->partition_to_resource_limit_map.end())
    return "InvalidPartition";
  const auto& user_partition_limit = partition_iter->second;
  std::string result;

  m_user_meta_map_.if_contains(
      task.Username(),
      [&](std::pair<const std::string, MetaResourceStat>& pair) {
        {  // qos
          auto& qos_to_resource_map = pair.second.qos_to_resource_map;
          auto& val = qos_to_resource_map[task.qos];
          if (val.jobs_count + 1 > qos->max_jobs_per_user) {
            result = "QosJobsAccountResourceLimit";
            return;
          }
          ResourceView resource_use{task.requested_node_res_view *
                                    task.node_num};
          resource_use += val.resource;
          if (resource_use.CpuCount() > qos->max_cpus_per_user) {
            result = "QosCpuResourceLimit";
            return;
          }
          result = CheckTres_(resource_use, qos->max_tres_per_user);
          if (!result.empty()) return;
        }  // end qos

        {  // partition
          auto& partition_to_resource_map =
              pair.second.partition_to_resource_map;
          auto& val = partition_to_resource_map[task.partition_id];
          if (qos->max_jobs == UINT32_MAX &&
              qos->max_jobs_per_user == UINT32_MAX) {
            if (val.jobs_count + 1 > user_partition_limit.max_jobs) {
              result = "UserPartJobsAccountResourceLimit";
              return;
            }
          }
          ResourceView resource_use{task.requested_node_res_view *
                                    task.node_num};
          resource_use += val.resource;
          if (qos->max_cpus_per_user == UINT32_MAX / 256 &&
              qos->max_tres_per_user.CpuCount() == UINT32_MAX / 256) {
            if (resource_use.CpuCount() >
                user_partition_limit.max_tres.CpuCount()) {
              result = "UserPartCpuResourceLimit";
              return;
            }
          }
          if (qos->max_tres_per_user.MemoryBytes() == UINT64_MAX) {
            if (resource_use.MemoryBytes() >
                user_partition_limit.max_tres.MemoryBytes()) {
              result = "UserPartMemResourceLimit";
              return;
            }
          }
          // TODO: Gres limit rule override？
          if (!CheckGres_(resource_use.GetDeviceMap(),
                          user_partition_limit.max_tres.GetDeviceMap())) {
            result = "UserPartGresResourceLimit";
            return;
          }
        }  // end partition
      });

  if (!result.empty()) return result;

  for (const auto& account_name : task.account_chain) {
    CRANE_ASSERT(m_account_meta_map_.contains(account_name));
    const auto& partition_to_resource_limit_map =
        account_map->at(account_name)->partition_to_resource_limit_map;
    auto iter = partition_to_resource_limit_map.find(task.partition_id);
    if (iter == partition_to_resource_limit_map.end()) {
      result = "InvalidPartition";
      return result;
    }
    const auto& partition_resource_limit = iter->second;
    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, MetaResourceStat>& pair) {
          {  // qos
            auto& val = pair.second.qos_to_resource_map[task.qos];
            if (val.jobs_count + 1 > qos->max_jobs_per_account) {
              result = "QosJobsAccountResourceLimit";
              return;
            }
            ResourceView resource_use{task.requested_node_res_view *
                                      task.node_num};
            resource_use += val.resource;
            result = CheckTres_(resource_use, qos->max_tres_per_account);
            if (!result.empty()) return;
          }  // end qos

          {  // partition
            auto& val =
                pair.second.partition_to_resource_map[task.partition_id];
            if (qos->max_jobs == UINT32_MAX &&
                qos->max_jobs_per_account == UINT32_MAX) {
              if (val.jobs_count + 1 > partition_resource_limit.max_jobs) {
                result = "AccPartJobsAccountResourceLimit";
                return;
              }
            }
            ResourceView resource_use{task.requested_node_res_view *
                                      task.node_num};
            resource_use += val.resource;
            if (qos->max_tres_per_account.CpuCount() == UINT32_MAX / 256) {
              if (resource_use.CpuCount() >
                  partition_resource_limit.max_tres.CpuCount()) {
                result = "AccPartCpuResourceLimit";
                return;
              }
            }
            if (qos->max_tres_per_account.MemoryBytes() == UINT64_MAX) {
              if (resource_use.MemoryBytes() >
                  partition_resource_limit.max_tres.MemoryBytes()) {
                result = "AccPartMemResourceLimit";
                return;
              }
            }
            if (!CheckGres_(resource_use.GetDeviceMap(),
                            partition_resource_limit.max_tres.GetDeviceMap())) {
              result = "AccPartGresResourceLimit";
              return;
            }
          }  // end partition
        });

    if (!result.empty()) return result;
  }

  CRANE_ASSERT(m_qos_meta_map_.contains(task.qos));

  m_qos_meta_map_.if_contains(
      task.qos, [&](std::pair<const std::string, MetaResource>& pair) {
        auto& val = pair.second;
        if (val.jobs_count + 1 > qos->max_jobs) {
          result = "QosJobsAccountResourceLimit";
          return;
        }
        if (qos->max_wall > absl::ZeroDuration()) {
          if (val.wall_time + task.time_limit > qos->max_wall) {
            result = "QosWallResourceLimit";
            return;
          }
        }
        ResourceView resource_use{task.requested_node_res_view * task.node_num};
        resource_use += val.resource;
        result = CheckTres_(resource_use, qos->max_tres);
      });

  if (!result.empty()) return result;

  return std::nullopt;
}

void AccountMetaContainer::MallocQosResource(const TaskInCtld& task) {
  CRANE_ASSERT(m_user_meta_map_.contains(task.Username()));

  m_user_meta_map_.if_contains(
      task.Username(),
      [&](std::pair<const std::string, MetaResourceStat>& pair) {
        {  // qos
          auto& val = pair.second.qos_to_resource_map[task.qos];
          val.resource += task.allocated_res_view;
          val.jobs_count++;
          val.wall_time += task.time_limit;
        }  // end qos

        {  // partition
          auto& val = pair.second.partition_to_resource_map[task.partition_id];
          val.resource += task.allocated_res_view;
          val.jobs_count++;
          val.wall_time += task.time_limit;
        }  // end partition
      });

  for (const auto& account_name : task.account_chain) {
    CRANE_ASSERT(m_account_meta_map_.contains(account_name));
    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, MetaResourceStat>& pair) {
          {  // qos
            auto& val = pair.second.qos_to_resource_map[task.qos];
            val.resource += task.allocated_res_view;
            val.jobs_count++;
            val.wall_time += task.time_limit;
          }  // end qos

          {  // partition
            auto& val =
                pair.second.partition_to_resource_map[task.partition_id];
            val.resource += task.allocated_res_view;
            val.jobs_count++;
            val.wall_time += task.time_limit;
          }  // end partition
        });
  }

  CRANE_ASSERT(m_qos_meta_map_.contains(task.qos));
  m_qos_meta_map_.if_contains(
      task.qos, [&](std::pair<const std::string, MetaResource>& pair) {
        auto& val = pair.second;
        val.resource += task.allocated_res_view;
        val.jobs_count++;
        val.wall_time += task.time_limit;
      });
}

void AccountMetaContainer::FreeQosSubmitResource(const TaskInCtld& task) {
  CRANE_ASSERT(m_user_meta_map_.contains(task.Username()));
  m_user_meta_map_.if_contains(
      task.Username(),
      [&](std::pair<const std::string, MetaResourceStat>& pair) {
        {  // qos
          auto& val = pair.second.qos_to_resource_map[task.qos];
          CRANE_ASSERT(val.submit_jobs_count > 0);
          val.submit_jobs_count--;
        }  // end qos

        {  // partition
          auto& val = pair.second.partition_to_resource_map[task.partition_id];
          CRANE_ASSERT(val.submit_jobs_count > 0);
          val.submit_jobs_count--;
        }  // end partition
      });

  for (const auto& account_name : task.account_chain) {
    CRANE_ASSERT(m_account_meta_map_.contains(account_name));
    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, MetaResourceStat>& pair) {
          {  // qos
            auto& val = pair.second.qos_to_resource_map[task.qos];
            CRANE_ASSERT(val.submit_jobs_count > 0);
            val.submit_jobs_count--;
          }  // end qos

          {  // partition
            auto& val =
                pair.second.partition_to_resource_map[task.partition_id];
            CRANE_ASSERT(val.submit_jobs_count > 0);
            val.submit_jobs_count--;
          }  // end partition
        });
  }

  CRANE_ASSERT(m_qos_meta_map_.contains(task.qos));
  m_qos_meta_map_.if_contains(
      task.qos, [&](std::pair<const std::string, MetaResource>& pair) {
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
      [&](std::pair<const std::string, MetaResourceStat>& pair) {
        {  // qos
          auto& val = pair.second.qos_to_resource_map[task.qos];
          CRANE_ASSERT(val.jobs_count > 0);
          CRANE_ASSERT(task.allocated_res_view <= val.resource);
          CRANE_ASSERT(val.submit_jobs_count > 0);
          val.jobs_count--;
          val.resource -= task.allocated_res_view;
          val.submit_jobs_count--;
          val.wall_time -= task.time_limit;
        }  // end qos

        {  // partition
          auto& val = pair.second.partition_to_resource_map[task.partition_id];
          CRANE_ASSERT(val.jobs_count > 0);
          CRANE_ASSERT(task.allocated_res_view <= val.resource);
          CRANE_ASSERT(val.submit_jobs_count > 0);
          val.jobs_count--;
          val.resource -= task.allocated_res_view;
          val.submit_jobs_count--;
          val.wall_time -= task.time_limit;
        }  // end partition
      });

  for (const auto& account_name : task.account_chain) {
    CRANE_ASSERT(m_account_meta_map_.contains(account_name));
    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, MetaResourceStat>& pair) {
          {  // qos
            auto& val = pair.second.qos_to_resource_map[task.qos];
            CRANE_ASSERT(val.submit_jobs_count > 0);
            CRANE_ASSERT(val.jobs_count > 0);
            CRANE_ASSERT(task.allocated_res_view <= val.resource);
            val.resource -= task.allocated_res_view;
            val.jobs_count--;
            val.submit_jobs_count--;
            val.wall_time -= task.time_limit;
          }  // end qos

          {  // partition
            auto& val =
                pair.second.partition_to_resource_map[task.partition_id];
            CRANE_ASSERT(val.submit_jobs_count > 0);
            CRANE_ASSERT(val.jobs_count > 0);
            CRANE_ASSERT(task.allocated_res_view <= val.resource);
            val.resource -= task.allocated_res_view;
            val.jobs_count--;
            val.submit_jobs_count--;
            val.wall_time -= task.time_limit;
          }  // end partition
        });
  }

  CRANE_ASSERT(m_qos_meta_map_.contains(task.qos));
  m_qos_meta_map_.if_contains(
      task.qos, [&](std::pair<const std::string, MetaResource>& pair) {
        auto& val = pair.second;
        CRANE_ASSERT(val.jobs_count > 0);
        CRANE_ASSERT(val.submit_jobs_count > 0);
        CRANE_ASSERT(task.allocated_res_view <= val.resource);
        val.resource -= task.allocated_res_view;
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

void AccountMetaContainer::UserAddTask(const std::string& username) {
  m_user_to_task_map_.try_emplace_l(
      username,
      [&](std::pair<const std::string, uint32_t>& pair) { ++pair.second; }, 1);
}

void AccountMetaContainer::UserReduceTask(const std::string& username) {
  CRANE_ASSERT(m_user_meta_map_.contains(username));

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

std::string AccountMetaContainer::CheckTres_(
    const ResourceView& resource_req, const ResourceView& resource_total) {
  if (resource_req.CpuCount() > resource_total.CpuCount()) {
    return "QosCpuResourceLimit";
  }
  if (resource_req.MemoryBytes() > resource_total.MemoryBytes()) {
    return "QosMemResourceLimit";
  }

  if (!CheckGres_(resource_req.GetDeviceMap(), resource_total.GetDeviceMap()))
    return "QosGresResourceLimit";

  return "";
}
bool AccountMetaContainer::CheckGres_(const DeviceMap& device_req,
                                      const DeviceMap& device_total) {
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
      [&](std::pair<const std::string, MetaResourceStat>& pair) {
        auto& qos_to_resource_map = pair.second.qos_to_resource_map;
        auto iter = qos_to_resource_map.find(task.qos);
        if (iter == qos_to_resource_map.end()) return;

        auto& val = iter->second;

        if (val.submit_jobs_count + 1 > qos.max_submit_jobs_per_user) {
          result = CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER;
          return;
        }

        if (qos.flags & QosFlags::DenyOnLimit) {
          if (val.jobs_count + 1 > qos.max_jobs_per_user) {
            result = CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER;
            return;
          }
          ResourceView resource_use{task.requested_node_res_view *
                                    task.node_num};
          resource_use += val.resource;
          // Compatible with the max_cpu_per_user parameter.
          if (resource_use.CpuCount() > qos.max_cpus_per_user) {
            result = CraneErrCode::ERR_MAX_TRES_PER_USER_BEYOND;
            return;
          }
          if (!CheckTres_(resource_use, qos.max_tres_per_user).empty())
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
        [&](std::pair<const std::string, MetaResourceStat>& pair) {
          auto& qos_to_resource_map = pair.second.qos_to_resource_map;
          auto iter = qos_to_resource_map.find(task.qos);
          if (iter == qos_to_resource_map.end()) return;

          auto& val = iter->second;

          if (val.submit_jobs_count + 1 > qos.max_submit_jobs_per_account) {
            result = CraneErrCode::ERR_MAX_JOB_COUNT_PER_ACCOUNT;
            return;
          }

          if (qos.flags & QosFlags::DenyOnLimit) {
            if (val.jobs_count + 1 > qos.max_jobs_per_account) {
              result = CraneErrCode::ERR_MAX_JOB_COUNT_PER_ACCOUNT;
              return;
            }
            ResourceView resource_use{task.requested_node_res_view *
                                      task.node_num};
            resource_use += val.resource;
            if (!CheckTres_(resource_use, qos.max_tres_per_account).empty())
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
      task.qos, [&](std::pair<const std::string, MetaResource>& pair) {
        auto& val = pair.second;
        if (val.submit_jobs_count + 1 > qos.max_submit_jobs) {
          result = CraneErrCode::ERR_MAX_JOB_COUNT_PER_QOS;
          return;
        }
        if (qos.flags & QosFlags::DenyOnLimit) {
          if (val.jobs_count + 1 > qos.max_jobs) {
            result = CraneErrCode::ERR_MAX_JOB_COUNT_PER_QOS;
            return;
          }

          // When max wall = 0, it means unlimited.
          if (qos.max_wall > absl::ZeroDuration()) {
            if (val.wall_time + task.time_limit > qos.max_wall) {
              result = CraneErrCode::ERR_TIME_TIMIT_BEYOND;
              return;
            }
          }

          ResourceView resource_use{task.requested_node_res_view *
                                    task.node_num};
          resource_use += val.resource;
          if (!CheckTres_(resource_use, qos.max_tres).empty())
            result = CraneErrCode::ERR_TRES_PER_TASK_BEYOND;
        }
      });

  return result;
}

CraneErrCode AccountMetaContainer::CheckPartitionSubmitResourceForUser_(
    const TaskInCtld& task, const Qos& qos, const User& user) {
  if (qos.max_submit_jobs < UINT32_MAX ||
      qos.max_submit_jobs_per_user < UINT32_MAX)
    return CraneErrCode::SUCCESS;

  auto iter = user.partition_to_resource_limit_map.find(task.partition_id);
  if (iter == user.partition_to_resource_limit_map.end())
    return CraneErrCode::ERR_PARTITION_MISSING;

  const auto& partition_resource_limit = iter->second;

  if (partition_resource_limit.max_submit_jobs == 0)
    return CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER;

  CraneErrCode result = CraneErrCode::SUCCESS;

  m_user_meta_map_.if_contains(
      task.Username(),
      [&](std::pair<const std::string, MetaResourceStat>& pair) {
        auto& partition_to_resource_map = pair.second.partition_to_resource_map;
        auto iter = partition_to_resource_map.find(task.partition_id);
        if (iter == partition_to_resource_map.end()) return;

        auto val = iter->second;
        if (val.submit_jobs_count + 1 >
            partition_resource_limit.max_submit_jobs) {
          result = CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER;
          return;
        }

        val.submit_jobs_count++;
      });

  return result;
}

CraneErrCode AccountMetaContainer::CheckPartitionSubmitResourceForAccount_(
    const TaskInCtld& task, const Qos& qos,
    const AccountManager::AccountMapMutexSharedPtr& account_map) {
  if (qos.max_submit_jobs < UINT32_MAX ||
      qos.max_submit_jobs_per_account < UINT32_MAX) {
    CRANE_DEBUG(
        "Qos {} restrictions override the partition {}'s max_submit_jobs rule, "
        "causing it to not take effect.",
        task.qos, task.partition_id);
    return CraneErrCode::SUCCESS;
  }

  CraneErrCode result = CraneErrCode::SUCCESS;

  for (const auto& account_name : task.account_chain) {
    auto iter = account_map->find(account_name);
    if (iter == account_map->end()) return CraneErrCode::ERR_INVALID_ACCOUNT;

    const auto& partition_to_resource_limit_map =
        iter->second->partition_to_resource_limit_map;
    auto partition_resource_limit_iter =
        partition_to_resource_limit_map.find(task.partition_id);
    if (partition_resource_limit_iter == partition_to_resource_limit_map.end())
      return CraneErrCode::ERR_PARTITION_MISSING;

    const auto& partition_resource_limit =
        partition_resource_limit_iter->second;
    if (partition_resource_limit.max_submit_jobs == 0)
      return CraneErrCode::ERR_MAX_JOB_COUNT_PER_ACCOUNT;

    m_account_meta_map_.if_contains(
        task.account,
        [&](std::pair<const std::string, MetaResourceStat>& pair) {
          auto& partition_to_resource_map =
              pair.second.partition_to_resource_map;
          auto iter = partition_to_resource_map.find(task.partition_id);
          if (iter == partition_to_resource_map.end()) return;

          auto val = iter->second;
          if (val.submit_jobs_count + 1 >
              partition_resource_limit.max_submit_jobs) {
            result = CraneErrCode::ERR_MAX_JOB_COUNT_PER_ACCOUNT;
            return;
          }

          val.submit_jobs_count++;
        });

    if (result != CraneErrCode::SUCCESS) break;
  }

  return result;
}

}  // namespace Ctld
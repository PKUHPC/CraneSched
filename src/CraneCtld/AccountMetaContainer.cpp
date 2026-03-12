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
#include "TaskScheduler.h"
#include <absl/time/time.h>

namespace Ctld {

bool MetaResource::operator<=(const MetaResource& rhs) const {
  return submit_jobs_count <= rhs.submit_jobs_count &&
         jobs_count <= rhs.jobs_count && wall_time <= rhs.wall_time &&
         resource <= rhs.resource;
}

MetaResource& MetaResource::operator+=(const MetaResource& rhs) {
  this->submit_jobs_count += rhs.submit_jobs_count;
  this->jobs_count += rhs.jobs_count;
  this->wall_time += rhs.wall_time;
  this->resource += rhs.resource;
  return *this;
}

MetaResource& MetaResource::operator-=(const MetaResource& rhs) {
  this->submit_jobs_count -= rhs.submit_jobs_count;
  this->jobs_count -= rhs.jobs_count;
  this->wall_time -= rhs.wall_time;
  this->resource -= rhs.resource;
  return *this;
}

void MetaResource::SetToZero() {
  this->submit_jobs_count = 0;
  this->jobs_count = 0;
  this->wall_time = absl::ZeroDuration();
  this->resource.SetToZero();
}

std::string MetaResource::DebugString() const {
  return fmt::format(
      "MetaResource{{submit_jobs_count: {}, jobs_count: {}, wall_time: {}s, "
      "resource: {}}}",
      submit_jobs_count, jobs_count, absl::ToInt64Seconds(wall_time),
      util::ReadableResourceView(resource));
}

CraneErrCode AccountMetaContainer::TryMallocQosSubmitResource(
    TaskInCtld& task) {
  CraneErrCode result = CraneErrCode::SUCCESS;

  CRANE_TRACE(
      "TryMallocQosSubmitResource for job of user {} and account {}.....",
      task.Username(), task.account);

  auto qos = g_account_manager->GetExistedQosInfo(task.qos);
  if (!qos) {
    CRANE_ERROR("Unknown QOS '{}'", task.qos);
    return CraneErrCode::ERR_INVALID_QOS;
  }

  const ResourceView& resource_use = task.req_total_res_view;

  if (qos->max_submit_jobs_per_user == 0)
    return CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER;

  if (qos->max_submit_jobs_per_account == 0)
    return CraneErrCode::ERR_MAX_JOB_COUNT_PER_ACCOUNT;

  if (qos->max_submit_jobs == 0)
    return CraneErrCode::ERR_QOS_JOB_COUNT_EXCEEDED;

  if (static_cast<double>(task.cpus_per_task) * task.node_num >
      qos->max_cpus_per_user)
    return CraneErrCode::ERR_CPUS_PER_TASK_BEYOND;

  if (!CheckTres_(resource_use, qos->max_tres_per_user) ||
      !CheckTres_(resource_use, qos->max_tres_per_account) ||
      !CheckTres_(resource_use, qos->max_tres))
    return CraneErrCode::ERR_TRES_PER_TASK_BEYOND;

  task.qos_priority = qos->priority;

  if (task.time_limit >= absl::Seconds(kTaskMaxTimeLimitSec)) {
    task.time_limit = qos->max_time_limit_per_task;
  } else if (task.time_limit > qos->max_time_limit_per_task) {
    CRANE_TRACE("time-limit beyond the user's limit");
    return CraneErrCode::ERR_TIME_TIMIT_BEYOND;
  }

  // Lock the specified user/account/qos to minimize the impact on other users
  // and accounts.
  std::scoped_lock user_lock(m_user_stripes_[StripeForKey_(task.Username())]);
  auto account_locks = LockAccountStripes_(task.account_chain);
  std::scoped_lock qos_lock(m_qos_stripes_[StripeForKey_(task.qos)]);

  result = CheckUserQosSubmitResourceUsage_(task, *qos);
  if (result != CraneErrCode::SUCCESS) return result;

  result = CheckAccountQosSubmitResourceUsage_(task, *qos);
  if (result != CraneErrCode::SUCCESS) return result;

  result = CheckQosSubmitResourceUsage_(task, *qos);
  if (result != CraneErrCode::SUCCESS) return result;

  MallocQosSubmitResource(task);

  return result;
}

void AccountMetaContainer::MallocQosSubmitResource(const TaskInCtld& task) {
  CRANE_DEBUG(
      "Malloc QOS {} submit resource for job of user {} and account {}.",
      task.qos, task.Username(), task.account);

  MetaResource meta_resource{.resource = ResourceView{},
                             .jobs_count = 0,
                             .submit_jobs_count = 1,
                             .wall_time = absl::ZeroDuration()};
  DoMallocResource_(task.TaskId(), task.Username(), task.account_chain,
                    task.qos, meta_resource);
}

void AccountMetaContainer::MallocQosResourceToRecoveredRunningTask(
    TaskInCtld& task) {
  auto qos = g_account_manager->GetExistedQosInfo(task.qos);
  // Under normal circumstances, QoS must exist.
  if (!qos) {
    CRANE_ERROR("Try to malloc resource from an unknown qos {}", task.qos);
    return;
  }

  CRANE_DEBUG(
      "Malloc QOS {} resource for recover job {} of user {} and account {}.",
      task.qos, task.TaskId(), task.Username(), task.account);

  g_account_meta_container->UserAddTask(task.Username());

  MetaResource meta_resource{.resource = task.allocated_res_view,
                             .jobs_count = 1,
                             .submit_jobs_count = 1,
                             .wall_time = task.time_limit};

  DoMallocResource_(task.TaskId(), task.Username(), task.account_chain,
                    task.qos, meta_resource);
}

std::expected<void, std::string>
AccountMetaContainer::CheckAndMallocQosResource(const PdJobInScheduler& job) {
  CRANE_TRACE("Check QOS {} resource for job {} of user {} and account {}.",
              job.qos, job.job_id, job.username, job.account);

  auto qos = g_account_manager->GetExistedQosInfo(job.qos);
  if (!qos) return std::unexpected("InvalidQOS");

  // lock
  std::scoped_lock user_lock(m_user_stripes_[StripeForKey_(job.username)]);
  auto account_locks = LockAccountStripes_(job.account_chain);
  std::scoped_lock qos_lock(m_qos_stripes_[StripeForKey_(job.qos)]);

  auto result = CheckQosResource_(*qos, job);
  if (!result) return result;

  CRANE_DEBUG("Malloc QOS {} resource for job {} of user {} and account {}.",
              job.qos, job.job_id, job.username, job.account);

  MetaResource meta_resource{.resource = job.allocated_res.View(),
                             .jobs_count = 1,
                             .submit_jobs_count = 0,
                             .wall_time = job.time_limit};

  DoMallocResource_(job.job_id, job.username, job.account_chain, job.qos,
                    meta_resource);

  return {};
}

void AccountMetaContainer::FreeQosSubmitResource(const TaskInCtld& task) {
  CRANE_DEBUG(
      "Free QOS {} submit resource for job {} of user {} and account {}.",
      task.qos, task.TaskId(), task.Username(), task.account);

  MetaResource meta_resource{.resource = ResourceView{},
                             .jobs_count = 0,
                             .submit_jobs_count = 1,
                             .wall_time = absl::ZeroDuration()};

  DoFreeResource_(task.TaskId(), task.Username(), task.account_chain, task.qos,
                  meta_resource);
}

void AccountMetaContainer::FreeQosResource(const TaskInCtld& task) {
  CRANE_DEBUG("Free QOS {} resource for job {} of user {} and account {}.",
              task.qos, task.TaskId(), task.Username(), task.account);

  MetaResource meta_resource{.resource = task.allocated_res_view,
                             .jobs_count = 1,
                             .submit_jobs_count = 1,
                             .wall_time = task.time_limit};

  DoFreeResource_(task.TaskId(), task.Username(), task.account_chain, task.qos,
                  meta_resource);
}

void AccountMetaContainer::UserAddTask(const std::string& username) {
  m_user_to_task_map_.try_emplace_l(
      username,
      [&](std::pair<const std::string, uint32_t>& pair) { ++pair.second; }, 1);
}

void AccountMetaContainer::UserReduceTask(const std::string& username) {
  bool is_contains = false;

  m_user_to_task_map_.if_contains(
      username, [&](std::pair<const std::string, uint32_t>& pair) {
        is_contains = true;
        if (pair.second == 0) {
          CRANE_ERROR("job_num == 0 when reduce user {} job", username);
          return;
        }
        --pair.second;
      });

  if (!is_contains)
    CRANE_ERROR("User '{}' not found in m_user_to_task_map_.", username);
}

bool AccountMetaContainer::UserHasTask(const std::string& username) {
  bool result = false;

  m_user_to_task_map_.if_contains(
      username, [&](std::pair<const std::string, uint32_t>& pair) {
        if (pair.second > 0) result = true;
      });

  return result;
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

/* ---------------------------------------------------------------------------
 * Primary
 * ---------------------------------------------------------------------------
 */

CraneErrCode AccountMetaContainer::CheckUserQosSubmitResourceUsage_(
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
          return;
        }

        if (qos.flags[QosFlags::DenyOnLimit]) {
          if (val.jobs_count + 1 > qos.max_jobs_per_user) {
            result = CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER;
            return;
          }
          ResourceView resource_use{task.req_total_res_view};
          resource_use += val.resource;
          // Compatible with the max_cpu_per_user parameter.
          if (resource_use.CpuCount() > qos.max_cpus_per_user) {
            result = CraneErrCode::ERR_MAX_TRES_PER_USER_BEYOND;
            return;
          }
          if (!CheckTres_(resource_use, qos.max_tres_per_user)) {
            result = CraneErrCode::ERR_MAX_TRES_PER_USER_BEYOND;
            return;
          }
        }
      });

  return result;
}

CraneErrCode AccountMetaContainer::CheckAccountQosSubmitResourceUsage_(
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
            return;
          }

          if (qos.flags[QosFlags::DenyOnLimit]) {
            if (val.jobs_count + 1 > qos.max_jobs_per_account) {
              result = CraneErrCode::ERR_MAX_JOB_COUNT_PER_ACCOUNT;
              return;
            }
            ResourceView resource_use{task.requested_node_res_view *
                                      task.node_num};
            resource_use += val.resource;
            if (!CheckTres_(resource_use, qos.max_tres_per_account)) {
              result = CraneErrCode::ERR_MAX_TRES_PER_ACCOUNT_BEYOND;
              return ;
            }
          }
        });
  }

  return result;
}

CraneErrCode AccountMetaContainer::CheckQosSubmitResourceUsage_(
    const TaskInCtld& task, const Qos& qos) {
  auto result = CraneErrCode::SUCCESS;
  m_qos_meta_map_.if_contains(
      task.qos, [&](std::pair<const std::string, MetaResource>& pair) {
        auto& val = pair.second;
        if (val.submit_jobs_count + 1 > qos.max_submit_jobs) {
          result = CraneErrCode::ERR_QOS_JOB_COUNT_EXCEEDED;
          return;
        }
        if (qos.flags[QosFlags::DenyOnLimit]) {
          if (val.jobs_count + 1 > qos.max_jobs) {
            result = CraneErrCode::ERR_QOS_JOB_COUNT_EXCEEDED;
            return;
          }
          
          if (qos.max_wall > absl::ZeroDuration()) {
            if (val.wall_time + task.time_limit > qos.max_wall) {
              result = CraneErrCode::ERR_TIME_TIMIT_BEYOND;
              return;
            }
          }
          ResourceView resource_use{task.requested_node_res_view *
                                    task.node_num};
          resource_use += val.resource;
          if (!CheckTres_(resource_use, qos.max_tres)) {
            result = CraneErrCode::ERR_TRES_PER_TASK_BEYOND;
            return ;
          }
        }
      });

  return result;
}

std::expected<void, std::string> AccountMetaContainer::CheckQosResource_(
    const Qos& qos, const PdJobInScheduler& job) {
  if (!m_user_meta_map_.contains(job.username)) {
    CRANE_ERROR("[job #{}]: User '{}' not found in m_user_meta_map_.",
                job.job_id, job.username);
    return std::unexpected("QosResourceLimit");
  }

  for (const auto& account_name : job.account_chain) {
    if (!m_account_meta_map_.contains(account_name)) {
      CRANE_ERROR("[job #{}]: Account '{}' not found in m_account_meta_map_.",
                  job.job_id, account_name);
      return std::unexpected("QosResourceLimit");
    }
  }

  if (!m_qos_meta_map_.contains(job.qos)) {
    CRANE_ERROR("[job #{}]: qos '{}' not found in m_qos_meta_map_.", job.job_id,
                job.qos);
    return std::unexpected("QosResourceLimit");
  }

  std::expected<void, std::string> result;

  m_user_meta_map_.if_contains(job.username, [&](std::pair<const std::string,
                                                           QosToResourceMap>&
                                                     pair) {
    auto iter = pair.second.find(job.qos);
    if (iter == pair.second.end()) {
      CRANE_ERROR(
          "Qos '{}' not found for user '{}', cannot free resource for task {}.",
          job.qos, job.username, job.job_id);
      result = std::unexpected("QosResourceLimit");
      return;
    }
    auto& val = iter->second;
    auto resource_use = job.allocated_res.View();
    resource_use += val.resource;
    if (resource_use.CpuCount() > qos.max_cpus_per_user) {
      result = std::unexpected("QosCpuResourceLimit");
      return;
    }
    if (val.jobs_count + 1 > qos.max_jobs_per_user) {
      result = std::unexpected("QosJobsResourceLimit");
      return;
    }
    if (qos.max_wall > absl::ZeroDuration()) {
      if (val.wall_time + job.time_limit > qos.max_wall) {
        result = std::unexpected("QosWallTimeLimit");
        return;
      }
    }
    result = CheckTres_(resource_use, qos.max_tres_per_user);
  });

  if (!result) return result;

  for (const auto& account_name : job.account_chain) {
    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto iter = pair.second.find(job.qos);
          if (iter == pair.second.end()) {
            CRANE_ERROR(
                "Qos '{}' not found for account '{}', cannot free resource for "
                "job {}.",
                job.qos, account_name, job.job_id);
            result = std::unexpected("QosResourceLimit");
            return;
          }
          auto& val = iter->second;
          auto resource_use = job.allocated_res.View();
          resource_use += val.resource;
          if (val.jobs_count + 1 > qos.max_jobs_per_account) {
            result = std::unexpected("QosJobsResourceLimit");
            return;
          }
          if (qos.max_wall > absl::ZeroDuration()) {
            if (val.wall_time + job.time_limit > qos.max_wall) {
              result = std::unexpected("QosWallTimeLimit");
              return;
            }
          }
          result = CheckTres_(resource_use, qos.max_tres_per_account);
        });
    if (!result) break;
  }

  if (!result) return result;

  m_qos_meta_map_.if_contains(
      job.qos, [&](std::pair<const std::string, MetaResource>& pair) {
        auto& val = pair.second;
        auto resource_use = job.allocated_res.View();
        resource_use += val.resource;
        if (val.jobs_count + 1 > qos.max_jobs) {
          result = std::unexpected("QosJobsResourceLimit");
          return;
        }
        if (qos.max_wall > absl::ZeroDuration()) {
          if (val.wall_time + job.time_limit > qos.max_wall) {
            result = std::unexpected("QosWallTimeLimit");
            return;
          }
        }
        result = CheckTres_(resource_use, qos.max_tres);
      });

  return result;
}

std::expected<void, std::string> AccountMetaContainer::CheckTres_(
    const ResourceView& resource_req, const ResourceView& resource_total) {
  if (resource_req.CpuCount() > resource_total.CpuCount()) {
    return std::unexpected("QosCpuResourceLimit");
  }

  if (resource_req.MemoryBytes() > resource_total.MemoryBytes()) {
    return std::unexpected("QosMemResourceLimit");
  }

  if (!CheckGres_(resource_req.GetDeviceMap(), resource_total.GetDeviceMap()))
    return std::unexpected("QosGresResourceLimit");

  return {};
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

void AccountMetaContainer::DoMallocResource_(
    job_id_t job_id, const std::string& username,
    const std::list<std::string>& account_chain, const std::string& qos,
    const MetaResource& meta_resource) {
  m_user_meta_map_.try_emplace_l(
      username,
      [&](std::pair<const std::string, QosToResourceMap>& pair) {
        auto& qos_to_resource_map = pair.second;
        auto iter = qos_to_resource_map.find(qos);
        if (iter == qos_to_resource_map.end()) {
          qos_to_resource_map.emplace(qos, meta_resource);
        } else {
          auto& val = iter->second;
          val += meta_resource;
        }
      },
      QosToResourceMap{{qos, meta_resource}});

  for (const auto& account_name : account_chain) {
    m_account_meta_map_.try_emplace_l(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto& qos_to_resource_map = pair.second;
          auto iter = qos_to_resource_map.find(qos);
          if (iter == qos_to_resource_map.end()) {
            qos_to_resource_map.emplace(qos, meta_resource);
          } else {
            auto& val = iter->second;
            val += meta_resource;
          }
        },
        QosToResourceMap{{qos, meta_resource}});
  }

  m_qos_meta_map_.try_emplace_l(
      qos,
      [&](std::pair<const std::string, MetaResource>& pair) {
        auto& val = pair.second;
        val += meta_resource;
      },
      meta_resource);
}

void AccountMetaContainer::DoFreeResource_(
    job_id_t job_id, const std::string& username,
    const std::list<std::string>& account_chain, const std::string& qos,
    const MetaResource& meta_resource) {
  m_user_meta_map_.if_contains(username, [&](std::pair<const std::string,
                                                       QosToResourceMap>&
                                                 pair) {
    auto iter = pair.second.find(qos);
    if (iter == pair.second.end()) {
      CRANE_ERROR(
          "Qos '{}' not found for user '{}', cannot free resource for job {}.",
          qos, username, job_id);
      return;
    }
    auto& val = iter->second;
    if (meta_resource <= val) {
      val -= meta_resource;
    } else {
      CRANE_ERROR(
          "Trying to free more resource than allocated for job {} of user {}, "
          "cur: {}, need: {}.",
          job_id, username, val.DebugString(), meta_resource.DebugString());
      val.SetToZero();
    }

    if (val.IsZero()) pair.second.erase(iter);
  });

  for (const auto& account_name : account_chain) {
    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, QosToResourceMap>& pair) {
          auto iter = pair.second.find(qos);
          if (iter == pair.second.end()) {
            CRANE_ERROR(
                "Qos '{}' not found for account '{}', cannot free resource for "
                "job {}.",
                qos, account_name, job_id);
            return;
          }
          auto& val = iter->second;
          if (meta_resource <= val) {
            val -= meta_resource;
          } else {
            CRANE_ERROR(
                "Trying to free more resource than allocated for job {} of "
                "account {}, cur: {}, need: {}.",
                job_id, account_name, val.DebugString(),
                meta_resource.DebugString());
            val.SetToZero();
          }

          if (val.IsZero()) pair.second.erase(iter);
        });
  }

  m_qos_meta_map_.if_contains(
      qos, [&](std::pair<const std::string, MetaResource>& pair) {
        auto& val = pair.second;
        if (meta_resource <= val) {
          val -= meta_resource;
        } else {
          CRANE_ERROR(
              "Trying to free more resource than allocated for job {} of qos "
              "{}, cur: {}, need: {}.",
              job_id, qos, val.DebugString(), meta_resource.DebugString());
          val.SetToZero();
        }
      });

  UserReduceTask(username);
}

}  // namespace Ctld
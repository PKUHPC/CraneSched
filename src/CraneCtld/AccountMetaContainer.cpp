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

#include <absl/time/time.h>

#include "AccountManager.h"
#include "JobScheduler.h"

namespace Ctld {

/* ---------------------------------------------------------------------------
 * MetaResource operators
 * ---------------------------------------------------------------------------
 */

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

/* ---------------------------------------------------------------------------
 * Public interface
 * ---------------------------------------------------------------------------
 */

CraneErrCode AccountMetaContainer::TryMallocMetaSubmitResource(JobInCtld& job) {
  CRANE_TRACE(
      "TryMallocMetaSubmitResource for job of user {} and account {}.....",
      job.Username(), job.account);

  auto qos = g_account_manager->GetExistedQosInfo(job.qos);
  if (!qos) {
    CRANE_ERROR("Unknown QOS '{}'", job.qos);
    return CraneErrCode::ERR_INVALID_QOS;
  }

  const ResourceView& resource_use = job.req_total_res_view;

  if (qos->max_submit_jobs_per_user == 0)
    return CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER;

  if (qos->max_submit_jobs_per_account == 0)
    return CraneErrCode::ERR_MAX_JOB_COUNT_PER_ACCOUNT;

  if (qos->max_submit_jobs == 0)
    return CraneErrCode::ERR_QOS_JOB_COUNT_EXCEEDED;

  if (resource_use.GetCpuCount() > qos->max_cpus_per_user)
    return CraneErrCode::ERR_CPUS_PER_TASK_BEYOND;

  if (!CheckTres_(resource_use, qos->max_tres_per_user) ||
      !CheckTres_(resource_use, qos->max_tres_per_account) ||
      !CheckTres_(resource_use, qos->max_tres))
    return CraneErrCode::ERR_TRES_PER_JOB_BEYOND;

  job.qos_priority = qos->priority;

  if (job.time_limit >= absl::Seconds(kJobMaxTimeLimitSec)) {
    job.time_limit = qos->max_time_limit_per_job;
  } else if (job.time_limit > qos->max_time_limit_per_job) {
    CRANE_TRACE("time-limit beyond the user's limit");
    return CraneErrCode::ERR_TIME_TIMIT_BEYOND;
  }

  // Lock the specified user/account/qos to minimize the impact on other users
  // and accounts.
  std::scoped_lock user_lock(m_user_stripes_[StripeForKey_(job.Username())]);
  auto account_locks = LockAccountStripes_(job.account_chain);
  std::scoped_lock qos_lock(m_qos_stripes_[StripeForKey_(job.qos)]);

  CraneErrCode result = CheckSubmitLimits_(job, *qos);
  if (result != CraneErrCode::SUCCESS) return result;

  MallocMetaSubmitResource(job);
  return CraneErrCode::SUCCESS;
}

void AccountMetaContainer::MallocMetaSubmitResource(const JobInCtld& job) {
  CRANE_DEBUG("Malloc meta submit resource for job of user {} and account {}.",
              job.Username(), job.account);

  MetaResource meta_resource{.resource = ResourceView{},
                             .jobs_count = 0,
                             .submit_jobs_count = 1,
                             .wall_time = absl::ZeroDuration()};
  DoMallocResource_(job.JobId(), job.Username(), job.account_chain, job.qos,
                    job.partition_id, meta_resource);
}

void AccountMetaContainer::MallocMetaResourceToRecoveredRunningJob(
    JobInCtld& job) {
  auto qos = g_account_manager->GetExistedQosInfo(job.qos);
  // Under normal circumstances, QoS must exist.
  if (!qos) {
    CRANE_ERROR("Try to malloc resource from an unknown qos {}", job.qos);
    return;
  }

  CRANE_DEBUG(
      "Malloc meta resource for recover job {} of user {} and account {}.",
      job.JobId(), job.Username(), job.account);

  g_account_meta_container->UserAddJob(job.Username());

  MetaResource meta_resource{.resource = job.allocated_res_view,
                             .jobs_count = 1,
                             .submit_jobs_count = 1,
                             .wall_time = job.time_limit};

  DoMallocResource_(job.JobId(), job.Username(), job.account_chain, job.qos,
                    job.partition_id, meta_resource);
}

std::expected<void, std::string>
AccountMetaContainer::CheckAndMallocMetaResource(const PdJobInScheduler& job) {
  CRANE_TRACE("Check meta resource for job {} of user {} and account {}.",
              job.job_id, job.username, job.account);

  auto qos = g_account_manager->GetExistedQosInfo(job.qos);
  if (!qos) return std::unexpected("InvalidQOS");

  // lock
  std::scoped_lock user_lock(m_user_stripes_[StripeForKey_(job.username)]);
  auto account_locks = LockAccountStripes_(job.account_chain);
  std::scoped_lock qos_lock(m_qos_stripes_[StripeForKey_(job.qos)]);

  auto result = CheckRunLimits_(job, *qos);
  if (!result) return result;

  CRANE_DEBUG("Malloc meta resource for job {} of user {} and account {}.",
              job.job_id, job.username, job.account);

  MetaResource meta_resource{.resource = job.allocated_res.View(),
                             .jobs_count = 1,
                             .submit_jobs_count = 0,
                             .wall_time = job.time_limit};

  DoMallocResource_(job.job_id, job.username, job.account_chain, job.qos,
                    job.partition_id, meta_resource);

  return {};
}

void AccountMetaContainer::FreeMetaSubmitResource(const JobInCtld& job) {
  CRANE_DEBUG("Free meta submit resource for job {} of user {} and account {}.",
              job.JobId(), job.Username(), job.account);

  MetaResource meta_resource{.resource = ResourceView{},
                             .jobs_count = 0,
                             .submit_jobs_count = 1,
                             .wall_time = absl::ZeroDuration()};

  DoFreeResource_(job.JobId(), job.Username(), job.account_chain, job.qos,
                  job.partition_id, meta_resource);
}

void AccountMetaContainer::FreeMetaResource(const JobInCtld& job) {
  CRANE_DEBUG("Free meta resource for job {} of user {} and account {}.",
              job.JobId(), job.Username(), job.account);

  MetaResource meta_resource{.resource = job.allocated_res_view,
                             .jobs_count = 1,
                             .submit_jobs_count = 1,
                             .wall_time = job.time_limit};

  DoFreeResource_(job.JobId(), job.Username(), job.account_chain, job.qos,
                  job.partition_id, meta_resource);
}

void AccountMetaContainer::UserAddJob(const std::string& username) {
  m_user_to_job_map_.try_emplace_l(
      username,
      [&](std::pair<const std::string, uint32_t>& pair) { ++pair.second; }, 1);
}

void AccountMetaContainer::UserReduceJob(const std::string& username) {
  bool is_contains = false;

  m_user_to_job_map_.if_contains(
      username, [&](std::pair<const std::string, uint32_t>& pair) {
        is_contains = true;
        if (pair.second == 0) {
          CRANE_ERROR("job_num == 0 when reduce user {} job", username);
          return;
        }
        --pair.second;
      });

  if (!is_contains)
    CRANE_ERROR("User '{}' not found in m_user_to_job_map_.", username);
}

bool AccountMetaContainer::UserHasJob(const std::string& username) {
  bool result = false;

  m_user_to_job_map_.if_contains(
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
 * Stateless primitive checks
 * ---------------------------------------------------------------------------
 */

std::expected<void, std::string> AccountMetaContainer::CheckTres_(
    const ResourceView& resource_req, const ResourceView& resource_total,
    const std::string& prefix) {
  if (resource_req.GetCpuCount() > resource_total.GetCpuCount()) {
    return std::unexpected(prefix + "CpuResourceLimit");
  }

  if (resource_req.GetMemoryBytes() > resource_total.GetMemoryBytes()) {
    return std::unexpected(prefix + "MemResourceLimit");
  }

  if (!(resource_req <= resource_total))
    return std::unexpected(prefix + "GresResourceLimit");

  return {};
}

bool AccountMetaContainer::IsUnlimitedTres_(const ResourceView& res) {
  return res.GetCpuCount() == kUnlimitedCpu &&
         res.GetMemoryBytes() == kMaxJobMemoryBytes && res.GetGresMap().empty();
}

/* ---------------------------------------------------------------------------
 * Per-entity checks (User or Account)
 *
 * QoS and Partition are peer-level dimensions under each entity.
 * ---------------------------------------------------------------------------
 */

CraneErrCode AccountMetaContainer::CheckQosSubmitLimitsForEntity_(
    const MetaResourceStat& stat, const std::string& qos_name, const Qos& qos,
    bool is_user, const ResourceView& req_res) const {
  auto it = stat.qos_to_resource_map.find(qos_name);
  if (it == stat.qos_to_resource_map.end()) return CraneErrCode::SUCCESS;
  const MetaResource& val = it->second;

  const uint32_t max_submit =
      is_user ? qos.max_submit_jobs_per_user : qos.max_submit_jobs_per_account;
  if (val.submit_jobs_count + 1 > max_submit) {
    return is_user ? CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER
                   : CraneErrCode::ERR_MAX_JOB_COUNT_PER_ACCOUNT;
  }

  if (qos.flags[QosFlags::DenyOnLimit]) {
    const uint32_t max_jobs =
        is_user ? qos.max_jobs_per_user : qos.max_jobs_per_account;
    if (val.jobs_count + 1 > max_jobs) {
      return is_user ? CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER
                     : CraneErrCode::ERR_MAX_JOB_COUNT_PER_ACCOUNT;
    }

    ResourceView resource_use{req_res};
    resource_use += val.resource;

    if (is_user) {
      if (resource_use.GetCpuCount() > qos.max_cpus_per_user)
        return CraneErrCode::ERR_CPUS_PER_TASK_BEYOND;
      if (!CheckTres_(resource_use, qos.max_tres_per_user))
        return CraneErrCode::ERR_MAX_TRES_PER_USER_BEYOND;
    } else {
      if (!CheckTres_(resource_use, qos.max_tres_per_account))
        return CraneErrCode::ERR_MAX_TRES_PER_ACCOUNT_BEYOND;
    }
  }

  return CraneErrCode::SUCCESS;
}

CraneErrCode AccountMetaContainer::CheckPartitionSubmitLimitsForEntity_(
    const MetaResourceStat& stat, const std::string& partition_id,
    const PartitionResourceLimit* partition_limit, const ResourceView& req_res,
    absl::Duration time_limit, bool is_user) const {
  if (!partition_limit) return CraneErrCode::SUCCESS;

  if (!CheckTres_(req_res, partition_limit->max_tres_per_job)) {
    return CraneErrCode::ERR_TRES_PER_JOB_BEYOND;
  }

  if (time_limit > partition_limit->max_wall_duration_per_job) {
    return CraneErrCode::ERR_TIME_TIMIT_BEYOND;
  }

  auto pit = stat.partition_to_resource_map.find(partition_id);
  if (pit != stat.partition_to_resource_map.end()) {
    if (pit->second.submit_jobs_count + 1 > partition_limit->max_submit_jobs)
      return is_user ? CraneErrCode::ERR_MAX_JOB_COUNT_PER_USER
                     : CraneErrCode::ERR_MAX_JOB_COUNT_PER_ACCOUNT;
  }

  return CraneErrCode::SUCCESS;
}

CraneErrCode AccountMetaContainer::CheckEntitySubmitLimits_(
    const MetaResourceStat& stat, const std::string& qos_name, const Qos& qos,
    const std::string& partition_id,
    const PartitionResourceLimit* partition_limit, bool is_user,
    const ResourceView& req_res, absl::Duration time_limit) const {
  // QoS dimension (peer-level with Partition).
  CraneErrCode result =
      CheckQosSubmitLimitsForEntity_(stat, qos_name, qos, is_user, req_res);
  if (result != CraneErrCode::SUCCESS) return result;

  // Partition dimension (peer-level with QoS).
  return CheckPartitionSubmitLimitsForEntity_(
      stat, partition_id, partition_limit, req_res, time_limit, is_user);
}

std::expected<void, std::string>
AccountMetaContainer::CheckQosRunLimitsForEntity_(
    const MetaResourceStat& stat, const std::string& qos_name, const Qos& qos,
    bool is_user, const ResourceView& allocated_res,
    absl::Duration time_limit) const {
  auto it = stat.qos_to_resource_map.find(qos_name);
  if (it == stat.qos_to_resource_map.end()) {
    // Entry must exist at schedule time (created at submit time).
    return std::unexpected("QosEntryNotFound");
  }
  const MetaResource& val = it->second;

  ResourceView resource_use{allocated_res};
  resource_use += val.resource;

  if (is_user) {
    if (resource_use.GetCpuCount() > qos.max_cpus_per_user)
      return std::unexpected("QosCpuResourceLimit");
    if (val.jobs_count + 1 > qos.max_jobs_per_user)
      return std::unexpected("QosJobsResourceLimit");
    if (qos.max_wall > absl::ZeroDuration() &&
        val.wall_time + time_limit > qos.max_wall)
      return std::unexpected("QosWallTimeLimit");
    return CheckTres_(resource_use, qos.max_tres_per_user);
  } else {
    if (val.jobs_count + 1 > qos.max_jobs_per_account)
      return std::unexpected("QosJobsResourceLimit");
    if (qos.max_wall > absl::ZeroDuration() &&
        val.wall_time + time_limit > qos.max_wall)
      return std::unexpected("QosWallTimeLimit");
    return CheckTres_(resource_use, qos.max_tres_per_account);
  }
}

std::expected<void, std::string>
AccountMetaContainer::CheckPartitionRunLimitsForEntity_(
    const MetaResourceStat& stat, const std::string& partition_id,
    const PartitionResourceLimit* partition_limit,
    const ResourceView& allocated_res, absl::Duration time_limit,
    const Qos& qos, bool is_user) const {
  if (!partition_limit) return {};

  auto pit = stat.partition_to_resource_map.find(partition_id);
  if (pit == stat.partition_to_resource_map.end())
    return std::unexpected("PartitionResourceLimit");

  const MetaResource& val = pit->second;

  // max_jobs: only enforced when QoS does not already cap running jobs.
  const uint32_t qos_max_jobs =
      is_user ? qos.max_jobs_per_user : qos.max_jobs_per_account;
  if (qos.max_jobs == std::numeric_limits<uint32_t>::max() &&
      qos_max_jobs == std::numeric_limits<uint32_t>::max()) {
    if (val.jobs_count + 1 > partition_limit->max_jobs) {
      return std::unexpected(is_user ? "UserPartitionJobsLimit"
                                     : "AccPartitionJobsLimit");
    }
  }

  // max_wall: only enforced when QoS does not already cap wall time.
  if (qos.max_wall == absl::ZeroDuration() &&
      partition_limit->max_wall > absl::ZeroDuration()) {
    if (val.wall_time + time_limit > partition_limit->max_wall) {
      return std::unexpected(is_user ? "UserPartitionWallTimeLimit"
                                     : "AccPartitionWallTimeLimit");
    }
  }

  const ResourceView& qos_max_tres =
      is_user ? qos.max_tres_per_user : qos.max_tres_per_account;
  if (IsUnlimitedTres_(qos.max_tres) && IsUnlimitedTres_(qos_max_tres)) {
    ResourceView resource_use{allocated_res};
    resource_use += val.resource;
    auto tres_result = CheckTres_(resource_use, partition_limit->max_tres, "Partition");
    if (!tres_result) return tres_result;
  }

  return {};
}

std::expected<void, std::string> AccountMetaContainer::CheckEntityRunLimits_(
    const MetaResourceStat& stat, const std::string& qos_name, const Qos& qos,
    const std::string& partition_id,
    const PartitionResourceLimit* partition_limit, bool is_user,
    const ResourceView& allocated_res, absl::Duration time_limit) const {
  // QoS dimension (peer-level with Partition).
  auto result = CheckQosRunLimitsForEntity_(stat, qos_name, qos, is_user,
                                            allocated_res, time_limit);
  if (!result) return result;

  // Partition dimension (peer-level with QoS).
  return CheckPartitionRunLimitsForEntity_(stat, partition_id, partition_limit,
                                           allocated_res, time_limit, qos,
                                           is_user);
}

/* ---------------------------------------------------------------------------
 *  Aggregated checks (User → AccountChain → GlobalQoS)
 * ---------------------------------------------------------------------------
 */

CraneErrCode AccountMetaContainer::CheckSubmitLimits_(const JobInCtld& job,
                                                      const Qos& qos) {
  CraneErrCode result = CraneErrCode::SUCCESS;

  // ---- User entity ----
  {
    const PartitionResourceLimit* user_part_limit = nullptr;
    auto user_ptr = g_account_manager->GetExistedUserInfo(job.Username());
    if (!user_ptr) {
      CRANE_ERROR("User '{}' not found in AccountManager during submit check.",
                  job.Username());
      return CraneErrCode::ERR_INVALID_USER;
    }
    auto acct_it = user_ptr->account_to_attrs_map.find(job.account);
    if (acct_it != user_ptr->account_to_attrs_map.end()) {
      auto part_it =
          acct_it->second.partition_to_limit_map.find(job.partition_id);
      if (part_it != acct_it->second.partition_to_limit_map.end())
        user_part_limit = &part_it->second;
    }

    m_user_meta_map_.if_contains(
        job.Username(),
        [&](std::pair<const std::string, MetaResourceStat>& pair) {
          result = CheckEntitySubmitLimits_(
              pair.second, job.qos, qos, job.partition_id, user_part_limit,
              /*is_user=*/true, job.req_total_res_view, job.time_limit);
        });
    if (result != CraneErrCode::SUCCESS) return result;
  }

  // ---- Account chain ----
  for (const auto& account_name : job.account_chain) {
    const PartitionResourceLimit* acct_part_limit = nullptr;
    auto acct_ptr = g_account_manager->GetExistedAccountInfo(account_name);
    if (!acct_ptr) {
      CRANE_ERROR(
          "Account '{}' not found in AccountManager during submit check.",
          account_name);
      return CraneErrCode::ERR_INVALID_ACCOUNT;
    }
    auto part_it = acct_ptr->partition_to_limit_map.find(job.partition_id);
    if (part_it != acct_ptr->partition_to_limit_map.end())
      acct_part_limit = &part_it->second;

    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, MetaResourceStat>& pair) {
          result = CheckEntitySubmitLimits_(
              pair.second, job.qos, qos, job.partition_id, acct_part_limit,
              /*is_user=*/false, job.req_total_res_view, job.time_limit);
        });
    if (result != CraneErrCode::SUCCESS) return result;
  }

  // ---- Global QoS counters ----
  m_qos_meta_map_.if_contains(
      job.qos, [&](std::pair<const std::string, MetaResource>& pair) {
        const MetaResource& val = pair.second;
        if (val.submit_jobs_count + 1 > qos.max_submit_jobs) {
          result = CraneErrCode::ERR_QOS_JOB_COUNT_EXCEEDED;
          return;
        }
        if (qos.flags[QosFlags::DenyOnLimit]) {
          if (val.jobs_count + 1 > qos.max_jobs) {
            result = CraneErrCode::ERR_QOS_JOB_COUNT_EXCEEDED;
            return;
          }
          if (qos.max_wall > absl::ZeroDuration() &&
              val.wall_time + job.time_limit > qos.max_wall) {
            result = CraneErrCode::ERR_TIME_TIMIT_BEYOND;
            return;
          }
          ResourceView resource_use{job.req_total_res_view};
          resource_use += val.resource;
          if (!CheckTres_(resource_use, qos.max_tres)) {
            result = CraneErrCode::ERR_TRES_PER_JOB_BEYOND;
          }
        }
      });

  return result;
}

std::expected<void, std::string> AccountMetaContainer::CheckRunLimits_(
    const PdJobInScheduler& job, const Qos& qos) {
  // Verify that all required entries exist before checking limits.
  if (!m_user_meta_map_.contains(job.username)) {
    CRANE_ERROR("[job #{}]: User '{}' not found in m_user_meta_map_.",
                job.job_id, job.username);
    return std::unexpected("UserMetaNotFound");
  }

  for (const auto& account_name : job.account_chain) {
    if (!m_account_meta_map_.contains(account_name)) {
      CRANE_ERROR("[job #{}]: Account '{}' not found in m_account_meta_map_.",
                  job.job_id, account_name);
      return std::unexpected("AccountMetaNotFound");
    }
  }

  if (!m_qos_meta_map_.contains(job.qos)) {
    CRANE_ERROR("[job #{}]: qos '{}' not found in m_qos_meta_map_.", job.job_id,
                job.qos);
    return std::unexpected("QosMetaNotFound");
  }

  const ResourceView allocated_res = job.allocated_res.View();
  std::expected<void, std::string> result;

  // ---- User entity ----
  {
    const PartitionResourceLimit* user_part_limit = nullptr;
    auto user_ptr = g_account_manager->GetExistedUserInfo(job.username);
    if (!user_ptr) {
      CRANE_ERROR(
          "[job #{}]: User '{}' not found in AccountManager during "
          "run check.",
          job.job_id, job.username);
      return std::unexpected("InvalidUser");
    }
    auto acct_it = user_ptr->account_to_attrs_map.find(job.account);
    if (acct_it != user_ptr->account_to_attrs_map.end()) {
      auto part_it =
          acct_it->second.partition_to_limit_map.find(job.partition_id);
      if (part_it != acct_it->second.partition_to_limit_map.end())
        user_part_limit = &part_it->second;
    }

    m_user_meta_map_.if_contains(
        job.username,
        [&](std::pair<const std::string, MetaResourceStat>& pair) {
          result = CheckEntityRunLimits_(
              pair.second, job.qos, qos, job.partition_id, user_part_limit,
              /*is_user=*/true, allocated_res, job.time_limit);
        });
    if (!result) return result;
  }

  // ---- Account chain ----
  for (const auto& account_name : job.account_chain) {
    const PartitionResourceLimit* acct_part_limit = nullptr;
    auto acct_ptr = g_account_manager->GetExistedAccountInfo(account_name);
    if (!acct_ptr) {
      CRANE_ERROR(
          "[job #{}]: Account '{}' not found in AccountManager during "
          "run check.",
          job.job_id, account_name);
      return std::unexpected("InvalidAccount");
    }
    auto part_it = acct_ptr->partition_to_limit_map.find(job.partition_id);
    if (part_it != acct_ptr->partition_to_limit_map.end())
      acct_part_limit = &part_it->second;

    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, MetaResourceStat>& pair) {
          result = CheckEntityRunLimits_(
              pair.second, job.qos, qos, job.partition_id, acct_part_limit,
              /*is_user=*/false, allocated_res, job.time_limit);
        });
    if (!result) return result;
  }

  // ---- Global QoS counters ----
  m_qos_meta_map_.if_contains(
      job.qos, [&](std::pair<const std::string, MetaResource>& pair) {
        const MetaResource& val = pair.second;
        ResourceView resource_use{allocated_res};
        resource_use += val.resource;

        if (val.jobs_count + 1 > qos.max_jobs) {
          result = std::unexpected("QosJobsResourceLimit");
          return;
        }
        if (qos.max_wall > absl::ZeroDuration() &&
            val.wall_time + job.time_limit > qos.max_wall) {
          result = std::unexpected("QosWallTimeLimit");
          return;
        }
        result = CheckTres_(resource_use, qos.max_tres);
      });

  return result;
}

/* ---------------------------------------------------------------------------
 * Malloc / Free helpers
 * ---------------------------------------------------------------------------
 */

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
    const std::string& partition_id, const MetaResource& meta_resource) {
  // Helper: upsert a MetaResource into a map by key.
  auto upsert = [&](auto& map, const std::string& key) {
    auto it = map.find(key);
    if (it == map.end())
      map.emplace(key, meta_resource);
    else
      it->second += meta_resource;
  };

  m_user_meta_map_.try_emplace_l(
      username,
      [&](std::pair<const std::string, MetaResourceStat>& pair) {
        upsert(pair.second.qos_to_resource_map, qos);
        upsert(pair.second.partition_to_resource_map, partition_id);
      },
      MetaResourceStat{
          .qos_to_resource_map = {{qos, meta_resource}},
          .partition_to_resource_map = {{partition_id, meta_resource}}});

  for (const auto& account_name : account_chain) {
    m_account_meta_map_.try_emplace_l(
        account_name,
        [&](std::pair<const std::string, MetaResourceStat>& pair) {
          upsert(pair.second.qos_to_resource_map, qos);
          upsert(pair.second.partition_to_resource_map, partition_id);
        },
        MetaResourceStat{
            .qos_to_resource_map = {{qos, meta_resource}},
            .partition_to_resource_map = {{partition_id, meta_resource}}});
  }

  m_qos_meta_map_.try_emplace_l(
      qos,
      [&](std::pair<const std::string, MetaResource>& pair) {
        pair.second += meta_resource;
      },
      meta_resource);
}

void AccountMetaContainer::DoFreeResource_(
    job_id_t job_id, const std::string& username,
    const std::list<std::string>& account_chain, const std::string& qos,
    const std::string& partition_id, const MetaResource& meta_resource) {
  // Helper: safely subtract meta_resource from map[key], zeroing on underflow.
  auto safe_sub = [&](auto& map, const std::string& key,
                      const std::string& entity_type,
                      const std::string& entity_name) {
    auto it = map.find(key);
    if (it == map.end()) {
      CRANE_ERROR(
          "'{}' not found for {} '{}', cannot free resource for job {}.", key,
          entity_type, entity_name, job_id);
      return;
    }
    auto& val = it->second;
    if (meta_resource <= val) {
      val -= meta_resource;
    } else {
      CRANE_ERROR(
          "Trying to free more resource than allocated for job {} of {} {}, "
          "cur: {}, need: {}.",
          job_id, entity_type, entity_name, val.DebugString(),
          meta_resource.DebugString());
      val.SetToZero();
    }
    if (val.IsZero()) map.erase(it);
  };

  m_user_meta_map_.if_contains(
      username, [&](std::pair<const std::string, MetaResourceStat>& pair) {
        safe_sub(pair.second.qos_to_resource_map, qos, "user", username);
        safe_sub(pair.second.partition_to_resource_map, partition_id, "user",
                 username);
      });

  for (const auto& account_name : account_chain) {
    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, MetaResourceStat>& pair) {
          safe_sub(pair.second.qos_to_resource_map, qos, "account",
                   account_name);
          safe_sub(pair.second.partition_to_resource_map, partition_id,
                   "account", account_name);
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

  UserReduceJob(username);
}

}  // namespace Ctld

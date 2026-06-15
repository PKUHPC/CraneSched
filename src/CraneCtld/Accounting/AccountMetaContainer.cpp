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

#include "Account/AccountManager.h"
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

CraneErrCode AccountMetaContainer::TryMallocMetaSubmitResource(JobInCtld& job,
                                                               const User& user,
                                                               uint32_t count) {
  CRANE_TRACE(
      "TryMallocMetaSubmitResource for job of user {} and account {}, "
      "count={}.",
      job.Username(), job.account, count);

  if (count == 0) return CraneErrCode::SUCCESS;

  auto account_map = g_account_manager->GetAllAccountInfo();
  if (!account_map) {
    CRANE_ERROR("No account found in AccountManager during submit check.");
    return CraneErrCode::ERR_INVALID_ACCOUNT;
  }

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

  CraneErrCode result =
      CheckSubmitLimits_(job, user, *account_map, *qos, count);
  if (result != CraneErrCode::SUCCESS) return result;

  MallocMetaSubmitResource(job, count);
  return CraneErrCode::SUCCESS;
}

void AccountMetaContainer::MallocMetaSubmitResource(const JobInCtld& job,
                                                    uint32_t count) {
  if (count == 0) return;
  CRANE_DEBUG(
      "Malloc meta submit resource for job of user {} and account {}, "
      "count={}.",
      job.Username(), job.account, count);

  MetaResource meta_resource{.resource = ResourceView{},
                             .jobs_count = 0,
                             .submit_jobs_count = count,
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

  uint32_t submit_jobs_count = job.IsArrayChild() ? 0u : 1u;
  g_account_meta_container->UserAddJob(job.Username(), submit_jobs_count);

  MetaResource meta_resource{.resource = job.allocated_res_view,
                             .jobs_count = 1,
                             .submit_jobs_count = submit_jobs_count,
                             .wall_time = job.time_limit};

  DoMallocResource_(job.JobId(), job.Username(), job.account_chain, job.qos,
                    job.partition_id, meta_resource);
}

std::expected<void, std::string>
AccountMetaContainer::CheckAndMallocMetaResource(const PdJobInScheduler& job) {
  CRANE_TRACE("Check meta resource for job {} of user {} and account {}.",
              job.job_id, job.username, job.account);

  auto user = g_account_manager->GetExistedUserInfo(job.username);
  if (!user) {
    CRANE_ERROR(
        "[job #{}]: User '{}' not found in AccountManager during run check.",
        job.job_id, job.username);
    return std::unexpected("InvalidUser");
  }

  auto account_map = g_account_manager->GetAllAccountInfo();
  if (!account_map) {
    CRANE_ERROR(
        "[job #{}]: No account found in AccountManager during run check.",
        job.job_id);
    return std::unexpected("InvalidAccount");
  }

  auto qos = g_account_manager->GetExistedQosInfo(job.qos);
  if (!qos) return std::unexpected("InvalidQOS");

  // lock
  std::scoped_lock user_lock(m_user_stripes_[StripeForKey_(job.username)]);
  auto account_locks = LockAccountStripes_(job.account_chain);
  std::scoped_lock qos_lock(m_qos_stripes_[StripeForKey_(job.qos)]);

  auto result = CheckRunLimits_(job, *user, *account_map, *qos);
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

void AccountMetaContainer::FreeMetaSubmitResource(const JobInCtld& job,
                                                  uint32_t count) {
  if (count == 0) return;
  CRANE_DEBUG(
      "Free meta submit resource for job {} of user {} and account {}, "
      "count={}.",
      job.JobId(), job.Username(), job.account, count);

  MetaResource meta_resource{.resource = ResourceView{},
                             .jobs_count = 0,
                             .submit_jobs_count = count,
                             .wall_time = absl::ZeroDuration()};

  DoFreeResource_(job.JobId(), job.Username(), job.account_chain, job.qos,
                  job.partition_id, meta_resource);
}

void AccountMetaContainer::FreeMetaResource(const JobInCtld& job) {
  CRANE_DEBUG("Free meta resource for job {} of user {} and account {}.",
              job.JobId(), job.Username(), job.account);

  MetaResource meta_resource{.resource = job.allocated_res_view,
                             .jobs_count = 1,
                             .submit_jobs_count = job.IsArrayChild() ? 0u : 1u,
                             .wall_time = job.time_limit};

  DoFreeResource_(job.JobId(), job.Username(), job.account_chain, job.qos,
                  job.partition_id, meta_resource);
}

void AccountMetaContainer::FreeMetaResource(const PdJobInScheduler& job) {
  CRANE_DEBUG("Free meta resource for job {} of user {} and account {}.",
              job.job_id, job.username, job.account);

  MetaResource meta_resource{.resource = job.allocated_res.View(),
                             .jobs_count = 1,
                             .submit_jobs_count = 0,
                             .wall_time = job.time_limit};

  DoFreeResource_(job.job_id, job.username, job.account_chain, job.qos,
                  job.partition_id, meta_resource);
}

void AccountMetaContainer::UserAddJob(const std::string& username,
                                      uint32_t count) {
  if (count == 0) return;
  m_user_to_job_map_.try_emplace_l(
      username,
      [&](std::pair<const std::string, uint32_t>& pair) {
        pair.second += count;
      },
      count);
}

void AccountMetaContainer::UserReduceJob(const std::string& username,
                                         uint32_t count) {
  if (count == 0) return;
  bool is_contains = false;

  m_user_to_job_map_.if_contains(
      username, [&](std::pair<const std::string, uint32_t>& pair) {
        is_contains = true;
        if (pair.second < count) {
          CRANE_ERROR(
              "job_num ({}) < count ({}) when reduce user {} job; clamping.",
              pair.second, count, username);
          pair.second = 0;
          return;
        }
        pair.second -= count;
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

  if (!CheckGres_(resource_req.GetGresMap(), resource_total.GetGresMap()))
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
    bool is_user, const ResourceView& req_res, uint32_t count) const {
  auto it = stat.qos_to_resource_map.find(qos_name);
  const MetaResource empty{};
  const MetaResource& val =
      it == stat.qos_to_resource_map.end() ? empty : it->second;

  const uint32_t max_submit =
      is_user ? qos.max_submit_jobs_per_user : qos.max_submit_jobs_per_account;
  if (static_cast<uint64_t>(val.submit_jobs_count) + count > max_submit) {
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
    const MetaResourceStat& stat, const std::string& account,
    const std::string& partition_id,
    const PartitionResourceLimit* partition_limit,
    const ResourceView& /*req_res*/, absl::Duration /*time_limit*/,
    const Qos& qos, bool is_user, uint32_t count) const {
  if (!partition_limit) return CraneErrCode::SUCCESS;

  if (is_user) {
    if (qos.max_submit_jobs_per_user !=
        std::numeric_limits<decltype(qos.max_submit_jobs_per_user)>::max()) {
      CRANE_TRACE(
          "Skip user partition submit limit for account '{}', partition '{}' "
          "because QoS max_submit_jobs_per_user is set to {}.",
          account, partition_id, qos.max_submit_jobs_per_user);
      return CraneErrCode::SUCCESS;
    }

    auto account_it = stat.account_to_partition_to_resource_map.find(account);
    if (account_it != stat.account_to_partition_to_resource_map.end()) {
      auto pit = account_it->second.find(partition_id);
      if (pit != account_it->second.end()) {
        if (pit->second.submit_jobs_count + count >
            partition_limit->max_submit_jobs) {
          CRANE_TRACE(
              "User-account-partition submit limit exceeded: account='{}', "
              "partition='{}', current_submit_jobs={}, request_count={}, "
              "limit={}.",
              account, partition_id, pit->second.submit_jobs_count, count,
              partition_limit->max_submit_jobs);
          return CraneErrCode::ERR_PARTITION_MAX_SUBMIT_JOBS_PER_USER;
        }
      } else {
        CRANE_TRACE(
            "No user partition submit meta entry for account '{}', partition "
            "'{}'.",
            account, partition_id);
      }
    } else {
      CRANE_TRACE("No user account partition meta entry for account '{}'.",
                  account);
    }
  } else {
    if (qos.max_submit_jobs_per_account !=
        std::numeric_limits<decltype(qos.max_submit_jobs_per_account)>::max()) {
      CRANE_TRACE(
          "Skip account partition submit limit for account '{}', partition "
          "'{}' "
          "because QoS max_submit_jobs_per_account is set to {}.",
          account, partition_id, qos.max_submit_jobs_per_account);
      return CraneErrCode::SUCCESS;
    }

    auto pit = stat.partition_to_resource_map.find(partition_id);
    if (pit != stat.partition_to_resource_map.end()) {
      if (pit->second.submit_jobs_count + count >
          partition_limit->max_submit_jobs) {
        CRANE_TRACE(
            "Account partition submit limit exceeded: account='{}', "
            "partition='{}', current_submit_jobs={}, request_count={}, "
            "limit={}.",
            account, partition_id, pit->second.submit_jobs_count, count,
            partition_limit->max_submit_jobs);
        return CraneErrCode::ERR_PARTITION_MAX_SUBMIT_JOBS_PER_ACCOUNT;
      }
    } else {
      CRANE_TRACE(
          "No account partition submit meta entry for account '{}', "
          "partition '{}'.",
          account, partition_id);
    }
  }

  return CraneErrCode::SUCCESS;
}

CraneErrCode AccountMetaContainer::CheckEntitySubmitLimits_(
    const MetaResourceStat& stat, const std::string& account,
    const std::string& qos_name, const Qos& qos,
    const std::string& partition_id,
    const PartitionResourceLimit* partition_limit, bool is_user,
    const ResourceView& req_res, absl::Duration time_limit,
    uint32_t count) const {
  // QoS dimension (peer-level with Partition).
  CraneErrCode result = CheckQosSubmitLimitsForEntity_(stat, qos_name, qos,
                                                       is_user, req_res, count);
  if (result != CraneErrCode::SUCCESS) return result;

  // Partition dimension (peer-level with QoS).
  return CheckPartitionSubmitLimitsForEntity_(stat, account, partition_id,
                                              partition_limit, req_res,
                                              time_limit, qos, is_user, count);
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
    const MetaResourceStat& stat, const std::string& account,
    const std::string& partition_id,
    const PartitionResourceLimit* partition_limit,
    const ResourceView& allocated_res, absl::Duration time_limit,
    const Qos& qos, bool is_user) const {
  if (!partition_limit) return {};

  if (is_user) {
    auto account_it = stat.account_to_partition_to_resource_map.find(account);
    if (account_it == stat.account_to_partition_to_resource_map.end()) {
      CRANE_ERROR(
          "User-account-partition run meta account entry missing: "
          "account='{}', "
          "partition='{}'.",
          account, partition_id);
      return std::unexpected("PartitionEntryNotFound");
    }

    auto pit = account_it->second.find(partition_id);
    if (pit == account_it->second.end()) {
      CRANE_ERROR(
          "User-account({})-partition run meta partition entry missing: "
          " partition='{}'.",
          account, partition_id);
      return std::unexpected("PartitionEntryNotFound");
    }

    const MetaResource& val = pit->second;

    // max_jobs: only enforced when QoS does not already cap running jobs.
    if (qos.max_jobs_per_user == std::numeric_limits<uint32_t>::max()) {
      if (val.jobs_count + 1 > partition_limit->max_jobs) {
        CRANE_TRACE(
            "User-account-partition running job limit exceeded: account='{}', "
            "partition='{}', current_jobs={}, request_jobs=1, limit={}.",
            account, partition_id, val.jobs_count, partition_limit->max_jobs);
        return std::unexpected("UserPartitionJobsLimit");
      }
    }

    // max_wall: only enforced when QoS does not already cap wall time.
    if (qos.max_wall == absl::ZeroDuration() &&
        partition_limit->max_wall > absl::ZeroDuration()) {
      if (val.wall_time + time_limit > partition_limit->max_wall) {
        CRANE_TRACE(
            "User-account-partition wall limit exceeded: account='{}', "
            "partition='{}', current_wall={}s, request_wall={}s, limit={}s.",
            account, partition_id, absl::ToInt64Seconds(val.wall_time),
            absl::ToInt64Seconds(time_limit),
            absl::ToInt64Seconds(partition_limit->max_wall));
        return std::unexpected("UserPartitionWallTimeLimit");
      }
    }

    if (IsUnlimitedTres_(qos.max_tres_per_user)) {
      ResourceView resource_use{allocated_res};
      resource_use += val.resource;
      auto tres_result =
          CheckTres_(resource_use, partition_limit->max_tres, "Partition");
      if (!tres_result) {
        CRANE_TRACE(
            "User-account-partition TRES limit exceeded: account='{}', "
            "partition='{}', reason='{}', requested_plus_current='{}', "
            "limit='{}'.",
            account, partition_id, tres_result.error(),
            util::ReadableResourceView(resource_use),
            util::ReadableResourceView(partition_limit->max_tres));
        return tres_result;
      }
    }
  } else {
    auto pit = stat.partition_to_resource_map.find(partition_id);
    if (pit == stat.partition_to_resource_map.end()) {
      CRANE_ERROR(
          "Account({}) partition run meta entry missing: "
          "partition='{}'.",
          account, partition_id);
      return std::unexpected("PartitionEntryNotFound");
    }

    const MetaResource& val = pit->second;

    // max_jobs: only enforced when QoS does not already cap running jobs.
    if (qos.max_jobs_per_account == std::numeric_limits<uint32_t>::max()) {
      if (val.jobs_count + 1 > partition_limit->max_jobs) {
        CRANE_TRACE(
            "Account partition running job limit exceeded: account='{}', "
            "partition='{}', current_jobs={}, request_jobs=1, limit={}.",
            account, partition_id, val.jobs_count, partition_limit->max_jobs);
        return std::unexpected("AccPartitionJobsLimit");
      }
    }

    // max_wall: only enforced when QoS does not already cap wall time.
    if (qos.max_wall == absl::ZeroDuration() &&
        partition_limit->max_wall > absl::ZeroDuration()) {
      if (val.wall_time + time_limit > partition_limit->max_wall) {
        CRANE_TRACE(
            "Account partition wall limit exceeded: account='{}', "
            "partition='{}', current_wall={}s, request_wall={}s, limit={}s.",
            account, partition_id, absl::ToInt64Seconds(val.wall_time),
            absl::ToInt64Seconds(time_limit),
            absl::ToInt64Seconds(partition_limit->max_wall));
        return std::unexpected("AccPartitionWallTimeLimit");
      }
    }

    if (IsUnlimitedTres_(qos.max_tres_per_account)) {
      ResourceView resource_use{allocated_res};
      resource_use += val.resource;
      auto tres_result =
          CheckTres_(resource_use, partition_limit->max_tres, "Partition");
      if (!tres_result) {
        CRANE_TRACE(
            "Account partition TRES limit exceeded: account='{}', "
            "partition='{}', reason='{}', requested_plus_current='{}', "
            "limit='{}'.",
            account, partition_id, tres_result.error(),
            util::ReadableResourceView(resource_use),
            util::ReadableResourceView(partition_limit->max_tres));
        return tres_result;
      }
    }
  }

  return {};
}

std::expected<void, std::string> AccountMetaContainer::CheckEntityRunLimits_(
    const MetaResourceStat& stat, const std::string& account,
    const std::string& qos_name, const Qos& qos,
    const std::string& partition_id,
    const PartitionResourceLimit* partition_limit, bool is_user,
    const ResourceView& allocated_res, absl::Duration time_limit) const {
  // QoS dimension (peer-level with Partition).
  auto result = CheckQosRunLimitsForEntity_(stat, qos_name, qos, is_user,
                                            allocated_res, time_limit);
  if (!result) return result;

  // Partition dimension (peer-level with QoS).
  return CheckPartitionRunLimitsForEntity_(stat, account, partition_id,
                                           partition_limit, allocated_res,
                                           time_limit, qos, is_user);
}

/* ---------------------------------------------------------------------------
 *  Aggregated checks (User → AccountChain → GlobalQoS)
 * ---------------------------------------------------------------------------
 */

CraneErrCode AccountMetaContainer::CheckSubmitLimits_(
    const JobInCtld& job, const User& user, const AccountRawMap& account_map,
    const Qos& qos, uint32_t count) {
  CraneErrCode result = CraneErrCode::SUCCESS;

  // ---- User entity ----
  {
    const PartitionResourceLimit* user_part_limit = nullptr;
    auto acct_it = user.account_to_attrs_map.find(job.account);
    if (acct_it == user.account_to_attrs_map.end()) {
      CRANE_ERROR(
          "Account '{}' is not in user '{}' account list during "
          "submit check.",
          job.account, job.Username());
      return CraneErrCode::ERR_USER_ACCOUNT_MISMATCH;
    }
    auto part_it =
        acct_it->second.partition_to_limit_map.find(job.partition_id);
    if (part_it != acct_it->second.partition_to_limit_map.end())
      user_part_limit = &part_it->second;

    if (user_part_limit) {  // first check partition limits
      if (!CheckTres_(job.req_total_res_view,
                      user_part_limit->max_tres_per_job)) {
        CRANE_TRACE(
            "[job #{}]: user partition TRES-per-job limit exceeded during "
            "submit check: user='{}', account='{}', partition='{}', "
            "request='{}', limit='{}'.",
            job.JobId(), job.Username(), job.account, job.partition_id,
            util::ReadableResourceView(job.req_total_res_view),
            util::ReadableResourceView(user_part_limit->max_tres_per_job));
        return CraneErrCode::ERR_PARTITION_TRES_PER_JOB_BEYOND;
      }
      if (qos.max_time_limit_per_job == absl::Seconds(kJobMaxTimeLimitSec)) {
        if (job.time_limit > user_part_limit->max_wall_duration_per_job) {
          CRANE_TRACE(
              "[job #{}]: user partition wall-per-job limit exceeded during "
              "submit check: user='{}', account='{}', partition='{}', "
              "request_wall={}s, limit={}s.",
              job.JobId(), job.Username(), job.account, job.partition_id,
              absl::ToInt64Seconds(job.time_limit),
              absl::ToInt64Seconds(user_part_limit->max_wall_duration_per_job));
          return CraneErrCode::ERR_PARTITION_TIME_BEYOND;
        }
      }
      if (qos.max_submit_jobs_per_user ==
          std::numeric_limits<decltype(qos.max_submit_jobs_per_user)>::max()) {
        if (user_part_limit->max_submit_jobs == count - 1) {
          CRANE_TRACE(
              "[job #{}]: user partition max_submit_jobs is zero during "
              "submit check: user='{}', account='{}', partition='{}'.",
              job.JobId(), job.Username(), job.account, job.partition_id);
          return CraneErrCode::ERR_PARTITION_MAX_SUBMIT_JOBS_PER_USER;
        }
      }
    }

    m_user_meta_map_.if_contains(
        job.Username(),
        [&](std::pair<const std::string, MetaResourceStat>& pair) {
          result = CheckEntitySubmitLimits_(
              pair.second, job.account, job.qos, qos, job.partition_id,
              user_part_limit, /*is_user=*/true, job.req_total_res_view,
              job.time_limit, count);
        });
    if (result != CraneErrCode::SUCCESS) {
      CRANE_TRACE(
          "[job #{}]: user submit limit check failed: user='{}', account='{}', "
          "partition='{}', qos='{}', count={}, error='{}'.",
          job.JobId(), job.Username(), job.account, job.partition_id, job.qos,
          count, CraneErrStr(result));
      return result;
    }
  }

  // ---- Account chain ----
  for (const auto& account_name : job.account_chain) {
    const PartitionResourceLimit* acct_part_limit = nullptr;
    auto acct_it = account_map.find(account_name);
    if (acct_it == account_map.end() || !acct_it->second) {
      CRANE_ERROR(
          "Account '{}' not found in AccountManager during submit check.",
          account_name);
      return CraneErrCode::ERR_INVALID_ACCOUNT;
    }
    auto part_it =
        acct_it->second->partition_to_limit_map.find(job.partition_id);
    if (part_it != acct_it->second->partition_to_limit_map.end())
      acct_part_limit = &part_it->second;

    if (acct_part_limit) {  // first check partition limits
      if (!CheckTres_(job.req_total_res_view,
                      acct_part_limit->max_tres_per_job)) {
        CRANE_TRACE(
            "[job #{}]: account partition TRES-per-job limit exceeded during "
            "submit check: account='{}', partition='{}', request='{}', "
            "limit='{}'.",
            job.JobId(), account_name, job.partition_id,
            util::ReadableResourceView(job.req_total_res_view),
            util::ReadableResourceView(acct_part_limit->max_tres_per_job));
        return CraneErrCode::ERR_PARTITION_TRES_PER_JOB_BEYOND;
      }
      if (qos.max_time_limit_per_job == absl::Seconds(kJobMaxTimeLimitSec)) {
        if (job.time_limit > acct_part_limit->max_wall_duration_per_job) {
          CRANE_TRACE(
              "[job #{}]: account partition wall-per-job limit exceeded during "
              "submit check: account='{}', partition='{}', request_wall={}s, "
              "limit={}s.",
              job.JobId(), account_name, job.partition_id,
              absl::ToInt64Seconds(job.time_limit),
              absl::ToInt64Seconds(acct_part_limit->max_wall_duration_per_job));
          return CraneErrCode::ERR_PARTITION_TIME_BEYOND;
        }
      }
      if (qos.max_submit_jobs_per_account ==
          std::numeric_limits<
              decltype(qos.max_submit_jobs_per_account)>::max()) {
        if (acct_part_limit->max_submit_jobs == count - 1) {
          CRANE_TRACE(
              "[job #{}]: account partition max_submit_jobs is zero during "
              "submit check: account='{}', partition='{}'.",
              job.JobId(), account_name, job.partition_id);
          return CraneErrCode::ERR_PARTITION_MAX_SUBMIT_JOBS_PER_ACCOUNT;
        }
      }
    }

    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, MetaResourceStat>& pair) {
          result = CheckEntitySubmitLimits_(
              pair.second, job.account, job.qos, qos, job.partition_id,
              acct_part_limit, /*is_user=*/false, job.req_total_res_view,
              job.time_limit, count);
        });
    if (result != CraneErrCode::SUCCESS) {
      CRANE_TRACE(
          "[job #{}]: account submit limit check failed: user='{}', "
          "job_account='{}', checked_account='{}', partition='{}', qos='{}', "
          "count={}, error='{}'.",
          job.JobId(), job.Username(), job.account, account_name,
          job.partition_id, job.qos, count, CraneErrStr(result));
      return result;
    }
  }

  // ---- Global QoS counters ----
  m_qos_meta_map_.if_contains(
      job.qos, [&](std::pair<const std::string, MetaResource>& pair) {
        const MetaResource& val = pair.second;
        if (val.submit_jobs_count + count > qos.max_submit_jobs) {
          CRANE_TRACE(
              "[job #{}]: global QoS submit limit exceeded: qos='{}', "
              "current_submit_jobs={}, request_count={}, limit={}.",
              job.JobId(), job.qos, val.submit_jobs_count, count,
              qos.max_submit_jobs);
          result = CraneErrCode::ERR_QOS_JOB_COUNT_EXCEEDED;
          return;
        }
        if (qos.flags[QosFlags::DenyOnLimit]) {
          if (val.jobs_count + 1 > qos.max_jobs) {
            CRANE_TRACE(
                "[job #{}]: global QoS running job limit exceeded during "
                "submit check: qos='{}', current_jobs={}, request_jobs=1, "
                "limit={}.",
                job.JobId(), job.qos, val.jobs_count, qos.max_jobs);
            result = CraneErrCode::ERR_QOS_JOB_COUNT_EXCEEDED;
            return;
          }
          if (qos.max_wall > absl::ZeroDuration() &&
              val.wall_time + job.time_limit > qos.max_wall) {
            CRANE_TRACE(
                "[job #{}]: global QoS wall limit exceeded during submit "
                "check: qos='{}', current_wall={}s, request_wall={}s, "
                "limit={}s.",
                job.JobId(), job.qos, absl::ToInt64Seconds(val.wall_time),
                absl::ToInt64Seconds(job.time_limit),
                absl::ToInt64Seconds(qos.max_wall));
            result = CraneErrCode::ERR_TIME_TIMIT_BEYOND;
            return;
          }
          ResourceView resource_use{job.req_total_res_view};
          resource_use += val.resource;
          if (!CheckTres_(resource_use, qos.max_tres)) {
            CRANE_TRACE(
                "[job #{}]: global QoS TRES limit exceeded during submit "
                "check: qos='{}', requested_plus_current='{}', limit='{}'.",
                job.JobId(), job.qos, util::ReadableResourceView(resource_use),
                util::ReadableResourceView(qos.max_tres));
            result = CraneErrCode::ERR_TRES_PER_JOB_BEYOND;
          }
        }
      });

  return result;
}

std::expected<void, std::string> AccountMetaContainer::CheckRunLimits_(
    const PdJobInScheduler& job, const User& user,
    const AccountRawMap& account_map, const Qos& qos) {
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
    auto acct_it = user.account_to_attrs_map.find(job.account);
    if (acct_it == user.account_to_attrs_map.end()) {
      CRANE_ERROR(
          "[job #{}]: Account '{}' is not in user '{}' account list during run "
          "check.",
          job.job_id, job.account, job.username);
      return std::unexpected("UserAccountMismatch");
    }
    auto part_it =
        acct_it->second.partition_to_limit_map.find(job.partition_id);
    if (part_it != acct_it->second.partition_to_limit_map.end())
      user_part_limit = &part_it->second;

    m_user_meta_map_.if_contains(
        job.username,
        [&](std::pair<const std::string, MetaResourceStat>& pair) {
          result = CheckEntityRunLimits_(
              pair.second, job.account, job.qos, qos, job.partition_id,
              user_part_limit, /*is_user=*/true, allocated_res, job.time_limit);
        });
    if (!result) {
      CRANE_TRACE(
          "[job #{}]: user run limit check failed: user='{}', account='{}', "
          "partition='{}', qos='{}', allocated='{}', time_limit={}s, "
          "reason='{}'.",
          job.job_id, job.username, job.account, job.partition_id, job.qos,
          util::ReadableResourceView(allocated_res),
          absl::ToInt64Seconds(job.time_limit), result.error());
      return result;
    }
  }

  // ---- Account chain ----
  for (const auto& account_name : job.account_chain) {
    const PartitionResourceLimit* acct_part_limit = nullptr;
    auto acct_it = account_map.find(account_name);
    if (acct_it == account_map.end() || !acct_it->second) {
      CRANE_ERROR(
          "[job #{}]: Account '{}' not found in AccountManager during "
          "run check.",
          job.job_id, account_name);
      return std::unexpected("InvalidAccount");
    }
    auto part_it =
        acct_it->second->partition_to_limit_map.find(job.partition_id);
    if (part_it != acct_it->second->partition_to_limit_map.end())
      acct_part_limit = &part_it->second;

    m_account_meta_map_.if_contains(
        account_name,
        [&](std::pair<const std::string, MetaResourceStat>& pair) {
          result = CheckEntityRunLimits_(pair.second, job.account, job.qos, qos,
                                         job.partition_id, acct_part_limit,
                                         /*is_user=*/false, allocated_res,
                                         job.time_limit);
        });
    if (!result) {
      CRANE_TRACE(
          "[job #{}]: account run limit check failed: user='{}', "
          "job_account='{}', checked_account='{}', partition='{}', qos='{}', "
          "allocated='{}', time_limit={}s, reason='{}'.",
          job.job_id, job.username, job.account, account_name, job.partition_id,
          job.qos, util::ReadableResourceView(allocated_res),
          absl::ToInt64Seconds(job.time_limit), result.error());
      return result;
    }
  }

  // ---- Global QoS counters ----
  m_qos_meta_map_.if_contains(
      job.qos, [&](std::pair<const std::string, MetaResource>& pair) {
        const MetaResource& val = pair.second;
        ResourceView resource_use{allocated_res};
        resource_use += val.resource;

        if (val.jobs_count + 1 > qos.max_jobs) {
          CRANE_TRACE(
              "[job #{}]: global QoS running job limit exceeded during run "
              "check: qos='{}', current_jobs={}, request_jobs=1, limit={}.",
              job.job_id, job.qos, val.jobs_count, qos.max_jobs);
          result = std::unexpected("QosJobsResourceLimit");
          return;
        }
        if (qos.max_wall > absl::ZeroDuration() &&
            val.wall_time + job.time_limit > qos.max_wall) {
          CRANE_TRACE(
              "[job #{}]: global QoS wall limit exceeded during run check: "
              "qos='{}', current_wall={}s, request_wall={}s, limit={}s.",
              job.job_id, job.qos, absl::ToInt64Seconds(val.wall_time),
              absl::ToInt64Seconds(job.time_limit),
              absl::ToInt64Seconds(qos.max_wall));
          result = std::unexpected("QosWallTimeLimit");
          return;
        }
        result = CheckTres_(resource_use, qos.max_tres);
        if (!result) {
          CRANE_TRACE(
              "[job #{}]: global QoS TRES limit exceeded during run check: "
              "qos='{}', reason='{}', requested_plus_current='{}', "
              "limit='{}'.",
              job.job_id, job.qos, result.error(),
              util::ReadableResourceView(resource_use),
              util::ReadableResourceView(qos.max_tres));
        }
      });

  return result;
}

bool AccountMetaContainer::CheckGres_(const GresMap& device_req,
                                      const GresMap& device_total) {
  for (const auto& [name, lhs] : device_req) {
    auto rhs_it = device_total.find(name);
    if (rhs_it == device_total.end()) return true;

    const auto& rhs = rhs_it->second;

    // Check total
    if (lhs.total > rhs.total) return false;

    // Check each specified type
    for (const auto& [type, lhs_cnt] : lhs.specified) {
      auto rhs_it = rhs.specified.find(type);
      if (rhs_it == rhs.specified.end()) return true;
      if (lhs_cnt > rhs_it->second) return false;
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
    const std::string& partition_id, const MetaResource& meta_resource) {
  CRANE_TRACE(
      "Malloc meta resource: job_id={}, user='{}', account='{}', qos='{}', "
      "partition='{}', delta={}.",
      job_id, username, account_chain.front(), qos, partition_id,
      meta_resource.DebugString());

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
        auto account_it = pair.second.account_to_partition_to_resource_map.find(
            account_chain.front());
        if (account_it ==
            pair.second.account_to_partition_to_resource_map.end()) {
          pair.second.account_to_partition_to_resource_map.emplace(
              account_chain.front(),
              PartitionToResourceMap{{partition_id, meta_resource}});
        } else {
          upsert(account_it->second, partition_id);
        }
      },
      MetaResourceStat{
          .qos_to_resource_map = {{qos, meta_resource}},
          .account_to_partition_to_resource_map = {
              {account_chain.front(), {{partition_id, meta_resource}}}}});

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
  CRANE_TRACE(
      "Free meta resource: job_id={}, user='{}', account='{}', qos='{}', "
      "partition='{}', delta={}.",
      job_id, username, account_chain.front(), qos, partition_id,
      meta_resource.DebugString());

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

        auto account_it = pair.second.account_to_partition_to_resource_map.find(
            account_chain.front());
        if (account_it !=
            pair.second.account_to_partition_to_resource_map.end()) {
          safe_sub(account_it->second, partition_id, "user", username);
        } else {
          CRANE_ERROR(
              "Account '{}' not found in user '{}' account list during free "
              "resource for job {}.",
              account_chain.front(), username, job_id);
        }
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

  // UserReduceJob mirrors the submit_jobs_count freed here so that
  // UserHasJob bookkeeping matches bulk allocations (e.g. array parent
  // reserving N slots at submit time).
  UserReduceJob(username, meta_resource.submit_jobs_count);
}

}  // namespace Ctld

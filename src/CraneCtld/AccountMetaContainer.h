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

#pragma once

#include "CtldPublicDefs.h"
// Precompiled header comes first!

namespace Ctld {

struct PdJobInScheduler;

struct MetaResource {
  ResourceView resource{};
  uint32_t jobs_count{0};
  uint32_t submit_jobs_count{0};
  absl::Duration wall_time{absl::ZeroDuration()};

  bool operator<=(const MetaResource& rhs) const;

  MetaResource& operator+=(const MetaResource& rhs);
  MetaResource& operator-=(const MetaResource& rhs);

  bool IsZero() const {
    return resource.IsZero() && jobs_count == 0 && submit_jobs_count == 0 &&
           wall_time == absl::ZeroDuration();
  }

  void SetToZero();

  std::string DebugString() const;
};

using QosToResourceMap = std::unordered_map<std::string,  // qos_name
                                            MetaResource>;

using PartitionToResourceMap =
    std::unordered_map<std::string,  // partition_name
                       MetaResource>;

// Tracks per-user and per-account resource usage across both QoS and partition
// dimensions.
struct MetaResourceStat {
  QosToResourceMap qos_to_resource_map;
  PartitionToResourceMap partition_to_resource_map;
};

class AccountMetaContainer final {
 public:
  AccountMetaContainer() = default;
  ~AccountMetaContainer() = default;

  /*
   * Validate QoS and partition submit-slot limits and reserve resources.
   * IN/OUT: job - job to be submitted; qos_priority field is set on success
   * RET: CraneErrCode::SUCCESS on success, error code on limit exceeded
   *
   * NOTE: Acquires internal stripe locks. Do not hold AccountManager locks.
   */
  CraneErrCode TryMallocMetaSubmitResource(JobInCtld& job);

  /*
   * Unconditionally reserve submit-slot resources without validation.
   * IN: job - job whose submit-slot resources should be reserved
   * RET: none
   *
   * NOTE: Call only after TryMallocMetaSubmitResource succeeds.
   */
  void MallocMetaSubmitResource(const JobInCtld& job);

  /*
   * Reserve running resources for a job recovered from the embedded DB.
   * IN/OUT: job - recovered running job; qos_priority is set on success
   * RET: none
   */
  void MallocMetaResourceToRecoveredRunningJob(JobInCtld& job);

  /*
   * Validate running-slot limits and atomically reserve resources.
   * IN: job - pending job to be scheduled
   * RET: void on success, string describing the pending reason on failure
   *
   * NOTE: Acquires internal stripe locks. Do not hold AccountManager locks.
   */
  std::expected<void, std::string> CheckAndMallocMetaResource(
      const PdJobInScheduler& job);

  /*
   * Release submit-slot resources when a pending job is cancelled or rejected.
   * IN: job - job whose submit-slot resources should be released
   * RET: none
   */
  void FreeMetaSubmitResource(const JobInCtld& job);

  /*
   * Release running-slot resources when a running job finishes.
   * IN: job - job whose running-slot resources should be released
   * RET: none
   */
  void FreeMetaResource(const JobInCtld& job);

  /*
   * Remove all cached meta entries for a deleted user/account/qos object.
   * IN: username/account/qos - name of the deleted entity
   * RET: none
   */
  void DeleteUserMeta(const std::string& username);
  void DeleteAccountMeta(const std::string& account);
  void DeleteQosMeta(const std::string& qos);

  /* Increment the running job counter for a user */
  void UserAddJob(const std::string& username);
  /* Decrement the running job counter for a user */
  void UserReduceJob(const std::string& username);
  /*
   * Check whether a user has any jobs.
   * IN:  username - name of the user to check
   * RET: true if the user has at least one job, false otherwise
   */
  bool UserHasJob(const std::string& username);

 private:
  const static int kNumStripes = 128;

  static int StripeForKey_(const std::string& key) {
    return std::hash<std::string>{}(key) % kNumStripes;
  }

  // Checks that resource_req does not exceed resource_total in any dimension.
  // prefix is used to distinguish QoS vs Partition in the error message.
  static std::expected<void, std::string> CheckTres_(
      const ResourceView& resource_req, const ResourceView& resource_total,
      const std::string& prefix = "Qos");

  static bool IsUnlimitedTres_(const ResourceView& res);

  // Submit-time QoS dimension check for a single entity.
  CraneErrCode CheckQosSubmitLimitsForEntity_(
      const MetaResourceStat& stat, const std::string& qos_name, const Qos& qos,
      bool is_user, const ResourceView& req_res) const;

  // Submit-time Partition dimension check for a single entity.
  // Returns SUCCESS when partition_limit is nullptr (no limit configured).
  CraneErrCode CheckPartitionSubmitLimitsForEntity_(
      const MetaResourceStat& stat, const std::string& partition_id,
      const PartitionResourceLimit* partition_limit,
      const ResourceView& req_res, absl::Duration time_limit,
      bool is_user) const;

  // Submit-time combined check for a single entity (QoS + Partition).
  CraneErrCode CheckEntitySubmitLimits_(
      const MetaResourceStat& stat, const std::string& qos_name, const Qos& qos,
      const std::string& partition_id,
      const PartitionResourceLimit* partition_limit, bool is_user,
      const ResourceView& req_res, absl::Duration time_limit) const;

  // Schedule-time QoS dimension check for a single entity.
  std::expected<void, std::string> CheckQosRunLimitsForEntity_(
      const MetaResourceStat& stat, const std::string& qos_name, const Qos& qos,
      bool is_user, const ResourceView& allocated_res,
      absl::Duration time_limit) const;

  std::expected<void, std::string> CheckPartitionRunLimitsForEntity_(
      const MetaResourceStat& stat, const std::string& partition_id,
      const PartitionResourceLimit* partition_limit,
      const ResourceView& allocated_res, absl::Duration time_limit,
      const Qos& qos, bool is_user) const;

  // Schedule-time combined check for a single entity (QoS + Partition).
  std::expected<void, std::string> CheckEntityRunLimits_(
      const MetaResourceStat& stat, const std::string& qos_name, const Qos& qos,
      const std::string& partition_id,
      const PartitionResourceLimit* partition_limit, bool is_user,
      const ResourceView& allocated_res, absl::Duration time_limit) const;

  // Submit-time aggregated check across all entities.
  // Replaces the old CheckMetaSubmitResourceUsage_.
  CraneErrCode CheckSubmitLimits_(const JobInCtld& job, const Qos& qos);

  // Schedule-time aggregated check across all entities.
  // Replaces the old CheckMetaResource_.
  std::expected<void, std::string> CheckRunLimits_(const PdJobInScheduler& job,
                                                   const Qos& qos);

  // Atomically increments both QoS and partition counters for user, every
  // account in the chain, and the global QoS map.
  void DoMallocResource_(job_id_t job_id, const std::string& username,
                         const std::list<std::string>& account_chain,
                         const std::string& qos,
                         const std::string& partition_id,
                         const MetaResource& meta_resource);

  // Atomically decrements both QoS and partition counters (inverse of
  // DoMallocResource_).
  void DoFreeResource_(job_id_t job_id, const std::string& username,
                       const std::list<std::string>& account_chain,
                       const std::string& qos, const std::string& partition_id,
                       const MetaResource& meta_resource);

  // Lock acquisition order:
  // Always acquire locks in the following order to avoid deadlocks:
  // 1. Lock user first.
  // 2. Then lock account(s).
  // 3. lock qos last.
  // For both users and accounts, acquire locks in ascending order by their IDs
  // (from smallest to largest).
  std::array<std::mutex, kNumStripes> m_user_stripes_;
  std::array<std::mutex, kNumStripes> m_account_stripes_;
  std::array<std::mutex, kNumStripes> m_qos_stripes_;
  std::vector<std::unique_lock<std::mutex>> LockAccountStripes_(
      const std::list<std::string>& account_chain);

  using ResourceMetaMap = phmap::parallel_flat_hash_map<
      std::string, MetaResourceStat,
      phmap::priv::hash_default_hash<std::string>,
      phmap::priv::hash_default_eq<std::string>,
      std::allocator<std::pair<const std::string, MetaResourceStat>>, 4,
      std::shared_mutex>;

  using UserToJobNumMap = phmap::parallel_flat_hash_map<
      std::string, uint32_t, phmap::priv::hash_default_hash<std::string>,
      phmap::priv::hash_default_eq<std::string>,
      std::allocator<std::pair<const std::string, uint32_t>>, 4,
      std::shared_mutex>;

  using QosResourceMap = phmap::parallel_flat_hash_map<
      std::string, MetaResource, phmap::priv::hash_default_hash<std::string>,
      phmap::priv::hash_default_eq<std::string>,
      std::allocator<std::pair<const std::string, MetaResource>>, 4,
      std::shared_mutex>;

  ResourceMetaMap m_user_meta_map_;
  ResourceMetaMap m_account_meta_map_;
  QosResourceMap m_qos_meta_map_;
  UserToJobNumMap m_user_to_job_map_;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::AccountMetaContainer> g_account_meta_container;

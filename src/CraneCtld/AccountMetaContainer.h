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

class AccountMetaContainer final {
 public:
  using QosToResourceMap = std::unordered_map<std::string,  // qos_name
                                              MetaResource>;

  using ResourceMetaMap = phmap::parallel_flat_hash_map<
      std::string, QosToResourceMap,
      phmap::priv::hash_default_hash<std::string>,
      phmap::priv::hash_default_eq<std::string>,
      std::allocator<std::pair<const std::string, QosToResourceMap>>, 4,
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

  AccountMetaContainer() = default;
  ~AccountMetaContainer() = default;

  CraneErrCode TryMallocQosSubmitResource(JobInCtld& job);

  void MallocQosSubmitResource(const JobInCtld& job);

  void MallocQosResourceToRecoveredRunningJob(JobInCtld& job);

  std::expected<void, std::string> CheckAndMallocQosResource(
      const PdJobInScheduler& job);

  void FreeQosSubmitResource(const JobInCtld& job);

  void FreeQosResource(const JobInCtld& job);

  // Free only running resource (not submit count) — used for requeue.
  void FreeQosRunningResource(const JobInCtld& job);

  // When a user/account object is deleted, resources need to be reset.
  void DeleteUserMeta(const std::string& username);

  void DeleteAccountMeta(const std::string& account);

  void DeleteQosMeta(const std::string& qos);

  void UserAddJob(const std::string& username);

  void UserReduceJob(const std::string& username);

  bool UserHasJob(const std::string& username);

 private:
  const static int kNumStripes = 128;

  static int StripeForKey_(const std::string& key) {
    return std::hash<std::string>{}(key) % kNumStripes;
  }

  CraneErrCode CheckUserQosSubmitResourceUsage_(const JobInCtld& job,
                                                const Qos& qos);

  CraneErrCode CheckAccountQosSubmitResourceUsage_(const JobInCtld& job,
                                                   const Qos& qos);

  CraneErrCode CheckQosSubmitResourceUsage_(const JobInCtld& job,
                                            const Qos& qos);

  std::expected<void, std::string> CheckQosResource_(
      const Qos& qos, const PdJobInScheduler& job);

  template <typename T>
  static void CheckAndSubResource_(T& current, const T& need,
                                   const std::string& resource_name,
                                   const std::string& username,
                                   const std::string& qos, job_id_t job_id) {
    if constexpr (std::is_same_v<T, ResourceView>) {
      if (!(need <= current)) {
        CRANE_ERROR(
            "Insufficient {} when freeing for user/account '{}', qos '{}', "
            "job {}.",
            resource_name, username, qos, job_id);
        current.SetToZero();
        return;
      }
    } else if constexpr (std::is_same_v<T, uint32_t>) {
      if (current < need) {
        CRANE_ERROR(
            "Insufficient {} when freeing for user/account '{}', qos '{}', "
            "job {}. cur={}, need={}",
            resource_name, username, qos, job_id, current, need);
        current = 0;
        return;
      }
    } else {
      if (current < need) {
        CRANE_ERROR("Unknown type: insufficient resource");
        return;
      }
    }
    current -= need;
  }

  static std::expected<void, std::string> CheckTres_(
      const ResourceView& resource_req, const ResourceView& resource_total);

  void DoMallocResource_(job_id_t job_id, const std::string& username,
                         const std::list<std::string>& account_chain,
                         const std::string& qos,
                         const MetaResource& meta_resource);
  void DoFreeResource_(job_id_t job_id, const std::string& username,
                       const std::list<std::string>& account_chain,
                       const std::string& qos,
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

  ResourceMetaMap m_user_meta_map_;
  ResourceMetaMap m_account_meta_map_;
  QosResourceMap m_qos_meta_map_;
  UserToJobNumMap m_user_to_job_map_;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::AccountMetaContainer> g_account_meta_container;
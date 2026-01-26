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

#include "AccountManager.h"
#include "TaskScheduler.h"

namespace Ctld {

constexpr int kNumStripes = 128;

class AccountMetaContainer final {
 public:
  using QosToResourceMap = std::unordered_map<std::string,  // qos_name
                                              QosResource>;

  using ResourceMetaMap = phmap::parallel_flat_hash_map<
      std::string, QosToResourceMap,
      phmap::priv::hash_default_hash<std::string>,
      phmap::priv::hash_default_eq<std::string>,
      std::allocator<std::pair<const std::string, QosToResourceMap>>, 4,
      std::shared_mutex>;

  using UserToTaskNumMap = phmap::parallel_flat_hash_map<
      std::string, uint32_t, phmap::priv::hash_default_hash<std::string>,
      phmap::priv::hash_default_eq<std::string>,
      std::allocator<std::pair<const std::string, uint32_t>>, 4,
      std::shared_mutex>;

  AccountMetaContainer() = default;
  ~AccountMetaContainer() = default;

  CraneErrCode TryMallocQosSubmitResource(TaskInCtld& task);

  void MallocQosSubmitResource(const TaskInCtld& task);

  void MallocQosResourceToRecoveredRunningTask(TaskInCtld& task);

  std::expected<void, std::string> CheckAndMallocQosResource(
      const PdJobInScheduler& job);

  void FreeQosSubmitResource(const TaskInCtld& task);

  void FreeQosResource(const TaskInCtld& task);

  // When a user/account object is deleted, resources need to be reset.
  void DeleteUserMeta(const std::string& username);

  void DeleteAccountMeta(const std::string& account);

  void UserAddTask(const std::string& username);

  void UserReduceTask(const std::string& username);

  bool UserHasTask(const std::string& username);

 private:
  static int StripeForKey_(const std::string& key) {
    return std::hash<std::string>{}(key) % kNumStripes;
  }

  CraneErrCode CheckQosSubmitResourceForUser_(const TaskInCtld& task,
                                              const Qos& qos);

  CraneErrCode CheckQosSubmitResourceForAccount_(const TaskInCtld& task,
                                                 const Qos& qos);

  bool CheckQosResource_(const Qos& qos, const PdJobInScheduler& job,
                         const ResourceView& resource_view);

  template<typename T>
  static void CheckAndSubResource_(T& current, T need, const std::string& resource_name,
                           const std::string& username, const std::string& qos, task_id_t task_id) {
      if (current < need) {
        if constexpr (std::is_same_v<T, AllocatableResource>) {
          CRANE_ERROR("Insufficient {} when freeing for user/account '{}', qos '{}', task {}.",
                     resource_name, username, qos, task_id);
          current.SetToZero();
        } else if constexpr (std::is_same_v<T, uint32_t>) {
          CRANE_ERROR("Insufficient {} when freeing for user/account '{}', qos '{}', task {}. cur={}, need={}",
                     resource_name, username, qos, task_id, current, need);
          current = 0;
        } else {
          CRANE_ERROR("Unknown type");
        }
        return;
      }

    current -= need;
  }

  // Lock acquisition order:
  // Always acquire locks in the following order to avoid deadlocks:
  // 1. Lock user(s) first.
  // 2. Then lock account(s).
  // For both users and accounts, acquire locks in ascending order by their IDs (from smallest to largest).
  std::array<std::mutex, kNumStripes> m_user_stripes_;
  std::array<std::mutex, kNumStripes> m_account_stripes_;
  std::vector<std::unique_lock<std::mutex>> LockAccountStripes_(
      const std::list<std::string>& account_chain);

  ResourceMetaMap m_user_meta_map_;
  ResourceMetaMap m_account_meta_map_;
  UserToTaskNumMap m_user_to_task_map_;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::AccountMetaContainer> g_account_meta_container;
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

  AccountMetaContainer() = default;
  ~AccountMetaContainer() = default;

  CraneErrCode TryMallocQosSubmitResource(TaskInCtld& task);

  void MallocQosResourceToRecoveredRunningTask(TaskInCtld& task);

  void MallocQosResourceToRecoveredPendingTask(TaskInCtld& task);

  std::optional<std::string> CheckQosResource(const TaskInCtld& task);

  void MallocQosResource(const TaskInCtld& task);

  void FreeQosSubmitResource(const TaskInCtld& task);

  void FreeQosResource(const TaskInCtld& task);

  // When a user/account object is deleted, resources need to be reset.
  void DeleteUserMeta(const std::string& username);

  void DeleteAccountMeta(const std::string& account);

 private:
  static int StripeForKey_(const std::string& key) {
    return std::hash<std::string>{}(key) % kNumStripes;
  }

  CraneErrCode CheckQosSubmitResourceForUser_(const TaskInCtld& task,
                                              const Qos& qos);

  CraneErrCode CheckQosSubmitResourceForAccount_(const TaskInCtld& task,
                                                 const Qos& qos);

  std::array<std::mutex, kNumStripes> m_user_stripes_;

  std::array<std::mutex, kNumStripes> m_account_stripes_;

  ResourceMetaMap m_user_meta_map_;

  ResourceMetaMap m_account_meta_map_;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::AccountMetaContainer> g_account_meta_container;
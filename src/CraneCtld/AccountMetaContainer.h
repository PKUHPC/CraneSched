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
#include "crane/PublicHeader.h"
#include "parallel_hashmap/phmap.h"
#include "parallel_hashmap/phmap_fwd_decl.h"

namespace Ctld {

class AccountMetaContainer final {
 public:
  using QosToResourceMap = std::unordered_map<std::string,  // qos_name
                                              QosResource>;

  using UserResourceMetaMap = phmap::parallel_flat_hash_map<
      std::string,  // username
      QosToResourceMap, phmap::priv::hash_default_hash<std::string>,
      phmap::priv::hash_default_eq<std::string>,
      std::allocator<std::pair<const std::string, QosToResourceMap>>, 4,
      std::shared_mutex>;

  using AccountResourceMetaMap = phmap::parallel_flat_hash_map<
      std::string,  // account_name
      QosToResourceMap, phmap::priv::hash_default_hash<std::string>,
      phmap::priv::hash_default_eq<std::string>,
      std::allocator<std::pair<const std::string, QosToResourceMap>>, 4,
      std::shared_mutex>;

  AccountMetaContainer() = default;
  ~AccountMetaContainer() = default;

  CraneExpected<void> CheckQosSubmitResource(
      const TaskInCtld& task, const Qos& qos,
      const std::unordered_map<std::string, std::unique_ptr<Account>>&
          account_map);

  void MallocQosSubmitResource(
      const TaskInCtld& task,
      const std::unordered_map<std::string, std::unique_ptr<Account>>&
          account_map);

  void FreeQosSubmitResource(const TaskInCtld& task);

  bool CheckQosResource(const TaskInCtld& task);

  void MallocQosResource(const TaskInCtld& task);

  void FreeQosResource(const TaskInCtld& task);

  void DeleteUserResource(const std::string& username);

  void DeleteAccountResource(const std::string& account);

 private:
  UserResourceMetaMap user_meta_map_;

  AccountResourceMetaMap account_meta_map_;

  CraneExpected<void> CheckQosSubmitResourceForUser_(const TaskInCtld& task,
                                                     const Qos& qos);

  CraneExpected<void> CheckQosSubmitResourceForAccount_(
      const TaskInCtld& task, const Qos& qos,
      const std::unordered_map<std::string, std::unique_ptr<Account>>&
          account_map);

  void MallocQosSubmitResourceForUser_(const TaskInCtld& task);

  void MallocQosSubmitResourceForAccount_(
      const TaskInCtld& task,
      const std::unordered_map<std::string, std::unique_ptr<Account>>&
          account_map);

  void FreeQosSubmitResourceForUser_(const TaskInCtld& task);

  void FreeQosSubmitResourceForAccount_(const TaskInCtld& task);

  bool CheckQosResourceForUser_(const TaskInCtld& task, const Qos& qos);

  bool CheckQosResourceForAccount_(const TaskInCtld& task, const Qos& qos);

  void MallocQosResourceForUser_(const TaskInCtld& task);

  void MallocQosResourceForAccount_(const TaskInCtld& task);

  void FreeQosResourceForUser_(const TaskInCtld& task);

  void FreeQosResourceForAccount_(const TaskInCtld& task);
};

inline std::unique_ptr<Ctld::AccountMetaContainer> g_account_meta_container;

}  // namespace Ctld
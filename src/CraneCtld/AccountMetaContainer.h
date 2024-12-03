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

class AccountMetaContainer final {
 public:
  using QosToQosResourceMap = phmap::parallel_flat_hash_map<
      std::string,  // QosName
      QosResourceLimit, phmap::priv::hash_default_hash<std::string>,
      phmap::priv::hash_default_eq<std::string>,
      std::allocator<std::pair<const std::string, QosResourceLimit>>, 4,
      std::shared_mutex>;

  using UserResourceMetaMap = std::unordered_map<std::string,  // username
                                                 QosToQosResourceMap>;

  AccountMetaContainer();
  ~AccountMetaContainer() = default;

  void AddQosResourceToUser(const std::string& username,
                            const std::string& qos_name,
                            const QosResource& qos_resource);

  void ModifyQosResourceOnUser(const std::string& qos_name,
                               const QosResource& qos_resource);

  void FreeQosResource(const std::string& username, const TaskInCtld& task);

  bool CheckQosLimitOnUser(const std::string& username, const TaskInCtld& task);

  void MallocQosResourceFromUser(const std::string& username,
                                 const TaskInCtld& task);

 private:
  UserResourceMetaMap user_meta_map_;

  void InitFromDB_();

  void TryEmplace_(const std::string& username, const std::string& qos_name,
                   const QosResource& qos_resource);
};

inline std::unique_ptr<Ctld::AccountMetaContainer> g_account_meta_container;

}  // namespace Ctld
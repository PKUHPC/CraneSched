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
      QosResource, phmap::priv::hash_default_hash<std::string>,
      phmap::priv::hash_default_eq<std::string>,
      std::allocator<std::pair<const std::string, QosResource>>, 4,
      std::shared_mutex>;

  using UserResourceMetaMap = std::unordered_map<std::string,  // username
                                                 QosToQosResourceMap>;

  AccountMetaContainer() = default;
  ~AccountMetaContainer() = default;

  bool CheckAndMallocQosResourceFromUser(const std::string& username,
                                         const TaskInCtld& task,
                                         const Qos& qos);

  void FreeQosResource(const std::string& username, const TaskInCtld& task);

 private:
  UserResourceMetaMap user_meta_map_;
};

inline std::unique_ptr<Ctld::AccountMetaContainer> g_account_meta_container;

}  // namespace Ctld
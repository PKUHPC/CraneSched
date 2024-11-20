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

#include "crane/AtomicHashMap.h"
#include "crane/Lock.h"
#include "crane/Pointer.h"

namespace Ctld {

class AccountMetaContainer final {
 public:
  template <typename K, typename V,
            typename Hash = absl::container_internal::hash_default_hash<K>>
  using HashMap = absl::flat_hash_map<K, V, Hash>;

  template <typename K,
            typename Hash = absl::container_internal::hash_default_hash<K>>
  using HashSet = absl::flat_hash_set<K, Hash>;

  template <typename K, typename V>
  using TreeMap = absl::btree_map<K, V>;

  template <typename K>
  using TreeSet = absl::btree_set<K>;

  using UserResourceMetaAtomicMap =
      util::AtomicHashMap<HashMap, std::string, /*username*/
                          UserResourceMeta>;
  using UserResourceMetaRawMap = UserResourceMetaAtomicMap::RawMap;

  using UserResourceMetaMapConstPtr =
      util::ScopeConstSharedPtr<UserResourceMetaRawMap, util::rw_mutex>;

  using UserResourceMetaMapExclusivePtr =
      util::ScopeExclusivePtr<UserResourceMetaRawMap, util::rw_mutex>;

  using UserResourceMetaPtr =
      util::ManagedScopeExclusivePtr<UserResourceMeta,
                                     UserResourceMetaAtomicMap::CombinedLock>;

  using QosResourceList = std::list<std::pair<std::string, QosResource>>;

  AccountMetaContainer();
  ~AccountMetaContainer() = default;

  UserResourceMetaPtr GetUserResourceMetaPtr(const std::string& username);

  UserResourceMetaMapConstPtr GetUserResourceMetaMapConstPtr();

  UserResourceMetaMapExclusivePtr GetUserResourceMetaMapExclusivePtr();

  void AddQosResourceToUser(const std::string& username,
                            const QosResourceList& qos_resource_list);

  void EraseQosResourceOnUser(const std::string& username,
                              const std::string& qos_name);

  void EraseUserResource(const std::string& username);

  void ModifyQosResourceOnUser(const std::string& qos_name,
                               const QosResource& qos_resource);

  void FreeQosResource(const std::string& username, const TaskInCtld& task);

  bool CheckQosLimitOnUser(const std::string& username, const TaskInCtld& task);

  void MallocQosResourceFromUser(const std::string& username,
                                 const TaskInCtld& task);

 private:
  UserResourceMetaAtomicMap user_meta_map_;

  void InitFromDB_();
};

inline std::unique_ptr<Ctld::AccountMetaContainer> g_account_meta_container;

}  // namespace Ctld
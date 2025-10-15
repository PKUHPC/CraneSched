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

namespace Ctld {

template <typename K, typename V,
          typename Hash = absl::container_internal::hash_default_hash<K>>
using HashMap = absl::flat_hash_map<K, V, Hash>;

using LicensesAtomicMap = util::AtomicHashMap<HashMap, LicenseId, License>;
using LicensesMetaRawMap = LicensesAtomicMap::RawMap;

class LicensesManager {
 public:
  LicensesManager();

  ~LicensesManager() = default;

  int Init(const std::unordered_map<LicenseId, uint32_t> &lic_id_to_count_map);

  void GetLicensesInfo(const crane::grpc::QueryLicensesInfoRequest *request,
                       crane::grpc::QueryLicensesInfoReply *response);

  bool CheckLicenseCountSufficient(
      const google::protobuf::Map<std::string, uint32_t>& lic_id_to_count_map,
      bool is_license_or,
      std::unordered_map<LicenseId, uint32_t> *actual_licenses);

  std::expected<void, std::string> CheckLicensesLegal(
      const ::google::protobuf::Map<std::string, uint32_t>
          &lic_id_to_count_map, bool is_license_or);

  void MallocLicenseResource(
      const std::unordered_map<LicenseId, uint32_t> &lic_id_to_count_map);

  void FreeLicenseResource(
      const std::unordered_map<LicenseId, uint32_t> &lic_id_to_count_map);

 private:
  LicensesAtomicMap licenses_map_;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::LicensesManager> g_licenses_manager;
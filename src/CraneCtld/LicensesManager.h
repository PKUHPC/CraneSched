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
#include "crane/PluginClient.h"
#include "DbClient.h"

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

  std::expected<void, std::string> CheckLicensesLegal(
      const google::protobuf::RepeatedPtrField<crane::grpc::TaskToCtld_License>
          &lic_id_to_count,
      bool is_license_or);

  bool CheckLicenseCountSufficient(
      const google::protobuf::RepeatedPtrField<crane::grpc::TaskToCtld_License>
          &lic_id_to_count,
      bool is_license_or,
      std::unordered_map<LicenseId, uint32_t> *actual_licenses);

  void FreeReserved(
      const std::unordered_map<LicenseId, uint32_t> &actual_license);

  bool MallocLicense(
      const std::unordered_map<LicenseId, uint32_t> &actual_license);

  void MallocLicenseWhenRecoverRunning(
      const std::unordered_map<LicenseId, uint32_t> &actual_license);

  void FreeLicense(
      const std::unordered_map<LicenseId, uint32_t> &actual_license);

  /* TODO：multi-cluster synchronization */
  CraneExpectedRich<void> AddLicenseResource(
      const std::string &name, const std::string &server,
      const std::vector<std::string> &clusters,
      const std::unordered_map<crane::grpc::LicenseResource_Field, std::string>
          &operators);

  CraneExpectedRich<void> ModifyLicenseResource(
      const std::string &name, const std::string &server,
      const std::vector<std::string> &clusters,
      const std::unordered_map<crane::grpc::LicenseResource_Field, std::string>
          &operators);

  CraneExpectedRich<void> RemoveLicenseResource(
      const std::string &name, const std::string &server,
      const std::vector<std::string> &clusters);

  CraneExpectedRich<void> QueryLicenseResource(
      const std::string &name, const std::string &server,
      const std::vector<std::string> &clusters,
      std::list<LicenseResource> *res_licenses);

 private:
  CraneExpectedRich<void> CheckAndUpdateFields_(const std::vector<std::string> &clusters,
      const std::unordered_map<crane::grpc::LicenseResource_Field, std::string>
          &operators,
      LicenseResource *res_resource);

  struct LicenseIdServerPairHash {
    std::size_t operator()(const std::pair<LicenseId, std::string> &p) const {
      std::size_t h1 = std::hash<LicenseId>{}(p.first);
      std::size_t h2 = std::hash<std::string>{}(p.second);
      return h1 ^ (h2 << 1);
    }
  };
  std::unordered_map<std::pair<LicenseId, std::string>,
                     std::unique_ptr<LicenseResource>, LicenseIdServerPairHash>
      m_license_resource_map_;

  util::rw_mutex m_rw_resource_mutex_;

  LicensesAtomicMap m_licenses_map_;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::LicensesManager> g_licenses_manager;
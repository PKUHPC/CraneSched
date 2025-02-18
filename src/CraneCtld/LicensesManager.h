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

#include "crane/Lock.h"

namespace Ctld {

using unorderedLicensesMap = absl::flat_hash_map<LicenseId, License>;

class LicensesManager {
 public:
  LicensesManager();

  ~LicensesManager() = default;

  int Init(const std::unordered_map<LicenseId, uint32_t> &lic_id_to_count_map);

  void GetLicensesInfo(const crane::grpc::QueryLicensesInfoRequest *request,
                       crane::grpc::QueryLicensesInfoReply *response);

  bool CheckLicenseCountSufficient(
      const std::unordered_map<LicenseId, uint32_t> &lic_id_to_count_map);

  std::expected<void, std::string> CheckLicensesLegal(
      const ::google::protobuf::Map<std::string, uint32_t>
          &lic_id_to_count_map);

  void MallocLicenseResource(
      const std::unordered_map<LicenseId, uint32_t> &lic_id_to_count_map);

  void FreeLicenseResource(
      const std::unordered_map<LicenseId, uint32_t> &lic_id_to_count_map);

 private:
  unorderedLicensesMap lic_id_to_lic_map_ ABSL_GUARDED_BY(rw_mutex_);

  util::rw_mutex rw_mutex_;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::LicensesManager> g_licenses_manager;
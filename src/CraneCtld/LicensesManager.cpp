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

#include "LicensesManager.h"

namespace Ctld {

LicensesManager::LicensesManager() {}

int LicensesManager::Init(
    const std::unordered_map<LicenseId, uint32_t>& lic_id_to_count_map) {
  HashMap<LicenseId, License> licenses_map;

  for (auto& [lic_id, count] : lic_id_to_count_map) {
    licenses_map.insert({lic_id, License(lic_id, count, 0, count)});
  }

  m_licenses_map_.InitFromMap(std::move(licenses_map));

  return 0;
}

void LicensesManager::GetLicensesInfo(
    const crane::grpc::QueryLicensesInfoRequest* request,
    crane::grpc::QueryLicensesInfoReply* response) {
  auto* list = response->mutable_license_info_list();

  auto licenses_map = m_licenses_map_.GetMapConstSharedPtr();

  if (request->license_name_list().empty()) {
    for (const auto& [lic_index, lic_ptr] : *licenses_map) {
      auto* lic_info = list->Add();
      auto lic = lic_ptr.GetExclusivePtr();
      lic_info->set_name(lic->license_id);
      lic_info->set_total(lic->total);
      lic_info->set_used(lic->used);
      lic_info->set_free(lic->free);
    }
  } else {
    for (auto& license_name : request->license_name_list()) {
      if (licenses_map->contains(license_name)) {
        auto* lic_info = list->Add();
        auto lic = licenses_map->at(license_name).GetExclusivePtr();
        lic_info->set_name(lic->license_id);
        lic_info->set_total(lic->total);
        lic_info->set_used(lic->used);
        lic_info->set_free(lic->free);
      }
    }
  }
}

bool LicensesManager::CheckLicenseCountSufficient(
  const google::protobuf::RepeatedPtrField<crane::grpc::TaskToCtld_License> &lic_id_to_count,
  bool is_license_or, std::unordered_map<LicenseId, uint32_t>* actual_licenses) {

  auto licenses_map = m_licenses_map_.GetMapConstSharedPtr();
  actual_licenses->clear();
  if (is_license_or) {
    for (const auto & license : lic_id_to_count) {
      const auto& lic_id = license.key();
      auto count = license.count();
      if (licenses_map->contains(lic_id)) {
        auto lic = licenses_map->at(lic_id).GetExclusivePtr();
        if (count <= lic->free) {
          actual_licenses->emplace(lic_id, count);
          return true;
        }
      }
    }
    return false;
  }

  for (const auto & license : lic_id_to_count) {
    const auto& lic_id = license.key();
    auto count = license.count();
    if (!licenses_map->contains(lic_id))
      return false;
    auto lic = licenses_map->at(lic_id).GetExclusivePtr();
    if (count > lic->free)
      return false;
    actual_licenses->emplace(lic_id, count);
  }

  return true;
}

std::expected<void, std::string> LicensesManager::CheckLicensesLegal(
  const google::protobuf::RepeatedPtrField<crane::grpc::TaskToCtld_License> &lic_id_to_count,
    bool is_license_or, std::unordered_map<LicenseId, uint32_t> *actual_licenses) {
  auto licenses_map = m_licenses_map_.GetMapConstSharedPtr();
  actual_licenses->clear();
  if (is_license_or) {
    for (const auto & license : lic_id_to_count) {
      const auto& lic_id = license.key();
      auto count = license.count();
      if (licenses_map->contains(lic_id)) {
        auto lic = licenses_map->at(lic_id).GetExclusivePtr();
        if (count <= lic->total)
          actual_licenses->emplace(lic_id, count);
      }
    }
    if (!actual_licenses->empty()) return {};

    return std::unexpected("Invalid license specification");
  }

  for (const auto & license : lic_id_to_count) {
    const auto& lic_id = license.key();
    auto count = license.count();
    if (!licenses_map->contains(lic_id))
      return std::unexpected("Invalid license specification");
    auto lic = licenses_map->at(lic_id).GetExclusivePtr();
    if (count > lic->total)
      return std::unexpected("Invalid license specification");
    actual_licenses->emplace(lic_id, count);
  }

  return {};
}

void LicensesManager::MallocLicenseResource(
    const std::unordered_map<LicenseId, uint32_t>& lic_id_to_count_map) {
  for (auto& [lic_id, count] : lic_id_to_count_map) {
    auto lic = m_licenses_map_[lic_id];
    lic->used += count;
    if (lic->free < count) {
      CRANE_ERROR("MallocLicenseResource: license [{}] requested={}, free={}, will set free=0", lic_id, count, lic->free);
      lic->free = 0;
    } else {
      lic->free -= count;
    }
  }
}

void LicensesManager::FreeLicenseResource(
    const std::unordered_map<LicenseId, uint32_t>& lic_id_to_count_map) {
  for (auto& [lic_id, count] : lic_id_to_count_map) {
    auto lic = m_licenses_map_[lic_id];
    if (lic->used < count) {
      CRANE_ERROR("FreeLicenseResource: license [{}] used < freeing count ({} < {}), will set used=0", lic_id, lic->used + count, count);
      lic->used = 0;
    } else {
      lic->used -= count;
    }
    lic->free += count;
  }
}

}  // namespace Ctld

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
  util::read_lock_guard readLock(rw_mutex_);
  for (auto& [lic_id, count] : lic_id_to_count_map) {
    lic_id_to_lic_map_.insert({lic_id, License(lic_id, count, 0, count)});
  }
  return 0;
}

void LicensesManager::GetLicensesInfo(
    const crane::grpc::QueryLicensesInfoRequest* request,
    crane::grpc::QueryLicensesInfoReply* response) {
  auto* list = response->mutable_license_info_list();

  if (request->license_name_list().empty()) {
    util::read_lock_guard readLock(rw_mutex_);
    for (auto& [lic_index, lic] : lic_id_to_lic_map_) {
      auto* lic_info = list->Add();
      lic_info->set_name(lic.license_id);
      lic_info->set_total(lic.total);
      lic_info->set_used(lic.used);
      lic_info->set_free(lic.free);
    }
  } else {
    util::read_lock_guard readLock(rw_mutex_);
    for (auto& license_name : request->license_name_list()) {
      auto it = lic_id_to_lic_map_.find(license_name);
      if (it != lic_id_to_lic_map_.end()) {
        auto* lic_info = list->Add();
        lic_info->set_name(it->second.license_id);
        lic_info->set_total(it->second.total);
        lic_info->set_used(it->second.used);
        lic_info->set_free(it->second.free);
      }
    }
  }
}

bool LicensesManager::CheckLicenseCountSufficient(
    const std::unordered_map<LicenseId, uint32_t>& lic_id_to_count_map) {
  util::read_lock_guard readLock(rw_mutex_);
  for (auto& [lic_id, count] : lic_id_to_count_map) {
    auto it = lic_id_to_lic_map_.find(lic_id);
    if (it == lic_id_to_lic_map_.end() || it->second.free < count) {
      return false;
    }
  }

  return true;
}

result::result<void, std::string> LicensesManager::CheckLicensesLegal(
    const ::google::protobuf::Map<std::string, uint32_t>& lic_id_to_count_map) {
  util::read_lock_guard readLock(rw_mutex_);
  for (auto& [lic_id, count] : lic_id_to_count_map) {
    auto it = lic_id_to_lic_map_.find(lic_id);
    if (it == lic_id_to_lic_map_.end() || count > it->second.total) {
      return result::fail("Invalid license specification");
    }
  }

  return {};
}

void LicensesManager::MallocLicenseResource(
    const std::unordered_map<LicenseId, uint32_t>& lic_id_to_count_map) {
  util::write_lock_guard writeLock(rw_mutex_);
  for (auto& [lic_id, count] : lic_id_to_count_map) {
    auto it = lic_id_to_lic_map_.find(lic_id);
    it->second.used += count;
    it->second.free -= count;
  }
}

void LicensesManager::FreeLicenseResource(
    const std::unordered_map<LicenseId, uint32_t>& lic_id_to_count_map) {
  util::write_lock_guard writeLock(rw_mutex_);
  for (auto& [lic_id, count] : lic_id_to_count_map) {
    auto it = lic_id_to_lic_map_.find(lic_id);
    it->second.used -= count;
    it->second.free += count;
  }
}

}  // namespace Ctld

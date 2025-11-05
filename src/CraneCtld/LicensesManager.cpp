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
    licenses_map.insert({lic_id, License{.license_id = lic_id,
                                         .total = count,
                                         .used = 0,
                                         .free = count,
                                         .reserved = 0}});
  }

  m_licenses_map_.InitFromMap(std::move(licenses_map));

  if (g_config.Plugin.Enabled) {
    std::vector<crane::grpc::LicenseInfo> license_infos;
    license_infos.reserve(lic_id_to_count_map.size());
    for (auto& [lic_id, count] : lic_id_to_count_map) {
      crane::grpc::LicenseInfo license_info;
      license_info.set_name(lic_id);
      license_info.set_total(count);
      license_info.set_free(count);
      license_info.set_used(0);
      license_infos.emplace_back(std::move(license_info));
    }
    g_plugin_client->UpdateLicensesHookAsync(license_infos);
  }

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
      auto iter = licenses_map->find(license_name);
      if (iter != licenses_map->end()) {
        auto* lic_info = list->Add();
        auto lic = iter->second.GetExclusivePtr();
        lic_info->set_name(lic->license_id);
        lic_info->set_total(lic->total);
        lic_info->set_used(lic->used);
        lic_info->set_free(lic->free);
      }
    }
  }
}

std::expected<void, std::string> LicensesManager::CheckLicensesLegal(
    const google::protobuf::RepeatedPtrField<crane::grpc::TaskToCtld_License>&
        lic_id_to_count,
    bool is_license_or) {
  auto licenses_map = m_licenses_map_.GetMapConstSharedPtr();
  if (is_license_or) {
    for (const auto& license : lic_id_to_count) {
      const auto& lic_id = license.key();
      auto count = license.count();
      auto iter = licenses_map->find(lic_id);
      if (iter != licenses_map->end()) {
        auto lic = iter->second.GetExclusivePtr();
        if (count <= lic->total) return {};
      }
    }

    return std::unexpected("Invalid license specification");
  }

  for (const auto& license : lic_id_to_count) {
    const auto& lic_id = license.key();
    auto count = license.count();
    auto iter = licenses_map->find(lic_id);
    if (iter == licenses_map->end())
      return std::unexpected("Invalid license specification");
    auto lic = iter->second.GetExclusivePtr();
    if (count > lic->total)
      return std::unexpected("Invalid license specification");
  }

  return {};
}

bool LicensesManager::CheckLicenseCountSufficient(
    const google::protobuf::RepeatedPtrField<crane::grpc::TaskToCtld_License>&
        lic_id_to_count,
    bool is_license_or,
    std::unordered_map<LicenseId, uint32_t>* actual_licenses) {
  auto licenses_map = m_licenses_map_.GetMapExclusivePtr();
  actual_licenses->clear();
  if (is_license_or) {
    for (const auto& license : lic_id_to_count) {
      auto iter = licenses_map->find(license.key());
      if (iter != licenses_map->end()) {
        auto lic = iter->second.GetExclusivePtr();
        if (license.count() + lic->reserved <= lic->free) {
          actual_licenses->emplace(license.key(), license.count());
          break;
        }
      }
    }
  } else {
    for (const auto& license : lic_id_to_count) {
      auto iter = licenses_map->find(license.key());
      if (iter == licenses_map->end()) {
        actual_licenses->clear();
        return false;
      }
      auto lic = iter->second.GetExclusivePtr();
      if (license.count() + lic->reserved > lic->free) {
        actual_licenses->clear();
        return false;
      }
      actual_licenses->emplace(license.key(), license.count());
    }
  }

  for (auto [lic_id, count] : *actual_licenses) {
    auto lic = licenses_map->at(lic_id).GetExclusivePtr();
    lic->reserved += count;
  }

  return !actual_licenses->empty();
}

void LicensesManager::FreeReserved(
    const std::unordered_map<LicenseId, uint32_t>& actual_license) {
  auto licenses_map = m_licenses_map_.GetMapExclusivePtr();

  for (const auto& [lic_id, count] : actual_license) {
    auto iter = licenses_map->find(lic_id);
    if (iter == licenses_map->end()) continue;
    auto lic = iter->second.GetExclusivePtr();
    if (lic->reserved < count) {
      CRANE_ERROR(
          "FreeReserved: license [{}] reserved < freeing count ({} < {}), "
          "will clamp to 0",
          lic_id, lic->reserved, count);
      lic->reserved = 0;
    } else {
      lic->reserved -= count;
    }
  }
}

bool LicensesManager::MallocLicenseResource(
    const std::unordered_map<LicenseId, uint32_t>& actual_license) {
  auto licenses_map = m_licenses_map_.GetMapExclusivePtr();

  for (const auto& [lic_id, count] : actual_license) {
    auto iter = licenses_map->find(lic_id);
    if (iter == licenses_map->end()) return false;
    auto lic = iter->second.GetExclusivePtr();
    if (lic->used + count > lic->total) return false;
  }

  for (auto& [lic_id, count] : actual_license) {
    auto lic = licenses_map->at(lic_id).GetExclusivePtr();
    lic->used += count;
    if (lic->free < count) {
      CRANE_ERROR(
          "MallocLicenseResource: license [{}] requested={}, free={}, will set "
          "free=0",
          lic_id, count, lic->free);
      lic->free = 0;
    } else {
      lic->free -= count;
    }
  }

    return true;
}

void LicensesManager::MallocLicenseResourceWhenRecoverRunning(
    const std::unordered_map<LicenseId, uint32_t>& actual_license) {
  auto licenses_map = m_licenses_map_.GetMapExclusivePtr();

  for (auto& [lic_id, count] : actual_license) {
    auto iter = licenses_map->find(lic_id);
    if (iter == licenses_map->end()) continue;
    auto lic = iter->second.GetExclusivePtr();
    lic->used += count;
    if (lic->free < count) {
      CRANE_ERROR(
          "MallocLicenseResourceWhenRecoverRunning: license [{}] requested={}, "
          "free={}, will set "
          "free=0",
          lic_id, count, lic->free);
      lic->free = 0;
    } else {
      lic->free -= count;
    }
  }

  if (g_config.Plugin.Enabled) {
    auto licenses_map = m_licenses_map_.GetMapConstSharedPtr();
    std::vector<crane::grpc::LicenseInfo> license_infos;
    license_infos.reserve(licenses_map->size());
    for (const auto& [lic_id, lic_ptr] : *licenses_map) {
      auto lic = lic_ptr.GetExclusivePtr();
      crane::grpc::LicenseInfo license_info;
      license_info.set_name(lic->license_id);
      license_info.set_total(lic->total);
      license_info.set_free(lic->free);
      license_info.set_used(lic->used);
      license_infos.emplace_back(std::move(license_info));
    }
    g_plugin_client->UpdateLicensesHookAsync(license_infos);
  }

}

void LicensesManager::FreeLicenseResource(
    const std::unordered_map<LicenseId, uint32_t>& actual_license) {
  auto licenses_map = m_licenses_map_.GetMapExclusivePtr();

  for (auto& [lic_id, count] : actual_license) {
    auto iter = licenses_map->find(lic_id);
    if (iter == licenses_map->end()) continue;
    auto lic = iter->second.GetExclusivePtr();
    if (lic->used < count) {
      CRANE_ERROR(
          "FreeLicenseResource: license [{}] used < freeing count ({} < {}), "
          "will set used=0",
          lic_id, lic->used, count);
      lic->used = 0;
    } else {
      lic->used -= count;
    }
    lic->free += count;
  }

  if (g_config.Plugin.Enabled) {
    auto licenses_map = m_licenses_map_.GetMapConstSharedPtr();
    std::vector<crane::grpc::LicenseInfo> license_infos;
    license_infos.reserve(licenses_map->size());
    for (const auto& [lic_id, lic_ptr] : *licenses_map) {
      auto lic = lic_ptr.GetExclusivePtr();
      crane::grpc::LicenseInfo license_info;
      license_info.set_name(lic->license_id);
      license_info.set_total(lic->total);
      license_info.set_free(lic->free);
      license_info.set_used(lic->used);
      license_infos.emplace_back(std::move(license_info));
    }
    g_plugin_client->UpdateLicensesHookAsync(license_infos);
  }
}

}  // namespace Ctld

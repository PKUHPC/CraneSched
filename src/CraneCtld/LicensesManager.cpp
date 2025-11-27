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
  std::list<LicenseResource> resource_list;

  g_db_client->SelectAllResource(&resource_list);

  util::write_lock_guard resource_guard(m_rw_resource_mutex_);
  for (const auto& resource : resource_list) {
    if (resource.type == crane::grpc::LicenseResource_Type_License) {
      auto iter = resource.cluster_resources.find(g_config.CraneClusterName);
      if (iter != resource.cluster_resources.end()) {
        auto license_id = std::format("{}@{}", resource.name, resource.server);
        licenses_map.emplace(license_id,
                             License{.license_id = license_id,
                                     .total = iter->second,
                                     .used = 0,
                                     .reserved = 0,
                                     .remote = true,
                                     .server = resource.server,
                                     .last_consumed = resource.last_consumed,
                                     .last_deficit = 0,
                                     .last_update = resource.last_update});
      }
    }
    auto key = std::make_pair(resource.name, resource.server);
    m_license_resource_map_.emplace(
        key, std::make_unique<LicenseResource>(std::move(resource)));
  }

  for (auto& [lic_id, count] : lic_id_to_count_map) {
    licenses_map.insert({lic_id, License{.license_id = lic_id,
                                         .total = count,
                                         .used = 0,
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
      lic_info->set_free(lic->total - lic->used - lic->reserved -
                         lic->last_deficit);
      lic_info->set_reserved(lic->reserved);
      lic_info->set_remote(lic->remote);
      lic_info->set_lastconsumed(lic->last_consumed);
      lic_info->set_lastdeficit(lic->last_deficit);
      lic_info->set_lastupdate(lic->last_update);
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
        lic_info->set_free(lic->total - lic->used - lic->reserved -
                           lic->last_deficit);
        lic_info->set_reserved(lic->reserved);
        lic_info->set_remote(lic->remote);
        lic_info->set_lastconsumed(lic->last_consumed);
        lic_info->set_lastdeficit(lic->last_deficit);
        lic_info->set_lastupdate(lic->last_update);
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
        if (license.count() + lic->reserved + lic->used + lic->last_deficit <=
            lic->total) {
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
      if (license.count() + lic->reserved + lic->used + lic->last_deficit >
          lic->total) {
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

bool LicensesManager::MallocLicense(
    const std::unordered_map<LicenseId, uint32_t>& actual_license) {
  auto licenses_map = m_licenses_map_.GetMapExclusivePtr();

  for (const auto& [lic_id, count] : actual_license) {
    auto iter = licenses_map->find(lic_id);
    if (iter == licenses_map->end()) return false;
    auto lic = iter->second.GetExclusivePtr();
    if (lic->used + count + lic->reserved + lic->last_deficit > lic->total)
      return false;
  }

  for (auto& [lic_id, count] : actual_license) {
    auto lic = licenses_map->at(lic_id).GetExclusivePtr();
    lic->used += count;
  }

  if (g_config.Plugin.Enabled) {
    std::vector<crane::grpc::LicenseInfo> license_infos;
    license_infos.reserve(actual_license.size());
    for (const auto& [lic_id, _] : actual_license) {
      auto lic = licenses_map->find(lic_id)->second.GetExclusivePtr();
      crane::grpc::LicenseInfo license_info;
      license_info.set_name(lic->license_id);
      license_info.set_total(lic->total);
      license_info.set_free(lic->free);
      license_info.set_used(lic->used);
      license_infos.emplace_back(std::move(license_info));
    }
    g_plugin_client->UpdateLicensesHookAsync(license_infos);
  }

  return true;
}

void LicensesManager::MallocLicenseWhenRecoverRunning(
    const std::unordered_map<LicenseId, uint32_t>& actual_license) {
  auto licenses_map = m_licenses_map_.GetMapExclusivePtr();

  for (auto& [lic_id, count] : actual_license) {
    auto iter = licenses_map->find(lic_id);
    if (iter == licenses_map->end()) continue;
    auto lic = iter->second.GetExclusivePtr();
    lic->used += count;
  }

  if (g_config.Plugin.Enabled) {
    std::vector<crane::grpc::LicenseInfo> license_infos;
    license_infos.reserve(actual_license.size());
    for (const auto& [lic_id, _] : actual_license) {
      auto lic = licenses_map->find(lic_id)->second.GetExclusivePtr();
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

void LicensesManager::FreeLicense(
    const std::unordered_map<LicenseId, uint32_t>& actual_license) {
  auto licenses_map = m_licenses_map_.GetMapExclusivePtr();

  for (auto& [lic_id, count] : actual_license) {
    auto iter = licenses_map->find(lic_id);
    if (iter == licenses_map->end()) continue;
    auto lic = iter->second.GetExclusivePtr();
    if (lic->used < count) {
      CRANE_ERROR(
          "FreeLicense: license [{}] used < freeing count ({} < {}), "
          "will set used=0",
          lic_id, lic->used, count);
      lic->used = 0;
    } else {
      lic->used -= count;
    }
  }

  if (g_config.Plugin.Enabled) {
    std::vector<crane::grpc::LicenseInfo> license_infos;
    license_infos.reserve(actual_license.size());
    for (const auto& [lic_id, _] : actual_license) {
      auto lic = licenses_map->find(lic_id)->second.GetExclusivePtr();
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

CraneExpectedRich<void> LicensesManager::AddLicenseResource(
    LicenseResource&& new_license) {
  util::write_lock_guard resource_guard(m_rw_resource_mutex_);

  if (new_license.name.empty() || new_license.server.empty())
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_INVALID_PARAM, "Name or server is empty"));

  CRANE_TRACE("Add License Resource {}@{}...", new_license.name, new_license.server);

  auto key = std::make_pair(new_license.name, new_license.server);

  if (m_license_resource_map_.contains(key))
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_RESOURCE_ALREADY_EXIST, ""));

  if (new_license.allocated > new_license.count) {
    CRANE_TRACE(
        "License Resource {}@{}: Allocated resources {} exceed total available "
        "count {}",
        new_license.name, new_license.server, new_license.allocated,
        new_license.count);
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_INVALID_PARAM,
                      "Allocated resources {} exceed total available count {}",
                      new_license.allocated, new_license.count));
  }

  new_license.last_update = absl::ToUnixMillis(absl::Now());

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->InsertResource(new_license);
      };

  if (!g_db_client->CommitTransaction(callback))
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_UPDATE_DATABASE, ""));

  if (new_license.type == crane::grpc::LicenseResource_Type_License) {
    auto iter = new_license.cluster_resources.find(g_config.CraneClusterName);
    if (iter != new_license.cluster_resources.end()) {
      auto license_id =
          std::format("{}@{}", new_license.name, new_license.server);
      m_licenses_map_.Emplace(
          license_id, License{.license_id = license_id,
                              .total = iter->second,
                              .used = 0,
                              .reserved = 0,
                              .remote = true,
                              .server = new_license.server,
                              .last_consumed = new_license.last_consumed,
                              .last_deficit = 0,
                              .last_update = new_license.last_update});
    }
  }

  m_license_resource_map_[key] =
      std::make_unique<LicenseResource>(std::move(new_license));

  return {};
}

CraneExpectedRich<void> LicensesManager::ModifyLicenseResource(
    const std::string& name, const std::vector<std::string>& clusters,
    const std::string& server, const crane::grpc::LicenseResource_Field& field,
    const std::string& value) {
  util::write_lock_guard resource_guard(m_rw_resource_mutex_);

  CRANE_TRACE("Modify license resource: {}@{}...", name, server);

  auto key = std::make_pair(name, server);
  auto iter = m_license_resource_map_.find(key);
  if (iter == m_license_resource_map_.end())
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_RESOURCE_NOT_FOUND, ""));

  LicenseResource res_resource(*(iter->second));
  switch (field) {
  case crane::grpc::LicenseResource_Field::LicenseResource_Field_Count:
    try {
      uint32_t new_count = std::stoul(value);
      if (new_count < res_resource.allocated) {
        CRANE_TRACE(
            "License Resource {}@{}: new count {} is less than allocated "
            "resources {}, which is not allowed",
            res_resource.name, res_resource.server, new_count,
            res_resource.allocated);
        return std::unexpected(FormatRichErr(
            CraneErrCode::ERR_INVALID_PARAM,
            "New count {} cannot be less than currently allocated resources {}",
            new_count, res_resource.allocated));
      }
      res_resource.count = new_count;
    } catch (std::exception& e) {
      CRANE_TRACE("Failed to parse 'count' from value '{}': {}", value,
                  e.what());
      return std::unexpected(FormatRichErr(CraneErrCode::ERR_INVALID_PARAM,
                                           "Invalid count parameter"));
    }
    break;
  case crane::grpc::LicenseResource_Field::LicenseResource_Field_Allowed:
    for (const auto& cluster : clusters) {
      std::unordered_map<std::string, uint32_t>::iterator cluster_iter;
      if (cluster == "local") {
        cluster_iter =
            res_resource.cluster_resources.find(g_config.CraneClusterName);
      } else {
        cluster_iter = res_resource.cluster_resources.find(cluster);
      }
      if (cluster_iter == res_resource.cluster_resources.end())
        return std::unexpected(
            FormatRichErr(CraneErrCode::ERR_RESOURCE_NOT_FOUND,
                          "Resources {}@{} not found in cluster {}",
                          res_resource.name, res_resource.server, cluster));
      try {
        res_resource.allocated -= cluster_iter->second;
        cluster_iter->second = std::stoul(value);
        res_resource.allocated += cluster_iter->second;
      } catch (std::exception& e) {
        CRANE_TRACE(
            "Failed to parse 'allowed' for cluster '{}' from value '{}': {}",
            cluster, value, e.what());
        return std::unexpected(FormatRichErr(CraneErrCode::ERR_INVALID_PARAM,
                                             "Invalid allowed parameter"));
      }
    }
    if (res_resource.allocated > res_resource.count) {
      CRANE_TRACE(
          "License Resource {}@{}: Allocated resources {} exceed total "
          "available "
          "count {}",
          res_resource.name, res_resource.server, res_resource.allocated,
          res_resource.count);
      return std::unexpected(FormatRichErr(
          CraneErrCode::ERR_INVALID_PARAM,
          "Allocated resources {} exceed total available count {}",
          res_resource.allocated, res_resource.count));
    }
    break;
  case crane::grpc::LicenseResource_Field_LastConsumed:
    try {
      res_resource.last_consumed = std::stoul(value);
    } catch (std::exception& e) {
      CRANE_TRACE("Failed to parse 'last_consumed' from value '{}': {}", value,
                  e.what());
      return std::unexpected(FormatRichErr(CraneErrCode::ERR_INVALID_PARAM,
                                           "Invalid last_consumed parameter"));
    }
    break;
  case crane::grpc::LicenseResource_Field_Description:
    res_resource.description = value;
    break;
  case crane::grpc::LicenseResource_Field_Flags:
    break;
  case crane::grpc::LicenseResource_Field_ServerType:
    res_resource.server_type = value;
    break;
  default:
    CRANE_ERROR("Unrecognized LicenseResource field: {}",
                std::to_string(field));
  }

  res_resource.last_update = absl::ToUnixMillis(absl::Now());

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateResource(res_resource);
      };

  if (!g_db_client->CommitTransaction(callback))
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_UPDATE_DATABASE, ""));

  // update license
  if (res_resource.type == crane::grpc::LicenseResource_Type_License) {
    auto cluster_iter =
        res_resource.cluster_resources.find(g_config.CraneClusterName);
    if (cluster_iter != res_resource.cluster_resources.end()) {
      auto licenses_map = m_licenses_map_.GetMapExclusivePtr();
      auto license_id =
          std::format("{}@{}", res_resource.name, res_resource.server);
      auto lic_iter = licenses_map->find(license_id);
      if (lic_iter == licenses_map->end()) {
        CRANE_ERROR("License resource {} not found in license map.",
                    license_id);
      } else {
        auto lic = lic_iter->second.GetExclusivePtr();
        lic->total = cluster_iter->second;
        auto external = res_resource.count - lic->total;
        lic->last_consumed = res_resource.last_consumed;
        if (lic->last_consumed <= (external + lic->used))
          lic->last_deficit = 0;
        else {
          lic->last_deficit = lic->last_consumed - external - lic->used;
        }
        lic->last_update = res_resource.last_update;
      }
    }
  }

  m_license_resource_map_[key] =
      std::make_unique<LicenseResource>(std::move(res_resource));

  return {};
}

CraneExpectedRich<void> LicensesManager::RemoveLicenseResource(
    const std::string& name, const std::string& server,
    const std::vector<std::string>& clusters) {
  util::write_lock_guard resource_guard(m_rw_resource_mutex_);

  CRANE_TRACE("Remove license resource{}@{}....", name, server);

  auto key = std::make_pair(name, server);
  auto iter = m_license_resource_map_.find(key);
  if (iter == m_license_resource_map_.end())
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_RESOURCE_NOT_FOUND, ""));

  LicenseResource res_resource(*iter->second);
  bool is_local = false;
  for (const auto& cluster : clusters) {
    std::unordered_map<std::string, uint32_t>::iterator cluster_iter;
    if (cluster == "local" || cluster == g_config.CraneClusterName) {
      cluster_iter =
          res_resource.cluster_resources.find(g_config.CraneClusterName);
      is_local = true;
    } else {
      cluster_iter = res_resource.cluster_resources.find(cluster);
    }
    if (cluster_iter == res_resource.cluster_resources.end())
      return std::unexpected(
          FormatRichErr(CraneErrCode::ERR_RESOURCE_NOT_FOUND,
                        "Resources {}@{} not found in cluster {}",
                        res_resource.name, res_resource.server, cluster));
    res_resource.allocated -= cluster_iter->second;
    res_resource.cluster_resources.erase(cluster_iter);
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        if (clusters.empty()) {
          g_db_client->DeleteResource(name, server);
        } else {
          g_db_client->UpdateResource(res_resource);
        }
      };

  if (!g_db_client->CommitTransaction(callback))
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_UPDATE_DATABASE, ""));

  if (clusters.empty() || is_local) {
    auto license_id =
        std::format("{}@{}", res_resource.name, res_resource.server);
    m_licenses_map_.Erase(license_id);
  }

  if (clusters.empty()) {
    m_license_resource_map_.erase(key);
  } else {
    m_license_resource_map_[key] =
        std::make_unique<LicenseResource>(std::move(res_resource));
  }

  return {};
}

CraneExpectedRich<void> LicensesManager::QueryLicenseResource(
    const std::string& name, const std::string& server,
    const std::vector<std::string>& clusters,
    std::list<LicenseResource>* res_licenses) {
  util::read_lock_guard resource_guard(m_rw_resource_mutex_);

  if (name.empty() || server.empty()) {
    for (const auto& [key, resource] : m_license_resource_map_) {
      if (!name.empty() && key.first != name) continue;
      if (!server.empty() && key.second != server) continue;
      if (!clusters.empty()) {
        bool found = false;
        for (const auto& cluster : clusters) {
          if (resource->cluster_resources.contains(cluster)) {
            found = true;
            break;
          }
        }
        if (!found) continue;
      }
      res_licenses->emplace_back(*resource);
    }
  } else {
    auto iter = m_license_resource_map_.find(std::make_pair(name, server));
    if (iter == m_license_resource_map_.end()) return {};
    if (!clusters.empty()) {
      bool found = false;
      for (const auto& cluster : clusters) {
        if (iter->second->cluster_resources.contains(cluster)) {
          found = true;
          break;
        }
      }
      if (!found) return {};
    }
    res_licenses->emplace_back(*iter->second);
  }

  return {};
}

}  // namespace Ctld

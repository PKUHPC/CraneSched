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
#include "TaskScheduler.h"

namespace Ctld {

LicensesManager::LicensesManager() {}

int LicensesManager::Init(
    const std::unordered_map<LicenseId, uint32_t>& lic_id_to_count_map) {
  HashMap<LicenseId, License> licenses_map;
  std::list<LicenseResourceInDb> resource_list;

  g_db_client->SelectAllLicenseResource(&resource_list);

  util::write_lock_guard resource_guard(m_rw_resource_mutex_);
  for (const auto& resource : resource_list) {
    if (resource.type == crane::grpc::LicenseResource_Type_License) {
      auto cluster_iter =
          resource.cluster_resources.find(g_config.CraneClusterName);
      if (cluster_iter != resource.cluster_resources.end()) {
        auto license_id = std::format("{}@{}", resource.name, resource.server);
        License lic{.license_id = license_id,
                    .used = 0,
                    .reserved = 0,
                    .remote = true,
                    .last_consumed = resource.last_consumed,
                    .last_deficit = 0,
                    .last_update = resource.last_update};
        UpdateLicense_(resource, cluster_iter->second, &lic);
        licenses_map.emplace(license_id, std::move(lic));
      }
    }
    auto key = std::make_pair(resource.name, resource.server);
    m_license_resource_map_.emplace(
        key, std::make_unique<LicenseResourceInDb>(resource));
  }

  for (const auto& [lic_id, count] : lic_id_to_count_map) {
    licenses_map.insert({lic_id, License{.license_id = lic_id,
                                         .total = count,
                                         .used = 0,
                                         .reserved = 0}});
  }

  if (g_config.Plugin.Enabled) {
    std::vector<crane::grpc::LicenseInfo> license_infos;
    license_infos.reserve(licenses_map.size());
    for (const auto& [lic_id, lic] : licenses_map) {
      crane::grpc::LicenseInfo license_info;
      license_info.set_name(lic_id);
      license_info.set_total(lic.total);
      license_info.set_free(lic.total - lic.used - lic.reserved -
                            lic.last_deficit);
      license_info.set_used(lic.used);
      license_info.set_reserved(lic.reserved);
      license_info.set_last_deficit(lic.last_deficit);
      license_info.set_last_consumed(lic.last_consumed);
      license_infos.emplace_back(std::move(license_info));
    }
    g_plugin_client->UpdateLicensesHookAsync(license_infos);
  }

  m_licenses_map_.InitFromMap(std::move(licenses_map));

  return 0;
}

LicensesMapExclusivePtr LicensesManager::GetLicensesMapExclusivePtr() {
  return m_licenses_map_.GetMapExclusivePtr();
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
      lic_info->set_last_consumed(lic->last_consumed);
      lic_info->set_last_deficit(lic->last_deficit);
      lic_info->set_last_update(absl::ToUnixSeconds(lic->last_update));
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
        lic_info->set_last_consumed(lic->last_consumed);
        lic_info->set_last_deficit(lic->last_deficit);
        lic_info->set_last_update(absl::ToUnixSeconds(lic->last_update));
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

void LicensesManager::CheckLicenseCountSufficient(
    std::vector<PdJobInScheduler*>* job_ptr_vec) {
  std::unordered_map<LicenseId, License> avail_license_map;

  {
    auto licenses_map = m_licenses_map_.GetMapExclusivePtr();
    for (const auto& [license_id, license] : *licenses_map) {
      avail_license_map.emplace(license_id, *license.GetExclusivePtr());
    }
  }

  for (const auto& job_ptr : *job_ptr_vec) {
    if (job_ptr->req_licenses.empty()) continue;

    job_ptr->actual_licenses.clear();

    if (job_ptr->is_license_or) {
      for (const auto& license : job_ptr->req_licenses) {
        auto iter = avail_license_map.find(license.key());
        if (iter != avail_license_map.end()) {
          auto lic = iter->second;
          if (license.count() + lic.reserved + lic.used + lic.last_deficit <=
              lic.total) {
            job_ptr->actual_licenses.emplace(license.key(), license.count());
            break;
          }
        }
      }
    } else {
      for (const auto& license : job_ptr->req_licenses) {
        auto iter = avail_license_map.find(license.key());
        if (iter == avail_license_map.end()) {
          job_ptr->actual_licenses.clear();
          break;
        }
        auto lic = iter->second;
        if (license.count() + lic.reserved + lic.used + lic.last_deficit >
            lic.total) {
          job_ptr->actual_licenses.clear();
          break;
        }
        job_ptr->actual_licenses.emplace(license.key(), license.count());
      }
    }

    if (job_ptr->actual_licenses.empty()) {
      job_ptr->reason = "License";
      continue;
    }

    for (const auto& [lic_id, count] : job_ptr->actual_licenses) {
      avail_license_map[lic_id].used += count;
    }
  }
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
      license_info.set_free(lic->total - lic->used - lic->reserved -
                            lic->last_deficit);
      license_info.set_used(lic->used);
      license_info.set_reserved(lic->reserved);
      license_info.set_last_deficit(lic->last_deficit);
      license_info.set_last_consumed(lic->last_consumed);
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
    if (lic->remote) {
      if (count > lic->last_deficit)
        lic->last_deficit = 0;
      else
        lic->last_deficit -= count;
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
      license_info.set_free(lic->total - lic->used - lic->reserved -
                            lic->last_deficit);
      license_info.set_used(lic->used);
      license_info.set_reserved(lic->reserved);
      license_info.set_last_deficit(lic->last_deficit);
      license_info.set_last_consumed(lic->last_consumed);
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
      license_info.set_free(lic->total - lic->used - lic->reserved -
                            lic->last_deficit);
      license_info.set_used(lic->used);
      license_info.set_reserved(lic->reserved);
      license_info.set_last_deficit(lic->last_deficit);
      license_info.set_last_consumed(lic->last_consumed);
      license_infos.emplace_back(std::move(license_info));
    }
    g_plugin_client->UpdateLicensesHookAsync(license_infos);
  }
}

CraneExpectedRich<void> LicensesManager::AddLicenseResource(
    const std::string& name, const std::string& server,
    const std::vector<std::string>& clusters,
    const std::unordered_map<crane::grpc::LicenseResource_Field, std::string>&
        operators) {
  util::write_lock_guard resource_guard(m_rw_resource_mutex_);

  CRANE_TRACE("Add License Resource {}@{}...", name, server);

  if (name.empty() || server.empty())
    return std::unexpected(FormatRichErr(CraneErrCode::ERR_INVALID_PARAM,
                                         "Name or server is empty"));

  LicenseResourceInDb new_lic_resource{.name = name, .server = server};
  bool is_exist = false;
  auto key = std::make_pair(name, server);
  auto iter = m_license_resource_map_.find(key);
  if (iter != m_license_resource_map_.end()) {
    if (clusters.empty())
      return std::unexpected(
          FormatRichErr(CraneErrCode::ERR_RESOURCE_ALREADY_EXIST, ""));
    for (const auto& cluster : clusters) {
      // TODO: Support multiple clusters in the future.
      if (cluster != "local" && cluster != g_config.CraneClusterName)
        return std::unexpected(
            FormatRichErr(CraneErrCode::ERR_INVALID_CLUSTER,
                          "The entered cluster {} does not exist", cluster));
      if (iter->second->cluster_resources.contains(g_config.CraneClusterName)) {
        return std::unexpected(
            FormatRichErr(CraneErrCode::ERR_RESOURCE_ALREADY_EXIST, ""));
      }
    }
    new_lic_resource = *iter->second;
    is_exist = true;
  }

  for (const auto& _ : clusters) {
    new_lic_resource.cluster_resources.emplace(g_config.CraneClusterName, 0);
  }

  if (g_config.AllLicenseResourcesAbsolute)
    new_lic_resource.flags |= crane::grpc::LicenseResource_Flag_Absolute;

  auto result = CheckAndUpdateFields_(clusters, operators, &new_lic_resource);
  if (!result) return result;

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        if (is_exist)
          g_db_client->UpdateLicenseResource(new_lic_resource);
        else
          g_db_client->InsertLicenseResource(new_lic_resource);
      };

  if (!g_db_client->CommitTransaction(callback))
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_UPDATE_DATABASE, ""));

  if (new_lic_resource.type == crane::grpc::LicenseResource_Type_License) {
    auto cluster_iter =
        new_lic_resource.cluster_resources.find(g_config.CraneClusterName);
    if (cluster_iter != new_lic_resource.cluster_resources.end()) {
      auto license_id =
          std::format("{}@{}", new_lic_resource.name, new_lic_resource.server);
      License lic{.license_id = license_id,
                  .used = 0,
                  .reserved = 0,
                  .remote = true,
                  .last_consumed = new_lic_resource.last_consumed,
                  .last_deficit = 0,
                  .last_update = new_lic_resource.last_update};
      UpdateLicense_(new_lic_resource, cluster_iter->second, &lic);
      m_licenses_map_.Emplace(license_id, std::move(lic));
    }
  }

  m_license_resource_map_[key] =
      std::make_unique<LicenseResourceInDb>(std::move(new_lic_resource));

  return {};
}

CraneExpectedRich<void> LicensesManager::ModifyLicenseResource(
    const std::string& name, const std::string& server,
    const std::vector<std::string>& clusters,
    const std::unordered_map<crane::grpc::LicenseResource_Field, std::string>&
        operators) {
  util::write_lock_guard resource_guard(m_rw_resource_mutex_);

  CRANE_TRACE("Modify license resource: {}@{}...", name, server);

  auto key = std::make_pair(name, server);
  auto iter = m_license_resource_map_.find(key);
  if (iter == m_license_resource_map_.end())
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_RESOURCE_NOT_FOUND, ""));

  LicenseResourceInDb res_lic_resource(*(iter->second));
  auto result = CheckAndUpdateFields_(clusters, operators, &res_lic_resource);
  if (!result) return result;

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateLicenseResource(res_lic_resource);
      };

  if (!g_db_client->CommitTransaction(callback))
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_UPDATE_DATABASE, ""));

  // update license
  if (res_lic_resource.type == crane::grpc::LicenseResource_Type_License) {
    auto cluster_iter =
        res_lic_resource.cluster_resources.find(g_config.CraneClusterName);
    if (cluster_iter != res_lic_resource.cluster_resources.end()) {
      auto licenses_map = m_licenses_map_.GetMapExclusivePtr();
      auto license_id =
          std::format("{}@{}", res_lic_resource.name, res_lic_resource.server);
      auto lic_iter = licenses_map->find(license_id);
      if (lic_iter == licenses_map->end()) {
        CRANE_ERROR(
            "License resource {} not found in license map. now adding...",
            license_id);
        License lic{.license_id = license_id,
                    .used = 0,
                    .reserved = 0,
                    .remote = true,
                    .last_consumed = res_lic_resource.last_consumed,
                    .last_deficit = 0,
                    .last_update = res_lic_resource.last_update};
        UpdateLicense_(res_lic_resource, cluster_iter->second, &lic);
        m_licenses_map_.Emplace(license_id, std::move(lic));
      } else {
        auto lic = lic_iter->second.GetExclusivePtr();
        UpdateLicense_(res_lic_resource, cluster_iter->second, lic.get());
        if (lic->used > lic->total) {
          CRANE_WARN("license {} total decreased", lic->license_id);
        }
      }
    }
  }

  m_license_resource_map_[key] =
      std::make_unique<LicenseResourceInDb>(std::move(res_lic_resource));

  return {};
}

CraneExpectedRich<void> LicensesManager::RemoveLicenseResource(
    const std::string& name, const std::string& server,
    const std::vector<std::string>& clusters) {
  util::write_lock_guard resource_guard(m_rw_resource_mutex_);

  CRANE_TRACE("Remove license resource {}@{}....", name, server);

  auto key = std::make_pair(name, server);
  auto iter = m_license_resource_map_.find(key);
  if (iter == m_license_resource_map_.end())
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_RESOURCE_NOT_FOUND, ""));

  LicenseResourceInDb res_resource(*iter->second);
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
    if (res_resource.allocated < cluster_iter->second) {
      CRANE_ERROR(
          "Allocated license count underflow when removing allocation: "
          "resource {}@{}, cluster '{}', allocated={}, to_remove={}. "
          "This should not happen and may indicate inconsistent resource "
          "state.",
          res_resource.name, res_resource.server, cluster,
          res_resource.allocated, cluster_iter->second);
    } else {
      res_resource.allocated -= cluster_iter->second;
    }
    res_resource.cluster_resources.erase(cluster_iter);
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        if (clusters.empty()) {
          g_db_client->DeleteLicenseResource(name, server);
        } else {
          g_db_client->UpdateLicenseResource(res_resource);
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
        std::make_unique<LicenseResourceInDb>(std::move(res_resource));
  }

  return {};
}

CraneExpectedRich<void> LicensesManager::QueryLicenseResource(
    const std::string& name, const std::string& server,
    const std::vector<std::string>& clusters,
    std::list<LicenseResourceInDb>* res_licenses) {
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

CraneExpectedRich<void> LicensesManager::CheckAndUpdateFields_(
    const std::vector<std::string>& clusters,
    const std::unordered_map<crane::grpc::LicenseResource_Field, std::string>&
        operators,
    LicenseResourceInDb* res_resource) {
  for (const auto& [field, value] : operators) {
    switch (field) {
    case crane::grpc::LicenseResource_Field::LicenseResource_Field_Count:
      try {
        res_resource->total_resource_count = std::stoul(value);
      } catch (std::exception& e) {
        CRANE_TRACE("Failed to parse 'count' from value '{}': {}", value,
                    e.what());
        return std::unexpected(FormatRichErr(CraneErrCode::ERR_INVALID_PARAM,
                                             "Invalid count parameter"));
      }
      break;
    case crane::grpc::LicenseResource_Field::LicenseResource_Field_Allowed:
      if (clusters.empty())
        return std::unexpected(FormatRichErr(CraneErrCode::ERR_INVALID_PARAM,
                                             "No cluster specified"));
      for (const auto& cluster : clusters) {
        std::unordered_map<std::string, uint32_t>::iterator cluster_iter;
        if (cluster == "local") {
          cluster_iter =
              res_resource->cluster_resources.find(g_config.CraneClusterName);
        } else {
          cluster_iter = res_resource->cluster_resources.find(cluster);
        }
        if (cluster_iter == res_resource->cluster_resources.end())
          return std::unexpected(
              FormatRichErr(CraneErrCode::ERR_RESOURCE_NOT_FOUND,
                            "Resources {}@{} not found in cluster {}",
                            res_resource->name, res_resource->server, cluster));
        try {
          res_resource->allocated -= cluster_iter->second;
          cluster_iter->second = std::stoul(value);
          res_resource->allocated += cluster_iter->second;
        } catch (std::exception& e) {
          CRANE_TRACE(
              "Failed to parse 'allowed' for cluster '{}' from value '{}': {}",
              cluster, value, e.what());
          return std::unexpected(FormatRichErr(CraneErrCode::ERR_INVALID_PARAM,
                                               "Invalid allowed parameter"));
        }
      }
      break;
    case crane::grpc::LicenseResource_Field_LastConsumed:
      try {
        res_resource->last_consumed = std::stoul(value);
      } catch (std::exception& e) {
        CRANE_TRACE("Failed to parse 'last_consumed' from value '{}': {}",
                    value, e.what());
        return std::unexpected(
            FormatRichErr(CraneErrCode::ERR_INVALID_PARAM,
                          "Invalid last_consumed parameter"));
      }
      break;
    case crane::grpc::LicenseResource_Field_Description:
      res_resource->description = value;
      break;
    case crane::grpc::LicenseResource_Field_Flags: {
      for (const auto& flag : absl::StrSplit(value, ',')) {
        std::string lower_str =
            absl::AsciiStrToLower(absl::StripAsciiWhitespace(flag));
        if (lower_str == "absolute") {
          res_resource->flags |= crane::grpc::LicenseResource_Flag_Absolute;
        } else if (lower_str == "none") {
          res_resource->flags = crane::grpc::LicenseResource_Flag_None;
          break;
        } else {
          CRANE_DEBUG("Invalid flags parameter: {}", lower_str);
        }
      }
      break;
    }
    case crane::grpc::LicenseResource_Field_ResourceType: {
      std::string lower_str = absl::AsciiStrToLower(value);
      if (lower_str == "license") {
        res_resource->type = crane::grpc::LicenseResource_Type_License;
      } else if (lower_str == "notset") {
        res_resource->type = crane::grpc::LicenseResource_Type_NotSet;
      } else {
        return std::unexpected(FormatRichErr(CraneErrCode::ERR_INVALID_PARAM,
                                             "Invalid type parameter"));
      }
      break;
    }
    case crane::grpc::LicenseResource_Field_ServerType:
      res_resource->server_type = value;
      break;
    default:
      CRANE_ERROR("Unrecognized LicenseResource field: {}",
                  std::to_string(field));
    }
  }

  res_resource->last_update = absl::Now();

  uint32_t allocated = 0;
  if (res_resource->flags & crane::grpc::LicenseResource_Flag_Absolute)
    allocated = res_resource->allocated;
  else {
    if (res_resource->allocated > 100)
      return std::unexpected(FormatRichErr(
          CraneErrCode::ERR_INVALID_PARAM,
          "Allocated resources {}% exceed 100%", res_resource->allocated));
    allocated =
        (res_resource->total_resource_count * res_resource->allocated) / 100;
  }

  if (allocated > res_resource->total_resource_count) {
    CRANE_TRACE(
        "License Resource {}@{}: Allocated resources {} exceed total "
        "available "
        "count {}",
        res_resource->name, res_resource->server, allocated,
        res_resource->total_resource_count);
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_INVALID_PARAM,
                      "Allocated resources {} exceed total available count {}",
                      allocated, res_resource->total_resource_count));
  }

  return {};
}

void LicensesManager::UpdateLicense_(
    const LicenseResourceInDb& license_resource, uint32_t cluster_allowed,
    License* lic) {
  if (license_resource.flags & crane::grpc::LicenseResource_Flag_Absolute)
    lic->total = cluster_allowed;
  else  // when non-absolute, the cluster_allowed is the percentage of the
        // resource.count
    lic->total =
        (license_resource.total_resource_count * cluster_allowed) / 100;
  uint32_t external = 0;
  if (license_resource.total_resource_count < lic->total)
    CRANE_ERROR(
        "allocated more licenses than exist total ({} > {}). this should "
        "not happen.",
        lic->total, license_resource.total_resource_count);
  else
    external = license_resource.total_resource_count - lic->total;
  lic->last_consumed = license_resource.last_consumed;
  if (lic->last_consumed <= (external + lic->used)) {
    /*
     * "Normal" operation - license consumption is below what the
     * local cluster, plus possible use from other clusters,
     * have assigned out. No deficit in this case.
     */
    lic->last_deficit = 0;
  } else {
    /*
     * "Deficit" operation. Someone is using licenses that aren't
     * included in our local tracking, and exceed that available
     * to other clusters. So... we need to adjust our scheduling
     * behavior here to avoid over-allocating licenses.
     */
    lic->last_deficit = lic->last_consumed - external - lic->used;
  }
  lic->last_update = license_resource.last_update;
}

}  // namespace Ctld

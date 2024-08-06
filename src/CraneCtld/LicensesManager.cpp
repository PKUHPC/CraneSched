 /**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

 #include "LicensesManager.h"


namespace Ctld {

int LicensesManager::Init(const std::unordered_map<LicenseId, uint32_t> &lic_id_to_count_map) {
    util::read_lock_guard readLock(rw_mutex_);
    for (auto& [lic_id, count] : lic_id_to_count_map) {
        lic_id_to_lic_map_.insert({lic_id, std::make_unique<License>(License(lic_id, count))});
    }
    return 0;
}

bool LicensesManager::AllocateLicensesToTask(std::vector<std::pair<LicenseId, uint32_t>> &request_lic_vec) {
    
    {
        util::read_lock_guard readLock(rw_mutex_);
        for (auto& [lic_id, count] : request_lic_vec) {
            if (!CheckLicensesCountSufficient_(lic_id, count)) {
                return false;
            }
        }
    }

    {
        util::write_lock_guard writeLock(rw_mutex_);
        for (auto& [lic_id, count] : request_lic_vec) {
            if (!CheckLicensesCountSufficient_(lic_id, count)) {
                return false;
            }
        }
        for (auto& [lic_id, count] : request_lic_vec) {
            DoAllocate_(lic_id, count);
        }
    }
    
    return true;
}

void LicensesManager::GetLicensesInfo(const crane::grpc::QueryLicensesInfoRequest* request, crane::grpc::QueryLicensesInfoReply* response) {
    auto* list = response->mutable_license_info_list();

    if (request->license_name().empty()) {
        util::read_lock_guard readLock(rw_mutex_);
        for(auto& [lic_index, lic_ptr] : lic_id_to_lic_map_) {
            auto *lic_info = list->Add();
            lic_info->set_name(lic_ptr->GetLicenseId());
            lic_info->set_total(lic_ptr->GetTotal());
            lic_info->set_used(lic_ptr->GetUsed());
            lic_info->set_free(lic_ptr->GetFree());
            lic_info->set_reserved(lic_ptr->GetReserved());
        }
    } else {
        util::read_lock_guard readLock(rw_mutex_);
        auto it = lic_id_to_lic_map_.find(request->license_name());
        if (it != lic_id_to_lic_map_.end()) {
            auto *lic_info = list->Add();
            lic_info->set_name(it->second->GetLicenseId());
            lic_info->set_total(it->second->GetTotal());
            lic_info->set_used(it->second->GetUsed());
            lic_info->set_free(it->second->GetFree());
            lic_info->set_reserved(it->second->GetReserved());
        }
    }
}

bool LicensesManager::CheckLicensesCountSufficient_(const LicenseId &license_id, uint32_t count) {
    auto it = lic_id_to_lic_map_.find(license_id);
    if (it == lic_id_to_lic_map_.end()) {
        return false;
    }

    return it->second->GetFree() >= count;
}

void LicensesManager::DoAllocate_(const LicenseId &license_id, uint32_t count) {
    auto it = lic_id_to_lic_map_.find(license_id);
    uint32_t used_count = it->second->GetUsed();
    it->second->SetUsed(used_count - count);
}

} // namespace Craned

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

 #pragma once

 #include "CtldPublicDefs.h"
// Precompiled header comes first!

#include "crane/Lock.h"

namespace Ctld {

class License {
 public:
    License(const std::string& license_id, uint32_t total)
        : license_id_(license_id), total_(total), used_(0), reserved_(0) {}
    ~License() { }

    void SetUsed(uint32_t used) { used_ = used; }
    void SetReserved(uint32_t reserved) { reserved_ = reserved; }

    [[nodiscard]] const std::string& GetLicenseId() const {
        return license_id_;
    }

    [[nodiscard]] const uint32_t GetTotal() const {
        return total_;
    }

    [[nodiscard]] const uint32_t GetReserved() const {
        return reserved_;
    }

    [[nodiscard]] const uint32_t GetUsed() const {
        return used_;
    }

    [[nodiscard]] uint32_t GetFree() const {
        return total_ - used_ - reserved_;
    }

 private:
    LicenseId license_id_; /* license name */
    uint32_t total_;  /* The total number of configured license */
    uint32_t used_;  /* Number of license in use */
    uint32_t reserved_;  /* Number of license reserved for users */
};


using unorderedLicensesMap = absl::flat_hash_map<LicenseId, std::unique_ptr<License>>;

class LicensesManager {
 public:
    int Init(const std::unordered_map<LicenseId, uint32_t> &lic_id_to_count_map);

    bool AllocateLicensesToTask(std::vector<std::pair<LicenseId, uint32_t>> &request_lic_vec);

    bool FreeLicenses(std::vector<std::pair<LicenseId, uint32_t>> &free_lic_vec);

    void GetLicensesInfo(const crane::grpc::QueryLicensesInfoRequest* request, 
                        crane::grpc::QueryLicensesInfoReply* response);

 private:
    bool CheckLicensesCountSufficient_(const LicenseId &license_id, uint32_t count);

    void DoAllocate_(const LicenseId &license_id, uint32_t count);
    
    unorderedLicensesMap lic_id_to_lic_map_ GUARDED_BY(rw_mutex_);
    
    util::rw_mutex rw_mutex_;
};

} // namespace Ctld


inline std::unique_ptr<Ctld::LicensesManager> g_licenses_manager;
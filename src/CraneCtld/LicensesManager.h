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

using unorderedLicensesMap = absl::flat_hash_map<LicenseId, std::unique_ptr<License>>;

class LicensesManager {
 public:
  LicensesManager();

  ~LicensesManager() = default;

  int Init(const std::unordered_map<LicenseId, uint32_t> &lic_id_to_count_map);

  void GetLicensesInfo(const crane::grpc::QueryLicensesInfoRequest* request, 
                        crane::grpc::QueryLicensesInfoReply* response);
   
  bool CheckLicenseCountSufficient(const std::unordered_map<LicenseId, uint32_t> &lic_id_to_count_map);

  result::result<void, std::string> CheckLicensesLegal(const ::google::protobuf::Map<std::string, uint32_t> &lic_id_to_count_map);

  void MallocLicenseResource(const std::unordered_map<LicenseId, uint32_t> &lic_id_to_count_map);

  void FreeLicenseResource(const std::unordered_map<LicenseId, uint32_t> &lic_id_to_count_map);
   
 private:
  unorderedLicensesMap lic_id_to_lic_map_ ABSL_GUARDED_BY(rw_mutex_);
    
  util::rw_mutex rw_mutex_;
};

} // namespace Ctld


inline std::unique_ptr<Ctld::LicensesManager> g_licenses_manager;
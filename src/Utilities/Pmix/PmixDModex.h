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

#ifdef HAVE_PMIX
#  include <pmix_common.h>
#endif

#include <string>

#include "PmixCommon.h"
#include "crane/Lock.h"
#include "crane/PublicHeader.h"

namespace pmix {

// Forward declaration to avoid pulling in PmixConn headers here.
class PmixClient;

// Context passed through the PMIx dmodex callback chain.
// Carries a PmixClient pointer so DModexOpCb can send the response without
// accessing any global variable.
struct DModexCbData {
  uint32_t seq_num;
  CranedId craned_id;
  std::string nspace;
  uint32_t rank;
  PmixClient* pmix_client{nullptr};  // injected, not owned
};

class PmixDModexReqManager {
 public:
#ifdef HAVE_PMIX
  // pmix_client and timeout are injected at construction time so this class
  // never needs to call PmixServer::GetInstance().
  PmixDModexReqManager(const PmixJobInfo& job_info, PmixClient* pmix_client,
                       std::chrono::seconds timeout)
      : m_pmix_job_info_(job_info),
        m_pmix_client_(pmix_client),
        m_timeout_(timeout) {}

  bool PmixDModexGet(const std::string& pmix_namespace, int rank,
                     pmix_modex_cbfunc_t cbfunc, void* cbdata);

  void PmixProcessRequest(uint32_t seq_num, const CranedId& craned_id,
                          const pmix_proc_t& pmix_proc,
                          const std::string& send_nspace);

  void PmixProcessResponse(uint32_t seq_num, const CranedId& craned_id,
                           const std::string& data, pmix_status_t status);

  void CleanupTimeoutRequests();

  void DrainAllRequests();

 private:
  void ResponseWithError_(uint32_t seq_num, const CranedId& craned_id,
                          pmix_status_t status);

  PmixJobInfo m_pmix_job_info_;
  PmixClient* m_pmix_client_{nullptr};  // injected, not owned
  std::chrono::seconds m_timeout_{5};

  struct PmixDModexReq {
    uint32_t seq_num;
    std::chrono::steady_clock::time_point ts;
    pmix_modex_cbfunc_t cb_func;
    void* cb_data;
  };

  uint32_t m_dmdx_seq_num_ = 0;
  util::mutex m_dmodex_mutex_;
  std::list<PmixDModexReq> m_pmix_dmodex_req_list_;
#endif
};

}  // namespace pmix

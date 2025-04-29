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

#include <pmix_common.h>

#include <string>

#include "crane/Lock.h"

namespace pmix {

struct DModexCbData {
  uint32_t seq_num;
  std::string craned_id;
  std::string nspace;
  uint32_t rank;
};

class PmixDModexReqManager {
public:
  PmixDModexReqManager() = default;

  bool PmixDModexGet(const std::string& pmix_namespace, int rank, pmix_modex_cbfunc_t cbfunc, void *cbdata);

  void PmixProcessRequest(uint32_t seq_num, CranedId craned_id,
                          const pmix_proc_t& pmix_proc,
                          const std::string& send_nspace);

  void PmixProcessResponse(uint32_t seq_num, const CranedId& craned_id, const std::string& data, int status);

private:
  void ResponseWithError_(uint32_t seq_num, const CranedId& craned_id,
                         const std::string& sender_ns, int status);

   struct PmixDModexReq {
     uint32_t m_seq_num_;
     time_t m_ts_;
     pmix_modex_cbfunc_t m_cb_func_;
     void* m_cb_data_;
   };

  uint32_t dmdx_seq_num_;
  util::mutex m_dmodex_mutex_;
  std::list<PmixDModexReq> m_pmix_dmodex_req_list_;
};

} // namespace pmix

inline std::unique_ptr<pmix::PmixDModexReqManager> g_dmodex_req_manager;
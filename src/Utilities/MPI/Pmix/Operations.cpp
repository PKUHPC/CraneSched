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

#include "Server.h"
#include "crane/Logger.h"
namespace pmix {
#ifdef HAVE_PMIX
pmix_status_t PmixServer::Abort(const pmix_proc_t* /*pmix_proc*/,
                                void* /*server_object*/, int status,
                                const char msg[], pmix_proc_t /*pmix_procs*/[],
                                size_t /*nprocs*/, pmix_op_cbfunc_t cbfunc,
                                void* cbdata) {
  CRANE_WARN("PMIx Abort invoked: status={}, msg={}", status, msg);
  // Complete pending collectives before terminating the step so PMIx shutdown
  // cannot wait for contributions from ranks that have already exited.
  if (m_collective_manager_) m_collective_manager_->AbortAll();
  if (m_modex_manager_) m_modex_manager_->DrainAllRequests();
  if (m_step_client_) m_step_client_->TerminateSteps();
  if (cbfunc) cbfunc(PMIX_SUCCESS, cbdata);
  return PMIX_SUCCESS;
}

pmix_status_t PmixServer::FenceNb(const pmix_proc_t procs_v2[], size_t nprocs,
                                  const pmix_info_t info[], size_t ninfo,
                                  char* data, size_t ndata,
                                  pmix_modex_cbfunc_t cbfunc, void* cbdata) {
  CRANE_DEBUG("FenceNb: nprocs={}, ndata={}", nprocs, ndata);

  std::vector<pmix_proc_t> procs;
  procs.reserve(nprocs);
  bool collect = false;

  for (size_t i = 0; i < nprocs; i++) procs.emplace_back(procs_v2[i]);

  if (info != nullptr) {
    for (size_t i = 0; i < ninfo; i++) {
      if (strncmp(info[i].key, PMIX_COLLECT_DATA, PMIX_MAX_KEYLEN) == 0) {
        collect = true;
        break;
      }
    }
  }

  if (data == nullptr && ndata > 0) {
    CRANE_ERROR("FenceNb: non-zero ndata={} but null data pointer", ndata);
    cbfunc(PMIX_ERROR, nullptr, 0, cbdata, nullptr, nullptr);
    return PMIX_ERROR;
  }

  auto* srv = this;
  CollectiveType type = ParseCollectiveType(srv->GetFenceType());
  if (type == CollectiveType::kInvalid) {
    type =
        (collect && ndata > 0) ? CollectiveType::kRing : CollectiveType::kTree;
  }

  CRANE_TRACE("FenceNb: selected collective type={}", ToString(type));

  auto coll = srv->GetCollectiveManager()->GetCollective(type, procs);
  if (!coll) {
    CRANE_ERROR("FenceNb: failed to obtain collective object for type={}",
                ToString(type));
    cbfunc(PMIX_ERROR, nullptr, 0, cbdata, nullptr, nullptr);
    return PMIX_ERROR;
  }

  std::string local_data;
  if (data != nullptr && ndata > 0) local_data.assign(data, ndata);
  if (!coll->Contribute(local_data, cbfunc, cbdata)) {
    CRANE_ERROR("FenceNb: failed to contribute local data to collective");
    cbfunc(PMIX_ERROR, nullptr, 0, cbdata, nullptr, nullptr);
    return PMIX_ERROR;
  }

  return PMIX_SUCCESS;
}

pmix_status_t PmixServer::DModex(const pmix_proc_t* proc,
                                 const pmix_info_t* /*info*/, size_t /*ninfo*/,
                                 pmix_modex_cbfunc_t cbfunc, void* cbdata) {
  if (!proc || !cbfunc) return PMIX_ERR_BAD_PARAM;
  if (!m_modex_manager_) return PMIX_ERR_INIT;
  CRANE_DEBUG("DModex: proc=[{}:{}]", proc->nspace, proc->rank);
  if (!m_modex_manager_->Get(proc->nspace, static_cast<int>(proc->rank), cbfunc,
                             cbdata)) {
    CRANE_ERROR("DModex: Get failed for [{}:{}]", proc->nspace, proc->rank);
    // A synchronous error leaves completion to PMIx. Only accepted requests
    // transfer callback ownership to ModexManager.
    return PMIX_ERROR;
  }
  return PMIX_SUCCESS;
}
#endif
}  // namespace pmix

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

#include "PmixDModex.h"

#include "CranedClient.h"
#include "Pmix.h"
#include "absl/synchronization/blocking_counter.h"
#include "crane/Logger.h"
#include "protos/Crane.pb.h"

namespace pmix {

struct DModexCbData {
  absl::BlockingCounter bc;
  std::string data;
};

namespace {

void DmodexOpCb(pmix_status_t status, char *data, size_t sz, void *cbdata) {
  auto *dmo_modex_cb_data = reinterpret_cast<DModexCbData *>(cbdata);
  dmo_modex_cb_data->bc.DecrementCount();
  dmo_modex_cb_data->data = std::string(data, sz);
  CRANE_DEBUG("dmodex callback is called with status={}: {}", status,
              PMIx_Error_string(status));
}

}  // namespace

bool PmixDModexGet(const std::string &pmix_namespace, int rank,
                   pmix_modex_cbfunc_t cbfunc, void *cbdata) {
  crane::grpc::PmixDModexReq request{};

  auto *pmix_proc = request.mutable_pmix_proc();
  pmix_proc->set_nspace(pmix_namespace);
  pmix_proc->set_rank(rank);

  request.set_local_namespace(pmix_namespace);

  // Find the node host corresponding to the nspace-rank.
  auto pmix_nspace = g_pmix_server->PmixNamespaceGet(pmix_namespace);
  if (!pmix_nspace) {
    CRANE_ERROR("Cannot find pmix namespace {}", pmix_namespace);
    return false;
  }

  CranedId craned_id = pmix_nspace->m_hostlist_[pmix_nspace->m_task_map_[rank]];

  auto result = g_craned_client->GetCranedStub(craned_id)->PmixDModex(std::move(request));
  if (!result) {
    CRANE_ERROR("Cannot send direct modex request to {}, status: {}", craned_id,
                PMIx_Error_string(result.error()));
    PmixLibModexInvoke(cbfunc, result.error(), nullptr, 0, cbdata, nullptr,
                       nullptr);
  } else {
    std::string data = result.value();
    PmixLibModexInvoke(cbfunc, PMIX_SUCCESS, data.c_str(), data.size(), cbdata,
                       nullptr, nullptr);
  }

  return true;
}

std::expected<std::string, int> PmixProcessRequest(
    const pmix_proc_t &pmix_proc, const std::string &send_nspace, bool status) {
  auto pmix_nspace = g_pmix_server->PmixNamespaceGet(pmix_proc.nspace);
  if (!pmix_nspace) {
    // CRANE_ERROR("Bad request from {}: asked for nspace = {}, mine is {}", );
    return std::unexpected(PMIX_ERR_INVALID_NAMESPACE);
  }

  if (pmix_nspace->m_task_num_ <= pmix_proc.rank) {
    return std::unexpected(PMIX_ERR_BAD_PARAM);
  }

  std::string result = "";

  {
    DModexCbData dmo_modex_cb_data{.bc = absl::BlockingCounter(1), .data = ""};
    auto rc =
        PMIx_server_dmodex_request(&pmix_proc, DmodexOpCb, &dmo_modex_cb_data);
    if (rc != PMIX_SUCCESS) {
      CRANE_ERROR("Error: PMIx_server_dmodex_request. {}",
                  PMIx_Error_string(rc));
      return std::unexpected(rc);
    }
    dmo_modex_cb_data.bc.Wait();
    result = dmo_modex_cb_data.data;
  }

  return result;
}

}  // namespace pmix
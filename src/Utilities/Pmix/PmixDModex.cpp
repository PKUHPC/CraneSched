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
#include "PmixCommon.h"
#include "absl/synchronization/blocking_counter.h"
#include "crane/Logger.h"
#include "protos/Crane.pb.h"

namespace pmix {

namespace {

void DModexOpCb(pmix_status_t status, char *data, size_t sz, void *cbdata) {
  auto *dmo_modex_cb_data = reinterpret_cast<DModexCbData*>(cbdata);

  CRANE_DEBUG("DmodexFn is called");

  crane::grpc::pmix::PmixDModexResponseReq request{};
  request.set_seq_num(dmo_modex_cb_data->seq_num);
  request.set_data(data, sz);
  request.set_status(PMIX_SUCCESS);

  auto context = std::make_shared<grpc::ClientContext>();
  auto reply = std::make_shared<crane::grpc::pmix::PmixDModexResponseReply>();
  auto stub = g_pmix_server->GetPmixClient()->GetPmixStub(dmo_modex_cb_data->craned_id);
  if (!stub) {
    CRANE_ERROR("Cannot send direct modex response to {}", dmo_modex_cb_data->craned_id);
    delete dmo_modex_cb_data;
    return ;
  }

  stub->PmixDModexResponse(
      context.get(), request, reply.get(),
      [context, reply,
       craned_id = dmo_modex_cb_data->craned_id](const grpc::Status& status) {
        if (!status.ok()) {
          /* not much we can do here. Caller will react by timeout */
          CRANE_ERROR("Cannot send direct modex response to {}", craned_id);
        }
      });

  delete dmo_modex_cb_data;
}

}  // namespace

bool PmixDModexReqManager::PmixDModexGet(const std::string &pmix_namespace,
                                         int rank, pmix_modex_cbfunc_t cbfunc,
                                         void *cbdata) {
  // Find the node host corresponding to the nspace-rank.
  if (g_pmix_server->GetNSpace() != pmix_namespace) {
    CRANE_ERROR("Cannot find pmix namespace {}", pmix_namespace);
    return false;
  }

  if (rank >= g_pmix_server->GetTaskMap().size()) {
    CRANE_ERROR("The rank is out of the range of the task_map.");
    return false;
  }

  CranedId craned_id = g_pmix_server->GetNodeList()[g_pmix_server->GetTaskMap()[rank]];

  crane::grpc::pmix::PmixDModexRequestReq request{};

  {
    util::lock_guard lock(m_dmodex_mutex_);
    m_pmix_dmodex_req_list_.emplace_back(
        PmixDModexReq{.seq_num = m_dmdx_seq_num_,
                      .ts = time(nullptr),
                      .cb_func = cbfunc,
                      .cb_data = cbdata});
    request.set_seq_num(m_dmdx_seq_num_++);
  }

  auto *pmix_proc = request.mutable_pmix_proc();
  pmix_proc->set_nspace(pmix_namespace);
  pmix_proc->set_rank(rank);

  request.set_local_namespace(pmix_namespace);
  request.set_craned_id(g_pmix_server->GetHostname());

  auto context = std::make_shared<grpc::ClientContext>();
  auto reply = std::make_shared<crane::grpc::pmix::PmixDModexRequestReply>();
  auto stub = g_pmix_server->GetPmixClient()->GetPmixStub(craned_id);
  if (!stub) {
    CRANE_ERROR("[#{}] Stub {} is not get, pmixDModex rpc failed.", pmix_namespace, craned_id);
    PmixLibModexInvoke(cbfunc, PMIX_ERROR, nullptr, 0, cbdata, nullptr,
                             nullptr);
    return false;
  }

  CRANE_TRACE("PmixDModexRequest to {} for nspace={}, rank={}", craned_id, pmix_namespace, rank);

  stub->PmixDModexRequest(
      context.get(), request, reply.get(),
      [context, reply, cbfunc, cbdata](const grpc::Status& status) {
        if (!status.ok()) {
          CRANE_ERROR("PmixDModex rpc failed.");
          // TODO: Is it needed?
          PmixLibModexInvoke(cbfunc, PMIX_ERROR, nullptr, 0, cbdata, nullptr,
                             nullptr);
        }
      });

  return true;
}

void PmixDModexReqManager::PmixProcessRequest(uint32_t seq_num,
                                              const CranedId &craned_id,
                                              const pmix_proc_t &pmix_proc,
                                              const std::string &send_nspace) {
  if (pmix_proc.nspace != g_pmix_server->GetNSpace()) {
    CRANE_ERROR("Bad request from {}: asked for nspace = {}", craned_id, send_nspace);
    ResponseWithError_(seq_num, craned_id, PMIX_ERR_INVALID_NAMESPACE);
    return;
  }

  if (g_pmix_server->GetTaskNum() <= pmix_proc.rank) {
    CRANE_ERROR("Bad request from {}: nspace {} has only {} ranks, asked for {}", craned_id, pmix_proc.nspace, g_pmix_server->GetTaskNum(), pmix_proc.rank);
    ResponseWithError_(seq_num, craned_id, PMIX_ERR_BAD_PARAM);
    return ;
  }

  CRANE_TRACE("PmixProcessRequest from {} for nspace={}, rank={}", craned_id, pmix_proc.nspace, pmix_proc.rank);

  // DModexCbData *dmo_modex_cb_data = new DModexCbData{.seq_num = seq_num, .craned_id = craned_id, .nspace = pmix_proc.nspace, .rank = pmix_proc.rank};
  auto dmo_modex_cb_data = std::make_unique<DModexCbData>();
  dmo_modex_cb_data->seq_num = seq_num;
  dmo_modex_cb_data->craned_id = craned_id;
  dmo_modex_cb_data->nspace = pmix_proc.nspace;
  dmo_modex_cb_data->rank = pmix_proc.rank;
  auto rc =
        PMIx_server_dmodex_request(&pmix_proc, DModexOpCb, dmo_modex_cb_data.release());
  if (rc != PMIX_SUCCESS) {
    CRANE_ERROR("Error: PMIx_server_dmodex_request. {}",
                  PMIx_Error_string(rc));
    ResponseWithError_(seq_num, craned_id, rc);
  }
}

void PmixDModexReqManager::PmixProcessResponse(uint32_t seq_num, const CranedId& craned_id, const std::string& data, int status) {

  PmixDModexReq  pmix_dmodex_req{};

  {
    util::lock_guard lock(m_dmodex_mutex_);
    auto it = std::ranges::find_if(
    m_pmix_dmodex_req_list_,
    [seq_num](const auto& req) {
        return req.seq_num == seq_num;
    });

    if (it != m_pmix_dmodex_req_list_.end()) {
      pmix_dmodex_req = *it;
      m_pmix_dmodex_req_list_.erase(it);
    } else {
      CRANE_ERROR("Received DMDX response with bad seq_num={} from {}!", seq_num, craned_id);
      return;
    }
  }

  CRANE_TRACE("PmixProcessResponse from {} for seq_num={}", craned_id, seq_num);

  PmixLibModexInvoke(pmix_dmodex_req.cb_func, status, data.data(), data.size(), pmix_dmodex_req.cb_data, nullptr, nullptr);
}

void PmixDModexReqManager::ResponseWithError_(uint32_t seq_num, const std::string& craned_id, int status) {
  crane::grpc::pmix::PmixDModexResponseReq request{};

  request.set_status(status);
  request.set_seq_num(seq_num);

  auto context = std::make_shared<grpc::ClientContext>();
  auto reply = std::make_shared<crane::grpc::pmix::PmixDModexResponseReply>();
  auto stub = g_pmix_server->GetPmixClient()->GetPmixStub(craned_id);
  if (!stub) {
    CRANE_ERROR("Cannot send direct modex error response to {}",
                      craned_id);
    return ;
  }
  stub->PmixDModexResponse(
      context.get(), request, reply.get(),
      [context, reply, craned_id](const grpc::Status& status) {
        if (!status.ok()) {
          CRANE_ERROR("Cannot send direct modex error response to {}",
                      craned_id);
        }
      });
}

void PmixDModexReqManager::CleanupTimeoutRequests() {
  time_t now = time(nullptr);

  {
    util::lock_guard lock(m_dmodex_mutex_);
    auto it = m_pmix_dmodex_req_list_.begin();
    while (it != m_pmix_dmodex_req_list_.end()) {
      if (now - it->ts > g_pmix_server->GetTimeout()) {
        PmixLibModexInvoke(it->cb_func, PMIX_ERR_TIMEOUT, nullptr, 0, it->cb_data, nullptr, nullptr);
        it = m_pmix_dmodex_req_list_.erase(it);
      } else {
        ++it;
      }
    }
  }
}

}  // namespace pmix
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

#include "PmixUcxServer.h"

#include "Pmix.h"

namespace pmix {

void PmixUcxServiceImpl::SendPmixRingMsg(const PmixUcxReq& req) {
  crane::grpc::pmix::SendPmixRingMsgReq request;
  if (!request.ParseFromString(req.data)) return;

  std::vector<pmix_proc_t> procs;

  for (const auto& proc : request.pmix_procs()) {
    pmix_proc_t tmp_proc;
    auto& nspace_str = proc.nspace();
    snprintf(tmp_proc.nspace, sizeof(tmp_proc.nspace), "%s",
             nspace_str.c_str());
    tmp_proc.rank = proc.rank();
    procs.emplace_back(tmp_proc);
  }

  std::shared_ptr<Coll> coll =
      g_pmix_state->PmixStateCollGet(CollType::FENCE_RING, procs, procs.size());

  if (!coll) {
    return;
  }

  coll->ProcessRingRequest(request.pmix_ring_msg_hdr(), request.msg());
}

void PmixUcxServiceImpl::PmixTreeUpwardForward(const PmixUcxReq& req) {
  crane::grpc::pmix::PmixTreeUpwardForwardReq request;

  if (!request.ParseFromString(req.data)) return ;

  std::vector<pmix_proc_t> procs;
  for (const auto &proc : request.pmix_procs()) {
    pmix_proc_t tmp_proc;
    auto& nspace_str = proc.nspace();
    snprintf(tmp_proc.nspace, sizeof(tmp_proc.nspace), "%s", nspace_str.c_str());
    tmp_proc.rank = proc.rank();
    procs.emplace_back(tmp_proc);
  }

  std::shared_ptr<Coll> coll =
      g_pmix_state->PmixStateCollGet(
          CollType::FENCE_TREE, procs, procs.size());

  if (!coll) {
    return;
  }

  coll->PmixCollTreeChild(request.peer_host(), request.seq(), request.msg());
}

void PmixUcxServiceImpl::PmixTreeDownwardForward(const PmixUcxReq& req) {
  crane::grpc::pmix::PmixTreeDownwardForwardReq request;
  if (!request.ParseFromString(req.data)) return ;

  std::vector<pmix_proc_t> procs;
  for (const auto &proc : request.pmix_procs()) {
    pmix_proc_t tmp_proc;
    auto& nspace_str = proc.nspace();
    snprintf(tmp_proc.nspace, sizeof(tmp_proc.nspace), "%s", nspace_str.c_str());
    tmp_proc.rank = proc.rank();
    procs.emplace_back(tmp_proc);
  }

  std::shared_ptr<Coll> coll =
    g_pmix_state->PmixStateCollGet(
       CollType::FENCE_TREE, procs, procs.size());

  if (!coll) {
    return ;
  }

  coll->PmixCollTreeParent(request.peer_host(), request.seq(), request.msg());
}

void PmixUcxServiceImpl::PmixDModexRequest(const PmixUcxReq& req) {
  crane::grpc::pmix::PmixDModexRequestReq request;

  if (!request.ParseFromString(req.data)) return;

  pmix_proc_t proc;
  auto& nspace_str = request.pmix_proc().nspace();
  snprintf(proc.nspace, sizeof(proc.nspace), "%s", nspace_str.c_str());
  proc.rank = request.pmix_proc().rank();

  g_dmodex_req_manager->PmixProcessRequest(
      request.seq_num(), request.craned_id(), proc, request.local_namespace());
}

void PmixUcxServiceImpl::PmixDModexResponse(const PmixUcxReq& req) {
  crane::grpc::pmix::PmixDModexResponseReq request;
  if (!request.ParseFromString(req.data)) return ;

  g_dmodex_req_manager->PmixProcessResponse(
      request.seq_num(), request.craned_id(), request.data(), request.status());
}

bool PmixUcxServer::Init(const Config& config) {
  m_service_impl_ = std::make_unique<PmixUcxServiceImpl>();

#if UCP_API_VERSION < UCP_VERSION(1, 5)
  setenv("UCX_MEM_MMAP_RELOC", "no", 1);
#endif
  setenv("UCX_MEM_MALLOC_HOOKS", "no", 1);
  setenv("UCX_MEM_MALLOC_RELOC", "no", 1);
  setenv("UCX_MEM_EVENTS", "no", 1);

  ucp_config_t* ucp_config;
  // TODO: modify CRANE?
  auto status = ucp_config_read("SLURM", nullptr, &ucp_config);
  if (status != UCS_OK) {
    CRANE_ERROR("fail to read UCX config: {}", ucs_status_string(status));
    return false;
  }

  ucp_params_t ucp_params = {};
  ucp_params.features = UCP_FEATURE_TAG | UCP_FEATURE_WAKEUP;
  ucp_params.request_size = sizeof(PmixUcxReq);
  // ucp_params.request_init    = request_init;
  ucp_params.request_cleanup = NULL;
  ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES |
                          UCP_PARAM_FIELD_REQUEST_SIZE |
                          // UCP_PARAM_FIELD_REQUEST_INIT |
                          UCP_PARAM_FIELD_REQUEST_CLEANUP;

  status = ucp_init(&ucp_params, ucp_config, &m_ucp_context_);
  ucp_config_release(ucp_config);
  if (status != UCS_OK) {
    CRANE_ERROR("Fail to init UCX: %s", ucs_status_string(status));
    return false;
  }

  ucp_worker_params_t worker_params = {};
  worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
  worker_params.thread_mode = UCS_THREAD_MODE_MULTI;

  status = ucp_worker_create(m_ucp_context_, &worker_params, &m_ucp_worker_);
  if (status != UCS_OK) {
    CRANE_ERROR("Fail to create UCX worker: %s", ucs_status_string(status));
    goto err_worker;
  }

  status = ucp_worker_get_address(m_ucp_worker_, &m_ucx_addr_, &m_ucx_alen_);
  if (status != UCS_OK) {
    CRANE_ERROR("Fail to get UCX address: %s", ucs_status_string(status));
    goto err_addr;
  }

  status = ucp_worker_get_efd(m_ucp_worker_, &m_server_fd_);
  if (status != UCS_OK) {
    CRANE_ERROR("Fail to get UCX epoll fd: %s", ucs_status_string(status));
    goto err_efd;
  }

  std::string addr_str(reinterpret_cast<const char*>(m_ucx_addr_), m_ucx_alen_);

  std::thread([addr_str]() {
    g_pmix_server->GetCranedClient()->BroadcastPmixPort(addr_str);
  }).detach();

  m_ucx_process_req_async_handle_ =
      g_pmix_server->GetUvwLoop()->resource<uvw::async_handle>();
  m_ucx_process_req_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanUcxProcessReqQueueCb_();
      });

  return true;

err_efd:
  ucp_worker_release_address(m_ucp_worker_, m_ucx_addr_);
err_addr:
  ucp_worker_destroy(m_ucp_worker_);
err_worker:
  ucp_cleanup(m_ucp_context_);
  return false;
}

void PmixUcxServer::OnUcxReadable() {
  ucs_status_t status = UCS_ERR_BUSY;

  do {
    while (UcxProgress_()) {}

    // TODO: 不长时间占用loop
    if (!m_pending_reqs_.empty()) continue;

    {
      util::lock_guard lock_guard(m_mutex_);
      status = ucp_worker_arm(m_ucp_worker_);
    }

  } while (status == UCS_ERR_BUSY);
}

bool PmixUcxServer::UcxProgress_() {
  util::lock_guard lock_guard(m_mutex_);

  bool events_observed = false;
  bool new_msg = false;
  ucp_worker_progress(m_ucp_worker_);
  ucp_tag_recv_info_t info_tag;

  while (true) {
    ucp_tag_message_h msg_tag =
        ucp_tag_probe_nb(m_ucp_worker_, 1, 0, 1, &info_tag);
    if (msg_tag == nullptr) break;

    events_observed = true;
    ;
    std::string msg(info_tag.length, '\0');
    PmixUcxReq* req = static_cast<PmixUcxReq*>(
        ucp_tag_msg_recv_nb(m_ucp_worker_, msg.data(), info_tag.length,
                            ucp_dt_make_contig(1), msg_tag, RecvHandle_));
    if (UCS_PTR_IS_ERR(req)) {
      CRANE_ERROR("ucp_tag_msg_recv_nb failed: %s",
                  ucs_status_string(UCS_PTR_STATUS(req)));
      continue;
    }
    new_msg = true;
    req->data = std::move(msg);
    if (PmixUcxStatus::ACTIVE == req->status.load()) {
      m_pending_reqs_.emplace_back(req);
    } else {
      m_ucx_process_req_queue_.enqueue(std::shared_ptr<PmixUcxReq>(req));
      m_ucx_process_req_async_handle_->send();
    }
  }

  if (!new_msg && m_pending_reqs_.empty()) return events_observed;

  for (const auto req : m_pending_reqs_) {
    if (req->status == PmixUcxStatus::ACTIVE) continue;
    m_ucx_process_req_queue_.enqueue(req);
    m_ucx_process_req_async_handle_->send();
    events_observed = true;
  }

  return events_observed;
}

void PmixUcxServer::RecvHandle_(void* request, ucs_status_t status,
                                ucp_tag_recv_info_t* info) {
  PmixUcxReq* req = (PmixUcxReq*)request;
  if (UCS_OK == status) {
    req->status = PmixUcxStatus::COMPLETE;
  } else {
    CRANE_ERROR("UCX send request failed: %s", ucs_status_string(status));
    req->status = PmixUcxStatus::FAILED;
  }
}

void PmixUcxServer::EvCleanUcxProcessReqQueueCb_() {
  std::shared_ptr<PmixUcxReq> req;
  while (m_ucx_process_req_queue_.try_dequeue(req)) {
    switch (req->type) {
    case PmixUcxMsgType::PMIX_UCX_SEND_PMIX_RING_MSG:
      m_service_impl_->SendPmixRingMsg(*req);
    case PmixUcxMsgType::PMIX_UCX_DMDEX_REQUEST:
      m_service_impl_->PmixDModexRequest(*req);
      break;
    case PmixUcxMsgType::PMIX_UCX_DMDEX_RESPONSE:
      m_service_impl_->PmixDModexResponse(*req);
      break;
    case PmixUcxMsgType::PMIX_UCX_TREE_DOWNWARD_FORWARD:
      m_service_impl_->PmixTreeDownwardForward(*req);
      break;
    case PmixUcxMsgType::PMIX_UCX_TREE_UPWARD_FORWARD:
      m_service_impl_->PmixTreeUpwardForward(*req);
      break;
    default:
      CRANE_ERROR("Unsupported PMIx UCX message type: {}", req->type);
    }
  }
}

}  // namespace pmix
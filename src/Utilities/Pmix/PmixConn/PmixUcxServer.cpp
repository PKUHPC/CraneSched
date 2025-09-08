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
#include "PmixUcxClient.h"

namespace pmix {

void PmixUcxServiceImpl::SendPmixRingMsg(const std::string& req_data) {
  CRANE_TRACE("PmixUcxServiceImpl::SendPmixRingMsg is called");
  crane::grpc::pmix::SendPmixRingMsgReq request;
  if (!request.ParseFromString(req_data)) {
    CRANE_ERROR("Failed to parse SendPmixRingMsgReq from string");
     return;
  }

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

void PmixUcxServiceImpl::PmixTreeUpwardForward(const std::string& req_data) {
  crane::grpc::pmix::PmixTreeUpwardForwardReq request;

  if (!request.ParseFromString(req_data)) return ;

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

void PmixUcxServiceImpl::PmixTreeDownwardForward(const std::string& req_data) {
  crane::grpc::pmix::PmixTreeDownwardForwardReq request;
  if (!request.ParseFromString(req_data)) return ;

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

void PmixUcxServiceImpl::PmixDModexRequest(const std::string& req_data) {
  crane::grpc::pmix::PmixDModexRequestReq request;

  if (!request.ParseFromString(req_data)) return;

  pmix_proc_t proc;
  auto& nspace_str = request.pmix_proc().nspace();
  snprintf(proc.nspace, sizeof(proc.nspace), "%s", nspace_str.c_str());
  proc.rank = request.pmix_proc().rank();

  g_dmodex_req_manager->PmixProcessRequest(
      request.seq_num(), request.craned_id(), proc, request.local_namespace());
}

void PmixUcxServiceImpl::PmixDModexResponse(const std::string& req_data) {
  crane::grpc::pmix::PmixDModexResponseReq request;
  if (!request.ParseFromString(req_data)) return ;

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
  // ucp_params.request_size = sizeof(PmixUcxReq);
  // ucp_params.request_init    = request_init;
  ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES;

  status = ucp_init(&ucp_params, ucp_config, &m_ucp_context_);
  ucp_config_release(ucp_config);
  if (status != UCS_OK) {
    CRANE_ERROR("Fail to init UCX: %s", ucs_status_string(status));
    return false;
  }

  std::string addr_str;
  PmixUcxClient* ucx_client;

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

  addr_str = std::string(reinterpret_cast<const char*>(m_ucx_addr_), m_ucx_alen_);

  ucx_client = dynamic_cast<PmixUcxClient*>(g_pmix_server->GetPmixClient());
  ucx_client->InitUcxWorker(m_mutex_, m_ucp_worker_);

  std::thread([addr_str]() {
    g_pmix_server->GetCranedClient()->BroadcastPmixPort(addr_str);
  }).detach();
  m_poll_ = g_pmix_server->GetUvwLoop()->resource<uvw::poll_handle>(m_server_fd_);
  m_poll_->on<uvw::poll_event>([this](const uvw::poll_event&, uvw::poll_handle&) {
    OnUcxReadable_();
  });

  m_ucx_process_req_async_handle_ =
      g_pmix_server->GetUvwLoop()->resource<uvw::async_handle>();
  m_ucx_process_req_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanUcxProcessReqQueueCb_();
      });
      
  RegisterReceivesAllTypes_();

  {
    util::lock_guard g(m_mutex_);
    auto s = ucp_worker_arm(m_ucp_worker_);
    if (s == UCS_ERR_BUSY) {
      while (ucp_worker_progress(m_ucp_worker_) > 0) {}
      (void)ucp_worker_arm(m_ucp_worker_);
    }
  }
  m_poll_->start(uvw::details::uvw_poll_event::READABLE);

  return true;

err_efd:
  ucp_worker_release_address(m_ucp_worker_, m_ucx_addr_);
err_addr:
  ucp_worker_destroy(m_ucp_worker_);
err_worker:
  ucp_cleanup(m_ucp_context_);
  return false;
}

void PmixUcxServer::OnUcxReadable_() {
  ucs_status_t status = UCS_ERR_BUSY;
  do {
    {
      util::lock_guard g(m_mutex_);
      while (ucp_worker_progress(m_ucp_worker_) > 0) {}
      status = ucp_worker_arm(m_ucp_worker_);
    }
  } while (status == UCS_ERR_BUSY);
}

void PmixUcxServer::RegisterReceivesAllTypes_() {
  RegisterReceivesForType_(PmixUcxMsgType::PMIX_UCX_TREE_UPWARD_FORWARD,   kInflightPerType);
  RegisterReceivesForType_(PmixUcxMsgType::PMIX_UCX_TREE_DOWNWARD_FORWARD, kInflightPerType);
  RegisterReceivesForType_(PmixUcxMsgType::PMIX_UCX_DMDEX_REQUEST,         kInflightPerType);
  RegisterReceivesForType_(PmixUcxMsgType::PMIX_UCX_DMDEX_RESPONSE,        kInflightPerType);
  RegisterReceivesForType_(PmixUcxMsgType::PMIX_UCX_SEND_PMIX_RING_MSG,    kInflightPerType);
}

void PmixUcxServer::RegisterReceivesForType_(PmixUcxMsgType type, int cnt) {
  const ucp_tag_t tag_value = (static_cast<uint64_t>(type) << kTagTypeShift);
  const ucp_tag_t tag_mask  = kTagTypeMask;

  for (int i = 0; i < cnt; ++i) {
    auto* req = new PmixUcxReq();
    req->self = this;
    req->type = type;
    req->data.resize(kRecvMaxBytes);
    ucp_request_param_t param{};
    param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
    param.user_data = req;
    param.cb.recv = RecvHandle_;

    ucp_tag_recv_nbx(
      m_ucp_worker_,
      reinterpret_cast<void*>(req->data.data()), req->data.size(),
      tag_value, tag_mask,
      &param
    );
  }
}

void PmixUcxServer::RecvHandle_(void* request, ucs_status_t status,const ucp_tag_recv_info_t* info,
                                void* user_data) {
  CRANE_TRACE("ucx callback recv handle is called");
  auto* req = static_cast<PmixUcxReq*>(user_data);
  PmixUcxServer* self = req->self;
  auto type = req->type;
  if (UCS_OK == status && info) {
    req->data.resize(info->length);
    self->m_ucx_process_req_queue_.enqueue(req);
    self->m_ucx_process_req_async_handle_->send();
  } else {
    CRANE_ERROR("UCX send request failed: %s", ucs_status_string(status));
  }
}

void PmixUcxServer::EvCleanUcxProcessReqQueueCb_() {
  PmixUcxReq* req;

  while (m_ucx_process_req_queue_.try_dequeue(req)) {
    std::string data(req->data.data(), req->data.size());
    switch (req->type) {
    case PmixUcxMsgType::PMIX_UCX_SEND_PMIX_RING_MSG:
      m_service_impl_->SendPmixRingMsg(data);
      break;
    case PmixUcxMsgType::PMIX_UCX_DMDEX_REQUEST:
      m_service_impl_->PmixDModexRequest(data);
      break;
    case PmixUcxMsgType::PMIX_UCX_DMDEX_RESPONSE:
      m_service_impl_->PmixDModexResponse(data);
      break;
    case PmixUcxMsgType::PMIX_UCX_TREE_DOWNWARD_FORWARD:
      m_service_impl_->PmixTreeDownwardForward(data);
      break;
    case PmixUcxMsgType::PMIX_UCX_TREE_UPWARD_FORWARD:
      m_service_impl_->PmixTreeUpwardForward(data);
      break;
    }
    delete req;
  }
}

}  // namespace pmix
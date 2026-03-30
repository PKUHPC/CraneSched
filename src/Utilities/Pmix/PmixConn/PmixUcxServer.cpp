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
#include "crane/Logger.h"

namespace pmix {

#ifdef HAVE_UCX

/*static*/
PmixUcxMsgType PmixUcxServer::TagToType_(uint64_t tag) {
  return static_cast<PmixUcxMsgType>((tag & kTagTypeMask) >> kTagTypeShift);
}

static void FillProcs(
    const google::protobuf::RepeatedPtrField<crane::grpc::PmixProc>& pb,
    std::vector<pmix_proc_t>& out) {
  out.reserve(static_cast<size_t>(pb.size()));
  for (const auto& p : pb) {
    pmix_proc_t tmp;
    snprintf(tmp.nspace, sizeof(tmp.nspace), "%s", p.nspace().c_str());
    tmp.rank = p.rank();
    out.push_back(tmp);
  }
}

// ── PmixUcxServiceImpl
// ────────────────────────────────────────────────────────

void PmixUcxServiceImpl::SendPmixRingMsg(const std::string& data) {
  crane::grpc::pmix::SendPmixRingMsgReq req;
  if (!req.ParseFromString(data)) {
    CRANE_ERROR("SendPmixRingMsg: ParseFromString failed");
    return;
  }
  std::vector<pmix_proc_t> procs;
  FillProcs(req.pmix_procs(), procs);
  auto coll = m_pmix_state_->PmixStateCollGet(CollType::FENCE_RING, procs);
  if (!coll) return;
  coll->ProcessRingRequest(req.pmix_ring_msg_hdr(), req.msg());
}

void PmixUcxServiceImpl::PmixTreeUpwardForward(const std::string& data) {
  crane::grpc::pmix::PmixTreeUpwardForwardReq req;
  if (!req.ParseFromString(data)) {
    CRANE_ERROR("PmixTreeUpwardForward: ParseFromString failed");
    return;
  }
  std::vector<pmix_proc_t> procs;
  FillProcs(req.pmix_procs(), procs);
  auto coll = m_pmix_state_->PmixStateCollGet(CollType::FENCE_TREE, procs);
  if (!coll) return;
  coll->PmixCollTreeChild(req.peer_host(), req.seq(), req.msg());
}

void PmixUcxServiceImpl::PmixTreeDownwardForward(const std::string& data) {
  crane::grpc::pmix::PmixTreeDownwardForwardReq req;
  if (!req.ParseFromString(data)) {
    CRANE_ERROR("PmixTreeDownwardForward: ParseFromString failed");
    return;
  }
  std::vector<pmix_proc_t> procs;
  FillProcs(req.pmix_procs(), procs);
  auto coll = m_pmix_state_->PmixStateCollGet(CollType::FENCE_TREE, procs);
  if (!coll) return;
  coll->PmixCollTreeParent(req.peer_host(), req.seq(), req.msg());
}

void PmixUcxServiceImpl::PmixDModexRequest(const std::string& data) {
  crane::grpc::pmix::PmixDModexRequestReq req;
  if (!req.ParseFromString(data)) {
    CRANE_ERROR("PmixDModexRequest: ParseFromString failed");
    return;
  }
  pmix_proc_t proc;
  snprintf(proc.nspace, sizeof(proc.nspace), "%s",
           req.pmix_proc().nspace().c_str());
  proc.rank = req.pmix_proc().rank();
  m_dmodex_mgr_->PmixProcessRequest(req.seq_num(), req.craned_id(), proc,
                                    req.local_namespace());
}

void PmixUcxServiceImpl::PmixDModexResponse(const std::string& data) {
  crane::grpc::pmix::PmixDModexResponseReq req;
  if (!req.ParseFromString(data)) {
    CRANE_ERROR("PmixDModexResponse: ParseFromString failed");
    return;
  }
  m_dmodex_mgr_->PmixProcessResponse(req.seq_num(), req.craned_id(), req.data(),
                                     req.status());
}

// ─────────────────────────────────────────────────────────────────────────────
// Init
// ─────────────────────────────────────────────────────────────────────────────
bool PmixUcxServer::Init(const Config& config) {
  m_service_impl_ =
      std::make_unique<PmixUcxServiceImpl>(m_dmodex_mgr_, m_pmix_state_);

#  if UCP_API_VERSION < UCP_VERSION(1, 5)
  setenv("UCX_MEM_MMAP_RELOC", "no", 1);
#  endif
  setenv("UCX_MEM_MALLOC_HOOKS", "no", 1);
  setenv("UCX_MEM_MALLOC_RELOC", "no", 1);
  setenv("UCX_MEM_EVENTS", "no", 1);

  // ── UCX context ───────────────────────────────────────────────────────────
  ucp_config_t* ucp_config = nullptr;
  auto status = ucp_config_read("SLURM", nullptr, &ucp_config);
  if (status != UCS_OK) {
    CRANE_ERROR("ucp_config_read failed: {}", ucs_status_string(status));
    return false;
  }

  ucp_params_t ucp_params{};
  ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES;

  ucp_params.features = UCP_FEATURE_TAG | UCP_FEATURE_WAKEUP;

  status = ucp_init(&ucp_params, ucp_config, &m_ucp_context_);
  ucp_config_release(ucp_config);
  if (status != UCS_OK) {
    CRANE_ERROR("ucp_init failed: {}", ucs_status_string(status));
    return false;
  }

  ucp_worker_params_t wp{};
  wp.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
  wp.thread_mode = UCS_THREAD_MODE_MULTI;

  status = ucp_worker_create(m_ucp_context_, &wp, &m_ucp_worker_);
  if (status != UCS_OK) {
    CRANE_ERROR("ucp_worker_create failed: {}", ucs_status_string(status));
    goto err_context;
  }

  status = ucp_worker_get_address(m_ucp_worker_, &m_ucx_addr_, &m_ucx_alen_);
  if (status != UCS_OK) {
    CRANE_ERROR("ucp_worker_get_address failed: {}", ucs_status_string(status));
    goto err_worker;
  }

  status = ucp_worker_get_efd(m_ucp_worker_, &m_server_fd_);
  if (status != UCS_OK) {
    CRANE_ERROR("ucp_worker_get_efd failed: {}", ucs_status_string(status));
    goto err_addr;
  }

  {
    auto* ucx_client = m_ucx_client_;
    if (!ucx_client) {
      CRANE_ERROR("GetPmixClient() is not PmixUcxClient");
      goto err_addr;
    }
    ucx_client->InitUcxWorker(m_ucp_worker_);
  }

  {
    std::string addr_str(reinterpret_cast<const char*>(m_ucx_addr_),
                         m_ucx_alen_);
    if (!m_craned_client_->BroadcastPmixPort(addr_str)) {
      CRANE_ERROR("BroadcastPmixPort failed");
      goto err_addr;
    }
  }

  m_pmix_async_ = m_main_uvw_loop_->resource<uvw::async_handle>();
  m_pmix_async_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        EvCleanUcxProcessReqQueueCb_();
      });

  {
    auto* ucx_client = m_ucx_client_;
    if (ucx_client) {
      ucx_client->SetNotifyFn([this]() {
        if (m_pmix_async_) m_pmix_async_->send();
      });
    }
  }

  RegisterReceivesAllTypes_();

  m_ucx_loop_ = uvw::loop::create();

  m_ucx_poll_ = m_ucx_loop_->resource<uvw::poll_handle>(m_server_fd_);
  m_ucx_poll_->on<uvw::poll_event>(
      [this](const uvw::poll_event&, uvw::poll_handle&) { OnUcxReadable_(); });

  m_ucx_stop_ = m_ucx_loop_->resource<uvw::async_handle>();
  m_ucx_stop_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        // Received stop signal: close all handles in the UCX loop
        if (m_ucx_poll_) {
          m_ucx_poll_->stop();
          m_ucx_poll_->close();
        }
        m_ucx_stop_->close();
      });

  {
    ucs_status_t arm_st = ucp_worker_arm(m_ucp_worker_);
    if (arm_st == UCS_ERR_BUSY) {
      while (ucp_worker_progress(m_ucp_worker_) > 0) {}
      ucp_worker_arm(m_ucp_worker_);
    }
  }

  m_ucx_poll_->start(uvw::details::uvw_poll_event::READABLE);

  m_ucx_thread_ = std::thread([this]() {
    pthread_setname_np(pthread_self(), "PmixUcxLoop");
    m_ucx_loop_->run();
  });

  CRANE_INFO("PmixUcxServer initialized, UCX loop started (efd={})",
             m_server_fd_);
  return true;

err_addr:
  ucp_worker_release_address(m_ucp_worker_, m_ucx_addr_);
  m_ucx_addr_ = nullptr;
err_worker:
  ucp_worker_destroy(m_ucp_worker_);
  m_ucp_worker_ = nullptr;
err_context:
  ucp_cleanup(m_ucp_context_);
  m_ucp_context_ = nullptr;
  return false;
}

void PmixUcxServer::Shutdown() {
  if (m_shutdown_.exchange(true)) return;

  CRANE_INFO("PmixUcxServer shutting down...");

  if (m_ucx_stop_) {
    m_ucx_stop_->send();
  }
  if (m_ucx_thread_.joinable()) {
    m_ucx_thread_.join();
  }
  m_ucx_loop_.reset();

  if (m_pmix_async_) {
    m_pmix_async_->close();
    m_pmix_async_.reset();
  }

  {
    PmixUcxReq* req = nullptr;
    while (m_req_queue_.try_dequeue(req)) delete req;
  }

  if (m_ucx_addr_) {
    ucp_worker_release_address(m_ucp_worker_, m_ucx_addr_);
    m_ucx_addr_ = nullptr;
  }
  if (m_ucp_worker_) {
    ucp_worker_destroy(m_ucp_worker_);
    m_ucp_worker_ = nullptr;
  }
  if (m_ucp_context_) {
    ucp_cleanup(m_ucp_context_);
    m_ucp_context_ = nullptr;
  }

  CRANE_INFO("PmixUcxServer shutdown complete");
}

void PmixUcxServer::OnUcxReadable_() {
  while (true) {
    while (ucp_worker_progress(m_ucp_worker_) > 0) {}

    ucs_status_t arm_st = ucp_worker_arm(m_ucp_worker_);

    if (arm_st == UCS_OK) {
      break;
    }
    if (arm_st == UCS_ERR_BUSY) {
      continue;
    }
    CRANE_ERROR("ucp_worker_arm unexpected: {}", ucs_status_string(arm_st));
    break;
  }
}

void PmixUcxServer::RegisterReceivesAllTypes_() {
  for (auto t : {
           PmixUcxMsgType::PMIX_UCX_SEND_PMIX_RING_MSG,
           PmixUcxMsgType::PMIX_UCX_TREE_UPWARD_FORWARD,
           PmixUcxMsgType::PMIX_UCX_TREE_DOWNWARD_FORWARD,
           PmixUcxMsgType::PMIX_UCX_DMDEX_REQUEST,
           PmixUcxMsgType::PMIX_UCX_DMDEX_RESPONSE,
       }) {
    RegisterReceivesForType_(t, kInflightPerType);
  }
}

void PmixUcxServer::RegisterReceivesForType_(PmixUcxMsgType type, int count) {
  const ucp_tag_t tag_val = (static_cast<uint64_t>(type) << kTagTypeShift);
  const ucp_tag_t tag_mask = kTagTypeMask;

  for (int i = 0; i < count; ++i) {
    auto* req = new PmixUcxReq();
    req->self = this;
    req->type = type;
    req->data.resize(kAmMaxMessageSize);

    ucp_request_param_t param{};
    param.op_attr_mask =
        UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
    param.cb.recv = RecvHandle_;
    param.user_data = req;

    void* ucp_req =
        ucp_tag_recv_nbx(m_ucp_worker_, req->data.data(), req->data.size(),
                         tag_val, tag_mask, &param);

    if (UCS_PTR_IS_ERR(ucp_req)) {
      CRANE_ERROR("ucp_tag_recv_nbx failed: {}",
                  ucs_status_string(UCS_PTR_STATUS(ucp_req)));
      delete req;
    }
  }
}

void PmixUcxServer::RecvHandle_(void* ucp_req, ucs_status_t status,
                                const ucp_tag_recv_info_t* info,
                                void* user_data) {
  auto* req = static_cast<PmixUcxReq*>(user_data);
  PmixUcxServer* self = req->self;
  const PmixUcxMsgType type = req->type;

  if (UCS_PTR_IS_PTR(ucp_req)) ucp_request_free(ucp_req);

  if (status == UCS_OK && info != nullptr) {
    if (info->length <= req->data.size()) {
      req->data.resize(info->length);
    } else {
      CRANE_ERROR("RecvHandle_: received {} bytes > buffer {} bytes",
                  info->length, req->data.size());
      req->data.clear();
    }

    CRANE_TRACE("UCX RecvHandle_ OK: type={}, len={}", static_cast<int>(type),
                info->length);

    self->m_req_queue_.enqueue(req);

    self->m_pmix_async_->send();
  } else {
    if (status != UCS_ERR_CANCELED) {
      CRANE_ERROR("UCX recv failed: {}", ucs_status_string(status));
    }
    delete req;
  }

  if (!self->m_shutdown_.load(std::memory_order_acquire)) {
    self->RegisterReceivesForType_(type, 1);
  }
}

void PmixUcxServer::EvCleanUcxProcessReqQueueCb_() {
  {
    auto* ucx_client = m_ucx_client_;
    if (ucx_client) ucx_client->DrainSendCallbacks();
  }

  PmixUcxReq* req = nullptr;
  while (m_req_queue_.try_dequeue(req)) {
    CRANE_TRACE("Processing UCX req type={}", static_cast<int>(req->type));

    switch (req->type) {
    case PmixUcxMsgType::PMIX_UCX_SEND_PMIX_RING_MSG:
      m_service_impl_->SendPmixRingMsg(req->data);
      break;
    case PmixUcxMsgType::PMIX_UCX_TREE_UPWARD_FORWARD:
      m_service_impl_->PmixTreeUpwardForward(req->data);
      break;
    case PmixUcxMsgType::PMIX_UCX_TREE_DOWNWARD_FORWARD:
      m_service_impl_->PmixTreeDownwardForward(req->data);
      break;
    case PmixUcxMsgType::PMIX_UCX_DMDEX_REQUEST:
      m_service_impl_->PmixDModexRequest(req->data);
      break;
    case PmixUcxMsgType::PMIX_UCX_DMDEX_RESPONSE:
      m_service_impl_->PmixDModexResponse(req->data);
      break;
    default:
      CRANE_ERROR("Unknown UCX msg type: {}", static_cast<int>(req->type));
      break;
    }
    delete req;
  }
}

#endif
}  // namespace pmix

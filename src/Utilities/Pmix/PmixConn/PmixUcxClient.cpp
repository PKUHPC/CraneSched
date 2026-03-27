#include "PmixUcxClient.h"
#include "crane/Logger.h"

namespace pmix {

#ifdef HAVE_UCX

PmixUcxStub::PmixUcxStub(PmixUcxClient* client) : m_client_(client) {}

PmixUcxStub::~PmixUcxStub() {
  if (!m_ep_) return;

  ucp_request_param_t param{};
  param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
  param.flags        = UCP_EP_CLOSE_FLAG_FORCE;

  // MULTI mode: no external lock required
  void* req = ucp_ep_close_nbx(m_ep_, &param);

  if (UCS_PTR_IS_PTR(req)) {
    ucs_status_t st = UCS_INPROGRESS;
    while (st == UCS_INPROGRESS) {
      ucp_worker_progress(m_client_->m_ucp_worker_);
      st = ucp_request_check_status(req);
    }
    ucp_request_free(req);
  } else if (UCS_PTR_IS_ERR(req)) {
    CRANE_WARN("ucp_ep_close_nbx error: {}",
               ucs_status_string(UCS_PTR_STATUS(req)));
  }
  m_ep_ = nullptr;
}

void PmixUcxStub::SendMessage_(PmixUcxMsgType type,
                               std::string    data,
                               AsyncCallback  callback) {
  const ucp_tag_t tag =
      (static_cast<uint64_t>(type) << kTagTypeShift) | (1ULL & kTagLowMask);

  auto*        ctx = new PmixSendCtx{std::move(callback), std::move(data), m_client_};
  const void*  buf = ctx->buffer.data();
  const size_t len = ctx->buffer.size();

  ucp_request_param_t param{};
  param.op_attr_mask =
      UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
  param.cb.send   = SendHandle_;
  param.user_data = ctx;

  // MULTI mode: direct call, thread-safe
  void* req = ucp_tag_send_nbx(m_ep_, buf, len, tag, &param);

  ucp_worker_signal(m_client_->m_ucp_worker_);

  if (req == nullptr || req == reinterpret_cast<void*>(UCS_OK)) {
    m_client_->m_send_cb_queue_.enqueue({std::move(ctx->callback), true});
    delete ctx;
    if (m_client_->m_notify_fn_) m_client_->m_notify_fn_();
  } else if (UCS_PTR_IS_ERR(req)) {
    CRANE_ERROR("ucp_tag_send_nbx failed: {}",
                ucs_status_string(UCS_PTR_STATUS(req)));
    m_client_->m_send_cb_queue_.enqueue({std::move(ctx->callback), false});
    delete ctx;
    if (m_client_->m_notify_fn_) m_client_->m_notify_fn_();
  }
  // else: 异步 — SendHandle_ 负责入队 + 通知
}


void PmixUcxStub::SendHandle_(void* request, ucs_status_t status,
                              void* user_data) {
  auto* ctx = static_cast<PmixSendCtx*>(user_data);
  if (ctx) {
    if (status != UCS_OK) {
      CRANE_ERROR("UCX async send failed: {}", ucs_status_string(status));
    }
    if (ctx->client) {
      ctx->client->m_send_cb_queue_.enqueue(
          {std::move(ctx->callback), status == UCS_OK});
      if (ctx->client->m_notify_fn_) ctx->client->m_notify_fn_();
    } else {
      // 兜底: client 为空时直接调用 (不应发生)
      ctx->callback(status == UCS_OK);
    }
    delete ctx;
  }
  if (UCS_PTR_IS_PTR(request)) ucp_request_free(request);
}

#define PMIX_STUB_SEND(msg_type, req_obj, cb)                      \
  do {                                                             \
    std::string _data;                                             \
    if (!(req_obj).SerializeToString(&_data)) {                    \
      CRANE_ERROR("Failed to serialize " #req_obj);                \
      (cb)(false);                                                 \
      return;                                                      \
    }                                                              \
    SendMessage_((msg_type), std::move(_data), std::move(cb));     \
  } while (0)

void PmixUcxStub::SendPmixRingMsgNoBlock(
    const crane::grpc::pmix::SendPmixRingMsgReq& req, AsyncCallback cb) {
  PMIX_STUB_SEND(PmixUcxMsgType::PMIX_UCX_SEND_PMIX_RING_MSG, req, cb);
}
void PmixUcxStub::PmixTreeUpwardForwardNoBlock(
    const crane::grpc::pmix::PmixTreeUpwardForwardReq& req, AsyncCallback cb) {
  PMIX_STUB_SEND(PmixUcxMsgType::PMIX_UCX_TREE_UPWARD_FORWARD, req, cb);
}
void PmixUcxStub::PmixTreeDownwardForwardNoBlock(
    const crane::grpc::pmix::PmixTreeDownwardForwardReq& req, AsyncCallback cb) {
  PMIX_STUB_SEND(PmixUcxMsgType::PMIX_UCX_TREE_DOWNWARD_FORWARD, req, cb);
}
void PmixUcxStub::PmixDModexRequestNoBlock(
    const crane::grpc::pmix::PmixDModexRequestReq& req, AsyncCallback cb) {
  PMIX_STUB_SEND(PmixUcxMsgType::PMIX_UCX_DMDEX_REQUEST, req, cb);
}
void PmixUcxStub::PmixDModexResponseNoBlock(
    const crane::grpc::pmix::PmixDModexResponseReq& req, AsyncCallback cb) {
  PMIX_STUB_SEND(PmixUcxMsgType::PMIX_UCX_DMDEX_RESPONSE, req, cb);
}
#undef PMIX_STUB_SEND

void PmixUcxClient::EmplacePmixStub(const CranedId&    craned_id,
                                    const std::string& addr_bytes) {
  if (m_stub_map_.contains(craned_id)) {
    CRANE_WARN("PmixUcxStub for {} already exists, skip", craned_id);
    return;
  }

  const auto* server_addr =
      reinterpret_cast<const ucp_address_t*>(addr_bytes.data());

  auto stub          = std::make_shared<PmixUcxStub>(this);
  stub->m_craned_id_ = craned_id;

  ucp_ep_params_t ep_params{};
  ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
  ep_params.address    = server_addr;

  ucs_status_t status = ucp_ep_create(m_ucp_worker_, &ep_params, &stub->m_ep_);
  if (status != UCS_OK) {
    CRANE_ERROR("ucp_ep_create for {} failed: {}",
                craned_id, ucs_status_string(status));
    return;
  }

  const bool inserted = m_stub_map_.emplace(craned_id, stub).second;
  if (!inserted) {
    CRANE_WARN("PmixUcxStub for {} race-inserted, discard", craned_id);
    return;
  }

  const uint64_t cur =
      m_channel_count_.fetch_add(1, std::memory_order_release) + 1;
  CRANE_TRACE("UCX channel to {} ready. channels={}/{}",
              craned_id, cur, m_node_num_ - 1);
  m_cv_.notify_all();
}

std::shared_ptr<PmixStub> PmixUcxClient::GetPmixStub(
    const CranedId& craned_id) {
  std::shared_ptr<PmixUcxStub> pmix_stub = nullptr;
  m_stub_map_.if_contains(craned_id,
      [&](const StubMap::value_type& kv) { pmix_stub = kv.second; });
      
  return pmix_stub;
}

bool PmixUcxClient::WaitAllStubReady() {
  if (m_node_num_ <= 1) return true;
  std::unique_lock<std::mutex> lk(m_cv_mu_);
  const bool ok = m_cv_.wait_for(lk, std::chrono::seconds(10),
                                 [this]() { return AllStubsReady_(); });
  if (!ok) {
    CRANE_ERROR("WaitAllStubReady timeout: {}/{} ready",
                m_channel_count_.load(), m_node_num_ - 1);
  }
  return ok;
}

#endif
}  // namespace pmix

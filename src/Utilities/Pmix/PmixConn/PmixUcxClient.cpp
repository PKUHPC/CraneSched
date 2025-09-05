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

#include "PmixUcxClient.h"

#include <ucp/api/ucp.h>

#include "PmixCommon.h"
#include "crane/Logger.h"

namespace pmix {

PmixUcxStub::PmixUcxStub(PmixUcxClient *pmix_client) : m_pmix_client_(pmix_client) {}

void PmixUcxStub::SendPmixRingMsgNoBlock(
    const crane::grpc::pmix::SendPmixRingMsgReq &request, AsyncCallback callback) {
  ucp_tag_t tag = (static_cast<uint64_t>(PmixUcxMsgType::PMIX_UCX_SEND_PMIX_RING_MSG) << kTagTypeShift) | (1 & kTagLowMask);
  std::string data;
  if (!request.SerializeToString(&data)) {
    CRANE_ERROR("Failed to serialize SendPmixRingMsgReq to string");
    return ;
  }
  ucp_request_param_t param;
  memset(&param, 0, sizeof(param));
  param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
  param.cb.send = SendHandle_;
  param.user_data = &callback;
  void* req = ucp_tag_send_nbx(m_ep_, data.data(), data.size(), tag, &param);
  if (req == nullptr) {
    CRANE_TRACE("ucx send request completed immediately");
    callback(true);
    return ;
  }
}

void PmixUcxStub::PmixTreeUpwardForwardNoBlock(
    const crane::grpc::pmix::PmixTreeUpwardForwardReq &request, AsyncCallback callback) {
  ucp_tag_t tag = (static_cast<uint64_t>(PmixUcxMsgType::PMIX_UCX_TREE_UPWARD_FORWARD) << kTagTypeShift) | (1 & kTagLowMask);
  std::string data;
  if (!request.SerializeToString(&data)) {
    CRANE_ERROR("Failed to serialize SendPmixRingMsgReq to string");
    return ;
  }
  ucp_request_param_t param;
  memset(&param, 0, sizeof(param));
  param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
  param.cb.send = SendHandle_;
  param.user_data = &callback;
  ucp_tag_send_nbx(m_ep_, data.data(), data.size(), tag, &param);
}

void PmixUcxStub::PmixTreeDownwardForwardNoBlock(
    const crane::grpc::pmix::PmixTreeDownwardForwardReq &request, AsyncCallback callback) {
  ucp_tag_t tag = (static_cast<uint64_t>(PmixUcxMsgType::PMIX_UCX_TREE_DOWNWARD_FORWARD) << kTagTypeShift) | (1 & kTagLowMask);
  std::string data;
  if (!request.SerializeToString(&data)) {
    CRANE_ERROR("Failed to serialize SendPmixRingMsgReq to string");
    return ;
  }
  ucp_request_param_t param;
  memset(&param, 0, sizeof(param));
  param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
  param.cb.send = SendHandle_;
  param.user_data = &callback;
  ucp_tag_send_nbx(m_ep_, data.data(), data.size(), tag, &param);
}

void PmixUcxStub::PmixDModexRequestNoBlock(
    const crane::grpc::pmix::PmixDModexRequestReq &request, AsyncCallback callback) {
  ucp_tag_t tag = (static_cast<uint64_t>(PmixUcxMsgType::PMIX_UCX_DMDEX_REQUEST) << kTagTypeShift) | (1 & kTagLowMask);
  std::string data;
  if (!request.SerializeToString(&data)) {
    CRANE_ERROR("Failed to serialize SendPmixRingMsgReq to string");
    return ;
  }
  ucp_request_param_t param;
  memset(&param, 0, sizeof(param));
  param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
  param.cb.send = SendHandle_;
  param.user_data = &callback;
  ucp_tag_send_nbx(m_ep_, data.data(), data.size(), tag, &param);
}

void PmixUcxStub::PmixDModexResponseNoBlock(
    const crane::grpc::pmix::PmixDModexResponseReq &request, AsyncCallback callback) {
  ucp_tag_t tag = (static_cast<uint64_t>(PmixUcxMsgType::PMIX_UCX_DMDEX_RESPONSE) << kTagTypeShift) | (1 & kTagLowMask);
  std::string data;
  if (!request.SerializeToString(&data)) {
    CRANE_ERROR("Failed to serialize SendPmixRingMsgReq to string");
    return ;
  }
  ucp_request_param_t param;
  memset(&param, 0, sizeof(param));
  param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
  param.cb.send = SendHandle_;
  param.user_data = &callback;
  ucp_tag_send_nbx(m_ep_, data.data(), data.size(), tag, &param);
}

void PmixUcxStub::SendHandle_(void *request, ucs_status_t status,
                              void *user_data) {
  CRANE_TRACE("ucx callback send handle is called");
  auto* callback = static_cast<AsyncCallback*>(user_data);
  if (UCS_PTR_IS_PTR(request) || status != UCS_OK) {
    (*callback)(false);
  } else {
    (*callback)(true);
  }
  ucp_request_free(request);
}

void PmixUcxClient::EmplacePmixStub(const CranedId &craned_id, const std::string& port) {

  const auto *server_addr = reinterpret_cast<const ucp_address_t *>(port.data());

  auto* craned = new PmixUcxStub(this);
  craned->m_craned_id_ = craned_id;

  ucp_ep_params_t ep_params = {};
  ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
  ep_params.address = server_addr;

  {
    util::lock_guard lock_guard(*m_worker_lock_);
    auto status = ucp_ep_create(m_ucp_worker_, &ep_params, &craned->m_ep_);
    if (status != UCS_OK) {
      CRANE_ERROR("ucp_ep_create failed: %s",
                  ucs_status_string(status));
      return ;
    }
  }

  size_t cur_count = m_channel_count_.fetch_add(1) + 1;
  CRANE_TRACE("Creating a channel to {}. Channel count: {}, node num: {}", craned_id, cur_count, m_node_num_);

  m_craned_id_stub_map_.emplace(craned_id, craned);
  {
    std::lock_guard lock(m_mutex_);
    if (cur_count >= m_node_num_) {
      m_cv_.notify_one();
    }
  }
}

std::shared_ptr<PmixStub> PmixUcxClient::GetPmixStub(const CranedId& craned_id) {

  std::shared_ptr<PmixUcxStub> pmix_stub = nullptr;
  for (int i = 0; i < 5; ++i) {
    m_craned_id_stub_map_.if_contains(
        craned_id, [&](std::pair<const CranedId, std::shared_ptr<PmixUcxStub>>& pair) {
            pmix_stub = pair.second;
        });
    if (pmix_stub)
      return pmix_stub;
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  return nullptr;
}

}// namespace pmix
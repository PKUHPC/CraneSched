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

#include "CranedClient.h"

#include <pmix_common.h>

namespace Craned {

CranedStub::CranedStub(CranedClient *craned_client)
    : m_craned_client_(craned_client) {

}

void CranedStub::SendPmixRingMsg(grpc::ClientContext* context,
                                 const crane::grpc::SendPmixRingMsgReq& request,
                                 crane::grpc::SendPmixRingMsgReply* reply,
                                 AsyncGrpcCallback callback) {
  m_stub_->async()->SendPmixRingMsg(context, &request, reply,
                                    std::move(callback));
}

void CranedStub::PmixTreeUpwardForward(grpc::ClientContext* context,
                                       const crane::grpc::PmixTreeUpwardForwardReq& request,
                                       crane::grpc::PmixTreeUpwardForwardReply* reply,
                                       AsyncGrpcCallback callback) {
  m_stub_->async()->PmixTreeUpwardForward(context, &request, reply, std::move(callback));
}

void CranedStub::PmixTreeDownwardForward(grpc::ClientContext* context,
                                         const crane::grpc::PmixTreeDownwardForwardReq& request,
                                         crane::grpc::PmixTreeDownwardForwardReply* reply,
                                         AsyncGrpcCallback callback) {
  m_stub_->async()->PmixTreeDownwardForward(context, &request, reply, std::move(callback));
}

void CranedStub::PmixDModexRequest(grpc::ClientContext* context, const crane::grpc::PmixDModexRequestReq& request, crane::grpc::PmixDModexRequestReply* reply, AsyncGrpcCallback callback) {

  m_stub_->async()->PmixDModexRequest(context, &request, reply, std::move(callback));
}

void CranedStub::PmixDModexResponse(grpc::ClientContext* context,
    const crane::grpc::PmixDModexResponseReq& request, crane::grpc::PmixDModexResponseReply* reply, AsyncGrpcCallback callback) {

  m_stub_->async()->PmixDModexResponse(context, &request, reply, std::move(callback));

}

void CranedClient::Init(const std::set<CranedId>& craned_id_list) {
  int node_id = 0;
  for (const auto& craned_id : craned_id_list) {
    if (craned_id == g_config.CranedIdOfThisNode) continue;

    std::string ip_addr;

    ipv4_t ipv4_addr;
    ipv6_t ipv6_addr;
    if (crane::ResolveIpv4FromHostname(craned_id, &ipv4_addr)) {
      ip_addr = crane::Ipv4ToStr(ipv4_addr);
    } else if (crane::ResolveIpv6FromHostname(craned_id, &ipv6_addr)) {
      ip_addr = crane::Ipv6ToStr(ipv6_addr);
    } else {
      // Just hostname. It should never happen,
      // but we add error handling here for robustness.
      CRANE_ERROR("Unresolved hostname: {}", craned_id);
      ip_addr = craned_id;
    }

    auto* craned = new CranedStub(this);

    grpc::ChannelArguments channel_args;
    if (g_config.CompressedRpc)
      channel_args.SetCompressionAlgorithm(GRPC_COMPRESS_GZIP);

    CRANE_TRACE("Creating a channel to {} {}:{}. Channel count: {}", craned_id,
                ip_addr, kCranedAsyncDefaultPort,
                m_channel_count_.fetch_add(1) + 1);

    if (g_config.ListenConf.UseTls) {
      // TODO: tls
    } else
      craned->m_channel_ = CreateTcpInsecureCustomChannel(
          ip_addr, kCranedAsyncDefaultPort, channel_args);

    craned->m_stub_ = crane::grpc::Craned::NewStub(craned->m_channel_);

    craned->m_craned_id_ = craned_id;

    craned->m_node_id_ = node_id++;

    m_craned_id_stub_map_.emplace(craned_id, std::move(craned));
  }
}

std::shared_ptr<CranedStub> CranedClient::GetCranedStub(
    const CranedId& craned_id) {

  auto iter = m_craned_id_stub_map_.find(craned_id);
  if (iter == m_craned_id_stub_map_.end()) return nullptr;

  return iter->second;
}

} // namespace Craned
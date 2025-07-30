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

#include "PmixClient.h"

#include "crane/GrpcHelper.h"
#include "crane/Logger.h"
#include "grpcpp/support/channel_arguments.h"

namespace pmix {

using namespace std::chrono_literals;

PmixStub::PmixStub(PmixClient* pmix_client) : m_pmix_client_(pmix_client) {

}

void PmixStub::SendPmixRingMsg(grpc::ClientContext* context,
                                 const crane::grpc::pmix::SendPmixRingMsgReq& request,
                                 crane::grpc::pmix::SendPmixRingMsgReply* reply,
                                 AsyncGrpcCallback callback) {
  m_stub_->async()->SendPmixRingMsg(context, &request, reply,
                                    std::move(callback));
}

void PmixStub::PmixTreeUpwardForward(grpc::ClientContext* context,
                                       const crane::grpc::pmix::PmixTreeUpwardForwardReq& request,
                                       crane::grpc::pmix::PmixTreeUpwardForwardReply* reply,
                                       AsyncGrpcCallback callback) {
  m_stub_->async()->PmixTreeUpwardForward(context, &request, reply, std::move(callback));
}

void PmixStub::PmixTreeDownwardForward(grpc::ClientContext* context,
                                         const crane::grpc::pmix::PmixTreeDownwardForwardReq& request,
                                         crane::grpc::pmix::PmixTreeDownwardForwardReply* reply,
                                         AsyncGrpcCallback callback) {
  m_stub_->async()->PmixTreeDownwardForward(context, &request, reply, std::move(callback));
}

void PmixStub::PmixDModexRequest(grpc::ClientContext* context, const crane::grpc::pmix::PmixDModexRequestReq& request, crane::grpc::pmix::PmixDModexRequestReply* reply, AsyncGrpcCallback callback) {

  m_stub_->async()->PmixDModexRequest(context, &request, reply, std::move(callback));
}

void PmixStub::PmixDModexResponse(grpc::ClientContext* context,
    const crane::grpc::pmix::PmixDModexResponseReq& request, crane::grpc::pmix::PmixDModexResponseReply* reply, AsyncGrpcCallback callback) {

  m_stub_->async()->PmixDModexResponse(context, &request, reply, std::move(callback));

}


void PmixClient::EmplacePmixStub(const CranedId& craned_id,
                                 uint32_t port) {

  std::string ip_addr = craned_id;

  // ipv4_t ipv4_addr;
  // ipv6_t ipv6_addr;
  // if (crane::ResolveIpv4FromHostname(craned_id, &ipv4_addr)) {
  //   ip_addr = crane::Ipv4ToStr(ipv4_addr);
  // } else if (crane::ResolveIpv6FromHostname(craned_id, &ipv6_addr)) {
  //   ip_addr = crane::Ipv6ToStr(ipv6_addr);
  // } else {
  //   // Just hostname. It should never happen,
  //   // but we add error handling here for robustness.
  //   CRANE_ERROR("Unresolved hostname: {}", craned_id);
  //   ip_addr = craned_id;
  // }

  auto* craned = new PmixStub(this);

  grpc::ChannelArguments channel_args;
  // if (g_config.CompressedRpc)
  //   channel_args.SetCompressionAlgorithm(GRPC_COMPRESS_GZIP);

  CRANE_TRACE("Creating a channel to {} {}:{}. Channel count: {}", craned_id,
              ip_addr, port, m_channel_count_.fetch_add(1) + 1);

  craned->m_channel_ =
      CreateTcpInsecureCustomChannel(ip_addr, std::to_string(port), channel_args);

  craned->m_stub_ = crane::grpc::pmix::Pmix::NewStub(craned->m_channel_);

  craned->m_craned_id_ = craned_id;

  m_craned_id_stub_map_.emplace(craned_id, craned);
}

std::shared_ptr<PmixStub> PmixClient::GetPmixStub(const CranedId& craned_id) {
  int retry = 5;
  std::shared_ptr<PmixStub> pmix_stub = nullptr;
  while ((retry--) != 0) {
    m_craned_id_stub_map_.if_contains(
      craned_id, [&](std::pair<const CranedId, std::shared_ptr<PmixStub>>& pair) {
        pmix_stub = pair.second;
      });

    if (!pmix_stub)
      return pmix_stub;

    std::this_thread::sleep_for(100ms);
  }

  return nullptr;
}

} // namespace pmix
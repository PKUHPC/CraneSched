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

#include "PmixGrpcClient.h"

#include "crane/GrpcHelper.h"
#include "crane/Logger.h"
#include "grpcpp/support/channel_arguments.h"

namespace pmix {

PmixGrpcStub::PmixGrpcStub(PmixGrpcClient* pmix_client) : m_pmix_client_(pmix_client) {}

void PmixGrpcStub::SendPmixRingMsgNoBlock(
    const crane::grpc::pmix::SendPmixRingMsgReq& request,
    AsyncCallback callback) {
  auto context = std::make_shared<grpc::ClientContext>();
  auto reply = std::make_shared<crane::grpc::pmix::SendPmixRingMsgReply>();

  m_stub_->async()->SendPmixRingMsg(context.get(), &request, reply.get(),
                                    [context, reply, callback](grpc::Status status) {
                                        callback(status.ok() && reply->ok());
                                    });

}

void PmixGrpcStub::PmixTreeUpwardForwardNoBlock(
    const crane::grpc::pmix::PmixTreeUpwardForwardReq& request,
    AsyncCallback callback) {
  auto context = std::make_shared<grpc::ClientContext>();
  auto reply = std::make_shared<crane::grpc::pmix::PmixTreeUpwardForwardReply>();

  m_stub_->async()->PmixTreeUpwardForward(context.get(), &request, reply.get(), [context, reply, callback](grpc::Status status) {
    callback(status.ok() && reply->ok());
  });
}

void PmixGrpcStub::PmixTreeDownwardForwardNoBlock(
    const crane::grpc::pmix::PmixTreeDownwardForwardReq& request,
    AsyncCallback callback) {
  auto context = std::make_shared<grpc::ClientContext>();
  auto reply = std::make_shared<crane::grpc::pmix::PmixTreeDownwardForwardReply>();
  m_stub_->async()->PmixTreeDownwardForward(context.get(), &request, reply.get(), [context, reply, callback](grpc::Status status) {
    callback(status.ok() && reply->ok());
  });
}

void PmixGrpcStub::PmixDModexRequestNoBlock(
    const crane::grpc::pmix::PmixDModexRequestReq& request,
    AsyncCallback callback) {
  auto context = std::make_shared<grpc::ClientContext>();
  auto reply = std::make_shared<crane::grpc::pmix::PmixDModexRequestReply>();
  m_stub_->async()->PmixDModexRequest(context.get(), &request, reply.get(), [context, reply, callback](grpc::Status status) {
    callback(status.ok() && reply->ok());
  });
}

void PmixGrpcStub::PmixDModexResponseNoBlock(
    const crane::grpc::pmix::PmixDModexResponseReq& request,
    AsyncCallback callback) {
  auto context = std::make_shared<grpc::ClientContext>();
  auto reply = std::make_shared<crane::grpc::pmix::PmixDModexResponseReply>();
  m_stub_->async()->PmixDModexResponse(context.get(), &request, reply.get(), [context, reply, callback](grpc::Status status) {
    callback(status.ok() && reply->ok());
  });
}


void PmixGrpcClient::EmplacePmixStub(const CranedId& craned_id,
                                 const std::string& port) {

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

  auto* craned = new PmixGrpcStub(this);

  grpc::ChannelArguments channel_args;
  // if (g_config.CompressedRpc)
  //   channel_args.SetCompressionAlgorithm(GRPC_COMPRESS_GZIP);

  int cur_count = m_channel_count_.fetch_add(1) + 1;
  CRANE_TRACE("Creating a channel to {} {}:{}. Channel count: {}, node num: {}", craned_id,
              ip_addr, port, cur_count, m_node_num_);

  craned->m_channel_ =
      CreateTcpInsecureCustomChannel(ip_addr, port, channel_args);

  craned->m_stub_ = crane::grpc::pmix::Pmix::NewStub(craned->m_channel_);

  craned->m_craned_id_ = craned_id;

  m_craned_id_stub_map_.emplace(craned_id, craned);


  {
    std::lock_guard<std::mutex> lock(m_mutex_);
    if (cur_count >= m_node_num_) {
      m_cv_.notify_one();
    }
  }

}

std::shared_ptr<PmixStub> PmixGrpcClient::GetPmixStub(const CranedId& craned_id) {

  std::shared_ptr<PmixGrpcStub> pmix_stub = nullptr;
  for (int i = 0; i < 5; ++i) {
    m_craned_id_stub_map_.if_contains(
        craned_id, [&](std::pair<const CranedId, std::shared_ptr<PmixGrpcStub>>& pair) {
            pmix_stub = pair.second;
        });
    if (pmix_stub)
      return pmix_stub;
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  return nullptr;
}

} // namespace pmix
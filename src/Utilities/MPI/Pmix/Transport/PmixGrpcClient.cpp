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

PmixGrpcStub::PmixGrpcStub(PmixGrpcClient* pmix_client)
    : m_pmix_client_(pmix_client) {}

void PmixGrpcStub::SendPmixRingMsgNoBlock(
    const crane::grpc::mpi::pmix::SendPmixRingMsgReq& request,
    AsyncCallback callback) {
  auto context = std::make_shared<grpc::ClientContext>();
  auto reply = std::make_shared<crane::grpc::mpi::pmix::SendPmixRingMsgReply>();

  m_stub_->async()->SendPmixRingMsg(
      context.get(), &request, reply.get(),
      [context, reply, callback](grpc::Status status) {
        callback(status.ok() && reply->ok());
      });
}

void PmixGrpcStub::PmixTreeUpwardForwardNoBlock(
    const crane::grpc::mpi::pmix::PmixTreeUpwardForwardReq& request,
    AsyncCallback callback) {
  auto context = std::make_shared<grpc::ClientContext>();
  auto reply =
      std::make_shared<crane::grpc::mpi::pmix::PmixTreeUpwardForwardReply>();

  m_stub_->async()->PmixTreeUpwardForward(
      context.get(), &request, reply.get(),
      [context, reply, callback](grpc::Status status) {
        callback(status.ok() && reply->ok());
      });
}

void PmixGrpcStub::PmixTreeDownwardForwardNoBlock(
    const crane::grpc::mpi::pmix::PmixTreeDownwardForwardReq& request,
    AsyncCallback callback) {
  auto context = std::make_shared<grpc::ClientContext>();
  auto reply =
      std::make_shared<crane::grpc::mpi::pmix::PmixTreeDownwardForwardReply>();
  m_stub_->async()->PmixTreeDownwardForward(
      context.get(), &request, reply.get(),
      [context, reply, callback](grpc::Status status) {
        callback(status.ok() && reply->ok());
      });
}

void PmixGrpcStub::PmixDModexRequestNoBlock(
    const crane::grpc::mpi::pmix::PmixDModexRequestReq& request,
    AsyncCallback callback) {
  auto context = std::make_shared<grpc::ClientContext>();
  auto reply =
      std::make_shared<crane::grpc::mpi::pmix::PmixDModexRequestReply>();
  m_stub_->async()->PmixDModexRequest(
      context.get(), &request, reply.get(),
      [context, reply, callback](grpc::Status status) {
        callback(status.ok() && reply->ok());
      });
}

void PmixGrpcStub::PmixDModexResponseNoBlock(
    const crane::grpc::mpi::pmix::PmixDModexResponseReq& request,
    AsyncCallback callback) {
  auto context = std::make_shared<grpc::ClientContext>();
  auto reply =
      std::make_shared<crane::grpc::mpi::pmix::PmixDModexResponseReply>();
  m_stub_->async()->PmixDModexResponse(
      context.get(), &request, reply.get(),
      [context, reply, callback](grpc::Status status) {
        callback(status.ok() && reply->ok());
      });
}

void PmixGrpcClient::EmplacePmixStub(const CranedId& craned_id,
                                     const std::string& port) {
  const auto hostname = m_config_.CranedIdToNodeHostname.find(craned_id);
  const auto address = m_config_.CranedIdToNodeAddr.find(craned_id);
  if (craned_id == m_config_.CranedIdOfThisNode ||
      hostname == m_config_.CranedIdToNodeHostname.end() ||
      address == m_config_.CranedIdToNodeAddr.end() ||
      hostname->second.empty() || address->second.empty()) {
    CRANE_ERROR("Missing PMIx peer configuration or invalid peer ID: {}",
                craned_id);
    return;
  }

  auto craned = std::make_shared<PmixGrpcStub>(this);

  grpc::ChannelArguments channel_args;
  // if (g_config.CompressedRpc)
  //   channel_args.SetCompressionAlgorithm(GRPC_COMPRESS_GZIP);

  if (m_config_.UseTls) {
    SetTlsTargetNameOverride(&channel_args, hostname->second);
    craned->m_channel_ = CreateTcpTlsCustomChannelByIp(
        address->second, port, m_config_.TlsCerts, channel_args);
  } else {
    craned->m_channel_ =
        CreateTcpInsecureCustomChannel(address->second, port, channel_args);
  }

  craned->m_stub_ = crane::grpc::mpi::pmix::Pmix::NewStub(craned->m_channel_);

  craned->m_craned_id_ = craned_id;

  // Only count a stub as ready after a successful insert.  This prevents
  // WaitAllStubReady() from returning before GetPmixStub() can find the entry,
  // and stops duplicate craned_ids from inflating the counter.
  auto [it, inserted] = m_craned_id_stub_map_.emplace(craned_id, craned);
  if (!inserted) {
    CRANE_WARN("Duplicate EmplacePmixStub for craned_id={}, ignored",
               craned_id);
    return;
  }

  int cur_count = m_channel_count_.fetch_add(1) + 1;
  CRANE_TRACE(
      "Created stub for {} at {}:{}, NodeHostname={}. Channel count: "
      "{}/{}",
      craned_id, address->second, port, hostname->second, cur_count,
      m_node_num_ - 1);

  {
    std::lock_guard<std::mutex> lock(m_mutex_);
    if (cur_count + 1 >= m_node_num_) {
      m_cv_.notify_one();
    }
  }
}

std::shared_ptr<PmixStub> PmixGrpcClient::GetPmixStub(
    const CranedId& craned_id) {
  std::shared_ptr<PmixGrpcStub> pmix_stub = nullptr;
  m_craned_id_stub_map_.if_contains(
      craned_id,
      [&](std::pair<const CranedId, std::shared_ptr<PmixGrpcStub>>& pair) {
        pmix_stub = pair.second;
      });

  if (!pmix_stub) {
    CRANE_ERROR("No gRPC stub found for craned_id={}, channel_count={}/{}",
                craned_id, m_channel_count_.load(), m_node_num_ - 1);
  }

  return pmix_stub;
}

}  // namespace pmix

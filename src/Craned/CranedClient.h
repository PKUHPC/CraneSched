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

#pragma once

#include "CranedPublicDefs.h"

#include "crane/Lock.h"
#include "crane/Network.h"
#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"

namespace Craned {

using AsyncGrpcCallback = std::function<void(grpc::Status)>;

class CranedClient;

class CranedStub {
 public:
  explicit CranedStub(CranedClient *craned_client);

  ~CranedStub() = default;

  void SendPmixRingMsg(grpc::ClientContext *context,
                       const crane::grpc::SendPmixRingMsgReq &request,
                       crane::grpc::SendPmixRingMsgReply *reply,
                       AsyncGrpcCallback callback);

  void PmixTreeUpwardForward(
      grpc::ClientContext *context,
      const crane::grpc::PmixTreeUpwardForwardReq &request,
      crane::grpc::PmixTreeUpwardForwardReply *reply,
      AsyncGrpcCallback callback);

  void PmixTreeDownwardForward(
      grpc::ClientContext *context,
      const crane::grpc::PmixTreeDownwardForwardReq &request,
      crane::grpc::PmixTreeDownwardForwardReply *reply,
      AsyncGrpcCallback callback);

  void PmixDModexRequest(grpc::ClientContext *context,
                         const crane::grpc::PmixDModexRequestReq &request,
                         crane::grpc::PmixDModexRequestReply *reply,
                         AsyncGrpcCallback callback);

  void PmixDModexResponse(grpc::ClientContext *context,
                          const crane::grpc::PmixDModexResponseReq &request,
                          crane::grpc::PmixDModexResponseReply *reply,
                          AsyncGrpcCallback callback);

  int GetNodeId() const { return m_node_id_; }

 private:

  CranedClient *m_craned_client_;

  std::shared_ptr<grpc::Channel> m_channel_;

  std::unique_ptr<crane::grpc::Craned::Stub> m_stub_;

  CranedId m_craned_id_;

  int m_node_id_;

  friend class CranedClient;
};

class CranedClient {
 public:
  CranedClient() = default;

  ~CranedClient() = default;

  void Init(const std::set<CranedId>& craned_id_list);

  std::shared_ptr<CranedStub> GetCranedStub(const CranedId &craned_id);

 private:
  template <typename K, typename V,
            typename Hash = absl::container_internal::hash_default_hash<K>>
  using NodeHashMap = absl::node_hash_map<K, V, Hash>;

  NodeHashMap<CranedId, std::shared_ptr<CranedStub>> m_craned_id_stub_map_;

  std::atomic_uint64_t m_channel_count_{0};
};

} // namespace Craned

inline std::unique_ptr<Craned::CranedClient> g_craned_client;
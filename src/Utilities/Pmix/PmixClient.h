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

#include "absl/container/node_hash_map.h"
#include "crane/Lock.h"
#include "crane/Network.h"
#include "crane/PublicHeader.h"
#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"
#include "protos/Pmix.grpc.pb.h"
#include "protos/Pmix.pb.h"
#include <parallel_hashmap/phmap.h>

namespace pmix {


using AsyncGrpcCallback = std::function<void(grpc::Status)>;

class PmixClient;

class PmixStub {
 public:
  explicit PmixStub(PmixClient *pmix_client);

  ~PmixStub() = default;

  void SendPmixRingMsg(grpc::ClientContext *context,
                       const crane::grpc::pmix::SendPmixRingMsgReq &request,
                       crane::grpc::pmix::SendPmixRingMsgReply *reply,
                       AsyncGrpcCallback callback);

  void PmixTreeUpwardForward(
      grpc::ClientContext *context,
      const crane::grpc::pmix::PmixTreeUpwardForwardReq &request,
      crane::grpc::pmix::PmixTreeUpwardForwardReply *reply,
      AsyncGrpcCallback callback);

  void PmixTreeDownwardForward(
      grpc::ClientContext *context,
      const crane::grpc::pmix::PmixTreeDownwardForwardReq &request,
      crane::grpc::pmix::PmixTreeDownwardForwardReply *reply,
      AsyncGrpcCallback callback);

  void PmixDModexRequest(grpc::ClientContext *context,
                         const crane::grpc::pmix::PmixDModexRequestReq &request,
                         crane::grpc::pmix::PmixDModexRequestReply *reply,
                         AsyncGrpcCallback callback);

  void PmixDModexResponse(grpc::ClientContext *context,
                          const crane::grpc::pmix::PmixDModexResponseReq &request,
                          crane::grpc::pmix::PmixDModexResponseReply *reply,
                          AsyncGrpcCallback callback);

  int GetNodeId() const { return m_node_id_; }

 private:

  PmixClient *m_pmix_client_;

  std::shared_ptr<grpc::Channel> m_channel_;

  std::unique_ptr<crane::grpc::pmix::Pmix::Stub> m_stub_;

  CranedId m_craned_id_;

  int m_node_id_;

  friend class PmixClient;
};

class PmixClient {
 public:
  PmixClient() = default;

  ~PmixClient() = default;

  void EmplacePmixStub(const CranedId &craned_id, uint32_t port);

  std::shared_ptr<PmixStub> GetPmixStub(const CranedId &craned_id);

 private:
  template <typename K, typename V,
            typename Hash = absl::container_internal::hash_default_hash<K>>
  using NodeHashMap = absl::node_hash_map<K, V, Hash>;

  using CranedIdToStubMap = phmap::parallel_flat_hash_map<
      CranedId,
      std::shared_ptr<PmixStub>, phmap::priv::hash_default_hash<CranedId>,
      phmap::priv::hash_default_eq<CranedId>,
      std::allocator<std::pair<const CranedId, std::shared_ptr<PmixStub>>>, 4,
      std::shared_mutex>;

  CranedIdToStubMap m_craned_id_stub_map_;

  std::atomic_uint64_t m_channel_count_{0};
};

} // namespace pmix
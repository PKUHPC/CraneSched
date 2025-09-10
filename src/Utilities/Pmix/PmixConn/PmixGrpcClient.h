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

#include <parallel_hashmap/phmap.h>

#include <condition_variable>

#include "PmixClient.h"
#include "absl/container/node_hash_map.h"
#include "crane/Lock.h"
#include "crane/Network.h"
#include "crane/PublicHeader.h"
#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"
#include "protos/Pmix.grpc.pb.h"
#include "protos/Pmix.pb.h"

namespace pmix {

class PmixGrpcClient;

class PmixGrpcStub : public PmixStub {
 public:
  explicit PmixGrpcStub(PmixGrpcClient *pmix_client);

  ~PmixGrpcStub() override = default;

  void SendPmixRingMsgNoBlock(
      const crane::grpc::pmix::SendPmixRingMsgReq &request, AsyncCallback callback) override;

  void PmixTreeUpwardForwardNoBlock(
      const crane::grpc::pmix::PmixTreeUpwardForwardReq &request, AsyncCallback callback) override;

  void PmixTreeDownwardForwardNoBlock(
      const crane::grpc::pmix::PmixTreeDownwardForwardReq &request, AsyncCallback callback) override;

  void PmixDModexRequestNoBlock(
      const crane::grpc::pmix::PmixDModexRequestReq &request, AsyncCallback callback) override;

  void PmixDModexResponseNoBlock(
      const crane::grpc::pmix::PmixDModexResponseReq &request, AsyncCallback callback) override;

 private:

  PmixGrpcClient *m_pmix_client_;

  std::shared_ptr<grpc::Channel> m_channel_;

  std::unique_ptr<crane::grpc::pmix::Pmix::Stub> m_stub_;

  CranedId m_craned_id_;

  friend class PmixGrpcClient;
};

class PmixGrpcClient : public PmixClient {
 public:
  explicit PmixGrpcClient(int node_num) : m_node_num_(node_num){}

  ~PmixGrpcClient() override = default;

  void EmplacePmixStub(const CranedId &craned_id, const std::string& port) override;

  std::shared_ptr<PmixStub> GetPmixStub(const CranedId &craned_id) override;

  uint64_t GetChannelCount() const override { return m_channel_count_.load(); }

  void WaitAllStubReady() override {
    std::unique_lock<std::mutex> lock(m_mutex_);
    if (GetChannelCount() >= m_node_num_) return ;
    m_cv_.wait(lock, [this](){ return GetChannelCount() >= m_node_num_; });
  }

 private:
  template <typename K, typename V,
            typename Hash = absl::container_internal::hash_default_hash<K>>
  using NodeHashMap = absl::node_hash_map<K, V, Hash>;

  using CranedIdToStubMap = phmap::parallel_flat_hash_map<
      CranedId,
      std::shared_ptr<PmixGrpcStub>, phmap::priv::hash_default_hash<CranedId>,
      phmap::priv::hash_default_eq<CranedId>,
      std::allocator<std::pair<const CranedId, std::shared_ptr<PmixGrpcStub>>>, 4,
      std::shared_mutex>;

  CranedIdToStubMap m_craned_id_stub_map_;

  std::mutex m_mutex_;
  std::condition_variable m_cv_;

  int m_node_num_;

  std::atomic_uint64_t m_channel_count_{0};
};

} // namespace pmix
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

#include <thread>
#include <ucp/api/ucp_compat.h>
#include <ucp/api/ucp_def.h>

#include "PmixClient.h"
#include "crane/Lock.h"
#include "protos/Pmix.grpc.pb.h"

namespace pmix {

class PmixUcxClient;

class PmixUcxStub : public PmixStub {
public:
  explicit PmixUcxStub(PmixUcxClient *pmix_client);

  ~PmixUcxStub() override {
    ucp_ep_destroy(m_ep_);
  }

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

  static void SendHandle_(void *request, ucs_status_t status, void *user_data);

  PmixUcxClient *m_pmix_client_;

  ucp_ep_h m_ep_;

  CranedId m_craned_id_;

  friend class PmixUcxClient;
};


class PmixUcxClient : public PmixClient {
public:
  PmixUcxClient(int node_num) : m_node_num_(node_num) {}

  ~PmixUcxClient() override = default;

  void InitUcxWorker(util::mutex& worker_lock, ucp_worker_h ucp_worker_) {
    m_ucp_worker_ = ucp_worker_;
    m_worker_lock_ = &worker_lock;
  }

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
      std::shared_ptr<PmixUcxStub>, phmap::priv::hash_default_hash<CranedId>,
      phmap::priv::hash_default_eq<CranedId>,
      std::allocator<std::pair<const CranedId, std::shared_ptr<PmixUcxStub>>>, 4,
      std::shared_mutex>;

  CranedIdToStubMap m_craned_id_stub_map_;

  std::mutex m_mutex_;
  std::condition_variable m_cv_;

  util::mutex* m_worker_lock_;
  ucp_worker_h m_ucp_worker_;
  int m_node_num_;

  std::atomic_uint64_t m_channel_count_{0};
  std::atomic_bool m_running_{true};
  std::thread m_progress_thread_;
};



}// namespace pmix
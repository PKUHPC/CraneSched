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

#include "PmixClient.h"
#include "PmixCommon.h"
#include "protos/Pmix.grpc.pb.h"

#include "concurrentqueue/concurrentqueue.h"
#include <parallel_hashmap/phmap.h>

#ifdef HAVE_UCX
#include <ucp/api/ucp.h>
#endif

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

namespace pmix {

class PmixUcxClient;

#ifdef HAVE_UCX

struct PmixSendCtx {
  AsyncCallback  callback;
  std::string    buffer;              // owned — valid until SendHandle_ fires
  PmixUcxClient* client{nullptr};
};
#endif

class PmixUcxStub : public PmixStub {
 public:
  explicit PmixUcxStub(PmixUcxClient* client);

#ifdef HAVE_UCX
  ~PmixUcxStub() override;

  void SendPmixRingMsgNoBlock(
      const crane::grpc::pmix::SendPmixRingMsgReq& request,
      AsyncCallback callback) override;

  void PmixTreeUpwardForwardNoBlock(
      const crane::grpc::pmix::PmixTreeUpwardForwardReq& request,
      AsyncCallback callback) override;

  void PmixTreeDownwardForwardNoBlock(
      const crane::grpc::pmix::PmixTreeDownwardForwardReq& request,
      AsyncCallback callback) override;

  void PmixDModexRequestNoBlock(
      const crane::grpc::pmix::PmixDModexRequestReq& request,
      AsyncCallback callback) override;

  void PmixDModexResponseNoBlock(
      const crane::grpc::pmix::PmixDModexResponseReq& request,
      AsyncCallback callback) override;

 private:
  // Serialize payload → apply tag → post async send
  void SendMessage_(PmixUcxMsgType type,
                    std::string    data,
                    AsyncCallback  callback);

  static void SendHandle_(void* request, ucs_status_t status, void* user_data);

  PmixUcxClient* m_client_;      // non-owning
  ucp_ep_h       m_ep_{nullptr};
  CranedId       m_craned_id_;

  friend class PmixUcxClient;
#endif  // HAVE_UCX
};

// ─────────────────────────────────────────────────────────────────────────────
// PmixUcxClient  —  Manages all UCX connections to remote nodes
//
// Lifecycle:
//   1. Construct (node_num)
//   2. InitUcxWorker()        Called by PmixUcxServer::Init() to inject worker
//   3. SetNotifyFn()          Called by PmixUcxServer::Init() to inject notify function
//   4. EmplacePmixStub() × N  Concurrency-safe, idempotent on duplicate key
//   5. WaitAllStubReady()     Block until node_num-1 connections are ready
//   6. GetPmixStub()          Can be called at any time, returns nullptr on timeout
//   7. DrainSendCallbacks()   Called periodically on PMIx Loop thread to consume deferred callbacks
// ─────────────────────────────────────────────────────────────────────────────
class PmixUcxClient : public PmixClient {
 public:
  explicit PmixUcxClient(int node_num) : m_node_num_(node_num) {}
  ~PmixUcxClient() override = default;

#ifdef HAVE_UCX
  void InitUcxWorker(ucp_worker_h worker) {
    m_ucp_worker_ = worker;
  }

  void SetNotifyFn(std::function<void()> fn) {
    m_notify_fn_ = std::move(fn);
  }

  void DrainSendCallbacks() {
    std::pair<AsyncCallback, bool> item;
    while (m_send_cb_queue_.try_dequeue(item)) {
      item.first(item.second);
    }
  }

  void EmplacePmixStub(const CranedId&    craned_id,
                       const std::string& addr_bytes) override;

  std::shared_ptr<PmixStub> GetPmixStub(const CranedId& craned_id) override;

  uint64_t GetChannelCount() const override {
    return m_channel_count_.load(std::memory_order_acquire);
  }

  bool WaitAllStubReady() override;

 private:
  bool AllStubsReady_() const {
    return static_cast<int>(
               m_channel_count_.load(std::memory_order_acquire)) >=
           m_node_num_ - 1;
  }

  using StubMap = phmap::parallel_flat_hash_map<
      CranedId, std::shared_ptr<PmixUcxStub>,
      phmap::priv::hash_default_hash<CranedId>,
      phmap::priv::hash_default_eq<CranedId>,
      std::allocator<std::pair<const CranedId, std::shared_ptr<PmixUcxStub>>>,
      4, std::shared_mutex>;

  StubMap                 m_stub_map_;
  mutable std::mutex      m_cv_mu_;
  std::condition_variable m_cv_;

  ucp_worker_h  m_ucp_worker_{nullptr}; 

  moodycamel::ConcurrentQueue<std::pair<AsyncCallback, bool>> m_send_cb_queue_;

  std::function<void()> m_notify_fn_;

  friend class PmixUcxStub;
#endif  // HAVE_UCX

  int                   m_node_num_;
  std::atomic<uint64_t> m_channel_count_{0};
};

}  // namespace pmix

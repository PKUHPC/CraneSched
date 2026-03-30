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

#include "CranedClient.h"
#include "PmixASyncServer.h"
#include "PmixCommon.h"
#include "PmixDModex.h"
#include "PmixState.h"
#include "concurrentqueue/concurrentqueue.h"
#include "uvw/async.h"
#include "uvw/loop.h"
#include "uvw/poll.h"

#ifdef HAVE_UCX
#  include <ucp/api/ucp.h>

// Forward declaration so the constructor signature compiles without pulling in
// the full PmixUcxClient header at this level.
#endif

#include <atomic>
#include <thread>

namespace pmix {

class PmixUcxServer;
class PmixUcxClient;

#ifdef HAVE_UCX
struct PmixUcxReq {
  PmixUcxMsgType type{};
  std::string data;
  PmixUcxServer* self{nullptr};
};
#endif

class PmixUcxServiceImpl {
 public:
  explicit PmixUcxServiceImpl(PmixDModexReqManager* dmodex_mgr,
                              PmixState* pmix_state)
      : m_dmodex_mgr_(dmodex_mgr), m_pmix_state_(pmix_state) {}

#ifdef HAVE_UCX
  void SendPmixRingMsg(const std::string& data);
  void PmixTreeUpwardForward(const std::string& data);
  void PmixTreeDownwardForward(const std::string& data);
  void PmixDModexRequest(const std::string& data);
  void PmixDModexResponse(const std::string& data);
#endif

 private:
  PmixDModexReqManager* m_dmodex_mgr_;
  PmixState* m_pmix_state_;
};

class PmixUcxServer : public PmixASyncServer {
 public:
#ifdef HAVE_UCX
  // main_uvw_loop — the PmixServer's primary uvw loop used to create the
  //   cross-thread async handle (m_pmix_async_).
  // ucx_client — the companion UCX client; injected here so Init() never
  //   needs to call PmixServer::GetInstance().
  explicit PmixUcxServer(PmixDModexReqManager* dmodex_mgr,
                         PmixState* pmix_state, CranedClient* craned_client,
                         uvw::loop* main_uvw_loop, PmixUcxClient* ucx_client)
      : m_dmodex_mgr_(dmodex_mgr),
        m_pmix_state_(pmix_state),
        m_craned_client_(craned_client),
        m_main_uvw_loop_(main_uvw_loop),
        m_ucx_client_(ucx_client) {}

  ~PmixUcxServer() override { Shutdown(); }

  bool Init(const Config& config) override;
  void Shutdown() override;
  void Wait() override {}

 private:
  void OnUcxReadable_();

  void RegisterReceivesAllTypes_();
  void RegisterReceivesForType_(PmixUcxMsgType type, int count);

  static void RecvHandle_(void* request, ucs_status_t status,
                          const ucp_tag_recv_info_t* info, void* user_data);

  void EvCleanUcxProcessReqQueueCb_();

  static PmixUcxMsgType TagToType_(uint64_t tag);

  ucp_context_h m_ucp_context_{nullptr};
  ucp_worker_h m_ucp_worker_{nullptr};
  ucp_address_t* m_ucx_addr_{nullptr};
  size_t m_ucx_alen_{0};
  int m_server_fd_{-1};

  std::unique_ptr<PmixUcxServiceImpl> m_service_impl_;

  PmixDModexReqManager* m_dmodex_mgr_;
  PmixState* m_pmix_state_;
  CranedClient* m_craned_client_;
  uvw::loop* m_main_uvw_loop_{nullptr};   // injected, not owned
  PmixUcxClient* m_ucx_client_{nullptr};  // injected, not owned

  std::shared_ptr<uvw::loop> m_ucx_loop_;
  std::thread m_ucx_thread_;
  std::shared_ptr<uvw::poll_handle> m_ucx_poll_;
  std::shared_ptr<uvw::async_handle> m_ucx_stop_;

  std::shared_ptr<uvw::async_handle> m_pmix_async_;

  moodycamel::ConcurrentQueue<PmixUcxReq*> m_req_queue_;

  std::atomic<bool> m_shutdown_{false};

  friend class PmixUcxServiceImpl;
#endif
};

}  // namespace pmix

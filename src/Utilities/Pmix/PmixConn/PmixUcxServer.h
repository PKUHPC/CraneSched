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

#include "PmixASyncServer.h"
#include "PmixCommon.h"
#include "concurrentqueue/concurrentqueue.h"
#include "uvw/async.h"
#ifdef HAVE_UCX
#include <ucp/api/ucp.h>
#endif

namespace pmix {

enum class PmixUcxStatus {
  ACTIVE = 0,
  COMPLETE,
  FAILED
};

enum class PmixUcxMsgType {
  PMIX_UCX_TREE_UPWARD_FORWARD = 0,
  PMIX_UCX_TREE_DOWNWARD_FORWARD,
  PMIX_UCX_DMDEX_REQUEST,
  PMIX_UCX_DMDEX_RESPONSE,
  PMIX_UCX_SEND_PMIX_RING_MSG
};

struct PmixUcxReq {
  std::atomic<PmixUcxStatus> status{PmixUcxStatus::ACTIVE};
  std::string data;
  PmixUcxMsgType type;
};

class PmixUcxServiceImpl {
public:
  explicit PmixUcxServiceImpl() = default;

  void SendPmixRingMsg(const PmixUcxReq& req);

  void PmixTreeUpwardForward(const PmixUcxReq& req);

  void PmixTreeDownwardForward(const PmixUcxReq& req);

  void PmixDModexRequest(const PmixUcxReq& req);

  void PmixDModexResponse(const PmixUcxReq& req);
};

class PmixUcxServer: public PmixASyncServer {
public:
  explicit PmixUcxServer() = default;

  bool Init(const Config& config) override;

  void Shutdown() override {  }

  void Wait() override { }

  void OnUcxReadable();
private:

  bool UcxProgress_();

  static void RecvHandle_(void *request, ucs_status_t status,
                        ucp_tag_recv_info_t *info);

  template <class T>
using ConcurrentQueue = moodycamel::ConcurrentQueue<T>;

  ucp_context_h m_ucp_context_{};
  ucp_worker_h m_ucp_worker_{};
  ucp_address_t *m_ucx_addr_{};
  size_t m_ucx_alen_{};
  int m_server_fd_{};

  util::mutex m_mutex_;
  std::list<std::shared_ptr<PmixUcxReq>> m_pending_reqs_;

  std::unique_ptr<PmixUcxServiceImpl> m_service_impl_;

  std::shared_ptr<uvw::async_handle> m_ucx_process_req_async_handle_;
  ConcurrentQueue<std::shared_ptr<PmixUcxReq>> m_ucx_process_req_queue_;
  void EvCleanUcxProcessReqQueueCb_();

  friend class PmixUcxServiceImpl;
};

} // namespace pmix
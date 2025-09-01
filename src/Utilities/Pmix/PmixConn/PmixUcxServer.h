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
class PmixUcxServer;

struct PmixUcxReq {
  PmixUcxMsgType type;
  std::string data;
  PmixUcxServer* self{};
};

class PmixUcxServiceImpl {
public:
  explicit PmixUcxServiceImpl() = default;

  void SendPmixRingMsg(const std::string& req_data);

  void PmixTreeUpwardForward(const std::string& req_data);

  void PmixTreeDownwardForward(const std::string& req_data);

  void PmixDModexRequest(const std::string& req_data);

  void PmixDModexResponse(const std::string& req_data);
};

class PmixUcxServer: public PmixASyncServer {
public:
  explicit PmixUcxServer() = default;

  ~PmixUcxServer() override {
    if (m_ucp_worker_) ucp_worker_destroy(m_ucp_worker_);
    if (m_ucp_context_) ucp_cleanup(m_ucp_context_);
  }

  bool Init(const Config& config) override;

  void Shutdown() override {  }

  void Wait() override { }
private:

  void OnUcxReadable_();

  void RegisterReceivesAllTypes_();
  void RegisterReceivesForType_(PmixUcxMsgType type, int cnt);

  static void RecvHandle_(void* request,
                                ucs_status_t status,
                                const ucp_tag_recv_info_t* info,
                                void* user_data);

  static uint64_t MakeTag_(PmixUcxMsgType type, uint64_t low48) {
    return (static_cast<uint64_t>(type) << kTagTypeShift) | (low48 & kTagLowMask);
  }
  static PmixUcxMsgType TagToType_(uint64_t tag) {
    return static_cast<PmixUcxMsgType>((tag & kTagTypeMask) >> kTagTypeShift);
  }
  static uint64_t TagLow_(uint64_t tag) { return (tag & kTagLowMask); }

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

  std::shared_ptr<uvw::poll_handle> m_poll_;
  std::shared_ptr<uvw::async_handle> m_ucx_process_req_async_handle_;
  ConcurrentQueue<PmixUcxReq*> m_ucx_process_req_queue_;
  void EvCleanUcxProcessReqQueueCb_();

  friend class PmixUcxServiceImpl;
};

} // namespace pmix
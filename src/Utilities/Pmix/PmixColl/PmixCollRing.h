/**
 * Copyright (c) 2026 Peking University and Peking University
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

#include "PmixColl.h"
#include "PmixCommon.h"
#include "crane/Logger.h"

namespace pmix {

// Forward declarations — full types included in PmixCollRing.cpp.
class PmixClient;
class CranedClient;

#define PMIX_COLL_RING_CTX_NUM 3

enum class CollRingState : std::uint8_t {
  SYNC,
  PROGRESS,
  FINALIZE,
};

inline std::string ToString(CollRingState state) {
  switch (state) {
  case CollRingState::SYNC:
    return "SYNC";
  case CollRingState::PROGRESS:
    return "PROGRESS";
  case CollRingState::FINALIZE:
    return "FINALIZE";
  default:
    return "UNKNOWN";
  }
};

struct CollRingCtx {
  int peers_cnt{0};
  bool in_use{false};
  uint32_t seq{0};
  bool contrib_local{false};
  uint32_t contrib_prev{0};
  uint32_t forward_cnt{0};
  std::vector<bool> contrib_map;
  CollRingState state{CollRingState::SYNC};
  std::string ring_buf;
#ifdef HAVE_PMIX
  // Per-context callback: each in-flight fence carries its own completion
  // callback so that overlapping fences never overwrite each other's cbfunc.
  pmix_modex_cbfunc_t cbfunc{};
  void* cbdata{};
#endif
};

class PmixCollRing : public Coll,
                     public std::enable_shared_from_this<PmixCollRing> {
 public:
#ifdef HAVE_PMIX
  // pmix_client and craned_client are injected so this class never needs to
  // call PmixServer::GetInstance().
  PmixCollRing(const PmixStepInfo& job_info, PmixClient* pmix_client,
               CranedClient* craned_client)
      : m_pmix_step_info_(job_info),
        m_pmix_client_(pmix_client),
        m_craned_client_(craned_client) {}

  bool PmixCollInit(CollType type,
                    const std::vector<pmix_proc_t>& procs) override;

  bool PmixCollContribLocal(const std::string& data, pmix_modex_cbfunc_t cbfunc,
                            void* cbdata) override;

  bool ProcessRingRequest(
      const crane::grpc::pmix::SendPmixRingMsgReq_PmixRingMsgHdr& hdr,
      const std::string& msg) override;

  bool PmixCollTreeChild(const CranedId& peer_host, uint32_t seq,
                         const std::string& data) override {
    CRANE_ERROR("Not implemented");
    return false;
  };

  bool PmixCollTreeParent(const CranedId& peer_host, uint32_t seq,
                          const std::string& data) override {
    CRANE_ERROR("Not implemented");
    return false;
  };

  void AbortOnTimeout() override;

  static void RingReleaseFn(void* rel_data);

 private:
  /* ring coll functions */
  bool PmixCollRingInit_(const std::set<std::string>& hostset);
  bool PmixCollRingLocal_(const std::string& data, pmix_modex_cbfunc_t cbfunc,
                          void* cbdata);

  CollRingCtx* CollRingCtxNew_();

  bool CollRingContrib_(CollRingCtx& coll_ring_ctx, uint32_t contrib_id,
                        uint32_t hop_seq, const std::string& data);

  void ProgressCollectRing_(CollRingCtx& coll_ring_ctx);

  void InvokeCallBackRing_(CollRingCtx& coll_ring_ctx);

  void ResetCollRing_(CollRingCtx& coll_ring_ctx);

  bool PmixCollRingNeighbor_(
      const crane::grpc::pmix::SendPmixRingMsgReq_PmixRingMsgHdr& hdr,
      const std::string& msg);

  struct CbData {
    std::shared_ptr<PmixCollRing> coll;
    CollRingCtx* coll_ring_ctx;
    uint32_t seq;
  };

  PmixStepInfo m_pmix_step_info_;
  PmixClient* m_pmix_client_{nullptr};      // injected, not owned
  CranedClient* m_craned_client_{nullptr};  // injected, not owned

  int m_next_peerid_{};
  CranedId m_next_craned_id_;
  std::array<CollRingCtx, PMIX_COLL_RING_CTX_NUM> m_ctx_array_;
#endif
};

}  // namespace pmix
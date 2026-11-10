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

#include "Collective.h"
#include "StepControlClient.h"
#include "Types.h"
#include "crane/Logger.h"

namespace pmix {

// Forward declarations — full types included in RingCollective.cpp.
class PmixClient;

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

class RingCollective : public Collective,
                       public std::enable_shared_from_this<RingCollective> {
 public:
#ifdef HAVE_PMIX
  // pmix_client and craned_client are injected so this class never needs to
  // call PmixServer::GetInstance().
  RingCollective(const StepInfo& job_info, PmixClient* pmix_client,
                 mpi::StepControlClient* craned_client)
      : m_step_info_(job_info),
        m_pmix_client_(pmix_client),
        m_step_client_(craned_client) {}

  bool Init(CollectiveType type,
            const std::vector<pmix_proc_t>& procs) override;

  bool Contribute(const std::string& data, pmix_modex_cbfunc_t cbfunc,
                  void* cbdata) override;

  bool ProcessRingRequest(
      const crane::grpc::mpi::pmix::SendPmixRingMsgReq_PmixRingMsgHdr& hdr,
      const std::string& msg) override;

  bool TreeCollectiveChild(const CranedId& peer_host, uint32_t seq,
                           const std::string& data) override {
    CRANE_ERROR("Not implemented");
    return false;
  };

  bool TreeCollectiveParent(const CranedId& peer_host, uint32_t seq,
                            const std::string& data) override {
    CRANE_ERROR("Not implemented");
    return false;
  };

  void AbortOnTimeout() override;

  static void RingReleaseFn(void* rel_data);

 private:
  /* ring coll functions */
  bool RingCollectiveInit_(const std::set<std::string>& hostset);

  CollRingCtx* CollRingCtxNew_();

  bool CollRingContrib_(CollRingCtx& coll_ring_ctx, uint32_t contrib_id,
                        uint32_t hop_seq, const std::string& data);

  void ProgressCollectRing_(CollRingCtx& coll_ring_ctx);

  void InvokeCallBackRing_(CollRingCtx& coll_ring_ctx);

  void ResetCollRing_(CollRingCtx& coll_ring_ctx);

  bool RingCollectiveNeighbor_(
      const crane::grpc::mpi::pmix::SendPmixRingMsgReq_PmixRingMsgHdr& hdr,
      const std::string& msg);

  StepInfo m_step_info_;
  PmixClient* m_pmix_client_{nullptr};              // injected, not owned
  mpi::StepControlClient* m_step_client_{nullptr};  // injected, not owned

  int m_next_peerid_{};
  CranedId m_next_craned_id_;
  CranedId m_prev_craned_id_;
  std::array<CollRingCtx, PMIX_COLL_RING_CTX_NUM> m_ctx_array_;

  // Set to true by AbortOnTimeout() to prevent new neighbor-initiated contexts
  // from being activated after the collective has been aborted.  This guards
  // against the race where a peer's ring message arrives just after
  // PMIx_Abort() fires: without this flag the message would activate a fresh
  // ctx with no cbfunc and no local contribution ever coming, leaving it stuck
  // in PROGRESS state and causing PMIx_server_finalize() to block.
  bool m_aborted_{false};
#endif
};

}  // namespace pmix

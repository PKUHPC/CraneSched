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

#include "PmixCollRing.h"

#include "Pmix.h"

namespace pmix {

#ifdef HAVE_PMIX

bool PmixCollRing::PmixCollInit(CollType type,
                                const std::vector<pmix_proc_t>& procs) {
  m_seq_ = 0;
  m_type_ = type;
  m_procs_.assign(procs.begin(), procs.end());

  std::set<std::string> hostname_set;

  for (const auto& proc : procs) {
    if (m_pmix_step_info_.nspace != proc.nspace) return false;

    if (proc.rank == PMIX_RANK_WILDCARD) {
      for (const auto& hostname : m_pmix_step_info_.node_list) {
        hostname_set.emplace(hostname);
      }
    } else {
      if (proc.rank >= m_pmix_step_info_.task_map.size()) {
        CRANE_ERROR("The rank is out of the task number range.");
        return false;
      }
      uint32_t node_id = m_pmix_step_info_.task_map[proc.rank];
      hostname_set.insert(m_pmix_step_info_.node_list[node_id]);
    }
  }

  m_peers_cnt_ = static_cast<uint32_t>(hostname_set.size());

  if (m_peers_cnt_ == 0) {
    CRANE_ERROR("No peers found");
    return false;
  }

  auto it = hostname_set.find(m_pmix_step_info_.hostname);
  if (it == hostname_set.end()) {
    CRANE_ERROR("unknown hostname: {}", m_pmix_step_info_.hostname);
    return false;
  }
  m_peerid_ = static_cast<int>(std::distance(hostname_set.begin(), it));

  auto next_iter = std::next(it);
  if (next_iter == hostname_set.end()) {
    next_iter = hostname_set.begin();
  }
  m_next_craned_id_ = *next_iter;

  for (int i = 0; i < PMIX_COLL_RING_CTX_NUM; i++) {
    CollRingCtx& coll_ctx = m_ctx_array_[i];
    coll_ctx.in_use = false;
    coll_ctx.seq = m_seq_;
    coll_ctx.contrib_local = false;
    coll_ctx.contrib_prev = 0;
    coll_ctx.state = CollRingState::SYNC;
    coll_ctx.contrib_map = std::vector(m_peers_cnt_, false);
  }

  CRANE_TRACE("ring coll {:p}: crane_id = {}, m_next_craned_id_ = {}",
              static_cast<void*>(this), m_pmix_step_info_.hostname,
              m_next_craned_id_);

  return true;
}

bool PmixCollRing::PmixCollContribLocal(const std::string& data,
                                        pmix_modex_cbfunc_t cbfunc,
                                        void* cbdata) {
  std::lock_guard lock_guard(this->m_lock_);

  CollRingCtx* coll_ctx = this->CollRingCtxNew_();
  if (coll_ctx == nullptr) {
    CRANE_ERROR("{:p}: Can not get new ring collective context, seq= {}",
                static_cast<void*>(this), this->m_seq_);
    return false;
  }

  /* setup per-context callback info */
  coll_ctx->cbfunc = cbfunc;
  coll_ctx->cbdata = cbdata;

  CRANE_DEBUG("{:p}: contrib/loc: seq_num={}, state={}, size={}",
              static_cast<void*>(coll_ctx), coll_ctx->seq,
              ToString(coll_ctx->state), data.size());

  // contrib peer node
  if (!CollRingContrib_(*coll_ctx, m_peerid_, 0, data)) {
    ResetCollRing_(*coll_ctx);
    return false;
  }

  coll_ctx->contrib_local = true;
  this->ProgressCollectRing_(*coll_ctx);

  return true;
}

CollRingCtx* PmixCollRing::CollRingCtxNew_() {
  uint32_t seq = this->m_seq_;
  CollRingCtx* res_ctx = nullptr;
  CollRingCtx* free_ring_ctx = nullptr;
  CollRingCtx* coll_ring_ctx = nullptr;

  for (int i = 0; i < PMIX_COLL_RING_CTX_NUM; i++) {
    coll_ring_ctx = &m_ctx_array_[i];

    if (coll_ring_ctx->in_use) {
      switch (coll_ring_ctx->state) {
      case CollRingState::FINALIZE:
        seq++;
        break;
      case CollRingState::SYNC:
      case CollRingState::PROGRESS:
        if (res_ctx == nullptr && !coll_ring_ctx->contrib_local)
          res_ctx = coll_ring_ctx;
        break;
      }
    } else {
      free_ring_ctx = coll_ring_ctx;
    }
  }

  if (res_ctx == nullptr && free_ring_ctx != nullptr) {
    res_ctx = free_ring_ctx;
    res_ctx->in_use = true;
    res_ctx->seq = seq;
    res_ctx->ring_buf.clear();
  }

  return res_ctx;
}

bool PmixCollRing::CollRingContrib_(CollRingCtx& coll_ring_ctx,
                                    uint32_t contrib_id, uint32_t hop_seq,
                                    const std::string& data) {
  m_ts_ = std::chrono::steady_clock::now();
  coll_ring_ctx.ring_buf.append(data);

  /* check for ring is complete */
  if (contrib_id != (m_peerid_ + 1) % m_peers_cnt_) {
    // forward data to the next node
    crane::grpc::pmix::SendPmixRingMsgReq request{};

    auto* pmix_ring_msg_hdr = request.mutable_pmix_ring_msg_hdr();
    pmix_ring_msg_hdr->set_msgsize(data.size());
    pmix_ring_msg_hdr->set_craned_id(m_pmix_step_info_.hostname);
    pmix_ring_msg_hdr->set_seq(coll_ring_ctx.seq);
    pmix_ring_msg_hdr->set_contrib_id(contrib_id);
    pmix_ring_msg_hdr->set_hop_seq(hop_seq);

    for (auto& proc : m_procs_) {
      auto* pmix_procs = request.mutable_pmix_procs()->Add();
      pmix_procs->set_nspace(proc.nspace);
      pmix_procs->set_rank(proc.rank);
    }

    request.set_msg(coll_ring_ctx.ring_buf);

    CRANE_DEBUG(
        "coll_ctx {:p}: transit data to nodeid={}, seq={}, hop={}, size={}, "
        "contrib={}",
        static_cast<void*>(&coll_ring_ctx), m_next_craned_id_,
        coll_ring_ctx.seq, hop_seq, data.size(), contrib_id);

    auto stub = m_pmix_client_->GetPmixStub(m_next_craned_id_);
    if (!stub) {
      CRANE_ERROR("{:p}, PmixStub is not get, cannot forward ring data",
                  static_cast<void*>(&coll_ring_ctx));
      coll_ring_ctx.ring_buf.clear();
      return false;
    }

    auto self = shared_from_this();
    stub->SendPmixRingMsgNoBlock(
        request, [seq = coll_ring_ctx.seq, &coll_ring_ctx, self](bool ok) {
          std::lock_guard lock_guard(self->m_lock_);
          if (!ok) {
            CRANE_ERROR("{:p}, Cannot forward ring data",
                        static_cast<void*>(&coll_ring_ctx));
            coll_ring_ctx.ring_buf.clear();
            if (coll_ring_ctx.cbfunc) {
              PmixLibModexInvoke(coll_ring_ctx.cbfunc, PMIX_ERR_TIMEOUT,
                                 nullptr, 0, coll_ring_ctx.cbdata, nullptr,
                                 nullptr);
              coll_ring_ctx.cbfunc = nullptr;
              coll_ring_ctx.cbdata = nullptr;
            }
            return;
          }

          CRANE_DEBUG("{:p}: called {}", static_cast<void*>(&coll_ring_ctx),
                      coll_ring_ctx.seq);
          if (seq != coll_ring_ctx.seq) {
            CRANE_DEBUG("{:p}: collective was reset!",
                        static_cast<void*>(&coll_ring_ctx));
            coll_ring_ctx.ring_buf.clear();
            return;
          }

          coll_ring_ctx.forward_cnt++;
          self->ProgressCollectRing_(coll_ring_ctx);
          coll_ring_ctx.ring_buf.clear();
        });
  }

  return true;
}

void PmixCollRing::ProgressCollectRing_(CollRingCtx& coll_ring_ctx) {
  bool result = false;

  if (!coll_ring_ctx.in_use) {
    CRANE_ERROR("{:p}: coll_ring_ctx is not in use, seq={}, please check!",
                static_cast<void*>(this), coll_ring_ctx.seq);
    m_craned_client_->TerminateSteps();
    return;
  }

  CRANE_TRACE("{:p}: ProgressCollectRing_ is called, seq={}, state={}",
              static_cast<void*>(this), coll_ring_ctx.seq,
              ToString(coll_ring_ctx.state));
  do {
    result = false;
    switch (coll_ring_ctx.state) {
    case CollRingState::SYNC:
      CRANE_TRACE("{:p}: SYNC state is processing, seq={}",
                  static_cast<void*>(this), coll_ring_ctx.seq);
      if (coll_ring_ctx.contrib_local || coll_ring_ctx.contrib_prev != 0) {
        coll_ring_ctx.state = CollRingState::PROGRESS;
        result = true;
      }
      break;
    case CollRingState::PROGRESS:
      CRANE_TRACE("{:p}: PROGRESS state is processing, seq={}",
                  static_cast<void*>(this), coll_ring_ctx.seq);
      /* check for all data is collected and forwarded */
      if (m_peers_cnt_ - (coll_ring_ctx.contrib_prev +
                          static_cast<uint32_t>(coll_ring_ctx.contrib_local)) ==
          0) {
        coll_ring_ctx.state = CollRingState::FINALIZE;
        InvokeCallBackRing_(coll_ring_ctx);
        result = true;
      }
      break;
    case CollRingState::FINALIZE:
      if (m_peers_cnt_ - coll_ring_ctx.forward_cnt - 1 == 0) {
        CRANE_DEBUG("{:p}: seq={} is DONE", static_cast<void*>(this), m_seq_);
        this->m_seq_++;  // Only when the sequence is done, the coll sequence is
                         // incremented by 1.
        ResetCollRing_(coll_ring_ctx);
        result = true;
      }
      break;
    default:
      CRANE_ERROR("Unknown state: {}", ToString(coll_ring_ctx.state));
      break;
    }
  } while (result);
}

void PmixCollRing::AbortOnTimeout() {
  std::lock_guard lock(m_lock_);

  // Check whether any context is actively progressing (not just idle SYNC).
  bool any_active =
      std::ranges::any_of(m_ctx_array_, [](const CollRingCtx& ctx) {
        return ctx.in_use && ctx.state != CollRingState::SYNC;
      });

  if (!any_active) return;

  CRANE_ERROR("coll {:p}: ring collective timed out (seq={})",
              static_cast<void*>(this), m_seq_);

  // Fire each in-use context's own callback with a timeout error so every
  // overlapping fence's MPI process is not left waiting forever.
  for (auto& ctx : m_ctx_array_) {
    if (!ctx.in_use) continue;
    if (ctx.cbfunc) {
      ctx.cbfunc(PMIX_ERR_TIMEOUT, nullptr, 0, ctx.cbdata, nullptr, nullptr);
      ctx.cbfunc = nullptr;
      ctx.cbdata = nullptr;
    }
    ResetCollRing_(ctx);
  }

  // Clear the activity timestamp so IsTimedOut() returns false for the
  // freshly reset (SYNC) state.
  m_ts_ = {};
}

void PmixCollRing::RingReleaseFn(void* rel_data) {
  auto* cb_data = static_cast<CbData*>(rel_data);

  std::lock_guard lock_guard(cb_data->coll->m_lock_);
  // Guard against a recycled context: only clear ring_buf when the context
  // slot still belongs to the same fence sequence.  If the slot has been
  // reused by a newer collective, touching ring_buf would corrupt its data.
  if (cb_data->coll_ring_ctx->seq == cb_data->seq) {
    cb_data->coll_ring_ctx->ring_buf.clear();
  }

  delete cb_data;
}

void PmixCollRing::InvokeCallBackRing_(CollRingCtx& coll_ring_ctx) {
  CRANE_DEBUG("{:p}: InvokeCallBackRing_ is called, seq={}",
              static_cast<void*>(this), coll_ring_ctx.seq);
  if (!coll_ring_ctx.cbfunc) return;

  auto cb_data = std::make_unique<CbData>();
  cb_data->coll = shared_from_this();
  cb_data->coll_ring_ctx = &coll_ring_ctx;
  cb_data->seq = coll_ring_ctx.seq;

  PmixLibModexInvoke(coll_ring_ctx.cbfunc, PMIX_SUCCESS,
                     coll_ring_ctx.ring_buf.data(),
                     coll_ring_ctx.ring_buf.size(), coll_ring_ctx.cbdata,
                     RingReleaseFn, cb_data.release());

  coll_ring_ctx.cbfunc = nullptr;
  coll_ring_ctx.cbdata = nullptr;
}

void PmixCollRing::ResetCollRing_(CollRingCtx& coll_ring_ctx) {
  coll_ring_ctx.in_use = false;
  coll_ring_ctx.state = CollRingState::SYNC;
  coll_ring_ctx.contrib_local = false;
  coll_ring_ctx.contrib_prev = 0;
  coll_ring_ctx.forward_cnt = 0;
  // Only reset the shared activity timestamp when every context slot is
  // now idle.  If another fence is still in progress, preserve m_ts_ so
  // IsTimedOut() continues to track it.  CollRingContrib_() refreshes
  // m_ts_ whenever a new contribution arrives.
  if (std::ranges::none_of(m_ctx_array_,
                           [](const CollRingCtx& c) { return c.in_use; })) {
    m_ts_ = {};
  }
  coll_ring_ctx.cbfunc = nullptr;
  coll_ring_ctx.cbdata = nullptr;
  coll_ring_ctx.contrib_map.assign(m_peers_cnt_, false);
  coll_ring_ctx.ring_buf.clear();
}

bool PmixCollRing::ProcessRingRequest(
    const crane::grpc::pmix::SendPmixRingMsgReq_PmixRingMsgHdr& hdr,
    const std::string& msg) {
  std::lock_guard lock_guard(this->m_lock_);

  CRANE_DEBUG(
      "collective message from nodeid={}, contrib_id={}, seq={}, hop={}, "
      "msgsize={}",
      hdr.craned_id(), hdr.contrib_id(), hdr.seq(), hdr.hop_seq(),
      hdr.msgsize());

  // Guard against uint32_t underflow when m_seq_ == 0
  if (m_seq_ > 0 && m_seq_ - 1 == hdr.seq()) {
    CRANE_DEBUG("{:p}: stale contrib from {}, coll->seq={}, seq={}",
                static_cast<void*>(this), hdr.craned_id(), m_seq_, hdr.seq());
    return false;
  }

  if (m_seq_ != hdr.seq() && m_seq_ + 1 != hdr.seq()) {
    /* this is an unacceptable event: either something went
     * really wrong or the state machine is incorrect.
     * This will 100% lead to application hang.
     */
    CRANE_DEBUG("{:p}: unexpected contrib from {}, coll->seq={}, seq={}",
                static_cast<void*>(this), hdr.craned_id(), m_seq_, hdr.seq());
    m_craned_client_->TerminateSteps();
    return false;
  }

  if (!PmixCollRingNeighbor_(hdr, msg)) return false;

  return true;
}

bool PmixCollRing::PmixCollRingNeighbor_(
    const crane::grpc::pmix::SendPmixRingMsgReq_PmixRingMsgHdr& hdr,
    const std::string& msg) {
  CollRingCtx* coll_ring_ctx = nullptr;

  for (size_t i = 0; i < PMIX_COLL_RING_CTX_NUM; i++) {
    CollRingCtx& ctx = this->m_ctx_array_[i];
    if (ctx.in_use && ctx.seq == hdr.seq()) {
      coll_ring_ctx = &ctx;
      break;
    }
    if (!ctx.in_use) coll_ring_ctx = &ctx;
  }

  if (coll_ring_ctx != nullptr && !coll_ring_ctx->in_use) {
    coll_ring_ctx->in_use = true;
    coll_ring_ctx->seq = hdr.seq();
    coll_ring_ctx->ring_buf.clear();
  }

  if (coll_ring_ctx == nullptr) {
    CRANE_ERROR("{:p}: Can not get ring collective context, seq={}",
                static_cast<void*>(this), hdr.seq());
    return false;
  }

  CRANE_DEBUG(
      "{:p}: contrib/nbr: seq_num={}, state={}, nodeid={}, contrib={}, "
      "hop_seq={}, size={}",
      static_cast<void*>(&coll_ring_ctx), coll_ring_ctx->seq,
      ToString(coll_ring_ctx->state), hdr.craned_id(), hdr.contrib_id(),
      hdr.hop_seq(), hdr.msgsize());

  /* compute the actual hops of ring: (src - dst + size) % size
   * dist = (peerid - contrib_id + peers_cnt) % peers_cnt, then hop_seq = dist
   * - 1. Guard against underflow: dist == 0 means self-loop which must not
   * happen. */
  uint32_t dist = (m_peerid_ + m_peers_cnt_ - hdr.contrib_id()) % m_peers_cnt_;
  if (dist == 0) {
    CRANE_ERROR("{:p}: received own contribution from {}, ignoring",
                static_cast<void*>(this), hdr.craned_id());
    return false;
  }
  uint32_t hop_seq = dist - 1;
  if (hop_seq != hdr.hop_seq()) {
    CRANE_DEBUG("{:p}: unexpected ring hop_seq={}, expect={}, coll seq={}",
                static_cast<void*>(this), hdr.hop_seq(), hop_seq, hdr.seq());
    return false;
  }

  if (hdr.contrib_id() >= m_peers_cnt_) return false;

  if (coll_ring_ctx->contrib_map[hdr.contrib_id()]) {
    CRANE_DEBUG(
        "coll {:p}: double receiving was detected from {}, "
        "local seq={}, seq={}, rejected",
        static_cast<void*>(this), hdr.contrib_id(), m_seq_, hdr.seq());
    return false;
  }

  coll_ring_ctx->contrib_map[hdr.contrib_id()] = true;

  if (!CollRingContrib_(*coll_ring_ctx, hdr.contrib_id(), hdr.hop_seq() + 1,
                        msg))
    return false;

  coll_ring_ctx->contrib_prev++;

  ProgressCollectRing_(*coll_ring_ctx);

  return true;
}

#endif

}  // namespace pmix
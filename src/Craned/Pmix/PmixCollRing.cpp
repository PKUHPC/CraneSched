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

#include "CranedClient.h"
#include "CranedPublicDefs.h"
#include "Pmix.h"
#include "PmixColl.h"
#include "PmixState.h"
#include "crane/Logger.h"

namespace pmix {

bool Coll::PmixCollRingInit_(const std::set<std::string>& hostset) {

  auto iter = hostset.find(g_pmix_server->GetHostname());
  if (iter != hostset.end()) {
    auto next_iter = std::next(iter);
    if (next_iter == hostset.end()) {
      next_iter = hostset.begin();
    }
    m_ring_.m_next_craned_id_ = *next_iter;
  } else {
    CRANE_ERROR("cannot find hostname: {}", g_pmix_server->GetHostname());
    return false;
  }

  for (int i = 0; i<PMIX_COLL_RING_CTX_NUM; i++) {
    CollRingCtx &coll_ctx = m_ring_.m_ctx_array_[i];
    coll_ctx.m_in_use_ = false;
    coll_ctx.m_seq_ = m_seq_;
    coll_ctx.m_contrib_local_ = false;
    coll_ctx.m_contrib_prev_ = 0;
    coll_ctx.m_state_ = CollRingState::SYNC;
    coll_ctx.m_contrib_map_ = std::vector<bool>(m_peers_cnt_, false);
  }

  return true;
}

bool Coll::PmixCollRingLocal_(const std::string& data,
                              pmix_modex_cbfunc_t cbfunc, void* cbdata) {
  std::lock_guard lock_guard(this->m_lock_);

  /* setup callback info */
  this->m_cbfunc_ = cbfunc;
  this->m_cbdata_ = cbdata;

  CollRingCtx* coll_ctx = this->CollRingCtxNew();
  if (!coll_ctx) {
    CRANE_ERROR("{:p}: Can not get new ring collective context, seq= {}",
                static_cast<void*>(this), this->m_seq_);
    return false;
  }

  CRANE_DEBUG("{:p}: contrib/loc: seq_num={}, state={}, size={}", static_cast<void*>(coll_ctx), coll_ctx->m_seq_, static_cast<int>(coll_ctx->m_state_), data.size());

  // contrib peer node
  CollRingContrib(*coll_ctx, m_peerid_, 0, data);

  coll_ctx->m_contrib_local_ = true;
  this->ProgressCollectRing_(*coll_ctx);

  return true;
}

Coll::CollRingCtx* Coll::CollRingCtxNew() {
  uint32_t seq = this->m_seq_;
  CollRingCtx* res_ctx = nullptr, *free_ring_ctx = nullptr,
              *coll_ring_ctx = nullptr;

  for (int i = 0; i < PMIX_COLL_RING_CTX_NUM; i++) {
    coll_ring_ctx = &m_ring_.m_ctx_array_[i];

    if (coll_ring_ctx->m_in_use_) {
      switch (coll_ring_ctx->m_state_) {
      case CollRingState::FINALIZE:
        seq++;
        break;
      case CollRingState::SYNC:
      case CollRingState::PROGRESS:
        if (!res_ctx && !coll_ring_ctx->m_contrib_local_)
          res_ctx = coll_ring_ctx;
        break;
      }
    } else {
      free_ring_ctx = coll_ring_ctx;
    }
  }

  if (!res_ctx && free_ring_ctx) {
    res_ctx = free_ring_ctx;
    res_ctx->m_in_use_ = true;
    res_ctx->m_seq_ = seq;
    res_ctx->m_ring_buf_.clear();
  }

  return res_ctx;
}

bool Coll::CollRingContrib(CollRingCtx& coll_ring_ctx, int contrib_id,
                           uint32_t hop_seq, const std::string& data) {

  m_ts_ = time(nullptr);
  coll_ring_ctx.m_ring_buf_.append(data);

  /* check for ring is complete */
  if (contrib_id != ((m_peerid_+1) % m_peers_cnt_)) {
    // forward data to the next node
    crane::grpc::SendPmixRingMsgReq request{};

    auto* pmix_ring_msg_hdr = request.mutable_pmix_ring_msg_hdr();
    pmix_ring_msg_hdr->set_msgsize(data.size());
    pmix_ring_msg_hdr->set_craned_id(g_pmix_server->GetHostname());
    pmix_ring_msg_hdr->set_seq(coll_ring_ctx.m_seq_);
    pmix_ring_msg_hdr->set_contrib_id(contrib_id);
    pmix_ring_msg_hdr->set_hop_seq(hop_seq);

    for (size_t i = 0; i < m_pset_.m_nprocs_; i++) {
      auto proc = m_pset_.m_procs_[i];
      auto* pmix_procs = request.mutable_pmix_procs()->Add();
      pmix_procs->set_nspace(proc.nspace);
      pmix_procs->set_rank(proc.rank);
    }

    request.set_msg(coll_ring_ctx.m_ring_buf_);

    CRANE_DEBUG("coll_ctx {:p}: transit data to nodeid={}, seq={}, hop={}, size={}, contrib={}", static_cast<void*>(&coll_ring_ctx), m_ring_.m_next_craned_id_, coll_ring_ctx.m_seq_, hop_seq, data.size(), contrib_id);

    auto context = std::make_shared<grpc::ClientContext>();
    auto reply = std::make_shared<crane::grpc::SendPmixRingMsgReply>();

    g_craned_client->GetCranedStub(m_ring_.m_next_craned_id_)->SendPmixRingMsg(
            context.get(), std::move(request), reply.get(),
            [context, reply, seq = coll_ring_ctx.m_seq_, &coll_ring_ctx, this](grpc::Status status) {
              std::lock_guard lock_guard(this->m_lock_);
              if (!status.ok()) {
                CRANE_ERROR("{:p}, Cannot forward ring data", static_cast<void*>(&coll_ring_ctx));
                coll_ring_ctx.m_ring_buf_.clear();
                return;
              }

              CRANE_DEBUG("{:p}: called {}" ,static_cast<void*>(&coll_ring_ctx), coll_ring_ctx.m_seq_);
              if (seq != coll_ring_ctx.m_seq_) {
                CRANE_DEBUG("{:p}: collective was reset!", static_cast<void*>(&coll_ring_ctx));
                coll_ring_ctx.m_ring_buf_.clear();
                return;
              }

              coll_ring_ctx.m_forward_cnt_++;
              this->ProgressCollectRing_(coll_ring_ctx);
              coll_ring_ctx.m_ring_buf_.clear();
            });
  }

  return true;
}

void Coll::ProgressCollectRing_(CollRingCtx& coll_ring_ctx) {
  bool result = false;

  assert(coll_ring_ctx.m_in_use_);

  do {
    result = false;
    switch (coll_ring_ctx.m_state_) {
    case CollRingState::SYNC:
      if (coll_ring_ctx.m_contrib_local_ || coll_ring_ctx.m_contrib_prev_) {
        coll_ring_ctx.m_state_ = CollRingState::PROGRESS;
        result = true;
      }
      break;
    case CollRingState::PROGRESS:
      /* check for all data is collected and forwarded */
        if (!(m_peers_cnt_ -
            (coll_ring_ctx.m_contrib_prev_ + coll_ring_ctx.m_contrib_local_))) {
        coll_ring_ctx.m_state_ = CollRingState::FINALIZE;
        InvokeCallBackRing(coll_ring_ctx);
        result = true;
      }
      break;
    case CollRingState::FINALIZE:
      if (!(m_peers_cnt_ - coll_ring_ctx.m_forward_cnt_ - 1)) {
        CRANE_DEBUG("{:p}: seq={} is DONE", static_cast<void*>(this), m_seq_);
        this->m_seq_++; // Only when the sequence is done, the coll sequence is incremented by 1.
        ResetCollRing(coll_ring_ctx);
        result = true;
      }
      break;
    default:
      // CRANE_ERROR("Unknown state: {}", coll_ring_ctx.m_state_);
      break;
    }
  } while (result);
}

void Coll::RingReleaseFn(void* rel_data) {
  auto* cb_data = static_cast<CbData*>(rel_data);

  std::lock_guard lock_guard(cb_data->coll->m_lock_);
  cb_data->coll_ring_ctx->m_ring_buf_.clear();

  delete cb_data;
}

void Coll::InvokeCallBackRing(CollRingCtx& coll_ring_ctx) {

  if (!m_cbfunc_)
      return ;

  auto cb_data = std::make_unique<CbData>();
  cb_data->coll = this;
  cb_data->coll_ring_ctx = &coll_ring_ctx;

  PmixLibModexInvoke(m_cbfunc_, PMIX_SUCCESS, coll_ring_ctx.m_ring_buf_.data(),
          coll_ring_ctx.m_ring_buf_.size(), m_cbdata_, (void*)(Coll::RingReleaseFn), cb_data.release());

  m_cbfunc_ = nullptr;
  m_cbdata_ = nullptr;
}

void Coll::ResetCollRing(CollRingCtx& coll_ring_ctx) {
  coll_ring_ctx.m_in_use_ = false;
  coll_ring_ctx.m_state_ = CollRingState::SYNC;
  coll_ring_ctx.m_contrib_local_ = false;
  coll_ring_ctx.m_contrib_prev_ = 0;
  coll_ring_ctx.m_forward_cnt_ = 0;
  m_ts_ = time(nullptr);
  coll_ring_ctx.m_contrib_map_.assign(m_peers_cnt_, false);
  coll_ring_ctx.m_ring_buf_.clear();
}

bool Coll::ProcessRingRequest(
    const crane::grpc::SendPmixRingMsgReq_PmixRingMsgHdr& hdr,
    const std::vector<pmix_proc_t>& procs, const std::string& msg) {

  CRANE_DEBUG("collective message from nodeid={}, contrib_id={}, seq={}, hop={}, msgsize={}", hdr.craned_id(), hdr.contrib_id(), hdr.seq(), hdr.hop_seq(), hdr.msgsize());

  std::lock_guard lock_guard(this->m_lock_);

  if ((m_seq_-1) == hdr.seq()) {
    CRANE_DEBUG("{:p}: unexpected contrib from {}, coll->seq={}, seq={}", static_cast<void*>(this), hdr.craned_id(), m_seq_, hdr.seq());
    return false;
  } else if (m_seq_ != hdr.seq() && (m_seq_+1) != hdr.seq()) {
    /* this is an unacceptable event: either something went
     * really wrong or the state machine is incorrect.
     * This will 100% lead to application hang.
     */
    CRANE_DEBUG("{:p}: unexpected contrib from {}, coll->seq={}, seq={}", static_cast<void*>(this), hdr.craned_id(), m_seq_, hdr.seq());
    // TODO: kill job
    return false;
  }

  if (!PmixCollRingNeighbor_(hdr, msg)) return false;

  return true;
}

bool Coll::PmixCollRingNeighbor_(
    const crane::grpc::SendPmixRingMsgReq_PmixRingMsgHdr& hdr,
    const std::string& msg) {

  CollRingCtx *coll_ring_ctx = nullptr;

  for (size_t i = 0; i < PMIX_COLL_RING_CTX_NUM; i++) {
    CollRingCtx &ctx = this->m_ring_.m_ctx_array_[i];
    if (ctx.m_in_use_ && ctx.m_seq_ == hdr.seq()) {
       coll_ring_ctx = &ctx;
       break;
    }
    if (!ctx.m_in_use_) {
      coll_ring_ctx = &ctx;
      continue;
    }
  }

  if (coll_ring_ctx && !coll_ring_ctx->m_in_use_) {
    coll_ring_ctx->m_in_use_ = true;
    coll_ring_ctx->m_seq_ = hdr.seq();
    coll_ring_ctx->m_ring_buf_.clear();
  }

  if (!coll_ring_ctx) {
    CRANE_ERROR("{:p}: Can not get ring collective context, seq={}", static_cast<void*>(this), hdr.seq());
    return false;
  }

  CRANE_DEBUG("{:p}: contrib/nbr: seq_num={}, state={}, nodeid={}, contrib={}, hop_seq={}, size={}",
                  static_cast<void*>(&coll_ring_ctx),
                  coll_ring_ctx->m_seq_, static_cast<int>(coll_ring_ctx->m_state_), hdr.craned_id(),
                  hdr.contrib_id(), hdr.hop_seq(), hdr.msgsize());

  /* compute the actual hops of ring: (src - dst + size) % size */
  uint32_t hop_seq = (m_peerid_ + m_peers_cnt_ - hdr.contrib_id()) % m_peers_cnt_ - 1;
  if (hop_seq != hdr.hop_seq()) {
    CRANE_DEBUG("{:p}: unexpected ring seq number={}, expect={}, coll seq={}", static_cast<void*>(this), hdr.hop_seq(), hop_seq, hdr.seq(), m_seq_);
    return false;
  }

  if (hdr.contrib_id() >= m_peers_cnt_) return false;

  if (coll_ring_ctx->m_contrib_map_[hdr.contrib_id()]) {
    CRANE_DEBUG("coll {:p}: double receiving was detected from {}, "
                            "local seq={}, seq={}, rejected", static_cast<void*>(this), hdr.contrib_id(), m_seq_, hdr.seq());
    return false;
  }

  coll_ring_ctx->m_contrib_map_[hdr.contrib_id()] = true;

  CollRingContrib(*coll_ring_ctx, hdr.contrib_id(), hdr.hop_seq()+1, msg);

  coll_ring_ctx->m_contrib_prev_++;

  ProgressCollectRing_(*coll_ring_ctx);

  return true;
}

}


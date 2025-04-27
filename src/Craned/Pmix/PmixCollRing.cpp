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

bool Coll::PmixCollRingInit_(std::set<std::string> hostset) {
  bool is_next = false;

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

  m_ring_.m_ctx_array_.resize(PMIX_COLL_RING_CTX_NUM);
  for (int i = 0; i<PMIX_COLL_RING_CTX_NUM; i++) {
    m_ring_.m_ctx_array_[i] = std::make_shared<CollRingCtx>();
    auto coll_ctx = m_ring_.m_ctx_array_[i];
    coll_ctx->m_in_use_ = false;
    coll_ctx->m_seq_ = m_seq_;
    coll_ctx->m_contrib_local_ = false;
    coll_ctx->m_contrib_prev_ = 0;
    coll_ctx->m_state_ = CollRingState::SYNC;
    coll_ctx->m_contrib_map_ = std::vector<bool>(m_peers_cnt_, false);
  }

  return true;
}

bool Coll::PmixCollRingLocal_(const std::string& data, size_t size,
                              pmix_modex_cbfunc_t cbfunc, void* cbdata) {
  std::lock_guard<std::mutex> lock_guard(this->m_lock_);

  /* setup callback info */
  this->m_cbfunc_ = cbfunc;
  this->m_cbdata_ = cbdata;

  std::shared_ptr<CollRingCtx> collCtx = this->CollRingCtxNew();
  if (!collCtx) {
    CRANE_ERROR("Can not get new ring collective context, seq= {}",
                this->m_seq_);
    return false;
  }

  if (!CollRingContrib(collCtx, m_peerid_, 0, data, size)) return false;

  collCtx->m_contrib_local_ = true;
  this->ProgressCollectRing_(collCtx);

  return true;
}

std::shared_ptr<Coll::CollRingCtx> Coll::CollRingCtxNew() {
  uint32_t seq = this->m_seq_;
  std::shared_ptr<CollRingCtx> res_ctx = nullptr, free_ring_ctx = nullptr,
              coll_ring_ctx = nullptr;

  for (int i = 0; i < PMIX_COLL_RING_CTX_NUM; i++) {
    coll_ring_ctx = m_ring_.m_ctx_array_[i];

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
    res_ctx->m_ring_buf_ = "";
  }

  return res_ctx;
}

bool Coll::CollRingContrib(const std::shared_ptr<CollRingCtx>& coll_ring_ctx, int contrib_id,
                           uint32_t hop_seq, const std::string& data, size_t size) {

  m_ts_ = time(nullptr);
  coll_ring_ctx->m_ring_buf_.append(data);

  /* check for ring is complete */
  if (contrib_id != (m_peerid_+1) % m_peers_cnt_) {
    // forward data to the next node
    crane::grpc::SendPmixRingMsgReq request{};

    auto* pmix_ring_msg_hdr = request.mutable_pmix_ring_msg_hdr();
    pmix_ring_msg_hdr->set_msgsize(size);
    pmix_ring_msg_hdr->set_craned_id(m_ring_.m_next_craned_id_);
    pmix_ring_msg_hdr->set_seq(coll_ring_ctx->m_seq_);
    pmix_ring_msg_hdr->set_contrib_id(contrib_id);
    pmix_ring_msg_hdr->set_hop_seq(hop_seq);

    for (size_t i = 0; i < m_pset_.m_nprocs_; i++) {
      auto proc = m_pset_.m_procs_[i];
      auto* pmix_procs = request.mutable_pmix_procs()->Add();
      pmix_procs->set_nspace(proc.nspace);
      pmix_procs->set_rank(proc.rank);
    }

    request.set_msg(coll_ring_ctx->m_ring_buf_);

    bool result = g_craned_client->GetCranedStub(m_ring_.m_next_craned_id_)->SendPmixRingMsg(std::move(request));
    if (!result) {
      CRANE_ERROR("Cannot forward ring data");
      return false;
    }

    coll_ring_ctx->m_forward_cnt_++;
    ProgressCollectRing_(coll_ring_ctx);
  }

  return true;
}

void Coll::ProgressCollectRing_(const std::shared_ptr<CollRingCtx> coll_ring_ctx) {
  bool result = false;

  do {
    result = false;
    switch (coll_ring_ctx->m_state_) {
    case CollRingState::SYNC:
      if (coll_ring_ctx->m_contrib_local_ || coll_ring_ctx->m_contrib_prev_) {
        coll_ring_ctx->m_state_ = CollRingState::PROGRESS;
        result = true;
      }
      break;
    case CollRingState::PROGRESS:
      /* check for all data is collected and forwarded */
      if (!m_peers_cnt_ -
          (coll_ring_ctx->m_contrib_prev_ + coll_ring_ctx->m_contrib_local_)) {
        coll_ring_ctx->m_state_ = CollRingState::FINALIZE;
        InvokeCallBackRing(coll_ring_ctx);
        result = true;
      }
      break;
    case CollRingState::FINALIZE:
      if (!(m_peers_cnt_ - coll_ring_ctx->m_forward_cnt_ - 1)) {
        this->m_seq_++;
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

void Coll::InvokeCallBackRing(std::shared_ptr<CollRingCtx> coll_ring_ctx) {

  if (!m_cbfunc_)
      return ;

  PmixLibModexInvoke(m_cbfunc_, PMIX_SUCCESS, coll_ring_ctx->m_ring_buf_.c_str(),
          coll_ring_ctx->m_ring_buf_.size(), m_cbdata_, nullptr, nullptr);

  m_cbfunc_ = nullptr;
  m_cbdata_ = nullptr;

}

void Coll::ResetCollRing(std::shared_ptr<CollRingCtx> coll_ring_ctx) {
  coll_ring_ctx->m_in_use_ = false;
  coll_ring_ctx->m_state_ = CollRingState::SYNC;
  coll_ring_ctx->m_contrib_local_ = false;
  coll_ring_ctx->m_contrib_prev_ = 0;
  coll_ring_ctx->m_forward_cnt_ = 0;
  m_ts_ = time(nullptr);
  coll_ring_ctx->m_contrib_map_.resize(m_peers_cnt_, false);
  coll_ring_ctx->m_ring_buf_ = "";
}

bool Coll::ProcessRingRequest(
    const crane::grpc::SendPmixRingMsgReq_PmixRingMsgHdr& hdr,
    const std::vector<pmix_proc_t>& procs, const std::string& msg) {


  if (!PmixCollRingNeighbor_(hdr, msg)) return false;

  return true;
}

bool Coll::PmixCollRingNeighbor_(
    const crane::grpc::SendPmixRingMsgReq_PmixRingMsgHdr& hdr,
    const std::string& msg) {

  std::lock_guard<std::mutex> lock_guard(this->m_lock_);

  std::shared_ptr<CollRingCtx> coll_ring_ctx = nullptr;

  for (size_t i = 0; i < PMIX_COLL_RING_CTX_NUM; i++) {
    auto ctx = this->m_ring_.m_ctx_array_[i];
    if (ctx->m_in_use_ && ctx->m_seq_ == hdr.seq()) {
       coll_ring_ctx = ctx;
    } else if (!ctx->m_in_use_) {
      coll_ring_ctx = ctx;
      continue;
    }
  }

  if (coll_ring_ctx && !coll_ring_ctx->m_in_use_) {
    coll_ring_ctx->m_in_use_ = true;
    coll_ring_ctx->m_seq_ = hdr.seq();
    coll_ring_ctx->m_ring_buf_ = "";
  }

  if (!coll_ring_ctx) {
    CRANE_ERROR("Can not get ring collective context, seq={}", hdr.seq());
    return false;
  }

  /* compute the actual hops of ring: (src - dst + size) % size */
  uint32_t hop_seq = (m_peerid_ + m_peers_cnt_ - hdr.contrib_id()) % m_peers_cnt_ - 1;
  if (hop_seq != hdr.hop_seq()) {
    CRANE_DEBUG("unexpected ring seq number={}, expect={}, coll seq={}", hdr.hop_seq(), hop_seq, hdr.seq(), m_seq_);
    return false;
  }

  if (hdr.contrib_id() >= m_peers_cnt_) return false;

  if (coll_ring_ctx->m_contrib_map_[hdr.contrib_id()]) {
    CRANE_DEBUG("double receiving was detected from {}, "
                            "local seq={}, seq={}, rejected", hdr.contrib_id(), m_seq_, hdr.seq());
    return false;
  }

  coll_ring_ctx->m_contrib_map_[hdr.contrib_id()] = true;

  if (!CollRingContrib(coll_ring_ctx, hdr.contrib_id(), hdr.hop_seq(), msg,
                       msg.size())) return false;

  coll_ring_ctx->m_contrib_prev_++;

  ProgressCollectRing_(coll_ring_ctx);

  return true;
}

}


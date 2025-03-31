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
#include "PmixCommon.h"
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
    m_ring_.next_craned_id = *next_iter;
  } else {
    CRANE_ERROR("cannot find hostname: {}", g_pmix_server->GetHostname());
    return false;
  }

  for (int i = 0; i<PMIX_COLL_RING_CTX_NUM; i++) {
    CollRingCtx &coll_ctx = m_ring_.ctx_array[i];
    coll_ctx.in_use = false;
    coll_ctx.seq = m_seq_;
    coll_ctx.contrib_local = false;
    coll_ctx.contrib_prev = 0;
    coll_ctx.state = CollRingState::SYNC;
    coll_ctx.contrib_map = std::vector(m_peers_cnt_, false);
  }

  return true;
}

bool Coll::PmixCollRingLocal_(const std::string& data,
                              pmix_modex_cbfunc_t cbfunc, void* cbdata) {
  std::lock_guard lock_guard(this->m_lock_);

  /* setup callback info */
  this->m_cbfunc_ = cbfunc;
  this->m_cbdata_ = cbdata;

  CollRingCtx* coll_ctx = this->CollRingCtxNew_();
  if (coll_ctx == nullptr) {
    CRANE_ERROR("{:p}: Can not get new ring collective context, seq= {}",
                static_cast<void*>(this), this->m_seq_);
    return false;
  }

  CRANE_DEBUG("{:p}: contrib/loc: seq_num={}, state={}, size={}", static_cast<void*>(coll_ctx), coll_ctx->seq, ToString(coll_ctx->state), data.size());

  // contrib peer node
  CollRingContrib_(*coll_ctx, m_peerid_, 0, data);

  coll_ctx->contrib_local = true;
  this->ProgressCollectRing_(*coll_ctx);

  return true;
}

Coll::CollRingCtx* Coll::CollRingCtxNew_() {
  uint32_t seq = this->m_seq_;
  CollRingCtx* res_ctx = nullptr;
  CollRingCtx* free_ring_ctx = nullptr;
  CollRingCtx* coll_ring_ctx = nullptr;

  for (int i = 0; i < PMIX_COLL_RING_CTX_NUM; i++) {
    coll_ring_ctx = &m_ring_.ctx_array[i];

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

bool Coll::CollRingContrib_(CollRingCtx& coll_ring_ctx, uint32_t contrib_id,
                           uint32_t hop_seq, const std::string& data) {

  m_ts_ = time(nullptr);
  coll_ring_ctx.ring_buf.append(data);

  /* check for ring is complete */
  if (contrib_id != (m_peerid_+1) % m_peers_cnt_) {
    // forward data to the next node
    crane::grpc::SendPmixRingMsgReq request{};

    auto* pmix_ring_msg_hdr = request.mutable_pmix_ring_msg_hdr();
    pmix_ring_msg_hdr->set_msgsize(data.size());
    pmix_ring_msg_hdr->set_craned_id(g_pmix_server->GetHostname());
    pmix_ring_msg_hdr->set_seq(coll_ring_ctx.seq);
    pmix_ring_msg_hdr->set_contrib_id(contrib_id);
    pmix_ring_msg_hdr->set_hop_seq(hop_seq);

    for (size_t i = 0; i < m_pset_.nprocs; i++) {
      auto proc = m_pset_.procs[i];
      auto* pmix_procs = request.mutable_pmix_procs()->Add();
      pmix_procs->set_nspace(proc.nspace);
      pmix_procs->set_rank(proc.rank);
    }

    request.set_msg(coll_ring_ctx.ring_buf);

    CRANE_DEBUG("coll_ctx {:p}: transit data to nodeid={}, seq={}, hop={}, size={}, contrib={}", static_cast<void*>(&coll_ring_ctx), m_ring_.next_craned_id, coll_ring_ctx.seq, hop_seq, data.size(), contrib_id);

    auto context = std::make_shared<grpc::ClientContext>();
    auto reply = std::make_shared<crane::grpc::SendPmixRingMsgReply>();
    auto stub = g_craned_client->GetCranedStub(m_ring_.next_craned_id);
    if (!stub) {
      CRANE_ERROR("{:p}, Cannot forward ring data", static_cast<void*>(&coll_ring_ctx));
      coll_ring_ctx.ring_buf.clear();
      return false;
    }

    auto self = shared_from_this();
    stub->SendPmixRingMsg(context.get(), request, reply.get(),
                          [context, reply, seq = coll_ring_ctx.seq,
                           &coll_ring_ctx, self](const grpc::Status& status) {
                            std::lock_guard lock_guard(self->m_lock_);
                            if (!status.ok() || !reply->ok()) {
                              CRANE_ERROR("{:p}, Cannot forward ring data",
                                          static_cast<void*>(&coll_ring_ctx));
                              coll_ring_ctx.ring_buf.clear();
                              return;
                            }

                            CRANE_DEBUG("{:p}: called {}",
                                        static_cast<void*>(&coll_ring_ctx),
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

void Coll::ProgressCollectRing_(CollRingCtx& coll_ring_ctx) {
  bool result = false;

  assert(coll_ring_ctx.in_use);

  do {
    result = false;
    switch (coll_ring_ctx.state) {
    case CollRingState::SYNC:
      if (coll_ring_ctx.contrib_local || coll_ring_ctx.contrib_prev != 0) {
        coll_ring_ctx.state = CollRingState::PROGRESS;
        result = true;
      }
      break;
    case CollRingState::PROGRESS:
      /* check for all data is collected and forwarded */
        if (m_peers_cnt_ -
            (coll_ring_ctx.contrib_prev + static_cast<uint32_t>(coll_ring_ctx.contrib_local)) == 0) {
        coll_ring_ctx.state = CollRingState::FINALIZE;
        InvokeCallBackRing_(coll_ring_ctx);
        result = true;
      }
      break;
    case CollRingState::FINALIZE:
      if (m_peers_cnt_ - coll_ring_ctx.forward_cnt - 1 == 0) {
        CRANE_DEBUG("{:p}: seq={} is DONE", static_cast<void*>(this), m_seq_);
        this->m_seq_++; // Only when the sequence is done, the coll sequence is incremented by 1.
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

void Coll::RingReleaseFn(void* rel_data) {
  auto* cb_data = static_cast<CbData*>(rel_data);

  std::lock_guard lock_guard(cb_data->coll->m_lock_);
  cb_data->coll_ring_ctx->ring_buf.clear();

  delete cb_data;
}

void Coll::InvokeCallBackRing_(CollRingCtx& coll_ring_ctx) {

  if (!m_cbfunc_)
      return ;

  auto cb_data = std::make_unique<CbData>();
  cb_data->coll = this;
  cb_data->coll_ring_ctx = &coll_ring_ctx;

  PmixLibModexInvoke(m_cbfunc_, PMIX_SUCCESS, coll_ring_ctx.ring_buf.data(),
          coll_ring_ctx.ring_buf.size(), m_cbdata_, reinterpret_cast<void*>(RingReleaseFn), cb_data.release());

  m_cbfunc_ = nullptr;
  m_cbdata_ = nullptr;
}

void Coll::ResetCollRing_(CollRingCtx& coll_ring_ctx) {
  coll_ring_ctx.in_use = false;
  coll_ring_ctx.state = CollRingState::SYNC;
  coll_ring_ctx.contrib_local = false;
  coll_ring_ctx.contrib_prev = 0;
  coll_ring_ctx.forward_cnt = 0;
  m_ts_ = time(nullptr);
  coll_ring_ctx.contrib_map.assign(m_peers_cnt_, false);
  coll_ring_ctx.ring_buf.clear();
}

bool Coll::ProcessRingRequest(
    const crane::grpc::SendPmixRingMsgReq_PmixRingMsgHdr& hdr, const std::string& msg) {

  CRANE_DEBUG("collective message from nodeid={}, contrib_id={}, seq={}, hop={}, msgsize={}", hdr.craned_id(), hdr.contrib_id(), hdr.seq(), hdr.hop_seq(), hdr.msgsize());

  std::lock_guard lock_guard(this->m_lock_);

  if (m_seq_-1 == hdr.seq()) {
    CRANE_DEBUG("{:p}: unexpected contrib from {}, coll->seq={}, seq={}", static_cast<void*>(this), hdr.craned_id(), m_seq_, hdr.seq());
    return false;
  }

  if (m_seq_ != hdr.seq() && m_seq_+1 != hdr.seq()) {
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
    CollRingCtx &ctx = this->m_ring_.ctx_array[i];
    if (ctx.in_use && ctx.seq == hdr.seq()) {
       coll_ring_ctx = &ctx;
       break;
    }
    if (!ctx.in_use)
      coll_ring_ctx = &ctx;
  }

  if (coll_ring_ctx != nullptr && !coll_ring_ctx->in_use) {
    coll_ring_ctx->in_use = true;
    coll_ring_ctx->seq = hdr.seq();
    coll_ring_ctx->ring_buf.clear();
  }

  if (coll_ring_ctx == nullptr) {
    CRANE_ERROR("{:p}: Can not get ring collective context, seq={}", static_cast<void*>(this), hdr.seq());
    return false;
  }

  CRANE_DEBUG("{:p}: contrib/nbr: seq_num={}, state={}, nodeid={}, contrib={}, hop_seq={}, size={}",
                  static_cast<void*>(&coll_ring_ctx),
                  coll_ring_ctx->seq, ToString(coll_ring_ctx->state), hdr.craned_id(),
                  hdr.contrib_id(), hdr.hop_seq(), hdr.msgsize());

  /* compute the actual hops of ring: (src - dst + size) % size */
  uint32_t hop_seq = ((m_peerid_ + m_peers_cnt_ - hdr.contrib_id()) % m_peers_cnt_) - 1;
  if (hop_seq != hdr.hop_seq()) {
    CRANE_DEBUG("{:p}: unexpected ring seq number={}, expect={}, coll seq={}", static_cast<void*>(this), hdr.hop_seq(), hop_seq, hdr.seq(), m_seq_);
    return false;
  }

  if (hdr.contrib_id() >= m_peers_cnt_) return false;

  if (coll_ring_ctx->contrib_map[hdr.contrib_id()]) {
    CRANE_DEBUG("coll {:p}: double receiving was detected from {}, "
                            "local seq={}, seq={}, rejected", static_cast<void*>(this), hdr.contrib_id(), m_seq_, hdr.seq());
    return false;
  }

  coll_ring_ctx->contrib_map[hdr.contrib_id()] = true;

  CollRingContrib_(*coll_ring_ctx, hdr.contrib_id(), hdr.hop_seq()+1, msg);

  coll_ring_ctx->contrib_prev++;

  ProgressCollectRing_(*coll_ring_ctx);

  return true;
}

} // namespace pmix


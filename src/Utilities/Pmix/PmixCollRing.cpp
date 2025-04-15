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

#include "PmixColl.h"
#include "crane/Logger.h"

namespace pmix {

bool Coll::PmixCollRingInit_(std::unordered_set<std::string> hostset) {

  CollRing* ring = &this->m_ring_;

  // TODO: hostset and buffer

  for (int i = 0; i<PMIX_COLL_RING_CTX_NUM; i++) {
    const auto& coll_ctx = &ring->m_ctx_array_[i];
    coll_ctx->m_in_use_ = false;
    coll_ctx->m_seq_ = m_seq_;
    coll_ctx->m_contrib_local_ = false;
    coll_ctx->m_contrib_prev_ = 0;
    coll_ctx->m_state_ = CollRingState::SYNC;
  }

  return true;
}

bool Coll::PmixCollRingLocal_(char *data, size_t size, pmix_modex_cbfunc_t cbfunc,
                              void *cbdata) {
  std::lock_guard<std::mutex> lock_guard(this->m_lock_);

  /* setup callback info */
  this->m_cbfunc_ = cbfunc;
  this->m_cbdata_ = cbdata;

  CollRingCtx* collCtx = this->CollRingCtxNew();
  if (!collCtx) {
    CRANE_ERROR("Can not get new ring collective context, seq= {}",
                            this->m_seq_);
    return false;
  }

  // TODO: buffer
  // if (CollRingContrib(*collCtx, )) return false;

  collCtx-> m_contrib_local_ = true;
  this->ProgressCollectRing_(*collCtx);

  return true;
}

Coll::CollRingCtx* Coll::CollRingCtxNew() {
  CollRing& ring = this->m_ring_;
  uint32_t seq = this->m_seq_;
  CollRingCtx *res_ctx = nullptr, *free_ring_ctx = nullptr,
              *coll_ring_ctx = nullptr;

  for (int i = 0; i < PMIX_COLL_RING_CTX_NUM; i++) {
    coll_ring_ctx = &ring.m_ctx_array_[i];

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
    // TODO: buf
    // res_ctx->m_ring_buf_ =
  }

  return res_ctx;
}

bool Coll::CollRingContrib(CollRingCtx& coll_ring_ctx, int contrib_id,
                           uint32_t hop, char* data, size_t size) {

  return true;
}

void Coll::ProgressCollectRing_(CollRingCtx& coll_ring_ctx) {

  bool result = false;

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
        if (!coll_ring_ctx.m_peers_cnt_ -
                (coll_ring_ctx.m_contrib_prev_ + coll_ring_ctx.m_contrib_local_)) {
          coll_ring_ctx.m_state_ = CollRingState::FINALIZE;
          // TODO: _invoke_callback(coll_ctx);
          result = true;
        }
        break;
    case CollRingState::FINALIZE:
      if (!(m_peers_cnt_ - coll_ring_ctx.m_forward_cnt_ - 1)) {
        this->m_seq_++;
        // TODO: reset_coll_ring()
        result = true;
      }
      break;
      default:
        // CRANE_ERROR("Unknown state: {}", coll_ring_ctx.m_state_);
    }
  }while (result);
}

void Coll::ResetCollRing(CollRingCtx& coll_ring_ctx) {
  coll_ring_ctx.m_in_use_ = false;
  coll_ring_ctx.m_state_ = CollRingState::SYNC;
  coll_ring_ctx.m_contrib_local_ = false;
  coll_ring_ctx.m_contrib_prev_ = 0;
  coll_ring_ctx.m_forward_cnt_ = 0;
  m_ts_ = time(nullptr);
  coll_ring_ctx.m_contrib_map_.resize(m_peers_cnt_, false);
  coll_ring_ctx.m_ring_buf_ = "";
}

}


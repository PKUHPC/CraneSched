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

#include <pmix.h>

#include <cassert>
#include <cstring>
#include <list>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

namespace pmix {

#define PMIX_COLL_RING_CTX_NUM 3

enum class CollType : std::uint8_t {
  FENCE_TREE,
  FENCE_RING,
  FENCE_MAX,
  CONNECT,
  DISCONNECT
};

inline std::string ToString(CollType type) {
  switch(type) {
  case CollType::FENCE_TREE:   return "FENCE_TREE";
  case CollType::FENCE_RING:   return "FENCE_RING";
  case CollType::FENCE_MAX:    return "FENCE_MAX";
  case CollType::CONNECT:      return "CONNECT";
  case CollType::DISCONNECT:   return "DISCONNECT";
  default:                     return "UNKNOWN";
  }
}

enum class CollTreeState : std::uint8_t {
  SYNC,
  COLLECT,
  UPFWD,
  UPFWD_WSC, /* Wait for the upward Send Complete */
  UPFWD_WPC, /* Wait for Parent Contrib */
  DOWNFWD,
};

inline std::string ToString(CollTreeState state) {
  switch(state) {
  case CollTreeState::SYNC:        return "SYNC";
  case CollTreeState::COLLECT:     return "COLLECT";
  case CollTreeState::UPFWD:       return "UPFWD";
  case CollTreeState::UPFWD_WSC:   return "UPFWD_WSC";
  case CollTreeState::UPFWD_WPC:   return "UPFWD_WPC";
  case CollTreeState::DOWNFWD:     return "DOWNFWD";
  default:                         return "UNKNOWN";
  }
}

enum class CollTreeSndState : std::uint8_t {
  NONE,
  ACTIVE,
  DONE,
  FAILED,
};

inline std::string ToString(CollTreeSndState state) {
  switch(state) {
  case CollTreeSndState::NONE:    return "NONE";
  case CollTreeSndState::ACTIVE:  return "ACTIVE";
  case CollTreeSndState::DONE:    return "DONE";
  case CollTreeSndState::FAILED:  return "FAILED";
  default:                        return "UNKNOWN";
  }
}

enum class CollRingState : std::uint8_t {
  SYNC,
  PROGRESS,
  FINALIZE,
};

inline std::string ToString(CollRingState state) {
  switch(state) {
  case CollRingState::SYNC:      return "SYNC";
  case CollRingState::PROGRESS:  return "PROGRESS";
  case CollRingState::FINALIZE:  return "FINALIZE";
  default:                       return "UNKNOWN";
  }
}


class Coll : public std::enable_shared_from_this<Coll> {
 public:
  Coll() = default;

  bool PmixCollInit(CollType type, const std::vector<pmix_proc_t>& procs, size_t nprocs);

  bool PmixCollContribLocal(CollType type, const std::string& data,
                            pmix_modex_cbfunc_t cbfunc, void* cbdata);

  bool ProcessRingRequest(
      const crane::grpc::SendPmixRingMsgReq_PmixRingMsgHdr& hdr,
      const std::string& msg);

  bool PmixCollTreeChild(const CranedId& peer_host, uint32_t seq,
                         const std::string& data);
  bool PmixCollTreeParent(const CranedId& peer_host, uint32_t seq,
                          const std::string& data);

  size_t GetProcNum() const { return m_pset_.nprocs; }

  const pmix_proc_t& GetProcs(size_t index) const {
    if (index >= m_pset_.procs.size())
        throw std::out_of_range("Index out of range in get_procs");

    return m_pset_.procs[index];
}

  const std::vector<pmix_proc_t>& GetProcs() const { return m_pset_.procs; }
  CollType GetType() const { return m_type_; }

  static void RingReleaseFn(void* rel_data);

  static void TreeReleaseFn(void* rel_data);

 private:
  /* coll states */
  struct CollRingCtx {
    int peers_cnt;
    bool in_use;
    uint32_t seq;
    bool contrib_local;
    uint32_t contrib_prev;
    uint32_t forward_cnt;
    std::vector<bool> contrib_map;
    CollRingState state;
    std::string ring_buf;
  };

  struct CollRing {
    int next_peerid{};
    CranedId next_craned_id;
    std::array<CollRingCtx, PMIX_COLL_RING_CTX_NUM> ctx_array;
  };

  struct CollTree {
    CollTreeState state;
    bool contrib_local;
    uint32_t contrib_children;
    std::vector<bool> contrib_child;
    CollTreeSndState upfwd_status;
    bool contrib_prnt;
    uint32_t downfwd_cb_cnt, downfwd_cb_wait;
    CollTreeSndState downfwd_status;
    std::string upfwd_buf, downfwd_buf;
    std::string parent_host;
    int parent_peerid;
    std::string root_host;
    int root_peerid;
    int childrn_cnt;
    std::list<std::string> all_chldrn_hl;
    std::string chldrn_str;
    std::vector<int> chldrn_ids;
    std::vector<std::string> childrn_hosts;
  };

  struct CbData {
    Coll* coll;
    CollRingCtx* coll_ring_ctx;
    uint32_t seq;
  };

  /* tree coll functions */
  bool PmixCollTreeInit_(const std::set<std::string>& hostset);
  bool PmixCollTreeLocal_(const std::string& data, pmix_modex_cbfunc_t cbfunc,
                          void* cbdata);

  void ProgressCollectTree_();
  bool ProgressCollect_();
  bool ProgressUpFwd_();
  bool ProgressUpFwdWsc_();
  bool ProgressUpFwdWpc_();
  bool ProgressDownFwd_();

  void ResetCollTree_();
  void ResetCollTreeUpFwd_();
  void ResetCollTreeDownFwd_();

  void PmixCollLocalCbNodata_(int status);

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

  bool PmixCollRingNeighbor_(const crane::grpc::SendPmixRingMsgReq_PmixRingMsgHdr& hdr, const std::string& msg);

  std::mutex m_lock_;
  uint32_t m_seq_{};
  CollType m_type_ {CollType::FENCE_MAX};
  struct {
    std::vector<pmix_proc_t> procs;
    size_t nprocs;
  } m_pset_;
  uint32_t m_peerid_{};
  uint32_t m_peers_cnt_{};
  pmix_modex_cbfunc_t m_cbfunc_{};
  void* m_cbdata_{};
  time_t m_ts_{}, m_ts_next_{};

  CollTree m_tree_;
  CollRing m_ring_;
};

} // namespace pmix
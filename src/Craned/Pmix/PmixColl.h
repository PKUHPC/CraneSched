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

enum class CollType {
  FENCE_TREE = 0,
  FENCE_RING,
  FENCE_MAX = 15,
  CONNECT,
  DISCONNECT
};

enum class CollTreeState {
  SYNC,
  COLLECT,
  UPFWD,
  UPFWD_WSC, /* Wait for the upward Send Complete */
  UPFWD_WPC, /* Wait for Parent Contrib */
  DOWNFWD,
};

enum class CollTreeSndState {
  NONE,
  ACTIVE,
  DONE,
  FAILED,
};

enum class CollReqState {
  PROGRESS,
  SKIP,
  FAILURE
};

enum class CollRingState {
  SYNC,
  PROGRESS,
  FINALIZE,
};


class Coll {
 public:
  Coll() = default;

  ~Coll() = default;

  bool PmixCollInit(CollType type, const std::vector<pmix_proc_t>& procs, size_t nprocs);

  bool PmixCollContribLocal(CollType type, char *data, size_t size, pmix_modex_cbfunc_t cbfunc, void *cbdata);

  bool ProcessRingRequest(const crane::grpc::SendPmixRingMsgReq_PmixRingMsgHdr& hdr,
                          const std::vector<pmix_proc_t>& procs,
                          const std::string& msg);

  size_t get_nprocs() const { return m_pset_.m_nprocs_; }
  const pmix_proc_t& get_procs(size_t index) const {
    if (index >= m_pset_.m_procs_.size())
        throw std::out_of_range("Index out of range in get_procs");

    return m_pset_.m_procs_[index];
}

  const std::vector<pmix_proc_t>& get_procs() const { return m_pset_.m_procs_; }
  CollType get_type() const { return m_type_; }

 private:
  /* coll states */
  struct CollRingCtx {
    CollType m_type_;
    int m_peers_cnt_;
    bool m_in_use_;
    uint32_t m_seq_;
    bool m_contrib_local_;
    uint32_t m_contrib_prev_;
    uint32_t m_forward_cnt_;
    std::vector<bool> m_contrib_map_;
    CollRingState m_state_;
    std::string m_ring_buf_;
  };

  struct CollRing {
    int m_next_peerid_;
    CranedId m_next_craned_id_;
    std::vector<std::shared_ptr<CollRingCtx>> m_ctx_array_;
    std::string m_fwrd_buf_pool_;
    std::string m_ring_buf_pool_;
  };

  struct CollTree {
    CollTreeState m_state_;
    bool m_contrib_local_;
    uint32_t m_contrib_children_;
    std::list<bool> m_contrib_child_;
    CollTreeSndState m_upfwd_status_;
    bool m_contrib_prnt_;
    uint32_t m_downfwd_cb_cnt_, m_downfwd_cb_wait_;
    CollTreeSndState m_downfwd_status_;
    std::string m_upfwd_buf_, m_downfwd_buf_;
    size_t m_serv_offs_, m_downfwd_offset_, m_upfwd_offset_;
    std::string m_parent_host_;
    int m_parent_peerid_;
    std::string m_root_host_;
    int m_root_peerid_;
    int m_childrn_cnt_;
    std::list<std::string> m_all_chldrn_hl_;
    std::string m_chldrn_str_;
    std::list<int> m_chldrn_ids_;
    std::vector<std::string> m_childrn_hosts_;
  };

  /* tree coll functions */
  bool PmixCollTreeInit_(std::set<std::string> hostset);
  bool PmixCollTreeLocal_(char *data, size_t size, pmix_modex_cbfunc_t cbfunc, void *cbdata);

  bool ProgressCollect_();
  bool ProgressUpFwd_();
  bool ProgressUpFwdWsc_();
  bool ProgressUpFwdWpc_();
  bool ProgressDownFwd_();

  // TODO: 函数名clang-format
  void ResetCollTree();
  void ResetCollTreeUpFwd();
  void ResetCollTreeDownFwd();

  /* ring coll functions */
  bool PmixCollRingInit_(std::set<std::string> hostset);
  bool PmixCollRingLocal_(const std::string& data, size_t size,
                          pmix_modex_cbfunc_t cbfunc, void* cbdata);

  std::shared_ptr<Coll::CollRingCtx> CollRingCtxNew();

  bool CollRingContrib(const std::shared_ptr<CollRingCtx>& coll_ring_ctx,
                       int contrib_id, uint32_t hop_seq,
                       const std::string& data, size_t size);

  void ProgressCollectRing_(std::shared_ptr<CollRingCtx> coll_ring_ctx);

  void InvokeCallBackRing(std::shared_ptr<CollRingCtx> coll_ring_ctx);

  void ResetCollRing(std::shared_ptr<CollRingCtx> coll_ring_ctx);

  bool PmixCollRingNeighbor_(const crane::grpc::SendPmixRingMsgReq_PmixRingMsgHdr& hdr, const std::string& msg);

  std::mutex m_lock_;
  uint32_t m_seq_;
  CollType m_type_;
  struct {
    std::vector<pmix_proc_t> m_procs_;
    size_t m_nprocs_;
  } m_pset_;
  int m_peerid_;
  int m_peers_cnt_;
  pmix_modex_cbfunc_t m_cbfunc_;
  void* m_cbdata_;
  time_t m_ts_, m_ts_next_;

  CollTree m_tree_;
  CollRing m_ring_;
};

} // namespace pmix
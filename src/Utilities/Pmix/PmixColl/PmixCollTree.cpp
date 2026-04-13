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

#include "PmixCollTree.h"

#include "Pmix.h"
#include "ReverseTree.h"

namespace pmix {

#ifdef HAVE_PMIX

bool PmixCollTree::PmixCollInit(CollType type,
                                const std::vector<pmix_proc_t>& procs) {
  m_seq_ = 0;
  m_type_ = type;
  m_procs_.assign(procs.begin(), procs.end());

  std::set<std::string> hostname_set;

  for (const auto& proc : procs) {
    if (m_pmix_job_info_.nspace != proc.nspace) return false;

    if (proc.rank == PMIX_RANK_WILDCARD) {
      for (const auto& hostname : m_pmix_job_info_.node_list) {
        hostname_set.emplace(hostname);
      }
    } else {
      if (proc.rank >= m_pmix_job_info_.task_map.size()) {
        CRANE_ERROR("The rank is out of the task number range.");
        return false;
      }
      uint32_t node_id = m_pmix_job_info_.task_map[proc.rank];
      hostname_set.insert(m_pmix_job_info_.node_list[node_id]);
    }
  }

  m_peers_cnt_ = hostname_set.size();

  if (m_peers_cnt_ <= 0) {
    CRANE_ERROR("No peers found");
    return false;
  }

  auto it = hostname_set.find(m_pmix_job_info_.hostname);
  if (it != hostname_set.end())
    m_peerid_ = std::distance(hostname_set.begin(), it);
  else {
    CRANE_ERROR("unknown hostname");
    return false;
  }

  int max_depth;
  int width;
  int depth;

  m_state_ = CollTreeState::SYNC;

  width = 5;  // TODO: config the width of the tree
  ReverseTreeInfo(m_peerid_, m_peers_cnt_, width, &m_parent_peerid_,
                  &m_childrn_cnt_, &depth, &max_depth);
  /* We interested in amount of direct children */
  m_contrib_children_ = 0;
  m_contrib_local_ = false;
  m_chldrn_ids_.resize(width);
  m_contrib_child_.resize(width);
  m_childrn_cnt_ = ReverseTreeDirectChildren(m_peerid_, m_peers_cnt_, width,
                                             depth, &m_chldrn_ids_);

  if (m_parent_peerid_ == -1) {
    /* if we are the root of the tree:
     * - we don't have a parent;
     * - we have large list of all_childrens (we don't want
     * ourselves there)
     */
    m_parent_host_ = "";
    for (const auto& host : hostname_set) {
      if (host == m_pmix_job_info_.hostname) continue;
      m_all_chldrn_hl_.emplace_back(host);
    }
    // host[1-3]
    m_chldrn_str_ =
        util::HostNameListToStr<std::list<std::string>>(m_all_chldrn_hl_);
  } else {
    /* for all other nodes in the tree we need to know:
     * - nodename of our parent;
     * - we don't need a list of all_childrens and hl anymore
     */

    /*
     * setup parent id's
     */
    auto it = hostname_set.begin();
    if (m_parent_peerid_ >= static_cast<std::ptrdiff_t>(hostname_set.size())) {
      CRANE_ERROR("parent_peerid indicates that the host does not exist.");
      return false;
    }
    std::advance(it, m_parent_peerid_);
    m_parent_host_ = *it;

    m_root_host_ = *hostname_set.begin();

    m_all_chldrn_hl_.clear();
    m_chldrn_str_.clear();
  }

  if (m_childrn_cnt_ >= static_cast<std::ptrdiff_t>(hostname_set.size())) {
    CRANE_ERROR("childrn_cnt indicates that the host does not exist.");
    return false;
  }

  m_childrn_hosts_.resize(m_childrn_cnt_);
  for (size_t i = 0; i < m_childrn_cnt_; i++) {
    auto iter = std::next(hostname_set.begin(), m_chldrn_ids_[i]);
    m_childrn_hosts_[i] = *iter;
  }
  CRANE_TRACE(
      "coll {:p}: tree: parent_host={}, childrn_cnt={}, childrn_hosts={}",
      static_cast<void*>(this), m_parent_host_, m_childrn_cnt_,
      absl::StrJoin(m_childrn_hosts_, ","));

  ResetCollTreeUpFwd_();
  ResetCollTreeDownFwd_();
  m_cbdata_ = nullptr;
  m_cbfunc_ = nullptr;

  CRANE_TRACE(
      "tree coll {:p}: peerid={}, peer_cnt = {}, parent_id={}, childrn_cnt={}, "
      "max_depth={}, width={}, chidrn_ids={}",
      static_cast<void*>(this), m_peerid_, m_peers_cnt_, m_parent_peerid_,
      m_childrn_cnt_, max_depth, width, absl::StrJoin(m_chldrn_ids_, ","));

  return true;
}

bool PmixCollTree::PmixCollContribLocal(const std::string& data,
                                        pmix_modex_cbfunc_t cbfunc,
                                        void* cbdata) {
  std::lock_guard lock_guard(this->m_lock_);

  CRANE_DEBUG("coll {:p}: contrib/loc: seq_num={}, state={}, size={}",
              static_cast<void*>(this), m_seq_, ToString(m_state_),
              data.size());

  switch (m_state_) {
  case CollTreeState::SYNC:
    m_ts_ = std::chrono::steady_clock::now();
    [[fallthrough]];  // SYNC → COLLECT: record start timestamp then accept the
                      // contribution the same way an already-collecting node
                      // would.
  case CollTreeState::COLLECT:
    break;
  case CollTreeState::DOWNFWD:
    /* We are waiting for some send requests
     * to be finished, but local node has started
     * the next contribution.
     * This is an OK situation, go ahead and store
     * it, the buffer with the contribution is not used
     * now.
     */
    CRANE_DEBUG("{:p}: contrib/loc: next coll!", static_cast<void*>(this));
    break;
  case CollTreeState::UPFWD:
  case CollTreeState::UPFWD_WPC:
  case CollTreeState::UPFWD_WSC:
    CRANE_DEBUG("coll {:p}: contrib/loc: before prev coll is finished!",
                static_cast<void*>(this));
    return false;
  default:
    CRANE_ERROR("{:p}: local contrib while active collective, state = {}",
                static_cast<void*>(this), ToString(m_state_));
    m_state_ = CollTreeState::SYNC;
    m_craned_client_->TerminateSteps();
    return false;
  }

  if (m_contrib_local_) {
    /* Double contribution - reject
     * FIXME: check if need to support multiple non-blocking
     * operations on the same process set */
    return false;
  }

  m_contrib_local_ = true;

  m_upfwd_buf_.append(data);

  this->m_cbfunc_ = cbfunc;
  this->m_cbdata_ = cbdata;

  ProgressCollectTree_();

  CRANE_DEBUG("{:p}: finish, state={}", static_cast<void*>(this),
              ToString(m_state_));

  return true;
}

void PmixCollTree::ProgressCollectTree_() {
  bool result = false;

  do {
    switch (m_state_) {
    case CollTreeState::SYNC:
      /* check if any activity was observed */
      if (m_contrib_local_ || m_contrib_children_) {
        m_state_ = CollTreeState::COLLECT;
        result = true;
      } else {
        result = false;
      }
      break;
    case CollTreeState::COLLECT:
      result = ProgressCollect_();
      break;
    case CollTreeState::UPFWD:
      result = ProgressUpFwd_();
      break;
    case CollTreeState::UPFWD_WPC:
      result = ProgressUpFwdWpc_();
      break;
    case CollTreeState::UPFWD_WSC:
      result = ProgressUpFwdWsc_();
      break;
    case CollTreeState::DOWNFWD:
      result = ProgressDownFwd_();
      break;
    default:
      CRANE_ERROR("{:p}: unknown state {}", static_cast<void*>(this),
                  ToString(m_state_));
    }
  } while (result);
}

bool PmixCollTree::ProgressCollect_() {
  CRANE_DEBUG("{:p}: progress collect: state={}, local={}, child_cntr={}",
              static_cast<void*>(this), ToString(m_state_), m_contrib_local_,
              m_contrib_children_);

  if (CollTreeState::COLLECT != m_state_) return false;

  if (!m_contrib_local_ || m_contrib_children_ != m_childrn_cnt_) {
    /* Not yet ready to go to the next step */
    return false;
  }

  // Currently, there is only a direct connection.
  /* We will need to forward aggregated
   * message back to our children */
  m_state_ = CollTreeState::UPFWD;

  if (!m_parent_host_.empty()) {
    CRANE_DEBUG("{:p}: ProgressCollect_: send data to parent {}, seq: {}",
                static_cast<void*>(this), m_parent_host_, m_seq_);

    m_upfwd_status_ = CollTreeSndState::ACTIVE;

    crane::grpc::pmix::PmixTreeUpwardForwardReq request{};

    for (auto& proc : m_procs_) {
      auto* pmix_procs = request.mutable_pmix_procs()->Add();
      pmix_procs->set_nspace(proc.nspace);
      pmix_procs->set_rank(proc.rank);
    }
    request.set_peer_host(m_pmix_job_info_.hostname);
    request.set_msg(m_upfwd_buf_);
    request.set_seq(this->m_seq_);

    auto stub = m_pmix_client_->GetPmixStub(m_parent_host_);
    if (!stub) {
      CRANE_ERROR("Stub not found for host: {}", this->m_parent_host_);
      this->m_upfwd_buf_.clear();
      this->m_upfwd_status_ = CollTreeSndState::FAILED;
      return false;
    }
    auto self = shared_from_this();
    stub->PmixTreeUpwardForwardNoBlock(
        request, [seq = this->m_seq_, self](bool ok) {
          std::lock_guard lock(self->m_lock_);
          if (!ok) {
            CRANE_ERROR("Cannot send data (size = {}), to {}, seq = {}",
                        self->m_upfwd_buf_.size(), self->m_parent_host_, seq);
            self->m_upfwd_buf_.clear();
            self->m_upfwd_status_ = CollTreeSndState::FAILED;
            return;
          }

          if (seq != self->m_seq_) {
            CRANE_DEBUG("Collective was reset!");
            self->ProgressCollectTree_();
            return;
          }

          self->m_upfwd_status_ = CollTreeSndState::DONE;

          CRANE_DEBUG("{:p}: state: {}, snd_status={}",
                      static_cast<void*>(self.get()),
                      static_cast<int>(self->m_state_),
                      static_cast<int>(self->m_upfwd_status_));

          self->ProgressCollectTree_();
        });
  } else {
    /* move data from input buffer to the output */
    m_downfwd_buf_.append(m_upfwd_buf_);
    m_upfwd_buf_.clear();
    m_upfwd_status_ = CollTreeSndState::DONE;
    m_contrib_prnt_ = true;
  }

  return true;
}

bool PmixCollTree::ProgressUpFwd_() {
  if (m_state_ != CollTreeState::UPFWD) {
    CRANE_ERROR("{:p}: invalid state {}, expected UPFWD",
                static_cast<void*>(this), ToString(m_state_));
    m_state_ = CollTreeState::SYNC;
    m_craned_client_->TerminateSteps();
    return false;
  }

  CRANE_TRACE(
      "{:p}: progress upfwd: state={}, snd_status={}, local={}, child_cntr={}, "
      "seq_num={}",
      static_cast<void*>(this), ToString(m_state_),
      static_cast<int>(m_upfwd_status_), m_contrib_local_, m_contrib_children_,
      m_seq_);

  switch (m_upfwd_status_) {
  case CollTreeSndState::FAILED:
    PmixCollLocalCbNodata_(PMIX_ERROR);
    ResetCollTree_();
    return false;
  case CollTreeSndState::ACTIVE:
    /* still waiting for the send completion */
    return false;
  case CollTreeSndState::DONE:
    if (m_contrib_prnt_) {
      /* all-set to go to the next stage */
      break;
    }
    return false;
  default:
    CRANE_ERROR("Bad collective ufwd state {}", ToString(m_upfwd_status_));
    m_state_ = CollTreeState::SYNC;
    m_craned_client_->TerminateSteps();
    return false;
  }

  ResetCollTreeUpFwd_();

  m_state_ = CollTreeState::DOWNFWD;
  m_downfwd_status_ = CollTreeSndState::ACTIVE;

  m_downfwd_cb_wait_ = m_childrn_cnt_;

  for (const auto& host : m_childrn_hosts_) {
    CRANE_DEBUG("{:p}: ProgressUpFwd_: send data to child {}, seq: {}",
                static_cast<void*>(this), host, m_seq_);
    crane::grpc::pmix::PmixTreeDownwardForwardReq request{};

    for (auto& proc : m_procs_) {
      auto* pmix_procs = request.mutable_pmix_procs()->Add();
      pmix_procs->set_nspace(proc.nspace);
      pmix_procs->set_rank(proc.rank);
    }

    request.set_peer_host(m_pmix_job_info_.hostname);
    request.set_msg(m_downfwd_buf_);
    request.set_seq(this->m_seq_);

    auto stub = m_pmix_client_->GetPmixStub(host);
    if (!stub) {
      CRANE_ERROR("stub is null, cannot send data (size = {}), to {}",
                  this->m_downfwd_buf_.size(), host);
      this->m_downfwd_buf_.clear();
      this->m_downfwd_status_ = CollTreeSndState::FAILED;
      return false;
    }
    auto self = shared_from_this();
    stub->PmixTreeDownwardForwardNoBlock(
        request, [seq = this->m_seq_, self](bool ok) {
          std::lock_guard lock(self->m_lock_);
          if (!ok) {
            CRANE_ERROR("Cannot send data (size = {}), to {}",
                        self->m_downfwd_buf_.size(), self->m_parent_host_);
            self->m_downfwd_buf_.clear();
            self->m_downfwd_status_ = CollTreeSndState::FAILED;
            return;
          }

          if (seq != self->m_seq_) {
            CRANE_DEBUG("Collective was reset!");
            self->ProgressCollectTree_();
            return;
          }

          self->m_downfwd_cb_cnt_++;

          CRANE_DEBUG("{:p}: state: {}, snd_status={}, compl_cnt={}/{}",
                      static_cast<void*>(self.get()),
                      static_cast<int>(self->m_state_),
                      static_cast<int>(self->m_downfwd_status_),
                      self->m_downfwd_cb_cnt_, self->m_downfwd_cb_wait_);

          self->ProgressCollectTree_();
        });

    CRANE_DEBUG("fwd to {}, size = {}", host, m_downfwd_buf_.size());
  }

  if (m_cbfunc_) {
    auto cb_data = std::make_unique<CbData>();
    cb_data->coll = shared_from_this();
    cb_data->seq = m_seq_;
    m_downfwd_cb_wait_++;
    PmixLibModexInvoke(m_cbfunc_, PMIX_SUCCESS, m_downfwd_buf_.data(),
                       m_downfwd_buf_.size(), m_cbdata_, (void*)TreeReleaseFn,
                       cb_data.release());

    m_cbfunc_ = nullptr;
    m_cbdata_ = nullptr;
    CRANE_DEBUG("{:p}: local delivery, size = {}", static_cast<void*>(this),
                m_downfwd_buf_.size());
  }

  return true;
}

bool PmixCollTree::ProgressUpFwdWsc_() {
  switch (m_upfwd_status_) {
  case CollTreeSndState::FAILED:
    if (m_cbfunc_) {
      m_cbfunc_(PMIX_ERROR, nullptr, 0, m_cbdata_, nullptr, nullptr);
    }
    m_cbfunc_ = nullptr;
    m_cbdata_ = nullptr;
    ResetCollTree_();
    return false;
  case CollTreeSndState::ACTIVE:
    /* still waiting for the send completion */
    return false;
  case CollTreeSndState::DONE:
    /* all-set to go to the next stage */
    break;
  default:
    CRANE_ERROR("Bad collective ufwd state {}", ToString(m_upfwd_status_));
    m_state_ = CollTreeState::SYNC;
    m_craned_client_->TerminateSteps();
    return false;
  }

  ResetCollTreeUpFwd_();

  m_state_ = CollTreeState::UPFWD_WPC;
  return true;
}

bool PmixCollTree::ProgressUpFwdWpc_() {
  if (!m_contrib_prnt_) return false;

  /* Need to wait only for the local completion callback if installed*/
  m_downfwd_status_ = CollTreeSndState::ACTIVE;
  m_downfwd_cb_wait_ = 0;

  /* move to the next state */
  m_state_ = CollTreeState::DOWNFWD;

  /* local delivery */
  if (this->m_cbfunc_) {
    auto cb_data = std::make_unique<CbData>();
    cb_data->coll = shared_from_this();
    cb_data->seq = m_seq_;

    PmixLibModexInvoke(m_cbfunc_, PMIX_SUCCESS, m_downfwd_buf_.data(),
                       m_downfwd_buf_.size(), m_cbdata_, (void*)TreeReleaseFn,
                       cb_data.release());
    m_downfwd_cb_wait_++;
    m_cbfunc_ = nullptr;
    m_cbdata_ = nullptr;
    CRANE_DEBUG("{:p}: local delivery, size = {}", static_cast<void*>(this),
                m_downfwd_buf_.size());
  }

  return true;
}

bool PmixCollTree::ProgressDownFwd_() {
  /* if all children + local callbacks was invoked */
  if (m_downfwd_cb_wait_ == m_downfwd_cb_cnt_)
    m_downfwd_status_ = CollTreeSndState::DONE;

  switch (m_downfwd_status_) {
  case CollTreeSndState::ACTIVE:
    return false;
  case CollTreeSndState::FAILED:
    CRANE_ERROR("{:p}: failed to send, abort collective",
                static_cast<void*>(this));
    PmixCollLocalCbNodata_(PMIX_ERROR);
    ResetCollTree_();
    return false;
  case CollTreeSndState::DONE:
    break;
  default:
    m_state_ = CollTreeState::SYNC;
    m_craned_client_->TerminateSteps();
    return false;
  }

  CRANE_DEBUG("{:p}: {} seq={} is DONE", static_cast<void*>(this),
              ToString(m_type_), m_seq_);
  ResetCollTree_();

  return true;
}

bool PmixCollTree::PmixCollTreeChild(const CranedId& peer_host, uint32_t seq,
                                     const std::string& data) {
  std::lock_guard lock(m_lock_);

  int child_id = -1;
  auto it =
      std::find(m_childrn_hosts_.begin(), m_childrn_hosts_.end(), peer_host);

  if (it == m_childrn_hosts_.end()) {
    CRANE_DEBUG("contribution from the non-child node {}", peer_host);
  } else {
    child_id = std::distance(m_childrn_hosts_.begin(), it);
  }

  CRANE_DEBUG(
      "{:p}: contrib/rem from node_id={}, child_id={}, seq: {}, native_seq: "
      "{}, state={}, size={}",
      static_cast<void*>(this), peer_host, child_id, seq, m_seq_,
      ToString(m_state_), data.size());

  switch (m_state_) {
  case CollTreeState::SYNC:
    m_ts_ = std::chrono::steady_clock::now();
    [[fallthrough]];  // SYNC → COLLECT: record start timestamp then
                      // validate seq and accept the child contribution.
  case CollTreeState::COLLECT:
    if (m_seq_ != seq) {
      CRANE_ERROR(
          "{:p}: unexpected contrib from {} (child #{}) seq = {}, coll->seq = "
          "{}, state={}",
          static_cast<void*>(this), peer_host, child_id, seq, m_seq_,
          ToString(m_state_));
      goto error;
    }
    break;
  case CollTreeState::UPFWD:
  case CollTreeState::UPFWD_WSC:
    CRANE_ERROR("{:p}: unexpected contrib from {}, state = {}",
                static_cast<void*>(this), peer_host, ToString(m_state_));
    goto error;
  case CollTreeState::UPFWD_WPC:
  case CollTreeState::DOWNFWD:
    CRANE_DEBUG(
        "{:p}: contrib for the next coll. node_id={}, child={} seq={}, "
        "coll->seq={}, state={}",
        static_cast<void*>(this), peer_host, child_id, seq, m_seq_,
        ToString(m_state_));
    if (m_seq_ + 1 != seq) {
      CRANE_ERROR(
          "{:p}: unexpected contrib from {}(x:{}) seq = {}, coll->seq = {}, "
          "state={}",
          static_cast<void*>(this), peer_host, child_id, seq, m_seq_,
          ToString(m_state_));
      goto error;
    }
    break;
  default:
    CRANE_ERROR("{:p}: unknown collective state {}", static_cast<void*>(this),
                ToString(m_state_));
    m_state_ = CollTreeState::SYNC;
    goto error2;
  }

  if (child_id < 0 || child_id >= static_cast<int>(m_contrib_child_.size())) {
    CRANE_ERROR("{:p}: invalid child_id={} from {}", static_cast<void*>(this),
                child_id, peer_host);
    goto error;
  }

  if (m_contrib_child_[child_id]) {
    CRANE_DEBUG("multiple contribs from {}:(x:{})", peer_host, child_id);
    ProgressCollectTree_();
    goto proceed;
  }

  m_upfwd_buf_.append(data);

  m_contrib_child_[child_id] = true;
  m_contrib_children_++;

proceed:
  ProgressCollectTree_();

  CRANE_DEBUG("finish nodeid={}, child={}", peer_host, child_id);

  return true;
error:
  ResetCollTree_();
error2:
  m_craned_client_->TerminateSteps();
  return false;
}

bool PmixCollTree::PmixCollTreeParent(const CranedId& peer_host, uint32_t seq,
                                      const std::string& data) {
  std::lock_guard lock(m_lock_);

  std::string expected_host = m_parent_host_;

  if (expected_host != peer_host) {
    CRANE_ERROR("{:p}: parent contrib from bad node_id={}, expect={}",
                static_cast<void*>(this), peer_host, expected_host);
    ProgressCollectTree_();
    return true;
  }

  CRANE_DEBUG("{:p}: contrib/rem node_id={}: state={}, size={}",
              static_cast<void*>(this), peer_host, ToString(m_state_),
              data.size());

  switch (m_state_) {
  case CollTreeState::SYNC:
  case CollTreeState::COLLECT:
    CRANE_DEBUG("{:p}: prev contrib node_id={}: seq={}, cur_seq={}, state={}",
                static_cast<void*>(this), peer_host, seq, m_seq_,
                ToString(m_state_));
    if (m_seq_ - 1 != seq) {
      CRANE_ERROR(
          "{:p}: unexpected from {}: seq = {}, coll->seq = {}, state={}",
          static_cast<void*>(this), peer_host, seq, m_seq_, ToString(m_state_));
      goto error;
    }
    ProgressCollectTree_();
    return true;
  case CollTreeState::UPFWD_WSC:
    CRANE_ERROR("{:p}: unexpected from {}: seq = {}, coll->seq = {}, state={}",
                static_cast<void*>(this), peer_host, seq, m_seq_,
                static_cast<int>(m_state_));
    goto error;
  case CollTreeState::UPFWD:
  case CollTreeState::UPFWD_WPC:
    break;
  case CollTreeState::DOWNFWD:
    CRANE_DEBUG("{:p}: double contrib node_id={} seq={}, cur_seq={}, state={}",
                static_cast<void*>(this), peer_host, seq, m_seq_,
                ToString(m_state_));
    if (m_seq_ != seq) {
      CRANE_ERROR(
          "{:p}: unexpected from {}: seq = {}, coll->seq = {}, state={}",
          static_cast<void*>(this), peer_host, seq, m_seq_, ToString(m_state_));
      goto error;
    }
    ProgressCollectTree_();
    return true;
  default:
    m_state_ = CollTreeState::SYNC;
    goto error2;
  }

  if (m_contrib_prnt_) {
    CRANE_DEBUG("{:p}: multiple contributions from parent {}",
                static_cast<void*>(this), peer_host);
    ProgressCollectTree_();
    return true;
  }

  m_contrib_prnt_ = true;
  m_downfwd_buf_.append(data);

  ProgressCollectTree_();

  CRANE_DEBUG("{:p}: finish: node_id={}, state={}", static_cast<void*>(this),
              peer_host, ToString(m_state_));

  return true;

error:
  ResetCollTree_();
error2:
  m_craned_client_->TerminateSteps();
  return false;
}

void PmixCollTree::ResetCollTree_() {
  CRANE_TRACE("{:p}: reset coll tree: state={}, seq: {}, ",
              static_cast<void*>(this), ToString(m_state_), m_seq_);

  switch (m_state_) {
  case CollTreeState::SYNC:
    break;
  case CollTreeState::COLLECT:
  case CollTreeState::UPFWD:
  case CollTreeState::UPFWD_WSC:
    this->m_seq_++;
    m_state_ = CollTreeState::SYNC;
    this->ResetCollTreeUpFwd_();
    this->ResetCollTreeDownFwd_();
    this->m_cbdata_ = nullptr;
    this->m_cbfunc_ = nullptr;
    // Clear the timestamp so IsTimedOut() returns false for the idle coll.
    m_ts_ = {};
    break;
  case CollTreeState::DOWNFWD:
  case CollTreeState::UPFWD_WPC:
    this->m_seq_++;
    this->ResetCollTreeDownFwd_();
    if (m_contrib_local_ || m_contrib_children_) {
      /* Next collective was already started — refresh the activity timestamp
       * so IsTimedOut() is relative to the new fence, not the old one. */
      m_state_ = CollTreeState::COLLECT;
      m_ts_ = std::chrono::steady_clock::now();
    } else {
      // No pending contributions: coll is truly idle.
      m_state_ = CollTreeState::SYNC;
      m_ts_ = {};
    }
    break;
  default:
    m_state_ = CollTreeState::SYNC;
    CRANE_ERROR("unknown state {}", ToString(m_state_));
    m_craned_client_->TerminateSteps();
  }
}

void PmixCollTree::ResetCollTreeUpFwd_() {
  m_contrib_children_ = 0;
  m_contrib_local_ = false;
  m_contrib_child_.assign(m_childrn_cnt_, false);
  m_upfwd_buf_.clear();
  m_upfwd_status_ = CollTreeSndState::DONE;
}

void PmixCollTree::ResetCollTreeDownFwd_() {
  /* downwards status */

  m_downfwd_buf_.clear();
  m_downfwd_cb_cnt_ = 0;
  m_downfwd_cb_wait_ = 0;
  m_downfwd_status_ = CollTreeSndState::DONE;
  m_contrib_prnt_ = false;
}

void PmixCollTree::PmixCollLocalCbNodata_(int status) {
  if (m_cbfunc_) {
    PmixLibModexInvoke(m_cbfunc_, status, nullptr, 0, m_cbdata_, nullptr,
                       nullptr);
    m_cbfunc_ = nullptr;
    m_cbdata_ = nullptr;
  }
}

void PmixCollTree::AbortOnTimeout() {
  std::lock_guard lock(m_lock_);

  // Nothing pending when the collective is idle.
  if (m_state_ == CollTreeState::SYNC) return;

  CRANE_ERROR("coll {:p}: tree collective timed out (state={}, seq={})",
              static_cast<void*>(this), ToString(m_state_), m_seq_);

  // Invoke the pending local callback with a timeout error so the MPI
  // process is not left waiting forever.
  PmixCollLocalCbNodata_(PMIX_ERR_TIMEOUT);
  ResetCollTree_();

  // Clear the activity timestamp so IsTimedOut() returns false for the
  // freshly reset (SYNC) state.
  m_ts_ = {};
}

void PmixCollTree::TreeReleaseFn(void* rel_data) {
  auto* cb_data = static_cast<CbData*>(rel_data);

  std::lock_guard lock(cb_data->coll->m_lock_);

  PmixCollTree* coll = cb_data->coll.get();

  coll->m_downfwd_buf_.clear();

  if (cb_data->seq != cb_data->coll->m_seq_) {
    CRANE_ERROR("{:p}: collective was reset: my_seq={}, cur_seq={}",
                static_cast<void*>(cb_data->coll.get()), cb_data->seq,
                cb_data->coll->m_seq_);
    goto exit;
  }

  assert(coll->m_state_ == CollTreeState::DOWNFWD);

  coll->m_downfwd_cb_cnt_++;
  CRANE_DEBUG("{:p}: state: {}, snd_status={}, compl_cnt={}/{}",
              static_cast<void*>(coll), ToString(coll->m_state_),
              static_cast<int>(coll->m_downfwd_status_),
              coll->m_downfwd_cb_cnt_, coll->m_downfwd_cb_wait_);

  coll->ProgressCollectTree_();

exit:
  delete cb_data;
}

#endif
}  // namespace pmix
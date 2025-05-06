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
#include "Pmix.h"
#include "PmixColl.h"
#include "PmixState.h"
#include "ReverseTree.h"
#include "crane/Logger.h"
#include "crane/String.h"

namespace pmix {

bool Coll::PmixCollTreeInit_(const std::set<std::string>& hostset) {
  int max_depth, width, depth;

  m_tree_.m_state_ = CollTreeState::SYNC;

  // TODO: conf定义
  width = 5;
  ReverseTreeInfo(m_peerid_, m_peers_cnt_, width,
                          &m_tree_.m_parent_peerid_, &m_tree_.m_childrn_cnt_, &depth,
                          &max_depth);
  /* We interested in amount of direct children */
  m_tree_.m_contrib_children_ = 0;
  m_tree_.m_contrib_local_ = false;
  m_tree_.m_chldrn_ids_.resize(width);
  m_tree_.m_contrib_child_.resize(width);
  m_tree_.m_childrn_cnt_ = ReverseTreeDirectChildren(m_peerid_, m_peers_cnt_, width, depth, &m_tree_.m_chldrn_ids_);

  if (m_tree_.m_parent_peerid_ == -1) {
    /* if we are the root of the tree:
     * - we don't have a parent;
     * - we have large list of all_childrens (we don't want
     * ourselves there)
     */
    m_tree_.m_parent_host_ = "";
    for (const auto& host : hostset) {
      if (host == g_pmix_server->GetHostname()) continue;
      m_tree_.m_all_chldrn_hl_.emplace_back(host);
    }
    // host[1-3]
    m_tree_.m_chldrn_str_ = util::HostNameListToStr<std::list<std::string>>(m_tree_.m_all_chldrn_hl_);
  } else {
    /* for all other nodes in the tree we need to know:
     * - nodename of our parent;
     * - we don't need a list of all_childrens and hl anymore
     */

    /*
     * setup parent id's
     */
    auto it = hostset.begin();
    if (m_tree_.m_parent_peerid_ >= static_cast<std::ptrdiff_t>(hostset.size())) {
      CRANE_ERROR("m_parent_peerid_ indicates that the host does not exist.");
      return false;
    }
    std::advance(it, m_tree_.m_parent_peerid_);
    m_tree_.m_parent_host_ = *it;

    m_tree_.m_root_host_ = *hostset.begin();

    m_tree_.m_all_chldrn_hl_.clear();
    m_tree_.m_chldrn_str_.clear();
  }

  auto iter = hostset.begin();
  if (m_tree_.m_childrn_cnt_ >= static_cast<std::ptrdiff_t>(hostset.size())) {
    CRANE_ERROR("m_childrn_cnt_ indicates that the host does not exist.");
    return false;
  }
  for (size_t i = 0; i < m_tree_.m_childrn_cnt_; i++) {
    m_tree_.m_childrn_hosts_.emplace_back(*iter);
    ++iter;
  }

  m_tree_.m_upfwd_buf_.clear();
  m_tree_.m_downfwd_buf_.clear();
  ResetCollTreeUpFwd();
  ResetCollTreeDownFwd();
  m_cbdata_ = nullptr;
  m_cbfunc_ = nullptr;

  return true;
}

bool Coll::PmixCollTreeLocal_(const std::string& data,
                              pmix_modex_cbfunc_t cbfunc, void* cbdata) {
  std::lock_guard<std::mutex> lock_guard(this->m_lock_);

  CRANE_DEBUG("coll {:p}: contrib/loc: seq_num={}, state={}, size={}", static_cast<void*>(this), m_seq_, static_cast<int>(m_tree_.m_state_), data.size());

  switch (m_tree_.m_state_) {
  case CollTreeState::SYNC:
    m_ts_ = time(nullptr);
  case CollTreeState::COLLECT:
    break;
  case CollTreeState::DOWNFWD:
    break;
  case CollTreeState::UPFWD:
  case CollTreeState::UPFWD_WPC:
  case CollTreeState::UPFWD_WSC:
    CRANE_DEBUG("coll {:p}: contrib/loc: before prev coll is finished!", static_cast<void*>(this));
    return false;
  default:
    CRANE_ERROR("{:p}: local contrib while active collective, state = {}", static_cast<void*>(this), static_cast<int>(m_tree_.m_state_));
    m_tree_.m_state_ = CollTreeState::SYNC;
    // TODO: 发送关闭作业通知
    return false;
  }

  if (m_tree_.m_contrib_local_) {
    /* Double contribution - reject
     * FIXME: check if need to support multiple non-blocking
     * operations on the same process set */
    return false;
  }

  m_tree_.m_contrib_local_ = true;

  m_tree_.m_upfwd_buf_.append(data);

  this->m_cbfunc_ = cbfunc;
  this->m_cbdata_ = cbdata;

  ProgressCollectTree_();

  CRANE_DEBUG("{:p}: finish, state={}", static_cast<void*>(this), static_cast<int>(m_tree_.m_state_));

  return true;
}

void Coll::ProgressCollectTree_() {
  bool result = false;

  do {
    switch (m_tree_.m_state_) {
    case CollTreeState::SYNC:
      /* check if any activity was observed */
      if (m_tree_.m_contrib_local_ || m_tree_.m_contrib_children_) {
        m_tree_.m_state_ = CollTreeState::COLLECT;
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
      CRANE_ERROR("{:p}: unknown state {}", static_cast<void*>(this), static_cast<int>(m_tree_.m_state_));
      break;
    }
  } while (result);
}

bool Coll::ProgressCollect_() {

  CRANE_DEBUG("{:p}: state={}, local={}, child_cntr={}", static_cast<void*>(this), static_cast<int>(m_tree_.m_state_), m_tree_.m_contrib_local_, m_tree_.m_contrib_children_);

  if (CollTreeState::COLLECT != m_tree_.m_state_)
    return false;

  if (!m_tree_.m_contrib_local_ || m_tree_.m_contrib_children_ != m_tree_.m_childrn_cnt_) {
    /* Not yet ready to go to the next step */
    return false;
  }

  // Currently, there is only a direct connection.
  /* We will need to forward aggregated
                 * message back to our children */
  m_tree_.m_state_ = CollTreeState::UPFWD;

  if (!m_tree_.m_parent_host_.empty()) {
    CRANE_DEBUG("{:p}: send data to {}", static_cast<void*>(this), m_tree_.m_parent_host_);
    m_tree_.m_upfwd_status_ = CollTreeSndState::ACTIVE;

    crane::grpc::PmixTreeUpwardForwardReq request{};

    for (size_t i = 0; i < m_pset_.m_nprocs_; i++) {
      auto proc = m_pset_.m_procs_[i];
      auto* pmix_procs = request.mutable_pmix_procs()->Add();
      pmix_procs->set_nspace(proc.nspace);
      pmix_procs->set_rank(proc.rank);
    }
    request.set_peer_host(g_pmix_server->GetHostname());
    request.set_msg(m_tree_.m_upfwd_buf_);

    auto context = std::make_shared<grpc::ClientContext>();
    auto reply = std::make_shared<crane::grpc::PmixTreeUpwardForwardReply>();

    g_craned_client->GetCranedStub(m_tree_.m_parent_host_)->PmixTreeUpwardForward(
      context.get(), std::move(request), reply.get(), [context, reply, seq = this->m_seq_, this](grpc::Status status) {
        std::lock_guard lock(this->m_lock_);
        if (!status.ok()) {
          CRANE_ERROR("Cannot send data (size = {}), to {}", this->m_tree_.m_upfwd_buf_, this->m_tree_.m_parent_host_);
          this->m_tree_.m_upfwd_buf_.clear();
          this->m_tree_.m_upfwd_status_ = CollTreeSndState::FAILED;
          return ;
        }

        if (seq != this->m_seq_) {
          CRANE_DEBUG("Collective was reset!");
          goto exit;
        }

        this->m_tree_.m_upfwd_status_ = CollTreeSndState::DONE;

        CRANE_DEBUG("{:p}: state: {}, snd_status={}",
          static_cast<void*>(this),
          static_cast<int>(this->m_tree_.m_state_),
          static_cast<int>(this->m_tree_.m_upfwd_status_));

    exit:
        this->ProgressCollectTree_();
      });
  } else {
    /* move data from input buffer to the output */
    m_tree_.m_downfwd_buf_.append(m_tree_.m_upfwd_buf_);
    m_tree_.m_upfwd_buf_.clear();
    m_tree_.m_upfwd_status_ = CollTreeSndState::DONE;
    m_tree_.m_contrib_prnt_ = true;
  }

  return true;
}

bool Coll::ProgressUpFwd_() {

  assert(m_tree_.m_state_ == CollTreeState::UPFWD);

  switch (m_tree_.m_upfwd_status_) {
    case CollTreeSndState::FAILED:
      PmixCollLocalCbNodata(PMIX_ERROR);
      ResetCollTree();
      return false;
    case CollTreeSndState::ACTIVE:
      /* still waiting for the send completion */
      return false;
    case CollTreeSndState::DONE:
      if (m_tree_.m_contrib_prnt_) {
        /* all-set to go to the next stage */
        break;
      }
      return false;
  default:
    CRANE_ERROR("Bad collective ufwd state {}", static_cast<int>(m_tree_.m_upfwd_status_));
    m_tree_.m_state_ = CollTreeState::SYNC;
    // TODO: 发送关闭作业通知
    return false;
  }

  ResetCollTreeUpFwd();

  m_tree_.m_state_ = CollTreeState::DOWNFWD;
  m_tree_.m_downfwd_status_ = CollTreeSndState::ACTIVE;

  // TODO: 异步回调
  m_tree_.m_downfwd_cb_wait_ = m_tree_.m_childrn_cnt_;

  for (const auto& host : m_tree_.m_childrn_hosts_) {
    crane::grpc::PmixTreeDownwardForwardReq request{};

    for (size_t i = 0; i < m_pset_.m_nprocs_; i++) {
      auto proc = m_pset_.m_procs_[i];
      auto* pmix_procs = request.mutable_pmix_procs()->Add();
      pmix_procs->set_nspace(proc.nspace);
      pmix_procs->set_rank(proc.rank);
    }
    request.set_peer_host(g_pmix_server->GetHostname());
    request.set_msg(m_tree_.m_downfwd_buf_);

    auto context = std::make_shared<grpc::ClientContext>();
    auto reply = std::make_shared<crane::grpc::PmixTreeDownwardForwardReply>();

    g_craned_client->GetCranedStub(host)->PmixTreeDownwardForward(
      context.get(), std::move(request), reply.get(),
      [context, reply, seq = this->m_seq_, this](grpc::Status status) {
        std::lock_guard lock(this->m_lock_);
        if (!status.ok()) {
          CRANE_ERROR("Cannot send data (size = {}), to {}", this->m_tree_.m_upfwd_buf_, this->m_tree_.m_parent_host_);
          this->m_tree_.m_downfwd_buf_.clear();
          this->m_tree_.m_downfwd_status_ = CollTreeSndState::FAILED;
          return ;
        }

        if (seq != this->m_seq_) {
          CRANE_DEBUG("Collective was reset!");
          goto exit;
        }

        this->m_tree_.m_downfwd_cb_cnt_++;

        CRANE_DEBUG("{:p}: state: {}, snd_status={}, compl_cnt={}/{}",
                  static_cast<void*>(this),
                  static_cast<int>(this->m_tree_.m_state_),
                  static_cast<int>(this->m_tree_.m_downfwd_status_),
                  this->m_tree_.m_downfwd_cb_cnt_,
                  this->m_tree_.m_downfwd_cb_wait_);

    exit:
        this->ProgressCollectTree_();
    });

    CRANE_DEBUG("fwd to {}, size = {}", host, m_tree_.m_downfwd_buf_.size());
  }

  if (m_cbfunc_) {
    auto cb_data = std::make_unique<CbData>();
    cb_data->coll = this;
    cb_data->seq = m_seq_;
    m_tree_.m_downfwd_cb_wait_++;
    PmixLibModexInvoke(m_cbfunc_, PMIX_SUCCESS, m_tree_.m_downfwd_buf_.data(), m_tree_.m_downfwd_buf_.size(), m_cbdata_, (void*)TreeReleaseFn, cb_data.release());

    m_cbfunc_ = nullptr;
    m_cbdata_ = nullptr;
    CRANE_DEBUG("{:p}: local delivery, size = {}", static_cast<void*>(this), m_tree_.m_downfwd_buf_.size());
  }

  return true;
}

bool Coll::ProgressUpFwdWsc_() {

  switch (m_tree_.m_upfwd_status_) {
  case CollTreeSndState::FAILED:
    if (m_cbfunc_) {
      m_cbfunc_(PMIX_ERROR, nullptr, 0, m_cbdata_, nullptr, nullptr);
    }
    m_cbfunc_ = nullptr;
    m_cbdata_ = nullptr;
    ResetCollTree();
    return false;
  case CollTreeSndState::ACTIVE:
    /* still waiting for the send completion */
      return false;
  case CollTreeSndState::DONE:
    /* all-set to go to the next stage */
      break;
  default:
    CRANE_ERROR("Bad collective ufwd state {}", static_cast<int>(m_tree_.m_upfwd_status_));
    m_tree_.m_state_ = CollTreeState::SYNC;
    // TODO: 发送关闭作业通知
    return false;
  }

  ResetCollTreeUpFwd();

  m_tree_.m_state_ = CollTreeState::UPFWD_WPC;
  return true;
}

bool Coll::ProgressUpFwdWpc_() {

  if (!m_tree_.m_contrib_prnt_)
    return false;

  /* Need to wait only for the local completion callback if installed*/
  m_tree_.m_downfwd_status_ = CollTreeSndState::ACTIVE;
  m_tree_.m_downfwd_cb_wait_ = 0;

  /* move to the next state */
  m_tree_.m_state_ = CollTreeState::DOWNFWD;

  /* local delivery */
  if (this->m_cbfunc_) {
    auto cb_data = std::make_unique<CbData>();
    cb_data->coll = this;
    cb_data->seq = m_seq_;

    PmixLibModexInvoke(m_cbfunc_, PMIX_SUCCESS,m_tree_.m_downfwd_buf_.data(), m_tree_.m_downfwd_buf_.size(), m_cbdata_, (void*)TreeReleaseFn, cb_data.release());
    m_tree_.m_downfwd_cb_wait_++;
    m_cbfunc_ = nullptr;
    m_cbdata_ = nullptr;
    CRANE_DEBUG("{:p}: local delivery, size = {}", static_cast<void*>(this), m_tree_.m_downfwd_buf_.size());
  }

  return true;
}

bool Coll::ProgressDownFwd_() {
  /* if all children + local callbacks was invoked */
  if (m_tree_.m_downfwd_cb_wait_ == m_tree_.m_downfwd_cb_cnt_)
    m_tree_.m_downfwd_status_ = CollTreeSndState::DONE;

  switch (m_tree_.m_downfwd_status_) {
  case CollTreeSndState::ACTIVE:
    return false;
  case CollTreeSndState::FAILED:
    CRANE_ERROR("{:p}: failed to send, abort collective", static_cast<void*>(this));
    PmixCollLocalCbNodata(PMIX_ERROR);
    ResetCollTree();
    return false;
  case CollTreeSndState::DONE:
    break;
  default:
    m_tree_.m_state_ = CollTreeState::SYNC;
    // TODO：kill job
    return false;
  }

  CRANE_DEBUG("{:p}: {} seq={} is DONE", static_cast<void*>(this), static_cast<int>(m_type_), m_seq_);
  ResetCollTree();

  return true;
}
bool Coll::PmixCollTreeChild(const CranedId& peer_host,
                              const std::string& data) {
  int child_id = -1;
  auto it = std::find(m_tree_.m_childrn_hosts_.begin(),
                      m_tree_.m_childrn_hosts_.end(), peer_host);

  if (it == m_tree_.m_childrn_hosts_.end()) {
    CRANE_DEBUG("contribution from the non-child node {}", peer_host);
  } else {
    child_id = std::distance(m_tree_.m_childrn_hosts_.begin(), it);
  }

  switch (m_tree_.m_state_) {
  case CollTreeState::SYNC:
    m_ts_ = time(nullptr);
  case CollTreeState::COLLECT:
    break;
  case CollTreeState::UPFWD:
  case CollTreeState::UPFWD_WSC:
    // TODO: kill job
    return false;
  case CollTreeState::UPFWD_WPC:
  case CollTreeState::DOWNFWD:
    break;
  default:
    m_tree_.m_state_ = CollTreeState::SYNC;
    // TODO: kill job
    return false;
  }

  if (m_tree_.m_contrib_child_[child_id]) {
    CRANE_DEBUG("multiple contribs from {}:(x:{})", peer_host, child_id);
    ProgressCollectTree_();
    return true;
  }

  m_tree_.m_upfwd_buf_.append(data);

  m_tree_.m_contrib_child_[child_id] = true;
  m_tree_.m_contrib_children_++;

  ProgressCollectTree_();

  CRANE_DEBUG("finish nodeid={}, child={}", peer_host, child_id);

  return true;
}
bool Coll::PmixCollTreeParent(const CranedId& peer_host,
                               const std::string& data) {
  std::string expected_host = m_tree_.m_parent_host_;

  if (expected_host != peer_host) {
    ProgressCollectTree_();
    return true;
  }

  switch (m_tree_.m_state_) {
  case CollTreeState::SYNC:
  case CollTreeState::COLLECT:
    ProgressCollectTree_();
    return true;
  case CollTreeState::UPFWD_WSC:
    // TODO: kill job
    return false;
  case CollTreeState::UPFWD:
  case CollTreeState::UPFWD_WPC:
    break;
  case CollTreeState::DOWNFWD:
    ProgressCollectTree_();
    return true;
  default:
    m_tree_.m_state_ = CollTreeState::SYNC;
    // TODO: kill job
    return false;
  }

  if (m_tree_.m_contrib_prnt_) {
    ProgressCollectTree_();
    return true;
  }

  m_tree_.m_contrib_prnt_ = true;
  m_tree_.m_downfwd_buf_.append(data);

  ProgressCollectTree_();

  return true;
}

void Coll::ResetCollTree() {

  switch (m_tree_.m_state_) {
    case CollTreeState::SYNC:
      break;
    case CollTreeState::COLLECT:
    case CollTreeState::UPFWD:
    case CollTreeState::UPFWD_WSC:
      this->m_seq_++;
      m_tree_.m_state_ = CollTreeState::SYNC;
      this->ResetCollTreeUpFwd();
      this->ResetCollTreeDownFwd();
      this->m_cbdata_ = nullptr;
      this->m_cbfunc_ = nullptr;
      break;
    case CollTreeState::DOWNFWD:
    case CollTreeState::UPFWD_WPC:
      this->m_seq_++;
      this->ResetCollTreeDownFwd();
      if (m_tree_.m_contrib_local_ || m_tree_.m_contrib_children_) {
        /* next collective was already started */
        m_tree_.m_state_ = CollTreeState::COLLECT;
      } else {
        m_tree_.m_state_ = CollTreeState::SYNC;
      }
      break;
    default:
      m_tree_.m_state_ = CollTreeState::SYNC;
      // CRANE_ERROR("unknown state {}", m_tree_.m_state_);
      // TODO: kill job
  }
}

void Coll::ResetCollTreeUpFwd() {

  m_tree_.m_contrib_children_ = 0;
  m_tree_.m_contrib_local_ = false;
  m_tree_.m_contrib_child_.assign(m_tree_.m_childrn_cnt_, false);
  m_tree_.m_upfwd_buf_.clear();
  m_tree_.m_upfwd_status_ = CollTreeSndState::DONE;
}

void Coll::ResetCollTreeDownFwd() {
  /* downwards status */

  m_tree_.m_downfwd_buf_.clear();
  m_tree_.m_downfwd_cb_cnt_ = 0;
  m_tree_.m_downfwd_cb_wait_ = 0;
  m_tree_.m_downfwd_status_ = CollTreeSndState::DONE;
  m_tree_.m_contrib_prnt_ = false;
}

void Coll::PmixCollLocalCbNodata(int status) {
  if (m_cbfunc_) {
    PmixLibModexInvoke(m_cbfunc_, status, nullptr, 0, m_cbdata_, nullptr, nullptr);
    m_cbfunc_ = nullptr;
    m_cbdata_ = nullptr;
  }
}

void Coll::TreeReleaseFn(void* rel_data) {
  auto* cb_data = static_cast<CbData*>(rel_data);

  std::lock_guard lock(cb_data->coll->m_lock_);

  auto coll = cb_data->coll;

  coll->m_tree_.m_downfwd_buf_.clear();

  if (cb_data->seq != cb_data->coll->m_seq_) {
    CRANE_ERROR("{:p}: collective was reset: my_seq={}, cur_seq={}", static_cast<void*>(cb_data->coll), cb_data->seq, cb_data->coll->m_seq_);
    goto exit;
  }

  assert(coll->m_tree_.m_state_ == CollTreeState::DOWNFWD);

  coll->m_tree_.m_downfwd_cb_cnt_++;
  CRANE_DEBUG("{:p}: state: {}, snd_status={}, compl_cnt={}/{}", static_cast<void*>(coll), static_cast<int>(coll->m_tree_.m_state_), static_cast<int>(coll->m_tree_.m_downfwd_status_), coll->m_tree_.m_downfwd_cb_cnt_, coll->m_tree_.m_downfwd_cb_wait_);

  coll->ProgressCollectTree_();

exit:
    delete cb_data;
}

} // namespace pmix

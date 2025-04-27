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

#include "Pmix.h"
#include "PmixColl.h"
#include "PmixState.h"
#include "ReverseTree.h"
#include "crane/Logger.h"
#include "crane/String.h"

namespace pmix {

bool Coll::PmixCollTreeInit_(std::set<std::string> hostset) {
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
  // TODO: hostlist
  auto it = hostset.begin();
  if (m_tree_.m_childrn_cnt_ >= static_cast<std::ptrdiff_t>(hostset.size())) {
    CRANE_ERROR("m_childrn_cnt_ indicates that the host does not exist.");
    return false;
  }
  for (size_t i = 0; i < m_tree_.m_childrn_cnt_; i++) {

  }
  //
  // ResetCollTreeUpFwd();
  // ResetCollTreeDownFwd();
  m_cbdata_ = nullptr;
  m_cbfunc_ = nullptr;

  return true;
}

bool Coll::PmixCollTreeLocal_(char* data, size_t size, pmix_modex_cbfunc_t cbfunc,
                              void* cbdata) {
  std::lock_guard<std::mutex> lock_guard(this->m_lock_);

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
    return true;
  default:
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

  m_tree_.m_upfwd_buf_.append(data, size);

  this->m_cbfunc_ = cbfunc;
  this->m_cbdata_ = cbdata;

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
      // CRANE_ERROR("unknown state {}", m_tree_.m_state_);
        break;
    }
  } while (result);

  return true;
}

bool Coll::ProgressCollect_() {

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
    m_tree_.m_upfwd_status_ = CollTreeSndState::ACTIVE;

    // TODO: 向父节点消息发送
    // rc = pmixp_server_send_nb(&ep, PMIXP_MSG_FAN_IN, coll->seq,m_tree_.ufwd_buf,_ufwd_sent_cb, cbdata);
    // 设置消息头部

    // 发送成功
    m_tree_.m_upfwd_status_ = CollTreeSndState::DONE;
  } else {
    //  /* move data from input buffer to the output */
    m_tree_.m_downfwd_buf_.append(m_tree_.m_upfwd_buf_);
    m_tree_.m_upfwd_buf_.clear();
    m_tree_.m_upfwd_status_ = CollTreeSndState::DONE;
    m_tree_.m_contrib_prnt_ = true;
  }

  return true;
}

bool Coll::ProgressUpFwd_() {

  switch (m_tree_.m_upfwd_status_) {
    case CollTreeSndState::FAILED:
      // TODO：Upward forwarding failed, notify libpmix and abort the collective communication.
      return false;
    case CollTreeSndState::ACTIVE:
      /* still waiting for the send completion */
      return false;
    case CollTreeSndState::DONE:
      if (m_tree_.m_contrib_prnt_)
        /* all-set to go to the next stage */
        break;
      return false;
  default:
    // CRANE_ERROR("Bad collective ufwd state {}", m_tree_.m_upfwd_status_);
    m_tree_.m_state_ = CollTreeState::SYNC;
    // TODO: 发送关闭作业通知
    return false;
  }

  m_tree_.m_state_ = CollTreeState::DOWNFWD;
  m_tree_.m_downfwd_status_ = CollTreeSndState::ACTIVE;


  // TODO: ep 通信
  for (int i = 0; i < m_tree_.m_childrn_cnt_; i++) {
    // ep[i].type = PMIXP_EP_NOIDEID; // 设置端点类型为节点ID
    // ep[i].ep.nodeid = m_tree_.chldrn_ids[i]; // 设置子节点的节点ID
    // ep_cnt++; // 增加端点计数
  }

  return true;
}

bool Coll::ProgressUpFwdWsc_() {

  switch (m_tree_.m_upfwd_status_) {
  case CollTreeSndState::FAILED:
    // TODO：Upward forwarding failed, notify libpmix and abort the collective communication.
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
    // CRANE_ERROR("Bad collective ufwd state {}", m_tree_.m_upfwd_status_);
    m_tree_.m_state_ = CollTreeState::SYNC;
    // TODO: 发送关闭作业通知
    return false;
  }

  ResetCollTreeUpFwd();

  m_tree_.m_state_ = CollTreeState::DOWNFWD;
  m_tree_.m_downfwd_status_ = CollTreeSndState::ACTIVE;

  // direct_conn
  /* only root of the tree should get here */
  if (m_tree_.m_parent_peerid_) {
    // TODO 发送数据
  }

  if (m_cbfunc_) {

  }

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
    // pmixp_coll_cbdata_t *cbdata;
    // TODO:
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
    // TODO：down forwarding failed, notify libpmix and abort the collective
    // communication.
    return false;
  case CollTreeSndState::DONE:
    break;
  default:
    m_tree_.m_state_ = CollTreeState::SYNC;
    // TODO：kill job
    return false;
  }

  return true;
}
void Coll::ResetCollTree() {

  switch (m_tree_.m_state_) {
    case CollTreeState::SYNC:
      break;
    case CollTreeState::COLLECT:
    case CollTreeState::UPFWD:
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
  m_tree_.m_contrib_child_.resize(m_tree_.m_childrn_cnt_);
  // TODO: ep operator
  // m_tree_.serv_offs = pmixp_server_buf_reset(m_tree_.ufwd_buf);
  // if (SLURM_SUCCESS != _pack_coll_info(coll, m_tree_.ufwd_buf)) {
  //   PMIXP_ERROR("Cannot pack ranges to message header!");
  // }
  // m_tree_.ufwd_offset = get_buf_offset(m_tree_.ufwd_buf);

  m_tree_.m_upfwd_status_ = CollTreeSndState::DONE;
}

void Coll::ResetCollTreeDownFwd() {
  /* downwards status */
  // TODO: buffer reset

  m_tree_.m_downfwd_cb_cnt_ = 0;
  m_tree_.m_downfwd_cb_wait_ = 0;
  m_tree_.m_downfwd_status_ = CollTreeSndState::DONE;
  m_tree_.m_contrib_prnt_ = false;
  /* Save the toal service offset */
  // this->state.m_tree_.dfwd_offset = get_buf_offset(
  //         coll->state.tree->dfwd_buf);
}

} // namespace pmix

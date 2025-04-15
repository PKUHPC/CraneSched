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
#include "ReverseTree.h"

namespace pmix {

bool Coll::PmixCollTreeInit_(std::unordered_set<std::string> hostset) {
  int max_depth, width, depth;
  CollTree tree = this->m_tree_;

  tree.m_state_ = CollTreeState::SYNC;

  // TODO: conf定义
  width = 5;
  ReverseTreeInfo(m_peerid_, m_peers_cnt_, width,
                          &tree.m_parent_peerid_, &tree.m_childrn_cnt_, &depth,
                          &max_depth);
  /* We interested in amount of direct children */
  tree.m_contrib_children_ = 0;
  tree.m_contrib_local_ = false;
  tree.m_chldrn_ids_.resize(width);
  tree.m_contrib_child_.resize(width);
  tree.m_childrn_cnt_ = ReverseTreeDirectChildren(m_peerid_, m_peers_cnt_, width, depth, &tree.m_chldrn_ids_);

  if (tree.m_parent_peerid_ == -1) {
    /* if we are the root of the tree:
     * - we don't have a parent;
     * - we have large list of all_childrens (we don't want
     * ourselves there)
     */
    tree.m_parent_host_ = "";
    // TODO: hostlist
  } else {
    // TODO: hostlist
  }

  // TODO: buffer
  ResetCollTreeUpFwd();
  ResetCollTreeDownFwd();
  m_cbdata_ = nullptr;
  m_cbfunc_ = nullptr;

  return true;
}

bool Coll::PmixCollTreeLocal_(char* data, size_t size, pmix_modex_cbfunc_t cbfunc,
                              void* cbdata) {
  std::lock_guard<std::mutex> lock_guard(this->m_lock_);
  CollTree& tree = this->m_tree_;

  switch (tree.m_state_) {
  case CollTreeState::SYNC:
  case CollTreeState::COLLECT:
  case CollTreeState::DOWNFWD:
    break;
  case CollTreeState::UPFWD:
  case CollTreeState::UPFWD_WPC:
  case CollTreeState::UPFWD_WSC:
    return true;
  default:
    tree.m_state_ = CollTreeState::SYNC;
    // TODO: 发送关闭作业通知
    return false;
  }

  if (tree.m_contrib_local_) {
    /* Double contribution - reject
     * FIXME: check if need to support multiple non-blocking
     * operations on the same process set */
    return false;
  }

  tree.m_contrib_local_ = true;

  // TODO: 将data内容复制进ufwd_buf

  this->m_cbfunc_ = cbfunc;
  this->m_cbdata_ = cbdata;

  bool result = false;
  do {
    switch (tree.m_state_) {
    case CollTreeState::SYNC:
      /* check if any activity was observed */
      if (tree.m_contrib_local_ || tree.m_contrib_children_) {
        tree.m_state_ = CollTreeState::COLLECT;
        result = true;
      } else {
        result = false;
      }
      break;
    case CollTreeState::COLLECT:
      break;
    case CollTreeState::DOWNFWD:
      break;
    case CollTreeState::UPFWD:
      break;
    case CollTreeState::UPFWD_WPC:
      break;
    case CollTreeState::UPFWD_WSC:
      break;
    default:
      // CRANE_ERROR("unknown state {}", tree.m_state_);
    }
  } while (result);

  return true;
}

bool Coll::ProgressCollect_() {
  CollTree& tree = this->m_tree_;

  if (CollTreeState::COLLECT != tree.m_state_)
    return false;

  if (!tree.m_contrib_local_ || tree.m_contrib_children_ != tree.m_childrn_cnt_) {
    /* Not yet ready to go to the next step */
    return false;
  }

  // Currently, there is only a direct connection.
  /* We will need to forward aggregated
                 * message back to our children */
  tree.m_state_ = CollTreeState::UPFWD;

  if (!tree.m_parent_host_.empty()) {
    tree.m_upfwd_status_ = CollTreeSndState::ACTIVE;
  } else {
    // TODO: /* move data from input buffer to the output */
    tree.m_upfwd_status_ = CollTreeSndState::DONE;
    tree.m_contrib_prnt_ = true;
  }

  // TODO: ep operator

  return true;
}

bool Coll::ProgressUpFwd_() {
  CollTree& tree = this->m_tree_;

  switch (tree.m_upfwd_status_) {
    case CollTreeSndState::FAILED:
      // TODO：Upward forwarding failed, notify libpmix and abort the collective communication.
      return false;
    case CollTreeSndState::ACTIVE:
      /* still waiting for the send completion */
      return false;
    case CollTreeSndState::DONE:
      if (tree.m_contrib_prnt_)
        /* all-set to go to the next stage */
        break;
      return false;
  default:
    // CRANE_ERROR("Bad collective ufwd state {}", tree.m_upfwd_status_);
    tree.m_state_ = CollTreeState::SYNC;
    // TODO: 发送关闭作业通知
    return false;
  }

  ResetCollTreeUpFwd();

  tree.m_state_ = CollTreeState::DOWNFWD;
  tree.m_downfwd_status_ = CollTreeSndState::ACTIVE;


  // TODO: ep 通信
  for (int i = 0; i < tree.m_childrn_cnt_; i++) {
    // ep[i].type = PMIXP_EP_NOIDEID; // 设置端点类型为节点ID
    // ep[i].ep.nodeid = tree->chldrn_ids[i]; // 设置子节点的节点ID
    // ep_cnt++; // 增加端点计数
  }

  return true;
}

bool Coll::ProgressUpFwdWsc_() {
  CollTree& tree = this->m_tree_;

  switch (tree.m_upfwd_status_) {
  case CollTreeSndState::FAILED:
    // TODO：Upward forwarding failed, notify libpmix and abort the collective communication.
      return false;
  case CollTreeSndState::ACTIVE:
    /* still waiting for the send completion */
      return false;
  case CollTreeSndState::DONE:
    /* all-set to go to the next stage */
      break;
  default:
    // CRANE_ERROR("Bad collective ufwd state {}", tree.m_upfwd_status_);
    tree.m_state_ = CollTreeState::SYNC;
    // TODO: 发送关闭作业通知
    return false;
  }

  ResetCollTreeUpFwd();

  tree.m_state_ = CollTreeState::UPFWD_WPC;

  return true;
}

bool Coll::ProgressUpFwdWpc_() {
  CollTree& tree = this->m_tree_;

  if (!tree.m_contrib_prnt_)
    return false;

  /* Need to wait only for the local completion callback if installed*/
  tree.m_downfwd_status_ = CollTreeSndState::ACTIVE;
  tree.m_downfwd_cb_wait_ = 0;

  /* move to the next state */
  tree.m_state_ = CollTreeState::DOWNFWD;

  /* local delivery */
  if (this->m_cbfunc_) {
    // pmixp_coll_cbdata_t *cbdata;
    // TODO:
  }

  return true;
}

bool Coll::ProgressDownFwd_() {
  CollTree& tree = this->m_tree_;

  /* if all children + local callbacks was invoked */
  if (tree.m_downfwd_cb_wait_ == tree.m_downfwd_cb_cnt_)
    tree.m_downfwd_status_ = CollTreeSndState::DONE;

  switch (tree.m_downfwd_status_) {
  case CollTreeSndState::ACTIVE:
    return false;
  case CollTreeSndState::FAILED:
    // TODO：down forwarding failed, notify libpmix and abort the collective
    // communication.
    return false;
  case CollTreeSndState::DONE:
    break;
  default:
    tree.m_state_ = CollTreeState::SYNC;
    // TODO：kill job
    return false;
  }

  return true;
}
void Coll::ResetCollTree() {
  CollTree& tree = this->m_tree_;

  switch (tree.m_state_) {
    case CollTreeState::SYNC:
      break;
    case CollTreeState::COLLECT:
    case CollTreeState::UPFWD:
      this->m_seq_++;
      tree.m_state_ = CollTreeState::SYNC;
      this->ResetCollTreeUpFwd();
      this->ResetCollTreeDownFwd();
      this->m_cbdata_ = nullptr;
      this->m_cbfunc_ = nullptr;
      break;
    case CollTreeState::DOWNFWD:
    case CollTreeState::UPFWD_WPC:
      this->m_seq_++;
      this->ResetCollTreeDownFwd();
      if (tree.m_contrib_local_ || tree.m_contrib_children_) {
        /* next collective was already started */
        tree.m_state_ = CollTreeState::COLLECT;
      } else {
        tree.m_state_ = CollTreeState::SYNC;
      }
      break;
    default:
      tree.m_state_ = CollTreeState::SYNC;
      // CRANE_ERROR("unknown state {}", tree.m_state_);
      // TODO: kill job
  }
}

void Coll::ResetCollTreeUpFwd() {
  CollTree& tree = this->m_tree_;

  tree.m_contrib_children_ = 0;
  tree.m_contrib_local_ = false;
  tree.m_contrib_child_.resize(tree.m_childrn_cnt_);
  // TODO: ep operator
  // tree.serv_offs = pmixp_server_buf_reset(tree->ufwd_buf);
  // if (SLURM_SUCCESS != _pack_coll_info(coll, tree->ufwd_buf)) {
  //   PMIXP_ERROR("Cannot pack ranges to message header!");
  // }
  // tree.ufwd_offset = get_buf_offset(tree.ufwd_buf);

  tree.m_upfwd_status_ = CollTreeSndState::DONE;
}

void Coll::ResetCollTreeDownFwd() {
  /* downwards status */
  // TODO: buffer reset

  this->m_tree_.m_downfwd_cb_cnt_ = 0;
  this->m_tree_.m_downfwd_cb_wait_ = 0;
  this->m_tree_.m_downfwd_status_ = CollTreeSndState::DONE;
  this->m_tree_.m_contrib_prnt_ = false;
  /* Save the toal service offset */
  // this->state.tree.dfwd_offset = get_buf_offset(
  //         coll->state.tree.dfwd_buf);
}

} // namespace pmix

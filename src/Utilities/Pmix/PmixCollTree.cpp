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
#include "PmixCommon.h"
#include "PmixState.h"
#include "ReverseTree.h"
#include "crane/Logger.h"
#include "crane/String.h"

namespace pmix {

bool Coll::PmixCollTreeInit_(const std::set<std::string>& hostset) {
  int max_depth;
  int width;
  int depth;

  m_tree_.state = CollTreeState::SYNC;

  // TODO: conf定义
  width = 5;
  ReverseTreeInfo(m_peerid_, m_peers_cnt_, width,
                          &m_tree_.parent_peerid, &m_tree_.childrn_cnt, &depth,
                          &max_depth);
  /* We interested in amount of direct children */
  m_tree_.contrib_children = 0;
  m_tree_.contrib_local = false;
  m_tree_.chldrn_ids.resize(width);
  m_tree_.contrib_child.resize(width);
  m_tree_.childrn_cnt = ReverseTreeDirectChildren(m_peerid_, m_peers_cnt_, width, depth, &m_tree_.chldrn_ids);

  if (m_tree_.parent_peerid == -1) {
    /* if we are the root of the tree:
     * - we don't have a parent;
     * - we have large list of all_childrens (we don't want
     * ourselves there)
     */
    m_tree_.parent_host = "";
    for (const auto& host : hostset) {
      if (host == g_pmix_server->GetHostname()) continue;
      m_tree_.all_chldrn_hl.emplace_back(host);
    }
    // host[1-3]
    m_tree_.chldrn_str = util::HostNameListToStr<std::list<std::string>>(m_tree_.all_chldrn_hl);
  } else {
    /* for all other nodes in the tree we need to know:
     * - nodename of our parent;
     * - we don't need a list of all_childrens and hl anymore
     */

    /*
     * setup parent id's
     */
    auto it = hostset.begin();
    if (m_tree_.parent_peerid >= static_cast<std::ptrdiff_t>(hostset.size())) {
      CRANE_ERROR("parent_peerid indicates that the host does not exist.");
      return false;
    }
    std::advance(it, m_tree_.parent_peerid);
    m_tree_.parent_host = *it;

    m_tree_.root_host = *hostset.begin();

    m_tree_.all_chldrn_hl.clear();
    m_tree_.chldrn_str.clear();
  }

  if (m_tree_.childrn_cnt >= static_cast<std::ptrdiff_t>(hostset.size())) {
    CRANE_ERROR("childrn_cnt indicates that the host does not exist.");
    return false;
  }

  m_tree_.childrn_hosts.resize(m_tree_.childrn_cnt);
  for (size_t i = 0; i < m_tree_.childrn_cnt; i++) {
    auto iter = std::next(hostset.begin(), m_tree_.chldrn_ids[i]);
    m_tree_.childrn_hosts[i] = *iter;
  }

  ResetCollTreeUpFwd_();
  ResetCollTreeDownFwd_();
  m_cbdata_ = nullptr;
  m_cbfunc_ = nullptr;

  return true;
}

bool Coll::PmixCollTreeLocal_(const std::string& data,
                              pmix_modex_cbfunc_t cbfunc, void* cbdata) {
  std::lock_guard lock_guard(this->m_lock_);

  CRANE_DEBUG("coll {:p}: contrib/loc: seq_num={}, state={}, size={}", static_cast<void*>(this), m_seq_, ToString(m_tree_.state), data.size());

  switch (m_tree_.state) {
  case CollTreeState::SYNC:
    m_ts_ = time(nullptr);
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
    CRANE_DEBUG("coll {:p}: contrib/loc: before prev coll is finished!", static_cast<void*>(this));
    return false;
  default:
    CRANE_ERROR("{:p}: local contrib while active collective, state = {}", static_cast<void*>(this), ToString(m_tree_.state));
    m_tree_.state = CollTreeState::SYNC;
    g_pmix_server->GetCranedClient()->TerminateTasks();
    return false;
  }

  if (m_tree_.contrib_local) {
    /* Double contribution - reject
     * FIXME: check if need to support multiple non-blocking
     * operations on the same process set */
    return false;
  }

  m_tree_.contrib_local = true;

  m_tree_.upfwd_buf.append(data);

  this->m_cbfunc_ = cbfunc;
  this->m_cbdata_ = cbdata;

  ProgressCollectTree_();

  CRANE_DEBUG("{:p}: finish, state={}", static_cast<void*>(this), ToString(m_tree_.state));

  return true;
}

void Coll::ProgressCollectTree_() {
  bool result = false;

  do {
    switch (m_tree_.state) {
    case CollTreeState::SYNC:
      /* check if any activity was observed */
      if (m_tree_.contrib_local || m_tree_.contrib_children) {
        m_tree_.state = CollTreeState::COLLECT;
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
      CRANE_ERROR("{:p}: unknown state {}", static_cast<void*>(this), ToString(m_tree_.state));
      g_pmix_server->GetCranedClient()->TerminateTasks();
      break;
    }
  } while (result);
}

bool Coll::ProgressCollect_() {

  CRANE_DEBUG("{:p}: state={}, local={}, child_cntr={}", static_cast<void*>(this), ToString(m_tree_.state), m_tree_.contrib_local, m_tree_.contrib_children);

  if (CollTreeState::COLLECT != m_tree_.state)
    return false;

  if (!m_tree_.contrib_local || m_tree_.contrib_children != m_tree_.childrn_cnt) {
    /* Not yet ready to go to the next step */
    return false;
  }

  // Currently, there is only a direct connection.
  /* We will need to forward aggregated
                 * message back to our children */
  m_tree_.state = CollTreeState::UPFWD;

  if (!m_tree_.parent_host.empty()) {
    CRANE_DEBUG("{:p}: send data to {}", static_cast<void*>(this), m_tree_.parent_host);
    m_tree_.upfwd_status = CollTreeSndState::ACTIVE;

    crane::grpc::pmix::PmixTreeUpwardForwardReq request{};

    for (size_t i = 0; i < m_pset_.nprocs; i++) {
      auto proc = m_pset_.procs[i];
      auto* pmix_procs = request.mutable_pmix_procs()->Add();
      pmix_procs->set_nspace(proc.nspace);
      pmix_procs->set_rank(proc.rank);
    }
    request.set_peer_host(g_pmix_server->GetHostname());
    request.set_msg(m_tree_.upfwd_buf);

    auto context = std::make_shared<grpc::ClientContext>();
    auto reply = std::make_shared<crane::grpc::pmix::PmixTreeUpwardForwardReply>();
    auto stub = g_pmix_server->GetPmixClient()->GetPmixStub(m_tree_.parent_host);
    if (!stub) {
      CRANE_ERROR("Cannot send data (size = {}), to {}", this->m_tree_.upfwd_buf, this->m_tree_.parent_host);
      this->m_tree_.upfwd_buf.clear();
      this->m_tree_.upfwd_status = CollTreeSndState::FAILED;
      return false;
    }
    auto self = shared_from_this();
    stub->PmixTreeUpwardForward(
        context.get(), request, reply.get(),
        [context, reply, seq = this->m_seq_, self](const grpc::Status& status) {
          std::lock_guard lock(self->m_lock_);
          if (!status.ok() || !reply->ok()) {
            CRANE_ERROR("Cannot send data (size = {}), to {}",
                        self->m_tree_.upfwd_buf, self->m_tree_.parent_host);
            self->m_tree_.upfwd_buf.clear();
            self->m_tree_.upfwd_status = CollTreeSndState::FAILED;
            return;
          }

          if (seq != self->m_seq_) {
            CRANE_DEBUG("Collective was reset!");
            self->ProgressCollectTree_();
            return;
          }

          self->m_tree_.upfwd_status = CollTreeSndState::DONE;

          CRANE_DEBUG("{:p}: state: {}, snd_status={}",
                      static_cast<void*>(self.get()),
                      static_cast<int>(self->m_tree_.state),
                      static_cast<int>(self->m_tree_.upfwd_status));

          self->ProgressCollectTree_();
        });
  } else {
    /* move data from input buffer to the output */
    m_tree_.downfwd_buf.append(m_tree_.upfwd_buf);
    m_tree_.upfwd_buf.clear();
    m_tree_.upfwd_status = CollTreeSndState::DONE;
    m_tree_.contrib_prnt = true;
  }

  return true;
}

bool Coll::ProgressUpFwd_() {

  assert(m_tree_.state == CollTreeState::UPFWD);

  switch (m_tree_.upfwd_status) {
    case CollTreeSndState::FAILED:
      PmixCollLocalCbNodata_(PMIX_ERROR);
      ResetCollTree_();
      return false;
    case CollTreeSndState::ACTIVE:
      /* still waiting for the send completion */
      return false;
    case CollTreeSndState::DONE:
      if (m_tree_.contrib_prnt) {
        /* all-set to go to the next stage */
        break;
      }
      return false;
  default:
    CRANE_ERROR("Bad collective ufwd state {}", ToString(m_tree_.upfwd_status));
    m_tree_.state = CollTreeState::SYNC;
    g_pmix_server->GetCranedClient()->TerminateTasks();
    return false;
  }

  ResetCollTreeUpFwd_();

  m_tree_.state = CollTreeState::DOWNFWD;
  m_tree_.downfwd_status = CollTreeSndState::ACTIVE;

  m_tree_.downfwd_cb_wait = m_tree_.childrn_cnt;

  for (const auto& host : m_tree_.childrn_hosts) {
    crane::grpc::pmix::PmixTreeDownwardForwardReq request{};

    for (size_t i = 0; i < m_pset_.nprocs; i++) {
      auto proc = m_pset_.procs[i];
      auto* pmix_procs = request.mutable_pmix_procs()->Add();
      pmix_procs->set_nspace(proc.nspace);
      pmix_procs->set_rank(proc.rank);
    }
    request.set_peer_host(g_pmix_server->GetHostname());
    request.set_msg(m_tree_.downfwd_buf);

    auto context = std::make_shared<grpc::ClientContext>();
    auto reply = std::make_shared<crane::grpc::pmix::PmixTreeDownwardForwardReply>();

    auto stub = g_pmix_server->GetPmixClient()->GetPmixStub(host);
    if (!stub) {
      CRANE_ERROR("Cannot send data (size = {}), to {}", this->m_tree_.upfwd_buf, this->m_tree_.parent_host);
      this->m_tree_.downfwd_buf.clear();
      this->m_tree_.downfwd_status = CollTreeSndState::FAILED;
      return false;
    }
    auto self = shared_from_this();
    stub->PmixTreeDownwardForward(
        context.get(), request, reply.get(),
        [context, reply, seq = this->m_seq_, self](const grpc::Status& status) {
          std::lock_guard lock(self->m_lock_);
          if (!status.ok() || !reply->ok()) {
            CRANE_ERROR("Cannot send data (size = {}), to {}",
                        self->m_tree_.upfwd_buf, self->m_tree_.parent_host);
            self->m_tree_.downfwd_buf.clear();
            self->m_tree_.downfwd_status = CollTreeSndState::FAILED;
            return;
          }

          if (seq != self->m_seq_) {
            CRANE_DEBUG("Collective was reset!");
            self->ProgressCollectTree_();
            return;
          }

          self->m_tree_.downfwd_cb_cnt++;

          CRANE_DEBUG(
              "{:p}: state: {}, snd_status={}, compl_cnt={}/{}",
              static_cast<void*>(self.get()), static_cast<int>(self->m_tree_.state),
              static_cast<int>(self->m_tree_.downfwd_status),
              self->m_tree_.downfwd_cb_cnt, self->m_tree_.downfwd_cb_wait);

          self->ProgressCollectTree_();
        });

    CRANE_DEBUG("fwd to {}, size = {}", host, m_tree_.downfwd_buf.size());
  }

  if (m_cbfunc_) {
    auto cb_data = std::make_unique<CbData>();
    cb_data->coll = this;
    cb_data->seq = m_seq_;
    m_tree_.downfwd_cb_wait++;
    PmixLibModexInvoke(m_cbfunc_, PMIX_SUCCESS, m_tree_.downfwd_buf.data(), m_tree_.downfwd_buf.size(), m_cbdata_, (void*)TreeReleaseFn, cb_data.release());

    m_cbfunc_ = nullptr;
    m_cbdata_ = nullptr;
    CRANE_DEBUG("{:p}: local delivery, size = {}", static_cast<void*>(this), m_tree_.downfwd_buf.size());
  }

  return true;
}

bool Coll::ProgressUpFwdWsc_() {

  switch (m_tree_.upfwd_status) {
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
    CRANE_ERROR("Bad collective ufwd state {}", ToString(m_tree_.upfwd_status));
    m_tree_.state = CollTreeState::SYNC;
    g_pmix_server->GetCranedClient()->TerminateTasks();
    return false;
  }

  ResetCollTreeUpFwd_();

  m_tree_.state = CollTreeState::UPFWD_WPC;
  return true;
}

bool Coll::ProgressUpFwdWpc_() {

  if (!m_tree_.contrib_prnt)
    return false;

  /* Need to wait only for the local completion callback if installed*/
  m_tree_.downfwd_status = CollTreeSndState::ACTIVE;
  m_tree_.downfwd_cb_wait = 0;

  /* move to the next state */
  m_tree_.state = CollTreeState::DOWNFWD;

  /* local delivery */
  if (this->m_cbfunc_) {
    auto cb_data = std::make_unique<CbData>();
    cb_data->coll = this;
    cb_data->seq = m_seq_;

    PmixLibModexInvoke(m_cbfunc_, PMIX_SUCCESS,m_tree_.downfwd_buf.data(), m_tree_.downfwd_buf.size(), m_cbdata_, (void*)TreeReleaseFn, cb_data.release());
    m_tree_.downfwd_cb_wait++;
    m_cbfunc_ = nullptr;
    m_cbdata_ = nullptr;
    CRANE_DEBUG("{:p}: local delivery, size = {}", static_cast<void*>(this), m_tree_.downfwd_buf.size());
  }

  return true;
}

bool Coll::ProgressDownFwd_() {
  /* if all children + local callbacks was invoked */
  if (m_tree_.downfwd_cb_wait == m_tree_.downfwd_cb_cnt)
    m_tree_.downfwd_status = CollTreeSndState::DONE;

  switch (m_tree_.downfwd_status) {
  case CollTreeSndState::ACTIVE:
    return false;
  case CollTreeSndState::FAILED:
    CRANE_ERROR("{:p}: failed to send, abort collective", static_cast<void*>(this));
    PmixCollLocalCbNodata_(PMIX_ERROR);
    ResetCollTree_();
    return false;
  case CollTreeSndState::DONE:
    break;
  default:
    m_tree_.state = CollTreeState::SYNC;
    g_pmix_server->GetCranedClient()->TerminateTasks();
    return false;
  }

  CRANE_DEBUG("{:p}: {} seq={} is DONE", static_cast<void*>(this), ToString(m_type_), m_seq_);
  ResetCollTree_();

  return true;
}
bool Coll::PmixCollTreeChild(const CranedId& peer_host, uint32_t seq,
                              const std::string& data) {
  std::lock_guard lock(m_lock_);

  int child_id = -1;
  auto it = std::find(m_tree_.childrn_hosts.begin(),
                      m_tree_.childrn_hosts.end(), peer_host);

  if (it == m_tree_.childrn_hosts.end()) {
    CRANE_DEBUG("contribution from the non-child node {}", peer_host);
  } else {
    child_id = std::distance(m_tree_.childrn_hosts.begin(), it);
  }

  CRANE_DEBUG("{:p}: contrib/rem from node_id={}, child_id={}, state={}, size={}",
              static_cast<void*>(this), peer_host, child_id, ToString(m_tree_.state), data.size());

  switch (m_tree_.state) {
  case CollTreeState::SYNC:
    m_ts_ = time(nullptr);
  case CollTreeState::COLLECT:
    if (m_seq_ != seq) {
      CRANE_ERROR("{:p}: unexpected contrib from {} (child #{}) seq = {}, coll->seq = {}, state={}",
                    static_cast<void*>(this), peer_host, child_id, seq, m_seq_, ToString(m_tree_.state));
      goto error;
    }
    break;
  case CollTreeState::UPFWD:
  case CollTreeState::UPFWD_WSC:
    CRANE_ERROR("{:p}: unexpected contrib from {}, state = {}",
                  static_cast<void*>(this), peer_host, ToString(m_tree_.state));
    goto error;
  case CollTreeState::UPFWD_WPC:
  case CollTreeState::DOWNFWD:
    CRANE_DEBUG("{:p}: contrib for the next coll. node_id={}, child={} seq={}, coll->seq={}, state={}",
                  static_cast<void*>(this), peer_host, child_id, seq, m_seq_, ToString(m_tree_.state));
    if (m_seq_+1 != seq) {
      CRANE_ERROR("{:p}: unexpected contrib from {}(x:{}) seq = {}, coll->seq = {}, state={}",
                  static_cast<void*>(this), peer_host, child_id, seq, m_seq_, ToString(m_tree_.state));
      goto error;
    }
    break;
  default:
    CRANE_ERROR("{:p}: unknown collective state {}",
                  static_cast<void*>(this), ToString(m_tree_.state));
    m_tree_.state = CollTreeState::SYNC;
    goto error2;
  }

  if (m_tree_.contrib_child[child_id]) {
    CRANE_DEBUG("multiple contribs from {}:(x:{})", peer_host, child_id);
    ProgressCollectTree_();
    return true;
  }

  m_tree_.upfwd_buf.append(data);

  m_tree_.contrib_child[child_id] = true;
  m_tree_.contrib_children++;

  ProgressCollectTree_();

  CRANE_DEBUG("finish nodeid={}, child={}", peer_host, child_id);

  return true;

error:
  ResetCollTree_();
error2:
  g_pmix_server->GetCranedClient()->TerminateTasks();
  return false;
}
bool Coll::PmixCollTreeParent(const CranedId& peer_host, uint32_t seq,
                               const std::string& data) {

  std::lock_guard lock(m_lock_);

  std::string expected_host = m_tree_.parent_host;

  if (expected_host != peer_host) {
    CRANE_ERROR("{:p}: parent contrib from bad node_id={}, expect={}", static_cast<void*>(this), peer_host, expected_host);
    ProgressCollectTree_();
    return true;
  }

  CRANE_DEBUG("{:p}: contrib/rem node_id={}: state={}, size={}", static_cast<void*>(this), peer_host, ToString(m_tree_.state), data.size());

  switch (m_tree_.state) {
  case CollTreeState::SYNC:
  case CollTreeState::COLLECT:
    CRANE_DEBUG("{:p}: prev contrib node_id={}: seq={}, cur_seq={}, state={}",
                static_cast<void*>(this), peer_host, seq, m_seq_, ToString(m_tree_.state));
    if (m_seq_-1 != seq) {
      CRANE_ERROR("{:p}: unexpected from {}: seq = {}, coll->seq = {}, state={}",
                  static_cast<void*>(this), peer_host, seq, m_seq_, ToString(m_tree_.state));
      goto error;
    }
    ProgressCollectTree_();
    return true;
  case CollTreeState::UPFWD_WSC:
    CRANE_ERROR("{:p}: unexpected from {}: seq = {}, coll->seq = {}, state={}",
                  static_cast<void*>(this), peer_host, seq, m_seq_, static_cast<int>(m_tree_.state));
    goto error;
  case CollTreeState::UPFWD:
  case CollTreeState::UPFWD_WPC:
    break;
  case CollTreeState::DOWNFWD:
    CRANE_DEBUG("{:p}: double contrib node_id={} seq={}, cur_seq={}, state={}",
              static_cast<void*>(this), peer_host, seq, m_seq_, ToString(m_tree_.state));
    if (m_seq_ != seq) {
      CRANE_ERROR("{:p}: unexpected from {}: seq = {}, coll->seq = {}, state={}",
                  static_cast<void*>(this), peer_host, seq, m_seq_, ToString(m_tree_.state));
      goto error;
    }
    ProgressCollectTree_();
    return true;
  default:
    m_tree_.state = CollTreeState::SYNC;
    goto error2;
  }

  if (m_tree_.contrib_prnt) {
    CRANE_DEBUG("{:p}: multiple contributions from parent {}", static_cast<void*>(this), peer_host);
    ProgressCollectTree_();
    return true;
  }

  m_tree_.contrib_prnt = true;
  m_tree_.downfwd_buf.append(data);

  ProgressCollectTree_();

  CRANE_DEBUG("{:p}: finish: node_id={}, state={}", static_cast<void*>(this), peer_host,  ToString(m_tree_.state));

  return true;

error:
  ResetCollTree_();
error2:
  g_pmix_server->GetCranedClient()->TerminateTasks();
  return false;
}

void Coll::ResetCollTree_() {

  switch (m_tree_.state) {
    case CollTreeState::SYNC:
      break;
    case CollTreeState::COLLECT:
    case CollTreeState::UPFWD:
    case CollTreeState::UPFWD_WSC:
      this->m_seq_++;
      m_tree_.state = CollTreeState::SYNC;
      this->ResetCollTreeUpFwd_();
      this->ResetCollTreeDownFwd_();
      this->m_cbdata_ = nullptr;
      this->m_cbfunc_ = nullptr;
      break;
    case CollTreeState::DOWNFWD:
    case CollTreeState::UPFWD_WPC:
      this->m_seq_++;
      this->ResetCollTreeDownFwd_();
      if (m_tree_.contrib_local || m_tree_.contrib_children) {
        /* next collective was already started */
        m_tree_.state = CollTreeState::COLLECT;
      } else {
        m_tree_.state = CollTreeState::SYNC;
      }
      break;
    default:
      m_tree_.state = CollTreeState::SYNC;
      CRANE_ERROR("unknown state {}", ToString(m_tree_.state));
      g_pmix_server->GetCranedClient()->TerminateTasks();
  }
}

void Coll::ResetCollTreeUpFwd_() {

  m_tree_.contrib_children = 0;
  m_tree_.contrib_local = false;
  m_tree_.contrib_child.assign(m_tree_.childrn_cnt, false);
  m_tree_.upfwd_buf.clear();
  m_tree_.upfwd_status = CollTreeSndState::DONE;
}

void Coll::ResetCollTreeDownFwd_() {
  /* downwards status */

  m_tree_.downfwd_buf.clear();
  m_tree_.downfwd_cb_cnt = 0;
  m_tree_.downfwd_cb_wait = 0;
  m_tree_.downfwd_status = CollTreeSndState::DONE;
  m_tree_.contrib_prnt = false;
}

void Coll::PmixCollLocalCbNodata_(int status) {
  if (m_cbfunc_) {
    PmixLibModexInvoke(m_cbfunc_, status, nullptr, 0, m_cbdata_, nullptr, nullptr);
    m_cbfunc_ = nullptr;
    m_cbdata_ = nullptr;
  }
}

void Coll::TreeReleaseFn(void* rel_data) {
  auto* cb_data = static_cast<CbData*>(rel_data);

  std::lock_guard lock(cb_data->coll->m_lock_);

  Coll* coll = cb_data->coll;

  coll->m_tree_.downfwd_buf.clear();

  if (cb_data->seq != cb_data->coll->m_seq_) {
    CRANE_ERROR("{:p}: collective was reset: my_seq={}, cur_seq={}", static_cast<void*>(cb_data->coll), cb_data->seq, cb_data->coll->m_seq_);
    goto exit;
  }

  assert(coll->m_tree_.state == CollTreeState::DOWNFWD);

  coll->m_tree_.downfwd_cb_cnt++;
  CRANE_DEBUG("{:p}: state: {}, snd_status={}, compl_cnt={}/{}", static_cast<void*>(coll), ToString(coll->m_tree_.state), static_cast<int>(coll->m_tree_.downfwd_status), coll->m_tree_.downfwd_cb_cnt, coll->m_tree_.downfwd_cb_wait);

  coll->ProgressCollectTree_();

exit:
    delete cb_data;
}

} // namespace pmix
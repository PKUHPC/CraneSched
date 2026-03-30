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

#pragma once

#include "PmixColl.h"
#include "PmixCommon.h"
#include "crane/Logger.h"

namespace pmix {

// Forward declarations — full types included in PmixCollTree.cpp.
class PmixClient;
class CranedClient;

enum class CollTreeState : std::uint8_t {
  SYNC,
  COLLECT,
  UPFWD,
  UPFWD_WSC, /* Wait for the upward Send Complete */
  UPFWD_WPC, /* Wait for Parent Contrib */
  DOWNFWD,
};

inline std::string ToString(CollTreeState state) {
  switch (state) {
  case CollTreeState::SYNC:
    return "SYNC";
  case CollTreeState::COLLECT:
    return "COLLECT";
  case CollTreeState::UPFWD:
    return "UPFWD";
  case CollTreeState::UPFWD_WSC:
    return "UPFWD_WSC";
  case CollTreeState::UPFWD_WPC:
    return "UPFWD_WPC";
  case CollTreeState::DOWNFWD:
    return "DOWNFWD";
  default:
    return "UNKNOWN";
  }
};

enum class CollTreeSndState : std::uint8_t {
  NONE,
  ACTIVE,
  DONE,
  FAILED,
};

inline std::string ToString(CollTreeSndState state) {
  switch (state) {
  case CollTreeSndState::NONE:
    return "NONE";
  case CollTreeSndState::ACTIVE:
    return "ACTIVE";
  case CollTreeSndState::DONE:
    return "DONE";
  case CollTreeSndState::FAILED:
    return "FAILED";
  default:
    return "UNKNOWN";
  }
};

class PmixCollTree : public Coll,
                     public std::enable_shared_from_this<PmixCollTree> {
 public:
#ifdef HAVE_PMIX
  // pmix_client and craned_client are injected so this class never needs to
  // call PmixServer::GetInstance().
  PmixCollTree(const PmixJobInfo& job_info, PmixClient* pmix_client,
               CranedClient* craned_client)
      : m_pmix_job_info_(job_info),
        m_pmix_client_(pmix_client),
        m_craned_client_(craned_client) {}

  bool PmixCollInit(CollType type,
                    const std::vector<pmix_proc_t>& procs) override;

  bool PmixCollContribLocal(const std::string& data, pmix_modex_cbfunc_t cbfunc,
                            void* cbdata) override;

  bool PmixCollTreeChild(const CranedId& peer_host, uint32_t seq,
                         const std::string& data) override;

  bool PmixCollTreeParent(const CranedId& peer_host, uint32_t seq,
                          const std::string& data) override;

  bool ProcessRingRequest(
      const crane::grpc::pmix::SendPmixRingMsgReq_PmixRingMsgHdr& hdr,
      const std::string& msg) override {
    CRANE_ERROR("Not implemented");
    return false;
  }

  void AbortOnTimeout() override;

  static void TreeReleaseFn(void* rel_data);

 private:
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

  struct CbData {
    PmixCollTree* coll;
    uint32_t seq;
  };

  PmixJobInfo m_pmix_job_info_;
  PmixClient* m_pmix_client_{nullptr};      // injected, not owned
  CranedClient* m_craned_client_{nullptr};  // injected, not owned

  CollTreeState m_state_;
  bool m_contrib_local_;
  uint32_t m_contrib_children_;
  std::vector<bool> m_contrib_child_;
  CollTreeSndState m_upfwd_status_;
  bool m_contrib_prnt_;
  uint32_t m_downfwd_cb_cnt_, m_downfwd_cb_wait_;
  CollTreeSndState m_downfwd_status_;
  std::string m_upfwd_buf_, m_downfwd_buf_;
  std::string m_parent_host_;
  int m_parent_peerid_;
  std::string m_root_host_;
  int m_root_peerid_;
  int m_childrn_cnt_;
  std::list<std::string> m_all_chldrn_hl_;
  std::string m_chldrn_str_;
  std::vector<int> m_chldrn_ids_;
  std::vector<std::string> m_childrn_hosts_;
#endif
};

}  // namespace pmix
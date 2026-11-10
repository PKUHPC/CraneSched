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

#ifdef HAVE_PMIX
#  include <pmix.h>
#endif

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

#include "crane/Logger.h"
#include "crane/PublicHeader.h"
#include "protos/mpi/Pmix.pb.h"

namespace pmix {

enum class CollectiveType : std::uint8_t {
  kTree,
  kRing,
  kInvalid,
};

inline std::string ToString(CollectiveType type) {
  switch (type) {
  case CollectiveType::kTree:
    return "kTree";
  case CollectiveType::kRing:
    return "kRing";
  case CollectiveType::kInvalid:
    return "kInvalid";
  default:
    return "UNKNOWN";
  }
}

inline CollectiveType ParseCollectiveType(const std::string& str) {
  if (str == "tree") return CollectiveType::kTree;
  if (str == "ring") return CollectiveType::kRing;

  return CollectiveType::kInvalid;
}

class Collective {
 public:
  Collective() = default;
  Collective(const Collective&) = delete;
  Collective& operator=(const Collective&) = delete;

#ifdef HAVE_PMIX
  virtual bool Init(CollectiveType type,
                    const std::vector<pmix_proc_t>& procs) = 0;

  virtual bool Contribute(const std::string& data, pmix_modex_cbfunc_t cbfunc,
                          void* cbdata) = 0;

  virtual bool ProcessRingRequest(
      const crane::grpc::mpi::pmix::SendPmixRingMsgReq_PmixRingMsgHdr& hdr,
      const std::string& msg) = 0;

  virtual bool TreeCollectiveChild(const CranedId& peer_host, uint32_t seq,
                                   const std::string& data) = 0;
  virtual bool TreeCollectiveParent(const CranedId& peer_host, uint32_t seq,
                                    const std::string& data) = 0;

  size_t GetProcNum() const { return m_procs_.size(); }

  const pmix_proc_t* GetProcs(size_t index) const {
    if (index >= m_procs_.size()) {
      CRANE_ERROR("Index {} out of range in get_procs, proc num is {}", index,
                  m_procs_.size());
      return nullptr;
    }

    return &m_procs_[index];
  }

  const std::vector<pmix_proc_t>& GetProcs() const { return m_procs_; }
  CollectiveType GetType() const { return m_type_; }

  // Returns true when the collective has been active longer than timeout.
  // Called by CollectiveManager::CleanupTimedOut from outside the class
  // hierarchy.
  bool IsTimedOut(std::chrono::seconds timeout) const {
    std::lock_guard lock(m_lock_);
    if (m_ts_ == std::chrono::steady_clock::time_point{}) return false;
    return std::chrono::steady_clock::now() - m_ts_ > timeout;
  }

  // Abort this collective: invoke the pending callback with PMIX_ERR_TIMEOUT
  // and reset internal state so the object can be reused or discarded.
  // Called by CollectiveManager::CleanupTimedOut from outside the class
  // hierarchy.
  virtual void AbortOnTimeout() = 0;

 protected:
  mutable std::mutex m_lock_;
  uint32_t m_seq_{};
  CollectiveType m_type_{CollectiveType::kInvalid};

  std::vector<pmix_proc_t> m_procs_;
  uint32_t m_peerid_{};
  uint32_t m_peers_cnt_{};
  pmix_modex_cbfunc_t m_cbfunc_{};
  void* m_cbdata_{};
  // Timestamp when this collective became active (set on first contribution).
  // Default-constructed time_point{} (epoch) signals "idle/not started".
  std::chrono::steady_clock::time_point m_ts_{};
#endif
};

}  // namespace pmix
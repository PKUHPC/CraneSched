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

#ifdef HAVE_PMIX
#include <pmix.h>
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

#include "crane/PublicHeader.h"
#include "protos/Pmix.pb.h"

namespace pmix {

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

inline CollType StrToCollType(const std::string& str) {
  if (str == "FENCE_TREE")      return CollType::FENCE_TREE;
  if (str == "FENCE_RING") return CollType::FENCE_RING;
  if (str == "FENCE_MAX")  return CollType::FENCE_MAX;
  if (str == "CONNECT")    return CollType::CONNECT;
  if (str == "DISCONNECT") return CollType::DISCONNECT;
                            
  return CollType::FENCE_MAX;
}


class Coll {
 public:
  Coll() = default;

#ifdef HAVE_PMIX
  virtual bool PmixCollInit(CollType type, const std::vector<pmix_proc_t>& procs, size_t nprocs) = 0;

  virtual bool PmixCollContribLocal(const std::string& data, pmix_modex_cbfunc_t cbfunc, void* cbdata) = 0;

  virtual bool ProcessRingRequest(
      const crane::grpc::pmix::SendPmixRingMsgReq_PmixRingMsgHdr& hdr,
      const std::string& msg) = 0;

  virtual bool PmixCollTreeChild(const CranedId& peer_host, uint32_t seq,
                         const std::string& data) = 0;
  virtual bool PmixCollTreeParent(const CranedId& peer_host, uint32_t seq,
                          const std::string& data) = 0;

  size_t GetProcNum() const { return m_pset_.nprocs; }

  const pmix_proc_t& GetProcs(size_t index) const {
    if (index >= m_pset_.procs.size())
        throw std::out_of_range("Index out of range in get_procs");

    return m_pset_.procs[index];
  }

  const std::vector<pmix_proc_t>& GetProcs() const { return m_pset_.procs; }
  CollType GetType() const { return m_type_; }

 protected:
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
#endif
};

} // namespace pmix
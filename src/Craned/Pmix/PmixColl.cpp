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

#include "Pmix.h"
#include "PmixState.h"
#include "crane/Logger.h"

namespace pmix {

bool Coll::PmixCollInit(CollType type, const std::vector<pmix_proc_t>& procs,
                         size_t nprocs) {
  m_seq_ = 0;
  m_type_ = type;
  m_pset_.m_nprocs_ = nprocs;
  m_pset_.m_procs_.assign(procs.begin(), procs.end());

  std::set<std::string> hostname_set;

  for (const auto& proc : procs) {
    auto pmix_namespace = g_pmix_server->PmixNamespaceGet(proc.nspace);
    if (!pmix_namespace) return false;

    if (proc.rank == PMIX_RANK_WILDCARD) {
      for (const auto& hostname : pmix_namespace->m_hostlist_) {
        hostname_set.emplace(hostname);
      }
    } else {
      int node_id = pmix_namespace->m_task_map_[proc.rank];
      hostname_set.insert(pmix_namespace->m_hostlist_[node_id]);
    }
  }

  if ((m_peers_cnt_ = hostname_set.size()) <= 0) {
    CRANE_ERROR("No peers found");
    return false;
  }

  auto it = hostname_set.find(g_pmix_server->GetHostname());
  if (it != hostname_set.end())
    m_peerid_ = std::distance(hostname_set.begin(), it);

  switch (type) {
    case CollType::FENCE_TREE:
      this->PmixCollTreeInit_(hostname_set);
      break;
    case CollType::FENCE_RING:
      this->PmixCollRingInit_(hostname_set);
      break;
    default:
      return false;
  }

  return true;
}

bool Coll::PmixCollContribLocal(CollType type, const std::string& data,
                                pmix_modex_cbfunc_t cbfunc, void* cbdata) {
  bool result = true;

  switch (type) {
  case CollType::FENCE_RING:
    result = PmixCollRingLocal_(data, cbfunc, cbdata);
    break;
  // case CollType::FENCE_TREE:
  //   result = PmixCollTreeLocal_(data, size, cbfunc, cbdata);
  //   break;
  default:
    result = false;
    break;
  }

  return result;
}

}  // namespace pmix
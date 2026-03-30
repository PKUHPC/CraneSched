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

#include "PmixState.h"

#include "PmixColl/PmixCollTree.h"
#include "PmixColl/PmixCollRing.h"
#include "crane/PublicHeader.h"

namespace pmix {

#ifdef HAVE_PMIX
std::shared_ptr<Coll> PmixState::PmixStateCollGet(
    CollType type, const std::vector<pmix_proc_t>& procs) {

  util::write_lock_guard lock_guard(m_mutex_);

  for (const auto& coll : m_coll_list_) {
    if (coll->GetType() != type) continue;
    if (coll->GetProcNum() != procs.size()) continue;
    // Zero-proc collectives are matched by type alone
    if (!coll->GetProcNum()) return coll;

    bool all_match = true;
    for (size_t i = 0; i < procs.size(); i++) {
      if (i >= coll->GetProcNum()) { all_match = false; break; }
      const auto* proc = coll->GetProcs(i);
      if (!proc || std::strcmp(proc->nspace, procs[i].nspace) != 0 ||
          proc->rank != procs[i].rank) {
        all_match = false;
        break;
      }
    }
    if (all_match) return coll;
  }

  std::shared_ptr<Coll> coll = nullptr;
  switch (type) {
    case CollType::FENCE_TREE:
      coll = std::make_shared<PmixCollTree>(m_pmix_job_info_);
      break;
    case CollType::FENCE_RING:
      coll = std::make_shared<PmixCollRing>(m_pmix_job_info_);
      break;
    default:
      CRANE_ERROR("Unsupported collective type: {}", ToString(type));
      return nullptr;
  }

  CRANE_TRACE("Creating new collective: type={}, proc_count={}",
              ToString(type), procs.size());

  if (!coll->PmixCollInit(type, procs)) {
    CRANE_ERROR("Failed to initialize collective: type={}, proc_count={}",
                ToString(type), procs.size());
    return nullptr;
  }

  m_coll_list_.emplace_back(coll);

  CRANE_DEBUG("New collective {:p} added to state: type={}, total_colls={}",
              static_cast<void*>(coll.get()), ToString(type), m_coll_list_.size());

  return coll;
}

#endif
} // namespace pmix
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

#include "Collective/CollectiveManager.h"

#include "Collective/RingCollective.h"
#include "Collective/TreeCollective.h"
#include "StepControlClient.h"
#include "Transport/PmixClient.h"
#include "crane/PublicHeader.h"

namespace pmix {

#ifdef HAVE_PMIX
std::shared_ptr<Collective> CollectiveManager::GetCollective(
    CollectiveType type, const std::vector<pmix_proc_t>& procs) {
  util::write_lock_guard lock_guard(m_mutex_);

  // Refuse to create new collectives after AbortAll() has been called.
  // Racing peer messages (e.g., a second kRing from the remote node
  // arriving after the abort) would otherwise cause GetCollective() to
  // allocate a brand-new collective object, which then hits the
  // "unexpected contrib" error path and fires a spurious TerminateSteps().
  if (m_aborted_) {
    CRANE_WARN(
        "GetCollective: state already aborted, rejecting new {} collective",
        ToString(type));
    return nullptr;
  }

  for (const auto& coll : m_coll_list_) {
    if (coll->GetType() != type) continue;
    if (coll->GetProcNum() != procs.size()) continue;
    // Zero-proc collectives are matched by type alone
    if (!coll->GetProcNum()) return coll;

    bool all_match = true;
    for (size_t i = 0; i < procs.size(); i++) {
      if (i >= coll->GetProcNum()) {
        all_match = false;
        break;
      }
      const auto* proc = coll->GetProcs(i);
      if (!proc || std::strcmp(proc->nspace, procs[i].nspace) != 0 ||
          proc->rank != procs[i].rank) {
        all_match = false;
        break;
      }
    }
    if (all_match) return coll;
  }

  std::shared_ptr<Collective> coll = nullptr;
  switch (type) {
  case CollectiveType::kTree:
    coll = std::make_shared<TreeCollective>(m_step_info_, m_pmix_client_,
                                            m_step_client_);
    break;
  case CollectiveType::kRing:
    coll = std::make_shared<RingCollective>(m_step_info_, m_pmix_client_,
                                            m_step_client_);
    break;
  default:
    CRANE_ERROR("Unsupported collective type: {}", ToString(type));
    return nullptr;
  }

  CRANE_TRACE("Creating new collective: type={}, proc_count={}", ToString(type),
              procs.size());

  if (!coll->Init(type, procs)) {
    CRANE_ERROR("Failed to initialize collective: type={}, proc_count={}",
                ToString(type), procs.size());
    return nullptr;
  }

  m_coll_list_.emplace_back(coll);

  CRANE_DEBUG("New collective {:p} added to state: type={}, total_colls={}",
              static_cast<void*>(coll.get()), ToString(type),
              m_coll_list_.size());

  return coll;
}

void CollectiveManager::CleanupTimedOut(std::chrono::seconds timeout) {
  std::vector<std::shared_ptr<Collective>> expired;
  {
    util::write_lock_guard lock(m_mutex_);
    std::erase_if(m_coll_list_, [&](const auto& collective) {
      if (!collective->IsTimedOut(timeout)) return false;
      expired.push_back(collective);
      return true;
    });
  }
  for (auto& collective : expired) collective->AbortOnTimeout();
}

void CollectiveManager::AbortAll() {
  std::vector<std::shared_ptr<Collective>> active;
  {
    util::write_lock_guard lock(m_mutex_);
    m_aborted_ = true;
    active.swap(m_coll_list_);
  }
  for (auto& collective : active) collective->AbortOnTimeout();
}

#endif
}  // namespace pmix

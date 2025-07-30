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

namespace pmix {

std::shared_ptr<Coll> PmixState::PmixStateCollGet(
    CollType type, const std::vector<pmix_proc_t>& procs, size_t nprocs) {

  util::write_lock_guard lock_guard(m_mutex_);

  for (const auto& coll : m_coll_list_) {
    if (coll->GetProcNum() != nprocs) continue;
    if (coll->GetType() != type) continue;
    if (!coll->GetProcNum()) return coll;

    for (size_t i = 0; i < nprocs; i++) {
      const auto& proc = coll->GetProcs(i);
      if (std::strcmp(proc.nspace, procs[i].nspace) == 0 &&
          proc.rank == procs[i].rank)
        return coll;
    }
  }

  std::shared_ptr<Coll> coll = std::make_shared<Coll>();
  if (!coll->PmixCollInit(type, procs, nprocs)) return nullptr;

  m_coll_list_.emplace_back(coll);

  return coll;
}

} // namespace pmix
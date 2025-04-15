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

namespace pmix {

bool Coll::PmixCollInit(CollType type, const std::vector<pmix_proc_t>& procs,
                         size_t nprocs) {
  m_seq_ = 0;
  m_type_ = type;
  m_pset_.m_nprocs_ = nprocs;
  m_pset_.m_procs_.assign(procs.begin(), procs.end());

  // TODO: hostset
  std::unordered_set<std::string> hostset;

  switch (type) {
    case CollType::FENCE_TREE:
      this->PmixCollTreeInit_(hostset);
      break;
    case CollType::FENCE_RING:
      this->PmixCollRingInit_(hostset);
      break;
    default:
      return false;
  }

  return true;
}

bool Coll::PmixCollContribLocal(CollType type, char* data, size_t size,
                                pmix_modex_cbfunc_t cbfunc, void* cbdata) {
  bool result = true;

  switch (type) {
  case CollType::FENCE_RING:
    result = PmixCollRingLocal_(data, size, cbfunc, cbdata);
    break;
  case CollType::FENCE_TREE:
    result = PmixCollTreeLocal_(data, size, cbfunc, cbdata);
    break;
  default:
    result = false;
    break;
  }

  return result;
}

}  // namespace pmix
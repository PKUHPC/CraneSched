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

#include <list>

#include "PmixColl/PmixColl.h"
#include "PmixCommon.h"

#include "crane/Lock.h"

namespace pmix {

class PmixState {
public:
#ifdef HAVE_PMIX
  PmixState(const PmixJobInfo& job_info) : m_pmix_job_info_(job_info) {};
  
  std::shared_ptr<Coll> PmixStateCollGet(CollType type, const std::vector<pmix_proc_t>& procs);

  // TODO: pmixp_state_coll_cleanup();
private:

  PmixJobInfo m_pmix_job_info_;

  util::rw_mutex m_mutex_;
  std::vector<std::shared_ptr<Coll>> m_coll_list_;
#endif
};

} // namespace pmix
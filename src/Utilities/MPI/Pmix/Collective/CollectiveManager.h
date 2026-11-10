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

#include "Collective/Collective.h"
#include "StepControlClient.h"
#include "Types.h"
#include "crane/Lock.h"

namespace pmix {

// Forward declarations — avoid including PmixConn headers at this level.
class PmixClient;

class CollectiveManager {
 public:
#ifdef HAVE_PMIX
  // pmix_client and craned_client are injected so collective objects created
  // here never need to call PmixServer::GetInstance().
  CollectiveManager(const StepInfo& job_info, PmixClient* pmix_client,
                    mpi::StepControlClient* craned_client)
      : m_step_info_(job_info),
        m_pmix_client_(pmix_client),
        m_step_client_(craned_client) {}

  std::shared_ptr<Collective> GetCollective(
      CollectiveType type, const std::vector<pmix_proc_t>& procs);

  // Scan all active collectives and abort any that have been waiting longer
  // than timeout_sec seconds.  Called periodically from the uvw cleanup timer.
  void CleanupTimedOut(std::chrono::seconds timeout);

  void AbortAll();

 private:
  StepInfo m_step_info_;
  PmixClient* m_pmix_client_{nullptr};              // injected, not owned
  mpi::StepControlClient* m_step_client_{nullptr};  // injected, not owned

  util::rw_mutex m_mutex_;
  std::vector<std::shared_ptr<Collective>> m_coll_list_;

  // Set to true by AbortAll() to prevent GetCollective() from creating
  // new collective objects after the step has been aborted.  Without this,
  // racing peer ring messages that arrive after AbortAll() clears
  // m_coll_list_ would cause GetCollective() to create a brand-new
  // RingCollective (with m_aborted_=false) that then hits the "unexpected
  // contrib" path, triggering a spurious extra TerminateSteps() call.
  bool m_aborted_{false};
#endif
};

}  // namespace pmix

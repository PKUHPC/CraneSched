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

#include <memory>
#include <string>

#include "Types.h"

namespace pmix {
class PmixClient;

class ModexManager {
 public:
#ifdef HAVE_PMIX
  ModexManager(const StepInfo& step_info, PmixClient* client,
               std::chrono::seconds timeout);
  ~ModexManager();
  ModexManager(const ModexManager&) = delete;
  ModexManager& operator=(const ModexManager&) = delete;

  // True transfers callback ownership, including asynchronous send failures.
  // False rejects synchronously without invoking the callback.
  bool Get(const std::string& nspace, int rank, pmix_modex_cbfunc_t callback,
           void* data);
  void HandleRequest(uint32_t sequence, const CranedId& sender,
                     const pmix_proc_t& process, const std::string& nspace);
  void HandleResponse(uint32_t sequence, const CranedId& sender,
                      const std::string& data, pmix_status_t status);
  void CleanupTimeoutRequests();
  void DrainAllRequests();

 private:
  void ResponseWithError_(uint32_t sequence, const CranedId& peer,
                          pmix_status_t status);
  struct PendingRequests;
  StepInfo m_step_info_;
  PmixClient* m_client_;
  std::chrono::seconds m_timeout_;
  std::shared_ptr<PendingRequests> m_requests_;
#endif
};
}  // namespace pmix

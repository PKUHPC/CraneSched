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
#include <pmix_common.h>
#include <pmix_server.h>
#endif

#include <sys/types.h>

#include <uvw.hpp>
#include <vector>

#include "CranedClient.h"
#include "PmixCallbacks.h"
#include "PmixConn/PmixASyncServer.h"
#include "PmixConn/PmixClient.h"
#include "PmixConn/PmixUcxServer.h"
#include "PmixDModex.h"
#include "PmixState.h"
#include "PmixCommon.h"

#include "concurrentqueue/concurrentqueue.h"
#include "crane/PublicHeader.h"

namespace pmix {

class PmixServer {
 public:
  PmixServer() = default;

  ~PmixServer();
  PmixServer(const PmixServer&) = delete;
  PmixServer& operator=(const PmixServer&) = delete;
  PmixServer(PmixServer&&) = delete;
  PmixServer& operator=(PmixServer&&) = delete;

  bool Init(const Config& config, const crane::grpc::StepToD& step);

  std::optional<std::unordered_map<std::string, std::string>> SetupFork(
      uint32_t rank);

  // Returns the singleton PmixServer instance (valid after Init(), null after
  // destruction).  All PMIx C callbacks reach server state through this accessor
  // instead of a raw global variable.
  static PmixServer* GetInstance() { return s_instance_; }

  uint64_t GetTimeout() const { return m_timeout_; }

  std::string GetFenceType() const { return m_pmix_job_info_.fence_type; }

  CranedClient* GetCranedClient() const { return m_craned_client_.get(); }
  PmixClient* GetPmixClient() const { return m_pmix_client_.get(); }
  PmixDModexReqManager* GetDmodexReqManager() const { return m_dmodex_mgr_.get(); }
  PmixState* GetPmixState() const { return m_pmix_state_.get(); }

  uvw::loop* GetUvwLoop() const { return m_uvw_loop_.get(); }

 private:
 #ifdef HAVE_PMIX
  bool InfoSet_(const Config& config, const crane::grpc::StepToD& step);

  bool ConnInit_(const Config& config);

  bool PmixInit_() const;

  bool JobSet_();

  template <typename T>
  pmix_info_t InfoLoad_(const std::string& key, const T& val,
                        pmix_data_type_t data_type);

  // Singleton instance pointer — set by Init(), cleared by ~PmixServer().
  // Private so external code must use GetInstance().
  static PmixServer* s_instance_;
#endif

  PmixJobInfo m_pmix_job_info_;

  uint64_t m_timeout_{5};

  std::unique_ptr<CranedClient> m_craned_client_;
  std::unique_ptr<PmixClient> m_pmix_client_;
  std::unique_ptr<PmixASyncServer> m_pmix_async_server_;

  std::unique_ptr<PmixDModexReqManager> m_dmodex_mgr_;
  std::unique_ptr<pmix::PmixState> m_pmix_state_;

  std::thread m_uvw_thread_;
  std::atomic_bool m_cq_closed_{false};
  std::shared_ptr<uvw::loop> m_uvw_loop_;
  std::shared_ptr<uvw::timer_handle> m_cleanup_timer_handle_;
};

}  // namespace pmix

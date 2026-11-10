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
#  include <pmix_common.h>
#  include <pmix_server.h>
#endif

#include <sys/types.h>

#include <uvw.hpp>
#include <vector>

#include "Callbacks.h"
#include "Collective/CollectiveManager.h"
#include "Modex/ModexManager.h"
#include "StepControlClient.h"
#include "Transport/PmixAsyncServer.h"
#include "Transport/PmixClient.h"
#include "Transport/PmixUcxServer.h"
#include "Types.h"
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

#ifdef HAVE_PMIX
  pmix_status_t Abort(const pmix_proc_t* proc, void* server_object, int status,
                      const char msg[], pmix_proc_t targets[], size_t ntargets,
                      pmix_op_cbfunc_t cbfunc, void* cbdata);
  pmix_status_t FenceNb(const pmix_proc_t procs[], size_t nprocs,
                        const pmix_info_t info[], size_t ninfo, char* data,
                        size_t ndata, pmix_modex_cbfunc_t cbfunc, void* cbdata);
  pmix_status_t DModex(const pmix_proc_t* proc, const pmix_info_t info[],
                       size_t ninfo, pmix_modex_cbfunc_t cbfunc, void* cbdata);
#endif

  // Returns the singleton PmixServer instance (valid after Init(), null after
  // destruction).  All PMIx C callbacks reach server state through this
  // accessor instead of a raw global variable.
#ifdef HAVE_PMIX
  static PmixServer* GetInstance() { return s_instance_; }
#else
  static PmixServer* GetInstance() { return nullptr; }
#endif

  std::chrono::seconds GetTimeout() const { return m_timeout_; }

  std::string GetFenceType() const { return m_step_info_.fence_type; }

  mpi::StepControlClient* GetStepControlClient() const {
    return m_step_client_.get();
  }
  PmixClient* GetPmixClient() const { return m_pmix_client_.get(); }
  ModexManager* GetModexManager() const { return m_modex_manager_.get(); }
  CollectiveManager* GetCollectiveManager() const {
    return m_collective_manager_.get();
  }

  uvw::loop* GetUvwLoop() const { return m_uvw_loop_.get(); }

 private:
  void Shutdown_();
#ifdef HAVE_PMIX
  bool InfoSet_(const Config& config, const crane::grpc::StepToD& step);

  bool ConnInit_(const Config& config);

  bool PmixInit_();

  bool JobSet_();

  // Singleton instance pointer — set by Init(), cleared by ~PmixServer().
  // Private so external code must use GetInstance().
  static PmixServer* s_instance_;
#endif

  StepInfo m_step_info_;

  bool m_enable_slurm_compatible_env_{false};

  std::chrono::seconds m_timeout_{5};

  std::unique_ptr<mpi::StepControlClient> m_step_client_;
  std::unique_ptr<PmixClient> m_pmix_client_;
  std::unique_ptr<PmixAsyncServer> m_pmix_async_server_;

  std::unique_ptr<ModexManager> m_modex_manager_;
  std::unique_ptr<pmix::CollectiveManager> m_collective_manager_;

  // Set to true only after PMIx_server_init() succeeds; guards against
  // double-finalize when Init() fails mid-way and the destructor also runs.
  bool m_pmix_inited_{false};

  std::thread m_uvw_thread_;
  std::shared_ptr<uvw::async_handle> m_shutdown_handle_;
  std::shared_ptr<uvw::loop> m_uvw_loop_;
  std::shared_ptr<uvw::timer_handle> m_cleanup_timer_handle_;
};

}  // namespace pmix

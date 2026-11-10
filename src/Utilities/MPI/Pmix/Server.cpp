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

#include "Server.h"

#include "Collective/Collective.h"
#include "Transport/PmixGrpcClient.h"
#include "Transport/PmixGrpcServer.h"

#ifdef HAVE_UCX
#  include "Transport/PmixUcxClient.h"
#  include "Transport/PmixUcxServer.h"
#endif

#include "absl/strings/str_split.h"
#include "absl/synchronization/blocking_counter.h"
#include "crane/OS.h"
#include "crane/String.h"
#include "fmt/printf.h"

namespace pmix {

// Singleton definition — one per process, set by Init(), cleared by
// ~PmixServer().
#ifdef HAVE_PMIX
PmixServer* PmixServer::s_instance_ = nullptr;
#endif

PmixServer::~PmixServer() { Shutdown_(); }

void PmixServer::Shutdown_() {
#ifdef HAVE_PMIX
  if (m_uvw_thread_.joinable()) {
    m_shutdown_handle_->send();
    m_uvw_thread_.join();
  }

  PmixServerCallbacks::DeregisterEvents();

  // Drain all active collectives and pending dmodex requests before destroying
  // PmixClient.  Any in-flight gRPC callbacks that still hold a
  // shared_ptr<Collective> (via 'self') will see a stale seq and take the
  // early-return path, so they will NOT dereference the raw m_pmix_client_
  // pointer after it is reset below.
  if (m_collective_manager_) m_collective_manager_->AbortAll();
  if (m_modex_manager_) m_modex_manager_->DrainAllRequests();

  // Guard against double-finalize: Init()'s err_conn path already called
  // PMIx_server_finalize() and cleared m_pmix_inited_.
  if (m_pmix_inited_) {
    // Deregister the nspace before finalizing so that PMIx forces all pending
    // client connections (including those waiting for in-flight fence/modex
    // operations) to be torn down.  Without this, PMIx_server_finalize() can
    // block indefinitely when a peer's kRing message arrived just before
    // PMIx_Abort was called, leaving a collective stuck in PROGRESS state with
    // no local contribution ever coming.
    if (!m_step_info_.nspace.empty()) {
      // PMIx_server_deregister_nspace() returns void; completion is signalled
      // exclusively through the callback (PmixOperationCallback →
      // BlockingCounter).
      absl::BlockingCounter bc(1);
      PMIx_server_deregister_nspace(m_step_info_.nspace.c_str(),
                                    PmixOperationCallback, &bc);
      bc.Wait();
    }
    int rc = PMIx_server_finalize();
    if (rc != PMIX_SUCCESS)
      CRANE_ERROR("Failed to finalize PMIx server: {}", PMIx_Error_string(rc));
    m_pmix_inited_ = false;
  }

  m_pmix_async_server_.reset();
  m_modex_manager_.reset();
  m_collective_manager_.reset();
  m_pmix_client_.reset();
  m_step_client_.reset();

  CRANE_TRACE("[Step#{}.{}] Finalize PmixServer.", m_step_info_.job_id,
              m_step_info_.step_id);

  // Clear the singleton so callbacks can detect that the server is gone.
  if (s_instance_ == this) s_instance_ = nullptr;
  if (!m_step_info_.server_tmpdir.empty())
    util::os::DeleteFolders(m_step_info_.server_tmpdir);
#endif
}

bool PmixServer::Init(const Config& config, const crane::grpc::StepToD& step) {
  m_enable_slurm_compatible_env_ = config.EnableSlurmCompatibleEnv;
#ifdef HAVE_PMIX
  CRANE_TRACE("[Step#{}.{}] Initializing PmixServer...", step.job_id(),
              step.step_id());
  m_uvw_loop_ = uvw::loop::create();

  if (!InfoSet_(config, step)) {
    Shutdown_();
    return false;
  }

  if (!PmixInit_()) {
    Shutdown_();
    return false;
  }

  // ConnInit_ creates m_pmix_client_ and m_step_client_ first, then
  // injects them into m_modex_manager_ and m_collective_manager_.
  if (!ConnInit_(config)) goto err_conn;

  CRANE_TRACE("[Step#{}.{}] PmixServer conn initialized.", step.job_id(),
              step.step_id());

  m_cleanup_timer_handle_ = m_uvw_loop_->resource<uvw::timer_handle>();

  m_cleanup_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle&) {
        // Periodically clean up timed-out direct-modex requests.
        m_modex_manager_->CleanupTimeoutRequests();
        // Periodically abort collectives that have been stalled too long.
        m_collective_manager_->CleanupTimedOut(m_timeout_);
      });

  m_cleanup_timer_handle_->start(m_timeout_, m_timeout_);

  m_shutdown_handle_ = m_uvw_loop_->resource<uvw::async_handle>();
  m_shutdown_handle_->on<uvw::async_event>(
      [](const uvw::async_event&, uvw::async_handle& handle) {
        handle.parent().walk([](auto&& resource) { resource.close(); });
        handle.parent().stop();
      });
  m_uvw_thread_ = std::thread([this] {
    util::SetCurrentThreadName("PmixLoop");
    m_uvw_loop_->run();
  });

  if (!JobSet_()) goto err_thread;

  CRANE_INFO("Launch the PMIx server, dir: {}, version {}.{}.{}",
             m_step_info_.server_tmpdir, PMIX_VERSION_MAJOR, PMIX_VERSION_MINOR,
             PMIX_VERSION_RELEASE);

  // Only register the singleton after full initialization succeeds.
  s_instance_ = this;
  return true;

err_thread:
err_conn:
  Shutdown_();
  return false;
#else
  CRANE_ERROR("PMIx support is not enabled in this build.");
  return false;
#endif
}

#ifdef HAVE_PMIX
bool PmixServer::ConnInit_(const Config& config) {
  // Phase 1: Create the transport client and craned client so they can be
  // injected into state objects in Phase 2.
  m_step_client_ = std::make_unique<mpi::StepControlClient>(
      m_step_info_.job_id, m_step_info_.step_id, m_step_info_.craned_id,
      m_step_info_.node_list, crane::grpc::mpi::PROTOCOL_PMIX);
  m_step_client_->InitChannelAndStub(config.CranedUnixSocketPath);

  if (m_step_info_.pmix_direct_conn_ucx == "true") {
#  ifdef HAVE_UCX
    m_pmix_client_ = std::make_unique<PmixUcxClient>(m_step_info_.node_num);
    CRANE_TRACE(
        "Using UCX for PMIx communication as CranePmixDirectConnUcx is set to "
        "true.");
#  else
    CRANE_ERROR(
        "UCX support is not enabled in this build, cannot use UCX for PMIx "
        "communication.");
    return false;
#  endif
  } else {
    m_pmix_client_ =
        std::make_unique<PmixGrpcClient>(m_step_info_.node_num, config);
    CRANE_TRACE("Using gRPC for PMIx communication as default.");
  }

  // Phase 2: Create state objects now that the client pointers are available,
  // injecting them so the objects never need to call GetInstance().
  m_modex_manager_ = std::make_unique<ModexManager>(
      m_step_info_, m_pmix_client_.get(), m_timeout_);
  m_collective_manager_ = std::make_unique<CollectiveManager>(
      m_step_info_, m_pmix_client_.get(), m_step_client_.get());

  // Phase 3: Create and initialize the async server.
  if (m_step_info_.pmix_direct_conn_ucx == "true") {
#  ifdef HAVE_UCX
    auto* ucx_client = dynamic_cast<PmixUcxClient*>(m_pmix_client_.get());
    m_pmix_async_server_ = std::make_unique<PmixUcxServer>(
        m_modex_manager_.get(), m_collective_manager_.get(),
        m_step_client_.get(), m_uvw_loop_.get(), ucx_client);
#  endif
  } else {
    m_pmix_async_server_ = std::make_unique<PmixGrpcServer>(
        m_modex_manager_.get(), m_collective_manager_.get(),
        m_step_client_.get());
  }

  if (!m_pmix_async_server_->Init(config)) return false;

  // Ensure all peer stubs are ready before proceeding.
  if (!m_pmix_client_->WaitAllStubReady()) {
    CRANE_ERROR(
        "Not all PMIx stubs are ready after waiting, node_num={}, "
        "channel_count={}",
        m_step_info_.node_num, m_pmix_client_->GetChannelCount());
    return false;
  }

  return true;
}

bool PmixServer::PmixInit_() {
  if (!util::os::CreateFolders(m_step_info_.server_tmpdir)) return false;

  if (!m_step_info_.cli_tmpdir.empty()) {
    if (!util::os::CreateFolders(m_step_info_.cli_tmpdir)) return false;
  }

  pmix_status_t rc;

  pmix_info_t* server_info;
  int server_info_size = 2;
  if (PMIX_VERSION_MAJOR < 5) ++server_info_size;
#  ifdef PMIX_HOSTNAME_KEEP_FQDN
  ++server_info_size;
#  endif

  PMIX_INFO_CREATE(server_info, server_info_size);
  int index = 0;
  if (PMIX_VERSION_MAJOR < 5)
    PMIX_INFO_LOAD(&server_info[index++], PMIX_USERID, &m_step_info_.uid,
                   PMIX_UINT32);
#  ifdef PMIX_HOSTNAME_KEEP_FQDN
  // PMIx 5 otherwise shortens FQDN node-map entries. Its legacy peer encoding
  // sends local node keys only when the server and namespace hostnames match.
  bool keep_fqdn = true;
  PMIX_INFO_LOAD(&server_info[index++], PMIX_HOSTNAME_KEEP_FQDN, &keep_fqdn,
                 PMIX_BOOL);
#  endif

  PMIX_INFO_LOAD(&server_info[index++], PMIX_SERVER_TMPDIR,
                 m_step_info_.server_tmpdir.c_str(), PMIX_STRING);
  PMIX_INFO_LOAD(&server_info[index], PMIX_HOSTNAME,
                 m_step_info_.node_hostnames[m_step_info_.node_id].c_str(),
                 PMIX_STRING);
  rc = PMIx_server_init(PmixServerCallbacks::Module(), server_info,
                        server_info_size);
  PMIX_INFO_FREE(server_info, server_info_size);
  if (PMIX_SUCCESS != rc) {
    CRANE_ERROR("Pmix Server Init failed with error {}", PMIx_Error_string(rc));
    return false;
  }
  // Mark PMIx as initialized so the destructor knows it must finalize.
  m_pmix_inited_ = true;

  PmixServerCallbacks::RegisterEvents();

  return true;
}

#endif

}  // namespace pmix

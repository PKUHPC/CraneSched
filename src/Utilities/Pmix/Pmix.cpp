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

#include "Pmix.h"

#include "PmixColl/PmixColl.h"
#include "PmixConn/PmixGrpcClient.h"
#include "PmixConn/PmixGrpcServer.h"

#ifdef HAVE_UCX
#  include "PmixConn/PmixUcxClient.h"
#  include "PmixConn/PmixUcxServer.h"
#endif

#include "absl/strings/str_split.h"
#include "absl/synchronization/blocking_counter.h"
#include "crane/OS.h"
#include "crane/String.h"
#include "fmt/printf.h"

namespace pmix {

// ── extern "C" wrappers ────────────────────────────────────────────────────
// Each wrapper is a one-line forward to PmixServerCallbacks so that all PMIx
// C callbacks have the correct ABI while the actual logic stays in a C++ class.
#ifdef HAVE_PMIX
extern "C" {

static int ClientConnectedWrapper(const pmix_proc_t* p, void* s,
                                  pmix_op_cbfunc_t cb, void* d) {
  return pmix::PmixServerCallbacks::ClientConnected(p, s, cb, d);
}
static pmix_status_t ClientFinalizedWrapper(const pmix_proc_t* p, void* s,
                                            pmix_op_cbfunc_t cb, void* d) {
  return pmix::PmixServerCallbacks::ClientFinalized(p, s, cb, d);
}
static pmix_status_t AbortWrapper(const pmix_proc_t* p, void* s, int st,
                                  const char m[], pmix_proc_t ps[], size_t np,
                                  pmix_op_cbfunc_t cb, void* d) {
  return pmix::PmixServerCallbacks::Abort(p, s, st, m, ps, np, cb, d);
}
static pmix_status_t FenceNbWrapper(const pmix_proc_t pv[], size_t np,
                                    const pmix_info_t inf[], size_t ni,
                                    char* data, size_t nd,
                                    pmix_modex_cbfunc_t cb, void* d) {
  return pmix::PmixServerCallbacks::FenceNb(pv, np, inf, ni, data, nd, cb, d);
}
static pmix_status_t DModexWrapper(const pmix_proc_t* p,
                                   const pmix_info_t inf[], size_t ni,
                                   pmix_modex_cbfunc_t cb, void* d) {
  return pmix::PmixServerCallbacks::DModex(p, inf, ni, cb, d);
}
static pmix_status_t JobControlWrapper(const pmix_proc_t* p,
                                       const pmix_proc_t t[], size_t nt,
                                       const pmix_info_t d2[], size_t nd,
                                       pmix_info_cbfunc_t cb, void* d) {
  return pmix::PmixServerCallbacks::JobControl(p, t, nt, d2, nd, cb, d);
}
static pmix_status_t PublishWrapper(const pmix_proc_t* p,
                                    const pmix_info_t inf[], size_t ni,
                                    pmix_op_cbfunc_t cb, void* d) {
  return pmix::PmixServerCallbacks::Publish(p, inf, ni, cb, d);
}
static pmix_status_t LookupWrapper(const pmix_proc_t* p, char** k,
                                   const pmix_info_t inf[], size_t ni,
                                   pmix_lookup_cbfunc_t cb, void* d) {
  return pmix::PmixServerCallbacks::Lookup(p, k, inf, ni, cb, d);
}
static pmix_status_t UnpublishWrapper(const pmix_proc_t* p, char** k,
                                      const pmix_info_t inf[], size_t ni,
                                      pmix_op_cbfunc_t cb, void* d) {
  return pmix::PmixServerCallbacks::Unpublish(p, k, inf, ni, cb, d);
}
static pmix_status_t SpawnWrapper(const pmix_proc_t* p, const pmix_info_t inf[],
                                  size_t ni, const pmix_app_t apps[], size_t na,
                                  pmix_spawn_cbfunc_t cb, void* d) {
  return pmix::PmixServerCallbacks::Spawn(p, inf, ni, apps, na, cb, d);
}
static pmix_status_t ConnectWrapper(const pmix_proc_t p[], size_t np,
                                    const pmix_info_t inf[], size_t ni,
                                    pmix_op_cbfunc_t cb, void* d) {
  return pmix::PmixServerCallbacks::Connect(p, np, inf, ni, cb, d);
}
static pmix_status_t DisconnectWrapper(const pmix_proc_t p[], size_t np,
                                       const pmix_info_t inf[], size_t ni,
                                       pmix_op_cbfunc_t cb, void* d) {
  return pmix::PmixServerCallbacks::Disconnect(p, np, inf, ni, cb, d);
}
static void AppCbWrapper(pmix_status_t st, pmix_info_t inf[], size_t ni,
                         void* cbd, pmix_op_cbfunc_t cb, void* cd) {
  pmix::PmixServerCallbacks::AppCb(st, inf, ni, cbd, cb, cd);
}
static void OpCbWrapper(pmix_status_t st, void* cd) {
  pmix::PmixServerCallbacks::OpCb(st, cd);
}
static void ErrHandlerRegCbWrapper(pmix_status_t st, size_t ref, void* cd) {
  pmix::PmixServerCallbacks::ErrHandlerRegCb(st, ref, cd);
}
static void ErrHandlerWrapper(size_t id, pmix_status_t st,
                              const pmix_proc_t* src, pmix_info_t inf[],
                              size_t ni, pmix_info_t* res, size_t nr,
                              pmix_event_notification_cbfunc_fn_t cb,
                              void* cd) {
  pmix::PmixServerCallbacks::ErrHandler(id, st, src, inf, ni, res, nr, cb, cd);
}

}  // extern "C"

// PMIx server module struct — references only the extern "C" wrappers above.
static pmix_server_module_t s_crane_pmix_module = {
    .client_connected = ClientConnectedWrapper,
    .client_finalized = ClientFinalizedWrapper,
    .abort = AbortWrapper,
    .fence_nb = FenceNbWrapper,
    .direct_modex = DModexWrapper,
    .publish = PublishWrapper,
    .lookup = LookupWrapper,
    .unpublish = UnpublishWrapper,
    .spawn = SpawnWrapper,
    .connect = ConnectWrapper,
    .disconnect = DisconnectWrapper,
    .job_control = JobControlWrapper,
};

#endif  // HAVE_PMIX

// Singleton definition — one per process, set by Init(), cleared by
// ~PmixServer().
#ifdef HAVE_PMIX
PmixServer* PmixServer::s_instance_ = nullptr;
#endif

PmixServer::~PmixServer() {
#ifdef HAVE_PMIX
  m_cq_closed_ = true;
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();

  util::os::DeleteFolders(m_pmix_job_info_.server_tmpdir);

  // Deregister the errhandler using the stored ref from registration.
  if (PmixServerCallbacks::s_errhandler_ref != 0) {
    PMIx_Deregister_event_handler(PmixServerCallbacks::s_errhandler_ref,
                                  OpCbWrapper, nullptr);
  }

  // Drain all active collectives and pending dmodex requests before destroying
  // PmixClient.  Any in-flight gRPC callbacks that still hold a
  // shared_ptr<Coll> (via 'self') will see a stale seq and take the
  // early-return path, so they will NOT dereference the raw m_pmix_client_
  // pointer after it is reset below.
  if (m_pmix_state_) m_pmix_state_->AbortAllColls();
  if (m_dmodex_mgr_) m_dmodex_mgr_->DrainAllRequests();

  // Guard against double-finalize: Init()'s err_conn path already called
  // PMIx_server_finalize() and cleared m_pmix_inited_.
  if (m_pmix_inited_) {
    int rc = PMIx_server_finalize();
    if (rc != PMIX_SUCCESS)
      CRANE_ERROR("Failed to finalize PMIx server: {}", PMIx_Error_string(rc));
    m_pmix_inited_ = false;
  }

  m_pmix_async_server_.reset();
  m_dmodex_mgr_.reset();
  m_pmix_state_.reset();
  m_pmix_client_.reset();
  m_craned_client_.reset();

  CRANE_TRACE("[Step#{}.{}] Finalize PmixServer.", m_pmix_job_info_.job_id,
              m_pmix_job_info_.step_id);

  // Clear the singleton so callbacks can detect that the server is gone.
  s_instance_ = nullptr;
#endif
}

bool PmixServer::Init(const Config& config, const crane::grpc::StepToD& step) {
#ifdef HAVE_PMIX
  CRANE_TRACE("[Step#{}.{}] Initializing PmixServer...", step.job_id(),
              step.step_id());
  m_uvw_loop_ = uvw::loop::create();

  if (!InfoSet_(config, step)) return false;

  if (!PmixInit_()) return false;

  // ConnInit_ creates m_pmix_client_ and m_craned_client_ first, then
  // injects them into m_dmodex_mgr_ and m_pmix_state_.
  if (!ConnInit_(config)) goto err_conn;

  CRANE_TRACE("[Step#{}.{}] PmixServer conn initialized.", step.job_id(),
              step.step_id());

  m_cleanup_timer_handle_ = m_uvw_loop_->resource<uvw::timer_handle>();

  m_cleanup_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle&) {
        // Periodically clean up timed-out direct-modex requests.
        m_dmodex_mgr_->CleanupTimeoutRequests();
        // Periodically abort collectives that have been stalled too long.
        m_pmix_state_->CleanupTimeoutColls(m_timeout_);
      });

  m_cleanup_timer_handle_->start(m_timeout_, m_timeout_);

  m_uvw_thread_ = std::thread([this] {
    util::SetCurrentThreadName("PmixLoopThread_");

    auto idle_handle = m_uvw_loop_->resource<uvw::idle_handle>();
    idle_handle->on<uvw::idle_event>(
        [this](const uvw::idle_event&, uvw::idle_handle& h) {
          if (m_cq_closed_) {
            h.parent().walk([](auto&& h) { h.close(); });
            h.parent().stop();
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
        });

    if (idle_handle->start() != 0) {
      CRANE_ERROR("Failed to start the idle event in CranedKeeper loop.");
    }

    m_uvw_loop_->run();
  });

  if (!JobSet_()) goto err_thread;

  CRANE_INFO("Launch the PMIx server, dir: {}, version {}.{}.{}",
             m_pmix_job_info_.server_tmpdir, PMIX_VERSION_MAJOR,
             PMIX_VERSION_MINOR, PMIX_VERSION_RELEASE);

  // Only register the singleton after full initialization succeeds.
  s_instance_ = this;
  return true;

err_thread:
  // Stop the UVW loop thread before tearing down PMIx connections.
  m_cq_closed_ = true;
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
  m_uvw_loop_.reset();
  // fall through to clean up PMIx and connections

err_conn:
  if (m_pmix_state_) m_pmix_state_->AbortAllColls();
  if (m_dmodex_mgr_) m_dmodex_mgr_->DrainAllRequests();
  // Only finalize if PmixInit_() succeeded; clear the flag so ~PmixServer()
  // does not attempt a second finalize on the same object.
  if (m_pmix_inited_) {
    PMIx_server_finalize();
    m_pmix_inited_ = false;
  }
  m_pmix_async_server_.reset();
  m_dmodex_mgr_.reset();
  m_pmix_state_.reset();
  m_pmix_client_.reset();
  m_craned_client_.reset();
  return false;
#else
  CRANE_ERROR("PMIx support is not enabled in this build.");
  return false;
#endif
}

std::optional<std::unordered_map<std::string, std::string>>
PmixServer::SetupFork(uint32_t rank) {
#ifdef HAVE_PMIX
  char** client_env = nullptr;

  // Compute the correct global rank of this local rank (0-based offset within
  // this node's task block) by scanning task_map for the start rank of this
  // node, then adding the local rank offset.
  pmix_proc_t proc;
  PMIX_LOAD_NSPACE(proc.nspace, m_pmix_job_info_.nspace.c_str());
  proc.rank = rank;
  pmix_status_t rc = PMIx_server_setup_fork(&proc, &client_env);
  if (PMIX_SUCCESS != rc) {
    CRANE_ERROR("Server fork setup failed with error {}", rc);
    return std::nullopt;
  }

  std::unordered_map<std::string, std::string> env_map;
  for (int i = 0; client_env[i] != nullptr; ++i) {
    std::string env_entry{client_env[i]};
    size_t pos = env_entry.find('=');
    if (pos != std::string::npos) {
      std::string key = env_entry.substr(0, pos);
      std::string value = env_entry.substr(pos + 1);
      env_map.emplace(key, value);
    }
  }

  env_map.emplace("SLURM_JOBID", std::to_string(m_pmix_job_info_.job_id));
  env_map.emplace("SLURM_NODELIST", m_pmix_job_info_.node_list_str);
  env_map.emplace("SLURM_STEP_ID", std::to_string(m_pmix_job_info_.step_id));

  if (!env_map.contains("OMPI_MCA_orte_precondition_transports")) {
    char key[64];
    std::snprintf(key, sizeof(key), "%08x%08x-%08x%08x",
                  m_pmix_job_info_.job_id, 0, m_pmix_job_info_.step_id, 0);
    env_map.emplace("OMPI_MCA_orte_precondition_transports", key);
  }

  if (client_env != nullptr) {
    for (int i = 0; client_env[i] != nullptr; ++i) {
      free(client_env[i]);
    }
    free(client_env);
  }

  return env_map;
#else
  CRANE_ERROR("PMIx support is not enabled in this build.");
  return std::nullopt;
#endif
}

#ifdef HAVE_PMIX
bool PmixServer::InfoSet_(const Config& config,
                          const crane::grpc::StepToD& step) {
  m_pmix_job_info_.uid = step.uid();
  m_pmix_job_info_.gid = step.gid()[0];
  m_pmix_job_info_.job_id = step.job_id();
  m_pmix_job_info_.step_id = step.step_id();
  m_pmix_job_info_.nspace = fmt::format(
      "crane.pmix.{}.{}", m_pmix_job_info_.job_id, m_pmix_job_info_.step_id);

  m_pmix_job_info_.node_num = step.node_num();
  // Use the actual total task count, NOT node_tasks * node_num,
  // because tasks may not be evenly distributed across nodes.
  m_pmix_job_info_.task_num = step.task_node_list().size();
  std::string hostname(HOST_NAME_MAX + 1, '\0');
  if (gethostname(hostname.data(), hostname.size()) == 0) {
    hostname.resize(strlen(hostname.c_str()));
  } else {
    CRANE_ERROR("Failed to get hostname");
    return false;
  }
  m_pmix_job_info_.hostname = hostname;
  m_pmix_job_info_.node_list =
      std::vector<std::string>(step.nodelist().begin(), step.nodelist().end());
  m_pmix_job_info_.node_list_str =
      absl::StrJoin(m_pmix_job_info_.node_list, ",");

  auto it =
      std::ranges::find(m_pmix_job_info_.node_list, m_pmix_job_info_.hostname);
  if (it == m_pmix_job_info_.node_list.end()) {
    CRANE_ERROR(
        "[Step#{}.{}] Current hostname '{}' is not in the node list [{}] "
        "assigned to this step.",
        m_pmix_job_info_.job_id, m_pmix_job_info_.step_id,
        m_pmix_job_info_.hostname, m_pmix_job_info_.node_list_str);
    return false;
  }

  m_pmix_job_info_.node_id =
      std::ranges::distance(m_pmix_job_info_.node_list.begin(), it);

  m_pmix_job_info_.peer_node_list = std::vector(m_pmix_job_info_.node_list);
  std::erase(m_pmix_job_info_.peer_node_list, m_pmix_job_info_.hostname);

  // Build task_map: task_map[rank] = node_id (block order).
  // tasks_per_node_list[i] is the number of tasks on nodelist[i].
  m_pmix_job_info_.task_map.assign(step.task_node_list().begin(),
                                   step.task_node_list().end());

  // node_tasks: the actual number of tasks on THIS specific node.
  for (const auto& node_id : step.task_node_list()) {
    if (node_id == m_pmix_job_info_.node_id) m_pmix_job_info_.node_tasks++;
  }

  if (step.env().contains("CRANE_PMIX_TIMEOUT")) {
    auto timeout_str = step.env().at("CRANE_PMIX_TIMEOUT");
    if (timeout_str != "") {
      try {
        m_timeout_ = std::chrono::seconds(std::stoul(timeout_str));
      } catch (const std::exception& e) {
        CRANE_WARN("Failed to parse {} with error {}, use default timeout {}s",
                   timeout_str, e.what(), m_timeout_.count());
      }
    }
  }

  m_pmix_job_info_.server_tmpdir =
      (std::filesystem::path(config.CraneBaseDir) /
       fmt::format("pmix.crane.{}", m_pmix_job_info_.hostname) /
       fmt::format("pmix.{}.{}", m_pmix_job_info_.job_id,
                   m_pmix_job_info_.step_id))
          .string();

  if (step.env().contains("PMIXP_PMIXLIB_TMPDIR")) {
    // pmixlib_tmpdir is left for the user to configure, as the directory must
    // be accessible to the user. By default, it is unset and the PMIx default
    // directory will be used.
    std::string pmixlib_tmpdir = step.env().at("PMIXP_PMIXLIB_TMPDIR");
    if (!pmixlib_tmpdir.empty()) {
      m_pmix_job_info_.cli_tmpdir_base =
          (std::filesystem::path(pmixlib_tmpdir) / "pmix.crane.cli").string();
      m_pmix_job_info_.cli_tmpdir =
          (std::filesystem::path(m_pmix_job_info_.cli_tmpdir_base) /
           fmt::format("spmix_appdir_{}.{}.{}", m_pmix_job_info_.uid,
                       m_pmix_job_info_.job_id, m_pmix_job_info_.step_id))
              .string();
    }
  }

  if (step.env().contains("CRANE_PMIX_FENCE"))
    m_pmix_job_info_.fence_type = step.env().at("CRANE_PMIX_FENCE");

  if (step.env().contains("CRANE_PMIX_DIRECT_CONN_UCX"))
    m_pmix_job_info_.pmix_direct_conn_ucx =
        step.env().at("CRANE_PMIX_DIRECT_CONN_UCX");

  return true;
}

bool PmixServer::ConnInit_(const Config& config) {
  // Phase 1: Create the transport client and craned client so they can be
  // injected into state objects in Phase 2.
  m_craned_client_ = std::make_unique<CranedClient>(m_pmix_job_info_);
  m_craned_client_->InitChannelAndStub(config.CranedUnixSocketPath);

  if (m_pmix_job_info_.pmix_direct_conn_ucx == "true") {
#  ifdef HAVE_UCX
    m_pmix_client_ = std::make_unique<PmixUcxClient>(m_pmix_job_info_.node_num);
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
        std::make_unique<PmixGrpcClient>(m_pmix_job_info_.node_num);
    CRANE_TRACE("Using gRPC for PMIx communication as default.");
  }

  // Phase 2: Create state objects now that the client pointers are available,
  // injecting them so the objects never need to call GetInstance().
  m_dmodex_mgr_ = std::make_unique<PmixDModexReqManager>(
      m_pmix_job_info_, m_pmix_client_.get(), m_timeout_);
  m_pmix_state_ = std::make_unique<PmixState>(
      m_pmix_job_info_, m_pmix_client_.get(), m_craned_client_.get());

  // Phase 3: Create and initialize the async server.
  if (m_pmix_job_info_.pmix_direct_conn_ucx == "true") {
#  ifdef HAVE_UCX
    auto* ucx_client = dynamic_cast<PmixUcxClient*>(m_pmix_client_.get());
    m_pmix_async_server_ = std::make_unique<PmixUcxServer>(
        m_dmodex_mgr_.get(), m_pmix_state_.get(), m_craned_client_.get(),
        m_uvw_loop_.get(), ucx_client);
#  endif
  } else {
    m_pmix_async_server_ = std::make_unique<PmixGrpcServer>(
        m_dmodex_mgr_.get(), m_pmix_state_.get(), m_craned_client_.get());
  }

  if (!m_pmix_async_server_->Init(config)) return false;

  // Ensure all peer stubs are ready before proceeding.
  if (!m_pmix_client_->WaitAllStubReady()) {
    CRANE_ERROR(
        "Not all PMIx stubs are ready after waiting, node_num={}, "
        "channel_count={}",
        m_pmix_job_info_.node_num, m_pmix_client_->GetChannelCount());
    return false;
  }

  return true;
}

bool PmixServer::PmixInit_() {
  if (!util::os::CreateFolders(m_pmix_job_info_.server_tmpdir)) return false;

  if (!m_pmix_job_info_.cli_tmpdir.empty()) {
    if (!util::os::CreateFolders(m_pmix_job_info_.cli_tmpdir)) return false;
  }

  pmix_status_t rc;

  pmix_info_t* server_info;
  int server_info_size = 1;
  if (PMIX_VERSION_MAJOR < 5) server_info_size = 2;

  PMIX_INFO_CREATE(server_info, server_info_size);
  if (PMIX_VERSION_MAJOR < 5)
    PMIX_INFO_LOAD(&server_info[0], PMIX_USERID, &m_pmix_job_info_.uid,
                   PMIX_UINT32);

  PMIX_INFO_LOAD(&server_info[server_info_size - 1], PMIX_SERVER_TMPDIR,
                 m_pmix_job_info_.server_tmpdir.c_str(), PMIX_STRING);
  rc = PMIx_server_init(&s_crane_pmix_module, server_info, server_info_size);
  PMIX_INFO_FREE(server_info, server_info_size);
  if (PMIX_SUCCESS != rc) {
    CRANE_ERROR("Pmix Server Init failed with error {}", PMIx_Error_string(rc));
    return false;
  }
  // Mark PMIx as initialized so the destructor knows it must finalize.
  m_pmix_inited_ = true;

  PMIx_Register_event_handler(NULL, 0, NULL, 0, ErrHandlerWrapper,
                              ErrHandlerRegCbWrapper, NULL);

  return true;
}

bool PmixServer::JobSet_() {
  pmix_status_t rc;

  {
    absl::BlockingCounter bc(1);
    rc = PMIx_server_setup_application(m_pmix_job_info_.nspace.c_str(), nullptr,
                                       0, AppCbWrapper, &bc);
    if (PMIX_SUCCESS != rc) {
      CRANE_ERROR("Failed to setup application: {}", PMIx_Error_string(rc));
      return false;
    }
    bc.Wait();
  }

  std::vector<pmix_info_t> info_list;

  info_list.emplace_back(InfoLoad_(PMIX_SPAWNED, false, PMIX_BOOL));

  if (!m_pmix_job_info_.cli_tmpdir.empty()) {
    info_list.emplace_back(
        InfoLoad_(PMIX_TMPDIR, m_pmix_job_info_.cli_tmpdir_base, PMIX_STRING));
    info_list.emplace_back(
        InfoLoad_(PMIX_NSDIR, m_pmix_job_info_.cli_tmpdir, PMIX_STRING));
  }
  info_list.emplace_back(InfoLoad_(PMIX_TDIR_RMCLEAN, true, PMIX_BOOL));

  info_list.emplace_back(InfoLoad_(
      PMIX_JOBID, fmt::format("{}", m_pmix_job_info_.job_id), PMIX_STRING));
  info_list.emplace_back(
      InfoLoad_(PMIX_NODEID, m_pmix_job_info_.node_id, PMIX_UINT32));

  // Precompute the start global rank of THIS node so we can compute local_rank.
  // task_map is in block order, so the first index where task_map[r] == node_id
  // is the start rank of this node.
  uint32_t my_node_start_rank = 0;
  for (uint32_t r = 0; r < m_pmix_job_info_.task_num; ++r) {
    if (m_pmix_job_info_.task_map[r] == m_pmix_job_info_.node_id) {
      my_node_start_rank = r;
      break;
    }
  }

  std::list<uint32_t> local_ranks;
  for (uint32_t rank = 0; rank < m_pmix_job_info_.task_num; rank++) {
    pmix_data_array_t* proc_data;
    PMIX_DATA_ARRAY_CREATE(proc_data, 9, PMIX_INFO);
    auto* proc_data_arr = reinterpret_cast<pmix_info_t*>(proc_data->array);
    PMIX_INFO_LOAD(&proc_data_arr[0], PMIX_RANK, &rank, PMIX_PROC_RANK);

    int tmp = 0;
    PMIX_INFO_LOAD(&proc_data_arr[1], PMIX_APPNUM, &tmp, PMIX_INT);
    PMIX_INFO_LOAD(&proc_data_arr[2], PMIX_APPLDR, &tmp, PMIX_INT);

    PMIX_INFO_LOAD(&proc_data_arr[3], PMIX_GLOBAL_RANK, &rank, PMIX_UINT32);
    PMIX_INFO_LOAD(&proc_data_arr[4], PMIX_APP_RANK, &rank, PMIX_UINT32);

    // Use task_map to determine which node owns this rank, and set the
    // correct hostname/node_id for that rank.
    uint32_t rank_node_id = m_pmix_job_info_.task_map[rank];
    const std::string& rank_hostname = m_pmix_job_info_.node_list[rank_node_id];

    /* this rank is local, store local info about it */
    if (rank_node_id == m_pmix_job_info_.node_id) {
      // local_rank = offset within this node's task block
      uint32_t local_rank = rank - my_node_start_rank;
      PMIX_INFO_LOAD(&proc_data_arr[5], PMIX_NODE_RANK, &local_rank,
                     PMIX_UINT16);
      PMIX_INFO_LOAD(&proc_data_arr[6], PMIX_LOCAL_RANK, &local_rank,
                     PMIX_UINT16);
      local_ranks.emplace_back(rank);  // store global rank for PMIX_LOCAL_PEERS
    }

    PMIX_INFO_LOAD(&proc_data_arr[7], PMIX_HOSTNAME, rank_hostname.c_str(),
                   PMIX_STRING);
    PMIX_INFO_LOAD(&proc_data_arr[8], PMIX_NODEID, &rank_node_id, PMIX_UINT32);

    pmix_info_t info;
    PMIX_INFO_CONSTRUCT(&info);
    PMIX_LOAD_KEY(info.key, PMIX_PROC_DATA);
    info.value.type = PMIX_DATA_ARRAY;
    info.value.data.darray = proc_data;
    info_list.emplace_back(info);
    // Ownership of proc_data is transferred to info; do NOT call
    // PMIX_DATA_ARRAY_DESTRUCT here — it will be freed by PMIX_INFO_FREE.
  }

  // Job Size
  info_list.emplace_back(
      InfoLoad_(PMIX_UNIV_SIZE, m_pmix_job_info_.task_num, PMIX_UINT32));
  info_list.emplace_back(
      InfoLoad_(PMIX_JOB_SIZE, m_pmix_job_info_.task_num, PMIX_UINT32));
  info_list.emplace_back(InfoLoad_(
      PMIX_LOCAL_SIZE, m_pmix_job_info_.node_tasks, PMIX_UINT32));
  info_list.emplace_back(
      InfoLoad_(PMIX_NODE_SIZE, m_pmix_job_info_.node_tasks, PMIX_UINT32));
  info_list.emplace_back(
      InfoLoad_(PMIX_MAX_PROCS, m_pmix_job_info_.task_num, PMIX_UINT32));

  // TODO: _set_topology

  if (PMIX_VERSION_MAJOR >= 5)
    info_list.emplace_back(
        InfoLoad_(PMIX_USERID, m_pmix_job_info_.uid, PMIX_UINT32));

  // node_list node1,node2,node3
  std::unique_ptr<char, decltype(&free)> regex(nullptr, &free);
  char* raw_regex = nullptr;
  rc = PMIx_generate_regex(m_pmix_job_info_.node_list_str.c_str(), &raw_regex);
  regex.reset(raw_regex);
  if (rc != PMIX_SUCCESS) {
    CRANE_ERROR("Error: PMIx_generate_regex. {}", PMIx_Error_string(rc));
    for (auto& info : info_list) PMIX_INFO_DESTRUCT(&info);
    return false;
  }
  info_list.emplace_back(InfoLoad_(PMIX_NODE_MAP, regex, PMIX_STRING));

  // proc_map "0,1,2;3,4" (per-node rank lists, separated by ';')
  // Use task_map (block-ordered) to build the correct per-node rank lists,
  // handling uneven distribution where nodes may have different task counts.
  std::vector<std::vector<uint32_t>> node_tasks(m_pmix_job_info_.node_num);
  for (uint32_t task = 0; task < m_pmix_job_info_.task_num; ++task)
    node_tasks[m_pmix_job_info_.task_map[task]].push_back(task);

  std::string ppn_str = fmt::format(
      "{}", fmt::join(node_tasks | std::views::transform([](const auto& tasks) {
                        return fmt::format("{}", fmt::join(tasks, ","));
                      }),
                      ";"));

  std::unique_ptr<char, decltype(&free)> ppn(nullptr, &free);
  char* raw_ppn = nullptr;
  rc = PMIx_generate_ppn(ppn_str.c_str(), &raw_ppn);
  ppn.reset(raw_ppn);
  if (rc != PMIX_SUCCESS) {
    CRANE_ERROR("Error: PMIx_generate_ppn. {}", PMIx_Error_string(rc));
    for (auto& info : info_list) PMIX_INFO_DESTRUCT(&info);
    return false;
  }
  info_list.emplace_back(InfoLoad_(PMIX_PROC_MAP, ppn, PMIX_STRING));
  info_list.emplace_back(InfoLoad_(PMIX_ANL_MAP, ppn, PMIX_STRING));

  std::string ranks_str = absl::StrJoin(local_ranks, ",");
  if (local_ranks.empty()) {
    CRANE_ERROR("No local PMIx ranks assigned on node {}",
                m_pmix_job_info_.hostname);
    for (auto& info : info_list) PMIX_INFO_DESTRUCT(&info);
    return false;
  }
  // PMIX_LOCAL_PEERS: global namespace ranks of processes sharing this node.
  // PMIX_LOCALLDR: global rank of the local leader (lowest-numbered on node).
  info_list.emplace_back(InfoLoad_(PMIX_LOCAL_PEERS, ranks_str, PMIX_STRING));
  info_list.emplace_back(
      InfoLoad_(PMIX_LOCALLDR, my_node_start_rank, PMIX_UINT32));

  pmix_info_t* ns_info;
  PMIX_INFO_CREATE(ns_info, info_list.size());
  for (size_t i = 0; i < info_list.size(); i++) {
    ns_info[i] = info_list[i];
  }

  {
    absl::BlockingCounter bc(1);
    rc = PMIx_server_register_nspace(
        m_pmix_job_info_.nspace.c_str(),
        static_cast<int>(m_pmix_job_info_.node_tasks), ns_info,
        info_list.size(), OpCbWrapper, &bc);
    PMIX_INFO_FREE(ns_info, info_list.size());
    if (rc != PMIX_SUCCESS) {
      CRANE_ERROR("Error: PMIx_server_register_nspace. {}",
                  PMIx_Error_string(rc));
      return false;
    }
    bc.Wait();
  }

  {
    absl::BlockingCounter bc(1);
    rc = PMIx_server_setup_local_support(m_pmix_job_info_.nspace.c_str(),
                                         nullptr, 0, OpCbWrapper, &bc);
    if (rc != PMIX_SUCCESS) {
      CRANE_ERROR("Setup local support failed: {}", PMIx_Error_string(rc));
      return false;
    }
    bc.Wait();
  }

  // Register only the local clients (ranks whose task_map entry == this node).
  // Use task_map as the authoritative source for which global ranks belong
  // here.
  pmix_proc_t proc;
  PMIX_LOAD_NSPACE(proc.nspace, m_pmix_job_info_.nspace.c_str());
  for (uint32_t global_rank = 0; global_rank < m_pmix_job_info_.task_num;
       ++global_rank) {
    if (m_pmix_job_info_.task_map[global_rank] != m_pmix_job_info_.node_id)
      continue;
    proc.rank = global_rank;
    {
      absl::BlockingCounter bc(1);
      rc = PMIx_server_register_client(&proc, m_pmix_job_info_.uid,
                                       m_pmix_job_info_.gid, nullptr,
                                       OpCbWrapper, &bc);
      if (rc != PMIX_SUCCESS) {
        CRANE_ERROR(
            "Pmix register_client failed for global rank {} with error {}",
            global_rank, PMIx_Error_string(rc));
        return false;
      }
      bc.Wait();
    }
  }

  return true;
}

template <typename T>
pmix_info_t PmixServer::InfoLoad_(const std::string& key, const T& val,
                                  pmix_data_type_t data_type) {
  pmix_info_t info;

  if constexpr (std::is_same_v<T, std::string>) {
    PMIX_INFO_LOAD(&info, key.c_str(), val.c_str(), data_type);
  } else if constexpr (std::is_same_v<T,
                                      std::unique_ptr<char, decltype(&free)>>) {
    PMIX_INFO_LOAD(&info, key.c_str(), val.get(), data_type);
  } else {
    T local_val = val;
    PMIX_INFO_LOAD(&info, key.c_str(), &local_val, data_type);
  }

  return info;
}
#endif

}  // namespace pmix
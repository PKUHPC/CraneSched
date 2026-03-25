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

#include "PmixConn/PmixGrpcClient.h"
#include "PmixConn/PmixGrpcServer.h"
#include "PmixColl/PmixColl.h"

#ifdef HAVE_UCX
#include "PmixConn/PmixUcxClient.h"
#include "PmixConn/PmixUcxServer.h"
#endif

#include "absl/strings/str_split.h"
#include "absl/synchronization/blocking_counter.h"
#include "crane/OS.h"
#include "crane/String.h"
#include "fmt/printf.h"

namespace pmix {

#ifdef HAVE_PMIX
extern "C" {

  static int ClientConnectedCb(const pmix_proc_t *proc, void *server_object,
                               pmix_op_cbfunc_t cbfunc, void *cbdata) {
    /* we don't do anything by now */
    CRANE_DEBUG("ClientConnected is called");
    if (nullptr != cbfunc) {
     cbfunc(PMIX_SUCCESS, cbdata);
    }
    return PMIX_SUCCESS;
  }

  static pmix_status_t ClientFinalizedCb(const pmix_proc_t *proc,
                                         void *server_object,
                                         pmix_op_cbfunc_t cbfunc, void *cbdata) {
    CRANE_DEBUG("ClientFinalized is called");
     /* don't do anything by now */
    if (nullptr != cbfunc) {
     cbfunc(PMIX_SUCCESS, cbdata);
    }
    return PMIX_SUCCESS;
  }

  static pmix_status_t AbortFn(const pmix_proc_t *pmix_proc,
                                 void *server_object, int status,
                                 const char msg[], pmix_proc_t pmix_procs[],
                                 size_t nprocs, pmix_op_cbfunc_t cbfunc,
                                 void *cbdata) {
    CRANE_DEBUG("abort_fn called: status = {}, msg = {}\n", status, msg);

    g_pmix_server->GetCranedClient()->TerminateTasks();

    if (nullptr != cbfunc) {
      cbfunc(PMIX_SUCCESS, cbdata);
    }

    return PMIX_SUCCESS;
  }

  static pmix_status_t FencenbFn(const pmix_proc_t procs_v2[], size_t nprocs,
                                   const pmix_info_t info[], size_t ninfo,
                                   char *data, size_t ndata,
                                   pmix_modex_cbfunc_t cbfunc, void *cbdata) {
    CRANE_DEBUG(" FencenbFn is called");
    /* pass the provided data back to each participating proc */

    std::vector<pmix_proc_t> procs;
    procs.reserve(nprocs);
    bool collect = false;

   for (size_t i = 0; i< nprocs; i++) {
     procs.emplace_back(procs_v2[i]);
    }

    if (info != nullptr) {
     for (size_t i = 0; i < ninfo; i++) {
       if (0 == strncmp(info[i].key, PMIX_COLLECT_DATA, PMIX_MAX_KEYLEN)) {
          collect = true;
          break;
       }
     }
   }

   if (data == nullptr && ndata > 0) {
    CRANE_ERROR("Fence called with non-zero ndata but null data pointer");
    cbfunc(PMIX_ERROR, nullptr, 0, cbdata, nullptr, nullptr);
    return PMIX_ERROR;
   }

    CollType type = StrToCollType(g_pmix_server->GetFenceType());

    if (type == CollType::FENCE_MAX) {
      type = CollType::FENCE_TREE;
      if (collect && ndata > 0)
        type = CollType::FENCE_RING;
    }

  auto coll = g_pmix_server->GetPmixState()->PmixStateCollGet(type, procs);

  if (coll == nullptr) {
    CRANE_ERROR("Failed to get collective object for fence");
    cbfunc(PMIX_ERROR, nullptr, 0, cbdata, nullptr, nullptr);
    return PMIX_ERROR;
  }

  if (!coll->PmixCollContribLocal(std::string(data, ndata), cbfunc, cbdata)) {
    CRANE_ERROR("Failed to contribute data to collective for fence");
    cbfunc(PMIX_ERROR, nullptr, 0, cbdata, nullptr, nullptr);
    return PMIX_ERROR;
  }

  return PMIX_SUCCESS;
}

  static pmix_status_t DmodexFn(const pmix_proc_t *proc,
                                  const pmix_info_t info[], size_t ninfo,
                                  pmix_modex_cbfunc_t cbfunc, void *cbdata) {

    CRANE_DEBUG("dmodex func called");
    auto rc = g_pmix_server->GetDmodexReqManager()->PmixDModexGet(proc->nspace, proc->rank, cbfunc, cbdata);

    if (!rc) {
      CRANE_ERROR("Failed to get dmodex data for proc [{}:{}]", proc->nspace, proc->rank);
      cbfunc(PMIX_ERROR, nullptr, 0, cbdata, nullptr, nullptr);
      return PMIX_ERROR;
    }

    return PMIX_SUCCESS;
  }

  static pmix_status_t JobControl(const pmix_proc_t *proct,
                                    const pmix_proc_t targets[], size_t ntargets,
                                    const pmix_info_t directives[], size_t ndirs,
                                    pmix_info_cbfunc_t cbfunc, void *cbdata) {
    CRANE_DEBUG("JobControl is called");
    return PMIX_ERR_NOT_SUPPORTED;
  }

  static pmix_status_t PublishFn(const pmix_proc_t *proc,
                                   const pmix_info_t info[], size_t ninfo,
                                   pmix_op_cbfunc_t cbfunc, void *cbdata) {
    return PMIX_ERR_NOT_SUPPORTED;
  }

  static pmix_status_t LookupFn(const pmix_proc_t *proc, char **keys,
                                  const pmix_info_t info[], size_t ninfo,
                                  pmix_lookup_cbfunc_t cbfunc, void *cbdata) {
    return PMIX_ERR_NOT_SUPPORTED;
  }

  static pmix_status_t UnpublishFn(const pmix_proc_t *proc, char **keys,
                                     const pmix_info_t info[], size_t ninfo,
                                     pmix_op_cbfunc_t cbfunc, void *cbdata) {
    return PMIX_ERR_NOT_SUPPORTED;
  }

  static pmix_status_t SpawnFn(const pmix_proc_t *proc,
                                 const pmix_info_t job_info[], size_t ninfo,
                                 const pmix_app_t apps[], size_t napps,
                                 pmix_spawn_cbfunc_t cbfunc, void *cbdata) {
    return PMIX_ERR_NOT_SUPPORTED;
  }

  static pmix_status_t ConnectFn(const pmix_proc_t procs[], size_t nprocs,
                                   const pmix_info_t info[], size_t ninfo,
                                   pmix_op_cbfunc_t cbfunc, void *cbdata) {
    return PMIX_ERR_NOT_SUPPORTED;
  }

  static pmix_status_t DisconnectFn(const pmix_proc_t procs[], size_t nprocs,
                                      const pmix_info_t info[], size_t ninfo,
                                      pmix_op_cbfunc_t cbfunc, void *cbdata) {
    return PMIX_ERR_NOT_SUPPORTED;
  }

pmix_server_module_t g_k_crane_pmix_cb = {
    .client_connected = ClientConnectedCb,
    .client_finalized = ClientFinalizedCb,
    .abort = AbortFn,
    .fence_nb = FencenbFn,
    .direct_modex = DmodexFn,
    .publish = PublishFn,
    .lookup = LookupFn,
    .unpublish = UnpublishFn,
    .spawn = SpawnFn,
    .connect = ConnectFn,
    .disconnect = DisconnectFn,
    .job_control = JobControl,
};

static void AppCb(pmix_status_t status, pmix_info_t info [[maybe_unused]][],
           size_t ninfo [[maybe_unused]], void* provided_cbdata,
           pmix_op_cbfunc_t cbfunc [[maybe_unused]],
           void* cbdata [[maybe_unused]]) {
  auto* bc = reinterpret_cast<absl::BlockingCounter*>(provided_cbdata);
  bc->DecrementCount();
  CRANE_DEBUG("app callback is called with status={}: {}", status,
              PMIx_Error_string(status));
}

static void OpCb(pmix_status_t status, void* cbdata) {
  auto* bc = reinterpret_cast<absl::BlockingCounter*>(cbdata);
  bc->DecrementCount();
  CRANE_DEBUG("op callback is called with status={}: {}", status,
              PMIx_Error_string(status));
}

static void ErrHandlerRegCb(pmix_status_t status, size_t errhandler_ref,
                                  void *cbdata) {
  CRANE_DEBUG(
      "Errhandler registration callback is called with status={}, "
      "ref={}",
      status, static_cast<int>(errhandler_ref));
}

void ErrHandler_(size_t evhdlr_registration_id,
                        pmix_status_t status,
                        const pmix_proc_t *source,
                        pmix_info_t info[], size_t ninfo,
                        pmix_info_t *results, size_t nresults,
                        pmix_event_notification_cbfunc_fn_t cbfunc,
                        void *cbdata) {
  /* TODO: do something more sophisticated here */
  /* FIXME: use proper specificator for nranges */
  CRANE_ERROR("Error handler invoked: status = {}, source = [{}:{}]",
              status, source->nspace, source->rank);
  g_pmix_server->GetCranedClient()->TerminateTasks();
}

}
#endif

PmixServer::~PmixServer() {
#ifdef HAVE_PMIX
  m_cq_closed_ = true;
  if (m_uvw_thread_.joinable()) 
    m_uvw_thread_.join();

  util::os::DeleteFolders(m_pmix_job_info_.server_tmpdir);

  /* deregister the errhandler */
//  PMIx_Deregister_event_handler(0, OpCb, NULL);

  int rc = PMIx_server_finalize();
  if (rc != PMIX_SUCCESS)
    CRANE_ERROR("Failed to finalize PMIx server: {}", PMIx_Error_string(rc));

  m_pmix_client_.reset();
  m_pmix_async_server_.reset();
  m_craned_client_.reset();
  m_dmodex_mgr_.reset();
  m_pmix_state_.reset();

  CRANE_TRACE("[Step#{}.{}] Finalize PmixServer.", m_pmix_job_info_.job_id, m_pmix_job_info_.step_id);
#endif
}

bool PmixServer::Init(const Config& config, const crane::grpc::StepToD& step) {
#ifdef HAVE_PMIX
  CRANE_TRACE("[Step#{}.{}] Initializing PmixServer...", step.job_id(), step.step_id());
  m_uvw_loop_ = uvw::loop::create();

  if (!InfoSet_(config, step)) return false;

  if (!PmixInit_()) return false;

  m_dmodex_mgr_ = std::make_unique<PmixDModexReqManager>(m_pmix_job_info_);
  m_pmix_state_ = std::make_unique<PmixState>(m_pmix_job_info_);

  if (!ConnInit_(config)) return false;

  CRANE_TRACE("[Step#{}.{}] PmixServer conn initialized.", step.job_id(), step.step_id());

  m_cleanup_timer_handle_ = m_uvw_loop_->resource<uvw::timer_handle>();

  m_cleanup_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle&) {
         m_dmodex_mgr_->CleanupTimeoutRequests();
      }
  );

  m_cleanup_timer_handle_->start(std::chrono::seconds(m_timeout_), std::chrono::seconds(m_timeout_));

  m_uvw_thread_ = std::thread([this] {
    util::SetCurrentThreadName("PmixLoopThread_");

    auto idle_handle = m_uvw_loop_->resource<uvw::idle_handle>();
    idle_handle->on<uvw::idle_event>(
        [this](const uvw::idle_event &, uvw::idle_handle &h) {
          if (m_cq_closed_) {
            h.parent().walk([](auto &&h) { h.close(); });
            h.parent().stop();
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
        });

    if (idle_handle->start() != 0) {
      CRANE_ERROR("Failed to start the idle event in CranedKeeper loop.");
    }

    m_uvw_loop_->run();
  });

  if (!JobSet_()) return false;

  CRANE_INFO("Launch the PMIx server, dir: {}, version {}.{}.{}",
              m_pmix_job_info_.server_tmpdir, PMIX_VERSION_MAJOR, PMIX_VERSION_MINOR,
              PMIX_VERSION_RELEASE);

  return true;
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
    std::snprintf(key, sizeof(key), "%08x%08x-%08x%08x", m_pmix_job_info_.job_id, 0, m_pmix_job_info_.step_id, 0);
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
bool PmixServer::InfoSet_(const Config& config, const crane::grpc::StepToD& step) {
  m_pmix_job_info_.uid = step.uid();
  m_pmix_job_info_.gid = step.gid()[0];
  m_pmix_job_info_.job_id = step.job_id();
  m_pmix_job_info_.step_id = step.step_id();
  m_pmix_job_info_.nspace = fmt::format("crane.pmix.{}.{}", m_pmix_job_info_.job_id, m_pmix_job_info_.step_id);

  m_pmix_job_info_.node_num = step.node_num();
  // Use the actual total task count, NOT ntasks_per_node * node_num,
  // because tasks may not be evenly distributed across nodes.
  m_pmix_job_info_.task_num = step.task_node_list().size();
  CRANE_TRACE("task_num: {}", m_pmix_job_info_.task_num);
  std::string hostname;
  hostname.resize(256);  // Resize to ensure enough space for the hostname
  if (gethostname(hostname.data(), hostname.size()) == 0) {
    hostname.resize(
        strlen(hostname.c_str()));  // Resize to actual length of hostname
  } else {
    CRANE_ERROR("Failed to get hostname");
    return false;
  }
  m_pmix_job_info_.hostname = hostname;
  m_pmix_job_info_.node_list = std::vector<std::string>(step.nodelist().begin(), step.nodelist().end());
  m_pmix_job_info_.node_list_str = absl::StrJoin(m_pmix_job_info_.node_list, ",");

  auto it = std::ranges::find(m_pmix_job_info_.node_list, m_pmix_job_info_.hostname);
  if (it == m_pmix_job_info_.node_list.end()) {
    CRANE_ERROR(
        "[Step#{}.{}] Current hostname '{}' is not in the node list [{}] "
        "assigned to this step.",
        m_pmix_job_info_.job_id, m_pmix_job_info_.step_id,
        m_pmix_job_info_.hostname, m_pmix_job_info_.node_list_str);
    return false;
  }

  m_pmix_job_info_.node_id = std::ranges::distance(m_pmix_job_info_.node_list.begin(), it);

  m_pmix_job_info_.peer_node_list = std::vector(m_pmix_job_info_.node_list);
  std::erase(m_pmix_job_info_.peer_node_list, m_pmix_job_info_.hostname);

  // Build task_map: task_map[rank] = node_id (block order).
  // tasks_per_node_list[i] is the number of tasks on nodelist[i].
  m_pmix_job_info_.task_map.assign(step.task_node_list().begin(), step.task_node_list().end());

  // ntasks_per_node: the actual number of tasks on THIS specific node.
  for (const auto& node_id : step.task_node_list()) {
    if (node_id == m_pmix_job_info_.node_id)
      m_pmix_job_info_.ntasks_per_node++;
  }

  for (uint32_t index = 0; index < step.task_node_list_size(); index++) {
    CRANE_TRACE("task_id: {}, node_id: {}", index, step.task_node_list(index));
  }

  if (step.env().contains("CRANE_PMIX_TIMEOUT")) {
    auto timeout_str = step.env().at("CRANE_PMIX_TIMEOUT");
    if (timeout_str != "") {
      try {
        m_timeout_ = std::stoul(timeout_str);
      } catch (const std::exception& e) {
        CRANE_WARN("Failed to parse {} with error {}, use default timeout {}s",
                  timeout_str, e.what(), m_timeout_);
      }
    }
  }


  m_pmix_job_info_.server_tmpdir = fmt::format("{}pmix.crane.{}/pmix.{}.{}", config.CraneBaseDir, m_pmix_job_info_.hostname, m_pmix_job_info_.job_id, m_pmix_job_info_.step_id);

  if (step.env().contains("PMIXP_PMIXLIB_TMPDIR")) {
    // pmixlib_tmpdir is left for the user to configure, as the directory must be accessible to the user. 
    // By default, it is unset and the PMIx default directory will be used.
    std::string pmixlib_tmpdir = step.env().at("PMIXP_PMIXLIB_TMPDIR");
    if (!pmixlib_tmpdir.empty()) {
      m_pmix_job_info_.cli_tmpdir_base = fmt::format("{}pmix.crane.cli", pmixlib_tmpdir);
      m_pmix_job_info_.cli_tmpdir = fmt::format("{}/spmix_appdir_{}.{}.{}", m_pmix_job_info_.cli_tmpdir_base, m_pmix_job_info_.uid, m_pmix_job_info_.job_id, m_pmix_job_info_.step_id);
    }
  }

  if (step.env().contains("CRANE_PMIX_FENCE")) 
    m_pmix_job_info_.fence_type = step.env().at("CRANE_PMIX_FENCE");

  if (step.env().contains("CRANE_PMIX_DIRECT_CONN_UCX"))
    m_pmix_job_info_.pmix_direct_conn_ucx = step.env().at("CRANE_PMIX_DIRECT_CONN_UCX");

  return true;
}

bool PmixServer::ConnInit_(const Config& config) {
  m_craned_client_ = std::make_unique<CranedClient>(m_pmix_job_info_);
  m_craned_client_->InitChannelAndStub(config.CranedUnixSocketPath);

  if (m_pmix_job_info_.pmix_direct_conn_ucx == "true") {
  #ifdef HAVE_UCX
    m_pmix_client_ = std::make_unique<PmixUcxClient>(m_pmix_job_info_.node_num);
    m_pmix_async_server_ = std::make_unique<PmixUcxServer>(m_dmodex_mgr_.get(), m_pmix_state_.get(), m_craned_client_.get());
    CRANE_TRACE("Using UCX for PMIx communication as CranePmixDirectConnUcx is set to true.");
  #else
    CRANE_ERROR("UCX support is not enabled in this build, cannot use UCX for PMIx communication.");
    return false;
  #endif
  } else {
    m_pmix_client_ = std::make_unique<PmixGrpcClient>(m_pmix_job_info_.node_num);
    m_pmix_async_server_ = std::make_unique<PmixGrpcServer>(m_dmodex_mgr_.get(), m_pmix_state_.get(), m_craned_client_.get());
    CRANE_TRACE("Using gRPC for PMIx communication as CranePmixDirectConnUcx is not set to true.");
  }

  // #ifdef HAVE_UCX
  //   m_pmix_client_ = std::make_unique<PmixUcxClient>(m_pmix_job_info_.node_num);
  //   m_pmix_async_server_ = std::make_unique<PmixUcxServer>(m_dmodex_mgr_.get(), m_pmix_state_.get(), m_craned_client_.get());
  //   CRANE_TRACE("Using UCX for PMIx communication as CranePmixDirectConnUcx is set to true.");
  // #else
  //   CRANE_ERROR("UCX support is not enabled in this build, cannot use UCX for PMIx communication.");
  //   return false;
  // #endif

  if (!m_pmix_async_server_->Init(config))
    return false;

  // must ensure that both sides have received [the message] and are ready.
  if (!m_pmix_client_->WaitAllStubReady()) {
    CRANE_ERROR("Not all PMIx stubs are ready after waiting for 5 seconds, node num: {}, channel count: {}",
                m_pmix_job_info_.node_num, m_pmix_client_->GetChannelCount());
    return false;
  }

  return true;
}

bool PmixServer::PmixInit_() const {
  if (!util::os::CreateFolders(m_pmix_job_info_.server_tmpdir))
    return false;

  if (!m_pmix_job_info_.cli_tmpdir.empty()) {
    if (!util::os::CreateFolders(m_pmix_job_info_.cli_tmpdir))
    return false;
  }

  pmix_status_t rc;

  pmix_info_t* server_info;
  int server_info_size = 1;
  if (PMIX_VERSION_MAJOR < 5) server_info_size = 2;

  PMIX_INFO_CREATE(server_info, server_info_size);
  if (PMIX_VERSION_MAJOR < 5)
    PMIX_INFO_LOAD(&server_info[0], PMIX_USERID, &m_pmix_job_info_.uid, PMIX_UINT32);

  PMIX_INFO_LOAD(&server_info[server_info_size - 1], PMIX_SERVER_TMPDIR,
                 m_pmix_job_info_.server_tmpdir.c_str(), PMIX_STRING);
  rc = PMIx_server_init(&g_k_crane_pmix_cb, server_info, server_info_size);
  PMIX_INFO_DESTRUCT(server_info);
  if (PMIX_SUCCESS != rc) {
    CRANE_ERROR("Pmix Server Init failed with error {}", PMIx_Error_string(rc));
    return false;
  }

  PMIx_Register_event_handler(NULL, 0, NULL, 0, ErrHandler_,
                                    ErrHandlerRegCb, NULL);

  return true;
}

bool PmixServer::JobSet_() {
    pmix_status_t rc;

  {
    absl::BlockingCounter bc(1);
    rc = PMIx_server_setup_application(m_pmix_job_info_.nspace.c_str(), nullptr, 0, AppCb,
                                       &bc);
    if (PMIX_SUCCESS != rc) {
      CRANE_ERROR("Failed to setup application: {}", PMIx_Error_string(rc));
      return false;
    }
    bc.Wait();
  }

  std::vector<pmix_info_t> info_list;

  info_list.emplace_back(InfoLoad_(PMIX_SPAWNED, false, PMIX_BOOL));

  if (!m_pmix_job_info_.cli_tmpdir.empty()) {
    info_list.emplace_back(InfoLoad_(PMIX_TMPDIR, m_pmix_job_info_.cli_tmpdir_base, PMIX_STRING));
    info_list.emplace_back(InfoLoad_(PMIX_NSDIR, m_pmix_job_info_.cli_tmpdir, PMIX_STRING));
  }
  info_list.emplace_back(InfoLoad_(PMIX_TDIR_RMCLEAN, true, PMIX_BOOL));

  info_list.emplace_back(InfoLoad_(PMIX_JOBID, fmt::format("{}", m_pmix_job_info_.job_id), PMIX_STRING));
  info_list.emplace_back(InfoLoad_(PMIX_NODEID, m_pmix_job_info_.node_id, PMIX_UINT32));

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
      local_ranks.emplace_back(local_rank);
    }

    PMIX_INFO_LOAD(&proc_data_arr[7], PMIX_HOSTNAME, m_pmix_job_info_.hostname.c_str(),
                   PMIX_STRING);
    PMIX_INFO_LOAD(&proc_data_arr[8], PMIX_NODEID, &m_pmix_job_info_.node_id, PMIX_UINT32);

    pmix_info_t info;
    PMIX_INFO_LOAD(&info, PMIX_PROC_DATA, proc_data, PMIX_DATA_ARRAY);
    info_list.emplace_back(info);
    PMIX_DATA_ARRAY_DESTRUCT(proc_data);
  }

  // Job Size
  info_list.emplace_back(InfoLoad_(PMIX_UNIV_SIZE, m_pmix_job_info_.task_num, PMIX_UINT32));
  info_list.emplace_back(InfoLoad_(PMIX_JOB_SIZE, m_pmix_job_info_.task_num, PMIX_UINT32));
  info_list.emplace_back(
      InfoLoad_(PMIX_LOCAL_SIZE, m_pmix_job_info_.ntasks_per_node, PMIX_UINT32));
  info_list.emplace_back(InfoLoad_(PMIX_NODE_SIZE, m_pmix_job_info_.node_num, PMIX_UINT32));
  info_list.emplace_back(InfoLoad_(PMIX_MAX_PROCS, m_pmix_job_info_.task_num, PMIX_UINT32));

  // TODO：_set_topology

  if (PMIX_VERSION_MAJOR >= 5)
    info_list.emplace_back(InfoLoad_(PMIX_USERID, m_pmix_job_info_.uid, PMIX_UINT32));

  // node_list node1,node2,node3
  std::unique_ptr<char, decltype(&free)> regex(nullptr, &free);
  char* raw_regex;
  rc = PMIx_generate_regex(m_pmix_job_info_.node_list_str.c_str(), &raw_regex);
  regex.reset(raw_regex);
  if (rc != PMIX_SUCCESS) {
    CRANE_ERROR("Error: PMIx_generate_regex. {}", PMIx_Error_string(rc));
    return false;
  }
  info_list.emplace_back(InfoLoad_(PMIX_NODE_MAP, regex, PMIX_STRING));

  // proc_map "0,1,2;3,4" (per-node rank lists, separated by ';')
  // Use task_map (block-ordered) to build the correct per-node rank lists,
  // handling uneven distribution where nodes may have different task counts.
  std::ostringstream ppn_oss;
  uint32_t rank = 0;
  for (uint32_t node = 0; node < m_pmix_job_info_.node_num; ++node) {
    bool first = true;
    while (rank < m_pmix_job_info_.task_num &&
             m_pmix_job_info_.task_map[rank] == node) {
        if (!first) ppn_oss << ",";
        ppn_oss << rank;
        first = false;
        ++rank;
    }
    if (node < m_pmix_job_info_.node_num - 1) ppn_oss << ";";
  }
  std::string ppn_str = ppn_oss.str();

  std::unique_ptr<char, decltype(&free)> ppn(nullptr, &free);
  char* raw_ppn;
  rc = PMIx_generate_ppn(ppn_str.c_str(), &raw_ppn);
  ppn.reset(raw_ppn);
  if (rc != PMIX_SUCCESS) {
    CRANE_ERROR("Error: PMIx_generate_ppn. {}", PMIx_Error_string(rc));
    return false;
  }
  info_list.emplace_back(InfoLoad_(PMIX_PROC_MAP, ppn, PMIX_STRING));
  // info_list.emplace_back(InfoLoad_(PMIX_ANL_MAP, ppn, PMIX_STRING));

  std::string ranks_str = absl::StrJoin(local_ranks, ",");
  // Identifies the set of processes of the same task on the same physical node.
  info_list.emplace_back(InfoLoad_(PMIX_LOCAL_PEERS, ranks_str, PMIX_STRING));
  info_list.emplace_back(
      InfoLoad_(PMIX_LOCALLDR, local_ranks.front(), PMIX_UINT32));

  pmix_info_t* ns_info;
  PMIX_INFO_CREATE(ns_info, info_list.size());
  for (size_t i = 0; i < info_list.size(); i++) {
    ns_info[i] = info_list[i];
  }

  {
    absl::BlockingCounter bc(1);
    rc = PMIx_server_register_nspace(m_pmix_job_info_.nspace.c_str(),
                                     static_cast<int>(m_pmix_job_info_.ntasks_per_node),
                                     ns_info, info_list.size(), OpCb, &bc);
    PMIX_INFO_DESTRUCT(ns_info);
    if (rc != PMIX_SUCCESS) {
      CRANE_ERROR("Error: PMIx_server_register_nspace. {}",
                  PMIx_Error_string(rc));
      return false;
    }
    bc.Wait();
  }

  {
    absl::BlockingCounter bc(1);
    if (PMIX_SUCCESS != PMIx_server_setup_local_support(
                            m_pmix_job_info_.nspace.c_str(), nullptr, 0, OpCb, &bc)) {
      CRANE_ERROR("Setup local support failed: {}", PMIx_Error_string(rc));
      return false;
    }
    bc.Wait();
  }

  // Register only the local clients (ranks whose task_map entry == this node).
  // Use task_map as the authoritative source for which global ranks belong here.
  pmix_proc_t proc;
  PMIX_LOAD_NSPACE(proc.nspace, m_pmix_job_info_.nspace.c_str());
  for (uint32_t global_rank = 0; global_rank < m_pmix_job_info_.task_num;
       ++global_rank) {
    if (m_pmix_job_info_.task_map[global_rank] != m_pmix_job_info_.node_id)
      continue;
    proc.rank = global_rank;
    {
      absl::BlockingCounter bc(1);
      if (PMIX_SUCCESS !=
          PMIx_server_register_client(&proc, m_pmix_job_info_.uid,
                                      m_pmix_job_info_.gid, nullptr, OpCb,
                                      &bc)) {
        CRANE_ERROR("Pmix register_client failed for global rank {} with error {}",
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
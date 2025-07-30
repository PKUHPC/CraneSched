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

#include "PmixCommon.h"
#include "PmixDModex.h"
#include "absl/strings/str_split.h"
#include "absl/synchronization/blocking_counter.h"
#include "crane/OS.h"
#include "fmt/printf.h"

namespace pmix {

namespace {

class PMIxServerModule {
  public:
   static int ClientConnectedCb(const pmix_proc_t *proc, void *server_object,
                               pmix_op_cbfunc_t cbfunc, void *cbdata) {
     /* we don't do anything by now */
     CRANE_DEBUG("ClientConnected is called");
     return PMIX_SUCCESS;
   }

   static void OpCb(pmix_status_t status, void *cbdata) {
     CRANE_DEBUG("op callback is called with status={}: {}", status,
            PMIx_Error_string(status));
   }

   static void ErrHandlerRegCb(pmix_status_t status, size_t errhandler_ref,
                                     void *cbdata) {
     CRANE_DEBUG(
         "Error handler registration callback is called with status={}, "
         "ref={}",
         status, static_cast<int>(errhandler_ref));
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

     // TODO: 从env中获取type，默认MAX  SLURM_PMIX_FENCE (mixed, tree, ring)
     CollType type = CollType::FENCE_MAX;

     if (type == CollType::FENCE_MAX) {
       type = CollType::FENCE_TREE;

       if (collect && ndata > 0)
         type = CollType::FENCE_RING;
     }

     auto coll = g_pmix_state->PmixStateCollGet(type, procs, nprocs);

     if (coll == nullptr) return PMIX_ERROR;

     if (!coll->PmixCollContribLocal(type, std::string(data, ndata), cbfunc, cbdata)) {
       cbfunc(PMIX_ERROR, nullptr, 0, cbdata, nullptr, nullptr);
       return PMIX_ERROR;
     }

     return PMIX_SUCCESS;
   }

   static pmix_status_t DmodexFn(const pmix_proc_t *proc,
                                  const pmix_info_t info[], size_t ninfo,
                                  pmix_modex_cbfunc_t cbfunc, void *cbdata) {

     auto rc = g_dmodex_req_manager->PmixDModexGet(proc->nspace, proc->rank, cbfunc, cbdata);

     if (!rc) return PMIX_ERROR;

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
 };

pmix_server_module_t g_k_crane_pmix_cb = {
    .client_connected = PMIxServerModule::ClientConnectedCb,
    .client_finalized = PMIxServerModule::ClientFinalizedCb,
    .abort = PMIxServerModule::AbortFn,
    .fence_nb = PMIxServerModule::FencenbFn,
    .direct_modex = PMIxServerModule::DmodexFn,
    .publish = PMIxServerModule::PublishFn,
    .lookup = PMIxServerModule::LookupFn,
    .unpublish = PMIxServerModule::UnpublishFn,
    .spawn = PMIxServerModule::SpawnFn,
    .connect = PMIxServerModule::ConnectFn,
    .disconnect = PMIxServerModule::DisconnectFn,
    .job_control = PMIxServerModule::JobControl,
};

void AppCb(pmix_status_t status, pmix_info_t info [[maybe_unused]][],
           size_t ninfo [[maybe_unused]], void* provided_cbdata,
           pmix_op_cbfunc_t cbfunc [[maybe_unused]],
           void* cbdata [[maybe_unused]]) {
  auto* bc = reinterpret_cast<absl::BlockingCounter*>(provided_cbdata);
  bc->DecrementCount();
  CRANE_DEBUG("app callback is called with status={}: {}", status,
              PMIx_Error_string(status));
}

void OpCb(pmix_status_t status, void* cbdata) {
  auto* bc = reinterpret_cast<absl::BlockingCounter*>(cbdata);
  bc->DecrementCount();
  CRANE_DEBUG("op callback is called with status={}: {}", status,
              PMIx_Error_string(status));
}
}  // namespace

PmixServer::~PmixServer() {
  if (!m_is_init_) return;

  util::os::DeleteFolders(m_server_tmpdir_);

  int rc = PMIx_server_finalize();
  if (rc != PMIX_SUCCESS)
    CRANE_ERROR("Failed to finalize PMIx server: {}", PMIx_Error_string(rc));

  g_dmodex_req_manager.reset();
  g_pmix_state.reset();
  m_craned_client_.reset();
  m_pmix_client_.reset();
  m_pmix_async_server_.reset();

  CRANE_TRACE("Task#{} Finalize PmixServer.", m_task_id_);
}

bool PmixServer::Init(
    const Config& config, const crane::grpc::TaskToD& task,
    const std::unordered_map<std::string, std::string>& env_map) {
  util::os::DeleteFolders(m_server_tmpdir_);
  m_server_tmpdir_ = fmt::format("{}pmix.crane", config.CraneBaseDir);
  if (!util::os::CreateFolders(m_server_tmpdir_)) {
    return false;
  }

  InfoSet_(task, env_map);

  pmix_status_t rc;

  pmix_info_t* server_info;
  int server_info_size = 1;
  if (PMIX_VERSION_MAJOR < 5) server_info_size = 2;

  PMIX_INFO_CREATE(server_info, server_info_size);
  if (PMIX_VERSION_MAJOR < 5)
    PMIX_INFO_LOAD(&server_info[0], PMIX_USERID, &m_uid_, PMIX_UINT32);

  PMIX_INFO_LOAD(&server_info[server_info_size - 1], PMIX_SERVER_TMPDIR,
                 m_server_tmpdir_.c_str(), PMIX_STRING);
  rc = PMIx_server_init(&g_k_crane_pmix_cb, server_info, server_info_size);
  PMIX_INFO_DESTRUCT(server_info);
  if (PMIX_SUCCESS != rc) {
    CRANE_ERROR("Pmix Server Init failed with error {}", PMIx_Error_string(rc));
    return false;
  }

  g_dmodex_req_manager = std::make_unique<PmixDModexReqManager>();
  g_pmix_state = std::make_unique<PmixState>();
  m_craned_client_ = std::make_unique<CranedClient>();
  m_craned_client_->InitChannelAndStub(config.CranedUnixSocketPath);

  if (task.node_num() > 1) {
    m_pmix_client_ = std::make_unique<PmixClient>();
    m_pmix_async_server_ = std::make_unique<PmixASyncServer>();
    if (!m_pmix_async_server_->Init(config))
      return false;
  }

  JobSet_();

  // TODO: wait for all PMIx clients to be ready ?

  CRANE_INFO("Crun Task #{} Launch the PMIx server, dir: {}, version {}.{}.{}",
              task.task_id(), m_server_tmpdir_, PMIX_VERSION_MAJOR, PMIX_VERSION_MINOR,
              PMIX_VERSION_RELEASE);

  return true;
}

std::optional<std::unordered_map<std::string, std::string>>
PmixServer::SetupFork(uint32_t rank) {
  char** client_env = nullptr;

  pmix_proc_t proc;
  PMIX_LOAD_NSPACE(proc.nspace, m_nspace_.c_str());
  proc.rank = m_ntasks_per_node_ * m_node_id_ + rank;
  pmix_status_t rc;

  if (PMIX_SUCCESS != PMIx_server_setup_fork(&proc, &client_env)) {
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

  env_map.emplace("SLURM_JOBID", m_task_id_);
  env_map.emplace("SLURM_NODELIST", m_node_list_str_);
  env_map.emplace("SLURM_STEP_ID", "0");

  if (!std::getenv("OMPI_MCA_orte_precondition_transports")) {
    char key[64];
    uint32_t id = std::strtoul(m_task_id_.c_str(), nullptr, 0);
    std::snprintf(key, sizeof(key), "%08x%08x-%08x%08x", id, 0, id, 0);
    env_map.emplace("OMPI_MCA_orte_precondition_transports", key);
  }

  return std::move(env_map);
}

bool PmixServer::JobSet_() {
    pmix_status_t rc;

  {
    absl::BlockingCounter bc(1);
    rc = PMIx_server_setup_application(m_nspace_.c_str(), nullptr, 0, AppCb,
                                       &bc);
    if (PMIX_SUCCESS != rc) {
      CRANE_ERROR("Failed to setup application: {}", PMIx_Error_string(rc));
      return false;
    }
    bc.Wait();
  }

  std::vector<pmix_info_t> info_list;

  info_list.emplace_back(InfoLoad_(PMIX_SPAWNED, false, PMIX_BOOL));

  // PMIX_TMPDIR cli_tmpdir_base
  // PMIX_NSDIR cli_tmpdir

  info_list.emplace_back(InfoLoad_(PMIX_TDIR_RMCLEAN, true, PMIX_BOOL));

  info_list.emplace_back(InfoLoad_(PMIX_JOBID, m_task_id_, PMIX_STRING));

  info_list.emplace_back(InfoLoad_(PMIX_NODEID, m_node_id_, PMIX_UINT32));

  std::list<uint32_t> local_ranks;

  for (uint32_t rank = 0; rank < m_task_num_; rank++) {
    pmix_data_array_t* proc_data;
    PMIX_DATA_ARRAY_CREATE(proc_data, 9, PMIX_INFO);
    auto* proc_data_arr = reinterpret_cast<pmix_info_t*>(proc_data->array);
    PMIX_INFO_LOAD(&proc_data_arr[0], PMIX_RANK, &rank, PMIX_PROC_RANK);

    int tmp = 0;
    PMIX_INFO_LOAD(&proc_data_arr[1], PMIX_APPNUM, &tmp, PMIX_INT);
    PMIX_INFO_LOAD(&proc_data_arr[2], PMIX_APPLDR, &tmp, PMIX_INT);

    PMIX_INFO_LOAD(&proc_data_arr[3], PMIX_GLOBAL_RANK, &rank, PMIX_UINT32);
    PMIX_INFO_LOAD(&proc_data_arr[4], PMIX_APP_RANK, &rank, PMIX_UINT32);

    /* this rank is local, store local info ab't it! */
    if (rank / m_ntasks_per_node_ == m_node_id_) {
      uint32_t local_rank = rank % m_ntasks_per_node_;
      PMIX_INFO_LOAD(&proc_data_arr[5], PMIX_NODE_RANK, &local_rank,
                     PMIX_UINT16);
      PMIX_INFO_LOAD(&proc_data_arr[6], PMIX_LOCAL_RANK, &local_rank,
                     PMIX_UINT16);
      local_ranks.emplace_back(local_rank);
    }

    PMIX_INFO_LOAD(&proc_data_arr[7], PMIX_HOSTNAME, m_hostname_.c_str(),
                   PMIX_STRING);
    PMIX_INFO_LOAD(&proc_data_arr[8], PMIX_NODEID, &m_node_id_, PMIX_UINT32);

    pmix_info_t info;
    PMIX_INFO_LOAD(&info, PMIX_PROC_DATA, proc_data, PMIX_DATA_ARRAY);
    info_list.emplace_back(info);
    PMIX_DATA_ARRAY_DESTRUCT(proc_data);
  }

  // Job Size
  info_list.emplace_back(InfoLoad_(PMIX_UNIV_SIZE, m_task_num_, PMIX_UINT32));
  info_list.emplace_back(InfoLoad_(PMIX_JOB_SIZE, m_task_num_, PMIX_UINT32));
  info_list.emplace_back(
      InfoLoad_(PMIX_LOCAL_SIZE, m_ntasks_per_node_, PMIX_UINT32));

  info_list.emplace_back(InfoLoad_(PMIX_NODE_SIZE, m_node_num_, PMIX_UINT32));
  info_list.emplace_back(InfoLoad_(PMIX_MAX_PROCS, m_task_num_, PMIX_UINT32));

  // TODO：_set_topology

  info_list.emplace_back(InfoLoad_(PMIX_USERID, m_uid_, PMIX_UINT32));

  // node_list node1,node2,node3
  std::unique_ptr<char, decltype(&free)> regex(nullptr, &free);
  char* raw_regex;
  rc = PMIx_generate_regex(m_node_list_str_.c_str(), &raw_regex);
  regex.reset(raw_regex);
  if (rc != PMIX_SUCCESS) {
    CRANE_ERROR("Error: PMIx_generate_regex. {}", PMIx_Error_string(rc));
    return false;
  }
  info_list.emplace_back(InfoLoad_(PMIX_NODE_MAP, regex, PMIX_STRING));

  // proc_map "0,1;2,3;4,5"
  std::ostringstream ppn_oss;
  for (uint32_t i = 0; i < m_node_num_; i++) {
    for (uint32_t j = 0; j < m_ntasks_per_node_; j++) {
      if (j > 0) ppn_oss << ",";
      ppn_oss << ((i * m_ntasks_per_node_) + j);
    }
    if (i < m_node_num_ - 1) {
      ppn_oss << ";";
    }
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
    rc = PMIx_server_register_nspace(m_nspace_.c_str(),
                                     static_cast<int>(m_ntasks_per_node_),
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
                            m_nspace_.c_str(), nullptr, 0, OpCb, &bc)) {
      CRANE_ERROR("Setup local support failed: {}", PMIx_Error_string(rc));
      return false;
    }
    bc.Wait();
  }

  pmix_proc_t proc;
  PMIX_LOAD_NSPACE(proc.nspace, m_nspace_.c_str());
  for (uint32_t rank = 0; rank < m_ntasks_per_node_; rank++) {
    uint32_t gloabl_rank = (m_ntasks_per_node_ * m_node_id_) + rank;
    proc.rank = gloabl_rank;
    {
      absl::BlockingCounter bc(1);
      if (PMIX_SUCCESS != PMIx_server_register_client(&proc, m_uid_, m_gid_,
                                                      nullptr, OpCb, &bc)) {
        CRANE_ERROR("Pmix Server fork setup failed with error {}",
                    PMIx_Error_string(rc));
        return false;
      }
      bc.Wait();
    }
  }
  m_is_init_ = true;

  return true;
}

void PmixServer::InfoSet_(
    const crane::grpc::TaskToD& task,
    const std::unordered_map<std::string, std::string>& env_map) {
  // job_set
  m_uid_ = task.uid();
  m_gid_ = task.gid();
  m_task_id_ = std::to_string(task.task_id());
  m_nspace_ = fmt::sprintf("crane.pmix.%d", task.task_id());

  m_ntasks_per_node_ = task.ntasks_per_node();
  m_node_num_ = task.node_num();
  m_task_num_ = m_ntasks_per_node_ * m_node_num_;

  std::string hostname;
  hostname.resize(256);  // Resize to ensure enough space for the hostname
  if (gethostname(hostname.data(), hostname.size()) == 0) {
    hostname.resize(
        strlen(hostname.c_str()));  // Resize to actual length of hostname
  } else {
    CRANE_ERROR("Failed to get hostname");
    hostname = "unknown";  // Set a default value if gethostname fails
  }
  m_hostname_ = hostname;
  m_node_list_str_ = env_map.at("CRANE_JOB_NODELIST");
  std::ranges::replace(m_node_list_str_, ';', ',');
  m_node_list_ = absl::StrSplit(m_node_list_str_, ',');

  auto it = std::ranges::find(m_node_list_, m_hostname_);
  if (it != m_node_list_.end())
    m_node_id_ = std::ranges::distance(m_node_list_.begin(), it);

  m_peer_node_list_ = std::vector(m_node_list_);
  std::erase(m_peer_node_list_, m_hostname_);

  size_t total_tasks = m_node_num_ * m_ntasks_per_node_;
  std::vector<uint32_t> task_map(total_tasks);

  for (size_t node = 0; node < m_node_num_; ++node) {
    std::fill_n(task_map.begin() + node * m_ntasks_per_node_, m_ntasks_per_node_, node);
  }

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

}  // namespace pmix
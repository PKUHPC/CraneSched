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

#include "absl/synchronization/blocking_counter.h"
#include "crane/OS.h"
#include "fmt/printf.h"

namespace pmix {

pmix_server_module_t PMIxServerModuleWrapper::CranePmixCb = {
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

namespace {
  void AppCb(pmix_status_t status, pmix_info_t info[], size_t ninfo,
    void *provided_cbdata, pmix_op_cbfunc_t cbfunc,
    void *cbdata) {
    auto *bc = reinterpret_cast<absl::BlockingCounter *>(provided_cbdata);
    bc->DecrementCount();
    CRANE_DEBUG("app callback is called with status=%d: %s\n", status,
    PMIx_Error_string(status));
  }

  void OpCb(pmix_status_t status, void *cbdata) {
    auto *bc = reinterpret_cast<absl::BlockingCounter *>(cbdata);
    bc->DecrementCount();
    CRANE_DEBUG("op callback is called with status=%d: %s\n", status,
    PMIx_Error_string(status));
  }
}


PmixServer::~PmixServer() {
  {
    absl::BlockingCounter bc(1);
    PMIx_server_deregister_nspace(m_nspace_.c_str(), OpCb, &bc);
    bc.Wait();
  }

  {
    absl::BlockingCounter bc(1);
    PMIx_Deregister_event_handler(0, OpCb, &bc);
    bc.Wait();
  }

  int rc = PMIx_server_finalize();
  if (rc != PMIX_SUCCESS)
    CRANE_ERROR("Failed to finalize PMIx server: {}", PMIx_Error_string(rc));

  if (!util::os::DeleteFile(m_server_tmpdir_))
    CRANE_ERROR("Failed to delete file {}", m_server_tmpdir_);
}

bool PmixServer::Init(const crane::grpc::TaskToD& task, const std::unordered_map<std::string, std::string>& env_map) {

  InfoSet_(task, env_map);

  pmix_status_t rc;

  pmix_info_t *server_info;
  PMIX_INFO_CREATE(server_info, 1);
  PMIX_INFO_LOAD(&server_info[0], PMIX_SERVER_TMPDIR, &m_server_tmpdir_, PMIX_STRING);

  rc = PMIx_server_init(PMIxServerModuleWrapper::Instance().GetModule(), server_info, 1);
  PMIX_INFO_DESTRUCT(server_info);
  if (PMIX_SUCCESS != rc) {
    CRANE_ERROR("Pmix Server Init failed with error {}", PMIx_Error_string(rc));
    return false;
  }

  {
    absl::BlockingCounter bc(1);
    if (PMIX_SUCCESS != PMIx_server_setup_application(m_nspace_.c_str(), nullptr, 0, AppCb, &bc)) {
      CRANE_ERROR("Failed to setup application: %d", PMIx_Error_string(rc));
      return false;
    }
    bc.Wait();
  }

  // TODO: optimize info load
  std::vector<pmix_info_t> info_list;
  pmix_info_t info;
  uint32_t task_id = task.task_id();
  PMIX_INFO_LOAD(&info, PMIX_JOBID, &task_id, PMIX_STRING);
  info_list.emplace_back(info);

  PMIX_INFO_LOAD(&info, PMIX_NODEID, 0, PMIX_UINT32);
  info_list.emplace_back(info);

  std::list<uint32_t> ranks;

  for (uint32_t rank = 0; rank < m_nprocs_; rank++) {
    pmix_data_array_t *proc_data;
    PMIX_DATA_ARRAY_CREATE(proc_data, 6, PMIX_INFO);
    auto *proc_data_arr = reinterpret_cast<pmix_info_t *>(proc_data->array);
    PMIX_INFO_LOAD(&proc_data_arr[0], PMIX_RANK, &rank, PMIX_PROC_RANK);

    PMIX_INFO_LOAD(&proc_data_arr[1], PMIX_GLOBAL_RANK, &rank, PMIX_UINT32);

    PMIX_INFO_LOAD(&proc_data_arr[2], PMIX_NODE_RANK, &rank, PMIX_UINT16);
    PMIX_INFO_LOAD(&proc_data_arr[3], PMIX_LOCAL_RANK, &rank, PMIX_UINT16);

    PMIX_INFO_LOAD(&proc_data_arr[4], PMIX_HOSTNAME, &m_hostname_, PMIX_STRING);
    PMIX_INFO_LOAD(&proc_data_arr[5], PMIX_NODEID, &m_node_id_, PMIX_UINT32);
    PMIX_INFO_LOAD(&info, PMIX_PROC_DATA, proc_data, PMIX_DATA_ARRAY);
    info_list.emplace_back(info);
    PMIX_DATA_ARRAY_DESTRUCT(proc_data);
    ranks.emplace_back(rank);
  }

  // Job Size
  PMIX_INFO_LOAD(&info, PMIX_UNIV_SIZE, &m_nprocs_, PMIX_UINT32);
  info_list.emplace_back(info);
  PMIX_INFO_LOAD(&info, PMIX_JOB_SIZE, &m_nprocs_, PMIX_UINT32);
  info_list.emplace_back(info);
  PMIX_INFO_LOAD(&info, PMIX_LOCAL_SIZE, &m_nprocs_, PMIX_UINT32);
  info_list.emplace_back(info);

  // Currently only supports a single node.
  uint32_t val32 = 1;
  PMIX_INFO_LOAD(&info, PMIX_NODE_SIZE, &val32, PMIX_UINT32);
  info_list.emplace_back(info);
  PMIX_INFO_LOAD(&info, PMIX_MAX_PROCS, &m_nprocs_, PMIX_UINT32);
  info_list.emplace_back(info);
  PMIX_INFO_LOAD(&info, PMIX_SPAWNED, 0, PMIX_UINT32);
  info_list.emplace_back(info);


  std::string ranks_str = absl::StrJoin(ranks, ",");
  // Identifies the set of processes of the same task on the same physical node.
  PMIX_INFO_LOAD(&info, PMIX_LOCAL_PEERS, ranks_str.c_str(), PMIX_STRING);
  info_list.emplace_back(info);

  // node_list
  std::unique_ptr<char, decltype(&free)> regex(nullptr, &free);
  char *raw_regex;
  rc = PMIx_generate_regex(m_node_list_.c_str(), &raw_regex);
  regex.reset(raw_regex);
  if (rc != PMIX_SUCCESS) {
    CRANE_ERROR("Error: PMIx_generate_regex. {}", PMIx_Error_string(rc));
    return false;
  }
  // node1,node2,node3
  PMIX_INFO_LOAD(&info, PMIX_NODE_MAP, regex.get(), PMIX_STRING);
  info_list.emplace_back(info);

  // proc_map
  std::unique_ptr<char, decltype(&free)> ppn(nullptr, &free);
  char *raw_ppn;
  rc = PMIx_generate_ppn(ranks_str.c_str(), &raw_ppn);
  ppn.reset(raw_ppn);
  if (rc != PMIX_SUCCESS) {
    CRANE_ERROR("Error: PMIx_generate_ppn. {}", PMIx_Error_string(rc));
    return false;
  }
  // rank0,rank1,rank2
  PMIX_INFO_LOAD(&info, PMIX_PROC_MAP, ppn.get(), PMIX_STRING);
  info_list.emplace_back(info);

  pmix_info_t *ns_info;
  PMIX_INFO_CREATE(ns_info, info_list.size());
  for (size_t i = 0; i < info_list.size(); i++) {
    PMIX_INFO_LOAD(&ns_info[i], info_list[i].key, &info_list[i].value, info_list[i].flags);
  }

  {
    absl::BlockingCounter bc(1);
    rc = PMIx_server_register_nspace(m_nspace_.c_str(), m_nprocs_, ns_info, info_list.size(), OpCb, &bc);
    PMIX_INFO_DESTRUCT(ns_info);
    if (rc != PMIX_SUCCESS) {
      CRANE_ERROR("Error: PMIx_server_register_nspace. {}", PMIx_Error_string(rc));
      return false;
    }
    bc.Wait();
  }

  {
    absl::BlockingCounter bc(1);
    if (PMIX_SUCCESS != PMIx_server_setup_local_support(m_nspace_.c_str(), nullptr, 0, OpCb, &bc)) {
      CRANE_ERROR("Setup local support failed: {}", PMIx_Error_string(rc));
      return false;
    }
    bc.Wait();
  }

  pmix_proc_t proc;
  PMIX_LOAD_NSPACE(proc.nspace, m_nspace_.c_str());
  for (uint32_t rank = 0; rank < m_nprocs_; rank++) {
    proc.rank = rank;
    {
      absl::BlockingCounter bc(1);
      if (PMIX_SUCCESS != PMIx_server_register_client(&proc, m_uid_, m_gid_, nullptr, OpCb, &bc)) {
        CRANE_ERROR("Pmix Server fork setup failed with error {}", PMIx_Error_string(rc));
        return false;
      }
      bc.Wait();
    }
  }

  return true;
}

std::optional<std::unordered_map<std::string, std::string>> PmixServer::SetupFork(uint32_t rank) {
  char **client_env = nullptr;

  pmix_proc_t proc;
  PMIX_LOAD_NSPACE(proc.nspace, m_nspace_.c_str());
  proc.rank = rank;
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
      env_map[std::move(key)] = std::move(value);
    }
  }

  return std::move(env_map);
}

void PmixServer::InfoSet_(const crane::grpc::TaskToD& task,  const std::unordered_map<std::string, std::string>& env_map) {
  // job_set
  m_uid_ = task.uid();
  m_gid_ = task.gid();
  m_nspace_ = fmt::sprintf("crane.pmix.%d", task.task_id());

  // TODO: modify /tmp to CraneBaseDir?
  m_server_tmpdir_ = fmt::sprintf("/tmp/pmix.%d", task.task_id());

  m_nprocs_ = task.ntasks_per_node();

  m_hostname_ = task.nodelist(0);
  m_node_id_ = 0;
  m_node_list_ = absl::StrJoin(task.nodelist(), ",");
}

}  // namespace pmix
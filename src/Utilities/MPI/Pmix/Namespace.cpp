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

#include "Collective/Collective.h"
#include "Server.h"
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
#ifdef HAVE_PMIX
namespace {
template <typename T>
pmix_info_t InfoLoad(const std::string& key, const T& value,
                     pmix_data_type_t data_type) {
  pmix_info_t info;
  if constexpr (std::is_same_v<T, std::string>) {
    PMIX_INFO_LOAD(&info, key.c_str(), value.c_str(), data_type);
  } else if constexpr (std::is_same_v<T,
                                      std::unique_ptr<char, decltype(&free)>>) {
    PMIX_INFO_LOAD(&info, key.c_str(), value.get(), data_type);
  } else {
    T local_value = value;
    PMIX_INFO_LOAD(&info, key.c_str(), &local_value, data_type);
  }
  return info;
}
}  // namespace

bool PmixServer::InfoSet_(const Config& config,
                          const crane::grpc::StepToD& step) {
  m_step_info_.uid = step.uid();
  if (step.gids().empty()) {
    CRANE_ERROR("[Step#{}.{}] GID list is empty, cannot initialize PMIx.",
                step.job_id(), step.step_id());
    return false;
  }
  m_step_info_.gid = step.gids()[0];
  m_step_info_.job_id = step.job_id();
  m_step_info_.step_id = step.step_id();
  m_step_info_.nspace = fmt::format("crane.pmix.{}.{}", m_step_info_.job_id,
                                    m_step_info_.step_id);

  m_step_info_.node_num = step.node_num();
  // Use the actual total task count, NOT node_tasks * node_num,
  // because tasks may not be evenly distributed across nodes.
  m_step_info_.task_num = step.task_node_list().size();
  m_step_info_.craned_id = config.CranedIdOfThisNode;
  m_step_info_.node_list =
      std::vector<std::string>(step.nodelist().begin(), step.nodelist().end());
  m_step_info_.node_list_str = absl::StrJoin(m_step_info_.node_list, ",");

  auto it = std::ranges::find(m_step_info_.node_list, m_step_info_.craned_id);
  if (it == m_step_info_.node_list.end()) {
    CRANE_ERROR(
        "[Step#{}.{}] Current CranedId '{}' is not in the node list [{}] "
        "assigned to this step.",
        m_step_info_.job_id, m_step_info_.step_id, m_step_info_.craned_id,
        m_step_info_.node_list_str);
    return false;
  }

  m_step_info_.node_id =
      std::ranges::distance(m_step_info_.node_list.begin(), it);

  m_step_info_.node_hostnames.clear();
  for (const auto& craned_id : m_step_info_.node_list) {
    const auto hostname = config.CranedIdToNodeHostname.find(craned_id);
    const auto address = config.CranedIdToNodeAddr.find(craned_id);
    if (hostname == config.CranedIdToNodeHostname.end() ||
        address == config.CranedIdToNodeAddr.end() ||
        hostname->second.empty() || address->second.empty()) {
      CRANE_ERROR("Missing PMIx node configuration for CranedId {}", craned_id);
      return false;
    }
    m_step_info_.node_hostnames.push_back(hostname->second);
  }

  m_step_info_.peer_node_list = std::vector(m_step_info_.node_list);
  std::erase(m_step_info_.peer_node_list, m_step_info_.craned_id);

  // Build task_map: task_map[rank] = node_id (block order).
  // tasks_per_node_list[i] is the number of tasks on nodelist[i].
  m_step_info_.task_map.assign(step.task_node_list().begin(),
                               step.task_node_list().end());

  // node_tasks: the actual number of tasks on THIS specific node.
  for (const auto& node_id : step.task_node_list()) {
    if (node_id == m_step_info_.node_id) m_step_info_.node_tasks++;
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

  m_step_info_.server_tmpdir =
      (std::filesystem::path(config.CraneBaseDir) /
       fmt::format("pmix.crane.{}", m_step_info_.craned_id) /
       fmt::format("pmix.{}.{}", m_step_info_.job_id, m_step_info_.step_id))
          .string();

  if (step.env().contains("PMIXP_PMIXLIB_TMPDIR")) {
    // pmixlib_tmpdir is left for the user to configure, as the directory must
    // be accessible to the user. By default, it is unset and the PMIx default
    // directory will be used.
    std::string pmixlib_tmpdir = step.env().at("PMIXP_PMIXLIB_TMPDIR");
    if (!pmixlib_tmpdir.empty()) {
      m_step_info_.cli_tmpdir_base =
          (std::filesystem::path(pmixlib_tmpdir) / "pmix.crane.cli").string();
      m_step_info_.cli_tmpdir =
          (std::filesystem::path(m_step_info_.cli_tmpdir_base) /
           fmt::format("spmix_appdir_{}.{}.{}", m_step_info_.uid,
                       m_step_info_.job_id, m_step_info_.step_id))
              .string();
    }
  }

  if (step.env().contains("CRANE_PMIX_FENCE"))
    m_step_info_.fence_type = step.env().at("CRANE_PMIX_FENCE");

  if (step.env().contains("CRANE_PMIX_DIRECT_CONN_UCX"))
    m_step_info_.pmix_direct_conn_ucx =
        step.env().at("CRANE_PMIX_DIRECT_CONN_UCX");

  return true;
}

bool PmixServer::JobSet_() {
  pmix_status_t rc;

  {
    absl::BlockingCounter bc(1);
    rc = PMIx_server_setup_application(m_step_info_.nspace.c_str(), nullptr, 0,
                                       PmixApplicationCallback, &bc);
    if (PMIX_SUCCESS != rc) {
      CRANE_ERROR("Failed to setup application: {}", PMIx_Error_string(rc));
      return false;
    }
    bc.Wait();
  }

  std::vector<pmix_info_t> info_list;

  info_list.emplace_back(InfoLoad(PMIX_SPAWNED, false, PMIX_BOOL));

  if (!m_step_info_.cli_tmpdir.empty()) {
    info_list.emplace_back(
        InfoLoad(PMIX_TMPDIR, m_step_info_.cli_tmpdir_base, PMIX_STRING));
    info_list.emplace_back(
        InfoLoad(PMIX_NSDIR, m_step_info_.cli_tmpdir, PMIX_STRING));
  }
  info_list.emplace_back(InfoLoad(PMIX_TDIR_RMCLEAN, true, PMIX_BOOL));

  info_list.emplace_back(InfoLoad(
      PMIX_JOBID, fmt::format("{}", m_step_info_.job_id), PMIX_STRING));
  info_list.emplace_back(
      InfoLoad(PMIX_NODEID, m_step_info_.node_id, PMIX_UINT32));

  // Precompute the start global rank of THIS node so we can compute local_rank.
  // task_map is in block order, so the first index where task_map[r] == node_id
  // is the start rank of this node.
  uint32_t my_node_start_rank = 0;
  for (uint32_t r = 0; r < m_step_info_.task_num; ++r) {
    if (m_step_info_.task_map[r] == m_step_info_.node_id) {
      my_node_start_rank = r;
      break;
    }
  }

  std::list<uint32_t> local_ranks;
  for (uint32_t rank = 0; rank < m_step_info_.task_num; rank++) {
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
    uint32_t rank_node_id = m_step_info_.task_map[rank];
    const std::string& rank_hostname =
        m_step_info_.node_hostnames[rank_node_id];

    /* this rank is local, store local info about it */
    if (rank_node_id == m_step_info_.node_id) {
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
      InfoLoad(PMIX_UNIV_SIZE, m_step_info_.task_num, PMIX_UINT32));
  info_list.emplace_back(
      InfoLoad(PMIX_JOB_SIZE, m_step_info_.task_num, PMIX_UINT32));
  info_list.emplace_back(
      InfoLoad(PMIX_LOCAL_SIZE, m_step_info_.node_tasks, PMIX_UINT32));
  info_list.emplace_back(
      InfoLoad(PMIX_NODE_SIZE, m_step_info_.node_tasks, PMIX_UINT32));
  info_list.emplace_back(
      InfoLoad(PMIX_MAX_PROCS, m_step_info_.task_num, PMIX_UINT32));

  // TODO: _set_topology

  if (PMIX_VERSION_MAJOR >= 5)
    info_list.emplace_back(
        InfoLoad(PMIX_USERID, m_step_info_.uid, PMIX_UINT32));

  // PMIx node-map names must match each rank's PMIX_HOSTNAME. Canonical IDs
  // remain in node_list for Crane's collective routing and job environment.
  std::unique_ptr<char, decltype(&free)> regex(nullptr, &free);
  char* raw_regex = nullptr;
  const auto hostnames = absl::StrJoin(m_step_info_.node_hostnames, ",");
  rc = PMIx_generate_regex(hostnames.c_str(), &raw_regex);
  regex.reset(raw_regex);
  if (rc != PMIX_SUCCESS) {
    CRANE_ERROR("Error: PMIx_generate_regex. {}", PMIx_Error_string(rc));
    for (auto& info : info_list) PMIX_INFO_DESTRUCT(&info);
    return false;
  }
#  ifdef PMIX_REGEX
  constexpr auto regex_type = PMIX_REGEX;
#  else
  constexpr auto regex_type = PMIX_STRING;
#  endif
  info_list.emplace_back(InfoLoad(PMIX_NODE_MAP, regex, regex_type));

  // proc_map "0,1,2;3,4" (per-node rank lists, separated by ';')
  // Use task_map (block-ordered) to build the correct per-node rank lists,
  // handling uneven distribution where nodes may have different task counts.
  std::vector<std::vector<uint32_t>> node_tasks(m_step_info_.node_num);
  for (uint32_t task = 0; task < m_step_info_.task_num; ++task)
    node_tasks[m_step_info_.task_map[task]].push_back(task);

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
  info_list.emplace_back(InfoLoad(PMIX_PROC_MAP, ppn, regex_type));

  std::string ranks_str = absl::StrJoin(local_ranks, ",");
  if (local_ranks.empty()) {
    CRANE_ERROR("No local PMIx ranks assigned on node {}",
                m_step_info_.craned_id);
    for (auto& info : info_list) PMIX_INFO_DESTRUCT(&info);
    return false;
  }
  // PMIX_LOCAL_PEERS: global namespace ranks of processes sharing this node.
  // PMIX_LOCALLDR: global rank of the local leader (lowest-numbered on node).
  info_list.emplace_back(InfoLoad(PMIX_LOCAL_PEERS, ranks_str, PMIX_STRING));
  info_list.emplace_back(
      InfoLoad(PMIX_LOCALLDR, my_node_start_rank, PMIX_UINT32));

  pmix_info_t* ns_info;
  PMIX_INFO_CREATE(ns_info, info_list.size());
  for (size_t i = 0; i < info_list.size(); i++) {
    ns_info[i] = info_list[i];
  }

  {
    absl::BlockingCounter bc(1);
    rc = PMIx_server_register_nspace(
        m_step_info_.nspace.c_str(), static_cast<int>(m_step_info_.node_tasks),
        ns_info, info_list.size(), PmixOperationCallback, &bc);
    if (rc != PMIX_SUCCESS) {
      PMIX_INFO_FREE(ns_info, info_list.size());
      CRANE_ERROR("Error: PMIx_server_register_nspace. {}",
                  PMIx_Error_string(rc));
      return false;
    }
    bc.Wait();
    PMIX_INFO_FREE(ns_info, info_list.size());
  }

  {
    absl::BlockingCounter bc(1);
    rc = PMIx_server_setup_local_support(m_step_info_.nspace.c_str(), nullptr,
                                         0, PmixOperationCallback, &bc);
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
  PMIX_LOAD_NSPACE(proc.nspace, m_step_info_.nspace.c_str());
  for (uint32_t global_rank = 0; global_rank < m_step_info_.task_num;
       ++global_rank) {
    if (m_step_info_.task_map[global_rank] != m_step_info_.node_id) continue;
    proc.rank = global_rank;
    {
      absl::BlockingCounter bc(1);
      rc =
          PMIx_server_register_client(&proc, m_step_info_.uid, m_step_info_.gid,
                                      nullptr, PmixOperationCallback, &bc);
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
#endif
std::optional<std::unordered_map<std::string, std::string>>
PmixServer::SetupFork(uint32_t rank) {
#ifdef HAVE_PMIX
  char** client_env = nullptr;

  // Compute the correct global rank of this local rank (0-based offset within
  // this node's task block) by scanning task_map for the start rank of this
  // node, then adding the local rank offset.
  pmix_proc_t proc;
  PMIX_LOAD_NSPACE(proc.nspace, m_step_info_.nspace.c_str());
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

  const auto crane_nodelist = absl::StrJoin(m_step_info_.node_list, ";");
  env_map.emplace("CRANE_JOB_NODELIST", crane_nodelist);
  env_map.emplace("CRANE_NODELIST", crane_nodelist);

  if (!m_enable_slurm_compatible_env_) {
    const auto slurm_nodelist = util::HostNameListToStr(m_step_info_.node_list);
    env_map.emplace("SLURM_JOBID", std::to_string(m_step_info_.job_id));
    env_map.emplace("SLURM_JOB_NODELIST", slurm_nodelist);
    env_map.emplace("SLURM_NODELIST", slurm_nodelist);
    env_map.emplace("SLURM_STEP_ID", std::to_string(m_step_info_.step_id));
  } else {
    const auto slurm_nodelist = absl::StrJoin(m_step_info_.node_list, ",");
    std::vector<uint32_t> task_counts(m_step_info_.node_list.size(), 0);
    for (uint32_t node_id : m_step_info_.task_map) {
      ++task_counts[node_id];
    }

    const uint32_t node_count =
        static_cast<uint32_t>(m_step_info_.node_list.size());
    const uint32_t task_count = m_step_info_.task_num;
    env_map.insert_or_assign("SLURM_JOBID",
                             std::to_string(m_step_info_.job_id));
    env_map.insert_or_assign("SLURM_JOB_ID",
                             std::to_string(m_step_info_.job_id));
    env_map.insert_or_assign("SLURM_STEP_ID",
                             std::to_string(m_step_info_.step_id));
    env_map.insert_or_assign("SLURM_STEPID",
                             std::to_string(m_step_info_.step_id));
    env_map.insert_or_assign("SLURM_STEP_NODELIST", slurm_nodelist);
    env_map.insert_or_assign("SLURM_STEP_NUM_NODES",
                             std::to_string(node_count));
    env_map.insert_or_assign("SLURM_STEP_NUM_TASKS",
                             std::to_string(task_count));
    env_map.insert_or_assign("SLURM_NNODES", std::to_string(node_count));
    env_map.insert_or_assign("SLURM_NTASKS", std::to_string(task_count));
    env_map.insert_or_assign("SLURM_NPROCS", std::to_string(task_count));

    env_map.insert_or_assign("SLURM_NODEID",
                             std::to_string(m_step_info_.node_id));
    env_map.insert_or_assign("SLURM_STEP_NODEID",
                             std::to_string(m_step_info_.node_id));
    env_map.insert_or_assign("SLURM_PROCID", std::to_string(rank));
    env_map.insert_or_assign("SLURM_STEP_PROCID", std::to_string(rank));

    uint32_t local_id = 0;
    for (uint32_t preceding_rank = 0; preceding_rank < rank; ++preceding_rank) {
      if (m_step_info_.task_map[preceding_rank] == m_step_info_.node_id) {
        ++local_id;
      }
    }
    env_map.insert_or_assign("SLURM_LOCALID", std::to_string(local_id));
    env_map.insert_or_assign("SLURM_STEP_LOCALID", std::to_string(local_id));

    std::vector<uint32_t> gtids;
    for (uint32_t task_id = 0; task_id < m_step_info_.task_map.size();
         ++task_id) {
      if (m_step_info_.task_map[task_id] == m_step_info_.node_id)
        gtids.emplace_back(task_id);
    }
    env_map.insert_or_assign("SLURM_GTIDS",
                             fmt::format("{}", fmt::join(gtids, ",")));

    const auto task_counts_str = absl::StrJoin(task_counts, ",");
    env_map.insert_or_assign("SLURM_TASKS_PER_NODE", task_counts_str);
    env_map.insert_or_assign("SLURM_STEP_TASKS_PER_NODE", task_counts_str);
  }

  if (!env_map.contains("OMPI_MCA_orte_precondition_transports")) {
    char key[64];
    std::snprintf(key, sizeof(key), "%08x%08x-%08x%08x", m_step_info_.job_id, 0,
                  m_step_info_.step_id, 0);
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

}  // namespace pmix

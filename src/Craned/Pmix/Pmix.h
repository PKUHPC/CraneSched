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

#include <fmt/format.h>
#include <parallel_hashmap/phmap.h>
#include <pmix_common.h>
#include <pmix_server.h>

#include <future>
#include <vector>

#include "PmixColl.h"
#include "PmixDModex.h"
#include "PmixState.h"
#include "absl/strings/str_join.h"
#include "crane/Logger.h"
#include "protos/Crane.grpc.pb.h"

namespace pmix {

class PmixServer;

struct PmixNameSpace {
  std::string nspace;
  uint32_t node_num = 0; /* number of nodes in this namespace */
  uint32_t task_num = 0; /* total number of tasks in this namespace */
  std::vector<uint32_t> task_map; /* i'th task is located on task_map[i] node */
  std::vector<std::string> hostlist;
};

class PmixTaskInstance {
 public:
  PmixTaskInstance() = default;

  ~PmixTaskInstance();
  PmixTaskInstance(const PmixTaskInstance&) = delete;
  PmixTaskInstance& operator=(const PmixTaskInstance&) = delete;
  PmixTaskInstance(PmixTaskInstance&&) = delete;
  PmixTaskInstance& operator=(PmixTaskInstance&&) = delete;

  bool Init(const crane::grpc::TaskToD& task, const std::unordered_map<std::string, std::string>& env_map);

  std::optional<std::unordered_map<std::string, std::string>> Setup(uint32_t rank);

 private:
  uint32_t m_uid_{};
  uint32_t m_gid_{};
  std::string m_task_id_;

  std::string m_nspace_; // crane.pmix.jobid
  std::string m_hostname_;
  std::vector<std::string> m_node_list_;
  std::string m_node_list_str_;
  uint32_t m_node_id_{};
  uint32_t m_node_num_ = 1;
  uint32_t m_ntasks_per_node_{}; /* number of tasks on *this* node */
  uint32_t m_task_num_{};

  uint32_t m_ncpus_{};  /* total possible number of cpus in job */
  std::string m_cli_tmpdir_;

  friend PmixServer;

  void InfoSet_(const crane::grpc::TaskToD& task, const std::unordered_map<std::string, std::string>& env_map);

  template <typename T>
  pmix_info_t InfoLoad_(const std::string& key, const T& val, pmix_data_type_t data_type);
};

class PmixServer {
 public:
  PmixServer() = default;

  ~PmixServer();
  PmixServer(const PmixServer&) = delete;
  PmixServer& operator=(const PmixServer&) = delete;
  PmixServer(PmixServer&&) = delete;
  PmixServer& operator=(PmixServer&&) = delete;

  bool Init(const std::string& server_base_dir);

  bool RegisterTask(const crane::grpc::TaskToD& task, const std::unordered_map<std::string, std::string>& env_map);

  std::optional<std::unordered_map<std::string, std::string>> SetupFork(task_id_t task_id, uint32_t rank);

  void DeregisterTask(task_id_t task_id);

  std::optional<PmixNameSpace> PmixNamespaceGet(const std::string& pmix_namespace);

  std::string GetHostname();

  std::optional<task_id_t> TaskIdGet(const std::string& pmix_namespace);

private:
  template <typename Key, typename Value>
  using ParallelMap = phmap::parallel_flat_hash_map<
      Key, Value,
      phmap::priv::hash_default_hash<Key>,
      phmap::priv::hash_default_eq<Key>,
      std::allocator<std::pair<const Key, Value>>,
      4, std::shared_mutex>;

  std::string m_server_tmpdir_;
  std::string m_hostname_;

  ParallelMap<task_id_t, std::unique_ptr<PmixTaskInstance>> m_task_instances_;

  ParallelMap<std::string, task_id_t> m_nspace_to_task_map_;

  ParallelMap<std::string, PmixNameSpace> m_namespace_map_;

  bool m_is_init_{false};
};

} // namespace pmix

inline std::unique_ptr<pmix::PmixServer> g_pmix_server;
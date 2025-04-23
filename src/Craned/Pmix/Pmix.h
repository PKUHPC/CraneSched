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
#include <pmix.h>
#include <pmix_common.h>
#include <pmix_server.h>

#include <future>
#include <vector>

#include "PmixColl.h"
#include "PmixState.h"
#include "absl/strings/str_join.h"
#include "crane/Logger.h"
#include "protos/Crane.grpc.pb.h"

namespace pmix {

class PmixTaskInstance {
 public:
  PmixTaskInstance() = default;

  ~PmixTaskInstance();

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
  uint32_t m_node_num_;
  uint32_t m_ntasks_per_node_{}; /* number of tasks on *this* node */
  uint32_t m_task_num_{};

  uint32_t m_ncpus_{};  /* total possible number of cpus in job */
  std::string m_cli_tmpdir_;

  void InfoSet_(const crane::grpc::TaskToD& task, const std::unordered_map<std::string, std::string>& env_map);

  template <typename T>
  pmix_info_t InfoLoad_(const std::string& key, const T& val, pmix_data_type_t data_type);
};

class PmixServer {
 public:
  PmixServer() = default;

  ~PmixServer();

  bool Init(const std::string& server_tmpdir);

  bool RegisterTask(const crane::grpc::TaskToD& task, const std::unordered_map<std::string, std::string>& env_map);

  std::optional<std::unordered_map<std::string, std::string>> SetupFork(task_id_t task_id, uint32_t rank);

  void DeregisterTask(task_id_t task_id);

private:
  std::string m_server_tmpdir_;
  //todo: Parallel
  std::mutex m_mutex_;
  std::unordered_map<task_id_t, std::unique_ptr<PmixTaskInstance>> m_task_instances_;
  bool m_is_init_{false};
};


} // namespace pmix
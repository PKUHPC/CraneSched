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
#include <pmix_common.h>
#include <pmix_server.h>
#include <sys/types.h>

#include <future>
#include <uvw.hpp>
#include <vector>

#include "CranedClient.h"
#include "PmixASyncServer.h"
#include "PmixClient.h"
#include "PmixColl.h"
#include "PmixCommon.h"
#include "PmixConn/PmixUcxServer.h"
#include "PmixDModex.h"
#include "PmixState.h"
#include "absl/strings/str_join.h"
#include "concurrentqueue/concurrentqueue.h"
#include "crane/Logger.h"
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

  bool Init(const Config& config, const crane::grpc::TaskToD& task,
            const std::unordered_map<std::string, std::string>& env_map);

  std::optional<std::unordered_map<std::string, std::string>> SetupFork(
      uint32_t rank);

  const std::string& GetHostname() const { return m_hostname_;}
  task_id_t GetTaskId() const { return static_cast<task_id_t>(std::stoul(m_task_id_)); }
  const std::vector<std::string>& GetNodeList() { return m_node_list_; }
  const std::vector<std::string>& GetPeerNodeList() { return m_peer_node_list_; }
  const std::vector<uint32_t>& GetTaskMap() { return m_task_map_; }
  const std::string& GetNSpace() { return m_nspace_; }
  uint32_t GetTaskNum() const { return m_task_num_; }
  uint64_t GetTimeout() const { return m_timeout_; }

  CranedClient* GetCranedClient() const { return m_craned_client_.get(); }
  PmixClient* GetPmixClient() const { return m_pmix_client_.get(); }
  PmixASyncServer* GetPmixAsyncServer() const {
    return m_pmix_async_server_.get();
  }
  uvw::loop* GetUvwLoop() const { return m_uvw_loop_.get(); }

 private:
  void InfoSet_(const Config& config, const crane::grpc::TaskToD& task,
                const std::unordered_map<std::string, std::string>& env_map);

  bool ConnInit_(const Config& config);

  bool PmixInit_() const;

  bool JobSet_();

  template <typename T>
  pmix_info_t InfoLoad_(const std::string& key, const T& val,
                        pmix_data_type_t data_type);

  uint32_t m_uid_{};
  uint32_t m_gid_{};
  std::string m_task_id_;
  uint32_t m_stepd_id_{0};

  std::string m_nspace_;  // crane.pmix.jobid
  std::string m_hostname_;
  std::vector<std::string> m_node_list_;
  std::vector<std::string> m_peer_node_list_;
  std::string m_node_list_str_;
  uint32_t m_node_id_{};
  uint32_t m_node_num_ = 1;
  uint32_t m_ntasks_per_node_{}; /* number of tasks on *this* node */
  uint32_t m_task_num_{};
  std::vector<uint32_t>
      m_task_map_; /* i'th task is located on task_map[i] node */

  uint32_t m_ncpus_{}; /* total possible number of cpus in job */

  uint64_t m_timeout_{10}; // TODO: PMIXP_TIMEOUT

  std::string m_server_tmpdir_;
  std::string m_cli_tmpdir_base_;
  std::string m_cli_tmpdir_;

  std::unique_ptr<CranedClient> m_craned_client_;
  std::unique_ptr<PmixClient> m_pmix_client_;
  std::unique_ptr<PmixASyncServer> m_pmix_async_server_;

  bool m_is_init_{false};

  std::thread m_uvw_thread_;
  std::atomic_bool m_cq_closed_{false};
  std::shared_ptr<uvw::loop> m_uvw_loop_;
  std::shared_ptr<uvw::timer_handle> m_cleanup_timer_handle_;
};

}  // namespace pmix

inline std::unique_ptr<pmix::PmixServer> g_pmix_server;
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
#include <pmix_common.h>
#endif

#include "crane/GrpcHelper.h"
#include "crane/Logger.h"

#include <string>
#include <unordered_map>
#include <vector>

namespace pmix {

struct Config {
  bool UseTls{false};
  TlsCertificates TlsCerts;
  bool CompressedRpc;
  std::filesystem::path CraneBaseDir;
  std::filesystem::path CraneScriptDir;
  std::filesystem::path CranedUnixSocketPath;
};

struct PmixJobInfo {
  uint32_t uid{};
  uint32_t gid{};
  job_id_t job_id{};
  step_id_t step_id{0};

  std::string nspace;  // crane.pmix.jobid.stepid
  std::string hostname;
  std::vector<std::string> node_list;
  std::vector<std::string> peer_node_list;
  std::string node_list_str;
  uint32_t node_id{};
  uint32_t node_num{1};
  uint32_t ntasks_per_node{}; /* number of tasks on *this* node */
  uint32_t task_num{};
  std::vector<uint32_t>
      task_map; /* i'th task is located on task_map[i] node */

  uint32_t ncpus{}; /* total possible number of cpus in job */

  std::string server_tmpdir;
  std::string cli_tmpdir_base;
  std::string cli_tmpdir;
  std::string fence_type;
  std::string pmix_direct_conn_ucx;
};

static constexpr uint64_t kTagTypeShift   = 48;
static constexpr uint64_t kTagTypeMask    = 0xFFFF000000000000ULL;
static constexpr uint64_t kTagLowMask     = 0x0000FFFFFFFFFFFFULL;
// 每个类型同时挂接的接收个数（可根据负载调大）
static constexpr int      kInflightPerType = 256;
// 每条消息最大长度（可配置/调优；若需要超大消息建议用 AM 或 pipeline）
static constexpr size_t   kRecvMaxBytes    = 1 << 20; // 1MB

static constexpr uint64_t kRpcTimeoutSeconds = 5;

enum class PmixUcxMsgType : uint16_t {
  PMIX_UCX_TREE_UPWARD_FORWARD = 0,
  PMIX_UCX_TREE_DOWNWARD_FORWARD,
  PMIX_UCX_DMDEX_REQUEST,
  PMIX_UCX_DMDEX_RESPONSE,
  PMIX_UCX_SEND_PMIX_RING_MSG
};

#ifdef HAVE_PMIX
inline void PmixLibModexInvoke(pmix_modex_cbfunc_t cbfunc, int status,
                        const char* data, size_t ndata, void* cbdata,
                        void* rel_fn, void* rel_data) {
  pmix_status_t rc = PMIX_SUCCESS;
  auto release_fn = reinterpret_cast<pmix_release_cbfunc_t>(rel_fn);

  // Currently, CraneSched only focuses on these two scenarios.
  switch (status) {
  case PMIX_SUCCESS:
    rc = PMIX_SUCCESS;
    break;
  case PMIX_ERR_INVALID_NAMESPACE:
    rc = PMIX_ERR_INVALID_NAMESPACE;
    break;
  case PMIX_ERR_BAD_PARAM:
    rc = PMIX_ERR_BAD_PARAM;
    break;
  case PMIX_ERR_TIMEOUT:
    rc = PMIX_ERR_TIMEOUT;
    break;
  default:
    rc = PMIX_ERROR;
  }

  cbfunc(rc, data, ndata, cbdata, release_fn, rel_data);
}
#endif

} // namespace pmix
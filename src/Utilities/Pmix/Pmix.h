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
#include <pmix.h>
#include <pmix_common.h>
#include <pmix_server.h>

#include <future>
#include <vector>

#include <parallel_hashmap/phmap.h>

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



     // TODO: kill job

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

     if (nullptr != cbfunc) {
       cbfunc(PMIX_SUCCESS, data, ndata, cbdata, nullptr, nullptr);
     }

     return PMIX_SUCCESS;
     std::vector<pmix_proc_t> procs;
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

     // TODO: 从env中获取type，默认tree
     // CollType type = pmixp_info_srv_fence_coll_type();
     CollType type = CollType::FENCE_TREE;

     if (type == CollType::FENCE_MAX) {
       type = CollType::FENCE_TREE;

       if (collect && (ndata > 0))
         type = CollType::FENCE_RING;
     }

     auto coll = pmix_state_ptr->PmixStateCollGet(type, procs, nprocs);

     if (coll == nullptr) return PMIX_ERROR;

     if (!coll->PmixCollContribLocal(type, data, ndata, cbfunc, cbdata))
       return PMIX_ERROR;

     return PMIX_SUCCESS;
   }

   static pmix_status_t DmodexFn(const pmix_proc_t *proc,
                                  const pmix_info_t info[], size_t ninfo,
                                  pmix_modex_cbfunc_t cbfunc, void *cbdata) {
     CRANE_DEBUG("DmodexFn is called");
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

} // namespace pmix
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
#include <pmix_common.h>

#include "PmixDModex.h"
#include "TaskManager.h"
#include "crane/Logger.h"

namespace pmix {

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

     auto result = g_pmix_server->TaskIdGet(pmix_proc->nspace);
     if (result) {
       g_task_mgr->TerminateTaskAsync(result.value());
     }

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

} // namespace pmix
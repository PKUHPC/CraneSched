

#pragma once

#include <pmix.h>

#include <string>
#include <unordered_map>

#include "absl/synchronization/blocking_counter.h"

namespace pmix {

class PMIxServerModule {
  public:
   static int ClientConnectedCb(const pmix_proc_t *proc, void *server_object,
                               pmix_op_cbfunc_t cbfunc, void *cbdata) {
     /* we don't do anything by now */
     printf("ClientConnected is called\n");
     return PMIX_SUCCESS;
   }

   static void OpCb(pmix_status_t status, void *cbdata) {
     printf("op callback is called with status=%d: %s\n", status,
            PMIx_Error_string(status));
   }

   static void ErrHandlerRegCb(pmix_status_t status, size_t errhandler_ref,
                                     void *cbdata) {
     printf(
         "Error handler registration callback is called with status=%d, "
         "ref=%d\n",
         status, (int)errhandler_ref);
   }

   static pmix_status_t ClientFinalizedCb(const pmix_proc_t *proc,
                                         void *server_object,
                                         pmix_op_cbfunc_t cbfunc, void *cbdata) {
     printf("ClientFinalized is called\n");
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
     printf("abort_fn called: status = %d, msg = %s\n", status, msg);
     pmix_status_t rc;
     if (NULL != pmix_procs) {
       printf( "SERVER: ABORT on %s:%d", pmix_procs[0].nspace, pmix_procs[0].rank);
     } else {
       printf("SERVER: ABORT OF ALL PROCS IN NSPACE %s", pmix_procs->nspace);
     }
     return PMIX_SUCCESS;
   }

   static pmix_status_t FencenbFn(const pmix_proc_t procs_v2[], size_t nprocs,
                                   const pmix_info_t info[], size_t ninfo,
                                   char *data, size_t ndata,
                                   pmix_modex_cbfunc_t cbfunc, void *cbdata) {
     printf(" FencenbFn is called\n");
     /* pass the provided data back to each participating proc */
     if (NULL != cbfunc) {
       cbfunc(PMIX_SUCCESS, data, ndata, cbdata, NULL, NULL);
     }
     return PMIX_SUCCESS;
   }

   static pmix_status_t DmodexFn(const pmix_proc_t *proc,
                                  const pmix_info_t info[], size_t ninfo,
                                  pmix_modex_cbfunc_t cbfunc, void *cbdata) {
     printf("DmodexFn is called\n");
     return PMIX_SUCCESS;
   }

   static pmix_status_t JobControl(const pmix_proc_t *proct,
                                    const pmix_proc_t targets[], size_t ntargets,
                                    const pmix_info_t directives[], size_t ndirs,
                                    pmix_info_cbfunc_t cbfunc, void *cbdata) {
     printf("JobControl is called\n");
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

static void AppCb(pmix_status_t status, pmix_info_t info[], size_t ninfo,
  void *provided_cbdata, pmix_op_cbfunc_t cbfunc,
  void *cbdata) {
  auto *bc = reinterpret_cast<absl::BlockingCounter *>(provided_cbdata);
  bc->DecrementCount();
  printf("app callback is called with status=%d: %s\n", status,
  PMIx_Error_string(status));
}

static void OpCb(pmix_status_t status, void *cbdata) {
  auto *bc = reinterpret_cast<absl::BlockingCounter *>(cbdata);
  bc->DecrementCount();
  printf("op callback is called with status=%d: %s\n", status,
  PMIx_Error_string(status));
}

static pmix_server_module_t CranePmixCb = {
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

};
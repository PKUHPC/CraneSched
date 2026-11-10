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

#include "Callbacks.h"

#ifdef HAVE_PMIX

#  include "Collective/Collective.h"
#  include "Server.h"
#  include "absl/synchronization/blocking_counter.h"
#  include "crane/Logger.h"

namespace pmix {
// PMIx invokes these adapters through its C ABI callback table.
#  ifdef HAVE_PMIX
extern "C" {

static int ClientConnectedWrapper(const pmix_proc_t* p, void* s,
                                  pmix_op_cbfunc_t cb, void* d) {
  return pmix::PmixServerCallbacks::ClientConnected(p, s, cb, d);
}
static pmix_status_t ClientFinalizedWrapper(const pmix_proc_t* p, void* s,
                                            pmix_op_cbfunc_t cb, void* d) {
  return pmix::PmixServerCallbacks::ClientFinalized(p, s, cb, d);
}
static pmix_status_t AbortWrapper(const pmix_proc_t* p, void* s, int st,
                                  const char m[], pmix_proc_t ps[], size_t np,
                                  pmix_op_cbfunc_t cb, void* d) {
  auto* server = PmixServer::GetInstance();
  if (!server) return PMIX_ERR_INIT;
  return server->Abort(p, s, st, m, ps, np, cb, d);
}
static pmix_status_t FenceNbWrapper(const pmix_proc_t pv[], size_t np,
                                    const pmix_info_t inf[], size_t ni,
                                    char* data, size_t nd,
                                    pmix_modex_cbfunc_t cb, void* d) {
  auto* server = PmixServer::GetInstance();
  if (!server) return PMIX_ERR_INIT;
  return server->FenceNb(pv, np, inf, ni, data, nd, cb, d);
}
static pmix_status_t DModexWrapper(const pmix_proc_t* p,
                                   const pmix_info_t inf[], size_t ni,
                                   pmix_modex_cbfunc_t cb, void* d) {
  auto* server = PmixServer::GetInstance();
  if (!server) return PMIX_ERR_INIT;
  return server->DModex(p, inf, ni, cb, d);
}
static pmix_status_t JobControlWrapper(const pmix_proc_t* p,
                                       const pmix_proc_t t[], size_t nt,
                                       const pmix_info_t d2[], size_t nd,
                                       pmix_info_cbfunc_t cb, void* d) {
  return pmix::PmixServerCallbacks::JobControl(p, t, nt, d2, nd, cb, d);
}
static pmix_status_t PublishWrapper(const pmix_proc_t* p,
                                    const pmix_info_t inf[], size_t ni,
                                    pmix_op_cbfunc_t cb, void* d) {
  return pmix::PmixServerCallbacks::Publish(p, inf, ni, cb, d);
}
static pmix_status_t LookupWrapper(const pmix_proc_t* p, char** k,
                                   const pmix_info_t inf[], size_t ni,
                                   pmix_lookup_cbfunc_t cb, void* d) {
  return pmix::PmixServerCallbacks::Lookup(p, k, inf, ni, cb, d);
}
static pmix_status_t UnpublishWrapper(const pmix_proc_t* p, char** k,
                                      const pmix_info_t inf[], size_t ni,
                                      pmix_op_cbfunc_t cb, void* d) {
  return pmix::PmixServerCallbacks::Unpublish(p, k, inf, ni, cb, d);
}
static pmix_status_t SpawnWrapper(const pmix_proc_t* p, const pmix_info_t inf[],
                                  size_t ni, const pmix_app_t apps[], size_t na,
                                  pmix_spawn_cbfunc_t cb, void* d) {
  return pmix::PmixServerCallbacks::Spawn(p, inf, ni, apps, na, cb, d);
}
static pmix_status_t ConnectWrapper(const pmix_proc_t p[], size_t np,
                                    const pmix_info_t inf[], size_t ni,
                                    pmix_op_cbfunc_t cb, void* d) {
  return pmix::PmixServerCallbacks::Connect(p, np, inf, ni, cb, d);
}
static pmix_status_t DisconnectWrapper(const pmix_proc_t p[], size_t np,
                                       const pmix_info_t inf[], size_t ni,
                                       pmix_op_cbfunc_t cb, void* d) {
  return pmix::PmixServerCallbacks::Disconnect(p, np, inf, ni, cb, d);
}
void PmixApplicationCallback(pmix_status_t st, pmix_info_t inf[], size_t ni,
                             void* cbd, pmix_op_cbfunc_t cb, void* cd) {
  pmix::PmixServerCallbacks::AppCb(st, inf, ni, cbd, cb, cd);
}
void PmixOperationCallback(pmix_status_t st, void* cd) {
  pmix::PmixServerCallbacks::OpCb(st, cd);
}
static void ErrHandlerRegCbWrapper(pmix_status_t st, size_t ref, void* cd) {
  pmix::PmixServerCallbacks::ErrHandlerRegCb(st, ref, cd);
}
static void ErrHandlerWrapper(size_t id, pmix_status_t st,
                              const pmix_proc_t* src, pmix_info_t inf[],
                              size_t ni, pmix_info_t* res, size_t nr,
                              pmix_event_notification_cbfunc_fn_t cb,
                              void* cd) {
  pmix::PmixServerCallbacks::ErrHandler(id, st, src, inf, ni, res, nr, cb, cd);
}

}  // extern "C"

// PMIx server module struct — references only the extern "C" wrappers above.
static pmix_server_module_t s_crane_pmix_module = {
    .client_connected = ClientConnectedWrapper,
    .client_finalized = ClientFinalizedWrapper,
    .abort = AbortWrapper,
    .fence_nb = FenceNbWrapper,
    .direct_modex = DModexWrapper,
    .publish = PublishWrapper,
    .lookup = LookupWrapper,
    .unpublish = UnpublishWrapper,
    .spawn = SpawnWrapper,
    .connect = ConnectWrapper,
    .disconnect = DisconnectWrapper,
    .job_control = JobControlWrapper,
};

#  endif  // HAVE_PMIX

#  ifdef HAVE_PMIX
pmix_server_module_t* PmixServerCallbacks::Module() {
  return &s_crane_pmix_module;
}
void PmixServerCallbacks::RegisterEvents() {
  PMIx_Register_event_handler(nullptr, 0, nullptr, 0, ErrHandlerWrapper,
                              ErrHandlerRegCbWrapper, nullptr);
}
void PmixServerCallbacks::DeregisterEvents() {
  if (s_errhandler_ref == 0) return;
  auto status =
      PMIx_Deregister_event_handler(s_errhandler_ref, nullptr, nullptr);
  if (status != PMIX_SUCCESS)
    CRANE_ERROR("Failed to deregister PMIx event handler: {}",
                PMIx_Error_string(status));
  s_errhandler_ref = 0;
}
#  endif

// Static member definition.
size_t PmixServerCallbacks::s_errhandler_ref = 0;

int PmixServerCallbacks::ClientConnected(const pmix_proc_t* proc,
                                         void* /*server_object*/,
                                         pmix_op_cbfunc_t cbfunc,
                                         void* cbdata) {
  CRANE_DEBUG("ClientConnected: proc=[{}:{}]", proc->nspace, proc->rank);
  if (cbfunc) cbfunc(PMIX_SUCCESS, cbdata);
  return PMIX_SUCCESS;
}

pmix_status_t PmixServerCallbacks::ClientFinalized(const pmix_proc_t* proc,
                                                   void* /*server_object*/,
                                                   pmix_op_cbfunc_t cbfunc,
                                                   void* cbdata) {
  CRANE_DEBUG("ClientFinalized: proc=[{}:{}]", proc->nspace, proc->rank);
  if (cbfunc) cbfunc(PMIX_SUCCESS, cbdata);
  return PMIX_SUCCESS;
}

pmix_status_t PmixServerCallbacks::JobControl(
    const pmix_proc_t* /*proct*/, const pmix_proc_t* /*targets*/,
    size_t /*ntargets*/, const pmix_info_t* /*directives*/, size_t /*ndirs*/,
    pmix_info_cbfunc_t cbfunc, void* cbdata) {
  CRANE_DEBUG("JobControl called (not supported)");
  // PMIx spec requires the completion callback to always be invoked, even when
  // the operation is not supported, so the caller is not left waiting forever.
  if (cbfunc)
    cbfunc(PMIX_ERR_NOT_SUPPORTED, nullptr, 0, cbdata, nullptr, nullptr);
  return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t PmixServerCallbacks::Publish(const pmix_proc_t*,
                                           const pmix_info_t[], size_t,
                                           pmix_op_cbfunc_t, void*) {
  return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t PmixServerCallbacks::Lookup(const pmix_proc_t*, char**,
                                          const pmix_info_t[], size_t,
                                          pmix_lookup_cbfunc_t, void*) {
  return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t PmixServerCallbacks::Unpublish(const pmix_proc_t*, char**,
                                             const pmix_info_t[], size_t,
                                             pmix_op_cbfunc_t, void*) {
  return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t PmixServerCallbacks::Spawn(const pmix_proc_t*,
                                         const pmix_info_t[], size_t,
                                         const pmix_app_t[], size_t,
                                         pmix_spawn_cbfunc_t, void*) {
  return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t PmixServerCallbacks::Connect(const pmix_proc_t[], size_t,
                                           const pmix_info_t[], size_t,
                                           pmix_op_cbfunc_t, void*) {
  return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t PmixServerCallbacks::Disconnect(const pmix_proc_t[], size_t,
                                              const pmix_info_t[], size_t,
                                              pmix_op_cbfunc_t, void*) {
  return PMIX_ERR_NOT_SUPPORTED;
}

void PmixServerCallbacks::AppCb(pmix_status_t status, pmix_info_t[] /*info*/,
                                size_t /*ninfo*/, void* provided_cbdata,
                                pmix_op_cbfunc_t /*cbfunc*/, void* /*cbdata*/) {
  auto* bc = reinterpret_cast<absl::BlockingCounter*>(provided_cbdata);
  bc->DecrementCount();
  CRANE_DEBUG("AppCb: status={}: {}", status, PMIx_Error_string(status));
}

void PmixServerCallbacks::OpCb(pmix_status_t status, void* cbdata) {
  auto* bc = reinterpret_cast<absl::BlockingCounter*>(cbdata);
  bc->DecrementCount();
  CRANE_DEBUG("OpCb: status={}: {}", status, PMIx_Error_string(status));
}

void PmixServerCallbacks::ErrHandlerRegCb(pmix_status_t status,
                                          size_t errhandler_ref,
                                          void* /*cbdata*/) {
  if (status == PMIX_SUCCESS) s_errhandler_ref = errhandler_ref;
  CRANE_DEBUG("ErrHandlerRegCb: status={}, ref={}", status,
              static_cast<int>(errhandler_ref));
}

void PmixServerCallbacks::ErrHandler(
    size_t /*evhdlr_registration_id*/, pmix_status_t status,
    const pmix_proc_t* source, pmix_info_t[] /*info*/, size_t /*ninfo*/,
    pmix_info_t* /*results*/, size_t /*nresults*/,
    pmix_event_notification_cbfunc_fn_t cbfunc, void* cbdata) {
  // Per PMIx RFC0002, the resource manager passes a NULL source for
  // system-wide events and internal PMIx server errors.  Guard before
  // dereferencing to avoid a crash in those cases.
  if (source != nullptr) {
    CRANE_ERROR("PMIx error handler invoked: status={}, source=[{}:{}]", status,
                source->nspace, source->rank);
  } else {
    CRANE_ERROR(
        "PMIx error handler invoked: status={}, source=[system/internal]",
        status);
  }
  if (auto* server = PmixServer::GetInstance()) {
    if (auto* client = server->GetStepControlClient()) client->TerminateSteps();
  }
  // Signal PMIx that this event handler has completed.  Per the PMIx event
  // notification specification this call is mandatory: omitting it permanently
  // blocks the PMIx progress thread and prevents any subsequent event handlers
  // from executing.
  if (cbfunc)
    cbfunc(PMIX_EVENT_ACTION_COMPLETE, nullptr, 0, nullptr, nullptr, cbdata);
}

}  // namespace pmix

#endif  // HAVE_PMIX

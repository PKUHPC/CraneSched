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

#include "PmixCallbacks.h"

#ifdef HAVE_PMIX

#  include "Pmix.h"
#  include "PmixColl/PmixColl.h"
#  include "absl/synchronization/blocking_counter.h"
#  include "crane/Logger.h"

namespace pmix {

// Static member definition.
size_t PmixServerCallbacks::s_errhandler_ref = 0;

// ── Lifecycle callbacks
// ───────────────────────────────────────────────────────

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

pmix_status_t PmixServerCallbacks::Abort(
    const pmix_proc_t* /*pmix_proc*/, void* /*server_object*/, int status,
    const char msg[], pmix_proc_t /*pmix_procs*/[], size_t /*nprocs*/,
    pmix_op_cbfunc_t cbfunc, void* cbdata) {
  CRANE_WARN("PMIx Abort invoked: status={}, msg={}", status, msg);
  PmixServer::GetInstance()->GetCranedClient()->TerminateSteps();
  if (cbfunc) cbfunc(PMIX_SUCCESS, cbdata);
  return PMIX_SUCCESS;
}

// ── Fence (collective)
// ────────────────────────────────────────────────────────

pmix_status_t PmixServerCallbacks::FenceNb(
    const pmix_proc_t procs_v2[], size_t nprocs, const pmix_info_t info[],
    size_t ninfo, char* data, size_t ndata, pmix_modex_cbfunc_t cbfunc,
    void* cbdata) {
  CRANE_DEBUG("FenceNb: nprocs={}, ndata={}", nprocs, ndata);

  std::vector<pmix_proc_t> procs;
  procs.reserve(nprocs);
  bool collect = false;

  for (size_t i = 0; i < nprocs; i++) procs.emplace_back(procs_v2[i]);

  if (info != nullptr) {
    for (size_t i = 0; i < ninfo; i++) {
      if (strncmp(info[i].key, PMIX_COLLECT_DATA, PMIX_MAX_KEYLEN) == 0) {
        collect = true;
        break;
      }
    }
  }

  if (data == nullptr && ndata > 0) {
    CRANE_ERROR("FenceNb: non-zero ndata={} but null data pointer", ndata);
    cbfunc(PMIX_ERROR, nullptr, 0, cbdata, nullptr, nullptr);
    return PMIX_ERROR;
  }

  auto* srv = PmixServer::GetInstance();
  CollType type = StrToCollType(srv->GetFenceType());
  if (type == CollType::FENCE_MAX) {
    type = (collect && ndata > 0) ? CollType::FENCE_RING : CollType::FENCE_TREE;
  }

  CRANE_TRACE("FenceNb: selected collective type={}", ToString(type));

  auto coll = srv->GetPmixState()->PmixStateCollGet(type, procs);
  if (!coll) {
    CRANE_ERROR("FenceNb: failed to obtain collective object for type={}",
                ToString(type));
    cbfunc(PMIX_ERROR, nullptr, 0, cbdata, nullptr, nullptr);
    return PMIX_ERROR;
  }

  std::string local_data;
  if (data != nullptr && ndata > 0) local_data.assign(data, ndata);
  if (!coll->PmixCollContribLocal(local_data, cbfunc, cbdata)) {
    CRANE_ERROR("FenceNb: failed to contribute local data to collective");
    cbfunc(PMIX_ERROR, nullptr, 0, cbdata, nullptr, nullptr);
    return PMIX_ERROR;
  }

  return PMIX_SUCCESS;
}

// ── Direct modex
// ──────────────────────────────────────────────────────────────

pmix_status_t PmixServerCallbacks::DModex(const pmix_proc_t* proc,
                                          const pmix_info_t* /*info*/,
                                          size_t /*ninfo*/,
                                          pmix_modex_cbfunc_t cbfunc,
                                          void* cbdata) {
  CRANE_DEBUG("DModex: proc=[{}:{}]", proc->nspace, proc->rank);
  auto* srv = PmixServer::GetInstance();
  if (!srv->GetDmodexReqManager()->PmixDModexGet(
          proc->nspace, static_cast<int>(proc->rank), cbfunc, cbdata)) {
    CRANE_ERROR("DModex: PmixDModexGet failed for [{}:{}]", proc->nspace,
                proc->rank);
    cbfunc(PMIX_ERROR, nullptr, 0, cbdata, nullptr, nullptr);
    return PMIX_ERROR;
  }
  return PMIX_SUCCESS;
}

// ── Job control
// ───────────────────────────────────────────────────────────────

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

// ── Unsupported publish/lookup/spawn/connect operations
// ───────────────────────

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

// ── Internal async callbacks
// ──────────────────────────────────────────────────

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
  PmixServer::GetInstance()->GetCranedClient()->TerminateSteps();
  // Signal PMIx that this event handler has completed.  Per the PMIx event
  // notification specification this call is mandatory: omitting it permanently
  // blocks the PMIx progress thread and prevents any subsequent event handlers
  // from executing.
  if (cbfunc)
    cbfunc(PMIX_EVENT_ACTION_COMPLETE, nullptr, 0, nullptr, nullptr, cbdata);
}

}  // namespace pmix

#endif  // HAVE_PMIX

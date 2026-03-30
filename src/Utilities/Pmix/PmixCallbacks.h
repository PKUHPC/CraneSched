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
#include <pmix_server.h>
#endif

#include "PmixCommon.h"

namespace pmix {

#ifdef HAVE_PMIX

/**
 * PmixServerCallbacks holds the actual C++ logic for all PMIx server module
 * callbacks and internal async callbacks.
 *
 * The thin extern "C" wrapper functions in Pmix.cpp forward every PMIx C
 * callback to a corresponding static method here.  This keeps the ABI boundary
 * (extern "C") as a one-liner per callback while all real logic lives in a
 * proper C++ class that can be unit-tested and extended freely.
 *
 * All methods that need server state reach it via PmixServer::GetInstance().
 */
class PmixServerCallbacks {
 public:
  // ── PMIx server module callbacks ──────────────────────────────────────────

  static int ClientConnected(const pmix_proc_t* proc, void* server_object,
                              pmix_op_cbfunc_t cbfunc, void* cbdata);

  static pmix_status_t ClientFinalized(const pmix_proc_t* proc,
                                        void* server_object,
                                        pmix_op_cbfunc_t cbfunc, void* cbdata);

  static pmix_status_t Abort(const pmix_proc_t* pmix_proc, void* server_object,
                              int status, const char msg[],
                              pmix_proc_t pmix_procs[], size_t nprocs,
                              pmix_op_cbfunc_t cbfunc, void* cbdata);

  static pmix_status_t FenceNb(const pmix_proc_t procs[], size_t nprocs,
                                const pmix_info_t info[], size_t ninfo,
                                char* data, size_t ndata,
                                pmix_modex_cbfunc_t cbfunc, void* cbdata);

  static pmix_status_t DModex(const pmix_proc_t* proc,
                               const pmix_info_t info[], size_t ninfo,
                               pmix_modex_cbfunc_t cbfunc, void* cbdata);

  static pmix_status_t JobControl(const pmix_proc_t* proct,
                                   const pmix_proc_t targets[], size_t ntargets,
                                   const pmix_info_t directives[], size_t ndirs,
                                   pmix_info_cbfunc_t cbfunc, void* cbdata);

  static pmix_status_t Publish(const pmix_proc_t* proc,
                                const pmix_info_t info[], size_t ninfo,
                                pmix_op_cbfunc_t cbfunc, void* cbdata);

  static pmix_status_t Lookup(const pmix_proc_t* proc, char** keys,
                               const pmix_info_t info[], size_t ninfo,
                               pmix_lookup_cbfunc_t cbfunc, void* cbdata);

  static pmix_status_t Unpublish(const pmix_proc_t* proc, char** keys,
                                  const pmix_info_t info[], size_t ninfo,
                                  pmix_op_cbfunc_t cbfunc, void* cbdata);

  static pmix_status_t Spawn(const pmix_proc_t* proc,
                              const pmix_info_t job_info[], size_t ninfo,
                              const pmix_app_t apps[], size_t napps,
                              pmix_spawn_cbfunc_t cbfunc, void* cbdata);

  static pmix_status_t Connect(const pmix_proc_t procs[], size_t nprocs,
                                const pmix_info_t info[], size_t ninfo,
                                pmix_op_cbfunc_t cbfunc, void* cbdata);

  static pmix_status_t Disconnect(const pmix_proc_t procs[], size_t nprocs,
                                   const pmix_info_t info[], size_t ninfo,
                                   pmix_op_cbfunc_t cbfunc, void* cbdata);

  // ── Internal async callbacks passed directly to PMIx API calls ───────────

  static void AppCb(pmix_status_t status,
                    pmix_info_t info[] [[maybe_unused]],
                    size_t ninfo [[maybe_unused]],
                    void* provided_cbdata,
                    pmix_op_cbfunc_t cbfunc [[maybe_unused]],
                    void* cbdata [[maybe_unused]]);

  static void OpCb(pmix_status_t status, void* cbdata);

  static void ErrHandlerRegCb(pmix_status_t status, size_t errhandler_ref,
                               void* cbdata);

  static void ErrHandler(size_t evhdlr_registration_id, pmix_status_t status,
                          const pmix_proc_t* source, pmix_info_t info[],
                          size_t ninfo, pmix_info_t* results, size_t nresults,
                          pmix_event_notification_cbfunc_fn_t cbfunc,
                          void* cbdata);

  // Stored so we can deregister the error handler on shutdown.
  static size_t s_errhandler_ref;
};

#endif  // HAVE_PMIX

}  // namespace pmix

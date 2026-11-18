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
#  include <pmix_server.h>
#endif

#include "Types.h"

namespace pmix {

#ifdef HAVE_PMIX

// PMIx C ABI adapter. Algorithm state lives in collective/modex managers.
extern "C" void PmixOperationCallback(pmix_status_t status, void* cbdata);
extern "C" void PmixApplicationCallback(pmix_status_t status,
                                        pmix_info_t info[], size_t ninfo,
                                        void* provided_cbdata,
                                        pmix_op_cbfunc_t cbfunc, void* cbdata);
class PmixServerCallbacks {
 public:
  static pmix_server_module_t* Module();
  static void RegisterEvents();
  static void DeregisterEvents();

  static int ClientConnected(const pmix_proc_t* proc, void* server_object,
                             pmix_op_cbfunc_t cbfunc, void* cbdata);

  static pmix_status_t ClientFinalized(const pmix_proc_t* proc,
                                       void* server_object,
                                       pmix_op_cbfunc_t cbfunc, void* cbdata);

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

  static void AppCb(pmix_status_t status, pmix_info_t info[], size_t ninfo,
                    void* provided_cbdata, pmix_op_cbfunc_t cbfunc,
                    void* cbdata);

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

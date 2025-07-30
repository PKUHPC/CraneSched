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
#include "PmixState.h"
#include "crane/GrpcHelper.h"
#include "crane/Logger.h"

namespace pmix {

struct Config {
  bool UseTls{false};
  TlsCertificates TlsCerts;
  bool CompressedRpc;
  std::filesystem::path CraneBaseDir;
  std::filesystem::path CraneScriptDir;
  std::filesystem::path CranedUnixSocketPath;
};


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

} // namespace pmix
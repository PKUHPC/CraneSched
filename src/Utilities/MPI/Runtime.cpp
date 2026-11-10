/**
 * Copyright (c) 2026 Peking University and Peking University
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

#include "Runtime.h"

#include "Pmix/Runtime.h"
#ifdef HAVE_PMI2
#  include "PMI/Runtime.h"
#endif

namespace mpi {
std::unique_ptr<Runtime> CreateRuntime(std::string_view protocol) {
#ifdef HAVE_PMIX
  if (protocol == kMpiTypePmix) return pmix::CreateRuntime();
#endif
#ifdef HAVE_PMI2
  if (protocol == kMpiTypePmi2) return pmi::CreateRuntime();
#endif
  return nullptr;
}
}  // namespace mpi

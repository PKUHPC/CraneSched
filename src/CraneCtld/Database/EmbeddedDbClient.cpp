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

#include "EmbeddedDbClient.h"

#include "LegacyEmbeddedDbClient.h"

#ifdef CRANE_HAVE_ROCKSDB
#  include "RocksDbEmbeddedStore.h"
#endif

namespace Ctld {

std::unique_ptr<EmbeddedDbClient> MakeEmbeddedDbClient(
    std::string_view backend) {
  if (backend == "RocksDB") {
#ifdef CRANE_HAVE_ROCKSDB
    return std::make_unique<RocksDbEmbeddedStore>();
#else
    CRANE_ERROR(
        "Select RocksDB as the embedded db but it's not been compiled.");
    return nullptr;
#endif
  }

  if (backend == "Unqlite") {
    return std::make_unique<LegacyEmbeddedDbClient>(
        LegacyEmbeddedDbBackend::Unqlite);
  }

  if (backend == "BerkeleyDB") {
    return std::make_unique<LegacyEmbeddedDbClient>(
        LegacyEmbeddedDbBackend::BerkeleyDB);
  }

  CRANE_ERROR("Invalid embedded database backend: {}", backend);
  return nullptr;
}

}  // namespace Ctld

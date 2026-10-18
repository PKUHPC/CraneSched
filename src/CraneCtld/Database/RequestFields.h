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

#pragma once

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/document/view.hpp>
#include <optional>

#include "protos/PublicDefs.pb.h"

namespace Ctld::accounting {

// Shared by job/step and live/recovery archival. A null GRES pointer means
// unknown, while an empty message is a known request with no GRES.
void AppendRequestFields(bsoncxx::builder::basic::document* doc,
                         uint32_t ntasks, const crane::grpc::GresMap* gres,
                         const crane::grpc::GresMap& per_node);

struct RequestFields {
  std::optional<uint32_t> ntasks;
  std::optional<crane::grpc::GresMap> gres;
};

RequestFields ReadRequestFields(bsoncxx::document::view doc);

// Older embedded records retain the submitted per-node request. An unknown
// normalized node count (0) cannot reconstruct a nonempty total request.
std::optional<crane::grpc::GresMap> RecoverRequestedGres(
    const crane::grpc::GresMap& per_node, uint32_t nodes,
    const crane::grpc::GresMap* snapshot);

template <typename Info>
void RestoreRequestFields(bsoncxx::document::view doc, Info* info) {
  auto fields = ReadRequestFields(doc);
  info->clear_ntasks();
  info->mutable_req_total_res_view()->clear_gres_map();
  if (fields.ntasks) info->set_ntasks(*fields.ntasks);
  if (fields.gres)
    *info->mutable_req_total_res_view()->mutable_gres_map() =
        std::move(*fields.gres);
}

}  // namespace Ctld::accounting

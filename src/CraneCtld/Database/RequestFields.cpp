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

#include "RequestFields.h"

#include <bsoncxx/builder/basic/kvp.hpp>
#include <limits>
#include <stdexcept>

namespace Ctld::accounting {
namespace {
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::sub_document;

void AppendGres(sub_document doc, const crane::grpc::GresMap& gres) {
  for (const auto& [name, count] : gres.name_gres_map()) {
    doc.append(kvp(name, [&](sub_document device) {
      device.append(kvp("total", static_cast<int64_t>(count.total())));
      device.append(kvp("type_count_map", [&](sub_document types) {
        for (const auto& [type, value] : count.specified())
          types.append(kvp(type, static_cast<int64_t>(value)));
      }));
    }));
  }
}

int64_t ReadInteger(bsoncxx::document::element elem) {
  return elem.type() == bsoncxx::type::k_int32 ? elem.get_int32().value
                                               : elem.get_int64().value;
}
}  // namespace

void AppendRequestFields(bsoncxx::builder::basic::document* doc,
                         uint32_t ntasks, const crane::grpc::GresMap* gres,
                         const crane::grpc::GresMap& per_node) {
  doc->append(kvp("request_schema_version", 1),
              kvp("ntasks", static_cast<int64_t>(ntasks)),
              kvp("gres_per_node",
                  [&](sub_document value) { AppendGres(value, per_node); }));
  if (gres)
    doc->append(
        kvp("gres_req", [&](sub_document value) { AppendGres(value, *gres); }));
}

RequestFields ReadRequestFields(bsoncxx::document::view doc) {
  RequestFields fields;
  if (auto elem = doc["ntasks"]; elem) {
    const auto value = ReadInteger(elem);
    if (value < 0 || value > std::numeric_limits<uint32_t>::max())
      throw std::out_of_range("Historical ntasks is out of range");
    fields.ntasks = static_cast<uint32_t>(value);
  }
  if (auto elem = doc["gres_req"]; elem) {
    fields.gres.emplace();
    for (auto device : elem.get_document().view()) {
      auto& count =
          (*fields.gres->mutable_name_gres_map())[std::string(device.key())];
      const auto value = device.get_document().view();
      count.set_total(ReadInteger(value["total"]));
      for (auto type : value["type_count_map"].get_document().view())
        (*count.mutable_specified())[std::string(type.key())] =
            ReadInteger(type);
    }
  }
  return fields;
}

std::optional<crane::grpc::GresMap> RecoverRequestedGres(
    const crane::grpc::GresMap& per_node, uint32_t nodes,
    const crane::grpc::GresMap* snapshot) {
  if (snapshot) return *snapshot;
  if (nodes == 0 && !per_node.name_gres_map().empty()) return std::nullopt;
  auto total = per_node;
  for (auto& [name, count] : *total.mutable_name_gres_map()) {
    count.set_total(count.total() * nodes);
    for (auto& [type, value] : *count.mutable_specified()) value *= nodes;
  }
  return total;
}
}  // namespace Ctld::accounting

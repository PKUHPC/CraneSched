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

#include "EndpointRendezvous.h"

#include <algorithm>
#include <charconv>

namespace mpi {
EndpointRendezvous::EndpointRendezvous(uint32_t job_id, uint32_t step_id,
                                       crane::grpc::mpi::Protocol protocol,
                                       std::vector<std::string> nodes)
    : m_job_id_(job_id),
      m_step_id_(step_id),
      m_protocol_(protocol),
      m_nodes_(std::move(nodes)) {
  m_addresses_.reserve(m_nodes_.size());
}

std::expected<std::optional<crane::grpc::mpi::ReceiveMpiEndpointsRequest>,
              std::string>
EndpointRendezvous::Accept(
    const crane::grpc::mpi::BroadcastMpiEndpointRequest& request) {
  const auto& endpoint = request.endpoint();
  if (request.job_id() != m_job_id_ || request.step_id() != m_step_id_ ||
      request.protocol() != m_protocol_ || m_nodes_.empty() ||
      !std::ranges::equal(request.craned_ids(), m_nodes_) ||
      std::ranges::find(m_nodes_, endpoint.craned_id()) == m_nodes_.end())
    return std::unexpected(
        "MPI endpoint does not match the live step allocation");
  if (endpoint.address().empty()) return std::unexpected("empty MPI endpoint");
  if (m_protocol_ == crane::grpc::mpi::PROTOCOL_PMI) {
    uint32_t port;
    const auto& address = endpoint.address();
    auto [end, error] =
        std::from_chars(address.data(), address.data() + address.size(), port);
    if (error != std::errc{} || end != address.data() + address.size() ||
        port == 0 || port > 65535)
      return std::unexpected("invalid PMI TCP endpoint");
  }
  auto [it, inserted] =
      m_addresses_.try_emplace(endpoint.craned_id(), endpoint.address());
  if (!inserted && it->second != endpoint.address())
    return std::unexpected("conflicting MPI endpoint announcement");
  if (m_addresses_.size() != m_nodes_.size()) return std::nullopt;
  crane::grpc::mpi::ReceiveMpiEndpointsRequest snapshot;
  snapshot.set_job_id(m_job_id_);
  snapshot.set_step_id(m_step_id_);
  snapshot.set_protocol(m_protocol_);
  snapshot.mutable_endpoints()->Reserve(m_nodes_.size());
  for (const auto& node : m_nodes_) {
    auto* peer = snapshot.add_endpoints();
    peer->set_craned_id(node);
    peer->set_address(m_addresses_.at(node));
  }
  return snapshot;
}
}  // namespace mpi

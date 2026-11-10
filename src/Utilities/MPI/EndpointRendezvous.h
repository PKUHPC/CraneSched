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
#include <expected>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "protos/mpi/Mpi.pb.h"

namespace mpi {
// Protected by its owner's step-map lock. Never creates or revives a step.
class EndpointRendezvous {
 public:
  EndpointRendezvous(uint32_t job_id, uint32_t step_id,
                     crane::grpc::mpi::Protocol protocol,
                     std::vector<std::string> nodes);
  std::expected<std::optional<crane::grpc::mpi::ReceiveMpiEndpointsRequest>,
                std::string>
  Accept(const crane::grpc::mpi::BroadcastMpiEndpointRequest& request);

 private:
  uint32_t m_job_id_;
  uint32_t m_step_id_;
  crane::grpc::mpi::Protocol m_protocol_;
  std::vector<std::string> m_nodes_;
  std::unordered_map<std::string, std::string> m_addresses_;
};
}  // namespace mpi

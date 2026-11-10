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

#include "Runtime.h"
#include "protos/Crane.grpc.pb.h"
#include "protos/mpi/Mpi.pb.h"

namespace mpi {

// Scheduler control for one step; independent of the application protocol.
class StepControlClient {
 public:
  StepControlClient(job_id_t job_id, step_id_t step_id, CranedId craned_id,
                    std::vector<CranedId> nodes,
                    crane::grpc::mpi::Protocol protocol);
  void InitChannelAndStub(const std::string& endpoint);
  bool ReportEndpoint(std::string_view address);
  void TerminateSteps();

 private:
  job_id_t m_job_id_;
  step_id_t m_step_id_;
  CranedId m_craned_id_;
  std::vector<CranedId> m_nodes_;
  crane::grpc::mpi::Protocol m_protocol_;
  std::unique_ptr<crane::grpc::Craned::Stub> m_stub_;
};
}  // namespace mpi

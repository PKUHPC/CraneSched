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

#include "Collective/CollectiveManager.h"
#include "Modex/ModexManager.h"
#include "PmixAsyncServer.h"
#include "StepControlClient.h"
#include "Types.h"
#include "grpcpp/server.h"
#include "protos/mpi/Pmix.grpc.pb.h"
#include "protos/mpi/Pmix.pb.h"

namespace pmix {

using grpc::Channel;
using grpc::Server;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;

using crane::grpc::mpi::pmix::Pmix;

class PmixGrpcServiceImpl final : public Pmix::CallbackService {
 public:
  PmixGrpcServiceImpl(ModexManager* dmodex_mgr, CollectiveManager* pmix_state)
      : m_modex_manager_(dmodex_mgr), m_collective_manager_(pmix_state) {}
#ifdef HAVE_PMIX
  grpc::ServerUnaryReactor* SendPmixRingMsg(
      grpc::CallbackServerContext* context,
      const ::crane::grpc::mpi::pmix::SendPmixRingMsgReq* request,
      crane::grpc::mpi::pmix::SendPmixRingMsgReply* response) override;

  grpc::ServerUnaryReactor* PmixTreeUpwardForward(
      grpc::CallbackServerContext* context,
      const crane::grpc::mpi::pmix::PmixTreeUpwardForwardReq* request,
      crane::grpc::mpi::pmix::PmixTreeUpwardForwardReply* response) override;

  grpc::ServerUnaryReactor* PmixTreeDownwardForward(
      grpc::CallbackServerContext* context,
      const crane::grpc::mpi::pmix::PmixTreeDownwardForwardReq* request,
      crane::grpc::mpi::pmix::PmixTreeDownwardForwardReply* response) override;

  grpc::ServerUnaryReactor* PmixDModexRequest(
      grpc::CallbackServerContext* context,
      const crane::grpc::mpi::pmix::PmixDModexRequestReq* request,
      crane::grpc::mpi::pmix::PmixDModexRequestReply* response) override;

  grpc::ServerUnaryReactor* PmixDModexResponse(
      grpc::CallbackServerContext* context,
      const crane::grpc::mpi::pmix::PmixDModexResponseReq* request,
      crane::grpc::mpi::pmix::PmixDModexResponseReply* response) override;
#endif

 private:
  ModexManager* m_modex_manager_;
  CollectiveManager* m_collective_manager_;
};

class PmixGrpcServer : public PmixAsyncServer {
 public:
  explicit PmixGrpcServer(ModexManager* dmodex_mgr,
                          CollectiveManager* pmix_state,
                          mpi::StepControlClient* craned_client)
      : m_modex_manager_(dmodex_mgr),
        m_collective_manager_(pmix_state),
        m_step_client_(craned_client) {}

  bool Init(const Config& config) override;

  void Shutdown() override { m_server_->Shutdown(); }

  void Wait() override { m_server_->Wait(); }

 private:
  ModexManager* m_modex_manager_;
  CollectiveManager* m_collective_manager_;
  mpi::StepControlClient* m_step_client_;

  std::unique_ptr<PmixGrpcServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  friend class PmixGrpcServiceImpl;
};

}  // namespace pmix
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

#include "PmixASyncServer.h"
#include "PmixCommon.h"
#include "grpcpp/server.h"
#include "protos/Pmix.grpc.pb.h"
#include "protos/Pmix.pb.h"

namespace pmix {

using grpc::Channel;
using grpc::Server;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;

using crane::grpc::pmix::Pmix;

class PmixGrpcServiceImpl final : public Pmix::CallbackService {
public:
  PmixGrpcServiceImpl() = default;

  grpc::ServerUnaryReactor* SendPmixRingMsg(
    grpc::CallbackServerContext* context, const ::crane::grpc::pmix::SendPmixRingMsgReq* request,
    crane::grpc::pmix::SendPmixRingMsgReply *response) override;

  grpc::ServerUnaryReactor* PmixTreeUpwardForward(
    grpc::CallbackServerContext* context, const crane::grpc::pmix::PmixTreeUpwardForwardReq *request,
    crane::grpc::pmix::PmixTreeUpwardForwardReply *response) override;

  grpc::ServerUnaryReactor* PmixTreeDownwardForward(
    grpc::CallbackServerContext* context, const crane::grpc::pmix::PmixTreeDownwardForwardReq* request,
    crane::grpc::pmix::PmixTreeDownwardForwardReply* response) override;

  grpc::ServerUnaryReactor* PmixDModexRequest(
    grpc::CallbackServerContext* context, const crane::grpc::pmix::PmixDModexRequestReq* request,
    crane::grpc::pmix::PmixDModexRequestReply* response) override;

  grpc::ServerUnaryReactor* PmixDModexResponse(
    grpc::CallbackServerContext* context, const crane::grpc::pmix::PmixDModexResponseReq* request,
    crane::grpc::pmix::PmixDModexResponseReply* response) override;
};

class PmixGrpcServer: public PmixASyncServer {
public:
  explicit PmixGrpcServer() = default;

  bool Init(const Config& config) override;

  void Shutdown() override { m_server_->Shutdown(); }

  void Wait() override { m_server_->Wait(); }

private:
  std::unique_ptr<PmixGrpcServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  friend class PmixASyncServiceImpl;
};

} // namespace pmix
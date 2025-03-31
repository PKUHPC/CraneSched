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

#include "CranedPublicDefs.h"
// Precompiled header comes first.

#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"


namespace Craned {

using grpc::Channel;
using grpc::Server;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;

using crane::grpc::Craned;

class CranedASyncServiceImpl final : public Craned::CallbackService {
public:
  CranedASyncServiceImpl() = default;

  grpc::ServerUnaryReactor* SendPmixRingMsg(
    grpc::CallbackServerContext* context, const ::crane::grpc::SendPmixRingMsgReq* request,
    crane::grpc::SendPmixRingMsgReply *response) override;

  grpc::ServerUnaryReactor* PmixTreeUpwardForward(
    grpc::CallbackServerContext* context, const crane::grpc::PmixTreeUpwardForwardReq *request,
    crane::grpc::PmixTreeUpwardForwardReply *response) override;

  grpc::ServerUnaryReactor* PmixTreeDownwardForward(
    grpc::CallbackServerContext* context, const crane::grpc::PmixTreeDownwardForwardReq* request,
    crane::grpc::PmixTreeDownwardForwardReply* response) override;

  grpc::ServerUnaryReactor* PmixDModexRequest(
    grpc::CallbackServerContext* context, const crane::grpc::PmixDModexRequestReq* request,
    crane::grpc::PmixDModexRequestReply* response) override;

  grpc::ServerUnaryReactor* PmixDModexResponse(
    grpc::CallbackServerContext* context, const crane::grpc::PmixDModexResponseReq* request,
    crane::grpc::PmixDModexResponseReply* response) override;
};

class CranedASyncServer {
public:
  explicit CranedASyncServer(const Config::CranedListenConf &listen_conf);

  inline void Shutdown() { m_server_->Shutdown(); }

  inline void Wait() { m_server_->Wait(); }

private:
  std::unique_ptr<CranedASyncServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  friend class CranedASyncServiceImpl;
};

} // namespace Craned

inline std::unique_ptr<Craned::CranedASyncServer> g_a_sync_server;
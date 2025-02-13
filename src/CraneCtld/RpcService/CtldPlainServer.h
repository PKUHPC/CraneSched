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

#include "../CtldPublicDefs.h"
// Precompiled header comes first!

#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"

namespace Ctld {

using crane::grpc::Craned;
using grpc::Channel;
using grpc::Server;

class CtldPlainServer;

class CraneCtldPlainServiceImpl final
    : public crane::grpc::CraneCtldPlain::Service {
 public:
  explicit CraneCtldPlainServiceImpl(CtldPlainServer *server)
      : m_ctld_plain_server_(server) {}

  grpc::Status QueryTasksInfo(
      grpc::ServerContext *context,
      const crane::grpc::QueryTasksInfoRequest *request,
      crane::grpc::QueryTasksInfoReply *response) override;

  grpc::Status QueryCranedInfo(
      grpc::ServerContext *context,
      const crane::grpc::QueryCranedInfoRequest *request,
      crane::grpc::QueryCranedInfoReply *response) override;

  grpc::Status QueryPartitionInfo(
      grpc::ServerContext *context,
      const crane::grpc::QueryPartitionInfoRequest *request,
      crane::grpc::QueryPartitionInfoReply *response) override;

  grpc::Status QueryClusterInfo(
      grpc::ServerContext *context,
      const crane::grpc::QueryClusterInfoRequest *request,
      crane::grpc::QueryClusterInfoReply *response) override;

  grpc::Status SignUserCertificate(
      grpc::ServerContext *context,
      const crane::grpc::SignUserCertificateRequest *request,
      crane::grpc::SignUserCertificateResponse *response) override;

 private:
  CtldPlainServer *m_ctld_plain_server_;
};

/***
 * Note: There should be only ONE instance of CtldServer!!!!
 */
class CtldPlainServer {
 public:
  /***
   * User must make sure that this constructor is called only once!
   * @param listen_address The "[Address]:[Port]" of SignServer.
   */
  explicit CtldPlainServer(const Config::CraneCtldListenConf &listen_conf);

  inline void Wait() { m_server_->Wait(); }

  void Shutdown();

 private:
  std::unique_ptr<CraneCtldPlainServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  friend class CraneCtldPlainServiceImpl;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::CtldPlainServer> g_ctld_plain_server;
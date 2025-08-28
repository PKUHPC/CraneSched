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

class CranedForPamServiceImpl : public crane::grpc::CranedForPam::Service {
 public:
  CranedForPamServiceImpl() = default;

  grpc::Status QueryStepFromPortForward(
      grpc::ServerContext *context,
      const crane::grpc::QueryStepFromPortForwardRequest *request,
      crane::grpc::QueryStepFromPortForwardReply *response) override;

  grpc::Status MigrateSshProcToCgroup(
      grpc::ServerContext *context,
      const crane::grpc::MigrateSshProcToCgroupRequest *request,
      crane::grpc::MigrateSshProcToCgroupReply *response) override;

  grpc::Status QuerySshStepEnvVariablesForward(
      grpc::ServerContext *context,
      const ::crane::grpc::QuerySshStepEnvVariablesForwardRequest *request,
      crane::grpc::QuerySshStepEnvVariablesForwardReply *response) override;
};

class CranedForPamServer {
 public:
  explicit CranedForPamServer(
      const Common::Config::CranedListenConf &listen_conf);

  void Shutdown() {
    m_server_->Shutdown(std::chrono::system_clock::now() +
                        std::chrono::seconds(1));
  }

  void Wait() { m_server_->Wait(); }

 private:
  std::unique_ptr<CranedForPamServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  friend class CranedForPamServiceImpl;
};

}  // namespace Craned

inline std::unique_ptr<Craned::CranedForPamServer> g_craned_for_pam_server;
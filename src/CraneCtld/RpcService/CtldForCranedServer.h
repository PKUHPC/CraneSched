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

class CtldForCranedServer;

class CtldForCranedServiceImpl final
    : public crane::grpc::CraneCtldForCraned::Service {
 public:
  explicit CtldForCranedServiceImpl(CtldForCranedServer *server)
      : m_ctld_for_craned_server_(server) {}
  grpc::Status TaskStatusChange(
      grpc::ServerContext *context,
      const crane::grpc::TaskStatusChangeRequest *request,
      crane::grpc::TaskStatusChangeReply *response) override;

  grpc::Status CranedRegister(
      grpc::ServerContext *context,
      const crane::grpc::CranedRegisterRequest *request,
      crane::grpc::CranedRegisterReply *response) override;

 private:
  CtldForCranedServer *m_ctld_for_craned_server_;
};

class CtldForCranedServer {
 public:
  /***
   * User must make sure that this constructor is called only once!
   * @param listen_address The "[Address]:[Port]" of CraneCtld.
   */
  explicit CtldForCranedServer(const Config::CraneCtldListenConf &listen_conf);

  inline void Wait() { m_server_->Wait(); }

  void Shutdown();

 private:
  std::unique_ptr<CtldForCranedServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  friend class CtldForCranedServiceImpl;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::CtldForCranedServer> g_ctld_for_craned_server;
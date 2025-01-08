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

class SignServer;

class SignServiceImpl final : public crane::grpc::SignService::Service {
 public:
  explicit SignServiceImpl(SignServer *server) : m_sign_server_(server) {}

  grpc::Status SignUserCertificate(
      grpc::ServerContext *context,
      const crane::grpc::SignUserCertificateRequest *request,
      crane::grpc::SignUserCertificateResponse *response) override;

 private:
  SignServer *m_sign_server_;
};

/***
 * Note: There should be only ONE instance of CtldServer!!!!
 */
class SignServer {
 public:
  /***
   * User must make sure that this constructor is called only once!
   * @param listen_address The "[Address]:[Port]" of SignServer.
   */
  explicit SignServer(const Config::CraneCtldListenConf &listen_conf);

  inline void Wait() { m_server_->Wait(); }

  void Shutdown();

 private:
  std::unique_ptr<SignServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  friend class SignServiceImpl;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::SignServer> g_sign_server;
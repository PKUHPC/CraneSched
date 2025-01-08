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

#include "CtldForCranedServer.h"

#include <google/protobuf/util/time_util.h>

#include "../CranedMetaContainer.h"
#include "../TaskScheduler.h"
#include "CranedKeeper.h"

namespace Ctld {

grpc::Status CtldForCranedServiceImpl::TaskStatusChange(
    grpc::ServerContext *context,
    const crane::grpc::TaskStatusChangeRequest *request,
    crane::grpc::TaskStatusChangeReply *response) {
  std::optional<std::string> reason;
  if (!request->reason().empty()) reason = request->reason();

  g_task_scheduler->TaskStatusChangeWithReasonAsync(
      request->task_id(), request->craned_id(), request->new_status(),
      request->exit_code(), std::move(reason));
  response->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status CtldForCranedServiceImpl::CranedRegister(
    grpc::ServerContext *context,
    const crane::grpc::CranedRegisterRequest *request,
    crane::grpc::CranedRegisterReply *response) {
  if (!g_meta_container->CheckCranedAllowed(request->craned_id())) {
    response->set_ok(false);
    return grpc::Status::OK;
  }

  bool alive = g_meta_container->CheckCranedOnline(request->craned_id());
  if (!alive) {
    g_craned_keeper->PutNodeIntoUnavailList(request->craned_id());
  }

  response->set_ok(true);
  response->set_already_registered(alive);

  return grpc::Status::OK;
}

void CtldForCranedServer::Shutdown() {
  m_server_->Shutdown(std::chrono::system_clock::now() +
                      std::chrono::seconds(1));
}

CtldForCranedServer::CtldForCranedServer(
    const Config::CraneCtldListenConf &listen_conf) {
  m_service_impl_ = std::make_unique<CtldForCranedServiceImpl>(this);

  grpc::ServerBuilder builder;

  if (g_config.CompressedRpc) ServerBuilderSetCompression(&builder);

  if (listen_conf.UseTls)
    ServerBuilderAddmTcpTlsListeningPort(
        &builder, listen_conf.CraneCtldListenAddr,
        listen_conf.CraneCtldForCranedListenPort,
        listen_conf.TlsCerts.InternalCerts,
        listen_conf.TlsCerts.InternalCaContent);
  else
    ServerBuilderAddTcpInsecureListeningPort(
        &builder, listen_conf.CraneCtldListenAddr,
        listen_conf.CraneCtldForCranedListenPort);

  builder.RegisterService(m_service_impl_.get());
  m_server_ = builder.BuildAndStart();
  if (!m_server_) {
    CRANE_ERROR("Cannot start gRPC server!");
    std::exit(1);
  }
  CRANE_INFO("CraneCtld For Craned Server is listening on {}:{} and Tls is {}",
             listen_conf.CraneCtldListenAddr,
             listen_conf.CraneCtldForCranedListenPort, listen_conf.UseTls);
}

}  // namespace Ctld
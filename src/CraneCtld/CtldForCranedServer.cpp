/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "CtldForCranedServer.h"

#include <google/protobuf/util/time_util.h>

#include "CranedKeeper.h"
#include "CranedMetaContainer.h"
#include "TaskScheduler.h"
#include "crane/String.h"

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

CtldForCranedServer::CtldForCranedServer(const Config::CraneCtldListenConf &listen_conf) {
  m_service_impl_ = std::make_unique<CtldForCranedServiceImpl>(this);

  grpc::ServerBuilder builder;

  if (g_config.CompressedRpc) ServerBuilderSetCompression(&builder);

  if (listen_conf.UseTls) {
    ServerBuilderAddmTcpTlsListeningPort(
        &builder, listen_conf.CraneCtldListenAddr,
        listen_conf.CraneCtldForCranedListenPort, listen_conf.TlsCerts.InternalCerts, listen_conf.TlsCerts.InternalCaContent);
  } else {
        ServerBuilderAddTcpInsecureListeningPort(&builder,
                                             listen_conf.CraneCtldListenAddr,
                                             listen_conf.CraneCtldForCranedListenPort);
  }

  builder.RegisterService(m_service_impl_.get());
  m_server_ = builder.BuildAndStart();
  if (!m_server_) {
    CRANE_ERROR("Cannot start gRPC server!");
    std::exit(1);
  }
  CRANE_INFO("CraneCtld For Craned Server is listening on {}:{} and Tls is {}",
             listen_conf.CraneCtldListenAddr, listen_conf.CraneCtldForCranedListenPort,
             listen_conf.UseTls);
}

}  // namespace Ctld
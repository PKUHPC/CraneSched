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

#include "CranedASyncServer.h"

#include "Pmix/Pmix.h"
#include "Pmix/PmixColl.h"
#include "Pmix/PmixDModex.h"
#include "TaskManager.h"

namespace Craned {

grpc::ServerUnaryReactor* CranedASyncServiceImpl::SendPmixRingMsg(
    grpc::CallbackServerContext* context, const ::crane::grpc::SendPmixRingMsgReq* request,
    crane::grpc::SendPmixRingMsgReply *response) {


  auto* reactor = context->DefaultReactor();

  std::vector<pmix_proc_t> procs;

  for (const auto &proc : request->pmix_procs()) {
    pmix_proc_t tmp_proc;
    auto& nspace_str = proc.nspace();
    snprintf(tmp_proc.nspace, sizeof(tmp_proc.nspace), "%s", nspace_str.c_str());
    tmp_proc.rank = proc.rank();
    procs.emplace_back(tmp_proc);
  }

  std::shared_ptr<pmix::Coll> coll =
      g_pmix_state->PmixStateCollGet(
          pmix::CollType::FENCE_RING, procs, procs.size());

  if (!coll) {
    response->set_ok(false);
    reactor->Finish(Status::OK);
    return reactor;
  }

  auto result = coll->ProcessRingRequest(request->pmix_ring_msg_hdr(), request->msg());
  response->set_ok(result);


  reactor->Finish(Status::OK);
  return reactor;
}

grpc::ServerUnaryReactor* CranedASyncServiceImpl::PmixTreeUpwardForward(
  grpc::CallbackServerContext* context, const crane::grpc::PmixTreeUpwardForwardReq *request,
  crane::grpc::PmixTreeUpwardForwardReply *response) {

  auto* reactor = context->DefaultReactor();

  std::vector<pmix_proc_t> procs;
  for (const auto &proc : request->pmix_procs()) {
    pmix_proc_t tmp_proc;
    auto& nspace_str = proc.nspace();
    snprintf(tmp_proc.nspace, sizeof(tmp_proc.nspace), "%s", nspace_str.c_str());
    tmp_proc.rank = proc.rank();
    procs.emplace_back(tmp_proc);
  }

  std::shared_ptr<pmix::Coll> coll =
      g_pmix_state->PmixStateCollGet(
          pmix::CollType::FENCE_TREE, procs, procs.size());

  if (!coll) {
    response->set_ok(false);
    reactor->Finish(Status::OK);
    return reactor;
  }

  auto result = coll->PmixCollTreeChild(request->peer_host(), request->seq(), request->msg());
  response->set_ok(result);

  reactor->Finish(Status::OK);
  return reactor;
}

grpc::ServerUnaryReactor* CranedASyncServiceImpl::PmixTreeDownwardForward(
  grpc::CallbackServerContext* context, const crane::grpc::PmixTreeDownwardForwardReq* request,
  crane::grpc::PmixTreeDownwardForwardReply* response) {

  auto* reactor = context->DefaultReactor();

  std::vector<pmix_proc_t> procs;
  for (const auto &proc : request->pmix_procs()) {
    pmix_proc_t tmp_proc;
    auto& nspace_str = proc.nspace();
    snprintf(tmp_proc.nspace, sizeof(tmp_proc.nspace), "%s", nspace_str.c_str());
    tmp_proc.rank = proc.rank();
    procs.emplace_back(tmp_proc);
  }

  std::shared_ptr<pmix::Coll> coll =
    g_pmix_state->PmixStateCollGet(
        pmix::CollType::FENCE_TREE, procs, procs.size());

  if (!coll) {
    response->set_ok(false);
    reactor->Finish(Status::OK);
    return reactor;
  }

  auto result = coll->PmixCollTreeParent(request->peer_host(), request->seq(), request->msg());
  response->set_ok(result);

  reactor->Finish(Status::OK);
  return reactor;
}


grpc::ServerUnaryReactor* CranedASyncServiceImpl::PmixDModexRequest(
    grpc::CallbackServerContext* context, const crane::grpc::PmixDModexRequestReq* request,
    crane::grpc::PmixDModexRequestReply* response) {

  auto* reactor = context->DefaultReactor();

  pmix_proc_t proc;
  auto& nspace_str = request->pmix_proc().nspace();
  snprintf(proc.nspace, sizeof(proc.nspace), "%s", nspace_str.c_str());
  proc.rank = request->pmix_proc().rank();

  g_dmodex_req_manager->PmixProcessRequest(request->seq_num(),
                                           request->craned_id(), proc,
                                           request->local_namespace());


  reactor->Finish(Status::OK);
  return reactor;
}

grpc::ServerUnaryReactor* CranedASyncServiceImpl::PmixDModexResponse(
    grpc::CallbackServerContext* context, const crane::grpc::PmixDModexResponseReq* request,
    crane::grpc::PmixDModexResponseReply* response) {
  auto* reactor = context->DefaultReactor();

  g_dmodex_req_manager->PmixProcessResponse(request->seq_num(), request->craned_id(), request->data(), request->status());

  reactor->Finish(Status::OK);
  return reactor;
}

CranedASyncServer::CranedASyncServer(const Config::CranedListenConf &listen_conf) {
  m_service_impl_ = std::make_unique<CranedASyncServiceImpl>();

  grpc::ServerBuilder builder;
  ServerBuilderSetKeepAliveArgs(&builder);

  if (g_config.CompressedRpc) ServerBuilderSetCompression(&builder);

  std::string craned_listen_addr = listen_conf.CranedListenAddr;
  if (listen_conf.UseTls) {
    ServerBuilderAddTcpTlsListeningPort(&builder, craned_listen_addr,
                                        kCranedAsyncDefaultPort,
                                        listen_conf.TlsCerts);
  } else {
    ServerBuilderAddTcpInsecureListeningPort(&builder, craned_listen_addr,
                                             kCranedAsyncDefaultPort);
  }

  builder.RegisterService(m_service_impl_.get());

  m_server_ = builder.BuildAndStart();
  CRANE_INFO("CranedASync is listening on [{}:{}]", craned_listen_addr, kCranedAsyncDefaultPort);
}

} // namespace Craned
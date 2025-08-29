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

#include "PmixGrpcServer.h"

#include "Pmix.h"
#include "PmixColl.h"
#include "PmixDModex.h"
#include "crane/GrpcHelper.h"

namespace pmix {

grpc::ServerUnaryReactor* PmixGrpcServiceImpl::SendPmixRingMsg(
    grpc::CallbackServerContext* context, const ::crane::grpc::pmix::SendPmixRingMsgReq* request,
    crane::grpc::pmix::SendPmixRingMsgReply *response) {

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

grpc::ServerUnaryReactor* PmixGrpcServiceImpl::PmixTreeUpwardForward(
  grpc::CallbackServerContext* context, const crane::grpc::pmix::PmixTreeUpwardForwardReq *request,
  crane::grpc::pmix::PmixTreeUpwardForwardReply *response) {

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

grpc::ServerUnaryReactor* PmixGrpcServiceImpl::PmixTreeDownwardForward(
  grpc::CallbackServerContext* context, const crane::grpc::pmix::PmixTreeDownwardForwardReq* request,
  crane::grpc::pmix::PmixTreeDownwardForwardReply* response) {

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


grpc::ServerUnaryReactor* PmixGrpcServiceImpl::PmixDModexRequest(
    grpc::CallbackServerContext* context, const crane::grpc::pmix::PmixDModexRequestReq* request,
    crane::grpc::pmix::PmixDModexRequestReply* response) {

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

grpc::ServerUnaryReactor* PmixGrpcServiceImpl::PmixDModexResponse(
    grpc::CallbackServerContext* context, const crane::grpc::pmix::PmixDModexResponseReq* request,
    crane::grpc::pmix::PmixDModexResponseReply* response) {
  auto* reactor = context->DefaultReactor();

  g_dmodex_req_manager->PmixProcessResponse(request->seq_num(), request->craned_id(), request->data(), request->status());

  reactor->Finish(Status::OK);
  return reactor;
}


bool PmixGrpcServer::Init(const Config& config) {
  m_service_impl_ = std::make_unique<PmixGrpcServiceImpl>();

  grpc::ServerBuilder builder;
  ServerBuilderSetKeepAliveArgs(&builder);

  if (config.CompressedRpc) ServerBuilderSetCompression(&builder);

  std::string listen_addr = "0.0.0.0";
  int selected_port = 0;
  if (config.UseTls)
    // TODO:
    ServerBuilderAddTcpInsecureListeningRandomPort(&builder, listen_addr, &selected_port);
  else
    ServerBuilderAddTcpInsecureListeningRandomPort(&builder, listen_addr, &selected_port);

  builder.RegisterService(m_service_impl_.get());

  m_server_ = builder.BuildAndStart();
  if (!m_server_ || selected_port == 0) {
    CRANE_ERROR("Failed to bind port for PMIx direct connection");
    g_pmix_server->GetCranedClient()->TerminateTasks();
    return false;
  }

  CRANE_INFO("PMIx direct connection is listening on [{}:{}]", listen_addr, selected_port);

  std::thread([selected_port]() {
    g_pmix_server->GetCranedClient()->BroadcastPmixPort(std::to_string(selected_port));
  }).detach();

  return true;
}

} // namespace pmix
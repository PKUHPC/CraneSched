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

#include "CtldPlainServer.h"

#include "../AccountManager.h"
#include "../CranedMetaContainer.h"
#include "../EmbeddedDbClient.h"
#include "../TaskScheduler.h"
#include "crane/VaultClient.h"

namespace Ctld {

grpc::Status CraneCtldPlainServiceImpl::QueryCranedInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryCranedInfoRequest *request,
    crane::grpc::QueryCranedInfoReply *response) {
  if (request->craned_name().empty()) {
    *response = g_meta_container->QueryAllCranedInfo();
  } else {
    *response = g_meta_container->QueryCranedInfo(request->craned_name());
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldPlainServiceImpl::QueryPartitionInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryPartitionInfoRequest *request,
    crane::grpc::QueryPartitionInfoReply *response) {
  if (request->partition_name().empty()) {
    *response = g_meta_container->QueryAllPartitionInfo();
  } else {
    *response = g_meta_container->QueryPartitionInfo(request->partition_name());
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldPlainServiceImpl::QueryTasksInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryTasksInfoRequest *request,
    crane::grpc::QueryTasksInfoReply *response) {
  // Query tasks in RAM
  g_task_scheduler->QueryTasksInRam(request, response);

  size_t num_limit = request->num_limit() == 0 ? kDefaultQueryTaskNumLimit
                                               : request->num_limit();
  if (!request->filter_task_ids().empty())
    num_limit = std::min((size_t)request->filter_task_ids_size(), num_limit);

  auto *task_list = response->mutable_task_info_list();

  auto sort_and_truncate = [](auto *task_list, size_t limit) -> void {
    std::sort(
        task_list->begin(), task_list->end(),
        [](const crane::grpc::TaskInfo &a, const crane::grpc::TaskInfo &b) {
          return (a.status() == b.status()) ? (a.priority() > b.priority())
                                            : (a.status() < b.status());
        });

    if (task_list->size() > limit)
      task_list->DeleteSubrange(limit, task_list->size());
  };

  if (task_list->size() >= num_limit ||
      !request->option_include_completed_tasks()) {
    sort_and_truncate(task_list, num_limit);
    response->set_ok(true);
    return grpc::Status::OK;
  }

  // Query completed tasks in Mongodb
  // (only for cacct, which sets `option_include_completed_tasks` to true)
  if (!g_db_client->FetchJobRecords(request, response,
                                    num_limit - task_list->size())) {
    CRANE_ERROR("Failed to call g_db_client->FetchJobRecords");
    return grpc::Status::OK;
  }

  sort_and_truncate(task_list, num_limit);
  response->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status CraneCtldPlainServiceImpl::QueryClusterInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryClusterInfoRequest *request,
    crane::grpc::QueryClusterInfoReply *response) {
  *response = g_meta_container->QueryClusterInfo(*request);
  return grpc::Status::OK;
}

grpc::Status CraneCtldPlainServiceImpl::SignUserCertificate(
    grpc::ServerContext *context,
    const crane::grpc::SignUserCertificateRequest *request,
    crane::grpc::SignUserCertificateResponse *response) {
  if (!g_config.VaultConf.AllowedNodes.empty()) {
    std::string client_address = context->peer();
    std::vector<std::string> str_list = absl::StrSplit(client_address, ":");
    std::string hostname;
    bool resolve_result = false;
    if (str_list[0] == "ipv4") {
      ipv4_t addr;
      if (!crane::StrToIpv4(str_list[1], &addr)) {
        CRANE_ERROR("Failed to parse ipv4 address: {}", str_list[1]);
      } else {
        resolve_result = crane::ResolveHostnameFromIpv4(addr, &hostname);
      }
    } else {
      ipv6_t addr;
      if (!crane::StrToIpv6(str_list[1], &addr)) {
        CRANE_ERROR("Failed to parse ipv6 address: {}", str_list[1]);
      } else {
        resolve_result = crane::ResolveHostnameFromIpv6(addr, &hostname);
      }
    }

    std::vector<std::string> name_list = absl::StrSplit(hostname, ".");
    if (!resolve_result ||
        !g_config.VaultConf.AllowedNodes.contains(name_list[0])) {
      response->set_ok(false);
      response->set_reason(crane::grpc::ErrCode::ERR_PERMISSION_USER);
      return grpc::Status::OK;
    }
  }

  auto result = g_account_manager->SignUserCertificate(
      request->uid(), request->csr_content(), request->alt_names());
  if (!result) {
    response->set_ok(false);
    response->set_reason(result.error());
  } else {
    response->set_ok(true);
    response->set_certificate(result.value());
    response->set_external_certificate(
        g_config.VaultConf.ExternalCerts.ServerCertContent);
  }

  return grpc::Status::OK;
}

void CtldPlainServer::Shutdown() {
  m_server_->Shutdown(std::chrono::system_clock::now() +
                      std::chrono::seconds(1));
}

CtldPlainServer::CtldPlainServer(
    const Config::CraneCtldListenConf &listen_conf) {
  m_service_impl_ = std::make_unique<CraneCtldPlainServiceImpl>(this);

  grpc::ServerBuilder builder;

  if (g_config.CompressedRpc) ServerBuilderSetCompression(&builder);

  ServerBuilderAddTcpInsecureListeningPort(
      &builder, listen_conf.CraneCtldListenAddr,
      listen_conf.CraneCtldPlainListenPort);

  builder.RegisterService(m_service_impl_.get());
  m_server_ = builder.BuildAndStart();
  if (!m_server_) {
    CRANE_ERROR("Cannot start gRPC server!");
    std::exit(1);
  }
  CRANE_INFO("CraneCtld For Ctld Plain Server is listening on {}:{}",
             listen_conf.CraneCtldListenAddr,
             listen_conf.CraneCtldPlainListenPort);
}

}  // namespace Ctld
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

#include "SignServer.h"

#include "../AccountManager.h"

namespace Ctld {

grpc::Status SignServiceImpl::SignUserCertificate(
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
        g_config.VaultConf.ExternalCACerts.CACertContent);
  }

  return grpc::Status::OK;
}

void SignServer::Shutdown() {
  m_server_->Shutdown(std::chrono::system_clock::now() +
                      std::chrono::seconds(1));
}

SignServer::SignServer(const Config::CraneCtldListenConf &listen_conf) {
  m_service_impl_ = std::make_unique<SignServiceImpl>(this);

  grpc::ServerBuilder builder;

  if (g_config.CompressedRpc) ServerBuilderSetCompression(&builder);

  ServerBuilderAddTcpInsecureListeningPort(
      &builder, listen_conf.CraneCtldListenAddr,
      listen_conf.CraneCtldForSignListenPort);

  builder.RegisterService(m_service_impl_.get());
  m_server_ = builder.BuildAndStart();
  if (!m_server_) {
    CRANE_ERROR("Cannot start gRPC server!");
    std::exit(1);
  }
  CRANE_INFO("CraneCtld For Sign Server is listening on {}:{} and Tls is {}",
             listen_conf.CraneCtldListenAddr,
             listen_conf.CraneCtldForSignListenPort, listen_conf.UseTls);
}

}  // namespace Ctld
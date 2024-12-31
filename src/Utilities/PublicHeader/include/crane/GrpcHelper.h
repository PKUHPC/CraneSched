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

#include <grpc++/grpc++.h>
#include <grpcpp/security/auth_metadata_processor.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/support/status.h>
#include <spdlog/fmt/bundled/format.h>

#include <expected>

#include "crane/Jwt.h"
#include "crane/Network.h"
#include "crane/String.h"

struct ServerCertificateConfig {
  std::string ServerCertFilePath;
  std::string ServerCertContent;
  std::string ServerKeyFilePath;
  std::string ServerKeyContent;
};

struct ClientCertificateConfig {
  std::string ClientCertFilePath;
  std::string ClientCertContent;
};

struct CACertificateConfig {
  std::string CACertFilePath;
  std::string CACertContent;
};

// class JwtAuthProcessor : public grpc::AuthMetadataProcessor {
//  public:
//   JwtAuthProcessor(std::string secret) : jwt_secret_(secret) {}
//   grpc::Status Process(const InputMetadata& auth_metadata,
//                        grpc::AuthContext* context,
//                        OutputMetadata* consumed_auth_metadata,
//                        OutputMetadata* response_metadata) override;

//  private:
//   std::string jwt_secret_;
// };

class JwtAuthInterceptor : public grpc::experimental::Interceptor {
 public:
  explicit JwtAuthInterceptor(grpc::experimental::ServerRpcInfo* info,
                              std::string secret)
      : info_(info), jwt_secret_(secret) {}

  void Intercept(grpc::experimental::InterceptorBatchMethods* methods) override;

 private:
  grpc::experimental::ServerRpcInfo* info_;
  std::string jwt_secret_;
};

class JwtAuthInterceptorFactory
    : public grpc::experimental::ServerInterceptorFactoryInterface {
 public:
  JwtAuthInterceptorFactory(std::string secret) : jwt_secret_(secret) {}
  grpc::experimental::Interceptor* CreateServerInterceptor(
      grpc::experimental::ServerRpcInfo* info) override {
    return new JwtAuthInterceptor(info, jwt_secret_);
  }

 private:
  std::string jwt_secret_;
};

std::expected<uint32_t, bool> ExtractUIDFromCert(
    const grpc::ServerContext* context);

void ServerBuilderSetCompression(grpc::ServerBuilder* builder);

void ServerBuilderSetKeepAliveArgs(grpc::ServerBuilder* builder);

void ServerBuilderAddUnixInsecureListeningPort(grpc::ServerBuilder* builder,
                                               const std::string& address);

void ServerBuilderAddTcpInsecureListeningPort(grpc::ServerBuilder* builder,
                                              const std::string& address,
                                              const std::string& port);

void ServerBuilderAddmTcpTlsListeningPort(grpc::ServerBuilder* builder,
                                          const std::string& address,
                                          const std::string& port,
                                          const ServerCertificateConfig& certs,
                                          const std::string& pem_root_cert);

void ServerBuilderAddTcpTlsListeningPort(grpc::ServerBuilder* builder,
                                         const std::string& address,
                                         const std::string& port,
                                         const ServerCertificateConfig& certs);

void SetGrpcClientKeepAliveChannelArgs(grpc::ChannelArguments* args);

void SetTlsHostnameOverride(grpc::ChannelArguments* args,
                            const std::string& hostname,
                            const std::string& domainSuffix);

std::shared_ptr<grpc::Channel> CreateUnixInsecureChannel(
    const std::string& socket_addr);

std::shared_ptr<grpc::Channel> CreateTcpInsecureChannel(
    const std::string& address, const std::string& port);

std::shared_ptr<grpc::Channel> CreateTcpInsecureCustomChannel(
    const std::string& address, const std::string& port,
    const grpc::ChannelArguments& args);

std::shared_ptr<grpc::Channel> CreateTcpTlsCustomChannelByIp(
    const std::string& ip, const std::string& port,
    const ServerCertificateConfig& certs,
    const ClientCertificateConfig& clientcerts,
    const grpc::ChannelArguments& args);

std::shared_ptr<grpc::Channel> CreateTcpTlsChannelByHostname(
    const std::string& hostname, const std::string& port,
    const ServerCertificateConfig& certs,
    const ClientCertificateConfig& clientcerts,
    const std::string& domainSuffix);

std::shared_ptr<grpc::Channel> CreateTcpTlsCustomChannelByHostname(
    const std::string& hostname, const std::string& port,
    const ServerCertificateConfig& certs,
    const ClientCertificateConfig& clientcerts, const std::string& domainSuffix,
    const grpc::ChannelArguments& args);

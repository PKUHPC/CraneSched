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
#include <spdlog/fmt/bundled/format.h>

#include "crane/Network.h"

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

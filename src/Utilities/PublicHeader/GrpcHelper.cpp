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

#include "crane/GrpcHelper.h"

void ServerBuilderSetCompression(grpc::ServerBuilder* builder) {
  builder->SetDefaultCompressionAlgorithm(GRPC_COMPRESS_GZIP);
}

void ServerBuilderSetKeepAliveArgs(grpc::ServerBuilder* builder) {
  builder->AddChannelArgument(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA,
                              0 /*no limit*/);
  builder->AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS,
                              1 /*true*/);
  builder->AddChannelArgument(
      GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS,
      kCranedGrpcServerPingRecvMinIntervalSec * 1000 /*ms*/);
  builder->AddChannelArgument(GRPC_ARG_HTTP2_MAX_PING_STRIKES,
                              0 /* unlimited */);
}

void ServerBuilderAddUnixInsecureListeningPort(grpc::ServerBuilder* builder,
                                               const std::string& address) {
  builder->AddListeningPort(address, grpc::InsecureServerCredentials());
}

void ServerBuilderAddTcpInsecureListeningPort(grpc::ServerBuilder* builder,
                                              const std::string& address,
                                              const std::string& port) {
  std::string listen_addr_port = fmt::format("{}:{}", address, port);
  builder->AddListeningPort(listen_addr_port,
                            grpc::InsecureServerCredentials());
}

void ServerBuilderAddTcpTlsListeningPort(grpc::ServerBuilder* builder,
                                         const std::string& address,
                                         const std::string& port,
                                         const TlsCertificates& certs) {
  std::string listen_addr_port = fmt::format("{}:{}", address, port);

  grpc::SslServerCredentialsOptions::PemKeyCertPair pem_key_cert_pair;
  pem_key_cert_pair.cert_chain = certs.ServerCertContent;
  pem_key_cert_pair.private_key = certs.ServerKeyContent;

  grpc::SslServerCredentialsOptions ssl_opts;
  // pem_root_certs is actually the certificate of server side rather than
  // CA certificate. CA certificate is not needed.
  // Since we use the same cert/key pair for both cranectld/craned,
  // pem_root_certs is set to the same certificate.
  ssl_opts.pem_root_certs = certs.ServerCertContent;
  ssl_opts.pem_key_cert_pairs.emplace_back(std::move(pem_key_cert_pair));
  ssl_opts.client_certificate_request =
      GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;

  builder->AddListeningPort(listen_addr_port,
                            grpc::SslServerCredentials(ssl_opts));
}

void SetKeepAliveChannelArgs(grpc::ChannelArguments* args) {
  args->SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 1000 /*ms*/);
  args->SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 2 /*s*/ * 1000
               /*ms*/);
  args->SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 30 /*s*/ * 1000 /*ms*/);

  // Sometimes, Craned might crash without cleaning up sockets and
  // the socket will remain ESTABLISHED state even if that craned has died.
  // Open KeepAlive option in case of such situation.
  // See https://grpc.github.io/grpc/cpp/md_doc_keepalive.html
  args->SetInt(GRPC_ARG_KEEPALIVE_TIME_MS,
               kCraneCtldGrpcClientPingSendIntervalSec /*s*/ * 1000 /*ms*/);
  args->SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 10 /*s*/ * 1000 /*ms*/);
  args->SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1 /*true*/);
  args->SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0 /*no limit*/);
}

void SetTlsHostnameOverride(grpc::ChannelArguments* args,
                            const std::string& hostname,
                            const TlsCertificates& certs) {
  args->SetSslTargetNameOverride(
      fmt::format("{}.{}", hostname, certs.DomainSuffix));
}

std::shared_ptr<grpc::Channel> CreateTcpInsecureChannel(
    const std::string& address, const std::string& port) {
  std::string target = fmt::format("{}:{}", address, port);
  return grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
}

std::shared_ptr<grpc::Channel> CreateTcpInsecureCustomChannel(
    const std::string& address, const std::string& port,
    const grpc::ChannelArguments& args) {
  std::string target = fmt::format("{}:{}", address, port);
  return grpc::CreateCustomChannel(target, grpc::InsecureChannelCredentials(),
                                   args);
}

static void SetSslCredOpts(grpc::SslCredentialsOptions* opts,
                           const TlsCertificates& certs) {
  // pem_root_certs is actually the certificate of server side rather than
  // CA certificate. CA certificate is not needed.
  // Since we use the same cert/key pair for both cranectld/craned,
  // pem_root_certs is set to the same certificate.
  opts->pem_root_certs = certs.ServerCertContent;
  opts->pem_cert_chain = certs.ServerCertContent;
  opts->pem_private_key = certs.ServerKeyContent;
}

std::shared_ptr<grpc::Channel> CreateTcpTlsCustomChannelByIp(
    const std::string& ip, const std::string& port,
    const TlsCertificates& certs, const grpc::ChannelArguments& args) {
  grpc::SslCredentialsOptions ssl_opts;
  SetSslCredOpts(&ssl_opts, certs);

  std::string target = fmt::format("{}:{}", ip, port);
  return grpc::CreateCustomChannel(target, grpc::SslCredentials(ssl_opts),
                                   args);
}

std::shared_ptr<grpc::Channel> CreateTcpTlsChannelByHostname(
    const std::string& hostname, const std::string& port,
    const TlsCertificates& certs) {
  grpc::SslCredentialsOptions ssl_opts;
  SetSslCredOpts(&ssl_opts, certs);

  std::string target =
      fmt::format("{}.{}:{}", hostname, certs.DomainSuffix, port);
  return grpc::CreateChannel(target, grpc::SslCredentials(ssl_opts));
}

std::shared_ptr<grpc::Channel> CreateTcpTlsCustomChannelByHostname(
    const std::string& hostname, const std::string& port,
    const TlsCertificates& certs, const grpc::ChannelArguments& args) {
  grpc::SslCredentialsOptions ssl_opts;
  SetSslCredOpts(&ssl_opts, certs);

  std::string target =
      fmt::format("{}.{}:{}", hostname, certs.DomainSuffix, port);
  return grpc::CreateCustomChannel(target, grpc::SslCredentials(ssl_opts),
                                   args);
}

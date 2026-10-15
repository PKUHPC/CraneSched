// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "Security/VaultClient.h"

namespace {

using Ctld::Security::VaultClient;

struct Request {
  std::string method;
  std::string path;
  std::string token;
};

// A minimal Vault HTTP server. Tokens issued by the userpass login are valid
// until the test expires them, like a token that reached its max TTL.
class FakeVault {
 public:
  explicit FakeVault(int64_t token_ttl_seconds = 3600)
      : m_token_ttl_seconds_(token_ttl_seconds) {
    m_listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(m_listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    bind(m_listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    socklen_t len = sizeof(addr);
    getsockname(m_listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
    m_port_ = ntohs(addr.sin_port);
    listen(m_listen_fd_, 16);
    m_thread_ = std::thread([this] { Serve_(); });
  }

  ~FakeVault() {
    m_stop_ = true;
    shutdown(m_listen_fd_, SHUT_RDWR);
    close(m_listen_fd_);
    m_thread_.join();
  }

  int Port() const { return m_port_; }

  void ExpireAllTokens() {
    std::lock_guard lock(m_mtx_);
    m_valid_tokens_.clear();
  }

  std::vector<Request> Requests() const {
    std::lock_guard lock(m_mtx_);
    return m_requests_;
  }

  int Count(const std::string& path_prefix) const {
    std::lock_guard lock(m_mtx_);
    int count = 0;
    for (const auto& req : m_requests_)
      if (req.path.starts_with(path_prefix)) ++count;
    return count;
  }

 private:
  void Serve_() {
    while (!m_stop_) {
      int fd = accept(m_listen_fd_, nullptr, nullptr);
      if (fd < 0) continue;
      Handle_(fd);
      close(fd);
    }
  }

  void Handle_(int fd) {
    std::string data;
    char buf[4096];
    size_t header_end = std::string::npos;
    size_t content_length = 0;
    while (true) {
      ssize_t n = recv(fd, buf, sizeof(buf), 0);
      if (n <= 0) return;
      data.append(buf, n);
      if (header_end == std::string::npos) {
        header_end = data.find("\r\n\r\n");
        if (header_end == std::string::npos) continue;
        content_length =
            Header_(data.substr(0, header_end), "content-length")
                .transform([](const std::string& v) { return std::stoul(v); })
                .value_or(0);
      }
      if (data.size() >= header_end + 4 + content_length) break;
    }

    std::string head = data.substr(0, header_end);
    Request req;
    req.method = head.substr(0, head.find(' '));
    size_t path_start = head.find(' ') + 1;
    req.path = head.substr(path_start, head.find(' ', path_start) - path_start);
    req.token = Header_(head, "x-vault-token").value_or("");

    auto [status, body] = Route_(req);
    std::string resp = std::format(
        "HTTP/1.1 {} X\r\nContent-Type: application/json\r\n"
        "Content-Length: {}\r\nConnection: close\r\n\r\n{}",
        status, body.size(), body);
    send(fd, resp.data(), resp.size(), MSG_NOSIGNAL);
  }

  std::pair<int, std::string> Route_(const Request& req) {
    std::lock_guard lock(m_mtx_);
    m_requests_.push_back(req);

    const std::string kDenied =
        R"({"errors":["2 errors occurred:\n\t* permission denied\n\t* invalid token\n\n"]})";

    if (req.path.starts_with("/v1/auth/userpass/login/")) {
      // Vault rejects a login request carrying an invalid token.
      if (!req.token.empty() && !m_valid_tokens_.contains(req.token))
        return {403, kDenied};
      std::string token = std::format("token-{}", ++m_token_seq_);
      m_valid_tokens_.insert(token);
      return {200, std::format(R"({{"auth":{{"client_token":"{}",)"
                               R"("lease_duration":{},"renewable":true}}}})",
                               token, m_token_ttl_seconds_)};
    }

    if (!m_valid_tokens_.contains(req.token)) return {403, kDenied};

    if (req.path == "/v1/sys/health") return {200, R"({"initialized":true})"};
    if (req.path == "/v1/auth/token/renew-self")
      return {200, std::format(R"({{"auth":{{"client_token":"{}",)"
                               R"("lease_duration":{},"renewable":true}}}})",
                               req.token, m_token_ttl_seconds_)};
    if (req.path == "/v1/pki/sign/CraneSched")
      return {200, std::format(R"({{"data":{{"serial_number":"sn-{}",)"
                               R"("certificate":"cert"}}}})",
                               ++m_serial_seq_)};
    if (req.path.starts_with("/v1/pki/cert/"))
      return {200, R"({"data":{"revocation_time":0}})"};
    if (req.path == "/v1/pki/revoke") return {200, R"({"data":{}})"};
    return {404, R"({"errors":[]})"};
  }

  static std::optional<std::string> Header_(const std::string& head,
                                            const std::string& name) {
    for (const auto& line : absl::StrSplit(head, "\r\n")) {
      std::string lower = absl::AsciiStrToLower(line);
      if (!lower.starts_with(name + ":")) continue;
      return std::string(
          absl::StripAsciiWhitespace(line.substr(name.size() + 1)));
    }
    return std::nullopt;
  }

  int m_listen_fd_{-1};
  int m_port_{0};
  std::atomic<bool> m_stop_{false};
  std::thread m_thread_;

  mutable std::mutex m_mtx_;
  std::set<std::string> m_valid_tokens_;
  std::vector<Request> m_requests_;
  int64_t m_token_ttl_seconds_;
  int m_token_seq_{0};
  int m_serial_seq_{0};
};

Ctld::Config::VaultConfig MakeConfig(int port) {
  Ctld::Config::VaultConfig config;
  config.Enabled = true;
  config.Addr = "127.0.0.1";
  config.Port = std::to_string(port);
  config.Username = "crane";
  config.Password = "secret";
  config.Tls = false;
  config.ExpirationMinutes = 30;
  return config;
}

TEST(VaultClientTest, SignLogsInAgainAfterTokenExpires) {
  FakeVault vault;
  VaultClient client;
  ASSERT_TRUE(client.InitFromConfig(MakeConfig(vault.Port())));
  ASSERT_TRUE(client.Sign("csr", "1000", "").has_value());

  vault.ExpireAllTokens();

  auto resp = client.Sign("csr", "1001", "");
  ASSERT_TRUE(resp.has_value());
  EXPECT_EQ(resp->certificate, "cert");
  EXPECT_EQ(vault.Count("/v1/auth/userpass/login/"), 2);

  // The re-login must not send the expired token.
  auto requests = vault.Requests();
  auto relogin =
      std::find_if(requests.rbegin(), requests.rend(), [](const Request& req) {
        return req.path.starts_with("/v1/auth/userpass/login/");
      });
  ASSERT_NE(relogin, requests.rend());
  EXPECT_TRUE(relogin->token.empty());
}

TEST(VaultClientTest, RevokeAndCertCheckLogInAgainAfterTokenExpires) {
  FakeVault vault;
  VaultClient client;
  ASSERT_TRUE(client.InitFromConfig(MakeConfig(vault.Port())));

  vault.ExpireAllTokens();
  EXPECT_TRUE(client.IsCertAllowed("sn-unknown"));

  vault.ExpireAllTokens();
  EXPECT_TRUE(client.RevokeCert("sn-unknown"));
  EXPECT_EQ(vault.Count("/v1/auth/userpass/login/"), 3);
}

TEST(VaultClientTest, ConcurrentRequestsShareOneRelogin) {
  FakeVault vault;
  VaultClient client;
  ASSERT_TRUE(client.InitFromConfig(MakeConfig(vault.Port())));
  vault.ExpireAllTokens();

  std::vector<std::thread> threads;
  std::atomic<int> succeeded{0};
  for (int i = 0; i < 8; ++i)
    threads.emplace_back([&, i] {
      if (client.Sign("csr", std::to_string(2000 + i), "")) ++succeeded;
    });
  for (auto& t : threads) t.join();

  EXPECT_EQ(succeeded, 8);
  EXPECT_EQ(vault.Count("/v1/auth/userpass/login/"), 2);
}

TEST(VaultClientTest, RenewsTokenInBackground) {
  // Renewal runs at half the TTL, clamped to at least 5 seconds.
  FakeVault vault(/*token_ttl_seconds=*/10);
  VaultClient client;
  ASSERT_TRUE(client.InitFromConfig(MakeConfig(vault.Port())));
  int renewals_at_init = vault.Count("/v1/auth/token/renew-self");

  std::this_thread::sleep_for(std::chrono::seconds(7));

  EXPECT_GT(vault.Count("/v1/auth/token/renew-self"), renewals_at_init);
}

TEST(VaultClientTest, InitFailsWhenVaultIsUnreachable) {
  VaultClient client;
  EXPECT_FALSE(client.InitFromConfig(MakeConfig(/*port=*/1)));
}

}  // namespace

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

#include "VaultClient.h"

namespace Ctld::Security {

namespace {

constexpr int64_t kHttpForbidden = 403;
// Renew when this fraction of the remaining TTL has passed.
constexpr double kRenewAtTtlFraction = 0.5;
constexpr absl::Duration kMinRenewInterval = absl::Seconds(5);
constexpr absl::Duration kMaxRenewInterval = absl::Hours(1);
constexpr absl::Duration kRetryInterval = absl::Seconds(30);
// Log in again once a renewal returns less than this fraction of the login
// TTL: Vault caps renewals at the max TTL of the userpass mount.
constexpr double kReloginAtTtlFraction = 0.25;

}  // namespace

VaultClient::~VaultClient() {
  {
    absl::MutexLock lock(&m_stop_mtx_);
    m_stop_ = true;
  }
  if (m_token_maintenance_thread_.joinable())
    m_token_maintenance_thread_.join();
}

bool VaultClient::InitFromConfig(const Config::VaultConfig& vault_config) {
  // Vault::Client keeps a reference to the strategy, so it must outlive the
  // client for re-login to work.
  m_auth_strategy_ = std::make_unique<UserPassStrategy>(vault_config.Username,
                                                        vault_config.Password);
  m_address_ = vault_config.Addr;
  m_port_ = vault_config.Port;
  m_tls_ = vault_config.Tls;

  Vault::Config config = Vault::ConfigBuilder()
                             .withDebug(false)
                             .withTlsEnabled(vault_config.Tls)
                             .withHost(Vault::Host{vault_config.Addr})
                             .withPort(Vault::Port{vault_config.Port})
                             .build();
  Vault::HttpErrorCallback http_error_cb = [](const std::string& err) {
    CRANE_ERROR(err);
  };

  Vault::ResponseErrorCallback resp_cb = [](const Vault::HttpResponse& err) {
    LogResponseError_(err);
  };

  m_root_client_ = std::make_unique<Vault::Client>(
      Vault::Client{config, *m_auth_strategy_, http_error_cb, resp_cb});

  if (!m_root_client_->is_authenticated()) {
    CRANE_ERROR("Vault client is not authenticated");
    return false;
  }

  {
    absl::MutexLock lock(&m_token_mtx_);
    m_token_ = m_root_client_->getToken();
  }

  if (!Vault::HttpConsumer::get(*m_root_client_,
                                GetUrl_("/v1/sys/", Vault::Path{"health"}))) {
    CRANE_ERROR(
        "Vault client connection failed, please check if it is started, "
        "initialized or unsealed.");
    return false;
  }

  std::optional<absl::Duration> ttl = RenewToken_();
  if (!ttl) {
    CRANE_ERROR("Failed to look up the TTL of the Vault token");
    return false;
  }
  {
    absl::MutexLock lock(&m_token_mtx_);
    m_login_ttl_ = *ttl;
  }

  m_token_maintenance_thread_ =
      std::thread([this] { TokenMaintenanceLoop_(); });

  CRANE_TRACE("Successfully connected to Vault, username: {}, token ttl: {}",
              vault_config.Username, absl::FormatDuration(*ttl));

  return true;
}

std::optional<SignResponse> VaultClient::Sign(const std::string& csr_content,
                                              const std::string& common_name,
                                              const std::string& alt_names) {
  Vault::Parameters parameters({{"csr", csr_content},
                                {"common_name", common_name},
                                {"alt_names", alt_names},
                                {"exclude_cn_from_sans", "true"}});

  try {
    std::optional<Vault::HttpResponse> resp = Request_(
        HttpMethod::kPost,
        GetPkiUrl_(Vault::SecretMount{"pki"}, Vault::Path{"sign/CraneSched"}),
        CreatJson_(parameters));
    if (!resp) return std::nullopt;

    nlohmann::json json = nlohmann::json::parse(resp->body.value());
    nlohmann::json const& data = json.at("data");

    auto it_ser = data.find("serial_number");
    auto it_cert = data.find("certificate");
    if (it_ser == data.end() || it_cert == data.end()) {
      CRANE_DEBUG("Serial_number or certificate is missing in response data");
      return std::nullopt;
    }

    m_allowed_certs_.insert_or_assign(std::string(*it_ser),
                                      absl::ToUnixSeconds(absl::Now()));

    return SignResponse{*it_ser, *it_cert};

  } catch (const std::exception& e) {
    CRANE_ERROR("Failed to sign certificate: {}", e.what());
    return std::nullopt;
  }
}

bool VaultClient::RevokeCert(const std::string& serial_number) {
  try {
    std::optional<Vault::HttpResponse> resp = Request_(
        HttpMethod::kPost,
        GetPkiUrl_(Vault::SecretMount{"pki"}, Vault::Path{"revoke"}),
        CreatJson_(Vault::Parameters{{"serial_number", serial_number}}));

    if (!resp) return false;

  } catch (const std::exception& e) {
    CRANE_ERROR("Failed to revoke certificate: {}", e.what());
    return false;
  }

  m_allowed_certs_.erase(serial_number);

  return true;
}

bool VaultClient::IsCertAllowed(const std::string& serial_number) {
  auto iter = m_allowed_certs_.find(serial_number);
  if (iter != m_allowed_certs_.end() &&
      absl::Now() - absl::FromUnixSeconds(iter->second) <
          absl::Minutes(g_config.VaultConf.ExpirationMinutes))
    return true;

  try {
    std::optional<Vault::HttpResponse> resp = Request_(
        HttpMethod::kGet, GetPkiUrl_(Vault::SecretMount{"pki"},
                                     Vault::Path{"cert/" + serial_number}));
    if (!resp) return false;

    nlohmann::json json = nlohmann::json::parse(resp->body.value());
    nlohmann::json const& data = json.at("data");

    auto it_revocation_time = data.find("revocation_time");
    if (it_revocation_time == data.end()) {
      CRANE_DEBUG("Missing required fields in response data");
      return false;
    }

    if (it_revocation_time->get<uint32_t>() > 0) return false;

  } catch (const std::exception& e) {
    CRANE_ERROR("Failed to read certificate: {}", e.what());
    return false;
  }

  m_allowed_certs_.insert_or_assign(serial_number,
                                    absl::ToUnixSeconds(absl::Now()));

  return true;
}

std::optional<Vault::HttpResponse> VaultClient::Request_(
    HttpMethod method, const Vault::Url& url,
    const std::optional<nlohmann::json>& body) {
  Vault::Token token = GetToken_();
  std::optional<Vault::HttpResponse> resp = Send_(method, url, body, token);
  if (!resp) return std::nullopt;

  if (resp->statusCode.value() == kHttpForbidden) {
    CRANE_WARN("Vault rejected the token for {}, logging in again.",
               url.value());
    if (!Relogin_(token)) {
      LogResponseError_(*resp);
      return std::nullopt;
    }
    resp = Send_(method, url, body, GetToken_());
    if (!resp) return std::nullopt;
  }

  if (Vault::HttpClient::is_success(resp)) return resp;

  // Revoking a non-existent certificate is an expected error, keep it quiet.
  static const LazyRE2 err_serial_regex(
      R"(.*certificate with serial (\S+) not found\..*)");
  if (RE2::FullMatch(resp->body.value(), *err_serial_regex))
    CRANE_TRACE("Certificate not found in Vault: {}", resp->body.value());
  else
    LogResponseError_(*resp);
  return std::nullopt;
}

std::optional<Vault::HttpResponse> VaultClient::Send_(
    HttpMethod method, const Vault::Url& url,
    const std::optional<nlohmann::json>& body, const Vault::Token& token) {
  const Vault::HttpClient& http = m_root_client_->getHttpClient();
  const Vault::Namespace& ns = m_root_client_->getNamespace();
  if (method == HttpMethod::kGet) return http.get(url, token, ns);
  return http.post(url, token, ns,
                   body.value_or(nlohmann::json::object()).dump());
}

bool VaultClient::Relogin_(const Vault::Token& stale_token) {
  absl::MutexLock lock(&m_token_mtx_);
  // Another request already replaced the rejected token.
  if (m_token_.value() != stale_token.value()) return true;

  std::optional<Vault::AuthenticationResponse> auth =
      m_auth_strategy_->authenticate(*m_root_client_);
  if (!auth || auth->token.empty()) {
    CRANE_ERROR("Failed to log in to Vault again.");
    return false;
  }

  m_token_ = auth->token;
  try {
    m_token_ttl_ =
        TokenTtl_(nlohmann::json::parse(auth->rawResponse.value()).at("auth"));
  } catch (const std::exception& e) {
    CRANE_WARN("Failed to read the TTL of the new Vault token: {}", e.what());
    m_token_ttl_ = absl::ZeroDuration();
  }
  m_login_ttl_ = m_token_ttl_;
  CRANE_INFO("Logged in to Vault again, token ttl: {}",
             absl::FormatDuration(m_token_ttl_));
  return true;
}

std::optional<absl::Duration> VaultClient::RenewToken_() {
  Vault::Token token = GetToken_();
  std::optional<Vault::HttpResponse> resp = Send_(
      HttpMethod::kPost, GetUrl_("/v1/auth/", Vault::Path{"token/renew-self"}),
      nlohmann::json::object(), token);
  if (!resp) return std::nullopt;

  if (resp->statusCode.value() == kHttpForbidden) {
    // The token expired: renewal is no longer possible.
    if (!Relogin_(token)) return std::nullopt;
    absl::MutexLock lock(&m_token_mtx_);
    return m_token_ttl_;
  }
  if (!Vault::HttpClient::is_success(resp)) {
    LogResponseError_(*resp);
    return std::nullopt;
  }

  absl::Duration ttl;
  try {
    ttl = TokenTtl_(nlohmann::json::parse(resp->body.value()).at("auth"));
  } catch (const std::exception& e) {
    CRANE_ERROR("Failed to parse the Vault token renewal: {}", e.what());
    return std::nullopt;
  }

  absl::MutexLock lock(&m_token_mtx_);
  m_token_ttl_ = ttl;
  return ttl;
}

void VaultClient::TokenMaintenanceLoop_() {
  util::SetCurrentThreadName("VaultTokenThr");

  absl::Duration wait;
  {
    absl::MutexLock lock(&m_token_mtx_);
    wait = m_token_ttl_ * kRenewAtTtlFraction;
  }

  while (true) {
    // A zero TTL means the token never expires; still check periodically so
    // that a revoked token is replaced.
    wait = std::clamp(wait == absl::ZeroDuration() ? kMaxRenewInterval : wait,
                      kMinRenewInterval, kMaxRenewInterval);
    {
      absl::MutexLock lock(&m_stop_mtx_);
      auto stopped = [this]() ABSL_SHARED_LOCKS_REQUIRED(m_stop_mtx_) {
        return m_stop_;
      };
      if (m_stop_mtx_.AwaitWithTimeout(absl::Condition(&stopped), wait)) return;
    }

    std::optional<absl::Duration> ttl = RenewToken_();
    if (!ttl) {
      CRANE_ERROR("Failed to renew the Vault token, retrying in {}.",
                  absl::FormatDuration(kRetryInterval));
      wait = kRetryInterval;
      continue;
    }

    // Near the max TTL, Vault caps the renewed TTL; log in again before the
    // token expires instead of renewing a nearly expired token.
    absl::Duration login_ttl;
    {
      absl::MutexLock lock(&m_token_mtx_);
      login_ttl = m_login_ttl_;
    }
    if (*ttl <
        std::max(login_ttl * kReloginAtTtlFraction, kMinRenewInterval * 2)) {
      Vault::Token token = GetToken_();
      if (Relogin_(token)) {
        absl::MutexLock lock(&m_token_mtx_);
        ttl = m_token_ttl_;
      }
    }
    CRANE_TRACE("Vault token renewed, ttl: {}", absl::FormatDuration(*ttl));
    wait = *ttl * kRenewAtTtlFraction;
  }
}

Vault::Token VaultClient::GetToken_() const {
  absl::MutexLock lock(&m_token_mtx_);
  return m_token_;
}

absl::Duration VaultClient::TokenTtl_(const nlohmann::json& auth) {
  return absl::Seconds(auth.at("lease_duration").get<int64_t>());
}

void VaultClient::LogResponseError_(const Vault::HttpResponse& resp) {
  CRANE_ERROR("{} : {}", resp.url.value(), resp.body.value());
}

Vault::Url VaultClient::GetPkiUrl_(const Vault::SecretMount& secret_mount,
                                   const Vault::Path& path) const {
  return GetUrl_("/v1/" + secret_mount,
                 path.empty() ? path : Vault::Path{"/" + path});
}

Vault::Url VaultClient::GetUrl_(const std::string& base,
                                const Vault::Path& path) const {
  std::string url = fmt::format("{}{}:{}{}{}", m_tls_ ? "https://" : "http://",
                                m_address_, m_port_, base, path.toString());
  return Vault::Url{url};
}

nlohmann::json VaultClient::CreatJson_(const Vault::Parameters& parameters) {
  nlohmann::json json = nlohmann::json::object();

  for (const auto& [key, value] : parameters) {
    std::visit(
        [&json, &key](const auto& v) {
          using T = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, std::string> ||
                        std::is_same_v<T, int> ||
                        std::is_same_v<T, std::vector<std::string>>) {
            json[key] = v;
          } else if constexpr (std::is_same_v<T, Vault::Map>) {
            nlohmann::json j;
            for (const auto& [map_key, map_value] : v) j[map_key] = map_value;
            json[key] = j;
          } else {
            throw std::runtime_error("Unsupported type in Vault::Parameters");
          }
        },
        value);
  }

  return json;
}

UserPassStrategy::UserPassStrategy(const std::string& username,
                                   const std::string& password)
    : m_username_(username), m_password_(password) {}

std::optional<Vault::AuthenticationResponse> UserPassStrategy::authenticate(
    const Vault::Client& client) {
  // Log in without a token: the client may still hold an expired one, which
  // Vault would reject.
  nlohmann::json body = nlohmann::json::object();
  body["password"] = m_password_;
  std::optional<Vault::HttpResponse> resp = client.getHttpClient().post(
      client.getUrl("/v1/auth/userpass/login/", Vault::Path{m_username_}),
      Vault::Token{}, client.getNamespace(), body.dump());

  if (!Vault::HttpClient::is_success(resp)) {
    if (resp) client.getHttpClient().handleResponseError(resp.value());
    return std::nullopt;
  }

  try {
    auto token = nlohmann::json::parse(resp->body.value())
                     .at("auth")
                     .at("client_token")
                     .get<std::string>();
    return Vault::AuthenticationResponse{resp->body, Vault::Token{token}};
  } catch (const std::exception& e) {
    CRANE_ERROR("Failed to parse the Vault login response: {}", e.what());
    return std::nullopt;
  }
}

}  // namespace Ctld::Security

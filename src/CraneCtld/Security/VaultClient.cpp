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

bool VaultClient::InitFromConfig(const Config::VaultConfig& vault_config) {
  UserPassStrategy user_pass_strategy{vault_config.Username,
                                      vault_config.Password};
  m_address_ = vault_config.Addr;
  m_port_ = vault_config.Port;
  m_tls_ = vault_config.Tls;

  Vault::Config config = Vault::ConfigBuilder()
                             .withDebug(false)
                             .withTlsEnabled(vault_config.Tls)
                             .withHost(Vault::Host{vault_config.Addr})
                             .withPort(Vault::Port{vault_config.Port})
                             .build();
  Vault::HttpErrorCallback http_error_cb = [&](const std::string& err) {
    CRANE_ERROR(err);
  };

  Vault::ResponseErrorCallback resp_cb = [&](const Vault::HttpResponse& err) {
    CRANE_ERROR("{} : {}", err.url.value(), err.body.value());
  };

  m_root_client_ = std::make_unique<Vault::Client>(
      Vault::Client{config, user_pass_strategy, http_error_cb, resp_cb});

  if (!m_root_client_->is_authenticated()) {
    CRANE_ERROR("Vault client is not authenticated");
    return false;
  }

  if (!Vault::HttpConsumer::get(*m_root_client_,
                                GetUrl_("/v1/sys/", Vault::Path{"health"}))) {
    CRANE_ERROR(
        "Vault client connection failed, please check if it is started, "
        "initialized or unsealed.");
    return false;
  }

  // Init Pki
  m_pki_root_ = std::make_unique<Vault::Pki>(
      Vault::Pki{*m_root_client_, Vault::SecretMount{"pki"}});
  m_pki_external_ = std::make_unique<Vault::Pki>(
      Vault::Pki{*m_root_client_, Vault::SecretMount{"pki_external"}});

  CRANE_TRACE("Successfully connected to Vault, username: {}",
              vault_config.Username);

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
    std::optional<std::string> resp =
        m_pki_external_->sign(Vault::Path{"external"}, parameters);
    if (!resp) return std::nullopt;

    nlohmann::json json = nlohmann::json::parse(resp.value());
    nlohmann::json const& data = json.at("data");

    auto it_ser = data.find("serial_number");
    auto it_cert = data.find("certificate");
    if (it_ser == data.end() || it_cert == data.end()) {
      CRANE_DEBUG("Serial_number or certificate is missing in response data");
      return std::nullopt;
    }

    return SignResponse{*it_ser, *it_cert};

  } catch (const std::exception& e) {
    CRANE_TRACE("Failed to sign certificate: {}", e.what());
    return std::nullopt;
  }
}

bool VaultClient::RevokeCert(const std::string& serial_number) {
  try {
    std::optional<std::string> resp = Post_(
        *m_root_client_,
        GetPkiUrl_(Vault::SecretMount{"pki_external"}, Vault::Path{"revoke"}),
        Vault::Parameters{{"serial_number", serial_number}});

    if (!resp) return false;

  } catch (const std::exception& e) {
    CRANE_TRACE("Failed to revoke certificate: {}", e.what());
    return false;
  }

  m_allowed_certs_.erase(serial_number);

  return true;
}

bool VaultClient::IsCertAllowed(const std::string& serial_number) {
  if (m_allowed_certs_.contains(serial_number)) return true;

  try {
    std::optional<std::string> resp =
        m_pki_external_->readCertificate(Vault::Path{serial_number});
    if (!resp) return false;

    nlohmann::json json = nlohmann::json::parse(resp.value());
    nlohmann::json const& data = json.at("data");

    auto it_revocation_time = data.find("revocation_time");
    if (it_revocation_time == data.end()) {
      CRANE_DEBUG("Missing required fields in response data");
      return false;
    }

    if (it_revocation_time->get<uint32_t>() > 0) return false;

  } catch (const std::exception& e) {
    CRANE_TRACE("Failed to list revoke certificate: {}", e.what());
    return false;
  }

  m_allowed_certs_.emplace(serial_number);

  return true;
}

// Revoking a non-existent certificate does not throw an error.
std::optional<std::string> VaultClient::Post_(
    const Vault::Client& client, const Vault::Url& url,
    const Vault::Parameters& parameters) {
  if (!client.is_authenticated()) return std::nullopt;

  nlohmann::json json = CreatJson_(parameters);

  std::optional<Vault::HttpResponse> resp = client.getHttpClient().post(
      url, client.getToken(), client.getNamespace(), json.dump());

  // No content in error response.
  if (!resp) return std::nullopt;

  if (Vault::HttpClient::is_success(resp)) return resp.value().body.value();

  nlohmann::json json_resp = nlohmann::json::parse(resp.value().body.value());
  auto it = json_resp.find("errors");
  if (it == json_resp.end() || !it->is_array() || it->empty())
    return std::nullopt;

  client.getHttpClient().handleResponseError(resp.value());

  std::string err_msg = it->at(0).get<std::string>();
  static const LazyRE2 err_serial_regex(
      R"(certificate with serial (\S+) not found\.)");
  if (RE2::FullMatch(err_msg, *err_serial_regex))
    CRANE_TRACE("Trying to revoke a non-existing certificate, {}", err_msg);

  return std::nullopt;
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
  return Vault::HttpConsumer::authenticate(
      client,
      client.getUrl("/v1/auth/userpass/login/", Vault::Path{m_username_}),
      [&]() {
        auto j = nlohmann::json::object();
        j["password"] = m_password_;
        return j.dump();
      });
}

}  // namespace Ctld::Security
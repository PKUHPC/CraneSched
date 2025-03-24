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

#include "VaultClientWrapper.h"

namespace vault {

VaultClientWrapper::VaultClientWrapper(const std::string& username,
                                       const std::string& password,
                                       const std::string& address,
                                       const std::string& port, bool tls)
    : address_(address), port_(port), tls_(tls) {
  UserPassStrategy user_pass_strategy{username, password};
  Vault::Config config = Vault::ConfigBuilder()
                             .withDebug(false)
                             .withTlsEnabled(tls_)
                             .withHost(Vault::Host{address_})
                             .withPort(Vault::Port{port_})
                             .build();
  Vault::HttpErrorCallback httpErrorCallback = [&](std::string err) {
    CRANE_ERROR(err);
  };
  Vault::ResponseErrorCallback responseCallback = [&](Vault::HttpResponse err) {
    CRANE_ERROR("{} : {}", err.url.value(), err.body.value());
  };
  root_client_ = std::make_unique<Vault::Client>(Vault::Client{
      config, user_pass_strategy, httpErrorCallback, responseCallback});
}

bool VaultClientWrapper::InitPki() {
  if (!root_client_->is_authenticated()) {
    CRANE_ERROR("Vault client is not authenticated");
    return false;
  }

  if (!GetVaultHealth_()) {
    CRANE_ERROR(
        "Vault client connect fail, Please check if it is started, initialized "
        "or unsealed.");
    return false;
  }

  pki_root_ = std::make_unique<Vault::Pki>(
      Vault::Pki{*root_client_, Vault::SecretMount{"pki"}});
  pki_external_ = std::make_unique<Vault::Pki>(
      Vault::Pki{*root_client_, Vault::SecretMount{"pki_external"}});

  return true;
}

std::expected<SignResponse, bool> VaultClientWrapper::Sign(
    const std::string& csr_content, const std::string& common_name,
    const std::string& alt_names) {
  Vault::Parameters parameters({{"csr", csr_content},
                                {"common_name", common_name},
                                {"alt_names", alt_names},
                                {"exclude_cn_from_sans", "true"}});

  nlohmann::json::value_type data;
  try {
    auto response = pki_external_->sign(Vault::Path{"external"}, parameters);
    if (!response) return std::unexpected(false);

    data = nlohmann::json::parse(response.value())["data"];

  } catch (const std::exception& e) {
    CRANE_DEBUG("Failed to sign certificate: {}", e.what());
    return std::unexpected(false);
  }

  return SignResponse{data["serial_number"], data["certificate"]};
}

bool VaultClientWrapper::RevokeCert(const std::string& serial_number) {
  try {
    auto response =
        RevokeCertificate_(Vault::Parameters{{"serial_number", serial_number}});
    if (!response) return false;
  } catch (const std::exception& e) {
    return false;
  }

  allowed_certs_.erase(serial_number);

  return true;
}

bool VaultClientWrapper::IsCertAllowed(const std::string& serial_number) {
  if (allowed_certs_.contains(serial_number)) return true;

  try {
    auto response = ListRevokeCertificate_();
    if (!response) return false;
    auto data = nlohmann::json::parse(response.value())["data"];
    for (const auto& key : data["keys"]) {
      std::string key_str = key;
      if (key_str == serial_number) return false;
    }
  } catch (const std::exception& e) {
    return false;
  }

  allowed_certs_.emplace(serial_number);

  return true;
}

std::optional<std::string> VaultClientWrapper::GetVaultHealth_() {
  return Vault::HttpConsumer::get(*root_client_,
                                  GetUrl_("/v1/sys/", Vault::Path{"health"}));
}

std::optional<std::string> VaultClientWrapper::ListRevokeCertificate_() {
  return list_(*root_client_, GetPkiUrl_(Vault::SecretMount{"pki_external"},
                                         Vault::Path{"certs/revoked"}));
}

std::optional<std::string> VaultClientWrapper::RevokeCertificate_(
    const Vault::Parameters& parameters) {
  return post_(
      *root_client_,
      GetPkiUrl_(Vault::SecretMount{"pki_external"}, Vault::Path{"revoke"}),
      parameters);
}

std::optional<std::string> VaultClientWrapper::list_(
    const Vault::Client& client, const Vault::Url& url) {
  if (!client.is_authenticated()) {
    return std::nullopt;
  }

  auto response = client.getHttpClient().list(url, client.getToken(),
                                              client.getNamespace());

  if (Vault::HttpClient::is_success(response)) {
    return {response.value().body.value()};
  }

  // Do not return an error when revoked is empty.
  if (response) {
    auto jsonResponse = nlohmann::json::parse(response.value().body.value());
    if (jsonResponse.contains("errors") && jsonResponse["errors"].is_array() &&
        jsonResponse["errors"].empty())
      return {response.value().body.value()};
    client.getHttpClient().handleResponseError(response.value());
  }

  return std::nullopt;
}

// Revoking a non-existent certificate does not throw an error.
std::optional<std::string> VaultClientWrapper::post_(
    const Vault::Client& client, const Vault::Url& url,
    const Vault::Parameters& parameters) {
  if (!client.is_authenticated()) {
    return std::nullopt;
  }

  nlohmann::json json = create_json(parameters);

  auto response = client.getHttpClient().post(
      url, client.getToken(), client.getNamespace(), json.dump());

  if (Vault::HttpClient::is_success(response)) {
    return response.value().body.value();
  }
  // Do not return an error when revoke not found certificate.
  if (response) {
    auto jsonResponse = nlohmann::json::parse(response.value().body.value());
    const auto res_err = jsonResponse["errors"];
    static const LazyRE2 pattern(
        R"(certificate with serial (\S+) not found\.)");
    if (jsonResponse.contains("errors") && res_err.is_array() &&
        res_err.size() == 1 && RE2::FullMatch(res_err[0], *pattern)) {
      CRANE_DEBUG("{}", res_err[0]);
      return "";
    } else {
      client.getHttpClient().handleResponseError(response.value());
    }
  }

  return std::nullopt;
}

Vault::Url VaultClientWrapper::GetPkiUrl_(const Vault::SecretMount secret_mount,
                                          const Vault::Path& path) const {
  return GetUrl_("/v1/" + secret_mount,
                 path.empty() ? path : Vault::Path{"/" + path});
}

Vault::Url VaultClientWrapper::GetUrl_(const std::string& base,
                                       const Vault::Path& path) const {
  return Vault::Url{(tls_ ? "https://" : "http://") + address_ + ":" + port_ +
                    base + path};
}

nlohmann::json VaultClientWrapper::create_json(
    const Vault::Parameters& parameters) {
  nlohmann::json json = nlohmann::json::object();
  for (auto& [key, value] : parameters) {
    if (std::holds_alternative<std::string>(value)) {
      json[key] = std::get<std::string>(value);
    } else if (std::holds_alternative<int>(value)) {
      json[key] = std::get<int>(value);
    } else if (std::holds_alternative<std::vector<std::string>>(value)) {
      json[key] = std::get<std::vector<std::string>>(value);
    } else if (std::holds_alternative<Vault::Map>(value)) {
      auto map = std::get<Vault::Map>(value);
      nlohmann::json j;
      for (auto& [map_key, map_value] : map) {
        j[map_key] = map_value;
      }
      json[key] = j;
    }
  }
  return json;
}

UserPassStrategy::UserPassStrategy(std::string username, std::string password)
    : username_(std::move(username)), password_(std::move(password)) {}

std::optional<Vault::AuthenticationResponse> UserPassStrategy::authenticate(
    const Vault::Client& client) {
  return Vault::HttpConsumer::authenticate(
      client, getUrl(client, Vault::Path{username_}), [&]() {
        nlohmann::json j;
        j = nlohmann::json::object();
        j["password"] = password_;
        return j.dump();
      });
}

Vault::Url UserPassStrategy::getUrl(const Vault::Client& client,
                                    const Vault::Path& username) {
  return client.getUrl("/v1/auth/userpass/login/", username);
}

}  // namespace vault
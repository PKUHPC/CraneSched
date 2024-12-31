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

#include "crane/VaultClient.h"

namespace vault {

VaultClient::VaultClient(const std::string& root_token,
                         const std::string& address, const std::string& port)
    : address_(address), port_(port) {
  Vault::TokenStrategy tokenStrategy{Vault::Token{root_token}};
  Vault::Config config = Vault::ConfigBuilder()
                             .withDebug(false)
                             .withTlsEnabled(false)
                             .withHost(Vault::Host{address_})
                             .withPort(Vault::Port{port_})
                             .build();
  Vault::HttpErrorCallback httpErrorCallback = [&](std::string err) {
    CRANE_DEBUG(err);
  };
  Vault::ResponseErrorCallback responseCallback = [&](Vault::HttpResponse err) {
    CRANE_DEBUG("{} : {}", err.url.value(), err.body.value());
  };
  root_client_ = std::make_unique<Vault::Client>(Vault::Client{
      config, tokenStrategy, httpErrorCallback, responseCallback});
}

bool VaultClient::InitPki(const std::string& domains,
                          CACertificateConfig* external_ca,
                          ServerCertificateConfig* external_cert) {
  if (!root_client_->is_authenticated()) {
    CRANE_ERROR("Root client is not authenticated");
    return false;
  }

  pki_root_ = std::make_unique<Vault::Pki>(
      Vault::Pki{*root_client_, Vault::SecretMount{"pki"}});
  pki_internal_ = std::make_unique<Vault::Pki>(
      Vault::Pki{*root_client_, Vault::SecretMount{"pki_internal"}});
  pki_external_ = std::make_unique<Vault::Pki>(
      Vault::Pki{*root_client_, Vault::SecretMount{"pki_external"}});

  // 检查CA是否存在，存在则写入文件中，不存在则创建CA
  if (!IssureExternalCa_(domains, external_ca)) return false;

  // 创建role
  if (!CreateRole_("external", domains)) return false;

  // 检查external_pem，external_key是否存在，不存在则创建，写入文件中
  if (!IssureExternalCert_("external", domains, external_cert)) return false;

  return true;
}

std::expected<SignResponse, bool> VaultClient::Sign(
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

bool VaultClient::IssureExternalCa_(const std::string& domains,
                                    CACertificateConfig* external_ca) {
  nlohmann::json::value_type data;
  std::optional<std::string> response;

  if (!external_ca->CACertContent.empty()) return true;

  try {
    response = pki_external_->readCACertificate();
    if (!response) {
      CRANE_ERROR("Failed to read external CA certificate");
      return false;
    }

    std::string external_ca_pem = response.value();

    if (external_ca_pem == "") {
      response = pki_external_->generateIntermediate(
          Vault::KeyType{"internal"},
          Vault::Parameters({{"common_name", std::format("*.{}", domains)},
                             {"issuer_name", "CraneSched_internal_CA"},
                             {"not_after", "9999-12-31T23:59:59Z"}}));

      data = nlohmann::json::parse(response.value())["data"];
      std::string external_ca_csr = data["csr"];

      response = pki_root_->signIntermediate(
          Vault::Parameters({{"csr", external_ca_csr},
                             {"format", "pem_bundle"},
                             {"not_after", "9999-12-31T23:59:59Z"}}));
      if (!response) {
        CRANE_ERROR("Failed to sign external CA certificate");
        return false;
      }

      data = nlohmann::json::parse(response.value())["data"];

      external_ca_pem = data["certificate"];

      response = pki_external_->setSignedIntermediate(
          Vault::Parameters({{"certificate", external_ca_pem}}));
      if (!response) {
        CRANE_ERROR("Failed to set signed external CA certificate");
        return false;
      }
    }

    external_ca->CACertContent = external_ca_pem;
    if (external_ca->CACertFilePath.empty()) {
      CRANE_ERROR("ExternalCAFilePath is empty");
      return false;
    }

    if (!util::os::SaveFile(external_ca->CACertFilePath, external_ca_pem, 0644))
      return false;

  } catch (const std::exception& e) {
    CRANE_ERROR("Failed to issue external CA certificate: {}", e.what());
    return false;
  }

  return true;
}

bool VaultClient::RevokeCert(const std::string& serial_number) {
  try {
    auto response = pki_external_->revokeCertificate(
        Vault::Parameters{{"serial_number", serial_number}});
    if (!response) return false;
  } catch (const std::exception& e) {
    return false;
  }

  allowed_certs_.erase(serial_number);

  return true;
}

bool VaultClient::IsCertAllowed(const std::string& serial_number) {
  if (allowed_certs_.contains(serial_number)) return true;

  try {
    // TODO: 检查crl列表
  } catch (const std::exception& e) {
    return false;
  }

  allowed_certs_.emplace(serial_number);

  return true;
}

bool VaultClient::CreateRole_(const std::string& role_name,
                              const std::string& domains) {
  nlohmann::json::value_type data;
  std::optional<std::string> response;

  try {
    response = pki_external_->readRole(Vault::Path{role_name});
    if (response) return true;

    response = pki_external_->createRole(
        Vault::Path{role_name},
        Vault::Parameters{{"allowed_domains", domains},
                          {"allow_subdomains", "true"},
                          {"allow_glob_domains", "true"},
                          {"not_after", "9999-12-31T23:59:59Z"}});
    if (!response) {
      CRANE_ERROR("Failed to create role {}", role_name);
      return false;
    }
  } catch (const std::exception& e) {
    CRANE_ERROR("Failed to create role {}: {}", role_name, e.what());
    return false;
  }

  return true;
}

bool VaultClient::IssureExternalCert_(const std::string& role_name,
                                      const std::string& domains,
                                      ServerCertificateConfig* external_cert) {
  nlohmann::json::value_type data;
  std::optional<std::string> response;

  if (!external_cert->ServerCertContent.empty()) return true;

  try {
    response = pki_external_->issue(
        Vault::Path(role_name),
        Vault::Parameters{{"common_name", std::format("external.{}", domains)},
                          {"alt_names", std::format("localhost,*.{}", domains)},
                          {"exclude_cn_from_sans", "true"}});
    if (!response) {
      CRANE_ERROR("Failed to issue external certificate");
      return false;
    }

    data = nlohmann::json::parse(response.value())["data"];
    std::string external_pem = data["certificate"];
    std::string external_key = data["private_key"];

    external_cert->ServerCertContent = external_pem;
    external_cert->ServerKeyContent = external_key;

    if (!util::os::SaveFile(external_cert->ServerCertFilePath, external_pem,
                            0644))
      return false;

    if (!util::os::SaveFile(external_cert->ServerKeyFilePath, external_key,
                            0600))
      return false;

  } catch (const std::exception& e) {
    CRANE_ERROR("Failed to issue external certificate: {}", e.what());
    return false;
  }

  return true;
}

}  // namespace vault
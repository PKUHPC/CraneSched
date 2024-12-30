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
  if (!root_client_->is_authenticated()) return false;

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
    if (!response) return false;

    std::string external_ca_pem = response.value();

    if (external_ca_pem == "") {
      response = pki_external_->generateIntermediate(
          Vault::KeyType{"internal"},
          Vault::Parameters({{"common_name", std::format("*.{}", domains)},
                             {"issuer_name", "CraneSched_internal_CA"},
                             {"not_after", "2030-12-31T23:59:59Z"}}));

      data = nlohmann::json::parse(response.value())["data"];
      std::string external_ca_csr = data["csr"];

      response = pki_root_->signIntermediate(
          Vault::Parameters({{"csr", external_ca_csr},
                             {"format", "pem_bundle"},
                             {"not_after", "2030-12-31T23:59:59Z"}}));
      if (!response) return false;

      data = nlohmann::json::parse(response.value())["data"];

      external_ca_pem = data["certificate"];

      response = pki_external_->setSignedIntermediate(
          Vault::Parameters({{"certificate", external_ca_pem}}));
      if (!response) return false;
    }

    if (!util::os::SaveFile(external_ca->CACertFilePath, external_ca_pem))
      return false;

  } catch (const std::exception& e) {
    return false;
  }

  return true;
}

bool VaultClient::CreateRole_(const std::string& role_name,
                              const std::string& domains) {
  nlohmann::json::value_type data;
  std::optional<std::string> response;

  try {
    response = pki_external_->createRole(
        Vault::Path{role_name},
        Vault::Parameters{{"allowed_domains", domains},
                          {"allow_subdomains", "true"},
                          {"allow_glob_domains", "true"},
                          {"not_after", "2030-12-31T23:59:59Z"}});
    if (!response) return false;
  } catch (const std::exception& e) {
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
    if (!response) return false;

    data = nlohmann::json::parse(response.value())["data"];
    std::string external_pem = data["certificate"];
    std::string external_key = data["private_key"];

    if (!util::os::SaveFile(external_cert->ServerCertFilePath, external_pem))
      return false;

    if (!util::os::SaveFile(external_cert->ServerKeyFilePath, external_key))
      return false;

  } catch (const std::exception& e) {
    return false;
  }

  return true;
}

}  // namespace vault
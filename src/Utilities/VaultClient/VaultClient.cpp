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
                                   const std::string& address,
                                   const std::string& port)
    : address_(address), port_(port) {
  Vault::TokenStrategy tokenStrategy{Vault::Token{root_token}};
  Vault::Config config = Vault::ConfigBuilder()
                             .withDebug(false)
                             .withTlsEnabled(false)
                             .withHost(Vault::Host{address_})
                             .withPort(Vault::Port{port_})
                             .build();
  Vault::HttpErrorCallback httpErrorCallback = [&](std::string err) {
    std::cout << err << std::endl;
  };
  Vault::ResponseErrorCallback responseCallback = [&](Vault::HttpResponse err) {
    std::cout << err.statusCode << " : " << err.url.value() << " : "
              << err.body.value() << std::endl;
  };
  root_client_ = std::make_unique<Vault::Client>(Vault::Client{
      config, tokenStrategy, httpErrorCallback, responseCallback});
}

bool VaultClient::InitPki(const std::string& domains) {
  if (!root_client_->is_authenticated()) return false;

  pki_admin_ = std::make_unique<Vault::Pki>(Vault::Pki{*root_client_});

  return true;
}

std::expected<SignResponse, bool> VaultClient::Sign(
    const std::string& csr_content, const std::string& common_name) {
  Vault::Parameters parameters(
      {{"csr", csr_content}, {"common_name", common_name}});

  auto response = pki_admin_->sign(Vault::Path{"user"}, parameters);
  if (!response) return std::unexpected(false);

  auto data = nlohmann::json::parse(response.value())["data"];

  return SignResponse{data["serial_number"], data["certificate"], data["issuing_ca"]};
}

}  // namespace vault
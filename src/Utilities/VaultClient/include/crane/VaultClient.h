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

#include <VaultClient.h>

#include <expected>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>

#include "crane/GrpcHelper.h"
#include "crane/Logger.h"
#include "crane/OS.h"

namespace vault {

struct SignResponse {
  std::string serial_number;
  std::string certificate;
};

class VaultClient {
 public:
  VaultClient(const std::string& root_token, const std::string& address,
              const std::string& port);
  bool InitPki(const std::string& domains, CACertificateConfig* external_ca,
               ServerCertificateConfig* external_cert);

  std::expected<SignResponse, bool> Sign(const std::string& csr_content,
                                         const std::string& common_name,
                                         const std::string& alt_names);

 private:
  bool IssureExternalCa_(const std::string& domains,
                         CACertificateConfig* external_ca);

  bool CreateRole_(const std::string& role_name, const std::string& domains);

  bool IssureExternalCert_(const std::string& role_name,
                           const std::string& domains,
                           ServerCertificateConfig* external_cert);

  std::unique_ptr<Vault::Client> root_client_;
  std::unique_ptr<Vault::Pki> pki_root_;
  std::unique_ptr<Vault::Pki> pki_internal_;
  std::unique_ptr<Vault::Pki> pki_external_;

  std::string address_;
  std::string port_;
};

}  // namespace vault

inline std::unique_ptr<vault::VaultClient> g_vault_client;

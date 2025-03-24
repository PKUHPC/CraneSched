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
#include <parallel_hashmap/phmap.h>
#include <re2/re2.h>

#include <expected>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_set>

#include "crane/GrpcHelper.h"
#include "crane/Lock.h"
#include "crane/Logger.h"
#include "crane/OS.h"

namespace vault {

struct SignResponse {
  std::string serial_number;
  std::string certificate;
};

using AllowedCerts = phmap::parallel_flat_hash_set<std::string>;

class VaultClientWrapper {
 public:
  VaultClientWrapper(const std::string& username, const std::string& password,
                     const std::string& address, const std::string& port,
                     bool tls = false);

  bool InitPki();

  std::expected<SignResponse, bool> Sign(const std::string& csr_content,
                                         const std::string& common_name,
                                         const std::string& alt_names);

  bool RevokeCert(const std::string& serial_number);

  bool IsCertAllowed(const std::string& serial_number);

 private:
  std::optional<std::string> GetVaultHealth_();

  std::optional<std::string> ListRevokeCertificate_();

  std::optional<std::string> RevokeCertificate_(
      const Vault::Parameters& parameters);

  std::optional<std::string> list_(const Vault::Client& client,
                                   const Vault::Url& url);

  std::optional<std::string> post_(const Vault::Client& client,
                                   const Vault::Url& url,
                                   const Vault::Parameters& parameters);

  Vault::Url GetUrl_(const std::string& base, const Vault::Path& path) const;

  Vault::Url GetPkiUrl_(const Vault::SecretMount secret_mount,
                        const Vault::Path& path) const;

  nlohmann::json create_json(const Vault::Parameters& parameters);

  std::unique_ptr<Vault::Client> root_client_;
  std::unique_ptr<Vault::Pki> pki_root_;
  std::unique_ptr<Vault::Pki> pki_external_;

  // Use parallel containers to improve performance.
  AllowedCerts allowed_certs_;

  std::string address_;
  std::string port_;
  bool tls_;
};

class UserPassStrategy : public Vault::AuthenticationStrategy {
 public:
  UserPassStrategy(std::string username, std::string password);

  std::optional<Vault::AuthenticationResponse> authenticate(
      const Vault::Client& client) override;

 private:
  static Vault::Url getUrl(const Vault::Client& client,
                           const Vault::Path& username);

  std::string username_;
  std::string password_;
};

}  // namespace vault

inline std::unique_ptr<vault::VaultClientWrapper> g_vault_client;
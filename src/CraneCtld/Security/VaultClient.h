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

#include "CtldPublicDefs.h"
// Precompiled header comes first!

#include <VaultClient.h>

#include "crane/Lock.h"

namespace Ctld::Security {

struct SignResponse {
  std::string serial_number;
  std::string certificate;
};

class VaultClient {
 public:
  VaultClient() = default;

  bool InitFromConfig(const Ctld::Config::VaultConfig& vault_config);

  std::optional<SignResponse> Sign(const std::string& csr_content,
                                   const std::string& common_name,
                                   const std::string& alt_names);

  bool RevokeCert(const std::string& serial_number);

  bool IsCertAllowed(const std::string& serial_number);

 private:
  std::optional<std::string> ListRevokeCertificate_();

  std::optional<std::string> RevokeCertificate_(
      const Vault::Parameters& parameters);

  std::optional<std::string> List_(const Vault::Client& client,
                                   const Vault::Url& url);

  std::optional<std::string> Post_(const Vault::Client& client,
                                   const Vault::Url& url,
                                   const Vault::Parameters& parameters);

  Vault::Url GetUrl_(const std::string& base, const Vault::Path& path) const;

  Vault::Url GetPkiUrl_(const Vault::SecretMount secret_mount,
                        const Vault::Path& path) const;

  static nlohmann::json CreatJson_(const Vault::Parameters& parameters);

  std::unique_ptr<Vault::Client> m_root_client_;
  std::unique_ptr<Vault::Pki> m_pki_root_;
  std::unique_ptr<Vault::Pki> m_pki_external_;

  template <typename T>
  using AllowedCertsSet = phmap::parallel_flat_hash_set<T>;

  // Use parallel containers to improve performance.
  AllowedCertsSet<std::string> m_allowed_certs_;

  std::string m_address_;
  std::string m_port_;
  bool m_tls_{};
};

class UserPassStrategy : public Vault::AuthenticationStrategy {
 public:
  UserPassStrategy(const std::string& username, const std::string& password);

  std::optional<Vault::AuthenticationResponse> authenticate(
      const Vault::Client& client) override;

 private:
  std::string m_username_;
  std::string m_password_;
};

}  // namespace Ctld::Security

inline std::unique_ptr<Ctld::Security::VaultClient> g_vault_client;
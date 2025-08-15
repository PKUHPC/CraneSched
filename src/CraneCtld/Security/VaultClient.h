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

namespace Ctld::Security {

struct SignResponse {
  std::string serial_number;
  std::string certificate;
};

class VaultClient {
 public:
  VaultClient() = default;

  bool InitFromConfig(const Config::VaultConfig& vault_config);

  std::optional<SignResponse> Sign(const std::string& csr_content,
                                   const std::string& common_name,
                                   const std::string& alt_names);

  bool RevokeCert(const std::string& serial_number);

  bool IsCertAllowed(const std::string& serial_number);

 private:
  static std::optional<std::string> Post_(const Vault::Client& client,
                                          const Vault::Url& url,
                                          const Vault::Parameters& parameters);

  Vault::Url GetUrl_(const std::string& base, const Vault::Path& path) const;

  Vault::Url GetPkiUrl_(const Vault::SecretMount& secret_mount,
                        const Vault::Path& path) const;

  static nlohmann::json CreatJson_(const Vault::Parameters& parameters);

  std::unique_ptr<Vault::Client> m_root_client_;
  std::unique_ptr<Vault::Pki> m_pki_root_;
  std::unique_ptr<Vault::Pki> m_pki_external_;

  using ParallelHashMap = phmap::parallel_flat_hash_map<
      std::string, int64_t, phmap::priv::hash_default_hash<std::string>,
      phmap::priv::hash_default_eq<std::string>,
      std::allocator<std::pair<const std::string, int64_t>>, 4,
      std::shared_mutex>;

  // Use parallel containers to improve performance.
  ParallelHashMap m_allowed_certs_;

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
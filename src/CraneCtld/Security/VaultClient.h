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

class UserPassStrategy : public Vault::AuthenticationStrategy {
 public:
  UserPassStrategy(const std::string& username, const std::string& password);

  std::optional<Vault::AuthenticationResponse> authenticate(
      const Vault::Client& client) override;

 private:
  std::string m_username_;
  std::string m_password_;
};

class VaultClient {
 public:
  VaultClient() = default;
  ~VaultClient();

  VaultClient(const VaultClient&) = delete;
  VaultClient& operator=(const VaultClient&) = delete;

  bool InitFromConfig(const Config::VaultConfig& vault_config);

  std::optional<SignResponse> Sign(const std::string& csr_content,
                                   const std::string& common_name,
                                   const std::string& alt_names);

  bool RevokeCert(const std::string& serial_number);

  bool IsCertAllowed(const std::string& serial_number);

 private:
  enum class HttpMethod : std::uint8_t { kGet, kPost };

  // Sends a request with the current token. A 403 means the token expired or
  // was revoked, so log in again and retry the request once.
  std::optional<Vault::HttpResponse> Request_(
      HttpMethod method, const Vault::Url& url,
      const std::optional<nlohmann::json>& body = std::nullopt);

  std::optional<Vault::HttpResponse> Send_(
      HttpMethod method, const Vault::Url& url,
      const std::optional<nlohmann::json>& body, const Vault::Token& token);

  // Logs in with username/password and replaces the current token, unless
  // another thread already replaced `stale_token`.
  bool Relogin_(const Vault::Token& stale_token);

  // Renews the token before its TTL ends; logs in again once it reaches the
  // max TTL of the userpass mount and can no longer be renewed.
  void TokenMaintenanceLoop_();

  // Returns the remaining TTL of the current token, or nullopt on failure.
  std::optional<absl::Duration> RenewToken_();

  Vault::Token GetToken_() const;

  Vault::Url GetUrl_(const std::string& base, const Vault::Path& path) const;

  Vault::Url GetPkiUrl_(const Vault::SecretMount& secret_mount,
                        const Vault::Path& path) const;

  static nlohmann::json CreatJson_(const Vault::Parameters& parameters);

  static absl::Duration TokenTtl_(const nlohmann::json& auth);

  static void LogResponseError_(const Vault::HttpResponse& resp);

  std::unique_ptr<UserPassStrategy> m_auth_strategy_;
  std::unique_ptr<Vault::Client> m_root_client_;

  mutable absl::Mutex m_token_mtx_;
  Vault::Token m_token_ ABSL_GUARDED_BY(m_token_mtx_);
  absl::Duration m_token_ttl_ ABSL_GUARDED_BY(m_token_mtx_);
  // TTL granted at the last login.
  absl::Duration m_login_ttl_ ABSL_GUARDED_BY(m_token_mtx_);

  absl::Mutex m_stop_mtx_;
  bool m_stop_ ABSL_GUARDED_BY(m_stop_mtx_){false};
  std::thread m_token_maintenance_thread_;

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

}  // namespace Ctld::Security

inline std::unique_ptr<Ctld::Security::VaultClient> g_vault_client;
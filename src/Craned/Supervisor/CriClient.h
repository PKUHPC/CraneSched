/**
 * Copyright (c) 2025 Peking University and Peking University
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

#include "SupervisorPreCompiledHeader.h"
// Precompiled header comes first

#include <grpcpp/channel.h>

#include "crane/PublicHeader.h"
#include "cri/api.grpc.pb.h"
#include "cri/api.pb.h"

namespace Supervisor {

namespace cri = runtime::v1;

inline constexpr std::string kDefaultPodNamespace = "cranesched";

class CriClient {
 public:
  CriClient() = default;
  ~CriClient();

  CriClient(const CriClient&) = delete;
  CriClient(const CriClient&&) = delete;

  CriClient& operator=(const CriClient&) = delete;
  CriClient& operator=(const CriClient&&) = delete;

  void InitChannelAndStub(const std::filesystem::path& runtime_service,
                          const std::filesystem::path& image_service);

  // FIXME: DEBUG ONLY
  void Version() const;
  void RuntimeConfig() const;

  // Runtime Service
  std::optional<std::string> RunPodSandbox(
      std::unique_ptr<cri::PodSandboxConfig> config) const;

  // Image Service
  std::optional<std::string> GetImageId(const std::string& image_ref) const;
  std::optional<std::string> PullImage(const std::string& image_ref) const;

  // Helper Methods

  // Generate a default PodMetadata.
  static cri::PodSandboxMetadata BuildPodSandboxMetaData(
      uid_t uid, job_id_t job_id, const std::string& name);

  // Generate default Pod labels
  static std::unordered_map<std::string, std::string> BuildPodLabels(
      uid_t uid, job_id_t job_id, const std::string& name);

  // Generate Linux Pod config
  static cri::LinuxPodSandboxConfig BuildLinuxPodConfig();

 private:
  std::thread m_async_send_thread_;
  std::atomic_bool m_thread_stop_;

  std::shared_ptr<grpc::Channel> m_rs_channel_;
  std::shared_ptr<grpc::Channel> m_is_channel_;
  std::shared_ptr<cri::RuntimeService::Stub> m_rs_stub_;
  std::shared_ptr<cri::ImageService::Stub> m_is_stub_;
};

}  // namespace Supervisor

inline std::unique_ptr<Supervisor::CriClient> g_cri_client;
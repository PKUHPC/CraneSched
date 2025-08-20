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
#include "crane/Lock.h"
#include "cri/api.grpc.pb.h"
#include "cri/api.pb.h"

namespace Supervisor {

namespace cri = runtime::v1;

inline constexpr std::string kDefaultPodNamespace = "cranesched";
inline constexpr std::chrono::seconds kDefaultCriReqTimeout =
    std::chrono::seconds(5);

class CriClient {
  using ContainerEventCallback =
      std::function<void(const cri::ContainerEventResponse&)>;

 public:
  CriClient() = default;
  ~CriClient();

  CriClient(const CriClient&) = delete;
  CriClient(const CriClient&&) = delete;

  CriClient& operator=(const CriClient&) = delete;
  CriClient& operator=(const CriClient&&) = delete;

  void InitChannelAndStub(const std::filesystem::path& runtime_service,
                          const std::filesystem::path& image_service);

  // TODO: Remove these debugging methods
  void Version() const;
  void RuntimeConfig() const;

  // ==== Runtime Service ====

  // Pod
  CraneExpected<std::string> RunPodSandbox(
      const cri::PodSandboxConfig& config) const;

  CraneExpected<void> StopPodSandbox(const std::string& pod_sandbox_id) const;

  CraneExpected<void> RemovePodSandbox(const std::string& pod_sandbox_id) const;

  // Containers
  CraneExpected<std::string> CreateContainer(
      const cri::ContainerConfig& config) const;

  CraneExpected<void> StartContainer(const std::string& container_id) const;

  CraneExpected<void> StopContainer(const std::string& container_id,
                                    int64_t timeout = 0) const;

  CraneExpected<void> RemoveContainer(const std::string& container_id) const;

  // Container Event Streaming

  // Start event streaming thread
  void StartContainerEventStream(ContainerEventCallback callback);

  // Stop event streaming thread
  void StopContainerEventStream();

  // Check if event streaming is started
  bool IsEventStreamActive() const;

  // Active Container Status Checking
  CraneExpected<std::vector<cri::Container>> ListContainers() const;

  CraneExpected<std::vector<cri::Container>> ListContainers(
      const std::map<std::string, std::string>& label_selector) const;

  CraneExpected<cri::ContainerStatus> GetContainerStatus(
      const std::string& container_id, bool verbose = false) const;

  // ==== Image Service ====

  std::optional<std::string> GetImageId(const std::string& image_ref) const;
  std::optional<std::string> PullImage(const std::string& image_ref) const;

  // ==== Helper Methods ====

  // Generate a default PodMetadata
  static cri::PodSandboxMetadata BuildPodSandboxMetaData(
      uid_t uid, job_id_t job_id, const std::string& name);

  // Generate default Pod labels
  static std::unordered_map<std::string, std::string> BuildPodLabels(
      uid_t uid, job_id_t job_id, const std::string& name);

  // Generate a default ContainerMetadata
  // TODO: Refactor these methods after we seperate job and step.
  static cri::ContainerMetadata BuildContainerMetaData(uid_t uid,
                                                       job_id_t job_id,
                                                       const std::string& name);

  // Generate default Container labels
  static std::unordered_map<std::string, std::string> BuildContainerLabels(
      uid_t uid, job_id_t job_id, const std::string& name);

 private:
  // Container Event Stream Management
  void ContainerEventStreamLoop_();
  void HandleContainerEvent_(const cri::ContainerEventResponse& event);

  std::thread m_async_send_thread_;
  std::atomic_bool m_thread_stop_;

  // Container event monitoring
  std::thread m_event_stream_thread_;
  std::atomic_bool m_event_stream_stop_;
  ContainerEventCallback m_event_callback_;
  util::mutex m_event_callback_mutex_;

  std::shared_ptr<grpc::Channel> m_rs_channel_;
  std::shared_ptr<grpc::Channel> m_is_channel_;
  std::shared_ptr<cri::RuntimeService::Stub> m_rs_stub_;
  std::shared_ptr<cri::ImageService::Stub> m_is_stub_;
};

}  // namespace Supervisor

inline std::unique_ptr<Supervisor::CriClient> g_cri_client;
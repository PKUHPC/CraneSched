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
#include "cri/api.grpc.pb.h"
#include "cri/api.pb.h"
// CRI protobuf must comes at first

#include <absl/container/flat_hash_map.h>
#include <google/protobuf/message.h>
#include <grpcpp/channel.h>
#include <grpcpp/support/status.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>

#include "crane/Lock.h"
#include "crane/PublicHeader.h"

namespace cri {

namespace api = runtime::v1;

inline constexpr std::string_view kCriDefaultPodNamespace = "cranesched";
inline constexpr std::string_view kCriDefaultLabel = kCriDefaultPodNamespace;

// Use for selecting containers created by CraneSched
static constexpr std::string_view kCriLabelJobIdKey = "job_id";
static constexpr std::string_view kCriLabelJobNameKey = "name";
static constexpr std::string_view kCriLabelUidKey = "uid";

inline constexpr std::chrono::seconds kCriDefaultReqTimeout =
    std::chrono::seconds(5);
inline constexpr std::chrono::seconds kCriDefaultImagePullingTimeout =
    std::chrono::seconds(60);

using ContainerEventCallback =
    std::function<void(const api::ContainerEventResponse&)>;

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

  // ==== Runtime Service ====
  void Version() const;
  void RuntimeConfig() const;

  // TODO: CraneExpected should be replaced here for richer error info from CRI

  // Pod
  CraneExpected<std::string> RunPodSandbox(api::PodSandboxConfig* config) const;

  CraneExpected<void> StopPodSandbox(const std::string& pod_sandbox_id) const;

  CraneExpected<void> RemovePodSandbox(const std::string& pod_sandbox_id) const;

  // Containers
  CraneExpected<std::string> CreateContainer(
      const std::string& pod_id, const api::PodSandboxConfig& pod_config,
      api::ContainerConfig* config) const;

  CraneExpected<void> StartContainer(const std::string& container_id) const;

  CraneExpected<void> StopContainer(const std::string& container_id,
                                    int64_t timeout = 0) const;

  CraneExpected<void> RemoveContainer(const std::string& container_id) const;

  CraneExpected<std::string> Attach(const std::string& container_id, bool tty,
                                    bool stdin, bool stdout, bool stderr) const;

  CraneExpected<std::string> Exec(const std::string& container_id,
                                  const std::vector<std::string>& command,
                                  bool tty, bool stdin, bool stdout,
                                  bool stderr) const;

  // Container Event Streaming

  // Start event streaming thread
  void StartContainerEventStream(ContainerEventCallback callback);
  // Stop event streaming thread
  void StopContainerEventStream();
  // Check if event streaming is started
  bool IsEventStreamActive() const;

  // Active Container Status Checking
  CraneExpected<std::vector<api::Container>> ListContainers() const;

  CraneExpected<std::vector<api::Container>> ListContainers(
      const std::unordered_map<std::string, std::string>& label_selector) const;

  // Select exactly one container id by label selector
  CraneExpected<std::string> SelectContainerId(
      const std::unordered_map<std::string, std::string>& label_selector) const;

  // ==== Image Service ====
  // TODO: Async image pulling?
  std::optional<std::string> GetImageId(const std::string& image_name) const;
  std::optional<std::string> PullImage(const std::string& image_name,
                                       const std::string& username,
                                       const std::string& password,
                                       const std::string& server_addr,
                                       const std::string& pull_policy) const;

 private:
  // Inject labels and metadata for future selection
  static void InjectConfig_(api::PodSandboxConfig* config) {
    config->mutable_metadata()->set_namespace_(kCriDefaultPodNamespace);
    config->mutable_labels()->emplace(kCriDefaultLabel,
                                      "true");  // Default label for CRI
  }
  static void InjectConfig_(api::ContainerConfig* config) {
    config->mutable_labels()->emplace(kCriDefaultLabel,
                                      "true");  // Default label for CRI
  }

  // Container Event Stream Management
  void ContainerEventStreamLoop_();
  void HandleContainerEvent_(const api::ContainerEventResponse& event);

  // Container event streaming
  std::thread m_event_stream_thread_;
  std::atomic_bool m_event_stream_stop_{true};

  // Cancellable context for gRPC stream
  std::shared_ptr<grpc::ClientContext> m_event_stream_context_;
  util::mutex m_event_stream_context_mutex_;

  // Container event callback
  ContainerEventCallback m_event_callback_;
  util::mutex m_event_callback_mutex_;

  std::shared_ptr<grpc::Channel> m_rs_channel_;
  std::shared_ptr<grpc::Channel> m_is_channel_;
  std::shared_ptr<api::RuntimeService::Stub> m_rs_stub_;
  std::shared_ptr<api::ImageService::Stub> m_is_stub_;
};

}  // namespace cri

// NOTE: This ptr is currently only used in Craned.
inline std::unique_ptr<cri::CriClient> g_cri_client;
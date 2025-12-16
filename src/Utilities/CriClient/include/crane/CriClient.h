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
#include <charconv>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <vector>

#include "crane/Lock.h"
#include "crane/PublicHeader.h"

namespace cri {

namespace api = runtime::v1;

inline constexpr std::string_view kCriDefaultPodNamespace = "cranesched";
inline constexpr std::string_view kCriDefaultLabel = kCriDefaultPodNamespace;

// Use for selecting containers created by CraneSched
inline constexpr std::string_view kCriLabelJobIdKey = "job_id";
inline constexpr std::string_view kCriLabelStepIdKey = "step_id";
inline constexpr std::string_view kCriLabelJobNameKey = "job_name";
inline constexpr std::string_view kCriLabelStepNameKey = "step_name";
inline constexpr std::string_view kCriLabelUidKey = "uid";

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

  // Pod
  CraneExpectedRich<std::string> RunPodSandbox(
      api::PodSandboxConfig* config) const;

  CraneExpectedRich<void> StopPodSandbox(
      const std::string& pod_sandbox_id) const;

  CraneExpectedRich<void> RemovePodSandbox(
      const std::string& pod_sandbox_id) const;

  CraneExpectedRich<api::PodSandboxStatus> GetPodSandboxStatus(
      const std::string& pod_sandbox_id, bool verbose = false) const;

  CraneExpectedRich<std::vector<api::PodSandbox>> ListPodSandbox() const;

  CraneExpectedRich<std::vector<api::PodSandbox>> ListPodSandbox(
      const std::unordered_map<std::string, std::string>& label_selector) const;

  CraneExpectedRich<std::string> GetPodSandboxId(
      const std::unordered_map<std::string, std::string>& label_selector) const;

  // Containers
  CraneExpectedRich<std::string> CreateContainer(
      const std::string& pod_id, const api::PodSandboxConfig& pod_config,
      api::ContainerConfig* config) const;

  CraneExpectedRich<void> StartContainer(const std::string& container_id) const;

  CraneExpectedRich<void> StopContainer(const std::string& container_id,
                                        int64_t timeout = 0) const;

  CraneExpectedRich<void> RemoveContainer(
      const std::string& container_id) const;

  CraneExpectedRich<std::string> Attach(const std::string& container_id,
                                        bool tty, bool stdin, bool stdout,
                                        bool stderr) const;

  CraneExpectedRich<std::string> Exec(const std::string& container_id,
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
  CraneExpectedRich<std::vector<api::Container>> ListContainers() const;

  CraneExpectedRich<std::vector<api::Container>> ListContainers(
      const std::unordered_map<std::string, std::string>& label_selector) const;

  // Get exactly one container id by label selector
  CraneExpectedRich<std::string> GetContainerId(
      const std::unordered_map<std::string, std::string>& label_selector) const;

  // ==== Image Service ====
  // TODO: Async image pulling?
  std::optional<std::string> GetImageId(const std::string& image_name) const;
  std::optional<std::string> PullImage(const std::string& image_name,
                                       const std::string& username,
                                       const std::string& password,
                                       const std::string& server_addr,
                                       const std::string& pull_policy) const;

  // ==== Helpers ====

  // Setup mapping for kid <-> uid. For kernel id and userspace id, See:
  // https://www.kernel.org/doc/html/next/filesystems/idmappings.html
  static cri::api::IDMapping MakeIdMapping(uid_t kernel_id, uid_t userspace_id,
                                           size_t size) {
    cri::api::IDMapping mapping{};
    mapping.set_host_id(kernel_id);
    mapping.set_container_id(userspace_id);
    mapping.set_length(size);
    return mapping;
  }

  // Parse and compare numeric labels (using std::from_chars)
  template <typename T>
    requires requires(const char* first, const char* last, T& value) {
      { std::from_chars(first, last, value) };
    }
  static bool ParseAndCompareLabel(const api::ContainerStatus& status,
                                   const std::string& key, const T& value) {
    const auto& labels = status.labels();
    auto it = labels.find(key);
    if (it == labels.end()) return false;

    T parsed_value{};
    auto [ptr, ec] = std::from_chars(
        it->second.data(), it->second.data() + it->second.size(), parsed_value);

    // Check both parsing success and complete consumption
    if (ec != std::errc{} || ptr != it->second.data() + it->second.size()) {
      CRANE_ERROR("Failed to parse {} label '{}': {}", key, it->second,
                  ec == std::errc{} ? "incomplete parse" : "invalid format");
      return false;
    }

    return parsed_value == value;
  }

  // Parse and compare string labels (direct comparison)
  template <typename T>
    requires std::convertible_to<T, std::string_view>
  bool ParseAndCompareLabel(const api::ContainerStatus& status,
                            const std::string& key, const T& value) const {
    const auto& labels = status.labels();
    auto it = labels.find(key);
    if (it == labels.end()) return false;

    // Convert value to string_view for comparison
    std::string_view value_view = value;
    return it->second == value_view;
  }

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

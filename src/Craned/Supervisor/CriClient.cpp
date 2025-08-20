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

#include "CriClient.h"

#include <grpcpp/client_context.h>

#include "crane/Lock.h"
#include "crane/Logger.h"
#include "crane/PublicHeader.h"
#include "cri/api.pb.h"

namespace Supervisor {

CriClient::~CriClient() {
  // Stop event stream first
  StopContainerEventStream();

  if (m_async_send_thread_.joinable()) {
    m_async_send_thread_.join();
  }
}

void CriClient::InitChannelAndStub(const std::filesystem::path& runtime_service,
                                   const std::filesystem::path& image_service) {
  if (runtime_service == image_service) {
    m_rs_channel_ = CreateUnixInsecureChannel(runtime_service);
    m_is_channel_ = m_rs_channel_;
  } else {
    m_rs_channel_ = CreateUnixInsecureChannel(runtime_service);
    m_is_channel_ = CreateUnixInsecureChannel(image_service);
  }

  m_rs_stub_ = cri::RuntimeService::NewStub(m_rs_channel_);
  m_is_stub_ = cri::ImageService::NewStub(m_is_channel_);
  m_async_send_thread_ = std::thread([this] {});
}

void CriClient::Version() const {
  using cri::VersionRequest;
  using cri::VersionResponse;

  VersionRequest request{};
  VersionResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kDefaultCriReqTimeout);

  auto status = m_rs_stub_->Version(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to get CRI version: {}", status.error_message());
    return;
  }

  CRANE_TRACE("CRI replied Version: version={}", response.version());
}

void CriClient::RuntimeConfig() const {
  using cri::RuntimeConfigRequest;
  using cri::RuntimeConfigResponse;

  RuntimeConfigRequest request{};
  RuntimeConfigResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kDefaultCriReqTimeout);

  auto status = m_rs_stub_->RuntimeConfig(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to get CRI runtime config: {}", status.error_message());
    return;
  }

#ifdef linux
#  pragma push_macro("linux")
#  undef linux
#endif
  auto cg = response.linux().cgroup_driver();
#ifdef linux
#  pragma pop_macro("linux")
#endif

  CRANE_TRACE("CRI replied RuntimeConfig: cgroup_driver={}",
              cri::CgroupDriver_Name(cg));
}

std::optional<std::string> CriClient::GetImageId(
    const std::string& image_ref) const {
  using cri::ImageStatusRequest;
  using cri::ImageStatusResponse;

  ImageStatusRequest request{};
  ImageStatusResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kDefaultCriReqTimeout);

  auto* image = request.mutable_image();
  image->set_image(image_ref);

  auto status = m_is_stub_->ImageStatus(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to get image status: {}", status.error_message());
    return std::nullopt;
  }

  if (!response.has_image()) return std::nullopt;

  return response.image().id();
}

std::optional<std::string> CriClient::PullImage(
    const std::string& image_ref) const {
  using cri::PullImageRequest;
  using cri::PullImageResponse;

  PullImageRequest request{};
  PullImageResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kDefaultCriReqTimeout);

  auto* image = request.mutable_image();
  image->set_image(image_ref);

  auto status = m_is_stub_->PullImage(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to pull image: {}", status.error_message());
    return std::nullopt;
  }

  if (response.image_ref().empty()) {
    CRANE_ERROR("Empty image ID returned after pulling image {}", image_ref);
    return std::nullopt;
  }

  return response.image_ref();
}

CraneExpected<std::string> CriClient::RunPodSandbox(
    const cri::PodSandboxConfig& config) const {
  using cri::RunPodSandboxRequest;
  using cri::RunPodSandboxResponse;

  RunPodSandboxRequest request{};
  RunPodSandboxResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kDefaultCriReqTimeout);

  request.mutable_config()->CopyFrom(config);
  auto status = m_rs_stub_->RunPodSandbox(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to run pod sandbox: {}", status.error_message());
    return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
  }

  return response.pod_sandbox_id();
}

CraneExpected<void> CriClient::StopPodSandbox(
    const std::string& pod_sandbox_id) const {
  using cri::StopPodSandboxRequest;
  using cri::StopPodSandboxResponse;

  CraneExpected<void> ret;
  StopPodSandboxRequest request{};
  StopPodSandboxResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kDefaultCriReqTimeout);

  request.set_pod_sandbox_id(pod_sandbox_id);
  auto status = m_rs_stub_->StopPodSandbox(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to stop pod sandbox: {}", status.error_message());
    return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
  }

  return ret;
}

CraneExpected<void> CriClient::RemovePodSandbox(
    const std::string& pod_sandbox_id) const {
  using cri::RemovePodSandboxRequest;
  using cri::RemovePodSandboxResponse;

  CraneExpected<void> ret;
  RemovePodSandboxRequest request{};
  RemovePodSandboxResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kDefaultCriReqTimeout);

  request.set_pod_sandbox_id(pod_sandbox_id);
  auto status = m_rs_stub_->RemovePodSandbox(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to remove pod sandbox: {}", status.error_message());
    return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
  }

  return ret;
}

CraneExpected<std::string> CriClient::CreateContainer(
    const cri::ContainerConfig& config) const {
  using cri::CreateContainerRequest;
  using cri::CreateContainerResponse;

  CreateContainerRequest request{};
  CreateContainerResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kDefaultCriReqTimeout);

  request.mutable_config()->CopyFrom(config);
  auto status = m_rs_stub_->CreateContainer(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to create container: {}", status.error_message());
    return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
  }

  return response.container_id();
}

CraneExpected<void> CriClient::StartContainer(
    const std::string& container_id) const {
  using cri::StartContainerRequest;
  using cri::StartContainerResponse;

  CraneExpected<void> ret;
  StartContainerRequest request{};
  StartContainerResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kDefaultCriReqTimeout);

  request.set_container_id(container_id);
  auto status = m_rs_stub_->StartContainer(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to start container: {}", status.error_message());
    return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
  }

  return ret;
}

CraneExpected<void> CriClient::StopContainer(const std::string& container_id,
                                             int64_t timeout) const {
  using cri::StopContainerRequest;
  using cri::StopContainerResponse;

  CraneExpected<void> ret;
  StopContainerRequest request{};
  StopContainerResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kDefaultCriReqTimeout);

  request.set_container_id(container_id);
  request.set_timeout(timeout);
  auto status = m_rs_stub_->StopContainer(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to stop container: {}", status.error_message());
    return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
  }

  return ret;
}

CraneExpected<void> CriClient::RemoveContainer(
    const std::string& container_id) const {
  using cri::RemoveContainerRequest;
  using cri::RemoveContainerResponse;

  CraneExpected<void> ret;
  RemoveContainerRequest request{};
  RemoveContainerResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kDefaultCriReqTimeout);

  request.set_container_id(container_id);
  auto status = m_rs_stub_->RemoveContainer(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to remove container: {}", status.error_message());
    return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
  }

  return ret;
}

cri::PodSandboxMetadata CriClient::BuildPodSandboxMetaData(
    uid_t uid, job_id_t job_id, const std::string& name) {
  cri::PodSandboxMetadata metadata{};
  std::string pod_id{};

  // TODO: Add node hostname as name suffix.
  if (name.empty()) {
    pod_id = std::format("{}-{}", uid, job_id);
    metadata.set_name(std::format("crane-pod-job-{}", job_id));
  } else {
    pod_id = std::format("{}-{}-{}", uid, job_id, name);
    metadata.set_name(name);
  }

  metadata.set_uid(pod_id);
  metadata.set_namespace_(kDefaultPodNamespace);

  return metadata;
}

std::unordered_map<std::string, std::string> CriClient::BuildPodLabels(
    uid_t uid, job_id_t job_id, const std::string& name) {
  std::unordered_map<std::string, std::string> labels;
  labels["uid"] = std::to_string(uid);
  labels["job_id"] = std::to_string(job_id);
  labels["name"] = name;
  return labels;
}

cri::ContainerMetadata CriClient::BuildContainerMetaData(
    uid_t uid, job_id_t job_id, const std::string& name) {
  cri::ContainerMetadata metadata{};
  if (!name.empty()) metadata.set_name(name);
  return metadata;
}

std::unordered_map<std::string, std::string> CriClient::BuildContainerLabels(
    uid_t uid, job_id_t job_id, const std::string& name) {
  std::unordered_map<std::string, std::string> labels;
  labels["uid"] = std::to_string(uid);
  labels["job_id"] = std::to_string(job_id);
  labels["name"] = name;
  return labels;
}

// ===== Container Event Monitoring Implementation =====

void CriClient::StartContainerEventStream(ContainerEventCallback callback) {
  // Stop existing stream if running
  StopContainerEventStream();

  {
    util::lock_guard lock(m_event_callback_mutex_);
    m_event_callback_ = std::move(callback);
  }

  m_event_stream_stop_ = false;
  m_event_stream_thread_ = std::thread([this] { ContainerEventStreamLoop_(); });

  CRANE_DEBUG("Container event stream started");
}

void CriClient::StopContainerEventStream() {
  if (m_event_stream_thread_.joinable()) {
    m_event_stream_stop_ = true;
    m_event_stream_thread_.join();
    CRANE_DEBUG("Container event stream stopped");
  }

  {
    util::lock_guard lock(m_event_callback_mutex_);
    m_event_callback_ = nullptr;
  }
}

bool CriClient::IsEventStreamActive() const {
  return m_event_stream_thread_.joinable() && !m_event_stream_stop_;
}

void CriClient::ContainerEventStreamLoop_() {
  using cri::ContainerEventResponse;
  using cri::GetEventsRequest;

  while (!m_event_stream_stop_) {
    try {
      GetEventsRequest request{};
      grpc::ClientContext context;

      // Set a reasonable timeout for the stream
      context.set_deadline(std::chrono::system_clock::now() +
                           std::chrono::minutes(5));

      CRANE_DEBUG("Starting container event stream...");
      auto stream = m_rs_stub_->GetContainerEvents(&context, request);

      ContainerEventResponse response;
      while (stream->Read(&response) && !m_event_stream_stop_) {
        HandleContainerEvent_(response);
      }

      auto status = stream->Finish();
      if (!status.ok() && !m_event_stream_stop_) {
        CRANE_ERROR("Container event stream failed: {} (code: {})",
                    status.error_message(),
                    static_cast<int>(status.error_code()));
      }

    } catch (const std::exception& e) {
      if (!m_event_stream_stop_) {
        CRANE_ERROR("Exception in container event stream: {}", e.what());
      }
    }

    // Reconnection delay if not stopping
    if (!m_event_stream_stop_) {
      CRANE_INFO(
          "Container event stream disconnected, reconnecting in 5 seconds...");
      std::this_thread::sleep_for(kDefaultCriReqTimeout);
    }
  }

  CRANE_DEBUG("Container event stream loop ended");
}

void CriClient::HandleContainerEvent_(
    const cri::ContainerEventResponse& event) {
  // Log the event
  auto event_type_str =
      cri::ContainerEventType_Name(event.container_event_type());

  CRANE_TRACE("Container event: {} {} at {}", event.container_id(),
              event_type_str, event.created_at());

  // Invoke the callback if set
  util::lock_guard lock(m_event_callback_mutex_);
  if (m_event_callback_) {
    try {
      m_event_callback_(event);
    } catch (const std::exception& e) {
      CRANE_ERROR("Exception in container event callback: {}", e.what());
    }
  }
}

// ===== Active Container Status Query Implementation =====

CraneExpected<std::vector<cri::Container>> CriClient::ListContainers() const {
  using cri::ListContainersRequest;
  using cri::ListContainersResponse;

  ListContainersRequest request{};
  ListContainersResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kDefaultCriReqTimeout);

  auto status = m_rs_stub_->ListContainers(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to list containers: {}", status.error_message());
    return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
  }

  std::vector<cri::Container> containers;
  containers.reserve(response.containers_size());
  for (const auto& container : response.containers()) {
    containers.push_back(container);
  }

  CRANE_DEBUG("Listed {} containers", containers.size());
  return containers;
}

CraneExpected<std::vector<cri::Container>> CriClient::ListContainers(
    const std::map<std::string, std::string>& label_selector) const {
  using cri::ContainerFilter;
  using cri::ListContainersRequest;
  using cri::ListContainersResponse;

  ListContainersRequest request{};
  ListContainersResponse response{};

  // Set up filter with label selector
  auto* filter = request.mutable_filter();
  auto* labels = filter->mutable_label_selector();
  for (const auto& [key, value] : label_selector) {
    (*labels)[key] = value;
  }

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kDefaultCriReqTimeout);

  auto status = m_rs_stub_->ListContainers(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to list containers by labels: {}",
                status.error_message());
    return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
  }

  std::vector<cri::Container> containers;
  containers.reserve(response.containers_size());
  for (const auto& container : response.containers()) {
    containers.push_back(container);
  }

  CRANE_DEBUG("Listed {} containers matching labels", containers.size());
  return containers;
}

}  // namespace Supervisor
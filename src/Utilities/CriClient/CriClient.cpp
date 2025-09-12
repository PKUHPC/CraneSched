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

#include "crane/CriClient.h"

#include <grpcpp/client_context.h>

#include "crane/GrpcHelper.h"
#include "crane/Lock.h"
#include "crane/Logger.h"
#include "crane/PublicHeader.h"
#include "cri/api.pb.h"
#include "protos/PublicDefs.pb.h"

namespace cri {

CriClient::~CriClient() {
  // Stop event stream
  StopContainerEventStream();
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

  m_rs_stub_ = api::RuntimeService::NewStub(m_rs_channel_);
  m_is_stub_ = api::ImageService::NewStub(m_is_channel_);
}

void CriClient::Version() const {
  using api::VersionRequest;
  using api::VersionResponse;

  VersionRequest request{};
  VersionResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

  auto status = m_rs_stub_->Version(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to get CRI version: {}", status.error_message());
    return;
  }

  CRANE_TRACE("CRI replied: version={}", response.version());
}

void CriClient::RuntimeConfig() const {
  using api::RuntimeConfigRequest;
  using api::RuntimeConfigResponse;

  RuntimeConfigRequest request{};
  RuntimeConfigResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

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

  CRANE_TRACE("CRI replied: cgroup_driver={}", api::CgroupDriver_Name(cg));
}

std::optional<std::string> CriClient::GetImageId(
    const std::string& image_ref) const {
  using api::ImageStatusRequest;
  using api::ImageStatusResponse;

  ImageStatusRequest request{};
  ImageStatusResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

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
  using api::PullImageRequest;
  using api::PullImageResponse;

  PullImageRequest request{};
  PullImageResponse response{};

  // TODO: Make it configurable in config files, now setting to 1 minutes
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultImagePullingTimeout);

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
    api::PodSandboxConfig* config) const {
  using api::RunPodSandboxRequest;
  using api::RunPodSandboxResponse;

  // Inject the config
  InjectConfig_(config);

  RunPodSandboxRequest request{};
  RunPodSandboxResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

  request.mutable_config()->CopyFrom(*config);
  auto status = m_rs_stub_->RunPodSandbox(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to run pod sandbox: {}", status.error_message());
    return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
  }

  return response.pod_sandbox_id();
}

CraneExpected<void> CriClient::StopPodSandbox(
    const std::string& pod_sandbox_id) const {
  using api::StopPodSandboxRequest;
  using api::StopPodSandboxResponse;

  CraneExpected<void> ret;
  StopPodSandboxRequest request{};
  StopPodSandboxResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

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
  using api::RemovePodSandboxRequest;
  using api::RemovePodSandboxResponse;

  CraneExpected<void> ret;
  RemovePodSandboxRequest request{};
  RemovePodSandboxResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

  request.set_pod_sandbox_id(pod_sandbox_id);
  auto status = m_rs_stub_->RemovePodSandbox(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to remove pod sandbox: {}", status.error_message());
    return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
  }

  return ret;
}

CraneExpected<std::string> CriClient::CreateContainer(
    const std::string& pod_id, const api::PodSandboxConfig& pod_config,
    api::ContainerConfig* config) const {
  using api::CreateContainerRequest;
  using api::CreateContainerResponse;

  // Inject the config
  InjectConfig_(config);

  CreateContainerRequest request{};
  CreateContainerResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

  request.set_pod_sandbox_id(pod_id);
  request.mutable_sandbox_config()->CopyFrom(pod_config);
  request.mutable_config()->CopyFrom(*config);
  auto status = m_rs_stub_->CreateContainer(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to create container: {}", status.error_message());
    return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
  }

  return response.container_id();
}

CraneExpected<void> CriClient::StartContainer(
    const std::string& container_id) const {
  using api::StartContainerRequest;
  using api::StartContainerResponse;

  CraneExpected<void> ret;
  StartContainerRequest request{};
  StartContainerResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

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
  using api::StopContainerRequest;
  using api::StopContainerResponse;

  CraneExpected<void> ret;
  StopContainerRequest request{};
  StopContainerResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

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
  using api::RemoveContainerRequest;
  using api::RemoveContainerResponse;

  CraneExpected<void> ret;
  RemoveContainerRequest request{};
  RemoveContainerResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

  request.set_container_id(container_id);
  auto status = m_rs_stub_->RemoveContainer(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to remove container: {}", status.error_message());
    return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
  }

  return ret;
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

  CRANE_TRACE("Container event stream started");
}

void CriClient::StopContainerEventStream() {
  if (m_event_stream_thread_.joinable()) {
    m_event_stream_stop_ = true;

    // Cancel the gRPC context to immediately unblock stream operations
    {
      util::lock_guard lock(m_event_stream_context_mutex_);
      if (m_event_stream_context_) {
        m_event_stream_context_->TryCancel();
      }
    }

    m_event_stream_thread_.join();
    CRANE_TRACE("Container event stream stopped");
  } else {
    CRANE_TRACE("Container event stream not running");
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
  using api::ContainerEventResponse;
  using api::GetEventsRequest;

  while (!m_event_stream_stop_) {
    std::shared_ptr<grpc::ClientContext> current_context;

    try {
      GetEventsRequest request{};

      // Create and store a cancellable context
      {
        util::lock_guard lock(m_event_stream_context_mutex_);
        m_event_stream_context_ = std::make_shared<grpc::ClientContext>();
        current_context = m_event_stream_context_;
      }

      CRANE_TRACE("Starting container event stream...");
      auto stream =
          m_rs_stub_->GetContainerEvents(current_context.get(), request);

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

    // Clear the shared context after each attempt
    {
      util::lock_guard lock(m_event_stream_context_mutex_);
      m_event_stream_context_.reset();
    }

    // Reconnection delay if not stopping
    if (!m_event_stream_stop_) {
      CRANE_INFO(
          "Container event stream disconnected, reconnecting in {} seconds...",
          kCriDefaultReqTimeout);
      std::this_thread::sleep_for(kCriDefaultReqTimeout);
    }
  }

  CRANE_TRACE("Container event stream loop ended");
}

void CriClient::HandleContainerEvent_(
    const api::ContainerEventResponse& event) {
  // Only propagate concerned events using label selection
  if (!event.pod_sandbox_status().labels().contains(kCriDefaultLabel)) return;

  // Invoke the callback if set
  util::lock_guard lock(m_event_callback_mutex_);
  if (m_event_callback_) {
    m_event_callback_(event);
  }
}

// ===== Active Container Status Query Implementation =====

CraneExpected<std::vector<api::Container>> CriClient::ListContainers() const {
  using api::ListContainersRequest;
  using api::ListContainersResponse;

  ListContainersRequest request{};
  ListContainersResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

  auto status = m_rs_stub_->ListContainers(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to list containers: {}", status.error_message());
    return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
  }

  std::vector<api::Container> containers;
  containers.reserve(response.containers_size());
  for (const auto& container : response.containers()) {
    containers.push_back(container);
  }

  CRANE_TRACE("Listed {} containers", containers.size());
  return containers;
}

CraneExpected<std::vector<api::Container>> CriClient::ListContainers(
    const std::map<std::string, std::string>& label_selector) const {
  using api::ContainerFilter;
  using api::ListContainersRequest;
  using api::ListContainersResponse;

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
                       kCriDefaultReqTimeout);

  auto status = m_rs_stub_->ListContainers(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to list containers by labels: {}",
                status.error_message());
    return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
  }

  std::vector<api::Container> containers;
  containers.reserve(response.containers_size());
  for (const auto& container : response.containers()) {
    containers.push_back(container);
  }

  CRANE_TRACE("Listed {} containers matching labels", containers.size());
  return containers;
}

}  // namespace cri
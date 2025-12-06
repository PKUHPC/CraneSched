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
    const std::string& image_name) const {
  using api::ImageStatusRequest;
  using api::ImageStatusResponse;

  ImageStatusRequest request{};
  ImageStatusResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

  auto* image = request.mutable_image();
  image->set_image(image_name);

  auto status = m_is_stub_->ImageStatus(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to get image status: {}", status.error_message());
    return std::nullopt;
  }

  if (!response.has_image()) return std::nullopt;

  return response.image().id();
}

std::optional<std::string> CriClient::PullImage(
    const std::string& image_name, const std::string& username,
    const std::string& password, const std::string& server_addr,
    const std::string& pull_policy) const {
  using api::PullImageRequest;
  using api::PullImageResponse;

  // Determine the effective pull policy
  std::string effective_policy = pull_policy;
  if (effective_policy.empty()) {
    // Smart default: Always for :latest or untagged, IfNotPresent for versioned
    size_t colon_pos = image_name.find_last_of(':');
    size_t at_pos = image_name.find_last_of('@');

    bool has_digest = (at_pos != std::string::npos);
    bool has_tag = (colon_pos != std::string::npos && !has_digest);

    if (!has_tag || (has_tag && image_name.substr(colon_pos + 1) == "latest")) {
      effective_policy = "Always";
      CRANE_TRACE(
          "Image {} uses default pull policy: Always (untagged or :latest)",
          image_name);
    } else {
      effective_policy = "IfNotPresent";
      CRANE_TRACE(
          "Image {} uses default pull policy: IfNotPresent (versioned tag)",
          image_name);
    }
  }

  // Handle "Never" policy
  if (effective_policy == "Never") {
    auto image_id = GetImageId(image_name);
    if (image_id.has_value()) {
      CRANE_TRACE("Using local image {} (pull policy: Never)", image_name);
      return image_id;
    } else {
      CRANE_ERROR("Image {} not found locally and pull policy is Never",
                  image_name);
      return std::nullopt;
    }
  }

  // Handle "IfNotPresent" policy
  if (effective_policy == "IfNotPresent") {
    auto image_id = GetImageId(image_name);
    if (image_id.has_value()) {
      CRANE_TRACE("Using existing local image {} (pull policy: IfNotPresent)",
                  image_name);
      return image_id;
    }
    CRANE_TRACE(
        "Image {} not found locally, pulling from registry (pull policy: "
        "IfNotPresent)",
        image_name);
  }

  // Handle "Always" policy (or fallback for IfNotPresent when image not found)
  CRANE_TRACE("Pulling image {} from registry (pull policy: {})", image_name,
              effective_policy);

  PullImageRequest request{};
  PullImageResponse response{};

  // TODO: Make it configurable in config files, now setting to 1 minutes
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultImagePullingTimeout);

  auto* image = request.mutable_image();
  image->set_image(image_name);

  auto* auth = request.mutable_auth();
  auth->set_username(username);
  auth->set_password(password);
  auth->set_server_address(server_addr);

  auto status = m_is_stub_->PullImage(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to pull image: {}", status.error_message());
    return std::nullopt;
  }

  if (response.image_ref().empty()) {
    CRANE_ERROR("Empty image ID returned after pulling image {}", image_name);
    return std::nullopt;
  }

  return response.image_ref();
}

CraneExpectedRich<std::string> CriClient::RunPodSandbox(
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
    std::string error_msg = std::format(
        "Failed to run pod sandbox: [gRPC:{}] {}",
        static_cast<int>(status.error_code()), status.error_message());
    CRANE_ERROR("{}", error_msg);
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_CRI_GENERIC, error_msg));
  }

  return response.pod_sandbox_id();
}

CraneExpectedRich<void> CriClient::StopPodSandbox(
    const std::string& pod_sandbox_id) const {
  using api::StopPodSandboxRequest;
  using api::StopPodSandboxResponse;

  StopPodSandboxRequest request{};
  StopPodSandboxResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

  request.set_pod_sandbox_id(pod_sandbox_id);
  auto status = m_rs_stub_->StopPodSandbox(&context, request, &response);
  if (!status.ok()) {
    std::string error_msg = std::format(
        "Failed to stop pod sandbox '{}': [gRPC:{}] {}", pod_sandbox_id,
        static_cast<int>(status.error_code()), status.error_message());
    CRANE_ERROR("{}", error_msg);
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_CRI_GENERIC, error_msg));
  }

  return {};
}

CraneExpectedRich<void> CriClient::RemovePodSandbox(
    const std::string& pod_sandbox_id) const {
  using api::RemovePodSandboxRequest;
  using api::RemovePodSandboxResponse;

  RemovePodSandboxRequest request{};
  RemovePodSandboxResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

  request.set_pod_sandbox_id(pod_sandbox_id);
  auto status = m_rs_stub_->RemovePodSandbox(&context, request, &response);
  if (!status.ok()) {
    std::string error_msg = std::format(
        "Failed to remove pod sandbox '{}': [gRPC:{}] {}", pod_sandbox_id,
        static_cast<int>(status.error_code()), status.error_message());
    CRANE_ERROR("{}", error_msg);
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_CRI_GENERIC, error_msg));
  }

  return {};
}

CraneExpectedRich<api::PodSandboxStatus> CriClient::GetPodSandboxStatus(
    const std::string& pod_sandbox_id, bool verbose) const {
  using api::PodSandboxStatusRequest;
  using api::PodSandboxStatusResponse;

  PodSandboxStatusRequest request{};
  PodSandboxStatusResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

  request.set_pod_sandbox_id(pod_sandbox_id);
  request.set_verbose(verbose);

  auto status = m_rs_stub_->PodSandboxStatus(&context, request, &response);
  if (!status.ok()) {
    std::string error_msg = std::format(
        "Failed to get pod sandbox status '{}': [gRPC:{}] {}", pod_sandbox_id,
        static_cast<int>(status.error_code()), status.error_message());
    CRANE_ERROR("{}", error_msg);
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_CRI_GENERIC, error_msg));
  }

  if (!response.has_status()) {
    std::string error_msg = std::format(
        "Empty status returned for pod sandbox '{}'", pod_sandbox_id);
    CRANE_ERROR("{}", error_msg);
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_CRI_GENERIC, error_msg));
  }

  return response.status();
}

CraneExpectedRich<std::vector<api::PodSandbox>> CriClient::ListPodSandbox()
    const {
  using api::ListPodSandboxRequest;
  using api::ListPodSandboxResponse;

  ListPodSandboxRequest request{};
  ListPodSandboxResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

  auto status = m_rs_stub_->ListPodSandbox(&context, request, &response);
  if (!status.ok()) {
    std::string error_msg = std::format(
        "Failed to list pod sandboxes: [gRPC:{}] {}",
        static_cast<int>(status.error_code()), status.error_message());
    CRANE_ERROR("{}", error_msg);
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_CRI_GENERIC, error_msg));
  }

  std::vector<api::PodSandbox> sandboxes;
  sandboxes.reserve(response.items_size());
  for (const auto& sandbox : response.items()) {
    sandboxes.push_back(sandbox);
  }

  CRANE_TRACE("Listed {} pod sandboxes", sandboxes.size());
  return sandboxes;
}

CraneExpectedRich<std::vector<api::PodSandbox>> CriClient::ListPodSandbox(
    const std::unordered_map<std::string, std::string>& label_selector) const {
  using api::ListPodSandboxRequest;
  using api::ListPodSandboxResponse;

  ListPodSandboxRequest request{};
  ListPodSandboxResponse response{};

  // Set up filter with label selector
  auto* filter = request.mutable_filter();
  auto* labels = filter->mutable_label_selector();
  for (const auto& [key, value] : label_selector) {
    (*labels)[key] = value;
  }

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

  auto status = m_rs_stub_->ListPodSandbox(&context, request, &response);
  if (!status.ok()) {
    std::string error_msg = std::format(
        "Failed to list pod sandboxes by labels: [gRPC:{}] {}",
        static_cast<int>(status.error_code()), status.error_message());
    CRANE_ERROR("{}", error_msg);
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_CRI_GENERIC, error_msg));
  }

  std::vector<api::PodSandbox> sandboxes;
  sandboxes.reserve(response.items_size());
  for (const auto& sandbox : response.items()) {
    sandboxes.push_back(sandbox);
  }

  CRANE_TRACE("Listed {} pod sandboxes matching labels", sandboxes.size());
  return sandboxes;
}

CraneExpectedRich<std::string> CriClient::GetPodSandboxId(
    const std::unordered_map<std::string, std::string>& label_selector) const {
  auto sandboxes_expt = ListPodSandbox(label_selector);
  if (!sandboxes_expt) {
    return std::unexpected(sandboxes_expt.error());
  }

  const auto& sandboxes = sandboxes_expt.value();
  if (sandboxes.empty()) {
    std::string error_msg = "No pod sandbox found matching labels";
    CRANE_ERROR("{}", error_msg);
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_CRI_GENERIC, error_msg));
  }

  if (sandboxes.size() > 1) {
    std::string error_msg = std::format(
        "Multiple pod sandboxes ({}) found matching labels",
        sandboxes.size());
    CRANE_ERROR("{}", error_msg);
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_CRI_GENERIC, error_msg));
  }

  return sandboxes.front().id();
}

CraneExpectedRich<std::string> CriClient::CreateContainer(
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
    std::string error_msg = std::format(
        "Failed to create container in pod '{}': [gRPC:{}] {}", pod_id,
        static_cast<int>(status.error_code()), status.error_message());
    CRANE_ERROR("{}", error_msg);
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_CRI_GENERIC, error_msg));
  }

  return response.container_id();
}

CraneExpectedRich<void> CriClient::StartContainer(
    const std::string& container_id) const {
  using api::StartContainerRequest;
  using api::StartContainerResponse;

  StartContainerRequest request{};
  StartContainerResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

  request.set_container_id(container_id);
  auto status = m_rs_stub_->StartContainer(&context, request, &response);
  if (!status.ok()) {
    std::string error_msg = std::format(
        "Failed to start container '{}': [gRPC:{}] {}", container_id,
        static_cast<int>(status.error_code()), status.error_message());
    CRANE_ERROR("{}", error_msg);
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_CRI_GENERIC, error_msg));
  }

  return {};
}

CraneExpectedRich<void> CriClient::StopContainer(
    const std::string& container_id, int64_t timeout) const {
  using api::StopContainerRequest;
  using api::StopContainerResponse;

  StopContainerRequest request{};
  StopContainerResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

  request.set_container_id(container_id);
  request.set_timeout(timeout);
  auto status = m_rs_stub_->StopContainer(&context, request, &response);
  if (!status.ok()) {
    std::string error_msg = std::format(
        "Failed to stop container '{}': [gRPC:{}] {}", container_id,
        static_cast<int>(status.error_code()), status.error_message());
    CRANE_ERROR("{}", error_msg);
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_CRI_GENERIC, error_msg));
  }

  return {};
}

CraneExpectedRich<void> CriClient::RemoveContainer(
    const std::string& container_id) const {
  using api::RemoveContainerRequest;
  using api::RemoveContainerResponse;

  RemoveContainerRequest request{};
  RemoveContainerResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

  request.set_container_id(container_id);
  auto status = m_rs_stub_->RemoveContainer(&context, request, &response);
  if (!status.ok()) {
    std::string error_msg = std::format(
        "Failed to remove container '{}': [gRPC:{}] {}", container_id,
        static_cast<int>(status.error_code()), status.error_message());
    CRANE_ERROR("{}", error_msg);
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_CRI_GENERIC, error_msg));
  }

  return {};
}

CraneExpectedRich<std::string> CriClient::Attach(
    const std::string& container_id, bool tty, bool stdin, bool stdout,
    bool stderr) const {
  using api::AttachRequest;
  using api::AttachResponse;

  AttachRequest request;
  AttachResponse response;

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

  request.set_container_id(container_id);
  request.set_tty(tty);
  request.set_stdin(stdin);
  request.set_stdout(stdout);
  request.set_stderr(stderr);

  auto status = m_rs_stub_->Attach(&context, request, &response);
  if (!status.ok()) {
    std::string error_msg = std::format(
        "Failed to attach to container '{}': [gRPC:{}] {}", container_id,
        static_cast<int>(status.error_code()), status.error_message());
    CRANE_ERROR("{}", error_msg);
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_CRI_GENERIC, error_msg));
  }

  return response.url();
}

CraneExpectedRich<std::string> CriClient::Exec(
    const std::string& container_id, const std::vector<std::string>& command,
    bool tty, bool stdin, bool stdout, bool stderr) const {
  using api::ExecRequest;
  using api::ExecResponse;

  ExecRequest request;
  ExecResponse response;

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

  request.set_container_id(container_id);
  for (const auto& cmd : command) {
    request.add_cmd(cmd);
  }
  request.set_tty(tty);
  request.set_stdin(stdin);
  request.set_stdout(stdout);
  request.set_stderr(stderr);

  auto status = m_rs_stub_->Exec(&context, request, &response);
  if (!status.ok()) {
    std::string error_msg = std::format(
        "Failed to exec in container '{}': [gRPC:{}] {}", container_id,
        static_cast<int>(status.error_code()), status.error_message());
    CRANE_ERROR("{}", error_msg);
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_CRI_GENERIC, error_msg));
  }

  return response.url();
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

CraneExpectedRich<std::vector<api::Container>> CriClient::ListContainers()
    const {
  using api::ListContainersRequest;
  using api::ListContainersResponse;

  ListContainersRequest request{};
  ListContainersResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       kCriDefaultReqTimeout);

  auto status = m_rs_stub_->ListContainers(&context, request, &response);
  if (!status.ok()) {
    std::string error_msg = std::format(
        "Failed to list containers: [gRPC:{}] {}",
        static_cast<int>(status.error_code()), status.error_message());
    CRANE_ERROR("{}", error_msg);
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_CRI_GENERIC, error_msg));
  }

  std::vector<api::Container> containers;
  containers.reserve(response.containers_size());
  for (const auto& container : response.containers()) {
    containers.push_back(container);
  }

  CRANE_TRACE("Listed {} containers", containers.size());
  return containers;
}

CraneExpectedRich<std::vector<api::Container>> CriClient::ListContainers(
    const std::unordered_map<std::string, std::string>& label_selector) const {
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
    std::string error_msg = std::format(
        "Failed to list containers by labels: [gRPC:{}] {}",
        static_cast<int>(status.error_code()), status.error_message());
    CRANE_ERROR("{}", error_msg);
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_CRI_GENERIC, error_msg));
  }

  std::vector<api::Container> containers;
  containers.reserve(response.containers_size());
  for (const auto& container : response.containers()) {
    containers.push_back(container);
  }

  CRANE_TRACE("Listed {} containers matching labels", containers.size());
  return containers;
}

CraneExpectedRich<std::string> CriClient::GetContainerId(
    const std::unordered_map<std::string, std::string>& label_selector) const {
  auto containers_expt = ListContainers(label_selector);
  if (!containers_expt) {
    return std::unexpected(containers_expt.error());
  }

  const auto& containers = containers_expt.value();
  if (containers.size() == 0) {
    std::string error_msg = "No container found matching labels";
    CRANE_ERROR("{}", error_msg);
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_CRI_GENERIC, error_msg));
  }

  if (containers.size() > 1) {
    std::string error_msg = std::format(
        "Multiple containers ({}) found matching labels", containers.size());
    CRANE_ERROR("{}", error_msg);
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_CRI_GENERIC, error_msg));
  }

  return containers.front().id();
}

}  // namespace cri

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

#include "crane/Logger.h"
#include "crane/PublicHeader.h"
#include "cri/api.pb.h"

namespace Supervisor {

CriClient::~CriClient() {
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
                       std::chrono::seconds(5));

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
                       std::chrono::seconds(5));

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
                       std::chrono::seconds(5));

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
                       std::chrono::seconds(5));

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
                       std::chrono::seconds(5));

  request.mutable_config()->CopyFrom(config);
  auto status = m_rs_stub_->RunPodSandbox(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to run pod sandbox: {}", status.error_message());
    return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
  }

  return response.pod_sandbox_id();
}

CraneExpected<std::string> CriClient::CreateContainer(
    const cri::ContainerConfig& config) const {
  using cri::CreateContainerRequest;
  using cri::CreateContainerResponse;

  CreateContainerRequest request{};
  CreateContainerResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(5));

  request.mutable_config()->CopyFrom(config);
  auto status = m_rs_stub_->CreateContainer(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to create container: {}", status.error_message());
    return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
  }

  return response.container_id();
}

CraneExpected<void> CriClient::StartContainer(std::string container_id) const {
  using cri::StartContainerRequest;
  using cri::StartContainerResponse;

  CraneExpected<void> ret;
  StartContainerRequest request{};
  StartContainerResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(5));

  request.set_container_id(std::move(container_id));
  auto status = m_rs_stub_->StartContainer(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to start container: {}", status.error_message());
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

}  // namespace Supervisor
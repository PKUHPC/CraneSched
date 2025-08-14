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
#include "cri/api.pb.h"

namespace Supervisor {

CriClient::~CriClient() {
  if (m_async_send_thread_.joinable()) {
    m_async_send_thread_.join();
  }
}

void CriClient::InitChannelAndStub(const std::string& endpoint) {
  m_channel_ = CreateUnixInsecureChannel(endpoint);
  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = runtime::v1::RuntimeService::NewStub(m_channel_);
  m_async_send_thread_ = std::thread([this] {});
}

void CriClient::Version() {
  using runtime::v1::VersionRequest;
  using runtime::v1::VersionResponse;

  VersionRequest request{};
  VersionResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(5));

  auto status = m_stub_->Version(&context, request, &response);
  if (!status.ok()) {
    CRANE_ERROR("Failed to get CRI version: {}", status.error_message());
    return;
  }

  CRANE_TRACE("CRI replied Version: version={}", response.version());
}

void CriClient::RuntimeConfig() {
  using runtime::v1::RuntimeConfigRequest;
  using runtime::v1::RuntimeConfigResponse;

  RuntimeConfigRequest request{};
  RuntimeConfigResponse response{};

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(5));

  auto status = m_stub_->RuntimeConfig(&context, request, &response);
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
              runtime::v1::CgroupDriver_Name(cg));
}

}  // namespace Supervisor
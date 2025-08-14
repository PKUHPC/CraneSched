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

#include "cri/api.grpc.pb.h"

namespace Supervisor {

namespace cri = runtime::v1;

class CriClient {
 public:
  CriClient() = default;
  ~CriClient();

  void InitChannelAndStub(const std::filesystem::path& runtime_service,
                          const std::filesystem::path& image_service);

  // FIXME: DEBUG ONLY
  void Version();
  void RuntimeConfig();

 private:
  std::thread m_async_send_thread_;
  std::atomic_bool m_thread_stop_;
  int m_finished_tasks_{0};
  std::shared_ptr<grpc::Channel> m_rs_channel_;
  std::shared_ptr<grpc::Channel> m_is_channel_;
  std::shared_ptr<cri::RuntimeService::Stub> m_rs_stub_;
  std::shared_ptr<cri::ImageService::Stub> m_is_stub_;
};

}  // namespace Supervisor

inline std::unique_ptr<Supervisor::CriClient> g_cri_client;
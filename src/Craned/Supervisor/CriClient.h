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

// Precompiled header comes first.
namespace Supervisor {

class CriClient {
 public:
  CriClient() = default;
  ~CriClient();

  void InitChannelAndStub(const std::string& endpoint);

  // FIXME: DEBUG ONLY
  void Version();
  void RuntimeConfig();

 private:
  std::thread m_async_send_thread_;
  std::atomic_bool m_thread_stop_;
  int m_finished_tasks_{0};
  std::shared_ptr<grpc::Channel> m_channel_;
  std::shared_ptr<runtime::v1::RuntimeService::Stub> m_stub_;
};

}  // namespace Supervisor

inline std::unique_ptr<Supervisor::CriClient> g_cri_client;
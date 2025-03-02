/**
 * Copyright (c) 2024 Peking University and Peking University
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
#include "SupervisorPublicDefs.h"
// Precompiled header comes first.

#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"

namespace Supervisor {
class CranedClient {
 public:
  ~CranedClient();
  void InitChannelAndStub(const std::string& endpoint);
  void TaskStatusChangeAsync(crane::grpc::TaskStatus new_status,
                             uint32_t exit_code,
                             std::optional<std::string> reason);

 private:
  void AsyncSendThread_();
  struct TaskStatusChangeQueueElem {
    task_id_t task_id{};
    crane::grpc::TaskStatus new_status{};
    uint32_t exit_code{};
    std::optional<std::string> reason;
  };
  moodycamel::ConcurrentQueue<TaskStatusChangeQueueElem>
      m_task_status_change_queue_;

  std::thread m_async_send_thread_;
  std::atomic_bool m_thread_stop_;
  std::shared_ptr<grpc::Channel> m_channel_;
  std::shared_ptr<crane::grpc::Craned::Stub> m_stub_;
};

}  // namespace Supervisor

inline std::unique_ptr<Supervisor::CranedClient> g_craned_client;
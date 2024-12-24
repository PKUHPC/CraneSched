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

#include "SupervisorKeeper.h"

#include <protos/Supervisor.grpc.pb.h>
#include <sys/stat.h>

namespace Craned {
void SupervisorClient::InitChannelAndStub(const std::string& endpoint) {
  m_channel_ = CreateUnixInsecureChannel(endpoint);
  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = crane::grpc::Supervisor::NewStub(m_channel_);
}

SupervisorKeeper::SupervisorKeeper() {
  try {
    std::filesystem::path path = kDefaultSupervisorUnixSockDir;
    if (std::filesystem::exists(path) && std::filesystem::is_directory(path)) {
      // 遍历目录中的所有条目
      for (const auto& it : std::filesystem::directory_iterator(path)) {
        if (std::filesystem::is_socket(it.path())) {
          g_thread_pool->detach_task(
              [this, it]() { this->RecoverSupervisorMt_(it.path()); });
        }
      }
    } else {
      CRANE_WARN("Supervisor socket dir dose not exit, skip recovery.");
    }
  } catch (const std::exception& e) {
    CRANE_ERROR("Error: {}, when recover supervisor", e.what());
  }
}

void SupervisorKeeper::AddSupervisor(task_id_t task_id) {
  auto sock_path = fmt::format("unix://{}/task_{}.sock",
                               kDefaultSupervisorUnixSockDir, task_id);
  std::shared_ptr stub = std::make_shared<SupervisorClient>();
  stub->InitChannelAndStub(sock_path);
  absl::WriterMutexLock lk(&m_mutex);
  if (auto it = m_supervisor_map.find(task_id); it != m_supervisor_map.end()) {
    CRANE_ERROR("Duplicate supervisor for task #{}", task_id);
    return;
  }
  m_supervisor_map.emplace(task_id, stub);
}

std::shared_ptr<SupervisorClient> SupervisorKeeper::GetStub(task_id_t task_id) {
  absl::ReaderMutexLock lk(&m_mutex);
  if (auto it = m_supervisor_map.find(task_id); it != m_supervisor_map.end()) {
    return it->second;
  } else {
    return nullptr;
  }
}
void SupervisorKeeper::RecoverSupervisorMt_(const std::filesystem::path& path) {
  std::shared_ptr stub = std::make_shared<SupervisorClient>();
  stub->InitChannelAndStub(path);
  crane::grpc::TaskToD task;
  auto err = stub->CheckTaskStatus(&task);
  if (err != CraneErr::kOk) {
    CRANE_ERROR("CheckTaskStatus for {} failed: {}", path, err);
    return;
  }
  absl::WriterMutexLock lk(&m_mutex);
  m_supervisor_map.emplace(task.task_id(), stub);
  g_task_mgr->AddRecoveredTask_(task);
}

}  // namespace Craned
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

#include <latch>

namespace Craned {
void SupervisorClient::InitChannelAndStub(const std::string& endpoint) {
  m_channel_ = CreateUnixInsecureChannel(endpoint);
  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = crane::grpc::Supervisor::NewStub(m_channel_);
}

CraneExpected<std::unordered_map<task_id_t, TaskStatusSpce>>
SupervisorKeeper::Init() {
  try {
    std::filesystem::path path = kDefaultSupervisorUnixSockDir;
    if (std::filesystem::exists(path) && std::filesystem::is_directory(path)) {
      std::vector<std::filesystem::path> files;
      for (const auto& it : std::filesystem::directory_iterator(path)) {
        if (std::filesystem::is_socket(it.path())) {
          files.emplace_back(it.path());
        }
      }
      std::unordered_map<task_id_t, TaskStatusSpce> tasks;
      tasks.reserve(files.size());
      std::latch all_supervisor_reply(files.size());
      absl::Mutex mtx;
      for (const auto& file : files) {
        g_thread_pool->detach_task(
            [this, &file, &all_supervisor_reply, &mtx, &tasks]() {
              auto task = this->RecoverSupervisorMt_(file);
              all_supervisor_reply.count_down();
              if (!task) return;
              absl::WriterMutexLock lk(&mtx);
              tasks.emplace(task->task_spec.task_id(), task.value());
            });
      }
      all_supervisor_reply.wait();
      return tasks;
    } else {
      CRANE_WARN("Supervisor socket dir dose not exit, skip recovery.");
      return {};
    }
  } catch (const std::exception& e) {
    CRANE_ERROR("Error: {}, when recover supervisor", e.what());
    return std::unexpected(CraneErr::kSystemErr);
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

CraneExpected<TaskStatusSpce> SupervisorKeeper::RecoverSupervisorMt_(
    const std::filesystem::path& path) {
  std::shared_ptr stub = std::make_shared<SupervisorClient>();
  stub->InitChannelAndStub(path);
  auto supervisor_state = stub->CheckTaskStatus();
  if (!supervisor_state) {
    CRANE_ERROR("CheckTaskStatus for {} failed", path.string());
    return std::unexpected(CraneErr::kSupervisorError);
  }
  absl::WriterMutexLock lk(&m_mutex);
  m_supervisor_map.emplace(supervisor_state.value().task_spec.task_id(), stub);
  return supervisor_state;
}

}  // namespace Craned
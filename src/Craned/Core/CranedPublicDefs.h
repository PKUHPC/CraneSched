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

#include <filesystem>

#include "PreCompiledHeader.h"
// Precompiled header comes first

#include "CommonPublicDefs.h"
#include "crane/Network.h"
#include "crane/OS.h"

namespace Craned {

inline constexpr uint64_t kEvSigChldResendMs = 500;
constexpr uint64_t kCtldClientTimeoutSec = 30;
constexpr int64_t kCranedRpcTimeoutSeconds = 5;

using EnvMap = std::unordered_map<std::string, std::string>;
using RegToken = google::protobuf::Timestamp;

enum class CallbackInvokeMode : std::uint8_t { SYNC = 0, ASYNC };

template <typename Cb, typename... Args>
  requires std::invocable<Cb, Args...>
struct CallbackWrapper {
  Cb cb;
  CallbackInvokeMode mode;
  bool consume;
};

struct TaskStatusChangeQueueElem {
  task_id_t step_id{};
  crane::grpc::TaskStatus new_status{};
  uint32_t exit_code{};
  std::optional<std::string> reason;
};

struct TaskInfoOfUid {
  uint32_t job_cnt;
  uint32_t first_task_id;
  bool cgroup_exists;
  std::string cgroup_path;
};

struct RunTimeStatus {
  std::shared_ptr<spdlog::async_logger> conn_logger;
};
using namespace Common;

inline RunTimeStatus g_runtime_status{};
}  // namespace Craned

inline std::unique_ptr<BS::thread_pool> g_thread_pool;
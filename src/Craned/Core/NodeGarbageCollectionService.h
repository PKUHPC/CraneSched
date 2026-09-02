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

#include "CranedPublicDefs.h"
// Precompiled header comes first.

namespace Craned {

struct NodeGcContext {
  absl::Time now;
  std::map<job_id_t, std::map<step_id_t, StepStatus>> active_steps;
};

struct NodeGcTaskRunStats {
  uint64_t scanned{0};
  uint64_t deleted{0};
  uint64_t skipped{0};
};

struct NodeGcRunStats {
  uint64_t task_count{0};
  uint64_t total_scanned{0};
  uint64_t total_deleted{0};
  uint64_t total_skipped{0};
  bool skipped_due_to_running{false};
  bool skipped_due_to_stopping{false};
  std::chrono::milliseconds elapsed{0};
};

class INodeGcTask {
 public:
  virtual ~INodeGcTask() = default;

  virtual std::string_view Name() const = 0;
  virtual NodeGcTaskRunStats Run(const NodeGcContext& ctx) = 0;
};

class NodeGarbageCollectionService {
 public:
  NodeGarbageCollectionService() = default;
  ~NodeGarbageCollectionService() = default;

  NodeGarbageCollectionService(const NodeGarbageCollectionService&) = delete;
  NodeGarbageCollectionService(NodeGarbageCollectionService&&) = delete;
  NodeGarbageCollectionService& operator=(const NodeGarbageCollectionService&) =
      delete;
  NodeGarbageCollectionService& operator=(NodeGarbageCollectionService&&) =
      delete;

  void RegisterTask(std::unique_ptr<INodeGcTask> task);
  [[nodiscard]] NodeGcRunStats RunOnceIfNeeded(const NodeGcContext& ctx);
  void Stop();

 private:
  std::vector<std::unique_ptr<INodeGcTask>> m_tasks_ ABSL_GUARDED_BY(m_mtx_);
  std::atomic_bool m_running_{false};
  std::atomic_bool m_stopping_{false};
  absl::Mutex m_mtx_;
};

}  // namespace Craned

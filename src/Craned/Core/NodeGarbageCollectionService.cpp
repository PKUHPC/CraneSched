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

#include "NodeGarbageCollectionService.h"
// Precompiled header comes first.

namespace Craned {

void NodeGarbageCollectionService::RegisterTask(
    std::unique_ptr<INodeGcTask> task) {
  if (!task) {
    CRANE_WARN("[NodeGC] Ignore null GC task registration request.");
    return;
  }

  const auto task_name = std::string(task->Name());
  absl::MutexLock lk(&m_mtx_);
  m_tasks_.emplace_back(std::move(task));
  CRANE_INFO("[NodeGC] Registered task '{}'.", task_name);
}

NodeGcRunStats NodeGarbageCollectionService::RunOnceIfNeeded(
    const NodeGcContext& ctx) {
  NodeGcRunStats run_stats{};

  if (m_stopping_.load(std::memory_order_acquire)) {
    run_stats.skipped_due_to_stopping = true;
    return run_stats;
  }

  bool expected = false;
  if (!m_running_.compare_exchange_strong(expected, true,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire)) {
    run_stats.skipped_due_to_running = true;
    return run_stats;
  }

  const auto start = std::chrono::steady_clock::now();

  auto release_running = [this] {
    m_running_.store(false, std::memory_order_release);
  };

  try {
    absl::MutexLock lk(&m_mtx_);
    run_stats.task_count = m_tasks_.size();

    for (const auto& task : m_tasks_) {
      if (m_stopping_.load(std::memory_order_acquire)) {
        run_stats.skipped_due_to_stopping = true;
        break;
      }

      try {
        const auto task_stats = task->Run(ctx);
        run_stats.total_scanned += task_stats.scanned;
        run_stats.total_deleted += task_stats.deleted;
        run_stats.total_skipped += task_stats.skipped;

        CRANE_DEBUG("[NodeGC] task='{}' scanned={} deleted={} skipped={}.",
                    task->Name(), task_stats.scanned, task_stats.deleted,
                    task_stats.skipped);
      } catch (const std::exception& ex) {
        CRANE_ERROR("[NodeGC] task='{}' failed with exception: {}.",
                    task->Name(), ex.what());
      } catch (...) {
        CRANE_ERROR("[NodeGC] task='{}' failed with unknown exception.",
                    task->Name());
      }
    }
  } catch (const std::exception& ex) {
    CRANE_ERROR("[NodeGC] service run failed with exception: {}.", ex.what());
  } catch (...) {
    CRANE_ERROR("[NodeGC] service run failed with unknown exception.");
  }

  run_stats.elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start);

  if (!run_stats.skipped_due_to_stopping) {
    CRANE_DEBUG(
        "[NodeGC] run finished tasks={} scanned={} deleted={} skipped={} "
        "elapsed_ms={} active_jobs={}.",
        run_stats.task_count, run_stats.total_scanned, run_stats.total_deleted,
        run_stats.total_skipped, run_stats.elapsed.count(),
        ctx.active_steps.size());
  }

  release_running();
  return run_stats;
}

void NodeGarbageCollectionService::Stop() {
  m_stopping_.store(true, std::memory_order_release);
}

}  // namespace Craned

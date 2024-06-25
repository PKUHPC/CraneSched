/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <unqlite.h>

#include "CtldPublicDefs.h"
#include "crane/PublicHeader.h"
#include "protos/PublicDefs.pb.h"

namespace Ctld {

class DependencyManager {
  using Mutex = absl::Mutex;
  using LockGuard = absl::MutexLock;

  template <typename K, typename V,
            typename Hash = absl::container_internal::hash_default_hash<K>>
  using HashMap = absl::flat_hash_map<K, V, Hash>;

  struct Info {
    enum ListStatus {
      WAITING = 0,
      SUCCEED = 1,
      FAILED = 2,
    };

    task_id_t task_id;
    bool depend_all;

    Mutex mutex;
    std::vector<std::shared_ptr<Info>>
        Dependents[crane::grpc::DependencyType_ARRAYSIZE];
    ListStatus listStatus[crane::grpc::DependencyType_ARRAYSIZE];
  };

 public:
  DependencyManager();
  ~DependencyManager() = default;

  result::result<void, std::string> addDependencies(
      task_id_t task_id, crane::grpc::Dependencies& dependencies);

  // Update dependencies, return the list of meet dependencies(to remove from
  // TaskInCtld), success tasks(to clear all dependencies), failed tasks(to
  // change TaskStatus to FAILED)
  bool updateDependencies(
      task_id_t task_id, crane::grpc::TaskStatus new_status, int exit_code,
      std::unordered_map<task_id_t, std::vector<task_id_t>>* dependencies,
      std::vector<task_id_t>* success_tasks,
      std::vector<task_id_t>* failed_tasks);

  // Will only be called by TaskScheduler::Init, lock is not needed
  void RecoverFromSnapshot(
      std::unordered_map<task_db_id_t, crane::grpc::TaskInEmbeddedDb>
          pending_queue);

 private:
  void clearList(
      const std::shared_ptr<Info>& info, crane::grpc::DependencyType type,
      Info::ListStatus status,
      std::unordered_map<task_id_t, std::vector<task_id_t>>* dependencies,
      std::vector<task_id_t>* success_tasks,
      std::vector<task_id_t>* failed_tasks);

  HashMap<task_id_t, std::shared_ptr<Info>> g_all_task_info
      ABSL_GUARDED_BY(g_dependency_mutex);
  Mutex g_dependency_mutex;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::DependencyManager> g_dependency_manager;
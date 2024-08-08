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

#include "DependencyManager.h"

#include <memory>
#include <utility>

#include "EmbeddedDbClient.h"
#include "crane/Logger.h"
#include "crane/PublicHeader.h"
#include "protos/PublicDefs.pb.h"

namespace Ctld {

DependencyManager::DependencyManager() {}

result::result<void, std::string> DependencyManager::addDependencies(
    task_id_t task_id, crane::grpc::Dependencies& dependencies) {
  std::shared_ptr<Info> self = std::make_shared<Info>();
  self->task_id = task_id;
  self->depend_all = dependencies.depend_all();

  std::vector<std::pair<std::shared_ptr<Info>, crane::grpc::DependencyType>>
      dependent_infos;
  {
    LockGuard global_lock(&g_dependency_mutex);
    for (const auto& dep : dependencies.dependencies()) {
      task_id_t dep_id = dep.task_id();
      auto it = g_all_task_info.find(dep_id);
      if (it == g_all_task_info.end()) {
        return result::fail(fmt::format(
            "Dependency task #{} does not exist or has ended", dep_id));
      }
      dependent_infos.push_back({it->second, dep.type()});
    }

    if (g_all_task_info.find(task_id) != g_all_task_info.end()) {
      return result::fail(fmt::format("Task #{} already exists", task_id));
    }
    g_all_task_info[task_id] = self;
  }

  int succeeded = 0;
  int failed = 0;

  crane::grpc::Dependencies new_dependencies;
  new_dependencies.set_depend_all(dependencies.depend_all());
  for (int i = 0; i < dependent_infos.size(); ++i) {
    auto& [dep_info, dep_type] = dependent_infos[i];
    LockGuard lock(&dep_info->mutex);
    if (dep_info->listStatus[dep_type] == 0) {
      dep_info->Dependents[dep_type].push_back(self);
      new_dependencies.add_dependencies()->CopyFrom(
          dependencies.dependencies(i));
    } else {
      dep_info->listStatus[dep_type] == Info::SUCCEED ? ++succeeded : ++failed;
    }
  }

  if (failed > 0 &&
      (dependencies.depend_all() || failed >= dependent_infos.size())) {
    // current task has no dependent before the function returns
    // just try to remove the task from the global map
    // then no dependent of current task will be added
    LockGuard lock(&g_dependency_mutex);
    g_all_task_info.erase(task_id);
    return result::fail("Dependencies already failed");
  } else if (succeeded > 0 && (!dependencies.depend_all() ||
                               succeeded >= dependent_infos.size())) {
    new_dependencies.mutable_dependencies()->Clear();
  }
  dependencies = new_dependencies;
  return {};
}

bool DependencyManager::updateDependencies(
    task_id_t task_id, crane::grpc::TaskStatus new_status, int exit_code,
    std::unordered_map<task_id_t, std::vector<task_id_t>>* dependencies,
    std::vector<task_id_t>* success_tasks,
    std::vector<task_id_t>* failed_tasks) {
  if (new_status == crane::grpc::TaskStatus::Pending) return {};

  std::shared_ptr<Info> self;
  {
    LockGuard global_lock(&g_dependency_mutex);
    auto it = g_all_task_info.find(task_id);
    if (it == g_all_task_info.end()) {
      return false;
    }
    self = it->second;
    if (new_status != crane::grpc::TaskStatus::Running) {
      // remove task from the global map to avoid adding dependents
      g_all_task_info.erase(it);
    }
  }

  if (new_status == crane::grpc::TaskStatus::Running) {
    clearList(self, crane::grpc::AFTER, Info::SUCCEED, dependencies,
              success_tasks, failed_tasks);
  } else if (new_status == crane::grpc::TaskStatus::Cancelled) {
    clearList(self, crane::grpc::AFTER, Info::SUCCEED, dependencies,
              success_tasks, failed_tasks);
    clearList(self, crane::grpc::AFTER_ANY, Info::SUCCEED, dependencies,
              success_tasks, failed_tasks);
    clearList(self, crane::grpc::AFTER_OK, Info::FAILED, dependencies,
              success_tasks, failed_tasks);
    clearList(self, crane::grpc::AFTER_NOT_OK, Info::SUCCEED, dependencies,
              success_tasks, failed_tasks);
  } else if (new_status == crane::grpc::TaskStatus::Completed) {
    clearList(self, crane::grpc::AFTER_ANY, Info::SUCCEED, dependencies,
              success_tasks, failed_tasks);
    clearList(self, crane::grpc::AFTER_OK,
              exit_code == 0 ? Info::SUCCEED : Info::FAILED, dependencies,
              success_tasks, failed_tasks);
    clearList(self, crane::grpc::AFTER_NOT_OK,
              exit_code == 0 ? Info::FAILED : Info::SUCCEED, dependencies,
              success_tasks, failed_tasks);
  } else if (new_status == crane::grpc::TaskStatus::Failed ||
             new_status == crane::grpc::TaskStatus::ExceedTimeLimit) {
    clearList(self, crane::grpc::AFTER, Info::FAILED, dependencies,
              success_tasks, failed_tasks);
    clearList(self, crane::grpc::AFTER_ANY, Info::SUCCEED, dependencies,
              success_tasks, failed_tasks);
    clearList(self, crane::grpc::AFTER_OK, Info::FAILED, dependencies,
              success_tasks, failed_tasks);
    clearList(self, crane::grpc::AFTER_NOT_OK, Info::SUCCEED, dependencies,
              success_tasks, failed_tasks);
  } else {
    CRANE_ERROR("Unknown task status: {}", static_cast<int>(new_status));
  }

  return true;
}

void DependencyManager::clearList(
    const std::shared_ptr<Info>& info, crane::grpc::DependencyType type,
    Info::ListStatus status,
    std::unordered_map<task_id_t, std::vector<task_id_t>>* dependencies,
    std::vector<task_id_t>* success_tasks,
    std::vector<task_id_t>* failed_tasks) {
  std::vector<std::shared_ptr<Info>> to_update;
  {
    LockGuard lock(&info->mutex);
    if (info->listStatus[type] != 0) return;
    info->listStatus[type] = status;
    std::swap(to_update, info->Dependents[type]);
  }
  for (auto& dep_info : to_update) {
    if (dep_info->depend_all && status == Info::FAILED) {
      failed_tasks->push_back(dep_info->task_id);
      clearList(dep_info, crane::grpc::AFTER, Info::FAILED, dependencies,
                success_tasks, failed_tasks);
      clearList(dep_info, crane::grpc::AFTER_ANY, Info::SUCCEED, dependencies,
                success_tasks, failed_tasks);
      clearList(dep_info, crane::grpc::AFTER_OK, Info::FAILED, dependencies,
                success_tasks, failed_tasks);
      clearList(dep_info, crane::grpc::AFTER_NOT_OK, Info::SUCCEED,
                dependencies, success_tasks, failed_tasks);
    } else if (!dep_info->depend_all && status == Info::SUCCEED) {
      success_tasks->push_back(dep_info->task_id);
    } else {
      (*dependencies)[dep_info->task_id].push_back(info->task_id);
    }
  }
}

void DependencyManager::RecoverFromSnapshot(
    std::unordered_map<task_db_id_t, crane::grpc::TaskInEmbeddedDb>
        pending_queue) {
  for (const auto& [task_db_id, task] : pending_queue) {
    const auto& runtime_attr = task.runtime_attr();
    const auto& task_to_ctld = task.task_to_ctld();
    task_id_t task_id = runtime_attr.task_id();
    const auto& submitted_dependencies = task_to_ctld.dependencies();
    const auto& updated_dependencies = runtime_attr.dependency_ids();
    if (g_all_task_info.find(task_id) == g_all_task_info.end()) {
      g_all_task_info[task_id] = std::make_shared<Info>();
      g_all_task_info[task_id]->task_id = task_id;
    }
    g_all_task_info[task_id]->depend_all = submitted_dependencies.depend_all();
    if (submitted_dependencies.dependencies_size() == 0 ||
        runtime_attr.dependency_ok())
      continue;
    std::unordered_map<task_id_t, crane::grpc::DependencyType>
        rest_dependencies;
    for (auto dep_info : submitted_dependencies.dependencies()) {
      rest_dependencies.emplace(dep_info.task_id(), dep_info.type());
    }
    for (auto dep_id : updated_dependencies) {
      if (!rest_dependencies.erase(dep_id)) {
        CRANE_ERROR("dependency #{} of Task#{} not exist.", dep_id, task_id);
        continue;
      }
    }
    if (rest_dependencies.empty()) {
      CRANE_ERROR("Task #{} has no dependency rested but dependency unmet.",
                  task_id);
      continue;
    }
    for (const auto& [dep_id, dep_type] : rest_dependencies) {
      if (g_all_task_info.find(dep_id) == g_all_task_info.end()) {
        g_all_task_info[dep_id] = std::make_shared<Info>();
        g_all_task_info[dep_id]->task_id = dep_id;
      }
      auto dep_info = g_all_task_info[dep_id];
      // If task status change has been written to db, then all changes to
      // dependency are also written. So recovered dependencies can be added
      // without check.
      // While the list state still need to be set to avoid invalid new
      // dependencies. this step is done in updateDependencies called by
      // TaskScheduler::Init.
      dep_info->Dependents[dep_type].push_back(g_all_task_info[task_id]);
    }
  }
}

}  // namespace Ctld
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

#include "JobManager.h"

#include "crane/PluginClient.h"

namespace Craned {

EnvMap JobSpec::GetJobEnvMap() const {
  auto env_map = CgroupManager::GetResourceEnvMapByResInNode(
      this->cgroup_spec.res_in_node);

  // TODO: Move all job level env to here.
  env_map.emplace("CRANE_JOB_ID", std::to_string(this->cgroup_spec.job_id));
  return env_map;
}

bool JobManager::AllocJobs(std::vector<JobSpec>&& job_specs) {
  CRANE_DEBUG("Allocating {} job", job_specs.size());

  auto begin = std::chrono::steady_clock::now();

  for (auto& job_spec : job_specs) {
    task_id_t job_id = job_spec.cgroup_spec.job_id;
    uid_t uid = job_spec.cgroup_spec.uid;
    CRANE_TRACE("Create lazily allocated cgroups for job #{}, uid {}", job_id,
                uid);
    m_job_map_.Emplace(job_id, JobInstance(job_spec));

    auto uid_map = m_uid_to_job_ids_map_.GetMapExclusivePtr();
    if (uid_map->contains(uid)) {
      uid_map->at(uid).RawPtr()->emplace(job_id);
    } else {
      uid_map->emplace(uid, absl::flat_hash_set<task_id_t>({job_id}));
    }
  }

  auto end = std::chrono::steady_clock::now();
  CRANE_TRACE("Allocating cgroups costed {} ms",
              std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
                  .count());
  return true;
}

CgroupInterface* JobManager::GetCgForJob(task_id_t job_id) {
  CgroupSpec spec;
  {
    auto job = m_job_map_.GetValueExclusivePtr(job_id);
    if (!job) {
      return nullptr;
    }
    if (job->cgroup) {
      return job->cgroup.get();
    }
    spec = job->job_spec.cgroup_spec;
  }

  auto cg = g_cg_mgr->AllocateAndGetJobCgroup(spec);
  auto* raw_ptr = cg.get();
  {
    auto job = m_job_map_.GetValueExclusivePtr(job_id);
    if (!job) {
      CRANE_ERROR("Cgroup created for a freed job #{}.", job_id);
      cg->Destroy();
      return nullptr;
    }
    job->cgroup = std::move(cg);
  }
  return raw_ptr;
}

bool JobManager::FreeJobs(const std::vector<task_id_t>& job_ids) {
  {
    auto map_ptr = m_job_map_.GetMapExclusivePtr();
    for (auto job_id : job_ids) {
      if (!map_ptr->contains(job_id)) {
        CRANE_WARN("Try to free nonexistent job#{}", job_id);
        return false;
      }
    }
  }

  std::vector<CgroupInterface*> cg_ptr_vec;
  std::vector<uid_t> uid_vec;
  {
    auto map_ptr = m_job_map_.GetMapExclusivePtr();
    for (auto job_id : job_ids) {
      cg_ptr_vec.push_back(map_ptr->at(job_id).RawPtr()->cgroup.release());
      uid_vec.push_back(map_ptr->at(job_id).RawPtr()->job_spec.cgroup_spec.uid);
      map_ptr->erase(job_id);
    }
  }
  {
    auto uid_map = m_uid_to_job_ids_map_.GetMapExclusivePtr();
    for (auto [idx, job_id] : job_ids | std::ranges::views::enumerate) {
      bool erase{false};
      {
        auto& value = uid_map->at(uid_vec[idx]);
        value.RawPtr()->erase(job_id);
        erase = value.RawPtr()->empty();
      }
      if (erase) {
        uid_map->erase(uid_vec[idx]);
      }
    }
  }
  for (auto [idx, cgroup] : cg_ptr_vec | std::ranges::views::enumerate) {
    if (cgroup == nullptr) continue;

    if (g_config.Plugin.Enabled) {
      g_plugin_client->DestroyCgroupHookAsync(job_ids[idx],
                                              cgroup->GetCgroupString());
    }
    g_thread_pool->detach_task([cgroup]() {
      int cnt = 0;

      while (true) {
        if (cgroup->Empty()) break;

        if (cnt >= 5) {
          CRANE_ERROR(
              "Couldn't kill the processes in cgroup {} after {} times. "
              "Skipping it.",
              cgroup->GetCgroupString(), cnt);
          break;
        }

        cgroup->KillAllProcesses();
        ++cnt;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
      cgroup->Destroy();

      delete cgroup;
    });
  }
  return true;
}

std::optional<TaskInfoOfUid> JobManager::QueryTaskInfoOfUid(uid_t uid) {
  TaskInfoOfUid info;
  info.job_cnt = 0;
  info.cgroup_exists = false;

  if (auto task_ids = this->m_uid_to_job_ids_map_[uid]; task_ids) {
    info.cgroup_exists = true;
    info.job_cnt = task_ids->size();
    info.first_task_id = *task_ids->begin();
  } else {
    CRANE_WARN("Uid {} not found in uid_to_task_ids_map", uid);
    return std::nullopt;
  }
  return info;
}

bool JobManager::MigrateProcToCgroupOfJob(pid_t pid, task_id_t job_id) {
  auto cg = GetCgForJob(job_id);
  if (!cg) return false;

  return cg->MigrateProcIn(pid);
}

CraneExpected<JobSpec> JobManager::QueryJobSpec(task_id_t job_id) {
  auto instance = m_job_map_.GetValueExclusivePtr(job_id);
  if (!instance) return std::unexpected(CraneErrCode::ERR_NON_EXISTENT);
  return instance->job_spec;
};

std::unordered_set<task_id_t> JobManager::QueryExistentJobIds() {
  std::unordered_set<task_id_t> job_ids;
  auto job_map_ptr = m_job_map_.GetMapConstSharedPtr();
  job_ids.reserve(job_map_ptr->size());
  return *job_map_ptr | std::ranges::views::keys |
         std::ranges::to<std::unordered_set<task_id_t>>();
}

};  // namespace Craned
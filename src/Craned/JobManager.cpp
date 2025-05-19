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

EnvMap JobInstance::GetJobEnvMap(const JobToD& job) {
  auto env_map = CgroupManager::GetResourceEnvMapByResInNode(job.res_in_node);

  // TODO: Move all job level env to here.
  env_map.emplace("CRANE_JOB_ID", std::to_string(job.job_id));
  return env_map;
}

bool JobManager::AllocJobs(std::vector<JobToD>&& jobs) {
  CRANE_DEBUG("Allocating {} job", jobs.size());

  auto begin = std::chrono::steady_clock::now();

  for (const auto& job : jobs) {
    task_id_t job_id = job.job_id;
    uid_t uid = job.uid;
    CRANE_TRACE("Create lazily allocated cgroups for job #{}, uid {}", job_id,
                uid);
    m_job_map_.Emplace(job_id, JobInstance(job));

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
  JobToD job;
  {
    auto job_inst = m_job_map_.GetValueExclusivePtr(job_id);
    if (!job_inst) {
      return nullptr;
    }
    if (job_inst->cgroup) {
      return job_inst->cgroup.get();
    }
    job = job_inst->job_to_d;
  }

  auto cg = g_cg_mgr->AllocateAndGetJobCgroup(job);
  auto* raw_ptr = cg.get();
  {
    auto job_inst = m_job_map_.GetValueExclusivePtr(job_id);
    if (!job_inst) {
      CRANE_ERROR("Cgroup created for a freed job #{}.", job_id);
      cg->Destroy();
      return nullptr;
    }
    job_inst->cgroup = std::move(cg);
  }
  return raw_ptr;
}

bool JobManager::FreeJobs(const std::set<task_id_t>& job_ids) {
  CRANE_DEBUG("Release Cgroup for job [{}]", absl::StrJoin(job_ids, ","));
  {
    auto map_ptr = m_job_map_.GetMapExclusivePtr();
    for (auto job_id : job_ids) {
      if (!map_ptr->contains(job_id)) {
        CRANE_WARN("Try to free nonexistent job#{}", job_id);
        return false;
      }
    }
  }

  std::unordered_map<task_id_t, CgroupInterface*> job_cg_map;
  std::vector<uid_t> uid_vec;
  {
    auto map_ptr = m_job_map_.GetMapExclusivePtr();
    for (auto job_id : job_ids) {
      JobInstance* job_inst = map_ptr->at(job_id).RawPtr();

      job_cg_map[job_id] = job_inst->cgroup.release();
      uid_vec.push_back(job_inst->job_to_d.uid);
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

  for (auto [job_id, cgroup] : job_cg_map) {
    if (cgroup == nullptr) continue;

    if (g_config.Plugin.Enabled) {
      g_plugin_client->DestroyCgroupHookAsync(job_id, cgroup->CgroupPathStr());
    }
    g_thread_pool->detach_task([cgroup]() {
      int cnt = 0;

      while (true) {
        if (cgroup->Empty()) break;

        if (cnt >= 5) {
          CRANE_ERROR(
              "Couldn't kill the processes in cgroup {} after {} times. "
              "Skipping it.",
              cgroup->CgroupPathStr(), cnt);
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
  CgroupInterface* cg = GetCgForJob(job_id);
  if (!cg) return false;

  return cg->MigrateProcIn(pid);
}

CraneExpected<JobToD> JobManager::QueryJob(task_id_t job_id) {
  auto instance = m_job_map_.GetValueExclusivePtr(job_id);
  if (!instance) return std::unexpected(CraneErrCode::ERR_NON_EXISTENT);
  return instance->job_to_d;
};

std::set<task_id_t> JobManager::GetAllocatedJobs() {
  std::unordered_set<task_id_t> job_ids;
  auto job_map_ptr = m_job_map_.GetMapConstSharedPtr();
  job_ids.reserve(job_map_ptr->size());
  return *job_map_ptr | std::ranges::views::keys |
         std::ranges::to<std::set<task_id_t>>();
}

};  // namespace Craned
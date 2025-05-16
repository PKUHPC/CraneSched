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
#pragma once

#include "CranedPublicDefs.h"
// Precompiled header comes first.

#include "CgroupManager.h"
#include "protos/Crane.grpc.pb.h"

namespace Craned {

struct JobInstance {
  explicit JobInstance(const JobToD& job) : job_id(job.job_id), job_to_d(job) {}

  JobInstance(const JobInstance& other) = delete;
  JobInstance(JobInstance&& other) noexcept
      : job_id(other.job_id),
        job_to_d(std::move(other.job_to_d)),
        cgroup(std::move(other.cgroup)) {};

  ~JobInstance() = default;

  JobInstance& operator=(const JobInstance& other) = delete;
  JobInstance& operator=(JobInstance&& other) noexcept {
    if (this != &other) {
      job_id = other.job_id;
      job_to_d = std::move(other.job_to_d);
      cgroup = std::move(other.cgroup);
    }
    return *this;
  }

  task_id_t job_id;
  JobToD job_to_d;

  std::unique_ptr<CgroupInterface> cgroup{nullptr};

  static EnvMap GetJobEnvMap(const JobToD& job);
};

class JobManager {
 public:
  JobManager() = default;

  bool AllocJobs(std::vector<JobToD>&& jobs);

  CgroupInterface* GetCgForJob(task_id_t job_id);

  bool FreeJobs(const std::set<task_id_t>& job_ids);

  std::optional<TaskInfoOfUid> QueryTaskInfoOfUid(uid_t uid);

  bool MigrateProcToCgroupOfJob(pid_t pid, task_id_t job_id);

  CraneExpected<JobToD> QueryJob(task_id_t job_id);

  std::set<task_id_t> GetAllocatedJobs();

 private:
  util::AtomicHashMap<absl::node_hash_map, task_id_t, JobInstance> m_job_map_;
  util::AtomicHashMap<absl::flat_hash_map, uid_t /*uid*/,
                      absl::flat_hash_set<task_id_t>>
      m_uid_to_job_ids_map_;
};

};  // namespace Craned

inline std::unique_ptr<Craned::JobManager> g_job_mgr;
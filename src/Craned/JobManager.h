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

struct JobSpec {
  JobSpec() = default;
  explicit JobSpec(const crane::grpc::JobSpec& spec) : cg_spec(spec) {}

  CgroupSpec cg_spec;
  EnvMap GetJobEnvMap() const;
};

struct JobInstance {
  explicit JobInstance(JobSpec&& spec)
      : job_id(spec.cg_spec.job_id), job_spec(spec) {}
  explicit JobInstance(const JobSpec& spec)
      : job_id(spec.cg_spec.job_id), job_spec(spec) {}

  JobInstance(const JobInstance& other) = delete;
  JobInstance(JobInstance&& other) noexcept
      : job_id(other.job_id),
        job_spec(std::move(other.job_spec)),
        cgroup(std::move(other.cgroup)) {};

  ~JobInstance() = default;

  JobInstance& operator=(const JobInstance& other) = delete;
  JobInstance& operator=(JobInstance&& other) noexcept {
    if (this != &other) {
      job_id = other.job_id;
      job_spec = std::move(other.job_spec);
      cgroup = std::move(other.cgroup);
    }
    return *this;
  }

  task_id_t job_id;
  JobSpec job_spec;

  std::unique_ptr<CgroupInterface> cgroup{nullptr};
};

class JobManager {
 public:
  JobManager() = default;

  bool AllocJobs(std::vector<JobSpec>&& job_specs);

  CgroupInterface* GetCgForJob(task_id_t job_id);

  bool FreeJobs(const std::vector<task_id_t>& job_ids);

  std::optional<TaskInfoOfUid> QueryTaskInfoOfUid(uid_t uid);

  bool MigrateProcToCgroupOfJob(pid_t pid, task_id_t job_id);

  CraneExpected<JobSpec> QueryJobSpec(task_id_t job_id);

  std::unordered_set<task_id_t> QueryExistentJobIds();

 private:
  util::AtomicHashMap<absl::node_hash_map, task_id_t, JobInstance> m_job_map_;
  util::AtomicHashMap<absl::flat_hash_map, uid_t /*uid*/,
                      absl::flat_hash_set<task_id_t>>
      m_uid_to_job_ids_map_;
};

};  // namespace Craned

inline std::unique_ptr<Craned::JobManager> g_job_mgr;
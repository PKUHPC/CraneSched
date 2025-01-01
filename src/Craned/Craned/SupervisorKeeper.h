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

#include <protos/Supervisor.grpc.pb.h>

#include "JobManager.h"
#include "crane/AtomicHashMap.h"
namespace Craned {

struct SuperVisorState {
  JobSpec job_spec;
  TaskSpec task_spec;
  pid_t task_pid;
};

class SupervisorClient {
 public:
  CraneExpected<pid_t> ExecuteTask(const TaskExecutionInstance* process);

  CraneExpected<EnvMap> QueryTaskEnv();
  CraneErr CheckTaskStatus(crane::grpc::TaskToD* task, pid_t* task_pid);

  CraneErr TerminateTask(bool mark_as_orphaned, bool terminated_by_user);
  CraneErr ChangeTaskTimeLimit(absl::Duration time_limit);

  void InitChannelAndStub(const std::string& endpoint);

 private:
  std::shared_ptr<grpc::Channel> m_channel_;

  std::unique_ptr<crane::grpc::Supervisor::Stub> m_stub_;
};

class SupervisorKeeper {
 public:
  SupervisorKeeper() = default;
  /**
   * @brief Query all existing supervisor for task they hold.
   * @return task_to_d from supervisors
   */
  CraneExpected<std::vector<SuperVisorState>> Init();

  void AddSupervisor(task_id_t task_id);
  std::shared_ptr<SupervisorClient> GetStub(task_id_t task_id);

 private:
  CraneExpected<SuperVisorState> RecoverSupervisorMt_(
      const std::filesystem::path& path);
  absl::flat_hash_map<task_id_t, std::shared_ptr<SupervisorClient>>
      m_supervisor_map;
  absl::Mutex m_mutex;
};
}  // namespace Craned

inline std::unique_ptr<Craned::SupervisorKeeper> g_supervisor_keeper;
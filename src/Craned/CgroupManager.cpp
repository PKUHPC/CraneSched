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

/*
 * Utility library for libcgroup initialization routines.
 *
 */

#include "CgroupManager.h"

#include "CranedPublicDefs.h"
#include "DeviceManager.h"
#include "crane/String.h"

namespace Craned {

/*
 * Initialize libcgroup and mount the controllers Condor will use (if possible)
 *
 * Returns 0 on success, -1 otherwise.
 */
int CgroupManager::Init() {
  // Initialize library and data structures
  CRANE_DEBUG("Initializing cgroup library.");
  cgroup_init();

  // cgroup_set_loglevel(CGROUP_LOG_DEBUG);

  void *handle = nullptr;
  controller_data info{};

  using CgroupConstant::Controller;
  using CgroupConstant::GetControllerStringView;

  ControllerFlags NO_CONTROLLERS;

  int ret = cgroup_get_all_controller_begin(&handle, &info);
  while (ret == 0) {
    if (info.name == GetControllerStringView(Controller::MEMORY_CONTROLLER)) {
      m_mounted_controllers_ |=
          (info.hierarchy != 0) ? ControllerFlags{Controller::MEMORY_CONTROLLER}
                                : NO_CONTROLLERS;

    } else if (info.name ==
               GetControllerStringView(Controller::CPUACCT_CONTROLLER)) {
      m_mounted_controllers_ |=
          (info.hierarchy != 0)
              ? ControllerFlags{Controller::CPUACCT_CONTROLLER}
              : NO_CONTROLLERS;

    } else if (info.name ==
               GetControllerStringView(Controller::FREEZE_CONTROLLER)) {
      m_mounted_controllers_ |=
          (info.hierarchy != 0) ? ControllerFlags{Controller::FREEZE_CONTROLLER}
                                : NO_CONTROLLERS;

    } else if (info.name ==
               GetControllerStringView(Controller::BLOCK_CONTROLLER)) {
      m_mounted_controllers_ |=
          (info.hierarchy != 0) ? ControllerFlags{Controller::BLOCK_CONTROLLER}
                                : NO_CONTROLLERS;

    } else if (info.name ==
               GetControllerStringView(Controller::CPU_CONTROLLER)) {
      m_mounted_controllers_ |=
          (info.hierarchy != 0) ? ControllerFlags{Controller::CPU_CONTROLLER}
                                : NO_CONTROLLERS;
    } else if (info.name ==
               GetControllerStringView(Controller::DEVICES_CONTROLLER)) {
      m_mounted_controllers_ |=
          (info.hierarchy != 0)
              ? ControllerFlags{Controller::DEVICES_CONTROLLER}
              : NO_CONTROLLERS;
    }
    ret = cgroup_get_all_controller_next(&handle, &info);
  }
  if (handle) {
    cgroup_get_all_controller_end(&handle);
  }

  if (!Mounted(Controller::BLOCK_CONTROLLER)) {
    CRANE_WARN("Cgroup controller for I/O statistics is not available.\n");
  }
  if (!Mounted(Controller::FREEZE_CONTROLLER)) {
    CRANE_WARN("Cgroup controller for process management is not available.\n");
  }
  if (!Mounted(Controller::CPUACCT_CONTROLLER)) {
    CRANE_WARN("Cgroup controller for CPU accounting is not available.\n");
  }
  if (!Mounted(Controller::MEMORY_CONTROLLER)) {
    CRANE_WARN("Cgroup controller for memory accounting is not available.\n");
  }
  if (!Mounted(Controller::CPU_CONTROLLER)) {
    CRANE_WARN("Cgroup controller for CPU is not available.\n");
  }
  if (!Mounted(Controller::DEVICES_CONTROLLER)) {
    CRANE_WARN("Cgroup controller for DEVICES is not available.\n");
  }
  if (ret != ECGEOF) {
    CRANE_WARN("Error iterating through cgroups mount information: {}\n",
               cgroup_strerror(ret));
    return -1;
  }

  return 0;
}

/*
 * Initialize a controller for a given cgroup.
 *
 * Not designed for external users - extracted from CgroupManager::create to
 * reduce code duplication.
 */
int CgroupManager::InitializeController_(struct cgroup &cgroup,
                                         CgroupConstant::Controller controller,
                                         bool required, bool has_cgroup,
                                         bool &changed_cgroup) {
  std::string_view controller_str =
      CgroupConstant::GetControllerStringView(controller);

  int err;

  if (!Mounted(controller)) {
    if (required) {
      CRANE_WARN("Error - cgroup controller {} not mounted, but required.\n",
                 CgroupConstant::GetControllerStringView(controller));
      return 1;
    } else {
      fmt::print("cgroup controller {} is already mounted",
                 CgroupConstant::GetControllerStringView(controller));
      return 0;
    }
  }

  struct cgroup_controller *p_raw_controller;
  if (!has_cgroup ||
      cgroup_get_controller(&cgroup, controller_str.data()) == nullptr) {
    changed_cgroup = true;
    if ((p_raw_controller = cgroup_add_controller(
             &cgroup, controller_str.data())) == nullptr) {
      CRANE_WARN("Unable to initialize cgroup {} controller.\n",
                 controller_str);
      return required ? 1 : 0;
    } else {
      // Try to turn on hierarchical memory accounting.
      if (controller == CgroupConstant::Controller::MEMORY_CONTROLLER) {
        if ((err = cgroup_add_value_bool(p_raw_controller,
                                         "memory.use_hierarchy", true))) {
          CRANE_WARN("Unable to set hierarchical memory settings: {} {}\n", err,
                     cgroup_strerror(err));
        }
      }
    }
  }

  return 0;
}

std::string CgroupManager::CgroupStrByTaskId_(task_id_t task_id) {
  return fmt::format("Crane_Task_{}", task_id);
}

/*
 * Create a new cgroup.
 * Parameters:
 *   - cgroup: reference to a Cgroup object to create/initialize.
 *   - preferred_controllers: Bitset of the controllers we would prefer.
 *   - required_controllers: Bitset of the controllers which are required.
 * Return values:
 *   - 0 on success if the cgroup is pre-existing.
 *   - -1 on error
 * On failure, the state of cgroup is undefined.
 */
std::unique_ptr<Cgroup> CgroupManager::CreateOrOpen_(
    const std::string &cgroup_string, ControllerFlags preferred_controllers,
    ControllerFlags required_controllers, bool retrieve) {
  using CgroupConstant::Controller;
  using CgroupConstant::GetControllerStringView;

  bool changed_cgroup = false;
  struct cgroup *native_cgroup = cgroup_new_cgroup(cgroup_string.c_str());
  if (native_cgroup == NULL) {
    CRANE_WARN("Unable to construct new cgroup object.\n");
    return nullptr;
  }

  // Make sure all required controllers are in preferred controllers:
  preferred_controllers |= required_controllers;

  // Try to fill in the struct cgroup from /proc, if it exists.
  bool has_cgroup = retrieve;
  if (retrieve && (ECGROUPNOTEXIST == cgroup_get_cgroup(native_cgroup))) {
    has_cgroup = false;
  }

  // Work through the various controllers.
  //  if ((preferred_controllers & Controller::CPUACCT_CONTROLLER) &&
  //      initialize_controller(
  //          *native_cgroup, Controller::CPUACCT_CONTROLLER,
  //          required_controllers & Controller::CPUACCT_CONTROLLER, has_cgroup,
  //          changed_cgroup)) {
  //    return nullptr;
  //  }
  if ((preferred_controllers & Controller::MEMORY_CONTROLLER) &&
      InitializeController_(
          *native_cgroup, Controller::MEMORY_CONTROLLER,
          required_controllers & Controller::MEMORY_CONTROLLER, has_cgroup,
          changed_cgroup)) {
    return nullptr;
  }
  if ((preferred_controllers & Controller::FREEZE_CONTROLLER) &&
      InitializeController_(
          *native_cgroup, Controller::FREEZE_CONTROLLER,
          required_controllers & Controller::FREEZE_CONTROLLER, has_cgroup,
          changed_cgroup)) {
    return nullptr;
  }
  //  if ((preferred_controllers & Controller::BLOCK_CONTROLLER) &&
  //      initialize_controller(*native_cgroup, Controller::BLOCK_CONTROLLER,
  //                            required_controllers &
  //                            Controller::BLOCK_CONTROLLER, has_cgroup,
  //                            changed_cgroup)) {
  //    return nullptr;
  //  }
  if ((preferred_controllers & Controller::CPU_CONTROLLER) &&
      InitializeController_(*native_cgroup, Controller::CPU_CONTROLLER,
                            required_controllers & Controller::CPU_CONTROLLER,
                            has_cgroup, changed_cgroup)) {
    return nullptr;
  }
  if ((preferred_controllers & Controller::DEVICES_CONTROLLER) &&
      InitializeController_(
          *native_cgroup, Controller::DEVICES_CONTROLLER,
          required_controllers & Controller::DEVICES_CONTROLLER, has_cgroup,
          changed_cgroup)) {
    return nullptr;
  }

  int err;
  if (!has_cgroup) {
    if ((err = cgroup_create_cgroup(native_cgroup, 0))) {
      // Only record at D_ALWAYS if any cgroup mounts are available.
      CRANE_WARN(
          "Unable to create cgroup {}. Cgroup functionality will not work: {}",
          cgroup_string.c_str(), cgroup_strerror(err));
      return nullptr;
    }
  } else if (changed_cgroup && (err = cgroup_modify_cgroup(native_cgroup))) {
    CRANE_WARN(
        "Unable to modify cgroup {}. Some cgroup functionality may not work: "
        "{} {}",
        cgroup_string.c_str(), err, cgroup_strerror(err));
  }

  return std::make_unique<Cgroup>(cgroup_string, native_cgroup);
}

bool CgroupManager::CheckIfCgroupForTasksExists(task_id_t task_id) {
  return m_task_id_to_cg_map_.Contains(task_id);
}

bool CgroupManager::AllocateAndGetCgroup(task_id_t task_id, Cgroup **cg) {
  crane::grpc::Resources res;
  Cgroup *pcg;

  {
    auto cg_spec_it = m_task_id_to_cg_spec_map_[task_id];
    if (!cg_spec_it) return false;
    res = cg_spec_it->resources;
  }
  {
    auto cg_it = m_task_id_to_cg_map_[task_id];
    auto &cg_unique_ptr = *cg_it;
    if (!cg_unique_ptr)
      cg_unique_ptr = CgroupManager::CreateOrOpen_(
          CgroupStrByTaskId_(task_id),
          NO_CONTROLLER_FLAG | CgroupConstant::Controller::CPU_CONTROLLER |
              CgroupConstant::Controller::MEMORY_CONTROLLER |
              CgroupConstant::Controller::DEVICES_CONTROLLER,
          NO_CONTROLLER_FLAG, false);

    if (!cg_unique_ptr) return false;

    pcg = cg_unique_ptr.get();
    if (cg) *cg = pcg;
  }
  CRANE_TRACE(
      "Setting cgroup limit of task #{}. CPU: {:.2f}, Mem: {:.2f} MB Gres: {}.",
      task_id, res.allocatable_resource().cpu_core_limit(),
      res.allocatable_resource().memory_limit_bytes() / (1024.0 * 1024.0),
      res.has_actual_dedicated_resource()
          ? util::ReadableGres(res.actual_dedicated_resource())
          : "None");

  bool ok =
      AllocatableResourceAllocator::Allocate(res.allocatable_resource(), pcg);
  if (ok)
    ok &= DedicatedResourceAllocator::Allocate(res.actual_dedicated_resource(),
                                               pcg);
  return ok;
}

bool CgroupManager::CreateCgroups(std::vector<CgroupSpec> &&cg_specs) {
  std::chrono::steady_clock::time_point begin;
  std::chrono::steady_clock::time_point end;

  CRANE_DEBUG("Creating cgroups for {} tasks", cg_specs.size());

  begin = std::chrono::steady_clock::now();

  for (int i = 0; i < cg_specs.size(); i++) {
    uid_t uid = cg_specs[i].uid;
    task_id_t task_id = cg_specs[i].task_id;

    CRANE_TRACE("Create lazily allocated cgroups for task #{}, uid {}", task_id,
                uid);

    this->m_task_id_to_cg_spec_map_.Emplace(task_id, std::move(cg_specs[i]));

    this->m_task_id_to_cg_map_.Emplace(task_id, nullptr);
    if (!this->m_uid_to_task_ids_map_.Contains(uid))
      this->m_uid_to_task_ids_map_.Emplace(
          uid, absl::flat_hash_set<uint32_t>{task_id});
    else
      this->m_uid_to_task_ids_map_[uid]->emplace(task_id);
  }

  end = std::chrono::steady_clock::now();
  CRANE_TRACE("Create cgroups costed {} ms",
              std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
                  .count());

  return true;
}

bool CgroupManager::ReleaseCgroupByTaskIdOnly(task_id_t task_id) {
  uid_t uid;
  {
    auto vp = this->m_task_id_to_cg_spec_map_.GetValueExclusivePtr(task_id);
    if (!vp) return false;

    CRANE_DEBUG(
        "Remove cgroup for task #{} for potential crashes of other craned.",
        task_id);
    uid = vp->uid;
  }
  return this->ReleaseCgroup(task_id, uid);
}

bool CgroupManager::ReleaseCgroup(uint32_t task_id, uid_t uid) {
  if (!this->m_uid_to_task_ids_map_.Contains(uid)) {
    CRANE_DEBUG(
        "Trying to release a non-existent cgroup for uid #{}. Ignoring it...",
        uid);
    return false;
  }

  this->m_task_id_to_cg_spec_map_.Erase(task_id);

  this->m_uid_to_task_ids_map_[uid]->erase(task_id);
  if (this->m_uid_to_task_ids_map_[uid]->empty()) {
    this->m_uid_to_task_ids_map_.Erase(uid);
  }

  if (!this->m_task_id_to_cg_map_.Contains(task_id)) {
    CRANE_DEBUG(
        "Trying to release a non-existent cgroup for task #{}. Ignoring "
        "it...",
        task_id);

    return false;
  } else {
    // The termination of all processes in a cgroup is a time-consuming work.
    // Therefore, once we are sure that the cgroup for this task exists, we
    // let gRPC call return and put the termination work into the thread pool
    // to avoid blocking the event loop of TaskManager.
    // Kind of async behavior.

    // avoid deadlock by Erase at next line
    Cgroup *cgroup = this->m_task_id_to_cg_map_[task_id]->release();
    this->m_task_id_to_cg_map_.Erase(task_id);

    if (cgroup != nullptr) {
      g_thread_pool->detach_task([cgroup]() {
        bool rc;
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

        delete cgroup;
      });
    }
    return true;
  }
}

bool CgroupManager::QueryTaskInfoOfUidAsync(uid_t uid, TaskInfoOfUid *info) {
  CRANE_DEBUG("Query task info for uid {}", uid);

  info->job_cnt = 0;
  info->cgroup_exists = false;

  if (this->m_uid_to_task_ids_map_.Contains(uid)) {
    auto task_ids = this->m_uid_to_task_ids_map_[uid];
    info->job_cnt = task_ids->size();
    info->first_task_id = *task_ids->begin();
  }
  return info->job_cnt > 0;
}

bool CgroupManager::MigrateProcToCgroupOfTask(pid_t pid, task_id_t task_id) {
  Cgroup *cg;
  bool ok = AllocateAndGetCgroup(task_id, &cg);
  if (!ok) return false;

  return cg->MigrateProcIn(pid);
}

std::optional<std::string> CgroupManager::QueryTaskExecutionNode(
    task_id_t task_id) {
  if (!this->m_task_id_to_cg_spec_map_.Contains(task_id)) return std::nullopt;
  return this->m_task_id_to_cg_spec_map_[task_id]->execution_node;
}

bool Cgroup::MigrateProcIn(pid_t pid) {
  using CgroupConstant::Controller;
  using CgroupConstant::GetControllerStringView;

  // We want to make sure task migration is turned on for the
  // associated memory controller.  So, we get to look up the original cgroup.
  //
  // If there is no memory controller present, we skip all this and just attempt
  // a migrate
  int err;
//  u_int64_t orig_migrate;
//  bool changed_orig = false;
//  char *orig_cgroup_path = nullptr;
//  struct cgroup *orig_cgroup;
//  struct cgroup_controller *memory_controller;
//  if (Mounted(Controller::MEMORY_CONTROLLER) &&
//      (err = cgroup_get_current_controller_path(
//           pid, GetControllerStringView(Controller::MEMORY_CONTROLLER).data(),
//           &orig_cgroup_path))) {
//    CRANE_WARN(
//        "Unable to determine current memory cgroup for PID {}. Error {}:
//        {}\n", pid, err, cgroup_strerror(err));
//    return false;
//  }
//  // We will migrate the PID to the new cgroup even if it is in the proper
//  // memory controller cgroup It is possible for the task to be in multiple
//  // cgroups.
//  if (Mounted(Controller::MEMORY_CONTROLLER) && (orig_cgroup_path != NULL) &&
//      (cgroup_path == orig_cgroup_path)) {
//    // Yes, there are race conditions here - can't really avoid this.
//    // Throughout this block, we can assume memory controller exists.
//    // Get original value of migrate.
//    orig_cgroup = cgroup_new_cgroup(orig_cgroup_path);
//    assert(orig_cgroup != nullptr);
//    if ((err = cgroup_get_cgroup(orig_cgroup))) {
//      CRANE_WARN("Unable to read original cgroup {}. Error {}: {}\n",
//                  orig_cgroup_path, err, cgroup_strerror(err));
//      cgroup_free(&orig_cgroup);
//      goto after_migrate;
//    }
//    if ((memory_controller = cgroup_get_controller(
//             orig_cgroup,
//             GetControllerStringView(Controller::MEMORY_CONTROLLER).data()))
//             ==
//        nullptr) {
//      CRANE_WARN(
//          "Unable to get memory controller of cgroup {}. Error {}: {}\n",
//          orig_cgroup_path, err, cgroup_strerror(err));
//      cgroup_free(&orig_cgroup);
//      goto after_migrate;
//    }
//    if ((err = cgroup_get_value_uint64(memory_controller,
//                                       "memory.move_charge_at_immigrate",
//                                       &orig_migrate))) {
//      if (err == ECGROUPVALUENOTEXIST) {
//        // Older kernels don't have the ability to migrate memory accounting
//        // to the new cgroup.
//        CRANE_WARN(
//            "This kernel does not support memory usage migration; cgroup "
//            "{} memory statistics"
//            " will be slightly incorrect.\n",
//            cgroup_path.c_str());
//      } else {
//        CRANE_WARN(
//            "Unable to read cgroup {} memory controller settings for "
//            "migration: {} {}\n",
//            orig_cgroup_path, err, cgroup_strerror(err));
//      }
//      cgroup_free(&orig_cgroup);
//      goto after_migrate;
//    }
//    if (orig_migrate != 3) {
//      cgroup_free(&orig_cgroup);
//      orig_cgroup = cgroup_new_cgroup(orig_cgroup_path);
//      memory_controller = cgroup_add_controller(
//          orig_cgroup,
//          GetControllerStringView(Controller::MEMORY_CONTROLLER).data());
//      assert(memory_controller !=
//             NULL);  // Memory controller must already exist
//      cgroup_add_value_uint64(memory_controller,
//                              "memory.move_charge_at_immigrate", 3);
//      if ((err = cgroup_modify_cgroup(orig_cgroup))) {
//        // Not allowed to change settings
//        CRANE_WARN(
//            "Unable to change cgroup {} memory controller settings for "
//            "migration. "
//            "Some memory accounting will be inaccurate: {} "
//            "{}\n",
//            orig_cgroup_path, err, cgroup_strerror(err));
//      } else {
//        changed_orig = true;
//      }
//    }
//    cgroup_free(&orig_cgroup);
//  }
//
after_migrate:

  //  orig_cgroup = NULL;
  err = cgroup_attach_task_pid(m_cgroup_, pid);
  if (err != 0) {
    CRANE_WARN("Cannot attach pid {} to cgroup {}: {} {}", pid,
               m_cgroup_path_.c_str(), err, cgroup_strerror(err));
  }

//  std::string cpu_cg_path =
//      fmt::format("/sys/fs/cgroup/cpu,cpuacct/{}/cgroup.procs", cgroup_path);
//
//  std::ifstream cpu_cg_content(cpu_cg_path);
//  std::string line;
//
//  FILE *cpu_cg_f = fopen(cpu_cg_path.c_str(), "ae");
//  if (cpu_cg_f == nullptr) {
//    CRANE_ERROR("fopen failed: {}", strerror(errno));
//    err = 1;
//    goto end;
//  } else {
//    CRANE_TRACE("Open {} succeeded.", cpu_cg_path);
//  }
//
//  err = fprintf(cpu_cg_f, "%d", pid);
//  if (err < 0) {
//    CRANE_ERROR("fprintf failed: {}", strerror(errno));
//    goto end;
//  } else {
//    CRANE_TRACE("fprintf {} bytes succeeded.", err);
//  }
//
//  err = fflush(cpu_cg_f);
//  if (err < 0) {
//    CRANE_ERROR("fflush failed: {}", strerror(errno));
//    goto end;
//  } else {
//    CRANE_TRACE("fflush succeeded.");
//  }
//
//  fclose(cpu_cg_f);
//
//  if (cpu_cg_content.is_open()) {
//    while (std::getline(cpu_cg_content, line)) {
//      CRANE_TRACE("Pid in {}: {}", cgroup_path, line);
//    }
//    cpu_cg_content.close();
//  }

//  if (changed_orig) {
//    if ((orig_cgroup = cgroup_new_cgroup(orig_cgroup_path)) == NULL) {
//      goto after_restore;
//    }
//    if (((memory_controller = cgroup_add_controller(
//              orig_cgroup,
//              GetControllerStringView(Controller::MEMORY_CONTROLLER).data()))
//              !=
//         nullptr) &&
//        (!cgroup_add_value_uint64(memory_controller,
//                                  "memory.move_charge_at_immigrate",
//                                  orig_migrate))) {
//      if ((err = cgroup_modify_cgroup(orig_cgroup))) {
//        CRANE_WARN(
//            "Unable to change cgroup {} memory controller settings for "
//            "migration. "
//            "Some memory accounting will be inaccurate: {} "
//            "{}\n",
//            orig_cgroup_path, err, cgroup_strerror(err));
//      } else {
//        changed_orig = true;
//      }
//    }
//    cgroup_free(&orig_cgroup);
//  }
//
// after_restore:
//  if (orig_cgroup_path != nullptr) {
//    free(orig_cgroup_path);
//  }
end:
  return err == 0;
}

/*
 * Cleanup cgroup.
 * If the cgroup was created by us in the OS, remove it..
 */
Cgroup::~Cgroup() {
  if (m_cgroup_) {
    int err;
    if ((err = cgroup_delete_cgroup_ext(
             m_cgroup_,
             CGFLAG_DELETE_EMPTY_ONLY | CGFLAG_DELETE_IGNORE_MIGRATION))) {
      CRANE_ERROR("Unable to completely remove cgroup {}: {} {}\n",
                  m_cgroup_path_.c_str(), err, cgroup_strerror(err));
    }

    cgroup_free(&m_cgroup_);
    m_cgroup_ = nullptr;
  }
}

bool Cgroup::SetMemorySoftLimitBytes(uint64_t memory_bytes) {
  return SetControllerValue(
      CgroupConstant::Controller::MEMORY_CONTROLLER,
      CgroupConstant::ControllerFile::MEMORY_SOFT_LIMIT_BYTES, memory_bytes);
}

bool Cgroup::SetMemorySwLimitBytes(uint64_t mem_bytes) {
  return SetControllerValue(
      CgroupConstant::Controller::MEMORY_CONTROLLER,
      CgroupConstant::ControllerFile::MEMORY_MEMSW_LIMIT_IN_BYTES, mem_bytes);
}

bool Cgroup::SetMemoryLimitBytes(uint64_t memory_bytes) {
  return SetControllerValue(CgroupConstant::Controller::MEMORY_CONTROLLER,
                            CgroupConstant::ControllerFile::MEMORY_LIMIT_BYTES,
                            memory_bytes);
}

bool Cgroup::SetCpuShares(uint64_t share) {
  return SetControllerValue(CgroupConstant::Controller::CPU_CONTROLLER,
                            CgroupConstant::ControllerFile::CPU_SHARES, share);
}

/*
 * CPU_CFS_PERIOD_US is the period of time in microseconds for how long a
 * cgroup's access to CPU resources is measured.
 * CPU_CFS_QUOTA_US is the maximum amount of time in microseconds for which a
 * cgroup's tasks are allowed to run during one period.
 * CPU_CFS_PERIOD_US should be set to between 1ms(1000) and 1s(1000'000).
 * CPU_CFS_QUOTA_US should be set to -1 for unlimited, or larger than 1ms(1000).
 * See
 * https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html/resource_management_guide/sec-cpu
 */
bool Cgroup::SetCpuCoreLimit(double core_num) {
  constexpr uint32_t base = 1 << 16;

  bool ret;
  ret = SetControllerValue(CgroupConstant::Controller::CPU_CONTROLLER,
                           CgroupConstant::ControllerFile::CPU_CFS_QUOTA_US,
                           uint64_t(std::round(base * core_num)));
  ret &= SetControllerValue(CgroupConstant::Controller::CPU_CONTROLLER,
                            CgroupConstant::ControllerFile::CPU_CFS_PERIOD_US,
                            base);

  return ret;
}

bool Cgroup::SetBlockioWeight(uint64_t weight) {
  return SetControllerValue(CgroupConstant::Controller::BLOCK_CONTROLLER,
                            CgroupConstant::ControllerFile::BLOCKIO_WEIGHT,
                            weight);
}

bool Cgroup::SetControllerValue(CgroupConstant::Controller controller,
                                CgroupConstant::ControllerFile controller_file,
                                uint64_t value) {
  if (!g_cg_mgr->Mounted(controller)) {
    CRANE_ERROR("Unable to set {} because cgroup {} is not mounted.",
                CgroupConstant::GetControllerFileStringView(controller_file),
                CgroupConstant::GetControllerStringView(controller));
    return false;
  }

  int err;

  struct cgroup_controller *cg_controller;

  if ((cg_controller = cgroup_get_controller(
           m_cgroup_,
           CgroupConstant::GetControllerStringView(controller).data())) ==
      nullptr) {
    CRANE_ERROR("Unable to get cgroup {} controller for {}.",
                CgroupConstant::GetControllerStringView(controller),
                m_cgroup_path_);
    return false;
  }

  if ((err = cgroup_set_value_uint64(
           cg_controller,
           CgroupConstant::GetControllerFileStringView(controller_file).data(),
           value))) {
    CRANE_ERROR("Unable to set uint64 value for {} in cgroup {}. Code {}, {}",
                CgroupConstant::GetControllerFileStringView(controller_file),
                m_cgroup_path_, err, cgroup_strerror(err));
    return false;
  }

  return ModifyCgroup_(controller_file);
}

bool Cgroup::SetControllerStr(CgroupConstant::Controller controller,
                              CgroupConstant::ControllerFile controller_file,
                              const std::string &str) {
  if (!g_cg_mgr->Mounted(controller)) {
    CRANE_ERROR("Unable to set {} because cgroup {} is not mounted.\n",
                CgroupConstant::GetControllerFileStringView(controller_file),
                CgroupConstant::GetControllerStringView(controller));
    return false;
  }

  int err;

  struct cgroup_controller *cg_controller;

  if ((cg_controller = cgroup_get_controller(
           m_cgroup_,
           CgroupConstant::GetControllerStringView(controller).data())) ==
      nullptr) {
    CRANE_ERROR("Unable to get cgroup {} controller for {}.\n",
                CgroupConstant::GetControllerStringView(controller),
                m_cgroup_path_);
    return false;
  }

  if ((err = cgroup_set_value_string(
           cg_controller,
           CgroupConstant::GetControllerFileStringView(controller_file).data(),
           str.c_str()))) {
    CRANE_ERROR("Unable to set string for {}: {} {}\n", m_cgroup_path_, err,
                cgroup_strerror(err));
    return false;
  }

  return ModifyCgroup_(controller_file);
}

bool Cgroup::ModifyCgroup_(CgroupConstant::ControllerFile controller_file) {
  int err;
  int retry_time = 0;
  while (true) {
    err = cgroup_modify_cgroup(m_cgroup_);
    if (err == 0) return true;
    if (err != ECGOTHER) {
      CRANE_ERROR("Unable to modify_cgroup for {} in cgroup {}. Code {}, {}",
                  CgroupConstant::GetControllerFileStringView(controller_file),
                  m_cgroup_path_, err, cgroup_strerror(err));
      return false;
    }

    int errno_code = cgroup_get_last_errno();
    if (errno_code != EINTR) {
      CRANE_ERROR(
          "Unable to modify_cgroup for {} in cgroup {} "
          "due to system error. Code {}, {}",
          CgroupConstant::GetControllerFileStringView(controller_file),
          m_cgroup_path_, errno_code, strerror(errno_code));
      return false;
    }

    CRANE_DEBUG(
        "Unable to modify_cgroup for {} in cgroup {} due to EINTR. Retrying...",
        CgroupConstant::GetControllerFileStringView(controller_file),
        m_cgroup_path_);
    retry_time++;
    if (retry_time > 3) {
      CRANE_ERROR("Unable to modify_cgroup for cgroup {} after 3 times.",
                  m_cgroup_path_);
      return false;
    }
  }

  return true;
}

bool Cgroup::SetControllerStrs(CgroupConstant::Controller controller,
                               CgroupConstant::ControllerFile controller_file,
                               const std::vector<std::string> &strs) {
  if (!g_cg_mgr->Mounted(controller)) {
    CRANE_ERROR("Unable to set {} because cgroup {} is not mounted.\n",
                CgroupConstant::GetControllerFileStringView(controller_file),
                CgroupConstant::GetControllerStringView(controller));
    return false;
  }

  int err;

  struct cgroup_controller *cg_controller;

  if ((cg_controller = cgroup_get_controller(
           m_cgroup_,
           CgroupConstant::GetControllerStringView(controller).data())) ==
      nullptr) {
    CRANE_WARN("Unable to get cgroup {} controller for {}.\n",
               CgroupConstant::GetControllerStringView(controller),
               m_cgroup_path_);
    return false;
  }
  for (const auto &str : strs) {
    if ((err = cgroup_set_value_string(
             cg_controller,
             CgroupConstant::GetControllerFileStringView(controller_file)
                 .data(),
             str.c_str()))) {
      CRANE_WARN("Unable to add string for {}: {} {}\n", m_cgroup_path_, err,
                 cgroup_strerror(err));
      return false;
    }
    // Commit cgroup modifications.
    if ((err = cgroup_modify_cgroup(m_cgroup_))) {
      CRANE_WARN("Unable to commit {} for cgroup {}: {} {}\n",
                 CgroupConstant::GetControllerFileStringView(controller_file),
                 m_cgroup_path_, err, cgroup_strerror(err));
      return false;
    }
  }
  return true;
}

bool Cgroup::KillAllProcesses() {
  using namespace CgroupConstant::Internal;

  const char *controller = CgroupConstant::GetControllerStringView(
                               CgroupConstant::Controller::CPU_CONTROLLER)
                               .data();

  const char *cg_name = m_cgroup_path_.c_str();

  int size, rc;
  pid_t *pids;

  rc = cgroup_get_procs(const_cast<char *>(cg_name),
                        const_cast<char *>(controller), &pids, &size);

  if (rc == 0) {
    for (int i = 0; i < size; ++i) {
      kill(pids[i], SIGKILL);
    }
    free(pids);
    return true;
  } else {
    CRANE_ERROR("cgroup_get_procs error on cgroup \"{}\": {}", cg_name,
                cgroup_strerror(rc));
    return false;
  }
}

bool Cgroup::Empty() {
  using namespace CgroupConstant::Internal;

  const char *controller = CgroupConstant::GetControllerStringView(
                               CgroupConstant::Controller::CPU_CONTROLLER)
                               .data();

  const char *cg_name = m_cgroup_path_.c_str();

  int size, rc;
  pid_t *pids;

  rc = cgroup_get_procs(const_cast<char *>(cg_name),
                        const_cast<char *>(controller), &pids, &size);
  if (rc == 0) {
    free(pids);
    return size == 0;
  } else {
    CRANE_ERROR("cgroup_get_procs error on cgroup \"{}\": {}", cg_name,
                cgroup_strerror(rc));
    return false;
  }
}
bool Cgroup::SetDeviceAccess(const std::unordered_set<SlotId> &devices,
                             bool set_read, bool set_write, bool set_mknod) {
  std::string op;
  if (set_read) op += "r";
  if (set_write) op += "w";
  if (set_mknod) op += "m";
  std::vector<std::string> allow_limits;
  std::vector<std::string> deny_limits;
  for (const auto &[_, this_device] : Craned::g_this_node_device) {
    if (devices.contains(this_device->device_metas.front().path)) {
      for (const auto &dev_meta : this_device->device_metas) {
        allow_limits.emplace_back(fmt::format("{} {}:{} {}", dev_meta.op_type,
                                              dev_meta.major, dev_meta.minor,
                                              op));
      }
    } else {
      for (const auto &dev_meta : this_device->device_metas) {
        deny_limits.emplace_back(fmt::format("{} {}:{} {}", dev_meta.op_type,
                                             dev_meta.major, dev_meta.minor,
                                             op));
      }
    }
  }
  return SetControllerStrs(CgroupConstant::Controller::DEVICES_CONTROLLER,
                           CgroupConstant::ControllerFile::DEVICES_ALLOW,
                           allow_limits) &&
         SetControllerStrs(CgroupConstant::Controller::DEVICES_CONTROLLER,
                           CgroupConstant::ControllerFile::DEVICES_DENY,
                           deny_limits);
}

bool AllocatableResourceAllocator::Allocate(const AllocatableResource &resource,
                                            Cgroup *cg) {
  bool ok;
  ok = cg->SetCpuCoreLimit(static_cast<double>(resource.cpu_count));
  ok &= cg->SetMemoryLimitBytes(resource.memory_bytes);

  // Depending on the system configuration, the following two options may not
  // be enabled, so we ignore the result of them.
  cg->SetMemorySoftLimitBytes(resource.memory_sw_bytes);
  cg->SetMemorySwLimitBytes(resource.memory_sw_bytes);
  return ok;
}

bool AllocatableResourceAllocator::Allocate(
    const crane::grpc::AllocatableResource &resource, Cgroup *cg) {
  bool ok;
  ok = cg->SetCpuCoreLimit(resource.cpu_core_limit());
  ok &= cg->SetMemoryLimitBytes(resource.memory_limit_bytes());

  // Depending on the system configuration, the following two options may not
  // be enabled, so we ignore the result of them.
  cg->SetMemorySoftLimitBytes(resource.memory_sw_limit_bytes());
  cg->SetMemorySwLimitBytes(resource.memory_sw_limit_bytes());
  return ok;
}

bool DedicatedResourceAllocator::Allocate(
    const crane::grpc::DedicatedResource &request_resource, Cgroup *cg) {
  std::unordered_set<std::string> all_request_slots;
  if (request_resource.each_node_gres().contains(g_config.Hostname)) {
    for (const auto &[_, type_slots_map] : request_resource.each_node_gres()
                                               .at(g_config.Hostname)
                                               .name_type_map()) {
      for (const auto &[__, slots] : type_slots_map.type_slots_map())
        all_request_slots.insert(slots.slots().cbegin(), slots.slots().cend());
    };
  }
  if (!cg->SetDeviceAccess(all_request_slots, true, true, true)) return false;
  return true;
}
}  // namespace Craned
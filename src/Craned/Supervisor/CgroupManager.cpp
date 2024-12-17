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

/*
 * Utility library for libcgroup initialization routines.
 *
 */

#include "CgroupManager.h"

#ifdef CRANE_ENABLE_BPF
#  include <bpf/bpf.h>
#  include <bpf/libbpf.h>
#  include <linux/bpf.h>
#endif

#include <dirent.h>

#include "SupervisorPublicDefs.h"
#include "crane/PluginClient.h"
#include "crane/String.h"

namespace Supervisor {

#ifdef CRANE_ENABLE_BPF
BpfRuntimeInfo CgroupV2::bpf_runtime_info_{};
#endif

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

  enum cg_setup_mode_t setup_mode;
  setup_mode = cgroup_setup_mode();
  switch (setup_mode) {
  case CGROUP_MODE_LEGACY:
    //("cgroup mode: Legacy\n");
    cg_version_ = CgroupConstant::CgroupVersion::CGROUP_V1;
    break;
  case CGROUP_MODE_HYBRID:
    //("cgroup mode: Hybrid\n");
    cg_version_ = CgroupConstant::CgroupVersion::UNDEFINED;
    break;
  case CGROUP_MODE_UNIFIED:
    //("cgroup mode: Unified\n");
    cg_version_ = CgroupConstant::CgroupVersion::CGROUP_V2;
    break;
  default:
    //("cgroup mode: Unknown\n");
    break;
  }

  using CgroupConstant::Controller;
  using CgroupConstant::GetControllerStringView;

  ControllerFlags NO_CONTROLLERS;

  if (GetCgroupVersion() == CgroupConstant::CgroupVersion::CGROUP_V1) {
    void *handle = nullptr;
    controller_data info{};
    int ret = cgroup_get_all_controller_begin(&handle, &info);
    while (ret == 0) {
      if (info.name == GetControllerStringView(Controller::MEMORY_CONTROLLER)) {
        m_mounted_controllers_ |=
            (info.hierarchy != 0)
                ? ControllerFlags{Controller::MEMORY_CONTROLLER}
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
            (info.hierarchy != 0)
                ? ControllerFlags{Controller::FREEZE_CONTROLLER}
                : NO_CONTROLLERS;

      } else if (info.name ==
                 GetControllerStringView(Controller::BLOCK_CONTROLLER)) {
        m_mounted_controllers_ |=
            (info.hierarchy != 0)
                ? ControllerFlags{Controller::BLOCK_CONTROLLER}
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

    ControllersMounted();
    if (ret != ECGEOF) {
      CRANE_WARN("Error iterating through cgroups mount information: {}\n",
                 cgroup_strerror(ret));
      return -1;
    }
  }
  // cgroup don't use /proc/cgroups to manage controller
  else if ((GetCgroupVersion() == CgroupConstant::CgroupVersion::CGROUP_V2)) {
    struct cgroup *root = nullptr;
    int ret;
    if ((root = cgroup_new_cgroup("/")) == nullptr) {
      CRANE_WARN("Unable to construct new root cgroup object.");
      return -1;
    }
    if ((ret = cgroup_get_cgroup(root)) != 0) {
      CRANE_WARN("Error : root cgroup not exist.");
      return -1;
    }

    if ((cgroup_get_controller(
            root,
            GetControllerStringView(Controller::CPU_CONTROLLER_V2).data())) !=
        nullptr) {
      m_mounted_controllers_ |= ControllerFlags{Controller::CPU_CONTROLLER_V2};
    }
    if ((cgroup_get_controller(
            root, GetControllerStringView(Controller::MEMORY_CONTORLLER_V2)
                      .data())) != nullptr) {
      m_mounted_controllers_ |=
          ControllerFlags{Controller::MEMORY_CONTORLLER_V2};
    }
    if ((cgroup_get_controller(
            root, GetControllerStringView(Controller::CPUSET_CONTROLLER_V2)
                      .data())) != nullptr) {
      m_mounted_controllers_ |=
          ControllerFlags{Controller::CPUSET_CONTROLLER_V2};
    }
    if ((cgroup_get_controller(
            root,
            GetControllerStringView(Controller::IO_CONTROLLER_V2).data())) !=
        nullptr) {
      m_mounted_controllers_ |= ControllerFlags{Controller::IO_CONTROLLER_V2};
    }
    if ((cgroup_get_controller(
            root,
            GetControllerStringView(Controller::PIDS_CONTROLLER_V2).data())) !=
        nullptr) {
      m_mounted_controllers_ |= ControllerFlags{Controller::PIDS_CONTROLLER_V2};
    }

    ControllersMounted();
    // root cgroup controller can't be change or created

  } else {
    CRANE_WARN("Error Cgroup version is not supported");
    return -1;
  }
  if (cg_version_ == CgroupConstant::CgroupVersion::CGROUP_V1) {
    RmAllTaskCgroups_();
  } else if (cg_version_ == CgroupConstant::CgroupVersion::CGROUP_V2) {
#ifdef CRANE_ENABLE_BPF
    RmBpfDevMap();
#endif
    RmAllTaskCgroupsV2_();
  } else {
    CRANE_WARN("Error Cgroup version is not supported");
  }
  return 0;
}

void CgroupManager::RmAllTaskCgroups_() {
  RmAllTaskCgroupsUnderController_(CgroupConstant::Controller::CPU_CONTROLLER);
  RmAllTaskCgroupsUnderController_(
      CgroupConstant::Controller::MEMORY_CONTROLLER);
  RmAllTaskCgroupsUnderController_(
      CgroupConstant::Controller::DEVICES_CONTROLLER);
}

void CgroupManager::ControllersMounted() {
  using namespace CgroupConstant;
  if (cg_version_ == CgroupVersion::CGROUP_V1) {
    if (!Mounted(Controller::BLOCK_CONTROLLER)) {
      CRANE_WARN("Cgroup controller for I/O statistics is not available.");
    }
    if (!Mounted(Controller::FREEZE_CONTROLLER)) {
      CRANE_WARN("Cgroup controller for process management is not available.");
    }
    if (!Mounted(Controller::CPUACCT_CONTROLLER)) {
      CRANE_WARN("Cgroup controller for CPU accounting is not available.");
    }
    if (!Mounted(Controller::MEMORY_CONTROLLER)) {
      CRANE_WARN("Cgroup controller for memory accounting is not available.");
    }
    if (!Mounted(Controller::CPU_CONTROLLER)) {
      CRANE_WARN("Cgroup controller for CPU is not available.");
    }
    if (!Mounted(Controller::DEVICES_CONTROLLER)) {
      CRANE_WARN("Cgroup controller for DEVICES is not available.");
    }
  } else if (cg_version_ == CgroupVersion::CGROUP_V2) {
    if (!Mounted(Controller::CPU_CONTROLLER_V2)) {
      CRANE_WARN("Cgroup controller for CPU is not available.");
    }
    if (!Mounted(Controller::MEMORY_CONTORLLER_V2)) {
      CRANE_WARN("Cgroup controller for memory is not available.");
    }
    if (!Mounted(Controller::CPUSET_CONTROLLER_V2)) {
      CRANE_WARN("Cgroup controller for cpuset is not available.");
    }
    if (!Mounted(Controller::IO_CONTROLLER_V2)) {
      CRANE_WARN("Cgroup controller for I/O statistics is not available.");
    }
    if (!Mounted(Controller::PIDS_CONTROLLER_V2)) {
      CRANE_WARN("Cgroup controller for pids is not available.");
    }
  }
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
      // Try to turn on hierarchical memory accounting in V1.
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
std::unique_ptr<CgroupInterface> CgroupManager::CreateOrOpen_(
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

  if (GetCgroupVersion() == CgroupConstant::CgroupVersion::CGROUP_V1) {
    //  if ((preferred_controllers & Controller::CPUACCT_CONTROLLER) &&
    //      initialize_controller(
    //          *native_cgroup, Controller::CPUACCT_CONTROLLER,
    //          required_controllers & Controller::CPUACCT_CONTROLLER,
    //          has_cgroup, changed_cgroup)) {
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
    if ((preferred_controllers & Controller::BLOCK_CONTROLLER) &&
        InitializeController_(
            *native_cgroup, Controller::BLOCK_CONTROLLER,
            required_controllers & Controller::BLOCK_CONTROLLER, has_cgroup,
            changed_cgroup)) {
      return nullptr;
    }
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
  } else if (GetCgroupVersion() == CgroupConstant::CgroupVersion::CGROUP_V2) {
    if ((preferred_controllers & Controller::CPU_CONTROLLER_V2) &&
        InitializeController_(
            *native_cgroup, Controller::CPU_CONTROLLER_V2,
            required_controllers & Controller::CPU_CONTROLLER_V2, has_cgroup,
            changed_cgroup)) {
      return nullptr;
    }
    if ((preferred_controllers & Controller::MEMORY_CONTORLLER_V2) &&
        InitializeController_(
            *native_cgroup, Controller::MEMORY_CONTORLLER_V2,
            required_controllers & Controller::MEMORY_CONTORLLER_V2, has_cgroup,
            changed_cgroup)) {
      return nullptr;
    }
    if ((preferred_controllers & Controller::IO_CONTROLLER_V2) &&
        InitializeController_(
            *native_cgroup, Controller::IO_CONTROLLER_V2,
            required_controllers & Controller::IO_CONTROLLER_V2, has_cgroup,
            changed_cgroup)) {
      return nullptr;
    }
    if ((preferred_controllers & Controller::CPUSET_CONTROLLER_V2) &&
        InitializeController_(
            *native_cgroup, Controller::CPUSET_CONTROLLER_V2,
            required_controllers & Controller::CPUSET_CONTROLLER_V2, has_cgroup,
            changed_cgroup)) {
      return nullptr;
    }
    if ((preferred_controllers & Controller::PIDS_CONTROLLER_V2) &&
        InitializeController_(
            *native_cgroup, Controller::PIDS_CONTROLLER_V2,
            required_controllers & Controller::PIDS_CONTROLLER_V2, has_cgroup,
            changed_cgroup)) {
      return nullptr;
    }
  }

  int err;
  if (!has_cgroup) {
    if ((err = cgroup_create_cgroup(native_cgroup, 0))) {
      // Only record at D_ALWAYS if any cgroup mounts are available.
      CRANE_WARN(
          "Unable to create cgroup {}. Cgroup functionality will not work:"
          "{} {}",
          cgroup_string.c_str(), err, cgroup_strerror(err));
      return nullptr;
    }
  } else if (changed_cgroup && (err = cgroup_modify_cgroup(native_cgroup))) {
    CRANE_WARN(
        "Unable to modify cgroup {}. Some cgroup functionality may not work: "
        "{} {}",
        cgroup_string.c_str(), err, cgroup_strerror(err));
  }

  if (GetCgroupVersion() == CgroupConstant::CgroupVersion::CGROUP_V1) {
    return std::make_unique<CgroupV1>(cgroup_string, native_cgroup);
  } else if (GetCgroupVersion() == CgroupConstant::CgroupVersion::CGROUP_V2) {
    struct stat cgroup_stat;
    std::string slash = "/";
    std::string cgroup_full_path =
        CgroupConstant::RootCgroupFullPath + slash + cgroup_string;
    if (stat(cgroup_full_path.c_str(), &cgroup_stat)) {
      CRANE_ERROR("Failed to get cgroup {} stat", cgroup_string);
      return nullptr;
    }
    return std::make_unique<CgroupV2>(
        cgroup_string, native_cgroup,
        static_cast<uint64_t>(cgroup_stat.st_ino));
  } else {
    CRANE_WARN("Unable to create cgroup {}. Cgroup version is not supported",
               cgroup_string.c_str());
    return nullptr;
  }
}

bool CgroupManager::CheckIfCgroupForTasksExists(task_id_t task_id) {
  return m_task_id_to_cg_map_.Contains(task_id);
}

bool CgroupManager::AllocateAndGetCgroup(task_id_t task_id,
                                         CgroupInterface **cg) {
  crane::grpc::ResourceInNode res;
  CgroupInterface *pcg;

  {
    auto cg_spec_it = m_task_id_to_cg_spec_map_[task_id];
    if (!cg_spec_it) return false;
    res = cg_spec_it->res_in_node;
  }

  {
    auto cg_it = m_task_id_to_cg_map_[task_id];
    auto &cg_unique_ptr = *cg_it;
    if (!cg_unique_ptr) {
      if (GetCgroupVersion() == CgroupConstant::CgroupVersion::CGROUP_V1) {
        cg_unique_ptr = CgroupManager::CreateOrOpen_(
            CgroupStrByTaskId_(task_id),
            NO_CONTROLLER_FLAG | CgroupConstant::Controller::CPU_CONTROLLER |
                CgroupConstant::Controller::MEMORY_CONTROLLER |
                CgroupConstant::Controller::DEVICES_CONTROLLER |
                CgroupConstant::Controller::BLOCK_CONTROLLER,
            NO_CONTROLLER_FLAG, false);
      } else if (GetCgroupVersion() ==
                 CgroupConstant::CgroupVersion::CGROUP_V2) {
        cg_unique_ptr = CgroupManager::CreateOrOpen_(
            CgroupStrByTaskId_(task_id),
            NO_CONTROLLER_FLAG | CgroupConstant::Controller::CPU_CONTROLLER_V2 |
                CgroupConstant::Controller::MEMORY_CONTORLLER_V2,
            NO_CONTROLLER_FLAG, false);
      } else {
        CRANE_WARN("cgroup version is not supported.");
      }
    }

    if (!cg_unique_ptr) return false;

    pcg = cg_unique_ptr.get();
    if (cg) *cg = pcg;
  }

  if (g_config.Plugin.Enabled) {
    g_plugin_client->CreateCgroupHookAsync(task_id, pcg->GetCgroupString(),
                                           res.dedicated_res_in_node());
  }

  CRANE_TRACE(
      "Setting cgroup limit of task #{}. CPU: {:.2f}, Mem: {:.2f} MB Gres: {}.",
      task_id, res.allocatable_res_in_node().cpu_core_limit(),
      res.allocatable_res_in_node().memory_limit_bytes() / (1024.0 * 1024.0),
      util::ReadableGrpcDresInNode(res.dedicated_res_in_node()));

  bool ok = AllocatableResourceAllocator::Allocate(
      res.allocatable_res_in_node(), pcg);
  if (ok)
    ok &=
        DedicatedResourceAllocator::Allocate(res.dedicated_res_in_node(), pcg);
  return ok;
}

bool CgroupManager::CreateCgroups(const CgroupSpec &cg_spec) {
  std::chrono::steady_clock::time_point begin;
  std::chrono::steady_clock::time_point end;

  CRANE_DEBUG("Creating cgroups for {} tasks", cg_specs.size());

  begin = std::chrono::steady_clock::now();

  m_cg_spec_ = cg_spec;

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
  this->m_task_id_to_cg_spec_map_.Erase(task_id);

  {
    // The termination of all processes in a cgroup is a time-consuming work.
    // Therefore, once we are sure that the cgroup for this task exists, we
    // let gRPC call return and put the termination work into the thread pool
    // to avoid blocking the event loop of TaskManager.
    // Kind of async behavior.

    // avoid deadlock by Erase at next line
    auto task_id_to_cg_map_ptr =
        this->m_task_id_to_cg_map_.GetMapExclusivePtr();
    auto it = task_id_to_cg_map_ptr->find(task_id);
    if (it == task_id_to_cg_map_ptr->end()) {
      CRANE_DEBUG(
          "Trying to release a non-existent cgroup for task #{}. Ignoring "
          "it...",
          task_id);

      return false;
    }
    CgroupInterface *cgroup = it->second.GetExclusivePtr()->release();

    if (g_config.Plugin.Enabled) {
      g_plugin_client->DestroyCgroupHookAsync(task_id,
                                              cgroup->GetCgroupString());
    }

    task_id_to_cg_map_ptr->erase(task_id);

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
  }

  {
    auto uid_task_ids_map_ptr =
        this->m_uid_to_task_ids_map_.GetMapExclusivePtr();
    auto it = uid_task_ids_map_ptr->find(uid);
    if (it == uid_task_ids_map_ptr->end()) {
      CRANE_DEBUG(
          "Trying to release a non-existent cgroup for uid #{}. Ignoring it...",
          uid);
      return false;
    }

    auto task_id_set_ptr = uid_task_ids_map_ptr->at(uid).RawPtr();

    task_id_set_ptr->erase(task_id);
    if (task_id_set_ptr->empty()) {
      uid_task_ids_map_ptr->erase(uid);
    }
    // Do not access task_id_set_ptr after erasing form map
  }
  return true;
}

void CgroupManager::RmAllTaskCgroupsUnderController_(
    CgroupConstant::Controller controller) {
  void *handle = nullptr;
  cgroup_file_info info{};

  const char *controller_str =
      CgroupConstant::GetControllerStringView(controller).data();

  int base_level;
  int depth = 1;
  int ret = cgroup_walk_tree_begin(controller_str, "/", depth, &handle, &info,
                                   &base_level);
  while (ret == 0) {
    if (info.type == cgroup_file_type::CGROUP_FILE_TYPE_DIR &&
        strstr(info.path, CgroupConstant::kTaskCgPathPrefix) != nullptr) {
      CRANE_DEBUG("Removing remaining task cgroup: {}", info.full_path);
      int err = rmdir(info.full_path);
      if (err != 0)
        CRANE_ERROR("Failed to remove cgroup {}: {}", info.full_path,
                    strerror(errno));
    }

    ret = cgroup_walk_tree_next(depth, &handle, &info, base_level);
  }

  if (handle) cgroup_walk_tree_end(&handle);
}

void CgroupManager::RmAllTaskCgroupsV2_() {
  RmCgroupsV2_(CgroupConstant::RootCgroupFullPath,
               CgroupConstant::kTaskCgPathPrefix);
}

void CgroupManager::RmCgroupsV2_(const std::string &root_cgroup_path,
                                 const std::string &match_str) {
  DIR *dir = nullptr;
  if ((dir = opendir(root_cgroup_path.c_str())) == nullptr) {
    CRANE_ERROR("Failed to open cgroup dir {}", root_cgroup_path);
  }
  struct dirent *entry;
  std::vector<std::string> cgroup_full_path_to_delete;
  while ((entry = readdir(dir)) != nullptr) {
    // Skip "." and ".." directories
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }

    std::string dir_name = entry->d_name;
    std::string full_path = root_cgroup_path + "/" + dir_name;

    // Check if it's a directory and if it contains the match_str
    struct stat info;
    if (stat(full_path.c_str(), &info) == 0 && S_ISDIR(info.st_mode)) {
      if (dir_name.find(match_str) != std::string::npos) {
        cgroup_full_path_to_delete.push_back(full_path);
      }
    }
  }
  closedir(dir);
  for (const auto &tf : cgroup_full_path_to_delete) {
    int err = rmdir(tf.c_str());
    if (err != 0) {
      CRANE_ERROR("Failed to remove cgroup {}: {}", tf.c_str(),
                  strerror(errno));
    }
  }
}

#ifdef CRANE_ENABLE_BPF
void CgroupManager::RmBpfDevMap() {
  try {
    if (std::filesystem::exists(CgroupConstant::BpfDeviceMapFile)) {
      std::filesystem::remove(CgroupConstant::BpfDeviceMapFile);
      CRANE_TRACE("Successfully removed: {}", CgroupConstant::BpfDeviceMapFile);
    } else {
      CRANE_TRACE("File does not exist: {}", CgroupConstant::BpfDeviceMapFile);
    }
  } catch (const std::filesystem::filesystem_error &e) {
    CRANE_ERROR("Error: {}", e.what());
  }
}
#endif

bool CgroupManager::QueryTaskInfoOfUidAsync(uid_t uid, TaskInfoOfUid *info) {
  CRANE_DEBUG("Query task info for uid {}", uid);

  info->job_cnt = 0;
  info->cgroup_exists = false;

  if (auto task_ids = this->m_uid_to_task_ids_map_[uid]) {
    if (!task_ids) {
      CRANE_WARN("Uid {} not found in uid_to_task_ids_map", uid);
      return false;
    }
    info->job_cnt = task_ids->size();
    info->first_task_id = *task_ids->begin();
  }
  return info->job_cnt > 0;
}

bool CgroupManager::MigrateProcToCgroupOfTask(pid_t pid, task_id_t task_id) {
  CgroupInterface *cg;
  bool ok = AllocateAndGetCgroup(task_id, &cg);
  if (!ok) return false;

  return cg->MigrateProcIn(pid);
}

std::optional<std::string> CgroupManager::QueryTaskExecutionNode(
    task_id_t task_id) {
  if (!this->m_task_id_to_cg_spec_map_.Contains(task_id)) return std::nullopt;
  return this->m_task_id_to_cg_spec_map_[task_id]->execution_node;
}

CraneExpected<crane::grpc::ResourceInNode> CgroupManager::GetTaskResourceInNode(
    task_id_t task_id) {
  auto cg_spec_ptr = this->m_task_id_to_cg_spec_map_[task_id];
  if (cg_spec_ptr) return cg_spec_ptr->res_in_node;

  return std::unexpected(CraneErr::kCgroupError);
}

EnvMap CgroupManager::GetResourceEnvMapByResInNode(
    const crane::grpc::ResourceInNode &res_in_node) {
  std::unordered_map env_map = DeviceManager::GetDevEnvMapByResInNode(
      res_in_node.dedicated_res_in_node());

  env_map.emplace(
      "CRANE_MEM_PER_NODE",
      std::to_string(
          res_in_node.allocatable_res_in_node().memory_limit_bytes() /
          (1024 * 1024)));

  return env_map;
}

CraneExpected<EnvMap> CgroupManager::GetResourceEnvMapOfTask(
    task_id_t task_id) {
  auto task_res = GetTaskResourceInNode(task_id);
  if (task_res.has_value()) {
    return GetResourceEnvMapByResInNode(task_res.value());
  }

  CRANE_ERROR("Trying to get resource env list of a non-existent task #{}",
              task_id);
  return std::unexpected(CraneErr::kSystemErr);
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

bool CgroupV1::MigrateProcIn(pid_t pid) {
  using CgroupConstant::Controller;
  using CgroupConstant::GetControllerStringView;

  // We want to make sure task migration is turned on for the
  // associated memory controller.  So, we get to look up the original cgroup.
  //
  // If there is no memory controller present, we skip all this and just attempt
  // a migrate
  int err;
  // TODO: handle memory.move_charge_at_immigrate
  // https://github.com/PKUHPC/CraneSched/pull/327/files/eaa0d04dcc4c12a1773ac9a3fd42aa9f898741aa..9dc93a50528c1b22dbf50d0bf40a11a98bbed36d#r1838007422
  err = cgroup_attach_task_pid(m_cgroup_info_.m_cgroup_, pid);
  if (err != 0) {
    CRANE_WARN("Cannot attach pid {} to cgroup {}: {} {}", pid,
               m_cgroup_info_.m_cgroup_path_.c_str(), err,
               cgroup_strerror(err));
  }
  return err == 0;
}

bool CgroupV1::SetMemorySoftLimitBytes(uint64_t memory_bytes) {
  return m_cgroup_info_.SetControllerValue(
      CgroupConstant::Controller::MEMORY_CONTROLLER,
      CgroupConstant::ControllerFile::MEMORY_SOFT_LIMIT_BYTES, memory_bytes);
}

bool CgroupV1::SetMemorySwLimitBytes(uint64_t mem_bytes) {
  return m_cgroup_info_.SetControllerValue(
      CgroupConstant::Controller::MEMORY_CONTROLLER,
      CgroupConstant::ControllerFile::MEMORY_MEMSW_LIMIT_IN_BYTES, mem_bytes);
}

bool CgroupV1::SetMemoryLimitBytes(uint64_t memory_bytes) {
  return m_cgroup_info_.SetControllerValue(
      CgroupConstant::Controller::MEMORY_CONTROLLER,
      CgroupConstant::ControllerFile::MEMORY_LIMIT_BYTES, memory_bytes);
}

bool CgroupV1::SetCpuShares(uint64_t share) {
  return m_cgroup_info_.SetControllerValue(
      CgroupConstant::Controller::CPU_CONTROLLER,
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
bool CgroupV1::SetCpuCoreLimit(double core_num) {
  constexpr uint32_t base = 1 << 16;

  bool ret;
  ret = m_cgroup_info_.SetControllerValue(
      CgroupConstant::Controller::CPU_CONTROLLER,
      CgroupConstant::ControllerFile::CPU_CFS_QUOTA_US,
      uint64_t(std::round(base * core_num)));
  ret &= m_cgroup_info_.SetControllerValue(
      CgroupConstant::Controller::CPU_CONTROLLER,
      CgroupConstant::ControllerFile::CPU_CFS_PERIOD_US, base);

  return ret;
}

bool CgroupV1::SetBlockioWeight(uint64_t weight) {
  return m_cgroup_info_.SetControllerValue(
      CgroupConstant::Controller::BLOCK_CONTROLLER,
      CgroupConstant::ControllerFile::BLOCKIO_WEIGHT, weight);
}

bool CgroupV1::KillAllProcesses() {
  using namespace CgroupConstant::Internal;

  const char *controller = CgroupConstant::GetControllerStringView(
                               CgroupConstant::Controller::CPU_CONTROLLER)
                               .data();

  const char *cg_name = m_cgroup_info_.m_cgroup_path_.c_str();

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

bool CgroupV1::Empty() {
  using namespace CgroupConstant::Internal;

  const char *controller = CgroupConstant::GetControllerStringView(
                               CgroupConstant::Controller::CPU_CONTROLLER)
                               .data();

  const char *cg_name = m_cgroup_info_.m_cgroup_path_.c_str();

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
bool CgroupV1::SetDeviceAccess(const std::unordered_set<SlotId> &devices,
                               bool set_read, bool set_write, bool set_mknod) {
  std::string op;
  if (set_read) op += "r";
  if (set_write) op += "w";
  if (set_mknod) op += "m";
  std::vector<std::string> deny_limits;
  for (const auto &[_, this_device] : Craned::g_this_node_device) {
    if (!devices.contains(this_device->slot_id)) {
      for (const auto &dev_meta : this_device->device_file_metas) {
        deny_limits.emplace_back(fmt::format("{} {}:{} {}", dev_meta.op_type,
                                             dev_meta.major, dev_meta.minor,
                                             op));
      }
    }
  }
  auto ok = true;
  if (!deny_limits.empty())
    ok &= m_cgroup_info_.SetControllerStrs(
        CgroupConstant::Controller::DEVICES_CONTROLLER,
        CgroupConstant::ControllerFile::DEVICES_DENY, deny_limits);
  return ok;
}

#ifdef CRANE_ENABLE_BPF

BpfRuntimeInfo::BpfRuntimeInfo() {
  bpf_obj_ = nullptr;
  bpf_prog_ = nullptr;
  dev_map_ = nullptr;
  bpf_debug_log_level_ = 0;
  bpf_mtx_ = new std::mutex;
  bpf_prog_fd_ = -1;
  cgroup_count_ = 0;
}

BpfRuntimeInfo::~BpfRuntimeInfo() {
  bpf_obj_ = nullptr;
  bpf_prog_ = nullptr;
  dev_map_ = nullptr;
  delete bpf_mtx_;
  bpf_prog_fd_ = -1;
  cgroup_count_ = 0;
}

bool BpfRuntimeInfo::InitializeBpfObj() {
  std::unique_lock<std::mutex> lk(*bpf_mtx_);

  if (cgroup_count_ == 0) {
    bpf_obj_ = bpf_object__open_file(CgroupConstant::BpfObjectFile, NULL);
    if (!bpf_obj_) {
      CRANE_ERROR("Failed to open BPF object file {}",
                  CgroupConstant::BpfObjectFile);
      bpf_object__close(bpf_obj_);
      return false;
    }

    // ban libbpf log
    libbpf_print_fn_t fn = libbpf_set_print(NULL);

    if (bpf_object__load(bpf_obj_)) {
      CRANE_ERROR("Failed to load BPF object {}",
                  CgroupConstant::BpfObjectFile);
      bpf_object__close(bpf_obj_);
      return false;
    }

    bpf_prog_ = bpf_object__find_program_by_name(
        bpf_obj_, CgroupConstant::BpfProgramName);
    if (!bpf_prog_) {
      CRANE_ERROR("Failed to find BPF program {}",
                  CgroupConstant::BpfProgramName);
      bpf_object__close(bpf_obj_);
      return false;
    }

    bpf_prog_fd_ = bpf_program__fd(bpf_prog_);
    if (bpf_prog_fd_ < 0) {
      CRANE_ERROR("Failed to get BPF program file descriptor {}",
                  CgroupConstant::BpfObjectFile);
      bpf_object__close(bpf_obj_);
      return false;
    }

    dev_map_ =
        bpf_object__find_map_by_name(bpf_obj_, CgroupConstant::BpfMapName);
    if (!dev_map_) {
      CRANE_ERROR("Failed to find BPF map {}", CgroupConstant::BpfMapName);
      close(bpf_prog_fd_);
      bpf_object__close(bpf_obj_);
      return false;
    }

    struct BpfKey key = {static_cast<uint64_t>(0), static_cast<uint32_t>(0),
                         static_cast<uint32_t>(0)};
    struct BpfDeviceMeta meta = {static_cast<uint32_t>(bpf_debug_log_level_),
                                 static_cast<uint32_t>(0), static_cast<int>(0),
                                 static_cast<short>(0), static_cast<short>(0)};
    if (bpf_map__update_elem(dev_map_, &key, sizeof(BpfKey), &meta,
                             sizeof(BpfDeviceMeta), BPF_ANY)) {
      CRANE_ERROR("Failed to set debug log level in BPF");
      return false;
    }
  }
  return ++cgroup_count_ >= 1;
}

void BpfRuntimeInfo::CloseBpfObj() {
  std::unique_lock<std::mutex> lk(*bpf_mtx_);
  if (BpfInvalid() && --cgroup_count_ == 0) {
    close(bpf_prog_fd_);
    bpf_object__close(bpf_obj_);
    bpf_prog_fd_ = -1;
    bpf_obj_ = nullptr;
    bpf_prog_ = nullptr;
    dev_map_ = nullptr;
    RmBpfDeviceMap();
  }
}

void BpfRuntimeInfo::RmBpfDeviceMap() {
  try {
    if (std::filesystem::exists(CgroupConstant::BpfDeviceMapFile)) {
      std::filesystem::remove(CgroupConstant::BpfDeviceMapFile);
      CRANE_TRACE("Successfully removed: {}", CgroupConstant::BpfDeviceMapFile);
    } else {
      CRANE_TRACE("File does not exist: {}", CgroupConstant::BpfDeviceMapFile);
    }
  } catch (const std::filesystem::filesystem_error &e) {
    CRANE_ERROR("Error: {}", e.what());
  }
}
#endif

CgroupV2::CgroupV2(const std::string &path, struct cgroup *handle, uint64_t id)
    : m_cgroup_info_(path, handle, id) {
#ifdef CRANE_ENABLE_BPF
  if (bpf_runtime_info_.InitializeBpfObj()) {
    CRANE_TRACE("Bpf object initialization succeed");
  } else {
    CRANE_TRACE("Bpf object initialization failed");
  }
#endif
}

CgroupV2::~CgroupV2() {
#ifdef CRANE_ENABLE_BPF
  if (!m_cgroup_bpf_devices.empty()) {
    EraseBpfDeviceMap();
  }
  bpf_runtime_info_.CloseBpfObj();
#endif
}

/**
 *If a controller implements an absolute resource guarantee and/or limit,
 * the interface files should be named “min” and “max” respectively.
 * If a controller implements best effort resource guarantee and/or limit,
 * the interface files should be named “low” and “high” respectively.
 */

bool CgroupV2::SetCpuCoreLimit(double core_num) {
  constexpr uint32_t period = 1 << 16;
  uint64_t quota = static_cast<uint64_t>(period * core_num);
  std::string cpuMaxValue =
      std::to_string(quota) + " " + std::to_string(period);
  return m_cgroup_info_.SetControllerStr(
      CgroupConstant::Controller::CPU_CONTROLLER_V2,
      CgroupConstant::ControllerFile::CPU_MAX_V2, cpuMaxValue.c_str());
}

bool CgroupV2::SetCpuShares(uint64_t share) {
  return m_cgroup_info_.SetControllerValue(
      CgroupConstant::Controller::CPU_CONTROLLER_V2,
      CgroupConstant::ControllerFile::CPU_WEIGHT_V2, share);
}

bool CgroupV2::SetMemoryLimitBytes(uint64_t memory_bytes) {
  return m_cgroup_info_.SetControllerValue(
      CgroupConstant::Controller::MEMORY_CONTORLLER_V2,
      CgroupConstant::ControllerFile::MEMORY_MAX_V2, memory_bytes);
}

bool CgroupV2::SetMemorySoftLimitBytes(uint64_t memory_bytes) {
  return m_cgroup_info_.SetControllerValue(
      CgroupConstant::Controller::MEMORY_CONTORLLER_V2,
      CgroupConstant::ControllerFile::MEMORY_HIGH_V2, memory_bytes);
}

bool CgroupV2::SetMemorySwLimitBytes(uint64_t memory_bytes) {
  return m_cgroup_info_.SetControllerValue(
      CgroupConstant::Controller::MEMORY_CONTORLLER_V2,
      CgroupConstant::ControllerFile::MEMORY_SWAP_MAX_V2, memory_bytes);
}

bool CgroupV2::SetBlockioWeight(uint64_t weight) {
  return m_cgroup_info_.SetControllerValue(
      CgroupConstant::Controller::IO_CONTROLLER_V2,
      CgroupConstant::ControllerFile::IO_WEIGHT_V2, weight);
}

bool CgroupV2::SetDeviceAccess(const std::unordered_set<SlotId> &devices,
                               bool set_read, bool set_write, bool set_mknod) {
#ifdef CRANE_ENABLE_BPF
  if (!bpf_runtime_info_.BpfInvalid()) {
    CRANE_WARN("BPF is not initialized.");
    return false;
  }
  int cgroup_fd;
  std::string slash = "/";
  std::string cgroup_path = CgroupConstant::RootCgroupFullPath + slash +
                            m_cgroup_info_.m_cgroup_path_;
  cgroup_fd = open(cgroup_path.c_str(), O_RDONLY);
  if (cgroup_fd < 0) {
    CRANE_ERROR("Failed to open cgroup");
    return false;
  }

  short access = 0;
  if (set_read) access |= BPF_DEVCG_ACC_READ;
  if (set_write) access |= BPF_DEVCG_ACC_WRITE;
  if (set_mknod) access |= BPF_DEVCG_ACC_MKNOD;

  auto &bpf_devices = m_cgroup_bpf_devices;
  for (const auto &[_, this_device] : Craned::g_this_node_device) {
    if (!devices.contains(this_device->slot_id)) {
      for (const auto &dev_meta : this_device->device_file_metas) {
        short op_type = 0;
        if (dev_meta.op_type == 'c') {
          op_type |= BPF_DEVCG_DEV_CHAR;
        } else if (dev_meta.op_type == 'b') {
          op_type |= BPF_DEVCG_DEV_BLOCK;
        } else {
          op_type |= 0xffff;
        }
        bpf_devices.push_back({dev_meta.major, dev_meta.minor,
                               BPF_PERMISSION::DENY, access, op_type});
      }
    }
  }
  {
    std::unique_lock<std::mutex> lk(*bpf_runtime_info_.BpfMutex());
    for (int i = 0; i < bpf_devices.size(); i++) {
      struct BpfKey key = {m_cgroup_info_.m_cgroup_id, bpf_devices[i].major,
                           bpf_devices[i].minor};
      if (bpf_map__update_elem(bpf_runtime_info_.BpfDevMap(), &key,
                               sizeof(BpfKey), &bpf_devices[i],
                               sizeof(BpfDeviceMeta), BPF_ANY)) {
        CRANE_ERROR("Failed to update BPF map major {},minor {} cgroup id {}",
                    bpf_devices[i].major, bpf_devices[i].minor, key.cgroup_id);
        close(cgroup_fd);
        return false;
      }
    }

    if (bpf_prog_attach(bpf_runtime_info_.BpfProgFd(), cgroup_fd,
                        BPF_CGROUP_DEVICE, 0) < 0) {
      CRANE_ERROR("Failed to attach BPF program");
      close(cgroup_fd);
      return false;
    }
  }
  close(cgroup_fd);
  return true;
#endif

#ifndef CRANE_ENABLE_BPF
  CRANE_WARN(
      "BPF is disabled in craned, you can use Cgroup V1 to set devices access");
  return false;
#endif
}

#ifdef CRANE_ENABLE_BPF
bool CgroupV2::EraseBpfDeviceMap() {
  {
    if (!bpf_runtime_info_.BpfInvalid()) {
      CRANE_WARN("BPF is not initialized.");
      return false;
    }
    std::unique_lock<std::mutex> lk(*bpf_runtime_info_.BpfMutex());
    auto &bpf_devices = m_cgroup_bpf_devices;
    for (int i = 0; i < bpf_devices.size(); i++) {
      struct BpfKey key = {m_cgroup_info_.m_cgroup_id, bpf_devices[i].major,
                           bpf_devices[i].minor};
      if (bpf_map__delete_elem(bpf_runtime_info_.BpfDevMap(), &key,
                               sizeof(BpfKey), BPF_ANY)) {
        CRANE_ERROR(
            "Failed to delete BPF map major {},minor {} in cgroup id {}",
            bpf_devices[i].major, bpf_devices[i].minor, key.cgroup_id);
        return false;
      }
    }
  }
  return true;
}
#endif

bool CgroupV2::KillAllProcesses() {
  using namespace CgroupConstant::Internal;

  const char *controller = CgroupConstant::GetControllerStringView(
                               CgroupConstant::Controller::CPU_CONTROLLER_V2)
                               .data();

  const char *cg_name = m_cgroup_info_.m_cgroup_path_.c_str();

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

bool CgroupV2::Empty() {
  using namespace CgroupConstant::Internal;

  const char *controller = CgroupConstant::GetControllerStringView(
                               CgroupConstant::Controller::CPU_CONTROLLER_V2)
                               .data();

  const char *cg_name = m_cgroup_info_.m_cgroup_path_.c_str();

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

bool CgroupV2::MigrateProcIn(pid_t pid) {
  using CgroupConstant::Controller;
  using CgroupConstant::GetControllerStringView;
  int err;
after_migrate:

  err = cgroup_attach_task_pid(m_cgroup_info_.m_cgroup_, pid);
  if (err != 0) {
    CRANE_WARN("Cannot attach pid {} to cgroup {}: {} {}", pid,
               m_cgroup_info_.m_cgroup_path_.c_str(), err,
               cgroup_strerror(err));
  }
end:
  return err == 0;
}

bool AllocatableResourceAllocator::Allocate(const AllocatableResource &resource,
                                            CgroupInterface *cg) {
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
    const crane::grpc::AllocatableResource &resource, CgroupInterface *cg) {
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
    const crane::grpc::DedicatedResourceInNode &request_resource,
    CgroupInterface *cg) {
  std::unordered_set<std::string> all_request_slots;
  for (const auto &[_, type_slots_map] : request_resource.name_type_map()) {
    for (const auto &[__, slots] : type_slots_map.type_slots_map())
      all_request_slots.insert(slots.slots().cbegin(), slots.slots().cend());
  };

  if (!cg->SetDeviceAccess(all_request_slots, true, true, true)) {
    if (g_cg_mgr->GetCgroupVersion() ==
        CgroupConstant::CgroupVersion::CGROUP_V1) {
      CRANE_WARN("Allocate devices access failed in Cgroup V1.");
    } else if (g_cg_mgr->GetCgroupVersion() ==
               CgroupConstant::CgroupVersion::CGROUP_V2) {
      CRANE_WARN("Allocate devices access failed in Cgroup V2.");
    }
    return true;
  }
  return true;
}
}  // namespace Supervisor
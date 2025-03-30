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

#include "CranedPublicDefs.h"
#include "DeviceManager.h"
#include "crane/PluginClient.h"
#include "crane/String.h"

namespace Craned {

std::optional<task_id_t> GetJobIdFromCg(const std::string &path) {
  static constexpr LazyRE2 cg_pattern(R"(Crane_Task_(\d+))");
  std::string job_id;
  if (RE2::FullMatch(path, *cg_pattern, &job_id)) {
    return std::stoul(job_id);
  } else {
    return std::nullopt;
  }
}

#ifdef CRANE_ENABLE_BPF
BpfRuntimeInfo CgroupManager::bpf_runtime_info{};

CgroupManager::~CgroupManager() {
  if (!bpf_runtime_info.Valid()) return;
  int bpf_map_count = 0;
  auto *pre_key = new BpfKey();
  if (bpf_map__get_next_key(bpf_runtime_info.BpfDevMap(), nullptr, pre_key,
                            sizeof(BpfKey)) == 0) {
    CRANE_ERROR("Failed to get first key of bpf map");
  }
  bpf_map_count++;
  auto *cur_key = new BpfKey();
  while (bpf_map__get_next_key(bpf_runtime_info.BpfDevMap(), pre_key, cur_key,
                               sizeof(BpfKey)) == 0) {
    ++bpf_map_count;
  }
  delete pre_key;
  delete cur_key;
  // always one key for logging
  if (bpf_map_count == 1) {
    // All task end
    BpfRuntimeInfo::RmBpfDeviceMap();
  }
}
#endif

CraneErrCode CgroupManager::Init() {
  // Initialize library and data structures
  CRANE_DEBUG("Initializing cgroup library.");
  cgroup_init();

  // cgroup_set_loglevel(CGROUP_LOG_DEBUG);

#ifdef CRANE_ENABLE_CGROUP_V2
  enum cg_setup_mode_t setup_mode;
  setup_mode = cgroup_setup_mode();
  switch (setup_mode) {
  case CGROUP_MODE_LEGACY:
    m_cg_version_ = CgroupConstant::CgroupVersion::CGROUP_V1;
    break;
  case CGROUP_MODE_HYBRID:
    m_cg_version_ = CgroupConstant::CgroupVersion::UNDEFINED;
    break;
  case CGROUP_MODE_UNIFIED:
    m_cg_version_ = CgroupConstant::CgroupVersion::CGROUP_V2;
    break;
  default:  // Cgroup Mode: Unknown
    break;
  }
#else
  m_cg_version_ = CgroupConstant::CgroupVersion::CGROUP_V1;
#endif

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
      return CraneErrCode::ERR_CGROUP;
    }
  }
  // cgroup don't use /proc/cgroups to manage controller
  else if (GetCgroupVersion() == CgroupConstant::CgroupVersion::CGROUP_V2) {
    cgroup *root = cgroup_new_cgroup("/");
    if (root == nullptr) {
      CRANE_WARN("Unable to construct new root cgroup object.");
      return CraneErrCode::ERR_CGROUP;
    }

    int ret = cgroup_get_cgroup(root);
    if (ret != 0) {
      CRANE_WARN("Error : root cgroup not exist.");
      return CraneErrCode::ERR_CGROUP;
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
    return CraneErrCode::ERR_CGROUP;
  }
  return CraneErrCode::SUCCESS;
}

CraneErrCode CgroupManager::Recover(
    const std::unordered_set<task_id_t> &running_job_ids) {
  // TODO: Remove these after csupervisor is stable
  std::set<task_id_t> cg_running_job_ids{};
  if (m_cg_version_ == CgroupConstant::CgroupVersion::CGROUP_V1) {
    cg_running_job_ids.merge(
        GetJobIdsFromCgroupV1(CgroupConstant::Controller::CPU_CONTROLLER));
    cg_running_job_ids.merge(
        GetJobIdsFromCgroupV1(CgroupConstant::Controller::MEMORY_CONTROLLER));
    cg_running_job_ids.merge(
        GetJobIdsFromCgroupV1(CgroupConstant::Controller::DEVICES_CONTROLLER));
  } else if (m_cg_version_ == CgroupConstant::CgroupVersion::CGROUP_V2) {
    cg_running_job_ids =
        GetJobIdsFromCgroupV2(CgroupConstant::RootCgroupFullPath);
#ifdef CRANE_ENABLE_BPF
    auto job_id_bpf_key_vec_map =
        GetJobBpfMapCgroupsV2(CgroupConstant::RootCgroupFullPath);
    if (!job_id_bpf_key_vec_map) {
      CRANE_ERROR("Failed to read job ebpf info, skip recovery.");
      return CraneErrCode::ERR_EBPF;
    }

    for (const auto &[job_id, bpf_key_vec] : job_id_bpf_key_vec_map.value()) {
      if (running_job_ids.contains(job_id)) continue;
      CRANE_DEBUG("Erase bpf map entry for not running job {}", job_id);
      for (const auto &key : bpf_key_vec) {
        if (bpf_map__delete_elem(bpf_runtime_info.BpfDevMap(), &key,
                                 sizeof(BpfKey), BPF_ANY)) {
          CRANE_ERROR(
              "Failed to delete BPF map major {},minor {} in cgroup id {}",
              key.major, key.minor, key.cgroup_id);
        }
      }
    }
#endif
  } else {
    CRANE_WARN("Error Cgroup version is not supported");
    return CraneErrCode::ERR_CGROUP;
  }
  for (auto job_id : cg_running_job_ids) {
    if (!running_job_ids.contains(job_id)) {
      CRANE_DEBUG("Removing cgroup for job #{}.", job_id);
      std::unique_ptr<CgroupInterface> cg_unique_ptr{nullptr};
      if (GetCgroupVersion() == CgroupConstant::CgroupVersion::CGROUP_V1) {
        cg_unique_ptr = CgroupManager::CreateOrOpen_(
            job_id, CgV1PreferredControllers, NO_CONTROLLER_FLAG, true);
      } else if (GetCgroupVersion() ==
                 CgroupConstant::CgroupVersion::CGROUP_V2) {
        cg_unique_ptr = CgroupManager::CreateOrOpen_(
            job_id, CgV2PreferredControllers, NO_CONTROLLER_FLAG, true);
      }
      if (cg_unique_ptr == nullptr) {
        CRANE_ERROR("Failed to reopen cgroup for job #{}.", job_id);
      }
      // cg_unique_ptr.reset();
    }
  }
  return CraneErrCode::SUCCESS;
}

void CgroupManager::ControllersMounted() {
  using namespace CgroupConstant;
  if (m_cg_version_ == CgroupVersion::CGROUP_V1) {
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
  } else if (m_cg_version_ == CgroupVersion::CGROUP_V2) {
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
      CRANE_WARN("cgroup controller {} is already mounted",
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

std::set<task_id_t> CgroupManager::GetJobIdsFromCgroupV1(
    CgroupConstant::Controller controller) {
  void *handle = nullptr;
  cgroup_file_info info{};
  std::set<task_id_t> job_ids;

  const char *controller_str =
      CgroupConstant::GetControllerStringView(controller).data();

  int base_level;
  int depth = 1;
  int ret = cgroup_walk_tree_begin(controller_str, "/", depth, &handle, &info,
                                   &base_level);
  while (ret == 0) {
    if (info.type == cgroup_file_type::CGROUP_FILE_TYPE_DIR) {
      if (auto job_id = GetJobIdFromCg(info.path); job_id.has_value())
        job_ids.emplace(job_id.value());
    }
    ret = cgroup_walk_tree_next(depth, &handle, &info, base_level);
  }

  if (handle) cgroup_walk_tree_end(&handle);
  return job_ids;
}

std::set<task_id_t> CgroupManager::GetJobIdsFromCgroupV2(
    const std::string &root_cgroup_path) {
  std::set<task_id_t> job_ids;
  try {
    for (const auto &it :
         std::filesystem::directory_iterator(root_cgroup_path)) {
      std::string job_id_str;
      if (it.is_directory()) {
        if (auto job_id = GetJobIdFromCg(it.path().filename());
            job_id.has_value())
          job_ids.emplace(job_id.value());
      }
    }
  } catch (const std::filesystem::filesystem_error &e) {
    CRANE_ERROR("Error: {}", e.what());
  }
  return job_ids;
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
/**
 * @brief Create or open cgroup for task, not guarantee cg spec exists.
 * @param task_id task id of cgroup to create.
 * @param preferred_controllers bitset of the controllers we would prefer.
 * @param required_controllers bitset of the controllers which are required.
 * @param retrieve just retrieve an existing cgroup.
 * @return unique_ptr to CgroupInterface, null if failed.
 */
std::unique_ptr<CgroupInterface> CgroupManager::CreateOrOpen_(
    task_id_t task_id, ControllerFlags preferred_controllers,
    ControllerFlags required_controllers, bool retrieve) {
  using CgroupConstant::Controller;
  using CgroupConstant::GetControllerStringView;

  std::string cgroup_string = CgroupStrByTaskId_(task_id);

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
    cgroup_free(&native_cgroup);
    return nullptr;
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
    // For cgroup V2,we put task cgroup under RootCgroupFullPath.
    struct stat cgroup_stat;
    std::string slash = "/";
    std::filesystem::path cgroup_full_path =
        CgroupConstant::RootCgroupFullPath + slash + cgroup_string;
    if (stat(cgroup_full_path.c_str(), &cgroup_stat)) {
      CRANE_ERROR("Cgroup {} created but stat failed: {}", cgroup_string,
                  std::strerror(errno));
      return nullptr;
    }

    return std::make_unique<CgroupV2>(cgroup_string, native_cgroup,
                                      cgroup_stat.st_ino);
  } else {
    CRANE_WARN("Unable to create cgroup {}. Cgroup version is not supported",
               cgroup_string);
    return nullptr;
  }
}

std::unique_ptr<CgroupInterface> CgroupManager::AllocateAndGetJobCgroup(
    const CgroupSpec &cg_spec) {
  crane::grpc::ResourceInNode res = cg_spec.res_in_node;
  bool recover = cg_spec.recovered;
  auto job_id = cg_spec.job_id;

  std::unique_ptr<CgroupInterface> cg_unique_ptr{nullptr};
  if (GetCgroupVersion() == CgroupConstant::CgroupVersion::CGROUP_V1) {
    cg_unique_ptr = CgroupManager::CreateOrOpen_(
        job_id, CgV1PreferredControllers, NO_CONTROLLER_FLAG, recover);
  } else if (GetCgroupVersion() == CgroupConstant::CgroupVersion::CGROUP_V2) {
    cg_unique_ptr = CgroupManager::CreateOrOpen_(
        job_id, CgV2PreferredControllers, NO_CONTROLLER_FLAG, recover);
  } else {
    CRANE_WARN("cgroup version is not supported.");
    return nullptr;
  }

  // If just recover cgroup, do not trigger plugin and apply res limit.
  if (recover) {
#ifdef CRANE_ENABLE_BPF
    if (GetCgroupVersion() != CgroupConstant::CgroupVersion::CGROUP_V2) {
      return cg_unique_ptr;
    }
    CgroupV2 *cg_v2_ptr = dynamic_cast<CgroupV2 *>(cg_unique_ptr.get());
    cg_v2_ptr->RecoverFromCgSpec(cg_spec);
#endif

    return cg_unique_ptr;
  }

  if (g_config.Plugin.Enabled) {
    g_plugin_client->CreateCgroupHookAsync(cg_spec.job_id,
                                           cg_unique_ptr->GetCgroupString(),
                                           res.dedicated_res_in_node());
  }

  CRANE_TRACE(
      "Setting cgroup for job #{}. CPU: {:.2f}, Mem: {:.2f} MB, Gres: {}.",
      job_id, res.allocatable_res_in_node().cpu_core_limit(),
      res.allocatable_res_in_node().memory_limit_bytes() / (1024.0 * 1024.0),
      util::ReadableGrpcDresInNode(res.dedicated_res_in_node()));

  bool ok = AllocatableResourceAllocator::Allocate(
      res.allocatable_res_in_node(), cg_unique_ptr.get());
  if (ok)
    ok &= DedicatedResourceAllocator::Allocate(res.dedicated_res_in_node(),
                                               cg_unique_ptr.get());
  return ok ? std::move(cg_unique_ptr) : nullptr;
}

std::unordered_map<ino_t, task_id_t> CgroupManager::GetCgJobIdMapCgroupV2(
    const std::string &root_cgroup_path) {
  std::unordered_map<ino_t, task_id_t> cg_job_id_map;
  try {
    for (const auto &it :
         std::filesystem::directory_iterator(root_cgroup_path)) {
      std::string job_id_str;
      if (it.is_directory()) {
        if (auto job_id = GetJobIdFromCg(it.path().filename());
            job_id.has_value()) {
          struct stat cg_stat{};
          if (stat(it.path().c_str(), &cg_stat)) {
            CRANE_ERROR("Cgroup {} stat failed: {}", it.path().c_str(),
                        std::strerror(errno));
            continue;
          }
          cg_job_id_map.emplace(cg_stat.st_ino, job_id.value());
        }
      }
    }
  } catch (const std::filesystem::filesystem_error &e) {
    CRANE_ERROR("Error: {}", e.what());
  }
  return cg_job_id_map;
}

#ifdef CRANE_ENABLE_BPF

CraneExpected<std::unordered_map<task_id_t, std::vector<BpfKey>>>
CgroupManager::GetJobBpfMapCgroupsV2(const std::string &root_cgroup_path) {
  std::unordered_map cg_ino_job_id_map =
      GetCgJobIdMapCgroupV2(root_cgroup_path);
  bool init_ebpf = !bpf_runtime_info.Valid();
  if (init_ebpf) {
    if (!bpf_runtime_info.InitializeBpfObj())
      return std::unexpected(CraneErrCode::ERR_EBPF);
  }

  std::unordered_map<task_id_t, std::vector<BpfKey>> results;

  auto add_task = [&results, &cg_ino_job_id_map](BpfKey *key) {
    // Skip log level record.
    if (key->cgroup_id == 0) {
      return;
    }
    CRANE_ASSERT(cg_ino_job_id_map.contains(key->cgroup_id));
    results[cg_ino_job_id_map[key->cgroup_id]].emplace_back(*key);
  };

  auto *pre_key = new BpfKey();
  if (bpf_map__get_next_key(bpf_runtime_info.BpfDevMap(), nullptr, pre_key,
                            sizeof(BpfKey)) == 0) {
    CRANE_ERROR("Failed to get first key of bpf map");
  }

  add_task(pre_key);
  auto *cur_key = new BpfKey();
  while (bpf_map__get_next_key(bpf_runtime_info.BpfDevMap(), pre_key, cur_key,
                               sizeof(BpfKey)) == 0) {
    add_task(cur_key);
  }

  delete pre_key;
  delete cur_key;
  if (init_ebpf) bpf_runtime_info.CloseBpfObj();

  return results;
}
#endif

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

CraneExpected<task_id_t> CgroupManager::GetTaskIdFromPid(pid_t pid) {
  static constexpr LazyRE2 cg_pattern(R"(.*/Crane_Task_(\d+).*)");
  std::string job_id_str;
  std::string cgroup_file = fmt::format("/proc/{}/cgroup", pid);
  std::ifstream infile(cgroup_file);
  if (!infile.is_open()) {
    CRANE_ERROR("Failed to open cgroup file for pid", pid);
    return std::unexpected(CraneErrCode::ERR_CGROUP);
  }
  if (m_cg_version_ == CgroupConstant::CgroupVersion::CGROUP_V1) {
    std::string line;
    while (std::getline(infile, line)) {
      if (RE2::FullMatch(line, *cg_pattern, &job_id_str)) {
        CRANE_TRACE("Get task Id {}", job_id_str);
        return std::stoi(job_id_str);
      }
    }
  } else if (m_cg_version_ == CgroupConstant::CgroupVersion::CGROUP_V2) {
    std::string line;
    if (!std::getline(infile, line)) {
      CRANE_ERROR("Failed to read cgroup file");
      return std::unexpected(CraneErrCode::ERR_SYSTEM_ERR);
    }
    if (RE2::FullMatch(line, *cg_pattern, &job_id_str)) {
      return std::stoi(job_id_str);
    }
  } else {
    std::unreachable();
  }
  return std::unexpected(CraneErrCode::ERR_NON_EXISTENT);
}

/*
 * Cleanup cgroup.
 * If the cgroup was created by us in the OS, remove it..
 */
Cgroup::~Cgroup() {
  if (m_cgroup_) {
    CRANE_DEBUG("Destroying cgroup {}.", m_cgroup_path_);
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

bool Cgroup::MigrateProcIn(pid_t pid) {
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
  err = cgroup_attach_task_pid(m_cgroup_, pid);
  if (err != 0) {
    CRANE_WARN("Cannot attach pid {} to cgroup {}: {} {}", pid,
               m_cgroup_path_.c_str(), err, cgroup_strerror(err));
  }
  return err == 0;
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
bool CgroupInterface::MigrateProcIn(pid_t pid) {
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

#ifdef CRANE_ENABLE_BPF

BpfRuntimeInfo::BpfRuntimeInfo() {
  bpf_obj_ = nullptr;
  bpf_prog_ = nullptr;
  dev_map_ = nullptr;
  enable_logging_ = false;
  bpf_mtx_ = new absl::Mutex;
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
  absl::MutexLock lk(bpf_mtx_);

  if (cgroup_count_ == 0) {
    bpf_obj_ = bpf_object__open_file(CgroupConstant::BpfObjectFilePath, NULL);
    if (!bpf_obj_) {
      CRANE_ERROR("Failed to open BPF object file {}",
                  CgroupConstant::BpfObjectFilePath);
      bpf_object__close(bpf_obj_);
      return false;
    }

    // ban libbpf log
    libbpf_print_fn_t fn = libbpf_set_print(NULL);

    if (bpf_object__load(bpf_obj_)) {
      CRANE_ERROR("Failed to load BPF object {}",
                  CgroupConstant::BpfObjectFilePath);
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
                  CgroupConstant::BpfObjectFilePath);
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
    struct BpfDeviceMeta meta = {static_cast<uint32_t>(enable_logging_),
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
  absl::MutexLock lk(bpf_mtx_);
  if (this->Valid() && --cgroup_count_ == 0) {
    close(bpf_prog_fd_);
    bpf_object__close(bpf_obj_);
    bpf_prog_fd_ = -1;
    bpf_obj_ = nullptr;
    bpf_prog_ = nullptr;
    dev_map_ = nullptr;
  }
}

void BpfRuntimeInfo::RmBpfDeviceMap() {
  try {
    if (std::filesystem::exists(CgroupConstant::BpfDeviceMapFilePath)) {
      std::filesystem::remove(CgroupConstant::BpfDeviceMapFilePath);
      CRANE_TRACE("Successfully removed: {}",
                  CgroupConstant::BpfDeviceMapFilePath);
    } else {
      CRANE_TRACE("File does not exist: {}",
                  CgroupConstant::BpfDeviceMapFilePath);
    }
  } catch (const std::filesystem::filesystem_error &e) {
    CRANE_ERROR("Error: {}", e.what());
  }
}
#endif

CgroupV2::CgroupV2(const std::string &path, struct cgroup *handle, uint64_t id)
    : CgroupInterface(path, handle, id) {
#ifdef CRANE_ENABLE_BPF
  if (CgroupManager::bpf_runtime_info.InitializeBpfObj()) {
    CRANE_TRACE("Bpf object initialization succeed");
  } else {
    CRANE_TRACE("Bpf object initialization failed");
  }
#endif
}

#ifdef CRANE_ENABLE_BPF
CgroupV2::CgroupV2(const std::string &path, struct cgroup *handle, uint64_t id,
                   std::vector<BpfDeviceMeta> &cgroup_bpf_devices)
    : CgroupV2(path, handle, id) {
  m_cgroup_bpf_devices = std::move(cgroup_bpf_devices);
  m_bpf_attached_ = true;
}
#endif

CgroupV2::~CgroupV2() {
#ifdef CRANE_ENABLE_BPF
  if (!m_cgroup_bpf_devices.empty()) {
    EraseBpfDeviceMap();
  }
  CgroupManager::bpf_runtime_info.CloseBpfObj();
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
  if (!CgroupManager::bpf_runtime_info.Valid()) {
    CRANE_WARN("BPF is not initialized.");
    return false;
  }
  int cgroup_fd;
  std::string slash = "/";
  std::filesystem::path cgroup_path = CgroupConstant::RootCgroupFullPath +
                                      slash + m_cgroup_info_.m_cgroup_path_;
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
    absl::MutexLock lk(CgroupManager::bpf_runtime_info.BpfMutex());
    for (int i = 0; i < bpf_devices.size(); i++) {
      struct BpfKey key = {m_cgroup_info_.m_cgroup_id, bpf_devices[i].major,
                           bpf_devices[i].minor};
      if (bpf_map__update_elem(CgroupManager::bpf_runtime_info.BpfDevMap(),
                               &key, sizeof(BpfKey), &bpf_devices[i],
                               sizeof(BpfDeviceMeta), BPF_ANY)) {
        CRANE_ERROR("Failed to update BPF map major {},minor {} cgroup id {}",
                    bpf_devices[i].major, bpf_devices[i].minor, key.cgroup_id);
        close(cgroup_fd);
        return false;
      }
    }

    // No need to attach ebpf prog twice.
    if (!m_bpf_attached_) {
      if (bpf_prog_attach(CgroupManager::bpf_runtime_info.BpfProgFd(),
                          cgroup_fd, BPF_CGROUP_DEVICE, 0) < 0) {
        CRANE_ERROR("Failed to attach BPF program");
        close(cgroup_fd);
        return false;
      }
      m_bpf_attached_ = true;
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

bool CgroupV2::RecoverFromCgSpec(const CgroupSpec &cg_spec) {
  if (!CgroupManager::bpf_runtime_info.Valid()) {
    CRANE_WARN("BPF is not initialized.");
    return false;
  }
  int cgroup_fd;
  std::string slash = "/";
  std::filesystem::path cgroup_path = CgroupConstant::RootCgroupFullPath +
                                      slash + m_cgroup_info_.m_cgroup_path_;
  cgroup_fd = open(cgroup_path.c_str(), O_RDONLY);
  if (cgroup_fd < 0) {
    CRANE_ERROR("Failed to open cgroup");
    return false;
  }

  short access = 0;
  if (CgroupConstant::CgroupLimitDeviceRead) access |= BPF_DEVCG_ACC_READ;
  if (CgroupConstant::CgroupLimitDeviceWrite) access |= BPF_DEVCG_ACC_WRITE;
  if (CgroupConstant::CgroupLimitDeviceMknod) access |= BPF_DEVCG_ACC_MKNOD;

  std::unordered_set<std::string> all_request_slots;
  for (const auto &[_, type_slots_map] :
       cg_spec.res_in_node.dedicated_res_in_node().name_type_map()) {
    for (const auto &[__, slots] : type_slots_map.type_slots_map())
      all_request_slots.insert(slots.slots().cbegin(), slots.slots().cend());
  };

  auto &bpf_devices = m_cgroup_bpf_devices;
  for (const auto &[_, this_device] : Craned::g_this_node_device) {
    if (!all_request_slots.contains(this_device->slot_id)) {
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
  m_bpf_attached_ = true;
  return true;
}

bool CgroupV2::EraseBpfDeviceMap() {
  {
    if (!CgroupManager::bpf_runtime_info.Valid()) {
      CRANE_WARN("BPF is not initialized.");
      return false;
    }
    absl::MutexLock lk(CgroupManager::bpf_runtime_info.BpfMutex());
    auto &bpf_devices = m_cgroup_bpf_devices;
    for (int i = 0; i < bpf_devices.size(); i++) {
      struct BpfKey key = {m_cgroup_info_.m_cgroup_id, bpf_devices[i].major,
                           bpf_devices[i].minor};
      if (bpf_map__delete_elem(CgroupManager::bpf_runtime_info.BpfDevMap(),
                               &key, sizeof(BpfKey), BPF_ANY)) {
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
  for (const auto &type_slots_map :
       request_resource.name_type_map() | std::ranges::views::values) {
    for (const auto &slots :
         type_slots_map.type_slots_map() | std::ranges::views::values)
      all_request_slots.insert(slots.slots().cbegin(), slots.slots().cend());
  };

  if (!cg->SetDeviceAccess(all_request_slots,
                           CgroupConstant::CgroupLimitDeviceRead,
                           CgroupConstant::CgroupLimitDeviceWrite,
                           CgroupConstant::CgroupLimitDeviceMknod)) {
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
}  // namespace Craned
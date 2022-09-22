
/*
 * Utility library for libcgroup initialization routines.
 *
 */

#include "cgroup.linux.h"

#include <csignal>
#include <fstream>

namespace util {

/*
 * Create a CgroupManager.  Note this is private - users of the CgroupManager
 * may create an instance via CgroupManager::getInstance()
 */

CgroupManager::CgroupManager() : m_mounted_controllers_() { initialize(); }

CgroupManager &CgroupManager::Instance() {
  static CgroupManager *singleton = new CgroupManager;
  return *singleton;
}

/*
 * Initialize libcgroup and mount the controllers Condor will use (if possible)
 *
 * Returns 0 on success, -1 otherwise.
 */
int CgroupManager::initialize() {
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
int CgroupManager::initialize_controller(
    struct cgroup &cgroup, const CgroupConstant::Controller controller,
    const bool required, const bool has_cgroup, bool &changed_cgroup) const {
  std::string_view controller_str =
      CgroupConstant::GetControllerStringView(controller);

  if (!Mounted(controller)) {
    if (required) {
      CRANE_WARN("Error - cgroup controller {} not mounted, but required.\n",
                 CgroupConstant::GetControllerStringView(controller));
      return 1;
    } else {
      fmt::print("cgroup controller {} is already mounted");
      return 0;
    }
  }

  if (!has_cgroup ||
      (cgroup_get_controller(&cgroup, controller_str.data()) == nullptr)) {
    changed_cgroup = true;
    if (cgroup_add_controller(&cgroup, controller_str.data()) == nullptr) {
      CRANE_WARN("Unable to initialize cgroup {} controller.\n",
                 controller_str);
      return required ? 1 : 0;
    }
  }

  return 0;
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
Cgroup *CgroupManager::CreateOrOpen(const std::string &cgroup_string,
                                    ControllerFlags preferred_controllers,
                                    ControllerFlags required_controllers,
                                    bool retrieve) {
  // Todo: In our design, the CgroupManager is the only owner and manager of
  //  all Cgroup in the system. Therefore, when creating a cgroup, there's no
  //  need to use the cgroup_get_cgroup in libcgroup function to check the
  //  existence of the cgroup.

  using CgroupConstant::Controller;
  using CgroupConstant::GetControllerStringView;

  LockGuard guard(m_mtx_);

  auto iter = m_cgroup_ref_count_map_.find(cgroup_string);
  if (iter != m_cgroup_ref_count_map_.end()) {
    iter->second.second++;
    return iter->second.first.get();
  }

  bool created_cgroup = false, changed_cgroup = false;
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
  if ((preferred_controllers & Controller::CPUACCT_CONTROLLER) &&
      initialize_controller(
          *native_cgroup, Controller::CPUACCT_CONTROLLER,
          required_controllers & Controller::CPUACCT_CONTROLLER, has_cgroup,
          changed_cgroup)) {
    return nullptr;
  }
  if ((preferred_controllers & Controller::MEMORY_CONTROLLER) &&
      initialize_controller(
          *native_cgroup, Controller::MEMORY_CONTROLLER,
          required_controllers & Controller::MEMORY_CONTROLLER, has_cgroup,
          changed_cgroup)) {
    return nullptr;
  }
  if ((preferred_controllers & Controller::FREEZE_CONTROLLER) &&
      initialize_controller(
          *native_cgroup, Controller::FREEZE_CONTROLLER,
          required_controllers & Controller::FREEZE_CONTROLLER, has_cgroup,
          changed_cgroup)) {
    return nullptr;
  }
  if ((preferred_controllers & Controller::BLOCK_CONTROLLER) &&
      initialize_controller(*native_cgroup, Controller::BLOCK_CONTROLLER,
                            required_controllers & Controller::BLOCK_CONTROLLER,
                            has_cgroup, changed_cgroup)) {
    return nullptr;
  }
  if ((preferred_controllers & Controller::CPU_CONTROLLER) &&
      initialize_controller(*native_cgroup, Controller::CPU_CONTROLLER,
                            required_controllers & Controller::CPU_CONTROLLER,
                            has_cgroup, changed_cgroup)) {
    return nullptr;
  }
  if ((preferred_controllers & Controller::DEVICES_CONTROLLER) &&
      initialize_controller(
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
          "Unable to create cgroup {}."
          " Cgroup functionality will not work: {}\n",
          cgroup_string.c_str(), cgroup_strerror(err));
      return nullptr;
    } else {
      created_cgroup = true;
    }
  } else if (has_cgroup && changed_cgroup &&
             (err = cgroup_modify_cgroup(native_cgroup))) {
    CRANE_WARN(
        "Unable to modify cgroup {}."
        "  Some cgroup functionality may not work: {} {}\n",
        cgroup_string.c_str(), err, cgroup_strerror(err));
  }

  // Try to turn on hierarchical memory accounting.
  struct cgroup_controller *mem_controller = cgroup_get_controller(
      native_cgroup,
      GetControllerStringView(Controller::MEMORY_CONTROLLER).data());
  if (retrieve && Mounted(Controller::MEMORY_CONTROLLER) && created_cgroup &&
      (mem_controller != NULL)) {
    // Todo: What's memory.use_hierarchy? Try to eliminate `retrieve`.
    if ((err = cgroup_add_value_bool(mem_controller, "memory.use_hierarchy",
                                     true))) {
      CRANE_WARN("Unable to set hierarchical memory settings for {}: {} {}\n",
                 cgroup_string.c_str(), err, cgroup_strerror(err));
    } else {
      if ((err = cgroup_modify_cgroup(native_cgroup))) {
        CRANE_WARN(
            "Unable to enable hierarchical memory accounting for {} "
            ": {} {}\n",
            cgroup_string.c_str(), err, cgroup_strerror(err));
      }
    }
  }

  auto cgroup = std::make_unique<Cgroup>(cgroup_string, native_cgroup);
  auto *p = cgroup.get();

  m_cgroup_ref_count_map_.emplace(cgroup_string,
                                  std::make_pair(std::move(cgroup), 1));

  return p;
}

bool CgroupManager::Release(const std::string &cgroup_path) {
  LockGuard guard(m_mtx_);

  auto it = m_cgroup_ref_count_map_.find(cgroup_path);
  if (it == m_cgroup_ref_count_map_.end()) {
    CRANE_WARN("Destroying an unknown cgroup.");
    return false;
  }

  size_t *ref_cnt = &it->second.second;
  (*ref_cnt)--;

  // Only delete if this is the last ref and we originally created it.
  if (*ref_cnt == 0) {
    int err;
    // Must re-initialize the cgroup structure before deletion.
    struct cgroup *dcg = cgroup_new_cgroup(cgroup_path.c_str());
    assert(dcg != nullptr);
    if ((err = cgroup_get_cgroup(dcg))) {
      CRANE_WARN("Unable to read cgroup {} for deletion: {} {}\n",
                 cgroup_path.c_str(), err, cgroup_strerror(err));
      cgroup_free(&dcg);
      return false;
    }

    // CGFLAG_DELETE_EMPTY_ONLY is set to avoid libgroup from finding parent
    // cgroup, which is usually the mount point of root cgroup and will cause
    // ENOENT error.
    //
    // Todo: Test this part when cgroup is not empty!
    if ((err = cgroup_delete_cgroup_ext(
             dcg, CGFLAG_DELETE_EMPTY_ONLY | CGFLAG_DELETE_IGNORE_MIGRATION))) {
      CRANE_WARN("Unable to completely remove cgroup {}: {} {}\n",
                 cgroup_path.c_str(), err, cgroup_strerror(err));
    } else {
      CRANE_TRACE("Deleted cgroup {}.", cgroup_path.c_str());
    }

    // Notice the cgroup struct freed here is not the one held by Cgroup class.
    cgroup_free(&dcg);

    // This call results in the destructor call of Cgroup, which frees the
    // internal libcgroup struct.
    m_cgroup_ref_count_map_.erase(cgroup_path);
  }

  return true;
}

bool CgroupManager::MigrateProcTo(pid_t pid, const std::string &cgroup_path) {
  LockGuard guard(m_mtx_);

  // Attempt to migrate a given process to a cgroup.
  // This can be done without regards to whether the
  // process is already in the cgroup
  auto iter = m_cgroup_ref_count_map_.find(cgroup_path);
  if (iter == m_cgroup_ref_count_map_.end()) {
    CRANE_WARN(cgroup_path);
    return false;
  }

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
  struct cgroup *pcg = iter->second.first->m_cgroup_;
  err = cgroup_attach_task_pid(pcg, pid);
  if (err != 0) {
    CRANE_WARN("Cannot attach pid {} to cgroup {}: {} {}\n", pid,
               cgroup_path.c_str(), err, cgroup_strerror(err));
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

Cgroup *CgroupManager::Find(const std::string &cgroup_path) {
  auto iter = m_cgroup_ref_count_map_.find(cgroup_path);
  if (iter == m_cgroup_ref_count_map_.end()) return nullptr;

  return iter->second.first.get();
}

/*
 * Cleanup cgroup.
 * If the cgroup was created by us in the OS, remove it..
 */
Cgroup::~Cgroup() {
  if (m_cgroup_) {
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
bool Cgroup::SetCpuCoreLimit(uint64_t core_num) {
  constexpr uint32_t base = 1000'000;

  bool ret;
  ret = SetControllerValue(CgroupConstant::Controller::CPU_CONTROLLER,
                           CgroupConstant::ControllerFile::CPU_CFS_QUOTA_US,
                           base * core_num);
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
  CgroupManager &cm = CgroupManager::Instance();

  if (!cm.Mounted(controller)) {
    CRANE_WARN("Unable to set {} because cgroup {} is not mounted.\n",
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

  if ((err = cgroup_set_value_uint64(
           cg_controller,
           CgroupConstant::GetControllerFileStringView(controller_file).data(),
           value))) {
    CRANE_WARN("Unable to set uint64 value for {}: {} {}\n", m_cgroup_path_,
               err, cgroup_strerror(err));
    return false;
  }

  // Commit cgroup modifications.
  if ((err = cgroup_modify_cgroup(m_cgroup_))) {
    CRANE_WARN("Unable to commit {} for cgroup {}: {} {}",
               CgroupConstant::GetControllerFileStringView(controller_file),
               m_cgroup_path_, err, cgroup_strerror(err));
    return false;
  }

  return true;
}

bool Cgroup::SetControllerStr(CgroupConstant::Controller controller,
                              CgroupConstant::ControllerFile controller_file,
                              const std::string &str) {
  CgroupManager &cm = CgroupManager::Instance();

  if (!cm.Mounted(controller)) {
    CRANE_WARN("Unable to set {} because cgroup {} is not mounted.\n",
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

  if ((err = cgroup_set_value_string(
           cg_controller,
           CgroupConstant::GetControllerFileStringView(controller_file).data(),
           str.c_str()))) {
    CRANE_WARN("Unable to set string for {}: {} {}\n", m_cgroup_path_, err,
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

}  // namespace util
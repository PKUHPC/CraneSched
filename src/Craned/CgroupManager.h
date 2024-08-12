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
 * Utility functions for dealing with libcgroup.
 *
 * This is not meant to replace direct interaction with libcgroup, however
 * it provides some simple initialization and RAII wrappers.
 *
 */
#pragma once

#include <libcgroup.h>

#include "CranedPublicDefs.h"
#include "crane/AtomicHashMap.h"

namespace Craned {

namespace CgroupConstant {

enum class Controller : uint64_t {
  MEMORY_CONTROLLER = 0,
  CPUACCT_CONTROLLER,
  FREEZE_CONTROLLER,
  BLOCK_CONTROLLER,
  CPU_CONTROLLER,
  DEVICES_CONTROLLER,

  ControllerCount,
};

enum class ControllerFile : uint64_t {
  CPU_SHARES = 0,
  CPU_CFS_PERIOD_US,
  CPU_CFS_QUOTA_US,

  MEMORY_LIMIT_BYTES,
  MEMORY_MEMSW_LIMIT_IN_BYTES,
  MEMORY_SOFT_LIMIT_BYTES,

  BLOCKIO_WEIGHT,

  DEVICES_DENY,
  DEVICES_ALLOW,

  ControllerFileCount
};

namespace Internal {

constexpr std::array<std::string_view,
                     static_cast<size_t>(Controller::ControllerCount)>
    ControllerStringView{
        "memory", "cpuacct", "freezer", "blkio", "cpu", "devices",
    };

constexpr std::array<std::string_view,
                     static_cast<size_t>(ControllerFile::ControllerFileCount)>
    ControllerFileStringView{
        "cpu.shares",
        "cpu.cfs_period_us",
        "cpu.cfs_quota_us",

        "memory.limit_in_bytes",
        "memory.memsw.limit_in_bytes",
        "memory.soft_limit_in_bytes",

        "blkio.weight",

        "devices.deny",
        "devices.allow",
    };
}  // namespace Internal

constexpr std::string_view GetControllerStringView(Controller controller) {
  return Internal::ControllerStringView[static_cast<uint64_t>(controller)];
}

constexpr std::string_view GetControllerFileStringView(
    ControllerFile controller_file) {
  return Internal::ControllerFileStringView[static_cast<uint64_t>(
      controller_file)];
}

}  // namespace CgroupConstant

class ControllerFlags {
 public:
  ControllerFlags() noexcept : m_flags_(0u) {}

  explicit ControllerFlags(CgroupConstant::Controller controller) noexcept
      : m_flags_(1u << static_cast<uint64_t>(controller)) {}

  ControllerFlags(const ControllerFlags &val) noexcept = default;

  ControllerFlags operator|=(const ControllerFlags &rhs) noexcept {
    m_flags_ |= rhs.m_flags_;
    return *this;
  }

  ControllerFlags operator&=(const ControllerFlags &rhs) noexcept {
    m_flags_ &= rhs.m_flags_;
    return *this;
  }

  operator bool() const noexcept { return static_cast<bool>(m_flags_); }

  ControllerFlags operator~() const noexcept {
    ControllerFlags cf;
    cf.m_flags_ = ~m_flags_;
    return cf;
  }

 private:
  friend ControllerFlags operator|(const ControllerFlags &lhs,
                                   const ControllerFlags &rhs) noexcept;
  friend ControllerFlags operator&(const ControllerFlags &lhs,
                                   const ControllerFlags &rhs) noexcept;
  friend ControllerFlags operator|(
      const ControllerFlags &lhs,
      const CgroupConstant::Controller &rhs) noexcept;
  friend ControllerFlags operator&(
      const ControllerFlags &lhs,
      const CgroupConstant::Controller &rhs) noexcept;
  friend ControllerFlags operator|(
      const CgroupConstant::Controller &lhs,
      const CgroupConstant::Controller &rhs) noexcept;
  uint64_t m_flags_;
};

inline ControllerFlags operator|(const ControllerFlags &lhs,
                                 const ControllerFlags &rhs) noexcept {
  ControllerFlags flags;
  flags.m_flags_ = lhs.m_flags_ | rhs.m_flags_;
  return flags;
}

inline ControllerFlags operator&(const ControllerFlags &lhs,
                                 const ControllerFlags &rhs) noexcept {
  ControllerFlags flags;
  flags.m_flags_ = lhs.m_flags_ & rhs.m_flags_;
  return flags;
}

inline ControllerFlags operator|(
    const ControllerFlags &lhs,
    const CgroupConstant::Controller &rhs) noexcept {
  ControllerFlags flags;
  flags.m_flags_ = lhs.m_flags_ | (1u << static_cast<uint64_t>(rhs));
  return flags;
}

inline ControllerFlags operator&(
    const ControllerFlags &lhs,
    const CgroupConstant::Controller &rhs) noexcept {
  ControllerFlags flags;
  flags.m_flags_ = lhs.m_flags_ & (1u << static_cast<uint64_t>(rhs));
  return flags;
}

inline ControllerFlags operator|(
    const CgroupConstant::Controller &lhs,
    const CgroupConstant::Controller &rhs) noexcept {
  ControllerFlags flags;
  flags.m_flags_ =
      (1u << static_cast<uint64_t>(lhs)) | (1u << static_cast<uint64_t>(rhs));
  return flags;
}

const ControllerFlags NO_CONTROLLER_FLAG{};

// In many distributions, 'cpu' and 'cpuacct' are mounted together. 'cpu'
//  and 'cpuacct' both point to a single 'cpu,cpuacct' account. libcgroup
//  handles this for us and no additional care needs to be taken.
const ControllerFlags ALL_CONTROLLER_FLAG = (~NO_CONTROLLER_FLAG);

class Cgroup {
 public:
  Cgroup(const std::string &path, struct cgroup *handle)
      : m_cgroup_path_(path), m_cgroup_(handle) {}
  ~Cgroup();

  struct cgroup *NativeHandle() { return m_cgroup_; }

  const std::string &GetCgroupString() const { return m_cgroup_path_; };

  // Using the zombie object pattern as exceptions are not available.
  bool Valid() const { return m_cgroup_ != nullptr; }

  bool SetCpuCoreLimit(double core_num);
  bool SetCpuShares(uint64_t share);
  bool SetMemoryLimitBytes(uint64_t memory_bytes);
  bool SetMemorySwLimitBytes(uint64_t mem_bytes);
  bool SetMemorySoftLimitBytes(uint64_t memory_bytes);
  bool SetBlockioWeight(uint64_t weight);
  bool SetDeviceAccess(const std::unordered_set<SlotId> &devices, bool set_read,
                       bool set_write, bool set_mknod);
  bool SetControllerValue(CgroupConstant::Controller controller,
                          CgroupConstant::ControllerFile controller_file,
                          uint64_t value);
  bool SetControllerStr(CgroupConstant::Controller controller,
                        CgroupConstant::ControllerFile controller_file,
                        const std::string &str);
  bool SetControllerStrs(CgroupConstant::Controller controller,
                         CgroupConstant::ControllerFile controller_file,
                         const std::vector<std::string> &strs);
  bool KillAllProcesses();

  bool Empty();

  bool MigrateProcIn(pid_t pid);

 private:
  bool ModifyCgroup_(CgroupConstant::ControllerFile controller_file);

  std::string m_cgroup_path_;
  mutable struct cgroup *m_cgroup_;
};

class AllocatableResourceAllocator {
 public:
  static bool Allocate(const AllocatableResource &resource, Cgroup *cg);
  static bool Allocate(const crane::grpc::AllocatableResource &resource,
                       Cgroup *cg);
};

class DedicatedResourceAllocator {
 public:
  static bool Allocate(
      const crane::grpc::DedicatedResourceInNode &request_resource, Cgroup *cg);
};

class CgroupManager {
 public:
  int Init();

  bool Mounted(CgroupConstant::Controller controller) {
    return bool(m_mounted_controllers_ & ControllerFlags{controller});
  }

  bool QueryTaskInfoOfUidAsync(uid_t uid, TaskInfoOfUid *info);

  std::optional<std::string> QueryTaskExecutionNode(task_id_t task_id);

  bool CreateCgroups(std::vector<CgroupSpec> &&cg_specs);

  bool CheckIfCgroupForTasksExists(task_id_t task_id);

  bool AllocateAndGetCgroup(task_id_t task_id, Cgroup **cg);

  bool MigrateProcToCgroupOfTask(pid_t pid, task_id_t task_id);

  bool ReleaseCgroup(uint32_t task_id, uid_t uid);

  bool ReleaseCgroupByTaskIdOnly(task_id_t task_id);

  std::vector<EnvPair> GetResourceEnvListOfTask(task_id_t task_id);

 private:
  static std::string CgroupStrByTaskId_(task_id_t task_id);

  std::unique_ptr<Cgroup> CreateOrOpen_(const std::string &cgroup_string,
                                        ControllerFlags preferred_controllers,
                                        ControllerFlags required_controllers,
                                        bool retrieve);

  int InitializeController_(struct cgroup &cgroup,
                            CgroupConstant::Controller controller,
                            bool required, bool has_cgroup,
                            bool &changed_cgroup);

  ControllerFlags m_mounted_controllers_;

  util::AtomicHashMap<absl::flat_hash_map, task_id_t, CgroupSpec>
      m_task_id_to_cg_spec_map_;

  util::AtomicHashMap<absl::flat_hash_map, task_id_t, std::unique_ptr<Cgroup>>
      m_task_id_to_cg_map_;

  util::AtomicHashMap<absl::flat_hash_map, uid_t /*uid*/,
                      absl::flat_hash_set<task_id_t>>
      m_uid_to_task_ids_map_;
};

}  // namespace Craned

inline std::unique_ptr<Craned::CgroupManager> g_cg_mgr;
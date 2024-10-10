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
#include "crane/OS.h"

namespace Craned {

namespace CgroupConstant {

enum class CgroupVersion : uint64_t {
  CGROUP_V1 = 0,
  CGROUP_V2,
  UNDEFINED,
};

enum class Controller : uint64_t {
  MEMORY_CONTROLLER = 0,
  CPUACCT_CONTROLLER,
  FREEZE_CONTROLLER,
  BLOCK_CONTROLLER,
  CPU_CONTROLLER,
  DEVICES_CONTROLLER,

  MEMORY_CONTORLLER_V2,
  CPU_CONTROLLER_V2,
  IO_CONTROLLER_V2,
  CPUSET_CONTROLLER_V2,
  PIDS_CONTROLLER_V2,

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
  // V2

  CPU_WEIGHT_V2,
  CPU_MAX_V2,

  MEMORY_MAX_V2,
  MEMORY_SWAP_MAX_V2,
  MEMORY_HIGH_V2,

  IO_WEIGHT_V2,
  // root cgroup controller can't be change or created

  ControllerFileCount,
};

// enum class ControllerV2 : uint64_t{
//   MEMORY_CONTORLLER_V2 = 0,
//   CPU_CONTROLLER_V2,
//   IO_CONTROLLER_V2,
//   CGROUP_CONTOLLER_V2,
//   CPUSET_CONTROLLER_V2,
//   PIDS_CONTROLLER_V2,

//   ControllerV2Count,
// };

// enum class ControllerV2File : uint64_t{
//   CPU_WEIGHT = 0,
//   CPU_MAX,

//   MEMORY_MAX,
//   MEMORY_SWAP_MAX,
//   MEMORY_HIGH,

//   IO_WEIGHT,

//   CGROUP_SUBTREE_CONTROL,

//   ControllerV2FileCount
// };

inline const char *kTaskCgPathPrefix = "Crane_Task_";
inline const char *RootCgroupFullPath = "/sys/fs/cgroup";
namespace Internal {

constexpr std::array<std::string_view,
                     static_cast<size_t>(Controller::ControllerCount)>
    ControllerStringView{
        "memory",
        "cpuacct",
        "freezer",
        "blkio",
        "cpu",
        "devices",
        // V2
        "memory",
        "cpu",
        "io",
        "cpuset",
        "pids",
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
        // V2

        "cpu.weight",
        "cpu.max",

        "memory.max",
        "memory.swap.max",
        "memory.high",

        "io.weight",

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
  virtual ~Cgroup();

  struct cgroup *NativeHandle() { return m_cgroup_; }

  const std::string &GetCgroupString() const { return m_cgroup_path_; };

  // Using the zombie object pattern as exceptions are not available.
  bool Valid() const { return m_cgroup_ != nullptr; }

  virtual bool SetCpuCoreLimit(double core_num) = 0;
  virtual bool SetCpuShares(uint64_t share) = 0;
  virtual bool SetMemoryLimitBytes(uint64_t memory_bytes) = 0;
  virtual bool SetMemorySwLimitBytes(uint64_t mem_bytes) = 0;
  virtual bool SetMemorySoftLimitBytes(uint64_t memory_bytes) = 0;
  virtual bool SetBlockioWeight(uint64_t weight) = 0;
  virtual bool SetDeviceAccess(const std::unordered_set<SlotId> &devices,
                               bool set_read, bool set_write,
                               bool set_mknod) = 0;

  bool SetControllerValue(CgroupConstant::Controller controller,
                          CgroupConstant::ControllerFile controller_file,
                          uint64_t value);
  bool SetControllerStr(CgroupConstant::Controller controller,
                        CgroupConstant::ControllerFile controller_file,
                        const std::string &str);
  bool SetControllerStrs(CgroupConstant::Controller controller,
                         CgroupConstant::ControllerFile controller_file,
                         const std::vector<std::string> &strs);
  virtual bool MigrateProcIn(pid_t pid) = 0;

  virtual bool KillAllProcesses() = 0;

  virtual bool Empty() = 0;

 protected:
  // CgroupConstant::CgroupVersion cg_vsion; // maybe for hybird mode
  virtual bool ModifyCgroup_(CgroupConstant::ControllerFile controller_file);
  std::string m_cgroup_path_;
  mutable struct cgroup *m_cgroup_;
};

class CgroupV1 : public Cgroup {
 public:
  CgroupV1(const std::string &path, struct cgroup *handle)
      : Cgroup(path, handle) {}
  ~CgroupV1() = default;
  bool SetCpuCoreLimit(double core_num) override;
  bool SetCpuShares(uint64_t share) override;
  bool SetMemoryLimitBytes(uint64_t memory_bytes) override;
  bool SetMemorySwLimitBytes(uint64_t mem_bytes) override;
  bool SetMemorySoftLimitBytes(uint64_t memory_bytes) override;
  bool SetBlockioWeight(uint64_t weight) override;

  bool SetDeviceAccess(const std::unordered_set<SlotId> &devices, bool set_read,
                       bool set_write, bool set_mknod) override;

  bool KillAllProcesses() override;

  bool Empty() override;

  bool MigrateProcIn(pid_t pid) override;

 private:
};

class CgroupV2 : public Cgroup {
 public:
  CgroupV2(const std::string &path, struct cgroup *handle)
      : Cgroup(path, handle) {}
  ~CgroupV2() = default;
  bool SetCpuCoreLimit(double core_num) override;
  bool SetCpuShares(uint64_t share) override;
  bool SetMemoryLimitBytes(uint64_t memory_bytes) override;
  bool SetMemorySwLimitBytes(uint64_t mem_bytes) override;
  bool SetMemorySoftLimitBytes(uint64_t memory_bytes) override;
  bool SetBlockioWeight(uint64_t weight) override;

  // use BPF
  bool SetDeviceAccess(const std::unordered_set<SlotId> &devices, bool set_read,
                       bool set_write, bool set_mknod) override;

  bool KillAllProcesses() override;

  bool Empty() override;

  bool MigrateProcIn(pid_t pid) override;

 private:
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

  void ControllersMounted();

  bool QueryTaskInfoOfUidAsync(uid_t uid, TaskInfoOfUid *info);

  std::optional<std::string> QueryTaskExecutionNode(task_id_t task_id);

  bool CreateCgroups(std::vector<CgroupSpec> &&cg_specs);

  bool CheckIfCgroupForTasksExists(task_id_t task_id);

  bool AllocateAndGetCgroup(task_id_t task_id, Cgroup **cg);

  bool MigrateProcToCgroupOfTask(pid_t pid, task_id_t task_id);

  bool ReleaseCgroup(uint32_t task_id, uid_t uid);

  bool ReleaseCgroupByTaskIdOnly(task_id_t task_id);

  std::optional<crane::grpc::ResourceInNode> GetTaskResourceInNode(
      task_id_t task_id);

  static std::vector<EnvPair> GetResourceEnvListByResInNode(
      const crane::grpc::ResourceInNode &res_in_node);

  std::vector<EnvPair> GetResourceEnvListOfTask(task_id_t task_id);

  void SetCgroupVersion(CgroupConstant::CgroupVersion v) { cg_version_ = v; }

  CgroupConstant::CgroupVersion GetCgroupVersion() { return cg_version_; }

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

  void RmAllTaskCgroups_();
  void RmAllTaskCgroupsUnderController_(CgroupConstant::Controller controller);

  void RmAllTaskCgroupsV2_();
  void RmCgroupsV2_(const std::string &root_cgroup_path,
                    const std::string &match_str);

  ControllerFlags m_mounted_controllers_;

  CgroupConstant::CgroupVersion cg_version_;

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
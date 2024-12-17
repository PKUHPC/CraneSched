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
#include "SupervisorPublicDefs.h"
// Precompiled header comes first.

#include <libcgroup.h>
#include <protos/Supervisor.pb.h>

#include "crane/AtomicHashMap.h"

#ifdef CRANE_ENABLE_BPF
#  include <bpf/libbpf.h>
#endif

namespace Supervisor {

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

inline const char *kTaskCgPathPrefix = "Crane_Task_";
inline const char *RootCgroupFullPath = "/sys/fs/cgroup";
#ifdef CRANE_ENABLE_BPF
inline const char *BpfObjectFile = "/usr/local/lib64/bpf/cgroup_dev_bpf.o";
inline const char *BpfDeviceMapFile = "/sys/fs/bpf/craned_dev_map";
inline const char *BpfMapName = "craned_dev_map";
inline const char *BpfProgramName = "craned_device_access";
#endif
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

#ifdef CRANE_ENABLE_BPF
enum BPF_PERMISSION { ALLOW = 0, DENY };

#  pragma pack(push, 8)
struct BpfKey {
  uint64_t cgroup_id;
  uint32_t major;
  uint32_t minor;
};
#  pragma pack(pop)

#  pragma pack(push, 8)
struct BpfDeviceMeta {
  uint32_t major;
  uint32_t minor;
  int permission;
  short access;
  short type;
};
#  pragma pack(pop)
#endif

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

class CgroupInterface {
 public:
  virtual ~CgroupInterface() {}
  virtual bool SetCpuCoreLimit(double core_num) = 0;
  virtual bool SetCpuShares(uint64_t share) = 0;
  virtual bool SetMemoryLimitBytes(uint64_t memory_bytes) = 0;
  virtual bool SetMemorySwLimitBytes(uint64_t mem_bytes) = 0;
  virtual bool SetMemorySoftLimitBytes(uint64_t memory_bytes) = 0;
  virtual bool SetBlockioWeight(uint64_t weight) = 0;
  virtual bool SetDeviceAccess(const std::unordered_set<SlotId> &devices,
                               bool set_read, bool set_write,
                               bool set_mknod) = 0;
  virtual bool SetDeviceAccess(const std::unordered_set<SlotId> &devices,
                               bool set_read, bool set_write,
                               bool set_mknod) = 0;
  virtual bool MigrateProcIn(pid_t pid) = 0;

  virtual bool KillAllProcesses() = 0;

  virtual bool Empty() = 0;
  virtual const std::string &GetCgroupString() const = 0;
};

class Cgroup {
 public:
  Cgroup(const std::string &path, struct cgroup *handle, uint64_t id = 0)
      : m_cgroup_path_(path), m_cgroup_(handle), m_cgroup_id(id) {}
  ~Cgroup();

  struct cgroup *NativeHandle() { return m_cgroup_; }

  // Using the zombie object pattern as exceptions are not available.
  bool Valid() const { return m_cgroup_ != nullptr; }

  bool SetControllerValue(CgroupConstant::Controller controller,
                          CgroupConstant::ControllerFile controller_file,
                          uint64_t value);
  bool SetControllerStr(CgroupConstant::Controller controller,
                        CgroupConstant::ControllerFile controller_file,
                        const std::string &str);
  bool SetControllerStrs(CgroupConstant::Controller controller,
                         CgroupConstant::ControllerFile controller_file,
                         const std::vector<std::string> &strs);

  // CgroupConstant::CgroupVersion cg_vsion; // maybe for hybird mode
  bool ModifyCgroup_(CgroupConstant::ControllerFile controller_file);
  std::string m_cgroup_path_;
  mutable struct cgroup *m_cgroup_;
  uint64_t m_cgroup_id;
};

class CgroupV1 : public CgroupInterface {
 public:
  CgroupV1(const std::string &path, struct cgroup *handle)
      : m_cgroup_info_(path, handle) {}
  ~CgroupV1() override = default;
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

  const std::string &GetCgroupString() const override {
    return m_cgroup_info_.m_cgroup_path_;
  }

 private:
  Cgroup m_cgroup_info_;
};

#ifdef CRANE_ENABLE_BPF
class BpfRuntimeInfo {
 public:
  BpfRuntimeInfo();
  ~BpfRuntimeInfo();
  bool InitializeBpfObj();
  void CloseBpfObj();
  void RmBpfDeviceMap();

  struct bpf_object *BpfObj() { return bpf_obj_; }
  struct bpf_program *BpfProgram() { return bpf_prog_; }
  std::mutex *BpfMutex() { return bpf_mtx_; }
  struct bpf_map *BpfDevMap() { return dev_map_; }
  int BpfProgFd() { return bpf_prog_fd_; }
  void SetLogLevel(uint32_t log_devel) { bpf_debug_log_level_ = log_devel; }
  bool BpfInvalid() {
    return bpf_obj_ && bpf_prog_ && dev_map_ && bpf_prog_fd_ != -1 &&
           cgroup_count_ > 0;
  }

 private:
  uint32_t bpf_debug_log_level_;
  struct bpf_object *bpf_obj_;
  struct bpf_program *bpf_prog_;
  struct bpf_map *dev_map_;
  int bpf_prog_fd_;
  std::mutex *bpf_mtx_;
  size_t cgroup_count_;
};
#endif

class CgroupV2 : public CgroupInterface {
 public:
  CgroupV2(const std::string &path, struct cgroup *handle, uint64_t id);
  ~CgroupV2() override;
  bool SetCpuCoreLimit(double core_num) override;
  bool SetCpuShares(uint64_t share) override;
  bool SetMemoryLimitBytes(uint64_t memory_bytes) override;
  bool SetMemorySwLimitBytes(uint64_t mem_bytes) override;
  bool SetMemorySoftLimitBytes(uint64_t memory_bytes) override;
  bool SetBlockioWeight(uint64_t weight) override;

  // use BPF
  /**
  * Device controller manages access to device files.
  It includes both creation of new device files (using mknod),
  and access to the existing device files.

  Cgroup v2 device controller has no interface files and
  is implemented on top of cgroup BPF. To control access
  to device files, a user may create bpf programs of the
  BPF_CGROUP_DEVICE type and attach them to cgroups.
  On an attempt to access a device file, corresponding BPF
  programs will be executed, and depending on the return
  value the attempt will succeed or fail with -EPERM.

  A BPF_CGROUP_DEVICE program takes a pointer to the
  bpf_cgroup_dev_ctx structure, which describes the device
  access attempt: access type (mknod/read/write) and device
  (type, major and minor numbers).
  If the program returns 0, the attempt fails with -EPERM, otherwise it
  succeeds.

  An example of BPF_CGROUP_DEVICE program may be found
  in the kernel source tree in the tools/testing/selftests/bpf/dev_cgroup.c
  file. reference from:
  https://www.kernel.org/doc/html/v5.10/admin-guide/cgroup-v2.html#device-controller
  */
  bool SetDeviceAccess(const std::unordered_set<SlotId> &devices, bool set_read,
                       bool set_write, bool set_mknod) override;
#ifdef CRANE_ENABLE_BPF
  bool EraseBpfDeviceMap();
  static void SetBpfDebugLogLevel(uint32_t l) {
    bpf_runtime_info_.SetLogLevel(l);
  }
#endif
  bool KillAllProcesses() override;

  bool Empty() override;

  bool MigrateProcIn(pid_t pid) override;

  const std::string &GetCgroupString() const override {
    return m_cgroup_info_.m_cgroup_path_;
  }

 private:
#ifdef CRANE_ENABLE_BPF
  std::vector<BpfDeviceMeta> m_cgroup_bpf_devices{};
  static BpfRuntimeInfo bpf_runtime_info_;
#endif
  Cgroup m_cgroup_info_;
};

class AllocatableResourceAllocator {
 public:
  static bool Allocate(const AllocatableResource &resource,
                       CgroupInterface *cg);
  static bool Allocate(const crane::grpc::AllocatableResource &resource,
                       CgroupInterface *cg);
};

class DedicatedResourceAllocator {
 public:
  static bool Allocate(
      const crane::grpc::DedicatedResourceInNode &request_resource,
      CgroupInterface *cg);
  static bool Allocate(
      const google::protobuf::RepeatedField<
          crane::grpc::CreateCgroupRequest::GresFileMeta> &gres_file_metas,
      CgroupInterface *cg);
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

  void CreateCgroup(const crane::grpc::CreateCgroupRequest *req);

  bool CheckIfCgroupForTasksExists(task_id_t task_id);

  bool AllocateAndGetCgroup(task_id_t task_id, CgroupInterface **cg);

  bool MigrateProcToCgroupOfTask(pid_t pid, task_id_t task_id);

  bool ReleaseCgroup();

  CraneExpected<crane::grpc::ResourceInNode> GetTaskResourceInNode(
      task_id_t task_id);

  static EnvMap GetResourceEnvMapByResInNode(
      const crane::grpc::ResourceInNode &res_in_node);

  CraneExpected<EnvMap> GetResourceEnvMapOfTask(task_id_t task_id);

  void SetCgroupVersion(CgroupConstant::CgroupVersion v) { cg_version_ = v; }

  CgroupConstant::CgroupVersion GetCgroupVersion() { return cg_version_; }

 private:
  static std::string CgroupStrByTaskId_(task_id_t task_id);

  std::unique_ptr<CgroupInterface> CreateOrOpen_(
      const std::string &cgroup_string, ControllerFlags preferred_controllers,
      ControllerFlags required_controllers, bool retrieve);

  int InitializeController_(struct cgroup &cgroup,
                            CgroupConstant::Controller controller,
                            bool required, bool has_cgroup,
                            bool &changed_cgroup);

  static void RmAllTaskCgroups_();
  static void RmAllTaskCgroupsUnderController_(
      CgroupConstant::Controller controller);

  void RmAllTaskCgroupsV2_();
  void RmCgroupsV2_(const std::string &root_cgroup_path,
                    const std::string &match_str);

#ifdef CRANE_ENABLE_BPF
  void RmBpfDevMap();
#endif

  ControllerFlags m_mounted_controllers_;

  CgroupConstant::CgroupVersion cg_version_;

  std::atomic<std::optional<crane::grpc::CreateCgroupRequest>> m_cg_spec_;

  std::unique_ptr<CgroupInterface> m_cg_;
  absl::Mutex m_cg_mutex_;
};

}  // namespace Supervisor

inline std::unique_ptr<Supervisor::CgroupManager> g_cg_mgr;
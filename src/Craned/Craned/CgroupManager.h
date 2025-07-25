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
#include "CranedPublicDefs.h"
// Precompiled header comes first.

#include <libcgroup.h>

#ifdef CRANE_ENABLE_BPF
#  include <bpf/libbpf.h>
#endif

namespace Craned {

class JobInD;

namespace CgConstant {

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

inline constexpr bool kCgLimitDeviceRead = true;
inline constexpr bool kCgLimitDeviceWrite = true;
inline constexpr bool kCgLimitDeviceMknod = true;

inline constexpr std::string kTaskCgPathPrefix = "Crane_Task_";
inline const std::filesystem::path kRootCgroupFullPath = "/sys/fs/cgroup";
#ifdef CRANE_ENABLE_BPF
inline const char *kBpfObjectFilePath = "/usr/local/lib64/bpf/cgroup_dev_bpf.o";
inline const char *kBpfDeviceMapFilePath = "/sys/fs/bpf/craned_dev_map";
inline const char *kBpfMapName = "craned_dev_map";
inline const char *kBpfProgramName = "craned_device_access";
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

}  // namespace CgConstant

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
  constexpr ControllerFlags() noexcept : m_flags_(0u) {}

  constexpr explicit ControllerFlags(CgConstant::Controller controller) noexcept
      : m_flags_(1u << static_cast<uint64_t>(controller)) {}

  ControllerFlags(const ControllerFlags &val) noexcept = default;

  constexpr ControllerFlags operator|=(const ControllerFlags &rhs) noexcept {
    m_flags_ |= rhs.m_flags_;
    return *this;
  }

  constexpr ControllerFlags operator&=(const ControllerFlags &rhs) noexcept {
    m_flags_ &= rhs.m_flags_;
    return *this;
  }

  operator bool() const noexcept { return static_cast<bool>(m_flags_); }

  constexpr ControllerFlags operator~() const noexcept {
    ControllerFlags cf;
    cf.m_flags_ = ~m_flags_;
    return cf;
  }

 private:
  friend constexpr ControllerFlags operator|(
      const ControllerFlags &lhs, const ControllerFlags &rhs) noexcept;
  friend constexpr ControllerFlags operator&(
      const ControllerFlags &lhs, const ControllerFlags &rhs) noexcept;
  friend constexpr ControllerFlags operator|(
      const ControllerFlags &lhs, const CgConstant::Controller &rhs) noexcept;
  friend constexpr ControllerFlags operator&(
      const ControllerFlags &lhs, const CgConstant::Controller &rhs) noexcept;
  friend constexpr ControllerFlags operator|(
      const CgConstant::Controller &lhs,
      const CgConstant::Controller &rhs) noexcept;
  uint64_t m_flags_;
};

constexpr ControllerFlags operator|(const ControllerFlags &lhs,
                                    const ControllerFlags &rhs) noexcept {
  ControllerFlags flags;
  flags.m_flags_ = lhs.m_flags_ | rhs.m_flags_;
  return flags;
}

constexpr ControllerFlags operator&(const ControllerFlags &lhs,
                                    const ControllerFlags &rhs) noexcept {
  ControllerFlags flags;
  flags.m_flags_ = lhs.m_flags_ & rhs.m_flags_;
  return flags;
}

constexpr ControllerFlags operator|(
    const ControllerFlags &lhs, const CgConstant::Controller &rhs) noexcept {
  ControllerFlags flags;
  flags.m_flags_ = lhs.m_flags_ | (1u << static_cast<uint64_t>(rhs));
  return flags;
}

constexpr ControllerFlags operator&(
    const ControllerFlags &lhs, const CgConstant::Controller &rhs) noexcept {
  ControllerFlags flags;
  flags.m_flags_ = lhs.m_flags_ & (1u << static_cast<uint64_t>(rhs));
  return flags;
}

constexpr ControllerFlags operator|(
    const CgConstant::Controller &lhs,
    const CgConstant::Controller &rhs) noexcept {
  ControllerFlags flags;
  flags.m_flags_ =
      (1u << static_cast<uint64_t>(lhs)) | (1u << static_cast<uint64_t>(rhs));
  return flags;
}

constexpr ControllerFlags NO_CONTROLLER_FLAG{};

// In many distributions, 'cpu' and 'cpuacct' are mounted together. 'cpu'
// and 'cpuacct' both point to a single 'cpu,cpuacct' account. libcgroup
// handles this for us and no additional care needs to be taken.
constexpr ControllerFlags ALL_CONTROLLER_FLAG = (~NO_CONTROLLER_FLAG);

constexpr ControllerFlags CG_V1_REQUIRED_CONTROLLERS =
    NO_CONTROLLER_FLAG | CgConstant::Controller::CPU_CONTROLLER |
    CgConstant::Controller::MEMORY_CONTROLLER |
    CgConstant::Controller::DEVICES_CONTROLLER |
    CgConstant::Controller::BLOCK_CONTROLLER;

constexpr ControllerFlags CG_V2_REQUIRED_CONTROLLERS =
    NO_CONTROLLER_FLAG | CgConstant::Controller::CPU_CONTROLLER_V2 |
    CgConstant::Controller::MEMORY_CONTORLLER_V2 |
    CgConstant::Controller::IO_CONTROLLER_V2;

#ifdef CRANE_ENABLE_BPF
class BpfRuntimeInfo {
 public:
  BpfRuntimeInfo();
  ~BpfRuntimeInfo();
  bool InitializeBpfObj();
  void CloseBpfObj();
  void Destroy();
  static void RmBpfDeviceMap();

  struct bpf_object *BpfObj() { return bpf_obj_; }
  struct bpf_program *BpfProgram() { return bpf_prog_; }
  absl::Mutex *BpfMutex() { return bpf_mtx_.get(); }
  struct bpf_map *BpfDevMap() { return dev_map_; }
  int BpfProgFd() { return bpf_prog_fd_; }
  void SetLogEnabled(bool enabled) { bpf_enable_logging_ = enabled; }
  bool Valid() const {
    return bpf_obj_ && bpf_prog_ && dev_map_ && bpf_prog_fd_ != -1 &&
           cgroup_count_ > 0;
  }

 private:
  bool bpf_enable_logging_;
  struct bpf_object *bpf_obj_;
  struct bpf_program *bpf_prog_;
  struct bpf_map *dev_map_;
  int bpf_prog_fd_;
  std::unique_ptr<absl::Mutex> bpf_mtx_;
  size_t cgroup_count_;
};
#endif

class Cgroup {
 public:
  Cgroup(const std::string &path, struct cgroup *handle, uint64_t id = 0)
      : m_cgroup_path_(path), m_cgroup_(handle), m_cgroup_id_(id) {}
  ~Cgroup() = default;

  struct cgroup *NativeHandle() { return m_cgroup_; }

  // Using the zombie object pattern as exceptions are not available.
  bool Valid() const { return m_cgroup_ != nullptr; }

  bool SetControllerValue(CgConstant::Controller controller,
                          CgConstant::ControllerFile controller_file,
                          uint64_t value);
  bool SetControllerStr(CgConstant::Controller controller,
                        CgConstant::ControllerFile controller_file,
                        const std::string &str);
  bool SetControllerStrs(CgConstant::Controller controller,
                         CgConstant::ControllerFile controller_file,
                         const std::vector<std::string> &strs);

  void Destroy();

  // CgConstant::CgroupVersion cg_version; // maybe for hybrid mode
  bool ModifyCgroup_(CgConstant::ControllerFile controller_file);

  const std::filesystem::path &GetCgroupPath() const { return m_cgroup_path_; }

  uint64_t GetCgroupId() const { return m_cgroup_id_; }

  struct cgroup *RawCgHandle() const { return m_cgroup_; }

 private:
  std::filesystem::path m_cgroup_path_;
  mutable struct cgroup *m_cgroup_;
  uint64_t m_cgroup_id_;
};

class CgroupInterface {
 public:
  CgroupInterface(const std::string &path, struct cgroup *handle,
                  uint64_t id = 0)
      : m_cgroup_info_(path, handle, id) {};
  virtual ~CgroupInterface() = default;
  virtual bool SetCpuCoreLimit(double core_num) = 0;
  virtual bool SetCpuShares(uint64_t share) = 0;
  virtual bool SetMemoryLimitBytes(uint64_t memory_bytes) = 0;
  virtual bool SetMemorySwLimitBytes(uint64_t mem_bytes) = 0;
  virtual bool SetMemorySoftLimitBytes(uint64_t memory_bytes) = 0;
  virtual bool SetBlockioWeight(uint64_t weight) = 0;
  virtual bool SetDeviceAccess(const std::unordered_set<SlotId> &devices,
                               bool set_read, bool set_write,
                               bool set_mknod) = 0;

  virtual bool KillAllProcesses() = 0;

  virtual bool Empty() = 0;

  virtual void Destroy();

  bool MigrateProcIn(pid_t pid);

  std::string CgroupPathStr() const {
    return m_cgroup_info_.GetCgroupPath().string();
  }

 protected:
  Cgroup m_cgroup_info_;
};

class CgroupV1 : public CgroupInterface {
 public:
  CgroupV1(const std::string &path, struct cgroup *handle)
      : CgroupInterface(path, handle) {}
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

  void Destroy() override;
};

class CgroupV2 : public CgroupInterface {
 public:
  CgroupV2(const std::string &path, struct cgroup *handle, uint64_t id);

#ifdef CRANE_ENABLE_BPF
  CgroupV2(const std::string &path, struct cgroup *handle, uint64_t id,
           std::vector<BpfDeviceMeta> &cgroup_bpf_devices);
#endif

  ~CgroupV2() override {}
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
  bool RecoverFromCgSpec(const JobInD &job);
  bool EraseBpfDeviceMap();
#endif
  bool KillAllProcesses() override;

  bool Empty() override;

  void Destroy() override;

 private:
#ifdef CRANE_ENABLE_BPF
  bool m_bpf_attached_{false};
  std::vector<BpfDeviceMeta> m_cgroup_bpf_devices{};
#endif
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
};

class CgroupManager {
 public:
  CgroupManager() = default;
  ~CgroupManager() = default;

  CgroupManager(const CgroupManager &) = delete;
  CgroupManager(CgroupManager &&) = delete;
  CgroupManager &operator=(const CgroupManager &) = delete;
  CgroupManager &operator=(CgroupManager &&) = delete;

  CraneErrCode Init();

  /**
   * @brief Destroy cgroups which CraneCtld doesn't have records of
   * corresponding running jobs and set `recovered` field for jobs with their
   * cgroup created.
   */
  CraneErrCode TryToRecoverCgForJobs(
      std::unordered_map<job_id_t, JobInD> &rn_jobs_from_ctld);

  bool Mounted(CgConstant::Controller controller) const {
    return bool(m_mounted_controllers_ & ControllerFlags{controller});
  }

  void ControllersMounted();

  /**
   * \brief Allocate and return cgroup handle for job, should only be called
   * once per job.
   * \param job cgroup spec for job.
   * \return CgroupInterface ptr,null if error.
   */
  std::unique_ptr<CgroupInterface> AllocateAndGetJobCgroup(const JobInD &job,
                                                           bool recovery_mode);

  static EnvMap GetResourceEnvMapByResInNode(
      const crane::grpc::ResourceInNode &res_in_node);

  CraneExpected<task_id_t> GetJobIdFromPid(pid_t pid) const;

  void SetCgroupVersion(CgConstant::CgroupVersion v) { m_cg_version_ = v; }
  [[nodiscard]] CgConstant::CgroupVersion GetCgroupVersion() const {
    return m_cg_version_;
  }

#ifdef CRANE_ENABLE_BPF
  static BpfRuntimeInfo bpf_runtime_info;
#endif
 private:
  inline static std::string CgroupStrByTaskId_(task_id_t task_id);
  inline static std::optional<task_id_t> GetJobIdFromCg_(
      const std::string &path);

  std::unique_ptr<CgroupInterface> CreateOrOpen_(
      task_id_t job_id, ControllerFlags preferred_controllers,
      ControllerFlags required_controllers, bool retrieve);

  int InitializeController_(struct cgroup &cgroup,
                            CgConstant::Controller controller, bool required,
                            bool has_cgroup, bool &changed_cgroup);

  static std::set<task_id_t> GetJobIdsFromCgroupV1_(
      CgConstant::Controller controller);

  static std::set<task_id_t> GetJobIdsFromCgroupV2_(
      const std::string &root_cgroup_path);

  static std::unordered_map<ino_t, task_id_t> GetCgJobIdMapCgroupV2_(
      const std::string &root_cgroup_path);

#ifdef CRANE_ENABLE_BPF
  CraneExpected<std::unordered_map<task_id_t, std::vector<BpfKey>>>
  GetJobBpfMapCgroupsV2_(const std::string &root_cgroup_path);
#endif

  ControllerFlags m_mounted_controllers_;

  CgConstant::CgroupVersion m_cg_version_{};
};

}  // namespace Craned

inline std::unique_ptr<Craned::CgroupManager> g_cg_mgr;
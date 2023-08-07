
/*
 * Utility functions for dealing with libcgroup.
 *
 * This is not meant to replace direct interaction with libcgroup, however
 * it provides some simple initialization and RAII wrappers.
 *
 */
#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>
#include <libcgroup.h>
#include <pthread.h>

#include <array>
#include <boost/move/move.hpp>
#include <cassert>
#include <map>
#include <optional>
#include <string_view>

#include "crane/Lock.h"
#include "crane/Logger.h"
#include "crane/PublicHeader.h"

namespace util {

class CgroupManager;  // Forward Declaration

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

constexpr std::array<uint64_t,
                     static_cast<size_t>(DedicatedResource::DeviceType::DeviceCount)>
    DeviceMajor{
        195,
    };

constexpr std::array<char,
                     static_cast<size_t>(DedicatedResource::DeviceType::DeviceCount)>
    DeviceOpType{
        'c',
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

constexpr uint64_t GetDeviceMajor(DedicatedResource::DeviceType Device_type){
  return Internal::DeviceMajor[static_cast<uint64_t>(Device_type)];
}

constexpr char GetDeviceOpType(DedicatedResource::DeviceType Device_type){
  return Internal::DeviceOpType[static_cast<uint64_t>(Device_type)];
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
//  handles this for us and no additional care needs to be take.
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
  bool SetDeviceLimit(DedicatedResource::DeviceType device_type,const uint64_t alloc_bitmap,bool allow,bool read,bool write,bool mknod);
  bool SetDeviceDeny(DedicatedResource::DeviceType device_type,const uint64_t alloc_bitmap);
  bool SetControllerValue(CgroupConstant::Controller controller,
                          CgroupConstant::ControllerFile controller_file,
                          uint64_t value);
  bool SetControllerStr(CgroupConstant::Controller controller,
                        CgroupConstant::ControllerFile controller_file,
                        const std::string &str);
  bool KillAllProcesses();

  bool Empty();

  bool MigrateProcIn(pid_t pid);

 private:
  std::string m_cgroup_path_;
  mutable struct cgroup *m_cgroup_;

  friend class CgroupManager;
};

class CgroupUtil {
 public:
  static int Init();

  static bool Mounted(CgroupConstant::Controller controller) {
    return bool(m_mounted_controllers_ &ControllerFlags{controller});
  }

  static std::shared_ptr<Cgroup> CreateOrOpen(
      const std::string &cgroup_string, ControllerFlags preferred_controllers,
      ControllerFlags required_controllers, bool retrieve);

  static DedicatedResource::DeviceType getDeviceType(std::string device_name);
 private:
  static int initialize_controller(struct cgroup &cgroup,
                                   CgroupConstant::Controller controller,
                                   bool required, bool has_cgroup,
                                   bool &changed_cgroup);

  inline static ControllerFlags m_mounted_controllers_;
};

}  // namespace util
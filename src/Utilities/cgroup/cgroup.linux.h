
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
#include <spdlog/spdlog.h>

#include <array>
#include <boost/move/move.hpp>
#include <cassert>
#include <map>
#include <optional>
#include <string_view>

#include "crane/Lock.h"
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
//  handles this for us and no additional care needs to be take.
const ControllerFlags ALL_CONTROLLER_FLAG = (~NO_CONTROLLER_FLAG);

class Cgroup {
 public:
  Cgroup(const std::string &path, struct cgroup *handle)
      : m_cgroup_path_(path), m_cgroup_(handle) {}
  ~Cgroup();

  struct cgroup *NativeHandle() {
    return m_cgroup_;
  }

  const std::string &GetCgroupString() const { return m_cgroup_path_; };

  // Using the zombie object pattern as exceptions are not available.
  bool Valid() const { return m_cgroup_ != nullptr; }

  bool SetCpuCoreLimit(uint64_t core_num);
  bool SetCpuShares(uint64_t share);
  bool SetMemoryLimitBytes(uint64_t memory_bytes);
  bool SetMemorySwLimitBytes(uint64_t mem_bytes);
  bool SetMemorySoftLimitBytes(uint64_t memory_bytes);
  bool SetBlockioWeight(uint64_t weight);
  bool SetControllerValue(CgroupConstant::Controller controller,
                          CgroupConstant::ControllerFile controller_file,
                          uint64_t value);
  bool SetControllerStr(CgroupConstant::Controller controller,
                        CgroupConstant::ControllerFile controller_file,
                        const std::string &str);

  bool KillAllProcesses();

  bool Empty();

 private:
  std::string m_cgroup_path_;
  mutable struct cgroup *m_cgroup_;

  friend class CgroupManager;
};

class CgroupManager {
 public:
  static CgroupManager &Instance();

  bool Mounted(CgroupConstant::Controller controller) const {
    return bool(m_mounted_controllers_ & ControllerFlags{controller});
  }

  Cgroup *CreateOrOpen(const std::string &cgroup_string,
                       ControllerFlags preferred_controllers,
                       ControllerFlags required_controllers, bool retrieve)
      LOCKS_EXCLUDED(m_mtx_);

  /*
   * Decrease the cgroup reference count by 1.
   * If the reference reaches 0, the cgroup will be removed in OS.
   * Returns true on success, false on failure;
   */
  bool Release(const std::string &cgroup_path) LOCKS_EXCLUDED(m_mtx_);

  Cgroup *Find(const std::string &cgroup_path) LOCKS_EXCLUDED(m_mtx_);

  bool MigrateProcTo(pid_t pid, const std::string &cgroup_path)
      LOCKS_EXCLUDED(m_mtx_);

 private:
  using Mutex = absl::Mutex;
  using LockGuard = util::lock_guard;

  CgroupManager();
  CgroupManager(const CgroupManager &);
  CgroupManager &operator=(const CgroupManager &);

  int initialize();

  int initialize_controller(struct cgroup &cgroup,
                            CgroupConstant::Controller controller,
                            bool required, bool has_cgroup,
                            bool &changed_cgroup) const;

  ControllerFlags m_mounted_controllers_;

  absl::flat_hash_map<std::string, std::pair<std::unique_ptr<Cgroup>, size_t>>
      m_cgroup_ref_count_map_ GUARDED_BY(m_mtx_);

  Mutex m_mtx_;
};

}  // namespace util
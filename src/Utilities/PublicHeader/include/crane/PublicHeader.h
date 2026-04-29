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

#pragma once

#include <google/protobuf/util/time_util.h>

#include <array>
#include <expected>
#include <format>
#include <fpm/fixed.hpp>
#include <unordered_map>

#include "protos/Crane.pb.h"

#if !defined(CRANE_VERSION_STRING)
#  define CRANE_VERSION_STRING "Unknown"
#endif

using job_id_t = uint32_t;
using task_id_t = uint32_t;  // Task ID within a step (Supervisor layer)
using step_id_t = uint32_t;

using PartitionId = std::string;
using CranedId = std::string;
inline const CranedId kCtldPrologInternalNodeIndex{
    "__CRANE_INTERNAL_PROLOG_CTLD_NODE_INDEX"};
using ResvId = std::string;
using cpu_t = fpm::fixed<int64_t, __int128, 8>;
using LicenseId = std::string;

// QoS "unlimited" CPU sentinel.
// Must fit in double mantissa (53 bits) to survive int64 → double → int64
// round-trip, and be divisible by 256 (FractionBits) to avoid fractional
// representation. (1LL << 53) / 256 = 35184372088832 CPUs.
inline const cpu_t kUnlimitedCpu = cpu_t::from_raw_value(int64_t{1} << 53);

using CraneErrCode = crane::grpc::ErrCode;

using CraneRichError = crane::grpc::RichError;

template <typename T>
using CraneExpected = std::expected<T, CraneErrCode>;

template <typename T>
using CraneExpectedRich = std::expected<T, CraneRichError>;

constexpr const char* kLogPattern =
    "[%^%L%$ %C-%m-%d %H:%M:%S.%e %s:%#][%n] %v";

constexpr int kMaxHealthCheckWaitTimeMs = 60000;

inline const char* const kDefaultHost = "0.0.0.0";

// CRI-managed steps (daemon pod / container step) only carry one task.
constexpr task_id_t kCriStepTaskId = 0;
constexpr step_id_t kDaemonStepId = 0;
constexpr step_id_t kPrimaryStepId = 1;

inline const char* const kCtldDefaultPort = "10011";
inline const char* const kCranedDefaultPort = "10010";
inline const char* const kCforedDefaultPort = "10012";
inline const char* const kCtldForInternalDefaultPort = "10013";

inline const char* const kDefaultConfigPath = "/etc/crane/config.yaml";
inline const char* const kDefaultDbConfigPath = "/etc/crane/database.yaml";
inline const char* const kDefaultPluginConfigPath = "/etc/crane/plugin.yaml";

inline const char* const kUnlimitedQosName = "UNLIMITED";

inline const char* const kHostFilePath = "/etc/hosts";

inline constexpr size_t kDefaultQueryJobNumLimit = 1000;
inline constexpr uint32_t kDefaultQosPriority = 1000;
inline constexpr uint64_t kPriorityDefaultMaxAge = 7UL * 24 * 3600;  // 7 days
inline constexpr double kMemoryToleranceGB = 0.01;

inline constexpr uint64_t kDefaultCraneCtldMaxLogFileSize =
    1024 * 1024 * 50;  // 50 MB
inline constexpr uint64_t kDefaultCraneCtldMaxLogFileNum = 3;
inline constexpr uint64_t kDefaultCranedMaxLogFileSize =
    1024 * 1024 * 50;  // 50 MB
inline constexpr uint64_t kDefaultCranedMaxLogFileNum = 3;
inline constexpr uint64_t kDefaultSupervisorMaxLogFileSize =
    1024 * 1024 * 50;  // 50 MB
inline constexpr uint64_t kDefaultSupervisorMaxLogFileNum = 3;

inline constexpr uint64_t kDefaultCertExpirationMinutes = 30;

inline constexpr size_t kMaxOutputQueueBytes = 10 * 1024 * 1024ULL;

inline const char* const kDefaultCraneBaseDir = "/var/crane/";
inline const char* const kDefaultCraneCtldMutexFile =
    "cranectld/cranectld.lock";
inline const char* const kDefaultCraneCtldLogPath = "cranectld/cranectld.log";
inline const char* const kDefaultCraneCtldDbPath = "cranectld/embedded.db";

inline const char* const kDefaultCraneCtldAlivePath =
    "cranectld/cranectld.alive";

inline const char* const kDefaultCranedScriptDir = "craned/scripts";
inline const char* const kDefaultCranedUnixSockPath = "craned/craned.sock";
inline const char* const kDefaultCranedForPamUnixSockPath =
    "craned/craned_pam.sock";
inline const char* const kDefaultCranedMutexFile = "craned/craned.lock";
inline const char* const kDefaultCranedLogPath = "craned/craned.log";

inline const char* const kDefaultContainerTempDir = "craned/container";
inline const char* const kDefaultContainerClusterDomain = "cluster.local";

inline const char* const kDefaultSupervisorPath = "/usr/libexec/csupervisor";
inline const char* const kDefaultSupervisorUnixSockDir = "/tmp/crane";

inline const char* const kDefaultPlugindUnixSockPath = "cplugind/cplugind.sock";

inline const char* const kResourceTypeGpu = "gpu";

constexpr uint64_t kJobMinTimeLimitSec = 11;
constexpr int64_t kJobMaxTimeLimitSec =
    google::protobuf::util::TimeUtil::kDurationMaxSeconds;
constexpr int64_t kJobMaxTimeStampSec =
    google::protobuf::util::TimeUtil::kTimestampMaxSeconds;

constexpr uint64_t kCranedPingIntervalSec = 10;
constexpr uint64_t kCranedTimeoutSec = 30;

constexpr uint64_t kEraseResvIntervalSec = 5;

constexpr const char* const kCrunFwdALL = "all";
constexpr const char* const kCrunFwdNONE = "none";

constexpr uint32_t kMaxReconnectAttempts = 1000;
constexpr uint32_t kMaxReconnectIntervalSec = 60;

enum PrologFlagEnum : std::uint8_t {
  Contain = 1 << 0,             // 0000 0001 = 1
  ForceRequeueOnFail = 1 << 1,  // 0000 0010 = 2
  RunInJob = 1 << 2,            // 0000 0100 = 4
  Serial = 1 << 3,              // 0000 1000 = 8
};

constexpr uint64_t kDefaultPrologOutputSize = 1024 * 1024;

constexpr uint64_t kMaxJobMemoryBytes = 10737418240000;  // 10000GB

namespace ExitCode {

constexpr size_t KCrunExitCodeStatusNum = 128;
inline constexpr size_t kExitStatusNum = 256;
inline constexpr size_t kTerminationSignalBase = kExitStatusNum;
inline constexpr size_t kTerminationSignalNum = 64;
inline constexpr size_t kSystemExitCodeNum =
    kExitStatusNum + kTerminationSignalNum;
inline constexpr size_t kCraneExitCodeBase = kSystemExitCodeNum;

enum ExitCodeEnum : uint16_t {
  // exit() code range begin
  EC_EXITSTATUS_BEGIN = 0,
  // exit() code range end
  EC_EXITSTATUS_END = 255,
  // termination by signal range begin
  EC_TERMINATION_SIGNAL_BEGIN = 256,
  // termination by signal range end
  EC_TERMINATION_SIGNAL_END = 319,
  // Crane defined exit code range begin
  EC_TERMINATED = 320,
  EC_PERMISSION_DENIED,
  EC_CGROUP_ERR,
  EC_FILE_NOT_FOUND,
  EC_SPAWN_FAILED,
  EC_EXCEED_TIME_LIMIT,
  EC_CRANED_DOWN,
  EC_EXEC_ERR,
  EC_RPC_ERR,
  EC_PROLOG_ERR,
  EC_REACHED_DEADLINE,
  // NOLINTNEXTLINE(bugprone-reserved-identifier,readability-identifier-naming)
  __MAX_EXIT_CODE
};

}  // namespace ExitCode

namespace Internal {
// clang-format off
constexpr std::array<std::string_view, crane::grpc::ErrCode_ARRAYSIZE>
    kCraneErrStrArr = {
        // 0 - 4
        "Success",

        "Invalid UID",
        "You are not a Crane user",
        "The specified user is invalid",
        "The user permission is insufficient",

        // 5 - 9
        "The user has been blocked",
        "The user already exists in this account",
        "The user doesn't own sufficient permission for the account operation",
        "Invalid admin level",
        "The user does not belong to this account",

        // 10 - 14
        "No account is specified for the user",
        "The specified account does not exist",
        "The account already exists in the database",
        "The parent account of the specified account does not exist",
        "The account has child accounts or users and thus can't be deleted",

        // 15 - 19
        "The account has been blocked",
        "The specified partition does not exist",
        "The specified account or user does not include this partition",
        "The partition already exists in the account or user",
        "The parent account does not include the partition",

        // 20 - 24
        "The QoS can't be added if the user has no partition",
        "Child has partition error",
        "The user has no QoS available for this partition",
        "The specified QoS is not in the partition's allowed QoS list",
        "The specified QoS does not exist",

        // 25 - 29
        "The QoS already exists in Crane",
        "The QoS is still being used by accounts or users and can't be deleted",
        "Failed to convert value to integer",
        "Invalid time limit value",
        "The entered account or user does not include this QoS",

        // 30 - 34
        "The QoS already exists in the account or user",
        "The parent account does not include the QoS",
        "The new QoS list does not include the current default QoS",
        "The new default QoS is not in the allowed QoS list",
        "The QoS is already the default QoS for the account",

        // 35 - 39
        "Some user is using the QoS as his default QoS",
        "The QoS is being used by some child accounts",
        "The QoS is not in the allowed list or is already the default QoS",
        "Is default QoS error",
        "Failed to update the database",

        // 40 - 44
        "Generic failure",
        "Not enough resources for the job",
        "Non-existent error",
        "Not enough nodes in the partition for the job",
        "Invalid node list",

        // 45 - 49
        "Invalid exclude node list",
        "Time limit reached the user's limit",
        "CPUs per task reached the user's limit",
        "Not enough nodes for the job",
        "System error",

        // 50 - 54
        "Existing job",
        "The number of pending jobs exceeded the maximum value",
        "Invalid parameter",
        "Stop error",
        "Permission denied",

        // 55 - 59
        "Connection timeout",
        "Connection aborted",
        "RPC failure",
        "Token request failure",
        "Stream broken",

        // 60 - 64
        "Invalid stub",
        "CGroup error",
        "Protobuf error",
        "LibEvent error",
        "No available node",

        // 65 - 69
        "The current running job exceeds the QoS limit (MaxJobPerUser)",
        "User has insufficient privilege",
        "The account does not have permission to run jobs in this partition. Please contact the administrator to add it to the allowed list",
        "The account has been denied access to this partition. Please contact the security administrator if access is required",
        "EBPF syscall error",

        // 70 - 74
        "Supervisor error",
        "Service shutting down",
        "The user failed to issue the certificate, please contact the administrator for assistance",
        "The certificate has already been issued to the user. If the certificate is lost and needs to be reissued, please contact the administrator for assistance",
        "Revocation of the certificate failed, Please check the logs",

        // 75 - 79
        "User information does not match, unable to submit the job.",
        "You need to set --force for this operation.",
        "Invalid username",
        "Legal licenses",
        "Invalid job id",

        // 80 - 84
        "CRI runtime returns error. For other errors in Crane, use ERR_GENERIC_FAILURE.",
        "CRI support is disabled in the cluster.",
        "Job is pending or container is not ready.",
        "Requested CRI operation is not supported in multi-node steps.",
        "Invalid memory format",

        // 85 - 89
        "Step resource request exceeds job resource",
        "The specified wckey does not exist",
        "The wckey already exists in crane",
        "The entered cluster does not exist",
        "Cannot delete the default wckey. Please set a different default wckey first",

        // 90-94
        "No default wckey is set. Please specify a wckey or set a default wckey",
        "ERR_MISSING_DEPENDENCY",
        "ERR_DB_INSERT_FAILED",
        "Lua script validation failed",
        "ERR_RESOURCE_NOT_FOUND",

        // 95-99
        "ERR_INVALID_ARGUMENT",
        "ERR_RESOURCE_ALREADY_EXIST",
        "The current submitted job exceeds the QoS limit (MaxSubmitJobsPerAccount)",
        "Cannot delete user with active jobs.",
        "The current submitted job exceeds the QoS limit (MaxJobsPerQos)",

        "Not a valide resource string",
        "The current submitted job exceeds the QoS limit (MAX_TRES_PER_USER_BEYOND)",
        "The current submitted job exceeds the QoS limit (MAX_TRES_PER_ACCOUNT_BEYOND)",
        "The current submitted job exceeds the QoS limit (ERR_TRES_PER_JOB_BEYOND)"
    };
// clang-format on
}  // namespace Internal

template <typename... Args>
inline CraneRichError FormatRichErr(CraneErrCode code, const std::string& fmt,
                                    Args&&... args) {
  CraneRichError rich_err;

  rich_err.set_code(code);
  rich_err.set_description(
      std::vformat(fmt, std::make_format_args(std::forward<Args>(args)...)));

  return rich_err;
}

inline std::string_view CraneErrStr(CraneErrCode err) {
  return Internal::kCraneErrStrArr[static_cast<uint16_t>(err)];
}

template <typename EnumType>
class FlagSet {
 private:
  static_assert(std::is_enum_v<EnumType>,
                "FlagSet can only be used with enum types");
  static constexpr size_t kBitSize = std::to_underlying(EnumType::_Count);
  std::bitset<kBitSize> m_bits_;

 public:
  constexpr FlagSet() = default;

  decltype(auto) operator[](EnumType flag) {
    return m_bits_[std::to_underlying(flag)];
  }

  bool operator[](EnumType flag) const {
    return m_bits_[std::to_underlying(flag)];
  }

  void FromInt64(int64_t value) {
    static_assert(kBitSize <= 64,
                  "FlagSet: enum _Count > 64, cannot convert to int64");
    m_bits_ = value;
  }

  int64_t ToInt64() const {
    static_assert(kBitSize <= 64,
                  "FlagSet: enum _Count > 64, cannot convert to int64");
    return static_cast<int64_t>(m_bits_.to_ullong());
  }

  std::string ToString() const { return m_bits_.to_string(); }
};

/* ----------- Public definitions for all components */

// TODO: refactor SlotId, it should not be a string of file path.
// Device path. e.g. /dev/nvidia0
using SlotId = std::string;

struct TypeSlotsMap {
  std::unordered_map<std::string /*type*/, std::set<SlotId> /*index*/>
      type_slots_map;

  TypeSlotsMap() = default;

  TypeSlotsMap(const TypeSlotsMap& rhs) = default;
  TypeSlotsMap& operator=(const TypeSlotsMap& rhs) = default;

  TypeSlotsMap(TypeSlotsMap&& rhs) = default;
  TypeSlotsMap& operator=(TypeSlotsMap&& rhs) = default;

  explicit TypeSlotsMap(const crane::grpc::DeviceTypeSlotsMap& rhs);
  TypeSlotsMap& operator=(const crane::grpc::DeviceTypeSlotsMap& rhs);

  explicit operator crane::grpc::DeviceTypeSlotsMap() const;

  bool IsZero() const;
  bool Contains(const std::string& type) const;

  std::set<SlotId>& operator[](const std::string& type);
  const std::set<SlotId>& At(const std::string& type) const;

  TypeSlotsMap& operator+=(const TypeSlotsMap& rhs);
  TypeSlotsMap& operator-=(const TypeSlotsMap& rhs);
};

bool operator==(const TypeSlotsMap& lhs, const TypeSlotsMap& rhs);
bool operator<=(const TypeSlotsMap& lhs, const TypeSlotsMap& rhs);

TypeSlotsMap Intersection(const TypeSlotsMap& lhs, const TypeSlotsMap& rhs);

struct DedicatedResourceInNode {
  DedicatedResourceInNode() = default;

  DedicatedResourceInNode(const DedicatedResourceInNode& rhs) = default;
  DedicatedResourceInNode& operator=(const DedicatedResourceInNode& rhs) =
      default;

  DedicatedResourceInNode(DedicatedResourceInNode&& rhs) = default;
  DedicatedResourceInNode& operator=(DedicatedResourceInNode&& rhs) = default;

  explicit DedicatedResourceInNode(
      const crane::grpc::DedicatedResourceInNode& rhs);
  DedicatedResourceInNode& operator=(
      const crane::grpc::DedicatedResourceInNode& rhs);

  // Access operators
  TypeSlotsMap& operator[](const std::string& device_name);
  TypeSlotsMap& At(const std::string& device_name);
  const TypeSlotsMap& At(const std::string& device_name) const;

  // Arithmetic operators
  DedicatedResourceInNode& operator+=(const DedicatedResourceInNode& rhs);
  DedicatedResourceInNode& operator-=(const DedicatedResourceInNode& rhs);

  bool Contains(const std::string& device_name) const;

  bool IsZero() const;
  void SetToZero();

  explicit operator crane::grpc::DedicatedResourceInNode() const;

 public:
  // config: gpu:a100 whit file /dev/nvidia[0-3]
  // parsed: name:gpu,slot:a100,index:/dev/nvidia0,....,/dev/nvidia3
  std::unordered_map<std::string /*name*/, TypeSlotsMap> name_type_slots_map;
};

bool operator<=(const DedicatedResourceInNode& lhs,
                const DedicatedResourceInNode& rhs);
bool operator==(const DedicatedResourceInNode& lhs,
                const DedicatedResourceInNode& rhs);

// GresCount: tracks total count and per-type specified counts for a GRES
// Example: {total: 8, specified: {"A100": 4, "H100": 4}}
// - total: the total count of this GRES (includes all types)
// - specified: per-type counts for specific scheduling requirements
struct GresCount {
  uint64_t total{0};
  std::unordered_map<std::string /*type*/, uint64_t /*count*/> specified;

  GresCount() = default;
  explicit GresCount(uint64_t t) : total(t) {}
  GresCount(uint64_t t, std::unordered_map<std::string, uint64_t> spec)
      : total(t), specified(std::move(spec)) {}

  // gRPC conversion
  explicit GresCount(const crane::grpc::GresCount& rhs);
  explicit operator crane::grpc::GresCount() const;

  bool IsZero() const { return total == 0 && specified.empty(); }

  GresCount& operator+=(const GresCount& rhs);
  GresCount& operator-=(const GresCount& rhs);
  GresCount& operator*=(uint32_t rhs);
};

bool operator==(const GresCount& lhs, const GresCount& rhs);
bool operator<=(const GresCount& lhs, const GresCount& rhs);

// Division: returns how many copies of rhs can fit in lhs
// For specified types: use specified / specified
// For total: use total / total
// Returns the minimum across all dimensions
uint64_t operator/(const GresCount& lhs, const GresCount& rhs);

GresCount GresCountMax(const GresCount& lhs, const GresCount& rhs);
GresCount GresCountMin(const GresCount& lhs, const GresCount& rhs);

// GresMap: maps GRES name to its count info
using GresMap = std::unordered_map<std::string /*name*/, GresCount>;

// GresMap gRPC conversion helper functions
crane::grpc::GresMap ToGrpcGresMap(const GresMap& gres_map);
GresMap FromGrpcGresMap(const crane::grpc::GresMap& grpc_gres_map);

DedicatedResourceInNode Intersection(const DedicatedResourceInNode& lhs,
                                     const DedicatedResourceInNode& rhs);

class ResourceView;

// CpuSet: Tracks CPU resources for execution phase
// - core_ids: specific CPU core IDs, valid only for integer allocation
// - cpu_count: total CPU amount (supports fractional, e.g. 0.5)
// When IsInteger(): core_ids contains the bound cores, cpu_count ==
// core_ids.size() When fractional: core_ids is empty, cpu_count holds the
// fractional amount
struct CpuSet {
  std::set<uint32_t> core_ids;
  cpu_t cpu_count{0};

  CpuSet() = default;
  // Integer allocation: bind to specific cores
  explicit CpuSet(std::set<uint32_t> ids);
  // Fractional allocation: quota only
  explicit CpuSet(cpu_t count);

  CpuSet& operator+=(const CpuSet& rhs);
  CpuSet& operator-=(const CpuSet& rhs);

  bool IsZero() const;
  void SetToZero();

  // True if this is an integer-core allocation (core_ids non-empty)
  bool IsInteger() const;
};

// ResourceInNodeV3: Execution phase resource tracking for a single node
// Tracks actual hardware resources with specific slot IDs for affinity
class ResourceInNodeV3 {
 public:
  ResourceInNodeV3() = default;

  ResourceInNodeV3(const ResourceInNodeV3& rhs) = default;
  ResourceInNodeV3& operator=(const ResourceInNodeV3& rhs) = default;
  ResourceInNodeV3(ResourceInNodeV3&& rhs) = default;
  ResourceInNodeV3& operator=(ResourceInNodeV3&& rhs) = default;

  // gRPC conversion
  explicit ResourceInNodeV3(const crane::grpc::ResourceInNodeV3& rhs);
  explicit operator crane::grpc::ResourceInNodeV3() const;

  // Only addition and subtraction for maintaining real resource state
  ResourceInNodeV3& operator+=(const ResourceInNodeV3& rhs);
  ResourceInNodeV3& operator-=(const ResourceInNodeV3& rhs);

  bool IsZero() const;
  bool IsExhausted() const;
  void SetToZero();

  // CPU (core IDs + fractional amount)
  const CpuSet& GetCpuSet() const;
  CpuSet& GetCpuSet();

  // Memory limits
  uint64_t GetMemoryBytes() const;
  void SetMemoryBytes(uint64_t bytes);
  uint64_t GetMemorySwBytes() const;
  void SetMemorySwBytes(uint64_t bytes);

  // GRES with specific slot IDs (reuses DedicatedResourceInNode structure)
  const DedicatedResourceInNode& GetGres() const;
  DedicatedResourceInNode& GetGres();

  // Deprecated: will be removed after TimeResMap migrates to ResourceView.
  // Use ResourceView::Min() for new code.
  [[deprecated("Use ResourceView::Min() after TimeResMap migration")]]
  void Ckmin(const ResourceInNodeV3& rhs);

  // Convert to ResourceView (aggregates to counts only)
  ResourceView ToResourceView() const;

 private:
  CpuSet m_cpu_set_;
  uint64_t m_memory_bytes_{0};
  uint64_t m_memory_sw_bytes_{0};
  DedicatedResourceInNode m_gres_;

  friend ResourceInNodeV3 operator+(const ResourceInNodeV3& lhs,
                                    const ResourceInNodeV3& rhs);
  friend ResourceInNodeV3 operator-(const ResourceInNodeV3& lhs,
                                    const ResourceInNodeV3& rhs);
};

ResourceInNodeV3 operator+(const ResourceInNodeV3& lhs,
                           const ResourceInNodeV3& rhs);
ResourceInNodeV3 operator-(const ResourceInNodeV3& lhs,
                           const ResourceInNodeV3& rhs);
bool operator<=(const ResourceInNodeV3& lhs, const ResourceInNodeV3& rhs);

// ResourceV3: Cluster-level execution phase resource tracking
// Maps node IDs to their specific resource allocations
class ResourceV3 {
 public:
  ResourceV3() = default;

  // gRPC conversion
  explicit ResourceV3(const crane::grpc::ResourceV3& rhs);
  explicit operator crane::grpc::ResourceV3() const;

  ResourceV3& operator+=(const ResourceV3& rhs);
  ResourceV3& operator-=(const ResourceV3& rhs);

  ResourceV3& AddResourceInNode(const std::string& craned_id,
                                const ResourceInNodeV3& rhs);
  ResourceV3& SubtractResourceInNode(const std::string& craned_id,
                                     const ResourceInNodeV3& rhs);

  ResourceInNodeV3& At(const std::string& craned_id);
  const ResourceInNodeV3& At(const std::string& craned_id) const;

  bool IsZero() const;
  void SetToZero();

  // Convert to ResourceView (aggregates all nodes to counts only)
  ResourceView View() const noexcept;

  std::unordered_map<std::string, ResourceInNodeV3>& EachNodeResMap() {
    return m_each_node_res_map_;
  }
  const std::unordered_map<std::string, ResourceInNodeV3>& EachNodeResMap()
      const {
    return m_each_node_res_map_;
  }

 private:
  std::unordered_map<std::string /*craned id*/, ResourceInNodeV3>
      m_each_node_res_map_;

  friend ResourceV3 operator+(const ResourceV3& lhs, const ResourceV3& rhs);
  friend ResourceV3 operator-(const ResourceV3& lhs, const ResourceV3& rhs);
};

ResourceV3 operator+(const ResourceV3& lhs, const ResourceV3& rhs);
ResourceV3 operator-(const ResourceV3& lhs, const ResourceV3& rhs);

// ResourceView: Flat structure for scheduling phase resource tracking
// Only tracks quantities, not specific slot IDs
class ResourceView {
 public:
  ResourceView() = default;

  // Grpc conversion
  explicit ResourceView(const crane::grpc::ResourceView& rhs);
  ResourceView& operator=(const crane::grpc::ResourceView& rhs);
  explicit operator crane::grpc::ResourceView() const;

  // Cluster level resource operations
  ResourceView& operator+=(const ResourceV3& rhs);
  ResourceView& operator-=(const ResourceV3& rhs);

  // Node level resource operations
  ResourceView& operator+=(const ResourceInNodeV3& rhs);
  ResourceView& operator-=(const ResourceInNodeV3& rhs);

  ResourceView& operator+=(const DedicatedResourceInNode& rhs);
  ResourceView& operator-=(const DedicatedResourceInNode& rhs);

  // Account level resource operations
  ResourceView& operator+=(const ResourceView& rhs);
  ResourceView& operator-=(const ResourceView& rhs);
  ResourceView& operator*=(uint32_t rhs);

  bool IsZero() const;
  void SetToZero();

  bool GetFeasibleResourceInNode(const ResourceInNodeV3& avail_res,
                                 ResourceInNodeV3* feasible_res) const;

  // Accessors
  cpu_t GetCpuCount() const;
  void SetCpuCount(cpu_t count);

  uint64_t GetMemoryBytes() const;
  void SetMemoryBytes(uint64_t bytes);

  uint64_t GetMemorySwBytes() const;
  void SetMemorySwBytes(uint64_t bytes);

  const GresMap& GetGresMap() const;
  GresMap& GetGresMap();

  // Convenience methods
  double CpuCountDouble() const;
  uint64_t GpuCount() const;

  // Element-wise max/min operations
  static ResourceView Max(const ResourceView& lhs, const ResourceView& rhs);
  static ResourceView Min(const ResourceView& lhs, const ResourceView& rhs);

 private:
  cpu_t m_cpu_count_{0};
  uint64_t m_memory_bytes_{0};
  uint64_t m_memory_sw_bytes_{0};
  GresMap m_gres_map_;

  friend ResourceView operator*(const ResourceView& lhs, uint32_t rhs);
  friend ResourceView operator+(const ResourceView& lhs,
                                const ResourceView& rhs);
  friend ResourceView operator-(const ResourceView& lhs,
                                const ResourceView& rhs);
  friend bool operator<=(const ResourceView& lhs, const ResourceInNodeV3& rhs);
  friend bool operator<=(const ResourceView& lhs, const ResourceView& rhs);
  friend uint64_t operator/(const ResourceView& lhs, const ResourceView& rhs);
};

ResourceView operator*(const ResourceView& lhs, uint32_t rhs);
ResourceView operator+(const ResourceView& lhs, const ResourceView& rhs);
ResourceView operator-(const ResourceView& lhs, const ResourceView& rhs);

bool operator<=(const ResourceView& lhs, const ResourceInNodeV3& rhs);
bool operator<=(const ResourceView& lhs, const ResourceView& rhs);

// Returns the minimum quotient across all resource dimensions
// (i.e., how many tasks with resource `rhs` can fit in `lhs`)
uint64_t operator/(const ResourceView& lhs, const ResourceView& rhs);

template <class... Ts>
struct VariantVisitor : Ts... {
  using Ts::operator()...;
};

template <class... Ts>
VariantVisitor(Ts...) -> VariantVisitor<Ts...>;

[[nodiscard]] constexpr bool IsFinishedStepStatus(
    crane::grpc::JobStatus status) noexcept {
  switch (status) {
  case crane::grpc::JobStatus::Deadline:
  case crane::grpc::JobStatus::ExceedTimeLimit:
  case crane::grpc::JobStatus::OutOfMemory:
  case crane::grpc::JobStatus::Cancelled:
  case crane::grpc::JobStatus::Failed:
  case crane::grpc::JobStatus::Completed:
    return true;
  default:
    return false;
  }
}

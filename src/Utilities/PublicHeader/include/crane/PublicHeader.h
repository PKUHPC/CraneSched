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

using task_id_t = uint32_t;
using step_id_t = uint32_t;

using CraneErrCode = crane::grpc::ErrCode;

using CraneRichError = crane::grpc::RichError;

template <typename T>
using CraneExpected = std::expected<T, CraneErrCode>;

template <typename T>
using CraneExpectedRich = std::expected<T, CraneRichError>;

inline const char* const kDefaultHost = "0.0.0.0";

inline const char* const kCtldDefaultPort = "10011";
inline const char* const kCranedDefaultPort = "10010";
inline const char* const kCforedDefaultPort = "10012";

inline const char* const kDefaultConfigPath = "/etc/crane/config.yaml";
inline const char* const kDefaultDbConfigPath = "/etc/crane/database.yaml";

inline const char* const kUnlimitedQosName = "UNLIMITED";
inline const char* const kHostFilePath = "/etc/hosts";

inline constexpr size_t kDefaultQueryTaskNumLimit = 1000;
inline constexpr uint32_t kDefaultQosPriority = 1000;
inline constexpr uint64_t kPriorityDefaultMaxAge = 7UL * 24 * 3600;  // 7 days

inline const char* const kDefaultCraneBaseDir = "/var/crane/";
inline const char* const kDefaultCraneCtldMutexFile =
    "cranectld/cranectld.lock";
inline const char* const kDefaultCraneCtldLogPath = "cranectld/cranectld.log";
inline const char* const kDefaultCraneCtldDbPath = "cranectld/embedded.db";

inline const char* const kDefaultCranedScriptDir = "craned/scripts";
inline const char* const kDefaultCranedUnixSockPath = "craned/craned.sock";
inline const char* const kDefaultCranedMutexFile = "craned/craned.lock";
inline const char* const kDefaultCranedLogPath = "craned/craned.log";


inline const char* const kDefaultContainerTempDir = "craned/container";

inline const char* const kDefaultSupervisorPath = "/usr/libexec/csupervisor";
inline const char* const kDefaultSupervisorUnixSockDir = "/tmp/crane";

inline const char* const kDefaultPlugindUnixSockPath = "cplugind/cplugind.sock";

constexpr uint64_t kTaskMinTimeLimitSec = 11;
constexpr int64_t kTaskMaxTimeLimitSec =
    google::protobuf::util::TimeUtil::kDurationMaxSeconds;
constexpr int64_t kTaskMaxTimeStampSec =
    google::protobuf::util::TimeUtil::kTimestampMaxSeconds;

constexpr uint64_t kEraseResvIntervalSec = 5;

namespace ExitCode {

inline constexpr size_t kExitStatusNum = 256;
inline constexpr size_t kTerminationSignalBase = kExitStatusNum;
inline constexpr size_t kTerminationSignalNum = 64;
inline constexpr size_t kSystemExitCodeNum =
    kExitStatusNum + kTerminationSignalNum;
inline constexpr size_t kCraneExitCodeBase = kSystemExitCodeNum;

enum ExitCodeEnum : uint16_t {
  kExitCodeTerminated = kCraneExitCodeBase,
  kExitCodePermissionDenied,
  kExitCodeCgroupError,
  kExitCodeFileNotFound,
  kExitCodeSpawnProcessFail,
  kExitCodeExceedTimeLimit,
  kExitCodeCranedDown,
  kExitCodeExecutionError,
  // NOLINTNEXTLINE(bugprone-reserved-identifier,readability-identifier-naming)
  __MAX_EXIT_CODE
};

}  // namespace ExitCode

namespace Internal {
// clang-format off
constexpr std::array<std::string_view, crane::grpc::ErrCode_ARRAYSIZE>
    CraneErrStrArr = {
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
        "Not enough resources for the task",
        "Non-existent error",
        "Not enough nodes in the partition for the task",
        "Invalid node list",

        // 45 - 49
        "Invalid exclude node list",
        "Time limit reached the user's limit",
        "CPUs per task reached the user's limit",
        "Not enough nodes for the task",
        "System error",

        // 50 - 54
        "Existing task",
        "The number of pending tasks exceeded the maximum value",
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

        // 65 - 67
        "The current running job exceeds the QoS limit (MaxJobPerUser)",
        "User has insufficient privilege"
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
  return Internal::CraneErrStrArr[static_cast<uint16_t>(err)];
}

/* ----------- Public definitions for all components */

using PartitionId = std::string;
using CranedId = std::string;
using ResvId = std::string;
using cpu_t = fpm::fixed_24_8;

// TODO: refactor SlotId, it should not be a string of file path.
// Device path. e.g. /dev/nvidia0
using SlotId = std::string;

// Model the allocatable resources on a craned node.
// It contains CPU and memory by now.
struct AllocatableResource {
  cpu_t cpu_count{0};

  // See documentation of cgroup memory.
  // https://www.kernel.org/doc/Documentation/cgroup-v1/memory.txt
  uint64_t memory_bytes = 0;     // limit of memory usage
  uint64_t memory_sw_bytes = 0;  // limit of memory+Swap usage

  AllocatableResource() = default;
  explicit AllocatableResource(const crane::grpc::AllocatableResource&);
  AllocatableResource& operator=(const crane::grpc::AllocatableResource&);

  AllocatableResource& operator+=(const AllocatableResource& rhs);
  AllocatableResource& operator-=(const AllocatableResource& rhs);
  AllocatableResource& operator*=(uint32_t rhs);

  explicit operator crane::grpc::AllocatableResource() const;

  double CpuCount() const;

  bool IsZero() const;
  bool IsAnyZero() const;
  void SetToZero();
};

bool operator<=(const AllocatableResource& lhs, const AllocatableResource& rhs);
bool operator<(const AllocatableResource& lhs, const AllocatableResource& rhs);
bool operator==(const AllocatableResource& lhs, const AllocatableResource& rhs);

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
  bool contains(const std::string& type) const;

  std::set<SlotId>& operator[](const std::string& type);
  const std::set<SlotId>& at(const std::string& type) const;

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
  TypeSlotsMap& at(const std::string& device_name);
  const TypeSlotsMap& at(const std::string& device_name) const;

  // Arithmetic operators
  DedicatedResourceInNode& operator+=(const DedicatedResourceInNode& rhs);
  DedicatedResourceInNode& operator-=(const DedicatedResourceInNode& rhs);

  bool contains(const std::string& device_name) const;

  bool IsZero() const;
  void SetToZero();

  explicit operator crane::grpc::DeviceMap() const;
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

using DeviceMap =
    std::unordered_map<std::string /*name*/,
                       std::pair<uint64_t /*untyped req count*/,
                                 std::unordered_map<std::string /*type*/,
                                                    uint64_t /*type total*/>>>;

crane::grpc::DeviceMap ToGrpcDeviceMap(const DeviceMap& device_map);
DeviceMap FromGrpcDeviceMap(const crane::grpc::DeviceMap& grpc_device_map);

void operator+=(DeviceMap& lhs, const DedicatedResourceInNode& rhs);
void operator-=(DeviceMap& lhs, const DedicatedResourceInNode& rhs);
void operator*=(DeviceMap& lhs, uint32_t rhs);

bool operator<=(const DeviceMap& lhs, const DeviceMap& rhs);
bool operator<=(const DeviceMap& lhs, const DedicatedResourceInNode& rhs);

DedicatedResourceInNode Intersection(const DedicatedResourceInNode& lhs,
                                     const DedicatedResourceInNode& rhs);

struct ResourceInNode {
  AllocatableResource allocatable_res;
  DedicatedResourceInNode dedicated_res;

  ResourceInNode() = default;
  explicit ResourceInNode(const crane::grpc::ResourceInNode& rhs);

  explicit operator crane::grpc::ResourceInNode() const;

  ResourceInNode& operator+=(const ResourceInNode& rhs);
  ResourceInNode& operator-=(const ResourceInNode& rhs);

  bool IsZero() const;
  void SetToZero();
};

bool operator<=(const ResourceInNode& lhs, const ResourceInNode& rhs);
bool operator==(const ResourceInNode& lhs, const ResourceInNode& rhs);

class ResourceView;

class ResourceV2 {
 public:
  ResourceV2() = default;

  // Grpc conversion
  explicit ResourceV2(const crane::grpc::ResourceV2& rhs);
  explicit operator crane::grpc::ResourceV2() const;
  ResourceV2& operator=(const crane::grpc::ResourceV2& rhs);

  // ResourceInNode& operator[](const std::string& craned_id);
  ResourceInNode& at(const std::string& craned_id);
  const ResourceInNode& at(const std::string& craned_id) const;

  ResourceV2& operator+=(const ResourceV2& rhs);
  ResourceV2& operator-=(const ResourceV2& rhs);
  ResourceV2& AddResourceInNode(const std::string& craned_id,
                                const ResourceInNode& rhs);
  ResourceV2& SubtractResourceInNode(const std::string& craned_id,
                                     const ResourceInNode& rhs);

  bool IsZero() const;
  void SetToZero();

  std::unordered_map<std::string, ResourceInNode>& EachNodeResMap() {
    return each_node_res_map;
  }
  const std::unordered_map<std::string, ResourceInNode>& EachNodeResMap()
      const {
    return each_node_res_map;
  }

 private:
  std::unordered_map<std::string /*craned id*/, ResourceInNode>
      each_node_res_map;

 public:
  friend bool operator<=(const ResourceV2& lhs, const ResourceV2& rhs);
  friend bool operator==(const ResourceV2& lhs, const ResourceV2& rhs);

  friend class ResourceView;
};

bool operator<=(const ResourceV2& lhs, const ResourceV2& rhs);
bool operator==(const ResourceV2& lhs, const ResourceV2& rhs);

class ResourceView {
 public:
  ResourceView() = default;

  // Grpc conversion
  explicit ResourceView(const crane::grpc::ResourceView& rhs);
  ResourceView& operator=(const crane::grpc::ResourceView& rhs);
  explicit operator crane::grpc::ResourceView() const;

  // Cluster level resource operations
  ResourceView& operator+=(const ResourceV2& rhs);
  ResourceView& operator-=(const ResourceV2& rhs);

  // Node level resource operations
  ResourceView& operator+=(const ResourceInNode& rhs);
  ResourceView& operator-=(const ResourceInNode& rhs);

  ResourceView& operator+=(const AllocatableResource& rhs);
  ResourceView& operator-=(const AllocatableResource& rhs);

  ResourceView& operator+=(const DedicatedResourceInNode& rhs);
  ResourceView& operator-=(const DedicatedResourceInNode& rhs);

  bool IsZero() const;
  void SetToZero();

  bool GetFeasibleResourceInNode(const ResourceInNode& avail_res,
                                 ResourceInNode* feasible_res);

  double CpuCount() const;
  uint64_t MemoryBytes() const;

  AllocatableResource& GetAllocatableRes() { return allocatable_res; }
  const AllocatableResource& GetAllocatableRes() const {
    return allocatable_res;
  }

  const DeviceMap& GetDeviceMap() const { return device_map; }

 private:
  AllocatableResource allocatable_res;
  DeviceMap device_map;

  friend ResourceView operator*(const ResourceView& lhs, uint32_t rhs);
  friend bool operator<=(const ResourceView& lhs, const ResourceInNode& rhs);
  friend bool operator<=(const ResourceView& lhs, const ResourceView& rhs);
};

ResourceView operator*(const ResourceView& lhs, uint32_t rhs);

bool operator<=(const ResourceView& lhs, const ResourceInNode& rhs);
bool operator<=(const ResourceView& lhs, const ResourceView& rhs);

struct CgroupSpec {
  CgroupSpec() = default;
  CgroupSpec(const CgroupSpec& spce) = default;
  explicit CgroupSpec(const crane::grpc::JobSpec& job_spec);
  CgroupSpec(const task_id_t job_id, const uid_t uid,
             const ResourceInNode& res_in_node,
             const std::string& execution_node);

  /**
   * @brief set grpc struct,will move res_in_node field
   * @param job_spec grpc job_spce to set
   */
  void SetJobSpec(crane::grpc::JobSpec* job_spec);
  task_id_t job_id;
  uid_t uid;
  crane::grpc::ResourceInNode res_in_node;
  std::string execution_node;
  // Recovered on start,no need to apply res limit.
  bool recovered;
};
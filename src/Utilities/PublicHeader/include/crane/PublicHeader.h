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
#include <fpm/fixed.hpp>
#include <unordered_map>

#include "protos/Crane.pb.h"

#if !defined(CRANE_VERSION_STRING)
#  define CRANE_VERSION_STRING "Unknown"
#endif

using task_id_t = uint32_t;

enum class CraneErr : uint16_t {
  kOk = 0,
  kGenericFailure,
  kNoResource,
  kNonExistent,
  kInvalidNodeNum,

  kSystemErr,  // represent the error which sets errno
  kExistingTask,
  kInvalidParam,
  kStop,
  kPermissionDenied,

  kConnectionTimeout,
  kConnectionAborted,
  kRpcFailure,
  kTokenRequestFailure,
  KStreamBroken,

  kInvalidStub,
  kCgroupError,
  kProtobufError,
  kLibEventError,
  kNoAvailNode,

  __ERR_SIZE  // NOLINT(bugprone-reserved-identifier)
};

using CraneErrCode = crane::grpc::ErrCode;

template <typename T>
using CraneExpected = std::expected<T, CraneErr>;

template <typename T>
using CraneErrCodeExpected = std::expected<T, CraneErrCode>;

inline const char* kCtldDefaultPort = "10011";
inline const char* kCranedDefaultPort = "10010";
inline const char* kCforedDefaultPort = "10012";

inline const char* kDefaultConfigPath = "/etc/crane/config.yaml";
inline const char* kDefaultDbConfigPath = "/etc/crane/database.yaml";

inline const char* kUnlimitedQosName = "UNLIMITED";
inline const char* kHostFilePath = "/etc/hosts";

inline constexpr size_t kDefaultQueryTaskNumLimit = 1000;
inline constexpr uint32_t kDefaultQosPriority = 1000;
inline constexpr uint64_t kPriorityDefaultMaxAge = 7 * 24 * 3600;  // 7 days

inline const char* kDefaultCraneBaseDir = "/var/crane/";
inline const char* kDefaultCraneCtldMutexFile = "cranectld/cranectld.lock";
inline const char* kDefaultCraneCtldLogPath = "cranectld/cranectld.log";
inline const char* kDefaultCraneCtldDbPath = "cranectld/embedded.db";

inline const char* kDefaultCranedScriptDir = "craned/scripts";
inline const char* kDefaultCranedUnixSockPath = "craned/craned.sock";
inline const char* kDefaultCranedMutexFile = "craned/craned.lock";
inline const char* kDefaultCranedLogPath = "craned/craned.log";

inline const char* kDefaultPlugindUnixSockPath = "cplugind/cplugind.sock";

constexpr uint64_t kTaskMinTimeLimitSec = 11;
constexpr int64_t kTaskMaxTimeLimitSec =
    google::protobuf::util::TimeUtil::kDurationMaxSeconds;

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

  __MAX_EXIT_CODE  // NOLINT(bugprone-reserved-identifier)
};

}  // namespace ExitCode

namespace Internal {

constexpr std::array<std::string_view, uint16_t(CraneErr::__ERR_SIZE)>
    CraneErrStrArr = {
        "Success",
        "Generic failure",
        "Resource not enough",
        "The object doesn't exist",
        "Invalid --node-num is passed",

        "Linux Error",
        "Task already exists",
        "Invalid Parameter",
        "The owner object of the function is stopping",
        "Permission denied",

        "Connection timeout",
        "Connection aborted",
        "RPC call failed",
        "Failed to request required token",
        "Stream is broken",

        "Craned stub is invalid",
        "Error when manipulating cgroup",
        "Error when using protobuf",
        "Error when using LibEvent",
        "Not enough nodes which satisfy resource requirements",
};

}

inline std::string_view CraneErrStr(CraneErr err) {
  return Internal::CraneErrStrArr[uint16_t(err)];
}

/* ----------- Public definitions for all components */

using PartitionId = std::string;
using CranedId = std::string;
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
  uid_t uid;
  task_id_t task_id;
  crane::grpc::ResourceInNode res_in_node;
  std::string execution_node;
};
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

#pragma once

#include <google/protobuf/util/time_util.h>

#include <array>
#include <fpm/fixed.hpp>
#include <optional>
#include <unordered_map>
#include <variant>

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

constexpr uint64_t kTaskMinTimeLimitSec = 11;
constexpr int64_t kTaskMaxTimeLimitSec =
    google::protobuf::util::TimeUtil::kDurationMaxSeconds;

// gRPC Doc: If smaller than 10 seconds, ten seconds will be used instead.
// See https://github.com/grpc/proposal/blob/master/A8-client-side-keepalive.md
constexpr int64_t kCraneCtldGrpcClientPingSendIntervalSec = 10;

// Server MUST have a high value of
// GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS than the value of
// GRPC_ARG_KEEPALIVE_TIME_MS. We set server's value to the multiple times of
// the client's value plus 1s to tolerate 1 time packet dropping. See
// https://github.com/grpc/grpc/blob/master/doc/keepalive.md
constexpr int64_t kCranedGrpcServerPingRecvMinIntervalSec =
    3 * kCraneCtldGrpcClientPingSendIntervalSec + 1;

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
// device path e.g.,/dev/nvidia0
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

  explicit operator crane::grpc::AllocatableResource() const;
};

bool operator<=(const AllocatableResource& lhs, const AllocatableResource& rhs);
bool operator<(const AllocatableResource& lhs, const AllocatableResource& rhs);
bool operator==(const AllocatableResource& lhs, const AllocatableResource& rhs);

struct TypeSlotsMap {
  std::unordered_map<std::string /*type*/, std::set<SlotId> /*index*/>
      type_slots_map;

  bool IsZero() const;
  bool contains(const std::string& type) const;

  std::set<SlotId>& operator[](const std::string& type);
  const std::set<SlotId>& at(const std::string& type) const;

  TypeSlotsMap& operator+=(const TypeSlotsMap& rhs);
  TypeSlotsMap& operator-=(const TypeSlotsMap& rhs);
};

struct DedicatedResourceInNode {
  // user req type
  using Req_t = std::unordered_map<
      std::string /*name*/,
      std::pair<
          uint64_t /*untyped req count*/,
          std::unordered_map<std::string /*type*/, uint64_t /*type total*/>>>;

  // Access operators
  TypeSlotsMap& operator[](const std::string& device_name);
  TypeSlotsMap& at(const std::string& device_name);
  const TypeSlotsMap& at(const std::string& device_name) const;

  // Arithmetic operators
  DedicatedResourceInNode& operator+=(const DedicatedResourceInNode& rhs);
  DedicatedResourceInNode& operator-=(const DedicatedResourceInNode& rhs);

  bool contains(const std::string& device_name) const;

  bool IsZero() const;
  bool empty(const std::string& device_name) const;
  bool empty(const std::string& device_name,
             const std::string& device_type) const;

  void flat_(std::set<std::string>& names, std::set<std::string>& types) const;

  explicit operator crane::grpc::DeviceMap() const;

 public:
  // config: gpu:a100 whit file /dev/nvidia[0-3]
  // parsed: name:gpu,slot:a100,index:/dev/nvidia0,....,/dev/nvidia3
  std::unordered_map<std::string /*name*/, TypeSlotsMap> name_type_slots_map;
};

bool operator<=(const DedicatedResourceInNode::Req_t& lhs,
                const DedicatedResourceInNode& rhs);
bool operator<=(const DedicatedResourceInNode& lhs,
                const DedicatedResourceInNode& rhs);
bool operator==(const DedicatedResourceInNode& lhs,
                const DedicatedResourceInNode& rhs);

/**
 * Model the dedicated resources in a craned node.
 * It contains GPU, NIC, etc.
 */
struct DedicatedResource {
  std::unordered_map<CranedId /*craned id*/, DedicatedResourceInNode>
      craned_id_dres_in_node_map;

  DedicatedResource() = default;
  explicit DedicatedResource(const crane::grpc::DedicatedResource& rhs);

  // Arithmetic operators
  DedicatedResource& operator+=(const DedicatedResource& rhs);
  DedicatedResource& operator-=(const DedicatedResource& rhs);
  DedicatedResource& AddDedicatedResourceInNode(const CranedId& craned_id,
                                                const DedicatedResource& rhs);
  DedicatedResource& SubtractDedicatedResourceInNode(
      const CranedId& craned_id, const DedicatedResource& rhs);

  // Access operators
  DedicatedResourceInNode& operator[](const std::string& craned_id);
  DedicatedResourceInNode& at(const std::string& craned_id);
  const DedicatedResourceInNode& at(const std::string& craned_id) const;

  bool contains(const CranedId& craned_id) const;
  bool IsZero() const;

  explicit operator crane::grpc::DedicatedResource() const;
};

bool operator<=(const DedicatedResource& lhs, const DedicatedResource& rhs);
bool operator==(const DedicatedResource& lhs, const DedicatedResource& rhs);

/**
 * When a task is allocated a resource UUID, it holds one instance of Resources
 * struct. Resource struct contains a AllocatableResource struct and a list of
 * DedicatedResource.
 */
struct Resources {
  AllocatableResource allocatable_resource;
  DedicatedResource dedicated_resource;

  Resources() = default;

  Resources& operator+=(const Resources& rhs);
  Resources& operator-=(const Resources& rhs);

  Resources& operator+=(const AllocatableResource& rhs);
  Resources& operator-=(const AllocatableResource& rhs);
  Resources operator+(const DedicatedResource& rhs) const;

  explicit operator crane::grpc::Resources() const;
};

bool operator<=(const Resources& lhs, const Resources& rhs);
bool operator==(const Resources& lhs, const Resources& rhs);

struct CgroupSpec {
  uid_t uid;
  task_id_t task_id;
  crane::grpc::Resources resources;
};
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

#include <fpm/fixed.hpp>

#include "protos/Crane.pb.h"

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

  __ERR_SIZE  // NOLINT(bugprone-reserved-identifier)
};

inline const char* kCtldDefaultPort = "10011";
inline const char* kCranedDefaultPort = "10010";
inline const char* kCforedDefaultPort = "10012";

inline const char* kDefaultConfigPath = "/etc/crane/config.yaml";
inline const char* kDefaultPredConfigPath = "/etc/crane/predictor.yaml";
inline const char* kDefaultDbConfigPath = "/etc/crane/database.yaml";

inline const char* kUnlimitedQosName = "UNLIMITED";
inline const char* kHostFilePath = "/etc/hosts";

inline constexpr size_t kDefaultQueryTaskNumLimit = 100;
inline constexpr uint64_t kPriorityDefaultMaxAge = 7 * 24 * 3600;  // 7 days

inline const char* kDefaultCraneBaseDir = "/var/crane/";
inline const char* kDefaultCraneCtldMutexFile = "cranectld/cranectld.lock";
inline const char* kDefaultCraneCtldLogPath = "cranectld/cranectld.log";
inline const char* kDefaultCraneCtldDbPath = "cranectld/embedded.db";

inline const char* kDefaultCranedScriptDir = "craned/scripts";
inline const char* kDefaultCranedUnixSockPath = "craned/craned.sock";
inline const char* kDefaultCranedMutexFile = "craned/craned.lock";
inline const char* kDefaultCranedLogPath = "craned/craned.log";

constexpr int64_t kMaxTimeLimitSecond =
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
};

}

inline std::string_view CraneErrStr(CraneErr err) {
  return Internal::CraneErrStrArr[uint16_t(err)];
}

/* ----------- Public definitions for all components */

using PartitionId = std::string;
using CranedId = std::string;
using cpu_t = fpm::fixed_24_8;

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
};

bool operator<=(const AllocatableResource& lhs, const AllocatableResource& rhs);
bool operator<(const AllocatableResource& lhs, const AllocatableResource& rhs);
bool operator==(const AllocatableResource& lhs, const AllocatableResource& rhs);

/**
 * Model the dedicated resources in a craned node.
 * It contains GPU, NIC, etc.
 */
struct DedicatedResource {};  // Todo: Crane GRES

/**
 * When a task is allocated a resource UUID, it holds one instance of Resources
 * struct. Resource struct contains a AllocatableResource struct and a list of
 * DedicatedResource.
 */
struct Resources {
  AllocatableResource allocatable_resource;

  Resources() = default;

  Resources& operator+=(const Resources& rhs);
  Resources& operator-=(const Resources& rhs);

  Resources& operator+=(const AllocatableResource& rhs);
  Resources& operator-=(const AllocatableResource& rhs);
};

bool operator<=(const Resources& lhs, const Resources& rhs);
bool operator<(const Resources& lhs, const Resources& rhs);
bool operator==(const Resources& lhs, const Resources& rhs);

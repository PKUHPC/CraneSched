#pragma once

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

inline const size_t kExitStatusNum = 256;
inline const size_t& kTerminationSignalStart = kExitStatusNum;
inline const size_t kTerminationSignalNum = 64;
inline const size_t kSystemExitCodeNum = kExitStatusNum + kTerminationSignalNum;
inline const size_t& kCraneExitCodeStart = kSystemExitCodeNum;

enum ExitCode : uint16_t {
  exTerminal = kCraneExitCodeStart,
  exPermissionDenied,
  exCgroupError,
  exFileNotFound,
  exSpawnProcessFail,

  __Exit_CODE_SIZE  // NOLINT(bugprone-reserved-identifier)
};

inline const size_t kDefaultQueryTaskNumLimit = 100;
inline const char* kCtldDefaultPort = "10011";
inline const char* kCranedDefaultPort = "10010";
inline const char* kDefaultConfigPath = "/etc/crane/config.yaml";

#define DEFAULT_CRANE_TEMP_DIR "/tmp/crane"

inline const char* kDefaultCraneTempDir = DEFAULT_CRANE_TEMP_DIR;
inline const char* kDefaultCranedScriptDir =
    DEFAULT_CRANE_TEMP_DIR "/craned/scripts";
inline const char* kDefaultCranedUnixSockPath =
    DEFAULT_CRANE_TEMP_DIR "/craned.sock";
inline const char* kDefaultCraneCtldMutexFile =
    DEFAULT_CRANE_TEMP_DIR "/cranectld.lock";
inline const char* kDefaultCranedMutexFile =
    DEFAULT_CRANE_TEMP_DIR "/craned.lock";

#undef DEFAULT_CRANE_TEMP_DIR

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

// (partition id, node index), by which a Craned is uniquely identified.
struct CranedId {
  uint32_t partition_id{0x3f3f3f3f};
  uint32_t craned_index{0x3f3f3f3f};

  struct Hash {
    std::size_t operator()(const CranedId& val) const {
      return std::hash<uint64_t>()(
          (static_cast<uint64_t>(val.partition_id) << 32) |
          static_cast<uint64_t>(val.craned_index));
    }
  };
};

inline bool operator==(const CranedId& lhs, const CranedId& rhs) {
  return (lhs.craned_index == rhs.craned_index) &&
         (lhs.partition_id == rhs.partition_id);
}


// Model the allocatable resources on a craned node.
// It contains CPU and memory by now.
struct AllocatableResource {
  double cpu_count = 0.0;

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

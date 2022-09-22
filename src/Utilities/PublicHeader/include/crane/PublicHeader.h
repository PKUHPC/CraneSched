#pragma once

#include <absl/time/time.h>  // NOLINT(modernize-deprecated-headers)
#include <spdlog/spdlog.h>

#include <boost/uuid/uuid.hpp>
#include <list>

#include "protos/Crane.pb.h"

// For better logging inside lambda functions
#if defined(__clang__) || defined(__GNUC__) || defined(__GNUG__)
#define __FUNCTION__ __PRETTY_FUNCTION__
#endif

#define CRANE_TRACE(...) SPDLOG_TRACE(__VA_ARGS__)
#define CRANE_DEBUG(...) SPDLOG_DEBUG(__VA_ARGS__)
#define CRANE_INFO(...) SPDLOG_INFO(__VA_ARGS__)
#define CRANE_WARN(...) SPDLOG_WARN(__VA_ARGS__)
#define CRANE_ERROR(...) SPDLOG_ERROR(__VA_ARGS__)
#define CRANE_CRITICAL(...) SPDLOG_CRITICAL(__VA_ARGS__)

#ifndef NDEBUG
#define CRANE_ASSERT_MSG_VA(condition, message, ...)                    \
  do {                                                                  \
    if (!(condition)) {                                                 \
      CRANE_CRITICAL("Assertion failed: \"" #condition "\": " #message, \
                     __VA_ARGS__);                                      \
      std::terminate();                                                 \
    }                                                                   \
  } while (false)

#define CRANE_ASSERT_MSG(condition, message)                             \
  do {                                                                   \
    if (!(condition)) {                                                  \
      CRANE_CRITICAL("Assertion failed: \"" #condition "\": " #message); \
      std::terminate();                                                  \
    }                                                                    \
  } while (false)

#define CRANE_ASSERT(condition)                               \
  do {                                                        \
    if (!(condition)) {                                       \
      CRANE_CRITICAL("Assertion failed: \"" #condition "\""); \
      std::terminate();                                       \
    }                                                         \
  } while (false)
#else
#define CRANE_ASSERT_MSG(condition, message) \
  do {                                       \
  } while (false)

#define CRANE_ASSERT(condition) \
  do {                          \
  } while (false)
#endif

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
inline const char* kDefaultConfigPath = "/etc/crane/config.yaml";

#define DEFAULT_CRANE_TEMP_DIR "/tmp/crane"

inline const char* kDefaultCraneTempDir = DEFAULT_CRANE_TEMP_DIR;
inline const char* kDefaultCranedScriptDir =
    DEFAULT_CRANE_TEMP_DIR "/craned/scripts";
inline const char* kDefaultCranedUnixSockPath =
    DEFAULT_CRANE_TEMP_DIR "/craned.sock";

#undef DEFAULT_CRANE_TEMP_DIR

namespace Internal {

constexpr std::array<std::string_view, uint16_t(CraneErr::__ERR_SIZE)>
    CraneErrStrArr = {
        "Success",
        "Generic failure",
        "Resource not enough",
        "The object doesn't exist",
        "Linux Error",
        "Task already exists",
        "Invalid Parameter",
        "The owner object of the function is stopping",
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

/**
 * Custom formatter for CranedId in fmt.
 */
template <>
struct fmt::formatter<CranedId> {
  constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const CranedId& id, FormatContext& ctx) -> decltype(ctx.out()) {
    // ctx.out() is an output iterator to write to.
    return format_to(ctx.out(), "({}, {})", id.partition_id, id.craned_index);
  }
};

// Model the allocatable resources on a craned node.
// It contains CPU and memory by now.
struct AllocatableResource {
  uint32_t cpu_count = 0;

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

namespace Internal {

struct StaticLogFormatSetter {
  StaticLogFormatSetter() { spdlog::set_pattern("[%^%L%$ %C-%m-%d %s:%#] %v"); }
};

// Set the global spdlog pattern in global variable initialization.
[[maybe_unused]] inline StaticLogFormatSetter _static_formatter_setter;

}  // namespace Internal
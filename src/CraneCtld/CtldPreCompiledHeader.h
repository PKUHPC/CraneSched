#pragma once

// Standard Libraries
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <source_location>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

// Absl
#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/container/node_hash_map.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>  // NOLINT(modernize-deprecated-headers)

// Boost
#include <boost/algorithm/string.hpp>
#include <boost/container_hash/hash.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#if Boost_MINOR_VERSION >= 71
#  include <boost/uuid/uuid_hash.hpp>
#endif

// Thread pool
#include <BS_thread_pool.hpp>

// gRPC
#include <grpc++/grpc++.h>

// Ranges-v3
#include <range/v3/all.hpp>

// Concurrent queue
#include <concurrentqueue/concurrentqueue.h>

// pevents
#include <pevents/pevents.h>

#include "crane/Logger.h"
// Logger.h must be the first

#include "crane/PasswordEntry.h"
#include "crane/PublicHeader.h"
#include "crane/String.h"
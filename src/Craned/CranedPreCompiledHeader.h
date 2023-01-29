#pragma once

// Standard Libraries
#include <any>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <forward_list>
#include <fstream>
#include <functional>
#include <future>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <regex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>

// Absl
#include <absl/base/thread_annotations.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <absl/synchronization/mutex.h>

// Boost
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

#if Boost_MINOR_VERSION >= 69
#  include <boost/uuid/uuid_hash.hpp>
#endif

// Thread pool
#include <BS_thread_pool.hpp>

// Concurrent queue
#include <concurrentqueue/concurrentqueue.h>

// gRPC
#include <grpc++/grpc++.h>

// Include the header which defines the static log level
#include "crane/Logger.h"

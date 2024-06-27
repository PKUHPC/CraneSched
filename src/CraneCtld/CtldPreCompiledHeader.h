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

// Standard Libraries
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <charconv>
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
#include <memory_resource>
#include <mutex>
#include <optional>
#include <queue>
#include <ranges>
#include <source_location>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

// backward-cpp
#include <backward.hpp>

// Absl
#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/container/node_hash_map.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <absl/strings/str_format.h>
#include <absl/synchronization/blocking_counter.h>
#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>  // NOLINT(modernize-deprecated-headers)

// Thread pool
#include <BS_thread_pool.hpp>

// gRPC
#include <google/protobuf/util/time_util.h>
#include <grpc++/alarm.h>
#include <grpc++/completion_queue.h>
#include <grpc++/grpc++.h>

// Ranges-v3
#include <range/v3/all.hpp>

// Concurrent queue
#include <concurrentqueue/concurrentqueue.h>

// UVW
#include <uvw.hpp>

// fpm
#include <fpm/fixed.hpp>

// result
#include <result/result.hpp>

#include "crane/Logger.h"
// Logger.h must be the first

#include "crane/PasswordEntry.h"
#include "crane/PublicHeader.h"
#include "crane/String.h"
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

// Standard Libraries
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <csignal>
#include <expected>
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
#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/container/node_hash_map.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <absl/synchronization/blocking_counter.h>
#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>  // NOLINT(modernize-deprecated-headers)

// parallel-hashmap
#include <parallel_hashmap/phmap.h>

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

#include "crane/GrpcHelper.h"
#include "crane/OS.h"
#include "crane/PasswordEntry.h"
#include "crane/PublicHeader.h"
#include "crane/String.h"
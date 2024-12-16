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
#include <any>
#include <atomic>
#include <cerrno>
#include <charconv>
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
#include <ranges>
#include <regex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>

// backward-cpp
#include <backward.hpp>

// Absl
#include <absl/base/thread_annotations.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/container/node_hash_map.h>
#include <absl/strings/match.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_replace.h>
#include <absl/strings/str_split.h>
#include <absl/synchronization/blocking_counter.h>
#include <absl/synchronization/mutex.h>

#include "absl/synchronization/notification.h"

// Thread pool
#include <BS_thread_pool.hpp>

// gRPC
#include <grpc++/grpc++.h>

// UVW
#include <uvw.hpp>

// fpm
#include <fpm/fixed.hpp>

// Concurrent queue
#include <concurrentqueue/concurrentqueue.h>

// Include the header which defines the static log level
#include "crane/Logger.h"

// Then include the other Crane headers
#include "crane/GrpcHelper.h"
#include "crane/Network.h"
#include "crane/PublicHeader.h"

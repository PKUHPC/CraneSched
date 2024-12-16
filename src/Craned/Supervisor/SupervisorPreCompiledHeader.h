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
#include <../../../cmake-build-debug/_deps/backward-src/backward.hpp>

// Absl
#include <../../../cmake-build-debug/_deps/absl-src/absl/base/thread_annotations.h>
#include <../../../cmake-build-debug/_deps/absl-src/absl/container/flat_hash_map.h>
#include <../../../cmake-build-debug/_deps/absl-src/absl/container/flat_hash_set.h>
#include <../../../cmake-build-debug/_deps/absl-src/absl/container/node_hash_map.h>
#include <../../../cmake-build-debug/_deps/absl-src/absl/strings/match.h>
#include <../../../cmake-build-debug/_deps/absl-src/absl/strings/str_join.h>
#include <../../../cmake-build-debug/_deps/absl-src/absl/strings/str_replace.h>
#include <../../../cmake-build-debug/_deps/absl-src/absl/strings/str_split.h>
#include <../../../cmake-build-debug/_deps/absl-src/absl/synchronization/blocking_counter.h>
#include <../../../cmake-build-debug/_deps/absl-src/absl/synchronization/mutex.h>

#include "../../../cmake-build-debug/_deps/absl-src/absl/synchronization/notification.h"

// Thread pool
#include <../../../cmake-build-debug/_deps/bs_thread_pool_src-src/include/BS_thread_pool.hpp>

// gRPC
#include <../../../cmake-build-debug/_deps/grpc-src/include/grpc++/grpc++.h>

// UVW
#include <../../../cmake-build-debug/_deps/uvw-src/src/uvw.hpp>

// fpm
#include <../../../cmake-build-debug/_deps/fpm-src/include/fpm/fixed.hpp>

// Concurrent queue
#include <../../../dependencies/pre_installed/concurrentqueue/include/concurrentqueue/concurrentqueue.h>

// Include the header which defines the static log level
#include "../../Utilities/PublicHeader/include/crane/Logger.h"

// Then include the other Crane headers
#include "../../Utilities/PublicHeader/include/crane/GrpcHelper.h"
#include "../../Utilities/PublicHeader/include/crane/Network.h"
#include "../../Utilities/PublicHeader/include/crane/PublicHeader.h"

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

#include <absl/synchronization/mutex.h>

#include <mutex>
#include <shared_mutex>

namespace util {

class ABSL_SCOPED_LOCKABLE AbslMutexLockGuard {
 private:
  absl::Mutex& m;

 public:
  explicit AbslMutexLockGuard(absl::Mutex& m_) : m(m_) { m.Lock(); }

  ~AbslMutexLockGuard() { m.Unlock(); }

  AbslMutexLockGuard(AbslMutexLockGuard const&) = delete;
  AbslMutexLockGuard& operator=(AbslMutexLockGuard const&) = delete;
};

using mutex = absl::Mutex;
using lock_guard = AbslMutexLockGuard;

using recursive_mutex = std::recursive_mutex;
using recursive_lock_guard = std::lock_guard<std::recursive_mutex>;

using rw_mutex = std::shared_mutex;
using read_lock_guard = std::shared_lock<std::shared_mutex>;
using write_lock_guard = std::unique_lock<std::shared_mutex>;

}  // namespace util
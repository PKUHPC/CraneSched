#pragma once

#include <absl/synchronization/mutex.h>

#include <mutex>
#include <shared_mutex>

namespace util {

class SCOPED_LOCKABLE AbslMutexLockGuard {
 private:
  absl::Mutex& m;

 public:
  explicit AbslMutexLockGuard(absl::Mutex& m_) EXCLUSIVE_LOCK_FUNCTION(m)
      : m(m_) {
    m.Lock();
  }

  ~AbslMutexLockGuard() UNLOCK_FUNCTION() { m.Unlock(); }

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
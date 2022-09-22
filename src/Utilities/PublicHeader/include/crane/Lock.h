#pragma once

#include <absl/synchronization/mutex.h>

#include <boost/thread/lock_guard.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/recursive_mutex.hpp>
#include <boost/thread/shared_lock_guard.hpp>
#include <boost/thread/shared_mutex.hpp>

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

using recursive_mutex = boost::recursive_mutex;
using recursive_lock_guard = boost::lock_guard<boost::recursive_mutex>;

using rw_mutex = boost::shared_mutex;
using read_lock_guard = boost::shared_lock_guard<boost::shared_mutex>;
using write_lock_guard = boost::lock_guard<boost::shared_mutex>;

}  // namespace util
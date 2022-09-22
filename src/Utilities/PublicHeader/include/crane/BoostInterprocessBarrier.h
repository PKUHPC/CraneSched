#pragma once

#include <errno.h>
#include <pthread.h>

#include <boost/interprocess/exceptions.hpp>

/// Code from Boost 1.41

namespace boost {
namespace interprocess {
namespace detail {

//! Makes pthread_barrierattr_t cleanup easy when using exceptions
struct barrierattr_wrapper {
  //! Constructor
  barrierattr_wrapper() {
    int err;

    err = pthread_barrierattr_init(&m_attr);
    if (err != 0)
      throw boost::interprocess::interprocess_exception(strerror(err));

    err = pthread_barrierattr_setpshared(&m_attr, PTHREAD_PROCESS_SHARED);
    if (err != 0)
      throw boost::interprocess::interprocess_exception(strerror(err));
  }

  //! Destructor
  ~barrierattr_wrapper() { pthread_barrierattr_destroy(&m_attr); }

  //! This allows using mutexattr_wrapper as pthread_barrierattr_t
  operator pthread_barrierattr_t &() { return m_attr; }

  pthread_barrierattr_t m_attr;
};

//! Makes initialized pthread_barrier_t cleanup easy when using exceptions
class barrier_initializer {
 public:
  //! Constructor. Takes barrier attributes to initialize the barrier
  barrier_initializer(pthread_barrier_t &mut, pthread_barrierattr_t &mut_attr,
                      int count)
      : mp_barrier(&mut) {
    int err;
    if ((err = pthread_barrier_init(mp_barrier, &mut_attr, count)) != 0)
      throw boost::interprocess::interprocess_exception(strerror(err));
  }

  ~barrier_initializer() {
    if (mp_barrier) pthread_barrier_destroy(mp_barrier);
  }

  void release() { mp_barrier = 0; }

 private:
  pthread_barrier_t *mp_barrier;
};

}  // namespace detail

//! An object of class barrier is a synchronization primitive that
//! can be placed in shared memory used to cause a set of threads from
//! different processes to wait until they each perform a certain
//! function or each reach a particular point in their execution.
class barrier {
 public:
  //! Constructs a barrier object that will cause count threads
  //! to block on a call to wait().
  barrier(unsigned int count) {
    if (count == 0) throw std::invalid_argument("count cannot be zero.");
    detail::barrierattr_wrapper barrier_attr;
    detail::barrier_initializer barrier(m_barrier, barrier_attr,
                                        static_cast<int>(count));
    barrier.release();
  }

  //! Destroys *this. If threads are still executing their wait()
  //! operations, the behavior for these threads is undefined.
  ~barrier() {
    int res = pthread_barrier_destroy(&m_barrier);
    assert(res == 0);
    (void)res;
  }

  //! Effects: Wait until N threads call wait(), where N equals the count
  //! provided to the constructor for the barrier object.
  //! Note that if the barrier is destroyed before wait() can return,
  //! the behavior is undefined.
  //! Returns: Exactly one of the N threads will receive a return value
  //! of true, the others will receive a value of false. Precisely which
  //! thread receives the return value of true will be implementation-defined.
  //! Applications can use this value to designate one thread as a leader that
  //! will take a certain action, and the other threads emerging from the
  //! barrier can wait for that action to take place.
  bool wait() {
    int res = pthread_barrier_wait(&m_barrier);

    if (res != PTHREAD_BARRIER_SERIAL_THREAD && res != 0) {
      throw interprocess_exception(res);
    }
    return res == PTHREAD_BARRIER_SERIAL_THREAD;
  }

 private:
  pthread_barrier_t m_barrier;
};

}  // namespace interprocess
}  // namespace boost
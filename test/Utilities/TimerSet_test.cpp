#include "crane/TimerSet.h"

#include <absl/synchronization/mutex.h>
#include <event2/thread.h>
#include <gtest/gtest.h>

TEST(TimerSet, simple) {
  evthread_use_pthreads();

  crane::TimerSet timer_set;

  absl::Mutex mtx;
  bool stop = false;

  timer_set.AddTimer(std::chrono::seconds(2),
                     [] { CRANE_INFO("Timer after 2s is triggered!"); });

  timer_set.AddTimer(std::chrono::seconds(3), [&] {
    CRANE_INFO("Timer after 3s is triggered!");
    mtx.Lock();
    stop = true;
    mtx.Unlock();
  });

  mtx.Lock();
  mtx.Await(absl::Condition(
      +[](bool *arg) -> bool { return *arg; }, &stop));

  timer_set.Stop();
}
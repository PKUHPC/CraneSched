#pragma once

#include <concurrentqueue/concurrentqueue.h>
#include <event2/event.h>

#include <functional>
#include <thread>

#include "crane/PublicHeader.h"

namespace crane {

class TimerSet {
 public:
  TimerSet();
  ~TimerSet();

  template <typename Duration>
  void AddTimer(Duration duration, std::function<void()> cb) {
    auto* meta = new TimerMeta;
    meta->cb = std::move(cb);

    std::chrono::seconds const sec =
        std::chrono::duration_cast<std::chrono::seconds>(duration);

    meta->tv.tv_sec = sec.count();
    meta->tv.tv_usec =
        std::chrono::duration_cast<std::chrono::microseconds>(duration - sec)
            .count();

    m_new_timer_queue_.enqueue(meta);
    event_active(m_event_new_timer_, 0, 0);
  }

  void Stop();

 private:
  static void OnStop_(int, short, void* arg);

  static void OnNewTimer_(int, short, void* arg);

  static void Ontimer_(int, short, void* arg);

  struct event_base* m_timer_events_base_;
  std::thread m_timer_thread_;

  struct event* m_event_new_timer_;

  struct event* m_event_stop_;

  struct TimerMeta {
    struct event* ev;
    std::function<void()> cb;
    timeval tv;
  };

  moodycamel::ConcurrentQueue<TimerMeta*> m_new_timer_queue_;
};

}  // namespace crane
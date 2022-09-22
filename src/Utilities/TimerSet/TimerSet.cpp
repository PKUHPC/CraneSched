#include "crane/TimerSet.h"

namespace crane {

TimerSet::TimerSet() {
  m_timer_events_base_ = event_base_new();
  CRANE_ASSERT_MSG(m_timer_events_base_ != nullptr,
                   "Timer event base initialization failed!");

  m_event_new_timer_ = event_new(m_timer_events_base_, -1, EV_READ | EV_PERSIST,
                                 OnNewTimer_, this);
  CRANE_ASSERT_MSG(m_event_new_timer_ != nullptr,
                   "Failed to create new timer event!");
  event_add(m_event_new_timer_, nullptr);

  m_event_stop_ = event_new(m_timer_events_base_, -1, EV_READ | EV_PERSIST,
                            OnStop_, m_timer_events_base_);
  CRANE_ASSERT_MSG(m_event_stop_ != nullptr,
                   "Failed to create new timer event!");
  event_add(m_event_stop_, nullptr);

  m_timer_thread_ =
      std::thread([this]() { event_base_dispatch(m_timer_events_base_); });
}

TimerSet::~TimerSet() {
  m_timer_thread_.join();

  if (m_event_stop_) event_free(m_event_stop_);
  if (m_event_new_timer_) event_free(m_event_new_timer_);
  if (m_timer_events_base_) event_base_free(m_timer_events_base_);
}

void TimerSet::OnNewTimer_(int, short, void* arg) {
  auto* this_ = reinterpret_cast<TimerSet*>(arg);
  TimerMeta* meta;

  // User-define events acts in an edge-triggered way. A while loop is needed.
  while (this_->m_new_timer_queue_.try_dequeue(meta)) {
    struct event* ev =
        event_new(this_->m_timer_events_base_, -1, 0, Ontimer_, meta);
    CRANE_ASSERT_MSG(ev != nullptr, "Failed to create new timer.");

    evtimer_add(ev, &meta->tv);
    meta->ev = ev;
  }
}

void TimerSet::Ontimer_(int, short, void* arg) {
  auto* meta = reinterpret_cast<TimerMeta*>(arg);
  meta->cb();

  event_free(meta->ev);
  delete meta;
}

void TimerSet::Stop() { event_active(m_event_stop_, 0, 0); }

void TimerSet::OnStop_(int, short, void* arg) {
  auto* base = reinterpret_cast<struct event_base*>(arg);
  timeval tv{};
  event_base_loopexit(base, &tv);
}

}  // namespace crane
#include <gtest/gtest.h>
#include <pevents/pevents.h>
#include <spdlog/fmt/fmt.h>

#include <array>
#include <thread>

namespace pevents {
using namespace neosmart;

using event_t = neosmart_event_t;
}  // namespace pevents

TEST(PEVENTS, Simple) {
  pevents::event_t ev_a = pevents::CreateEvent(true);
  pevents::event_t ev_b = pevents::CreateEvent();
  std::array evs{ev_a, ev_b};

  pevents::SetEvent(ev_a);

  int index;
  int i = 0;
  while (true) {
    pevents::WaitForMultipleEvents(evs.data(), evs.size(), false,
                                   pevents::WAIT_INFINITE, index);
    GTEST_LOG_(INFO) << fmt::format("Event #{} triggered!", index);
    if (++i >= 3) break;
  }
}
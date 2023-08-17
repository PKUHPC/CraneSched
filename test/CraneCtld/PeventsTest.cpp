/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

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
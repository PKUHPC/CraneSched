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
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

#include <absl/container/btree_map.h>
#include <absl/synchronization/blocking_counter.h>
#include <absl/synchronization/mutex.h>
#include <gtest/gtest.h>
#include <spdlog/fmt/fmt.h>

#include <atomic>
#include <queue>
#include <thread>

TEST(Misc, AbseilBtreeMap) {
  absl::btree_map<int, int> map;
  map.emplace(1, 2);
  map.emplace(2, 3);
  map.emplace(3, 4);

  auto iter = map.begin();
  for (int i = 1; i <= 3; i++, iter++) {
    EXPECT_EQ(iter->first, i);
  }

  iter = map.find(3);
  map.emplace(4, 5);
  EXPECT_EQ((++iter)->first, 4);
  map.emplace(5, 6);
  EXPECT_EQ((++iter)->second, 6);
}

TEST(Misc, AbseilMutexCondition) {
  absl::Mutex mtx;
  std::queue<int> int_queue;
  std::atomic_bool stop{false};
  std::thread t;
  absl::BlockingCounter counter(1);

  absl::Condition cond(
      +[](decltype(int_queue)* queue) { return !queue->empty(); }, &int_queue);

  t = std::thread([&] {
    int cnt = 0;
    int val;

    while (true) {
      if (stop) break;
      bool has_msg = mtx.LockWhenWithTimeout(cond, absl::Milliseconds(300));
      if (!has_msg) {
        mtx.Unlock();
        continue;
      }

      val = int_queue.front();
      EXPECT_EQ(cnt++, val);
      GTEST_LOG_(INFO) << std::format("Popped val: {}", val);

      int_queue.pop();
      mtx.Unlock();

      if (cnt == 3) counter.DecrementCount();
    }
  });

  int_queue.push(0);
  int_queue.push(1);
  int_queue.push(2);

  counter.Wait();
  stop = true;

  t.join();
}

TEST(Misc, AbslTime) {
  absl::Time now = absl::Now();
  absl::Time Inf = now + absl::InfiniteDuration();
  EXPECT_TRUE(now < Inf);
}
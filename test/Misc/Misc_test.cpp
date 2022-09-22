#include <absl/container/btree_map.h>
#include <absl/synchronization/blocking_counter.h>
#include <absl/synchronization/mutex.h>
#include <gmock/gmock.h>
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
      GTEST_LOG_(INFO) << fmt::format("Popped val: {}", val);

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
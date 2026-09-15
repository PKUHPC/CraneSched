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

#include "crane/AccountingTime.h"

#include <google/protobuf/util/time_util.h>
#include <gtest/gtest.h>

namespace {

using util::accounting::ClampEnd;
using util::accounting::ElapsedSeconds;
using util::accounting::Normalize;

TEST(AccountingTime, NormalizesToSupportedWholeSeconds) {
  const absl::Time value =
      absl::FromUnixSeconds(1'700'000'000) + absl::Milliseconds(900);
  EXPECT_EQ(Normalize(value), absl::FromUnixSeconds(1'700'000'000));
  EXPECT_EQ(Normalize(absl::InfinitePast()), absl::UnixEpoch());
  EXPECT_EQ(Normalize(absl::InfiniteFuture()),
            absl::FromUnixSeconds(
                google::protobuf::util::TimeUtil::kTimestampMaxSeconds));
}

TEST(AccountingTime, ClampsOnlyValidReverseIntervals) {
  const absl::Time start = absl::FromUnixSeconds(1'700'000'010);
  EXPECT_EQ(ClampEnd(start, absl::FromUnixSeconds(1'700'000'005)), start);
  EXPECT_EQ(ClampEnd(start, start), start);
  EXPECT_EQ(ClampEnd(absl::UnixEpoch(), absl::UnixEpoch()), absl::UnixEpoch());
}

TEST(AccountingTime, CalculatesZeroAndRejectsInvalidIntervals) {
  const absl::Time start = absl::FromUnixSeconds(1'700'000'000);
  EXPECT_EQ(ElapsedSeconds(start, start), std::optional<int64_t>{0});
  EXPECT_EQ(ElapsedSeconds(start, start + absl::Seconds(5)),
            std::optional<int64_t>{5});
  EXPECT_FALSE(ElapsedSeconds(start, start - absl::Seconds(1)).has_value());
  EXPECT_FALSE(ElapsedSeconds(absl::UnixEpoch(), start).has_value());
}

}  // namespace

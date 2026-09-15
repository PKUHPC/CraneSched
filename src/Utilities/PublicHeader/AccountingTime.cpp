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

namespace util::accounting {
namespace {

const absl::Time kMaxTimestamp = absl::FromUnixSeconds(
    google::protobuf::util::TimeUtil::kTimestampMaxSeconds);

bool IsSet(absl::Time value) {
  return value > absl::UnixEpoch() && value < kMaxTimestamp;
}

}  // namespace

absl::Time Normalize(absl::Time value) {
  if (value <= absl::UnixEpoch()) return absl::UnixEpoch();
  if (value >= kMaxTimestamp) return kMaxTimestamp;
  return absl::FromUnixSeconds(absl::ToUnixSeconds(value));
}

absl::Time ClampEnd(absl::Time start, absl::Time end) {
  start = Normalize(start);
  end = Normalize(end);
  return IsSet(start) && IsSet(end) && end < start ? start : end;
}

std::optional<int64_t> ElapsedSeconds(absl::Time start, absl::Time end) {
  start = Normalize(start);
  end = Normalize(end);
  if (!IsSet(start) || !IsSet(end) || end < start) return std::nullopt;
  return absl::ToInt64Seconds(end - start);
}

}  // namespace util::accounting

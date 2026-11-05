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

package util

import (
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var earliestCraneTimestamp = time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)

func ParseCraneTimestamp(timestamp *timestamppb.Timestamp) (time.Time, bool) {
	if timestamp == nil || !timestamp.IsValid() ||
		timestamp.Seconds >= MaxJobTimeStamp {
		return time.Time{}, false
	}
	value := timestamp.AsTime()
	return value, !value.Before(earliestCraneTimestamp)
}

func ValidDurationSeconds(duration *durationpb.Duration) (int64, bool) {
	if duration == nil || !duration.IsValid() || duration.AsDuration() < 0 {
		return 0, false
	}
	return duration.Seconds, true
}

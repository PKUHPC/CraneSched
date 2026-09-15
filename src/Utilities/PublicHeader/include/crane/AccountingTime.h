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

#pragma once

#include <absl/time/time.h>

#include <cstdint>
#include <optional>

namespace util::accounting {

[[nodiscard]] absl::Time Normalize(absl::Time value);
[[nodiscard]] absl::Time ClampEnd(absl::Time start, absl::Time end);
[[nodiscard]] std::optional<int64_t> ElapsedSeconds(absl::Time start,
                                                    absl::Time end);

}  // namespace util::accounting

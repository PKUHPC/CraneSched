/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#include <cerrno>
#include <cstddef>
#include <cstdlib>
#include <limits>

extern "C" void* reallocarray(void* pointer, std::size_t count,
                              std::size_t size) {
  if (size != 0 && count > std::numeric_limits<std::size_t>::max() / size) {
    errno = ENOMEM;
    return nullptr;
  }
  return std::realloc(pointer, count * size);
}

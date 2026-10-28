/**
 * Copyright (c) 2026 Peking University and Peking University
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

#include <unistd.h>

#include <utility>

namespace util {

// Owns a file descriptor and closes it on destruction.
class FileDescriptor {
 public:
  explicit FileDescriptor(int fd = -1) : m_fd_(fd) {}
  ~FileDescriptor() {
    if (m_fd_ >= 0) close(m_fd_);
  }
  FileDescriptor(FileDescriptor&& other) noexcept
      : m_fd_(std::exchange(other.m_fd_, -1)) {}
  FileDescriptor(const FileDescriptor&) = delete;
  FileDescriptor& operator=(const FileDescriptor&) = delete;
  int Get() const { return m_fd_; }

 private:
  int m_fd_;
};

}  // namespace util

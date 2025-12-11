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

#include <fcntl.h>
#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <filesystem>

#include "crane/Logger.h"
#include "crane/OS.h"

struct SystemRelInfo {
  std::string name;
  std::string release;
  std::string version;
};

struct NodeSpecInfo {
  std::string name;
  int64_t cpu;
  double memory_gb;
};

namespace util {

namespace os {

bool GetNodeInfo(NodeSpecInfo* info);

bool DeleteFile(std::string const& p);

bool DeleteFolders(std::string const& p);

bool CreateFolders(std::string const& p);

bool CreateFoldersForFile(std::string const& p);

bool CreateFoldersForFileEx(const std::filesystem::path& file_path, uid_t owner,
                            gid_t group, mode_t permissions = 0755);

bool SetFdNonBlocking(int fd);

// Close file descriptors within [fd_begin, fd_end)
void CloseFdRange(int fd_begin, int fd_end);

// Close file descriptors from fd_begin to the max fd.
// This may be slow if fd_max is too large.
void CloseFdFrom(int fd_begin);

// Set close-on-exec flag on [fd_begin, fd_end).
void SetCloseOnExecOnFdRange(int fd_begin, int fd_end);

// Set close-on-exec flag from fd_begin to the max fd.
// This may be slow if fd_max is too large.
void SetCloseOnExecFromFd(int fd_begin);

bool SetMaxFileDescriptorNumber(unsigned long num);

bool GetSystemReleaseInfo(SystemRelInfo* info);

bool CheckProxyEnvironmentVariable();

absl::Time GetSystemBootTime();

CraneExpected<std::string> GetHostname();

}  // namespace os

}  // namespace util

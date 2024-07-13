/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <fcntl.h>
#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <filesystem>

#include "crane/Logger.h"
#include "crane/OS.h"

namespace util {

namespace os {

bool DeleteFile(std::string const& p);

bool CreateFolders(std::string const& p);

bool CreateFoldersForFile(std::string const& p);

// Close file descriptors within [fd_begin, fd_end)
void CloseFdRange(int fd_begin, int fd_end);

// Close file descriptors from fd_begin to the max fd.
void CloseFdFrom(int fd_begin);

// Set close-on-exec flag on [fd_begin, fd_end).
void SetCloseOnExecOnFdRange(int fd_begin, int fd_end);

// Set close-on-exec flag from fd_begin to the max fd.
void SetCloseOnExecFromFd(int fd_begin);

bool SetMaxFileDescriptorNumber(unsigned long num);

bool GetSystemReleaseInfo(std::string* system_name, std::string* system_release,
                          std::string* system_version);

absl::Time GetSystemBootTime();
}  // namespace os

}  // namespace util
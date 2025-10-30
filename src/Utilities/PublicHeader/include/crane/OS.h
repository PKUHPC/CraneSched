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
#include <fcntl.h>
#include <sys/resource.h>
#include <unistd.h>

#include <filesystem>
#include <set>
#include <string>

#include <grp.h>
#include <pwd.h>
#include <sys/wait.h>

#include <subprocess/subprocess.h>

#include "crane/Logger.h"

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

// prolog or epilog
struct RunLogHookArgs {
  std::vector<std::string> scripts;
  std::unordered_map<std::string, std::string> envs;
  uint32_t timeout_sec;
  uid_t run_uid;
  gid_t run_gid;
  bool is_prolog;
  task_id_t job_id;
  std::function<bool(pid_t, task_id_t)> callback;
};

namespace util::os {

bool GetNodeInfo(NodeSpecInfo* info);

bool DeleteFile(std::string const& p);

bool DeleteFolders(std::string const& p);

bool CreateFile(std::string const& p);

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

// Close file descriptors from fd_begin to the max fd except for `skip_fds`.
// This may be slow if fd_max is too large.
void CloseFdFromExcept(int fd_begin, const std::set<int>& skip_fds);

// Set close-on-exec flag on [fd_begin, fd_end).
void SetCloseOnExecOnFdRange(int fd_begin, int fd_end);

// Set close-on-exec flag from fd_begin to the max fd.
// This may be slow if fd_max is too large.
void SetCloseOnExecFromFd(int fd_begin);

bool SetMaxFileDescriptorNumber(uint64_t num);

bool GetSystemReleaseInfo(SystemRelInfo* info);

bool CheckProxyEnvironmentVariable();

bool CheckUserHasPermission(uid_t uid, gid_t gid,
                            std::filesystem::path const& p);

absl::Time GetSystemBootTime();

std::optional<std::string> RunPrologOrEpiLog(const RunLogHookArgs& args);

void ApplyPrologOutputToEnvAndStdout(const std::string& output,
                                     std::unordered_map<std::string, std::string>* env_map,
                                     int task_stdout_fd);

}  // namespace util::os

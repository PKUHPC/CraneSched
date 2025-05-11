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

#include "crane/OS.h"

#include <subprocess/subprocess.h>

#if defined(__linux__) || defined(__unix__)
#  include <sys/stat.h>
#  include <sys/sysinfo.h>
#  include <sys/utsname.h>
#elif defined(_WIN32)
#  error "Win32 Platform is not supported now!"
#else
#  error "Unsupported OS"
#endif

namespace util::os {

bool DeleteFile(std::string const& p) {
  std::error_code ec;
  bool ok = std::filesystem::remove(p, ec);

  if (!ok) CRANE_ERROR("Failed to remove file {}: {}", p, ec.message());

  return ok;
}

bool DeleteFolders(std::string const& p) {
  std::error_code ec;
  std::filesystem::remove_all(p, ec);
  if (ec) {
    CRANE_ERROR("Failed to remove folder {}: {}", p, ec.message());
    return false;
  }

  return true;
}

bool CreateFolders(std::string const& p) {
  if (std::filesystem::exists(p)) return true;

  std::error_code ec;
  bool ok = std::filesystem::create_directories(p, ec);

  if (!ok) CRANE_ERROR("Failed to create folder {}: {}", p, ec.message());

  return ok;
}

bool CreateFoldersForFile(std::string const& p) {
  try {
    std::filesystem::path log_path{p};
    auto log_dir = log_path.parent_path();
    if (!std::filesystem::exists(log_dir))
      std::filesystem::create_directories(log_dir);
  } catch (const std::exception& e) {
    CRANE_ERROR("Failed to create folder for {}: {}", p, e.what());
    return false;
  }

  return true;
}

bool CreateFoldersForFileEx(const std::string& p, uid_t owner, gid_t group,
                            mode_t permissions = 0755) {
  namespace fs = std::filesystem;

  try {
    fs::path dir_path = p;
    dir_path = dir_path.parent_path();

    fs::path current_dir;
    for (auto& part : dir_path) {
      current_dir /= part;
      if (!fs::exists(current_dir)) {
        if (mkdir(current_dir.c_str(), permissions) != 0) {
          CRANE_ERROR("Failed to create directory {}: {}", current_dir.c_str(),
                      strerror(errno));
          return false;
        }
      }

      if (chown(current_dir.c_str(), owner, group) != 0) {
        CRANE_ERROR("Failed to change ownership of directory {}: {}",
                    current_dir.c_str(), strerror(errno));
        return false;
      }
    }
  } catch (const std::exception& e) {
    CRANE_ERROR("Failed to create folder for {}: {}", p.c_str(), e.what());
    return false;
  }

  return true;
}

int GetFdOpenMax() { return static_cast<int>(sysconf(_SC_OPEN_MAX)); }

void CloseFdRange(int fd_begin, int fd_end) {
  int fd_max = std::min(GetFdOpenMax(), fd_end);
  for (int i = fd_begin; i < fd_max; i++) close(i);
}

void CloseFdFrom(int fd_begin) {
  int fd_max = GetFdOpenMax();
  for (int i = fd_begin; i < fd_max; i++) close(i);
}

void SetCloseOnExecOnFdRange(int fd_begin, int fd_end) {
  int fd_max;
  int flag;

  fd_max = std::min(GetFdOpenMax(), fd_end);
  for (int i = fd_begin; i < fd_max; i++) {
    flag = fcntl(i, F_GETFD);

    if (flag != -1) {
      fcntl(i, F_SETFD, flag | FD_CLOEXEC);
    }
  }
}

void SetCloseOnExecFromFd(int fd_begin) {
  int fd_max;
  int flag;

  fd_max = GetFdOpenMax();
  for (int i = fd_begin; i < fd_max; i++) {
    flag = fcntl(i, F_GETFD);

    if (flag != -1) {
      fcntl(i, F_SETFD, flag | FD_CLOEXEC);
    }
  }
}

bool SetMaxFileDescriptorNumber(unsigned long num) {
  struct rlimit rlim{};
  rlim.rlim_cur = num;
  rlim.rlim_max = num;

  return setrlimit(RLIMIT_NOFILE, &rlim) == 0;
}

std::tuple<int, std::string, std::string> RunSubprocess(
    const std::string& cmd) {
  std::vector<const char*> argv;
  for (auto&& s : std::views::split(cmd, ' ')) {
    argv.emplace_back(s.data());
  }
  argv.push_back(nullptr);

  subprocess_s subprocess{};
  int rc = subprocess_create(argv.data(), 0, &subprocess);
  if (rc) {
    return {rc, "", ""};
  }

  auto buf = std::make_unique<char[]>(4096);
  std::string stdout_str, stderr_str;

  std::FILE* cmd_fd = subprocess_stdout(&subprocess);
  while (std::fgets(buf.get(), 4096, cmd_fd) != nullptr)
    stdout_str.append(buf.get());

  cmd_fd = subprocess_stderr(&subprocess);
  while (std::fgets(buf.get(), 4096, cmd_fd) != nullptr)
    stderr_str.append(buf.get());

  if (0 != subprocess_join(&subprocess, &rc))
    CRANE_ERROR("Failed to join subprocess for {}.", cmd);

  if (0 != subprocess_destroy(&subprocess))
    CRANE_ERROR("Failed to destroy subprocess for {}.", cmd);

  return {rc, stdout_str, stderr_str};
}

bool CheckProxyEnvironmentVariable() {
  bool has_proxy = false;
  const char* HTTP_PROXY = std::getenv("HTTP_PROXY");
  if (HTTP_PROXY) {
    has_proxy = true;
    CRANE_WARN("HTTP_PROXY is set to {}", HTTP_PROXY);
  }
  const char* http_proxy = std::getenv("http_proxy");
  if (http_proxy) {
    has_proxy = true;
    CRANE_WARN("http_proxy is set to {}", http_proxy);
  }
  const char* HTTPS_PROXY = std::getenv("HTTPS_PROXY");
  if (HTTPS_PROXY) {
    has_proxy = true;
    CRANE_WARN("HTTPS_PROXY is set to {}", HTTPS_PROXY);
  }
  const char* https_proxy = std::getenv("https_proxy");
  if (https_proxy) {
    has_proxy = true;
    CRANE_WARN("https_proxy is set to {}", https_proxy);
  }
  return has_proxy;
}

bool GetSystemReleaseInfo(SystemRelInfo* info) {
#if defined(__linux__) || defined(__unix__)
  utsname utsname_info{};

  if (uname(&utsname_info) != -1) {
    info->name = utsname_info.sysname;
    info->release = utsname_info.release;
    info->version = utsname_info.version;
    return true;
  }

  return false;

#else
#  error "Unsupported OS"
#endif
}

absl::Time GetSystemBootTime() {
#if defined(__linux__) || defined(__unix__)
  struct sysinfo system_info{};
  if (sysinfo(&system_info) != 0) {
    CRANE_ERROR("Failed to get sysinfo {}.", strerror(errno));
    return {};
  }

  absl::Time current_time = absl::FromTimeT(time(nullptr));
  absl::Duration uptime = absl::Seconds(system_info.uptime);
  return current_time - uptime;

#else
#  error "Unsupported OS"
#endif
}

}  // namespace util::os
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

#include <grp.h>
#include <pwd.h>
#include <sys/types.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <string>
#include <vector>

#include "crane/Logger.h"

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

bool GetNodeInfo(NodeSpecInfo* info) {
  if (info == nullptr) return false;

  std::array<char, HOST_NAME_MAX + 1> hostname{};
  if (gethostname(hostname.data(), hostname.size()) != 0) {
    int err = errno;
    fmt::print(stderr, "gethostname failed: errno={} ({})\n", err,
               strerror(err));
    return false;
  }

  int64_t cpu_count = sysconf(_SC_NPROCESSORS_ONLN);
  cpu_count = std::max<int64_t>(cpu_count, 1);

  struct sysinfo sys_info{};
  if (sysinfo(&sys_info) != 0) {
    int err = errno;
    fmt::print(stderr, "sysinfo failed: errno={} ({})\n", err, strerror(err));
    return false;
  }

  uint64_t mem_bytes = sys_info.totalram * sys_info.mem_unit;
  double mem_gb = static_cast<double>(mem_bytes) / (1024 * 1024 * 1024);

  info->name = std::string(hostname.data());
  info->cpu = cpu_count;
  info->memory_gb = mem_gb;

  return true;
}

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

bool CreateFile(std::string const& p) {
  if (std::filesystem::exists(p)) return true;

  int fd = open(p.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd == -1) {
    CRANE_ERROR("Create file {} failed: {}", p, std::strerror(errno));
    return false;
  }
  close(fd);
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

bool CreateFoldersForFileEx(const std::filesystem::path& file_path, uid_t owner,
                            gid_t group, mode_t permissions) {
  namespace fs = std::filesystem;

  try {
    fs::path dir_path = file_path;
    dir_path = dir_path.parent_path();

    fs::path current_dir;
    for (const auto& part : dir_path) {
      current_dir /= part;
      if (fs::exists(current_dir)) {
        if (!fs::is_directory(current_dir)) {
          CRANE_WARN("Path {} exists but not a directory", current_dir);
          return false;
        }
        continue;
      }
      // FIXME: potential TOCTOU + symlink attack: create a symlink after the
      // existence check and before mkdir.
      if (mkdir(current_dir.c_str(), permissions) != 0) {
        CRANE_ERROR("Failed to create directory {}: {}", current_dir.c_str(),
                    strerror(errno));
        return false;
      }

      if (chown(current_dir.c_str(), owner, group) != 0) {
        CRANE_ERROR("Failed to change ownership of directory {}: {}",
                    current_dir.c_str(), strerror(errno));
        return false;
      }
    }
  } catch (const std::exception& e) {
    CRANE_ERROR("Failed to create folder for {}: {}", file_path.c_str(),
                e.what());
    return false;
  }

  return true;
}

// NOLINTNEXTLINE(misc-use-internal-linkage)
int GetFdOpenMax() { return static_cast<int>(sysconf(_SC_OPEN_MAX)); }

bool SetFdNonBlocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags == -1) {
    return false;
  }
  fcntl(fd, F_SETFL, flags | O_NONBLOCK);
  return true;
}

void CloseFdRange(int fd_begin, int fd_end) {
  int fd_max = std::min(GetFdOpenMax(), fd_end);
  for (int i = fd_begin; i < fd_max; i++) close(i);
}

void CloseFdFrom(int fd_begin) {
  int fd_max = GetFdOpenMax();
  for (int i = fd_begin; i < fd_max; i++) close(i);
}

void CloseFdFromExcept(int fd_begin, const std::set<int>& skip_fds) {
  int fd_max = GetFdOpenMax();
  for (int i = fd_begin; i < fd_max; i++)
    if (!skip_fds.contains(i)) close(i);
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

bool SetMaxFileDescriptorNumber(uint64_t num) {
  struct rlimit rlim{};
  rlim.rlim_cur = num;
  rlim.rlim_max = num;

  return setrlimit(RLIMIT_NOFILE, &rlim) == 0;
}

bool CheckProxyEnvironmentVariable() {
  bool has_proxy = false;

  // NOLINTBEGIN
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
  // NOLINTEND

  return has_proxy;
}

bool CheckUserHasPermission(uid_t uid, gid_t gid,
                            std::filesystem::path const& p) {
  // Use lstat to avoid following symlinks (prevent symlink traversal attacks)
  // and get all information in a single syscall
  struct stat st{};
  if (::lstat(p.c_str(), &st) != 0) {
    int e = errno;
    if (e == ENOENT) {
      CRANE_ERROR("Path {} does not exist.", p.c_str());
    } else {
      CRANE_ERROR("lstat({}) failed: {} ({})", p.c_str(), std::strerror(e), e);
    }
    return false;
  }

  const uid_t owner_uid = st.st_uid;
  const gid_t owner_gid = st.st_gid;
  const mode_t mode = st.st_mode;
  const bool is_dir = S_ISDIR(mode);

  if (S_ISLNK(mode)) {
    CRANE_ERROR("Path {} is a symlink and is not allowed.", p.c_str());
    return false;
  }

  if (uid == 0) {
    return true;
  }

  auto user_in_group = [uid, gid](gid_t target_gid) -> bool {
    if (gid == target_gid) {
      return true;
    }

    struct passwd pwd{};
    struct passwd* result = nullptr;
    long buf_size = sysconf(_SC_GETPW_R_SIZE_MAX);
    if (buf_size < 0) {
      buf_size = 16384;
    }
    std::string buf;
    buf.resize(static_cast<size_t>(buf_size));

    int rc = 0;
    while (true) {
      rc = getpwuid_r(uid, &pwd, buf.data(), buf.size(), &result);
      if (rc != ERANGE) {
        break;
      }
      buf.resize(buf.size() * 2);
    }
    if (rc != 0 || result == nullptr) {
      if (rc != 0) {
        CRANE_ERROR("getpwuid_r({}) failed: {} ({})", uid, std::strerror(rc),
                    rc);
      } else {
        CRANE_ERROR("getpwuid_r({}) failed: user not found", uid);
      }
      return false;
    }

    std::vector<gid_t> groups(16);
    int ngroups = static_cast<int>(groups.size());
    int gl_ret = getgrouplist(pwd.pw_name, gid, groups.data(), &ngroups);
    if (gl_ret == -1) {
      if (ngroups <= 0) {
        CRANE_ERROR("getgrouplist({}) failed", pwd.pw_name);
        return false;
      }
      groups.resize(ngroups);
      gl_ret = getgrouplist(pwd.pw_name, gid, groups.data(), &ngroups);
    }
    if (gl_ret == -1) {
      CRANE_ERROR("getgrouplist({}) failed", pwd.pw_name);
      return false;
    }

    for (int i = 0; i < ngroups; ++i) {
      if (groups[i] == target_gid) {
        return true;
      }
    }

    return false;
  };

  auto has_perm_for_class = [is_dir, mode](mode_t read_bit,
                                           mode_t exec_bit) -> bool {
    if (is_dir) {
      // For directory, require both read and exec bits
      return ((mode & read_bit) != 0) && ((mode & exec_bit) != 0);
    } else {  // NOLINT(readability-else-after-return)
      // For file, only require read bit
      return (mode & read_bit) != 0;
    }
  };

  // 5. Check permissions based on POSIX rules:
  //   - If uid matches owner uid, ONLY check owner permissions
  //   - Else if user is in owner group (primary or supplementary), ONLY check
  //     group permissions
  //   - Else ONLY check others permissions
  //   Each category is checked exclusively without fallback to lower privilege
  //   levels.
  if (uid == owner_uid) {
    return has_perm_for_class(S_IRUSR, S_IXUSR);
  } else if (user_in_group(owner_gid)) {
    return has_perm_for_class(S_IRGRP, S_IXGRP);
  } else {
    return has_perm_for_class(S_IROTH, S_IXOTH);
  }
}

bool GetSystemReleaseInfo(SystemRelInfo* info) {
#if defined(__linux__) || defined(__unix__)
  utsname utsname_info{};

  // NOLINTBEGIN(hicpp-no-array-decay)
  if (uname(&utsname_info) != -1) {
    info->name = utsname_info.sysname;
    info->release = utsname_info.release;
    info->version = utsname_info.version;
    return true;
  }
  // NOLINTEND(hicpp-no-array-decay)

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

RunCommandResult RunCommand(const RunCommandArgs& run_command_args) {
  int pipefd[2];
  RunCommandResult result{127, "", false, 0};

    if (pipe(pipefd) != 0)
        return result;

    pid_t pid = fork();
    if (pid < 0) {
        close(pipefd[0]);
        close(pipefd[1]);
        return result;
    }

    if (pid == 0) {
        close(pipefd[0]);
        setsid();
        if (run_command_args.run_gid >= 0) {
            if (setgid(run_command_args.run_gid) != 0) _exit(127);
        }
        if (run_command_args.run_uid >= 0) {
            struct passwd *pw = getpwuid(run_command_args.run_uid);
            if (pw) initgroups(pw->pw_name, run_command_args.run_gid >= 0 ? run_command_args.run_gid : pw->pw_gid);
            if (setuid(run_command_args.run_uid) != 0) _exit(127);
        }

        dup2(pipefd[1], STDOUT_FILENO);
        dup2(pipefd[1], STDERR_FILENO);
        close(pipefd[1]);

        std::vector<char*> argv;
        argv.push_back(const_cast<char*>(run_command_args.program.c_str()));
        for (const auto& arg : run_command_args.args)
            argv.push_back(const_cast<char*>(arg.c_str()));
        argv.push_back(nullptr);

        std::vector<char*> envp;
        if (!run_command_args.envs.empty()) {
            for (const auto& [k, v] : run_command_args.envs)
                envp.push_back(fmt::format("{}={}", k, v).data());
            envp.push_back(nullptr);
        }

        if (!run_command_args.envs.empty())
            execve(run_command_args.program.c_str(), argv.data(), envp.data());
        else
            execv(run_command_args.program.c_str(), argv.data());
        _exit(127);
    }

    close(pipefd[1]);
    int flags = fcntl(pipefd[0], F_GETFL, 0);
    fcntl(pipefd[0], F_SETFL, flags | O_NONBLOCK);

    char buf[256];
    int elapsed = 0;
    const int interval = 100;  // ms
    int status = 0;
    bool child_exited = false;
    uint32_t timeout_ms = run_command_args.timeout_sec > 0 ? run_command_args.timeout_sec * 1000 : 0;

    while (timeout_ms <= 0 || elapsed < timeout_ms) {
        ssize_t n;
        while ((n = read(pipefd[0], buf, sizeof(buf) - 1)) > 0) {
            buf[n] = '\0';
            result.output += buf;
        }
        pid_t r = waitpid(pid, &status, WNOHANG);
        if (r == pid) {
            child_exited = true;
            break;
        }
        usleep(interval * 1000);
        elapsed += interval;
    }

    ssize_t n;
    while ((n = read(pipefd[0], buf, sizeof(buf) - 1)) > 0) {
        buf[n] = '\0';
        result.output += buf;
    }
    close(pipefd[0]);

    if (!child_exited) {
        kill(-pid, SIGKILL);
        waitpid(pid, &status, 0);
        result.time_out = true;
        return result;
    }

    if (WIFEXITED(status)) {
        result.exit_code = WEXITSTATUS(status);
        result.term_signal = 0;
    } else if (WIFSIGNALED(status)) {
        result.exit_code = 127;
        result.term_signal = WTERMSIG(status);
    } else {
        result.exit_code = 127;
        result.term_signal = 0;
    }
    return result;
}

}  // namespace util::os

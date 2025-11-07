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

#include <future>

#include "absl/strings/str_split.h"
#include "re2/re2.h"

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
  if (!info) return false;

  char hostname[HOST_NAME_MAX + 1];
  if (gethostname(hostname, sizeof(hostname)) != 0) {
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

  info->name = hostname;
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

std::optional<std::string> RunPrologOrEpiLog(const RunLogHookArgs& args) {
  bool is_failed = false;

  auto read_stream = [](int fd) {
    std::string out;
    char buf[4096];
    ssize_t bytes_read;
    while ((bytes_read = read(fd, buf, sizeof(buf))) > 0) {
      out.append(buf, bytes_read);
    }
    return out;
  };

  std::string output;

  auto start_time = std::chrono::steady_clock::now();

  for (const auto& script : args.scripts) {
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - start_time);
    if (args.timeout_sec > 0 && elapsed.count() >= args.timeout_sec) {
      CRANE_ERROR("Total timeout ({}s) reached before running {}.",
                  args.timeout_sec, script);
      return std::nullopt;
    }

    int stdout_pipe[2], stderr_pipe[2], sync_pipe[2];
    if (pipe(stdout_pipe) == -1 || pipe(stderr_pipe) == -1 || pipe(sync_pipe) == -1) {
      CRANE_ERROR("{} pipe creation failed: {}", script, strerror(errno));
      if (args.is_prolog) return std::nullopt;
      is_failed = true;
      continue;
    }

    pid_t pid = fork();

    if (pid == -1) {
      CRANE_ERROR("{} subprocess creation failed: {}.", script,
                  strerror(errno));
      if (args.is_prolog) return std::nullopt;
      is_failed = true;
      continue;
    }

    if (pid > 0) {
      if (args.callback) {
        bool result = args.callback(pid, args.job_id);
        if (!result) {
          CRANE_ERROR("subprocess callback failed");
          return std::nullopt;
        }
      }

      close(stdout_pipe[1]);
      close(stderr_pipe[1]);
      int status = 0;
      auto fut = std::async(std::launch::async, [pid, &status]() {
        return waitpid(pid, &status, 0);
      });

      write(sync_pipe[1], "x", 1);
      close(sync_pipe[0]);
      close(sync_pipe[1]);

      auto now = std::chrono::steady_clock::now();
      auto elapsed_now =
          std::chrono::duration_cast<std::chrono::seconds>(now - start_time);
      uint32_t remaining_time =
          (args.timeout_sec > 0)
              ? std::max<uint32_t>(0, args.timeout_sec - elapsed_now.count())
              : 0;
      bool child_exited = false;

      if (args.timeout_sec == 0) {
        fut.get();
        child_exited = true;
      } else if (fut.wait_for(std::chrono::seconds(remaining_time)) ==
                 std::future_status::ready) {
        child_exited = true;
      }

      if (!child_exited) {
        kill(pid, SIGKILL);
        waitpid(pid, &status, 0);
        CRANE_ERROR("{} Timeout. stdout: {}, stderr: {}", script,
                  read_stream(stdout_pipe[0]),
                  read_stream(stderr_pipe[0]));
        if (args.is_prolog) return std::nullopt;
        is_failed = true;
        continue;
      }

      if (status != 0) {
        CRANE_ERROR("{} Failed (exit code:{}). stdout: {}, stderr: {}", script,
                  status, read_stream(stdout_pipe[0]),
                  read_stream(stderr_pipe[0]));
        if (args.is_prolog) return std::nullopt;
        is_failed = true;
        continue;
      }

      output.append(read_stream(stdout_pipe[0]));

      CRANE_DEBUG("{} finished successfully.", script);

    } else { // child proc
      close(stdout_pipe[0]);
      close(stderr_pipe[0]);
      dup2(stdout_pipe[1], STDOUT_FILENO);
      dup2(stderr_pipe[1], STDERR_FILENO);
      close(stdout_pipe[1]);
      close(stderr_pipe[1]);

      for (const auto& [name, value] : args.envs)
        if (setenv(name.c_str(), value.c_str(), 1))
          fmt::print(stderr, "[Subprocess] Warning: setenv() for {}={} failed.\n",
                     name, value);

      char buf;
      read(sync_pipe[0], &buf, 1);
      close(sync_pipe[1]);
      close(sync_pipe[0]);


      std::vector<const char*> argv = {script.c_str(), nullptr};
      execvp(argv[0], const_cast<char* const*>(argv.data()));
      fmt::print(stderr, "[Subprocess] execvp() failed: %s\n", strerror(errno));
      exit(EXIT_FAILURE);
    }
  }

  if (is_failed) return std::nullopt;

  return output;
}

void ApplyPrologOutputToEnvAndStdout(
    const std::string& output,
    std::unordered_map<std::string, std::string>* env_map, int task_stdout_fd) {
  static const LazyRE2 export_re = {
    R"(^export\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$)"};
  static const LazyRE2 unset_re = {R"(^unset\s+([A-Za-z_][A-Za-z0-9_]*)\s*$)"};
  static const LazyRE2 print_re = {R"(^print\s+(.*)$)"};

  for (std::string_view line : absl::StrSplit(output, '\n')) {
    std::string name, value, to_print;
    if (RE2::FullMatch(line, *export_re, &name, &value)) {
      (*env_map)[name] = value;
    } else if (RE2::FullMatch(line, *unset_re, &name)) {
      env_map->erase(name);
    } else if (RE2::FullMatch(line, *print_re, &to_print)) {
      write(task_stdout_fd, to_print.data(), to_print.size());
      write(task_stdout_fd, "\n", 1);
    }
  }
}

}  // namespace util::os
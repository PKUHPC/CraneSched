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

#include <absl/cleanup/cleanup.h>
#include <grp.h>
#include <poll.h>
#include <pwd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <future>
#include <string>
#include <uvw.hpp>
#include <vector>

#include "absl/strings/str_split.h"
#include "crane/Logger.h"
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

CraneExpected<std::string> GetHostname() {
  char hostname[HOST_NAME_MAX + 1];
  if (gethostname(hostname, sizeof(hostname)) != 0) {
    int err = errno;
    CRANE_ERROR("gethostname failed: errno={} ({})\n", err, strerror(err));
    return std::unexpected{CraneErrCode::ERR_SYSTEM_ERR};
  }
  return std::string(hostname);
}

bool IsAbsolutePath(const std::string& path) {
  return std::filesystem::path(path).is_absolute();
}

void KillPg(pid_t pid) {
  killpg(pid, SIGTERM);
  usleep(10000);
  killpg(pid, SIGKILL);
}

/**
 * @brief Execute a sequence of prolog or epilog scripts serially.
 *
 * Each script is forked as a child process whose stdout/stderr are captured
 * via a pipe.  Scripts are run one-by-one: the next script starts only after
 * the previous one exits successfully.  If any script fails or the aggregate
 * wall-time budget is exhausted, execution stops and an error is returned.
 *
 * @param args  Configuration: script paths, timeout, uid/gid, environment
 *              variables, optional fork-and-watch hook, etc.
 * @return      The concatenated stdout/stderr of all scripts on success, or
 *              a RunPrologEpilogStatus describing the first failure.
 */
std::expected<std::string, RunPrologEpilogStatus> RunPrologOrEpiLog(
    const RunPrologEpilogArgs& args) {
  using namespace std::chrono;

  // Record the wall-clock start time so we can enforce an aggregate timeout
  // across all scripts in the sequence.
  const auto start_time = steady_clock::now();
  const auto timeout = duration_cast<milliseconds>(seconds(args.timeout_sec));

  // Accumulated stdout/stderr from every successfully completed script.
  std::string output;

  // Overall function result; initialised to a generic failure so that any
  // early-return path without an explicit assignment still signals an error.
  std::expected<std::string, RunPrologEpilogStatus> result =
      std::unexpected(RunPrologEpilogStatus{.exit_code = 1, .signal_num = 0});

  // Index into args.scripts – advanced by one each time a script succeeds.
  size_t script_idx = 0;

  // run_next_script is a recursive lambda: it runs scripts[script_idx], and
  // on success increments script_idx and calls itself again.  Using
  // std::function allows the lambda to capture a reference to itself.
  std::function<void()> run_next_script;

  run_next_script = [&]() {
    // -------------------------------------------------------------------------
    // Base case: all scripts have finished successfully.
    // -------------------------------------------------------------------------
    if (script_idx >= args.scripts.size()) {
      result = output;
      return;
    }

    const auto& script = args.scripts[script_idx];

    // Reject relative paths to prevent accidental execution of unintended
    // binaries when the working directory changes.
    if (!IsAbsolutePath(script)) {
      CRANE_ERROR("Script path '{}' is not absolute.", script);
      result = std::unexpected(
          RunPrologEpilogStatus{.exit_code = 1, .signal_num = 0});
      return;
    }

    // -------------------------------------------------------------------------
    // Create a pipe to capture the child's stdout and stderr.
    //   stdout_pipe[0] – read  end (parent reads script output)
    //   stdout_pipe[1] – write end (child writes its output here)
    // -------------------------------------------------------------------------
    int stdout_pipe[2];
    if (pipe(stdout_pipe) != 0) {
      CRANE_ERROR("Failed to create pipe for script '{}': {}", script,
                  strerror(errno));
      result = std::unexpected(
          RunPrologEpilogStatus{.exit_code = 1, .signal_num = 0});
      return;
    }

    // -------------------------------------------------------------------------
    // Fork the child process.
    //
    // Two modes depending on whether a fork_and_watch_fn hook is provided:
    //
    //  1. With hook (fork_and_watch_fn set):
    //     The hook acquires a mutex BEFORE calling fork(), then immediately
    //     registers the child PID in a ChildExitWatcher.  This eliminates the
    //     TOCTOU race where SIGCHLD could arrive between fork() and Watch().
    //     The hook returns:
    //       nullopt          – fork() failed (pid < 0)
    //       {0,  empty fut}  – we are the child process
    //       {pid, future}    – we are the parent; future delivers waitpid
    //       status
    //
    //  2. Without hook:
    //     Plain fork(); the caller is responsible for reaping via waitpid().
    // -------------------------------------------------------------------------
    pid_t pid = -1;
    std::future<int>
        exit_future;  // valid only in parent when use_watcher==true
    bool use_watcher = false;

    std::vector<char*> envp;
    std::vector<std::string> env_storage;
    envp.reserve(args.envs.size() + 1);
    for (const auto& [name, value] : args.envs) {
      env_storage.emplace_back(name + "=" + value);
      envp.emplace_back(const_cast<char*>(env_storage.back().c_str()));
    }
    envp.emplace_back(nullptr);

    const char* exec_argv[] = {script.c_str(), nullptr};

    if (args.fork_and_watch_fn) {
      auto fork_result = args.fork_and_watch_fn([]() { return fork(); });

      if (!fork_result) {
        // fork() itself failed (errno is already logged inside the hook).
        close(stdout_pipe[0]);
        close(stdout_pipe[1]);
        result = std::unexpected(
            RunPrologEpilogStatus{.exit_code = 1, .signal_num = 0});
        return;
      }

      pid = fork_result->first;

      // Only the parent process registers the watcher future.
      // The child (pid == 0) receives an empty future but will exec shortly
      // and never use it.
      if (pid > 0) {
        exit_future = std::move(fork_result->second);
        use_watcher = true;
      }
    } else {
      pid = fork();
      if (pid == -1) {
        CRANE_ERROR("Failed to fork for script '{}': {}", script,
                    strerror(errno));
        result = std::unexpected(
            RunPrologEpilogStatus{.exit_code = 1, .signal_num = 0});
        close(stdout_pipe[0]);
        close(stdout_pipe[1]);
        return;
      }
    }

    // =========================================================================
    // Child process
    // =========================================================================
    if (pid == 0) {
      // The child only writes to the pipe; close the read end immediately.
      close(stdout_pipe[0]);

      // Redirect both stdout and stderr to the write end of the pipe so the
      // parent can capture all script output through a single fd.
      if (dup2(stdout_pipe[1], STDOUT_FILENO) == -1 ||
          dup2(stdout_pipe[1], STDERR_FILENO) == -1) {
        close(stdout_pipe[1]);
        const char msg[] = "[Subprocess] dup2 failed\n";
        write(STDERR_FILENO, msg, sizeof(msg) - 1);
      }
      close(stdout_pipe[1]);  // original fd no longer needed after dup2

      // Close every fd >= 3 to avoid leaking parent file descriptors into
      // the script (e.g. sockets, log files, gRPC channels).
#if defined(__linux__) && defined(SYS_close_range)
      syscall(SYS_close_range, 3, UINT_MAX, 0);  // 单次系统调用
#else
      CloseFdFrom(3);
#endif

      // Place the child in its own process group so KillPg() can send
      // signals to the entire group (child + any grandchildren it spawns).
      setpgid(0, 0);

      // Optional caller-supplied callback executed in the child context
      // (e.g., to move the child into a cgroup before exec).
      // TODO: move to parent
      if (args.at_child_setup_cb) {
        if (!args.at_child_setup_cb(getpid())) {
          const char msg[] = "[Subprocess] child setup callback failed\n";
          write(STDERR_FILENO, msg, sizeof(msg) - 1);
          _exit(EXIT_FAILURE);
        }
      }

      // Drop to the requested group and user credentials before executing
      // the script.  Group must be dropped first (setgid fails after setuid
      // on most Linux configurations).
      if (setgid(args.run_gid) != 0) {
        const char msg[] = "[Subprocess] setgid failed\n";
        write(STDERR_FILENO, msg, sizeof(msg) - 1);
        _exit(EXIT_FAILURE);
      }
      if (setuid(args.run_uid) != 0) {
        const char msg[] = "[Subprocess] setuid failed\n";
        write(STDERR_FILENO, msg, sizeof(msg) - 1);
        _exit(EXIT_FAILURE);
      }

      // Replace the child image with the script.  execvp searches PATH, but
      // since the script path is absolute, PATH is irrelevant here.
      execvpe(exec_argv[0], const_cast<char* const*>(exec_argv), envp.data());

      // If execvp() returns, it has failed.
      const char msg[] = "[Subprocess] execvp failed\n";
      write(STDERR_FILENO, msg, sizeof(msg) - 1);
      _exit(EXIT_FAILURE);

    } else {
      // =======================================================================
      // Parent process
      // =======================================================================

      // The parent only reads from the pipe; close the write end so that
      // EOF (end_event) is triggered when the child closes its write end.
      close(stdout_pipe[1]);

      // Create a dedicated libuv event loop for this script.
      // Using a fresh loop per script keeps state isolated and avoids
      // cross-contamination between sequential script executions.
      auto loop = uvw::loop::create();
      auto pipe_handle = loop->uninitialized_resource<uvw::pipe_handle>(false);
      auto timeout_timer = loop->resource<uvw::timer_handle>();

      // poll_timer replaces the previous idle_handle + sleep_for(50ms) design.
      // A repeating 50ms timer is far less invasive than sleeping inside an
      // idle callback, which would block the entire event loop and delay both
      // pipe reads and the timeout timer.
      // CPU overhead: ~20 lightweight syscalls per second (waitpid/wait_for),
      // which is negligible even on the busiest node.
      auto poll_timer = loop->resource<uvw::timer_handle>();

      std::string script_output;

      // -----------------------------------------------------------------------
      // Completion state
      //
      // We need TWO independent events before it is safe to inspect results:
      //
      //   child_exited  – poll_timer detected the child has terminated and
      //                   stored the raw waitpid() status in exit_status.
      //
      //   pipe_ended    – end_event fired, meaning the kernel has flushed all
      //                   buffered pipe data to us via data_event callbacks.
      //
      // Checking child_exited alone would risk processing a truncated
      // script_output if pipe data was still in flight.  Checking pipe_ended
      // alone would miss the exit status.  try_finish() gates on both.
      // -----------------------------------------------------------------------
      bool child_exited = false;
      bool pipe_ended = false;
      bool has_error = false;      // fatal error; suppress normal completion
      int exit_status = 0;         // raw waitpid() status (not just exit code)
      bool loop_stopping = false;  // guard against double-closing handles

      // --- stop_loop
      // ----------------------------------------------------------- Close all
      // active handles.  Once every handle is closed, libuv exits loop->run()
      // automatically.  Idempotent via loop_stopping guard.
      // -------------------------------------------------------------------------
      auto stop_loop = [&]() {
        if (loop_stopping) return;
        loop_stopping = true;
        pipe_handle->close();
        timeout_timer->close();
        poll_timer->close();
      };

      // --- try_finish
      // ---------------------------------------------------------- Called from
      // both poll_timer and end_event callbacks.  Proceeds only when both
      // events have fired so that script_output is complete and exit_status is
      // valid.
      //
      // On success, appends output and tail-calls run_next_script(), which
      // creates a NEW event loop internally.  This is safe because stop_loop()
      // has already closed all handles on the current loop before we call
      // run_next_script(); the outer loop->run() therefore returns as soon as
      // the current callback completes.  Stack depth grows by one frame per
      // script, which is acceptable for the small number of prolog/epilog
      // scripts typically configured (< 10).
      // -------------------------------------------------------------------------
      auto try_finish = [&]() {
        if (!child_exited || !pipe_ended) return;

        // Shut down the current event loop before (possibly) entering a new
        // one.
        stop_loop();
        if (has_error) return;

        // Decode the raw waitpid() status using standard POSIX macros.
        int exit_code = 0;
        int signal_num = 0;
        if (WIFEXITED(exit_status)) {
          exit_code = WEXITSTATUS(exit_status);
        } else if (WIFSIGNALED(exit_status)) {
          signal_num = WTERMSIG(exit_status);
        } else {
          // Unexpected status (e.g., stopped process); treat as error.
          exit_code = exit_status;
        }

        if (exit_code != 0 || signal_num != 0) {
          CRANE_TRACE(
              "Script '{}' failed (exit_code={}, signal={}), output: {}.",
              script, exit_code, signal_num, script_output);
          result =
              std::unexpected(RunPrologEpilogStatus{.exit_code = exit_code,
                                                    .signal_num = signal_num});
        } else {
          // Script succeeded; queue the next one.
          output += script_output;
          script_idx++;
          run_next_script();
        }
      };

      // -----------------------------------------------------------------------
      // Initialise the pipe handle and associate it with the OS pipe fd.
      // -----------------------------------------------------------------------
      int err = pipe_handle->init();
      if (err) {
        CRANE_ERROR("Failed to init pipe_handle for '{}': {}", script,
                    uv_strerror(err));
        result = std::unexpected(
            RunPrologEpilogStatus{.exit_code = 1, .signal_num = 0});
        close(stdout_pipe[0]);
        return;
      }

      err = pipe_handle->open(stdout_pipe[0]);
      if (err) {
        CRANE_ERROR("Failed to open pipe_handle for '{}': {}", script,
                    uv_strerror(err));
        result = std::unexpected(
            RunPrologEpilogStatus{.exit_code = 1, .signal_num = 0});
        close(stdout_pipe[0]);
        return;
      }

      // -----------------------------------------------------------------------
      // Pipe: data event
      // Append incoming bytes to script_output, honouring the caller-supplied
      // size cap.  Excess bytes are silently discarded to bound memory use.
      // -----------------------------------------------------------------------
      pipe_handle->on<uvw::data_event>([&](const uvw::data_event& event,
                                           uvw::pipe_handle&) {
        if (event.length > 0) {
          const size_t remain = args.output_size > script_output.size()
                                    ? args.output_size - script_output.size()
                                    : 0;
          if (remain > 0)
            script_output.append(event.data.get(),
                                 std::min<size_t>(event.length, remain));
        }
      });

      // -----------------------------------------------------------------------
      // Pipe: end event (EOF)
      // The kernel delivers this only after the child has closed its write end
      // (i.e., after it has exited or explicitly closed stdout/stderr).
      // All data_event callbacks for this pipe have already been delivered by
      // the time end_event fires, so script_output is complete here.
      // -----------------------------------------------------------------------
      pipe_handle->on<uvw::end_event>(
          [&](const uvw::end_event&, uvw::pipe_handle& h) {
            h.close();  // release the handle
            pipe_ended = true;
            try_finish();  // proceed if child has also exited
          });

      // -----------------------------------------------------------------------
      // Pipe: error event
      // Any I/O error on the read end is treated as a fatal failure.
      // All handles are closed so the event loop terminates promptly.
      // -----------------------------------------------------------------------
      pipe_handle->on<uvw::error_event>(
          [&](uvw::error_event& e, uvw::pipe_handle&) {
            CRANE_WARN("Pipe error for script '{}'({}): {}.", script, pid,
                       e.what());
            has_error = true;
            result = std::unexpected(
                RunPrologEpilogStatus{.exit_code = 1, .signal_num = 0});
            stop_loop();  // also closes timeout_timer and poll_timer
          });

      pipe_handle->read();

      // -----------------------------------------------------------------------
      // Timeout timer (one-shot)
      // Fires once after the remaining time budget has elapsed.
      // Sends SIGKILL to the child's entire process group, then closes
      // itself.  The poll_timer is intentionally left running so we can
      // still detect the child's eventual exit and drain the pipe cleanly.
      // -----------------------------------------------------------------------
      timeout_timer->on<uvw::timer_event>([&](const uvw::timer_event&,
                                              uvw::timer_handle& h) {
        CRANE_TRACE("Script '{}' timed out; killing process group {}.", script,
                    pid);
        KillPg(pid);
        h.close();  // close only the timeout timer; poll_timer keeps running
      });

      // -----------------------------------------------------------------------
      // Poll timer (repeating, 50 ms interval)
      // Periodically checks whether the child process has exited.
      //
      // Why a timer instead of idle + sleep?
      //   idle + sleep_for(50ms) blocks the entire event loop thread, which
      //   prevents libuv from processing pipe data or firing the timeout timer
      //   during the sleep window.  A repeating timer is non-blocking: libuv
      //   sleeps efficiently in epoll_wait() between timer ticks, and other
      //   I/O events (pipe reads, timeout) are handled in between.
      //
      // CPU overhead estimation:
      //   20 ticks/s × 1 waitpid (or wait_for) syscall × ~1 µs each ≈ 0.002%
      //   of a single CPU core.  Negligible even under heavy load.
      // -----------------------------------------------------------------------
      poll_timer->on<uvw::timer_event>(
          [&](const uvw::timer_event&, uvw::timer_handle& h) {
            bool exited = false;

            if (use_watcher) {
              // Non-blocking check of the shared future.
              // TryDeliver() MUST call promise::set_value() with the raw
              // waitpid() status (not a processed exit code), so that the
              // WIFEXITED / WEXITSTATUS macros in try_finish() work correctly.
              if (exit_future.wait_for(milliseconds(0)) ==
                  std::future_status::ready) {
                exit_status = exit_future.get();
                exited = true;
              }
            } else {
              // WNOHANG: return immediately if no child has changed state.
              //   rc == pid  → child exited; exit_status is valid
              //   rc == 0    → child still running; try again next tick
              //   rc == -1   → error (e.g., child already reaped elsewhere)
              int rc = waitpid(pid, &exit_status, WNOHANG);
              if (rc == pid) {
                exited = true;
              } else if (rc == -1) {
                CRANE_ERROR("waitpid failed for script '{}' pid={}: {}", script,
                            pid, strerror(errno));
                has_error = true;
                result = std::unexpected(
                    RunPrologEpilogStatus{.exit_code = 1, .signal_num = 0});
                stop_loop();
                return;
              }
              // rc == 0: child still running; do nothing and wait for next tick
            }

            if (exited) {
              // Stop polling; child will not exit again.
              h.close();
              child_exited = true;
              // Attempt completion; deferred if the pipe has not yet signalled
              // EOF.
              try_finish();
            }
          });

      // -----------------------------------------------------------------------
      // Pre-flight timeout check
      // If previous scripts consumed all of the time budget, skip even
      // starting the event loop and kill the child we just forked.
      // -----------------------------------------------------------------------
      const auto elapsed =
          duration_cast<milliseconds>(steady_clock::now() - start_time);
      if (elapsed >= timeout) {
        CRANE_TRACE("Script '{}' timed out before its event loop started.",
                    script);
        KillPg(pid);

        // Reap the child and capture its true exit status to avoid leaving
        // a zombie process.
        int raw_status = 0;
        if (use_watcher) {
          // The fork_and_watch_fn hook is responsible for calling waitpid();
          // block on the future to ensure the child is fully reaped before
          // we return.
          raw_status = exit_future.get();
        } else {
          // Plain fork() path: we must reap the child ourselves.
          waitpid(pid, &raw_status, 0);
        }

        // Decode the raw waitpid() status (same logic as try_finish()).
        int exit_code = 0;
        int signal_num = 0;
        if (WIFEXITED(raw_status)) {
          exit_code = WEXITSTATUS(raw_status);
        } else if (WIFSIGNALED(raw_status)) {
          signal_num = WTERMSIG(raw_status);
        } else {
          exit_code = raw_status;  // unexpected status, treat as error
        }

        CRANE_TRACE("Script '{}' timed out (exit_code={}, signal={}).", script,
                    exit_code, signal_num);
        pipe_handle->close();
        result =
            std::unexpected(RunPrologEpilogStatus{.exit_code = exit_code,
                                                  .signal_num = signal_num});
        return;
      }

      // Start the one-shot timeout timer for the remaining time budget.
      timeout_timer->start(timeout - elapsed, milliseconds(0));

      // Start the repeating poll timer.  First tick after 50 ms; repeats
      // every 50 ms thereafter.
      poll_timer->start(milliseconds(50), milliseconds(50));

      // Block until all handles have been closed (i.e., stop_loop() was called
      // or every handle reached its natural end state).
      loop->run();
    }
  };

  // Kick off the recursive script execution chain.
  run_next_script();

  return result;
}

void ApplyPrologOutputToEnvAndStdout(
    const std::string& output,
    std::unordered_map<std::string, std::string>* env_map, int job_stdout_fd) {
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
      write(job_stdout_fd, to_print.data(), to_print.size());
      write(job_stdout_fd, "\n", 1);
    }
  }
}

}  // namespace util::os

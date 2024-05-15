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

#include "TaskExecutor.h"

#include <absl/strings/str_split.h>
#include <fcntl.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/util/delimited_message_util.h>
#include <sys/stat.h>

#include <filesystem>
#include <nlohmann/json.hpp>
#include <ranges>

#include "CranedPublicDefs.h"
#include "TaskManager.h"
#include "crane/Logger.h"
#include "crane/OS.h"
#include "protos/CraneSubprocess.pb.h"

namespace Craned {

/**
 * Generate CRANE_* vars using task information and
 * extract task.env() from frontend.
 */
TaskExecutor::EnvironVars TaskExecutor::GetEnvironVarsFromTask(
    const TaskInstance& instance) {
  TaskExecutor::EnvironVars env_vec{};
  env_vec.emplace_back("CRANE_JOB_NODELIST",
                       absl::StrJoin(instance.task.allocated_nodes(), ";"));
  env_vec.emplace_back("CRANE_EXCLUDES",
                       absl::StrJoin(instance.task.excludes(), ";"));
  env_vec.emplace_back("CRANE_JOB_NAME", instance.task.name());
  env_vec.emplace_back("CRANE_ACCOUNT", instance.task.account());
  env_vec.emplace_back("CRANE_PARTITION", instance.task.partition());
  env_vec.emplace_back("CRANE_QOS", instance.task.qos());
  env_vec.emplace_back("CRANE_MEM_PER_NODE",
                       std::to_string(instance.task.resources()
                                          .allocatable_resource()
                                          .memory_limit_bytes() /
                                      (1024 * 1024)));
  env_vec.emplace_back("CRANE_JOB_ID", std::to_string(instance.task.task_id()));

  int64_t time_limit_sec = instance.task.time_limit().seconds();
  int hours = time_limit_sec / 3600;
  int minutes = (time_limit_sec % 3600) / 60;
  int seconds = time_limit_sec % 60;
  std::string time_limit =
      fmt::format("{:0>2}:{:0>2}:{:0>2}", hours, minutes, seconds);
  env_vec.emplace_back("CRANE_TIMELIMIT", time_limit);

  // Add env from user
  auto& env_from_user = instance.task.env();
  for (auto&& [name, value] : env_from_user) {
    env_vec.emplace_back(name, value);
  }

  return env_vec;
}

std::string ProcessInstance::WriteBatchScript(const std::string_view script) {
  m_executive_path_ =
      fmt::format("{}/Crane-{}.sh", g_config.CranedScriptDir, m_meta_.id);

  FILE* fptr = fopen(m_executive_path_.c_str(), "w");
  if (fptr == nullptr) return "";
  fputs(script.data(), fptr);
  fclose(fptr);

  chmod(m_executive_path_.c_str(), strtol("0755", nullptr, 8));

  return m_executive_path_;
}

CraneErr ProcessInstance::Spawn(util::Cgroup* cgroup) {
  using google::protobuf::io::FileInputStream;
  using google::protobuf::io::FileOutputStream;
  using google::protobuf::util::ParseDelimitedFromZeroCopyStream;
  using google::protobuf::util::SerializeDelimitedToZeroCopyStream;

  using crane::grpc::subprocess::CanStartMessage;
  using crane::grpc::subprocess::ChildProcessReady;

  int socket_pair[2];

  if (socketpair(AF_UNIX, SOCK_STREAM, 0, socket_pair) != 0) {
    CRANE_ERROR("Failed to create socket pair: {}", strerror(errno));
    return CraneErr::kSystemErr;
  }

  // save the current uid/gid
  std::pair<uid_t, gid_t> saved_priv{getuid(), getgid()};

  int rc = setegid(m_meta_.pwd.Gid());
  if (rc == -1) {
    CRANE_ERROR("error: setegid. {}", strerror(errno));
    return CraneErr::kSystemErr;
  }
  __gid_t gid_a[1] = {m_meta_.pwd.Gid()};
  setgroups(1, gid_a);
  rc = seteuid(m_meta_.pwd.Uid());
  if (rc == -1) {
    CRANE_ERROR("error: seteuid. {}", strerror(errno));
    return CraneErr::kSystemErr;
  }

  pid_t child_pid = fork();
  if (child_pid > 0) {  // Parent proc
    close(socket_pair[1]);
    int fd = socket_pair[0];
    bool ok;
    CraneErr err;

    setegid(saved_priv.second);
    seteuid(saved_priv.first);
    setgroups(0, nullptr);

    FileInputStream istream(fd);
    FileOutputStream ostream(fd);
    CanStartMessage msg;
    ChildProcessReady child_process_ready;

    CRANE_DEBUG("Subprocess was created for task #{} pid: {}", m_meta_.id,
                child_pid);

    SetPid(child_pid);

    // Add event for stdout/stderr of the new subprocess
    // struct bufferevent* ev_buf_event;
    // ev_buf_event =
    //     bufferevent_socket_new(m_ev_base_, fd, BEV_OPT_CLOSE_ON_FREE);
    // if (!ev_buf_event) {
    //   CRANE_ERROR(
    //       "Error constructing bufferevent for the subprocess of task #!",
    //       instance->task.task_id());
    //   err = CraneErr::kLibEventError;
    //   goto AskChildToSuicide;
    // }
    // bufferevent_setcb(ev_buf_event, EvSubprocessReadCb_, nullptr, nullptr,
    //                   (void*)process.get());
    // bufferevent_enable(ev_buf_event, EV_READ);
    // bufferevent_disable(ev_buf_event, EV_WRITE);
    // process->SetEvBufEvent(ev_buf_event);

    // Migrate the new subprocess to newly created cgroup
    if (!cgroup->MigrateProcIn(GetPid())) {
      CRANE_ERROR(
          "Terminate the subprocess of task #{} due to failure of cgroup "
          "migration.",
          m_meta_.id);

      err = CraneErr::kCgroupError;
      goto AskChildToSuicide;
    }

    CRANE_TRACE("New task #{} is ready. Asking subprocess to execv...",
                m_meta_.id);

    // Tell subprocess that the parent process is ready. Then the
    // subprocess should continue to exec().
    msg.set_ok(true);
    ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);
    ok &= ostream.Flush();
    if (!ok) {
      CRANE_ERROR("Failed to send ok=true to subprocess {} for task #{}",
                  child_pid, m_meta_.id);
      close(fd);
      return CraneErr::kProtobufError;
    }

    ParseDelimitedFromZeroCopyStream(&child_process_ready, &istream, nullptr);
    if (!msg.ok()) {
      CRANE_ERROR("Failed to read protobuf from subprocess {} of task #{}",
                  child_pid, m_meta_.id);
      close(fd);
      return CraneErr::kProtobufError;
    }

    close(fd);
    return CraneErr::kOk;

  AskChildToSuicide:
    msg.set_ok(false);

    ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);
    close(fd);
    if (!ok) {
      CRANE_ERROR("Failed to ask subprocess {} to suicide for task #{}",
                  child_pid, m_meta_.id);
      return CraneErr::kProtobufError;
    }
    return err;
  } else {  // Child proc
    rc = chdir(m_cwd_.c_str());
    if (rc == -1) {
      CRANE_ERROR("[Child Process] Error: chdir to {}. {}", m_cwd_.c_str(),
                  strerror(errno));
      std::abort();
    }

    setreuid(m_meta_.pwd.Uid(), m_meta_.pwd.Uid());
    setregid(m_meta_.pwd.Gid(), m_meta_.pwd.Gid());

    // Set pgid to the pid of task root process.
    setpgid(0, 0);

    close(socket_pair[0]);
    int fd = socket_pair[1];

    FileInputStream istream(fd);
    FileOutputStream ostream(fd);
    CanStartMessage msg;
    ChildProcessReady child_process_ready;
    bool ok;

    ParseDelimitedFromZeroCopyStream(&msg, &istream, nullptr);
    if (!msg.ok()) std::abort();

    const std::string& stdout_file_path =
        m_batch_meta_.parsed_output_file_pattern;
    const std::string& stderr_file_path =
        m_batch_meta_.parsed_error_file_pattern;

    int stdout_fd =
        open(stdout_file_path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
    if (stdout_fd == -1) {
      CRANE_ERROR("[Child Process] Error: open {}. {}", stdout_file_path,
                  strerror(errno));
      std::abort();
    }
    dup2(stdout_fd, 1);  // stdout -> output file

    if (stderr_file_path.empty()) {
      // if stderr filename is not specified
      dup2(stdout_fd, 2);  // stderr -> output file
    } else {
      int stderr_fd =
          open(stderr_file_path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
      if (stderr_fd == -1) {
        CRANE_ERROR("[Child Process] Error: open {}. {}", stderr_file_path,
                    strerror(errno));
        std::abort();
      }
      dup2(stderr_fd, 2);  // stderr -> error file
      close(stderr_fd);
    }

    close(stdout_fd);

    child_process_ready.set_ok(true);
    ok = SerializeDelimitedToZeroCopyStream(child_process_ready, &ostream);
    ok &= ostream.Flush();
    if (!ok) {
      CRANE_ERROR("[Child Process] Error: Failed to flush.");
      std::abort();
    }

    close(fd);

    // If these file descriptors are not closed, a program like mpirun may
    // keep waiting for the input from stdin or other fds and will never end.
    close(0);  // close stdin
    util::os::CloseFdFrom(3);

    // Use bash to create the executing environment
    std::vector<const char*> argv{"/bin/bash"};

    // If get_user_env, will load envs using --login
    // Maybe should use `su` instead.
    if (m_meta_.get_user_env) {
      // If --get-user-env is set, the new environment is inherited
      // from the execution CraneD rather than the submitting node.
      //
      // Since we want to reinitialize the environment variables of the user
      // by reloading the settings in something like .bashrc or /etc/profile,
      // we are actually performing two steps: login -> start shell.
      // Shell starting is done by calling "bash --login".
      //
      // During shell starting step, the settings in
      // /etc/profile, ~/.bash_profile, ... are loaded.
      //
      // During login step, "HOME" and "SHELL" are set.
      // Here we are just mimicking the login module.

      // Slurm uses `su <username> -c /usr/bin/env` to retrieve
      // all the environment variables.
      // We use a more tidy way.
      m_env_.emplace_back("HOME", m_meta_.pwd.HomeDir());
      m_env_.emplace_back("SHELL", m_meta_.pwd.Shell());
      argv.emplace_back("--login");
    }

    // Set environment variables
    if (clearenv()) {
      fmt::print("clearenv() failed!\n");
    }

    for (const auto& [name, value] : m_env_) {
      if (setenv(name.c_str(), value.c_str(), 1)) {
        fmt::print("setenv for {}={} failed!\n", name, value);
      }
    }

    // Add arguments
    // TODO: Arguments for the interpreter or executive?
    std::string arguments{};
    for (const auto& arg : m_arguments_) {
      arguments += arg + " ";
    }

    // e.g., /bin/bash -c "/bin/bash script.sh --arg1 --arg2 ..."
    argv.emplace_back("-c");
    argv.emplace_back(fmt::format("\"{} {} {}\"", m_interpreter_,
                                  m_executive_path_, arguments)
                          .c_str());
    argv.emplace_back(nullptr);

    execv(argv[0], const_cast<char* const*>(argv.data()));

    // Error occurred since execv returned. At this point, errno is set.
    // Ctld use SIGABRT to inform the client of this failure.
    fmt::print(stderr, "[Craned Subprocess Error] Failed to execv. Error: {}\n",
               strerror(errno));
    // Todo: See https://tldp.org/LDP/abs/html/exitcodes.html, return standard
    //  exit codes
    abort();
  }
}

CraneErr ProcessInstance::Kill(int signum) {
  // TODO: Add timer which sends SIGTERM for those tasks who
  //  will not quit when receiving SIGINT.
  if (m_pid_) {
    // Send the signal to the whole process group.
    int err = kill(-m_pid_, signum);

    if (err == 0)
      return CraneErr::kOk;
    else {
      CRANE_TRACE("kill pid {} failed. error: {}", m_pid_, strerror(errno));
      return CraneErr::kGenericFailure;
    }
  }

  return CraneErr::kNonExistent;
}

TaskExecutor::ChldStatus ProcessInstance::CheckChldStatus(pid_t pid,
                                                          int status) {
  ChldStatus chld_status{};

  if (WIFEXITED(status)) {
    // Exited with status WEXITSTATUS(status)
    chld_status = {pid, false, WEXITSTATUS(status)};
    CRANE_TRACE("Receiving SIGCHLD for pid {}. Signaled: false, Status: {}",
                pid, WEXITSTATUS(status));

  } else if (WIFSIGNALED(status)) {
    // Killed by signal WTERMSIG(status)
    chld_status = {pid, true, WTERMSIG(status)};
    CRANE_TRACE("Receiving SIGCHLD for pid {}. Signaled: true, Signal: {}", pid,
                WTERMSIG(status));
  }

  return chld_status;
}

CraneErr ContainerInstance::ModifyBundleConfig_(const std::string& src,
                                                const std::string& dst) {
  using json = nlohmann::json;

  // Check if bundle is valid
  auto src_config = std::filesystem::path(src) / "config.json";
  auto src_rootfs = std::filesystem::path(src) / "rootfs";
  if (!std::filesystem::exists(src_config) ||
      !std::filesystem::exists(src_rootfs)) {
    CRANE_ERROR("Bundle provided by Task #{} not exists : {}", m_meta_.id,
                src_config.string());
    return CraneErr::kInvalidParam;
  }

  std::ifstream fin{src_config};
  if (!fin) {
    CRANE_ERROR("Failed to open bundle config provided by Task #{}: {}",
                m_meta_.id, src_config.string());
    return CraneErr::kSystemErr;
  }

  json config = json::parse(fin, nullptr, false);
  if (config.is_discarded()) {
    CRANE_ERROR("Bundle config provided by Task #{} is invalid: {}", m_meta_.id,
                src_config.string());
    return CraneErr::kInvalidParam;
  }

  try {
    // Set root object, see:
    // https://github.com/opencontainers/runtime-spec/blob/main/config.md#root
    // Set real rootfs path in the modified config.
    config["root"]["path"] = src_rootfs;

    // Set mounts array, see:
    // https://github.com/opencontainers/runtime-spec/blob/main/config.md#mounts
    // Bind mount script into the container
    auto mounts = config["mounts"].get<std::vector<json>>();
    std::string mounted_executive = "/tmp/crane/script.sh";
    mounts.emplace_back(json::object({
        {"destination", mounted_executive},
        {"source", m_executive_path_},
        {"options", json::array({"bind", "ro"})},
    }));
    config["mounts"] = mounts;

    // Set process object, see:
    // https://github.com/opencontainers/runtime-spec/blob/main/config.md#process
    auto& process = config["process"];
    // Set pass-through mode, see runc docs for detail.
    process["terminal"] = false;
    // Write environment variables in IEEE format.
    process["env"] = [this]() {
      // TODO: Will discard `--get-user-env`
      auto env_str = std::vector<std::string>();
      for (const auto& [name, value] : m_env_)
        env_str.emplace_back(fmt::format("{}={}", name, value));
      return env_str;
    }();
    // TODO: Support more arguments.
    process["args"] = {
        m_interpreter_,
        mounted_executive,
    };
  } catch (json::exception& e) {
    CRANE_ERROR("Failed to generate bundle config for Task #{}: {}", m_meta_.id,
                e.what());
    return CraneErr::kGenericFailure;
  }

  // Write the modified config
  auto dst_config = std::filesystem::path(dst) / "config.json";
  std::ofstream fout{dst_config};
  if (!fout) {
    CRANE_ERROR("Failed to write bundle config for Task #{}: {}", m_meta_.id,
                dst_config.string());
    return CraneErr::kSystemErr;
  }
  fout << config.dump(4);
  fout.flush();

  return CraneErr::kOk;
}

std::string ContainerInstance::WriteBatchScript(const std::string_view script) {
  // Create temp folder
  if (AssureContainerTempDir_() != CraneErr::kOk) return "";

  // Write into the temp folder
  m_executive_path_ = std::filesystem::path(m_temp_path_) /
                      fmt::format("Crane-{}.sh", m_meta_.id);

  FILE* fptr = fopen(m_executive_path_.c_str(), "w");
  if (fptr == nullptr) return "";
  fputs(script.data(), fptr);
  fclose(fptr);

  chmod(m_executive_path_.c_str(), strtol("0755", nullptr, 8));

  return m_executive_path_;
}

CraneErr ContainerInstance::Spawn(util::Cgroup* cgroup) {
  using google::protobuf::io::FileInputStream;
  using google::protobuf::io::FileOutputStream;
  using google::protobuf::util::ParseDelimitedFromZeroCopyStream;
  using google::protobuf::util::SerializeDelimitedToZeroCopyStream;

  using crane::grpc::subprocess::CanStartMessage;
  using crane::grpc::subprocess::ChildProcessReady;

  int socket_pair[2];

  if (socketpair(AF_UNIX, SOCK_STREAM, 0, socket_pair) != 0) {
    CRANE_ERROR("Failed to create socket pair: {}", strerror(errno));
    return CraneErr::kSystemErr;
  }

  // save the current uid/gid
  std::pair<uid_t, gid_t> saved_id{getuid(), getgid()};

  int rc = setegid(m_meta_.pwd.Gid());
  if (rc == -1) {
    CRANE_ERROR("error: setegid. {}", strerror(errno));
    return CraneErr::kSystemErr;
  }
  __gid_t gid_a[1] = {m_meta_.pwd.Gid()};
  setgroups(1, gid_a);
  rc = seteuid(m_meta_.pwd.Uid());
  if (rc == -1) {
    CRANE_ERROR("error: seteuid. {}", strerror(errno));
    return CraneErr::kSystemErr;
  }

  pid_t child_pid = fork();
  if (child_pid > 0) {  // Parent proc
    close(socket_pair[1]);
    int fd = socket_pair[0];
    bool ok;
    CraneErr err;

    setegid(saved_id.second);
    seteuid(saved_id.first);
    setgroups(0, nullptr);

    FileInputStream istream(fd);
    FileOutputStream ostream(fd);
    CanStartMessage msg;
    ChildProcessReady child_process_ready;

    CRANE_DEBUG("Subprocess was created for task #{} pid: {}", m_meta_.id,
                child_pid);

    SetPid(child_pid);

    // Add event for stdout/stderr of the new subprocess
    // struct bufferevent* ev_buf_event;
    // ev_buf_event =
    //     bufferevent_socket_new(m_ev_base_, fd, BEV_OPT_CLOSE_ON_FREE);
    // if (!ev_buf_event) {
    //   CRANE_ERROR(
    //       "Error constructing bufferevent for the subprocess of task #!",
    //       instance->task.task_id());
    //   err = CraneErr::kLibEventError;
    //   goto AskChildToSuicide;
    // }
    // bufferevent_setcb(ev_buf_event, EvSubprocessReadCb_, nullptr, nullptr,
    //                   (void*)process.get());
    // bufferevent_enable(ev_buf_event, EV_READ);
    // bufferevent_disable(ev_buf_event, EV_WRITE);
    // process->SetEvBufEvent(ev_buf_event);

    // Migrate the new subprocess to newly created cgroup
    if (!cgroup->MigrateProcIn(GetPid())) {
      CRANE_ERROR(
          "Terminate the subprocess of task #{} due to failure of cgroup "
          "migration.",
          m_meta_.id);

      err = CraneErr::kCgroupError;
      goto AskChildToSuicide;
    }

    CRANE_TRACE("New task #{} is ready. Asking subprocess to execv...",
                m_meta_.id);

    // Tell subprocess that the parent process is ready. Then the
    // subprocess should continue to exec().
    msg.set_ok(true);
    ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);
    ok &= ostream.Flush();
    if (!ok) {
      CRANE_ERROR("Failed to send ok=true to subprocess {} for task #{}",
                  child_pid, m_meta_.id);
      close(fd);
      return CraneErr::kProtobufError;
    }

    ParseDelimitedFromZeroCopyStream(&child_process_ready, &istream, nullptr);
    if (!msg.ok()) {
      CRANE_ERROR("Failed to read protobuf from subprocess {} of task #{}",
                  child_pid, m_meta_.id);
      close(fd);
      return CraneErr::kProtobufError;
    }

    close(fd);
    return CraneErr::kOk;

  AskChildToSuicide:
    msg.set_ok(false);

    ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);
    close(fd);
    if (!ok) {
      CRANE_ERROR("Failed to ask subprocess {} to suicide for task #{}",
                  child_pid, m_meta_.id);
      return CraneErr::kProtobufError;
    }
    return err;
  } else {  // Child proc
    // Change work dir to execute
    rc = chdir(m_cwd_.c_str());
    if (rc == -1) {
      fmt::print("[Child Process] Error: chdir to {}. {}", m_cwd_.c_str(),
                 strerror(errno));
      std::abort();
    }

    setreuid(m_meta_.pwd.Uid(), m_meta_.pwd.Uid());
    setregid(m_meta_.pwd.Gid(), m_meta_.pwd.Gid());

    // Set pgid to the pid of task root process.
    setpgid(0, 0);

    close(socket_pair[0]);
    int fd = socket_pair[1];

    FileInputStream istream(fd);
    FileOutputStream ostream(fd);
    CanStartMessage msg;
    ChildProcessReady child_process_ready;
    bool ok;

    ParseDelimitedFromZeroCopyStream(&msg, &istream, nullptr);
    if (!msg.ok()) std::abort();

    const std::string& stdout_file_path =
        m_batch_meta_.parsed_output_file_pattern;
    const std::string& stderr_file_path =
        m_batch_meta_.parsed_error_file_pattern;

    int stdout_fd =
        open(stdout_file_path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
    if (stdout_fd == -1) {
      fmt::print("[Child Process] Error: open {}. {}", stdout_file_path,
                 strerror(errno));
      std::abort();
    }
    dup2(stdout_fd, 1);  // stdout -> output file

    if (stderr_file_path.empty()) {
      // if stderr filename is not specified
      dup2(stdout_fd, 2);  // stderr -> output file
    } else {
      int stderr_fd =
          open(stderr_file_path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
      if (stderr_fd == -1) {
        fmt::print("[Child Process] Error: open {}. {}", stderr_file_path,
                   strerror(errno));
        std::abort();
      }
      dup2(stderr_fd, 2);  // stderr -> error file
      close(stderr_fd);
    }

    close(stdout_fd);

    child_process_ready.set_ok(true);
    ok = SerializeDelimitedToZeroCopyStream(child_process_ready, &ostream);
    ok &= ostream.Flush();
    if (!ok) {
      fmt::print("[Child Process] Error: Failed to flush.");
      std::abort();
    }

    close(fd);

    // If these file descriptors are not closed, a program like mpirun may
    // keep waiting for the input from stdin or other fds and will never end.
    close(0);  // close stdin
    util::os::CloseFdFrom(3);

    // Generate modified bundle config.
    CraneErr err = ModifyBundleConfig_(m_bundle_path_, m_temp_path_);
    if (err != CraneErr::kOk) {
      fmt::print("[Child Process] Error: Failed to spawn container.");
      std::abort();
    }

    // Parse the pattern in run command.
    // we provide m_temp_path_ to runtime so that it would use the modified
    // config.json
    auto run_cmd = ParseContainerCmdPattern_(
        g_config.CranedContainer.RuntimeRun, m_meta_.id, m_meta_.name,
        m_meta_.pwd, m_temp_path_);

    std::vector<std::string> split = absl::StrSplit(std::move(run_cmd), " ");
    auto split_view =
        split | std::views::transform([](auto& s) { return s.c_str(); });
    auto argv = std::vector<const char*>(split_view.begin(), split_view.end());
    argv.push_back(nullptr);

    // Call OCI Runtime to run container.
    execv(argv[0], const_cast<char* const*>(argv.data()));

    // Error occurred since execv returned. At this point, errno is set.
    // Ctld use SIGABRT to inform the client of this failure.
    fmt::print(stderr, "[Craned Subprocess Error] Failed to execv. Error: {}\n",
               strerror(errno));
    // Todo: See https://tldp.org/LDP/abs/html/exitcodes.html, return standard
    // exit codes
    std::abort();
  }
}

CraneErr ContainerInstance::Kill(int signum) {
  using json = nlohmann::json;
  if (m_pid_) {
    // If m_pid_ not exists, no further operation.
    int rc = 0;
    std::array<char, 128> buffer;
    std::string cmd, ret, status;
    json jret;

    // Check the state of the container
    cmd = ParseContainerCmdPattern_(g_config.CranedContainer.RunTimeState,
                                    m_meta_.id, m_meta_.name, m_meta_.pwd,
                                    m_temp_path_);

    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"),
                                                  pclose);
    if (!pipe) {
      CRANE_TRACE("Error in getting container status: popen() failed.");
      goto ProcessKill;
    }
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe.get()) !=
           nullptr) {
      ret += buffer.data();
    }

    jret = json::parse(std::move(ret), nullptr, false);
    if (jret.is_discarded() || !jret.contains("status") ||
        !jret["status"].is_string()) {
      CRANE_TRACE("Error in parsing container status: {}", cmd);
      goto ProcessKill;
    }

    // Take action according to OCI container states, see:
    // https://github.com/opencontainers/runtime-spec/blob/main/runtime.md#state
    status = std::move(jret["status"]);
    if (status == "creating") {
      goto ProcessKill;
    } else if (status == "created") {
      goto ContainerDelete;
    } else if (status == "running") {
      goto ContainerKill;
    } else if (status == "stopped") {
      goto ContainerDelete;
    } else {
      CRANE_WARN("Unknown container status received: {}", status);
      goto ProcessKill;
    }

  ContainerKill:
    // Try to stop gracefully.
    // Note: Signum is configured in config.yaml instead of in the param.
    cmd = ParseContainerCmdPattern_(g_config.CranedContainer.RuntimeKill,
                                    m_meta_.id, m_meta_.name, m_meta_.pwd,
                                    m_temp_path_);
    rc = system(cmd.c_str());
    if (rc) {
      CRANE_TRACE("Failed to kill container for Task #{}: error in {}",
                  m_meta_.id, cmd);
    }

  ContainerDelete:
    // Delete the container
    // Note: Admin could choose if --force is configured or not.
    cmd = ParseContainerCmdPattern_(g_config.CranedContainer.RuntimeDelete,
                                    m_meta_.id, m_meta_.name, m_meta_.pwd,
                                    m_temp_path_);
    rc = system(cmd.c_str());
    if (rc) {
      CRANE_TRACE("Failed to delete container for Task #{}: error in {}",
                  m_meta_.id, cmd);
    }

  ProcessKill:
    // Kill runc process as the last resort.
    // Note: If runc is launched in `detached` mode, this will not work.
    rc = kill(-m_pid_, signum);
    if (rc && (errno != ESRCH)) {
      CRANE_TRACE("Failed to kill pid {}. error: {}", m_pid_, strerror(errno));
      return CraneErr::kGenericFailure;
    }

    return CraneErr::kOk;
  }

  return CraneErr::kNonExistent;
}

TaskExecutor::ChldStatus ContainerInstance::CheckChldStatus(pid_t pid,
                                                            int status) {
  ChldStatus chld_status{};

  if (WIFEXITED(status)) {
    // Exited with status WEXITSTATUS(status)
    chld_status = {pid, false, WEXITSTATUS(status)};
    if (chld_status.value > 128) {
      // Note: When cancel a container task,
      // the OCI runtime will signal the process inside the container.
      // In this case, WIFSIGNALED is false and WEXITSTATUS is set to `128 +
      // signum` (e.g., 128 + SIGTERM = 143).
      chld_status.signaled = true;
      chld_status.value -= 128;
      CRANE_TRACE(
          "Receiving SIGCHLD for pid {}. Signaled: true (in container), "
          "Signal: {}",
          pid, chld_status.value);
    } else {
      CRANE_TRACE("Receiving SIGCHLD for pid {}. Signaled: false, Status: {}",
                  pid, WEXITSTATUS(status));
    }
  } else if (WIFSIGNALED(status)) {
    // Killed by signal WTERMSIG(status)
    // Note: This could happen when the OCI runtime itself got killed.
    chld_status = {pid, true, WTERMSIG(status)};
    CRANE_TRACE("Receiving SIGCHLD for pid {}. Signaled: true, Signal: {}", pid,
                WTERMSIG(status));
  }

  return chld_status;
}

}  // namespace Craned
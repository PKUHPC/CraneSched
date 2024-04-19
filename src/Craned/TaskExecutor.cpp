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

#include <fcntl.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/util/delimited_message_util.h>
#include <sys/stat.h>

#include "crane/OS.h"
#include "protos/CraneSubprocess.pb.h"

namespace Craned {

CraneErr ProcessInstance::Spawn(util::Cgroup* cgroup, EnvironVars task_envs) {
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

    if (stderr_file_path.empty()) {  // if stderr filename is not specified
      dup2(stdout_fd, 2);            // stderr -> output file
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

    // Set environment variables related to task info (begin with `CRANE`).
    EnvironVars envs{std::move(task_envs)};

    // FIXME: --get-user-env
    // if (instance->task.get_user_env()) {
    if (true) {
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
      envs.emplace_back("HOME", m_meta_.pwd.HomeDir());
      envs.emplace_back("SHELL", m_meta_.pwd.Shell());
    }

    if (clearenv()) {
      fmt::print("clearenv() failed!\n");
    }

    for (const auto& [name, value] : envs) {
      if (setenv(name.c_str(), value.c_str(), 1)) {
        fmt::print("setenv for {}={} failed!\n", name, value);
      }
    }

    // Prepare the command line arguments.
    std::vector<const char*> argv;

    // Argv[0] is the program name which can be anything.
    argv.emplace_back("CraneScript");

    // FIXME: Check instance->task.get_user_env():
    // if (instance->task.get_user_env()) {
    if (true) {
      // If --get-user-env is specified,
      // we need to use --login option of bash to load settings from the user's
      // settings.
      argv.emplace_back("--login");
    }

    argv.emplace_back(m_executive_path_.c_str());
    for (auto&& arg : m_arguments_) {
      argv.push_back(arg.c_str());
    }
    argv.push_back(nullptr);

    execv("/bin/bash", const_cast<char* const*>(argv.data()));

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
  if (!m_pid_) {
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

CraneErr ContainerInstance::Spawn(util::Cgroup* cgroup,
                                  Craned::TaskExecutor::EnvironVars task_envs) {
  // TODO: Implement this
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

  // use the user's uid/gid
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

    // Parent proc use original uid/gid
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

    // Migrate the new subprocess to newly created cgroup
    if (!cgroup->MigrateProcIn(GetPid())) {
      CRANE_ERROR(
          "Terminate the subprocess of task #{} due to failure of cgroup "
          "migration.",
          m_meta_.id);

      err = CraneErr::kCgroupError;
      goto AskChildToSuicide;
    }

    CRANE_TRACE(
        "New task #{} is ready. Asking subprocess to launch the container...",
        m_meta_.id);

    // Tell subprocess that the parent process is ready. Then the
    // subprocess should continue to call OCI runtime.
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

    // FIXME: Add support for `cwd` in containers.
    // rc = chdir(m_cwd_.c_str());
    // if (rc == -1) {
    //   CRANE_ERROR("[Child Process] Error: chdir to {}. {}", m_cwd_.c_str(),
    //               strerror(errno));
    //   std::abort();
    // }

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

    auto out_file_path = m_batch_meta_.parsed_output_file_pattern;
    int out_fd = open(out_file_path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
    if (out_fd == -1) {
      CRANE_ERROR("[Child Process] Error: open {}. {}", out_file_path,
                  strerror(errno));
      std::abort();
    }

    dup2(out_fd, 1);  // stdout -> output file
    dup2(out_fd, 2);  // stderr -> output file
    close(out_fd);

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

    // Set environment variables related to task info (begin with `CRANE`).
    EnvironVars env_vec{std::move(task_envs)};

    // FIXME: Check instance->task.get_user_env():
    // env_vec.emplace_back("HOME", pwd_entry.HomeDir());
    // env_vec.emplace_back("SHELL", pwd_entry.Shell());

    if (clearenv()) {
      fmt::print("clearenv() failed!\n");
    }

    for (const auto& [name, value] : env_vec) {
      if (setenv(name.c_str(), value.c_str(), 1)) {
        fmt::print("setenv for {}={} failed!\n", name, value);
      }
    }

    // // Prepare the command line arguments.
    // std::vector<const char*> argv;

    // // Argv[0] is the program name which can be anything.
    // argv.emplace_back("CraneScript");

    // // FIXME: Check instance->task.get_user_env():
    // argv.emplace_back("--login");

    // argv.emplace_back(m_executive_path_.c_str());
    // for (auto&& arg : m_arguments_) {
    //   argv.push_back(arg.c_str());
    // }
    // argv.push_back(nullptr);

    // // TODO: Execute runc here
    // execv("/bin/bash", const_cast<char* const*>(argv.data()));

    // Error occurred since execv returned. At this point, errno is set.
    // Ctld use SIGABRT to inform the client of this failure.
    fmt::print(stderr, "[Craned Subprocess Error] Failed to execv. Error: {}\n",
               strerror(errno));
    // Todo: See https://tldp.org/LDP/abs/html/exitcodes.html, return standard
    //  exit codes
    abort();
  }

  return CraneErr::kOk;
}

CraneErr ContainerInstance::Kill(int signum) {
  // TODO: Implement this
  return CraneErr::kOk;
}

}  // namespace Craned
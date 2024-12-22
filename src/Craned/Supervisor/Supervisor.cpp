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
#include "SupervisorPublicDefs.h"
// Precompiled header comes first.

#include <google/protobuf/util/delimited_message_util.h>

#include <cxxopts.hpp>

#include "CforedClient.h"
#include "CranedClient.h"
#include "SupervisorServer.h"
#include "TaskManager.h"
#include "crane/PasswordEntry.h"
#include "crane/PluginClient.h"

using Supervisor::g_config;

void InitFromStdin(int argc, char** argv) {
  cxxopts::Options options("Supervisor");

  // clang-format off
  options.add_options()
      ("v,version", "Display version information")
      ("h,help", "Display help for Supervisor")
      ;
  // clang-format on

  cxxopts::ParseResult parsed_args;
  try {
    parsed_args = options.parse(argc, argv);
  } catch (cxxopts::OptionException& e) {
    fmt::print(stderr, "{}\n{}", e.what(), options.help());
    std::exit(1);
  }

  if (parsed_args.count("help") > 0) {
    fmt::print("{}\n", options.help());
    std::exit(0);
  }

  if (parsed_args.count("version") > 0) {
    fmt::print("Version: {}\n", CRANE_VERSION_STRING);
    fmt::print("Build Time: {}\n", CRANE_BUILD_TIMESTAMP);
    std::exit(0);
  }

  using google::protobuf::io::FileInputStream;
  using google::protobuf::util::ParseDelimitedFromZeroCopyStream;

  auto istream = FileInputStream(STDIN_FILENO);
  crane::grpc::InitSupervisorRequest msg;
  auto ok = ParseDelimitedFromZeroCopyStream(&msg, &istream, nullptr);
  if (!ok || !msg.ok()) {
    std::abort();
  }
  g_config.TaskId = msg.task_id();
  g_config.SupervisorDebugLevel = msg.debug_level();
  g_config.SupervisorLogFile =
      g_config.CraneBaseDir + fmt::format("Supervisor/{}.log", g_config.TaskId);

  g_config.CranedUnixSocketPath = msg.craned_unix_socket_path();

  auto log_level = StrToLogLevel(g_config.SupervisorDebugLevel);
  if (log_level.has_value()) {
    InitLogger(log_level.value(), g_config.SupervisorLogFile, false);
  } else {
    ok = false;
  }

  if (!ok) {
    using google::protobuf::io::FileOutputStream;
    using google::protobuf::util::SerializeDelimitedToZeroCopyStream;
    auto ostream = FileOutputStream(STDOUT_FILENO);
    crane::grpc::SupervisorReady msg;
    msg.set_ok(ok);
    SerializeDelimitedToZeroCopyStream(msg, &ostream);
    ostream.Flush();
    std::abort();
  }
}

void CreatePidFile() {
  pid_t pid = getpid();
  auto pid_file_path =
      Supervisor::kSupervisorPidFileDir /
      std::filesystem::path(fmt::format("supervisor_{}.pid", g_config.TaskId));
  if (std::filesystem::exists(pid_file_path)) {
    std::ifstream pid_file(pid_file_path);
    pid_t existing_pid;
    pid_file >> existing_pid;

    if (kill(existing_pid, 0) == 0) {
      CRANE_TRACE("Supervisor is already running with PID: {}", existing_pid);
      std::exit(1);
    } else {
      CRANE_TRACE("Stale PID file detected. Cleaning up.");
      std::filesystem::remove(pid_file_path);
    }
  }
  std::ofstream pid_file(pid_file_path, std::ios::out | std::ios::trunc);
  if (!pid_file) {
    CRANE_TRACE("Failed to create PID file: {}", pid_file_path);
    std::exit(1);
  }
  pid_file << pid << std::endl;
  pid_file.flush();
  pid_file.close();
}

void CreateRequiredDirectories() {
  bool ok;
  ok = util::os::CreateFolders(g_config.CraneScriptDir);
  if (!ok) std::exit(1);

  ok = util::os::CreateFolders(Supervisor::kSupervisorPidFileDir);
  if (!ok) std::exit(1);

  if (g_config.SupervisorDebugLevel != "off") {
    ok = util::os::CreateFoldersForFile(g_config.SupervisorLogFile);
    if (!ok) std::exit(1);
  }
}

void GlobalVariableInit() {
  CreateRequiredDirectories();

  // Ignore following sig
  signal(SIGINT, SIG_IGN);
  signal(SIGTERM, SIG_IGN);
  signal(SIGTSTP, SIG_IGN);
  signal(SIGQUIT, SIG_IGN);
  // Mask SIGPIPE to prevent Supervisor from crushing due to
  // SIGPIPE while communicating with spawned task processes.
  signal(SIGPIPE, SIG_IGN);
  signal(SIGUSR1, SIG_IGN);
  signal(SIGUSR2, SIG_IGN);
  signal(SIGALRM, SIG_IGN);
  signal(SIGHUP, SIG_IGN);

  CreatePidFile();

  PasswordEntry::InitializeEntrySize();

  g_thread_pool =
      std::make_unique<BS::thread_pool>(std::thread::hardware_concurrency());
  g_task_mgr = std::make_unique<Supervisor::TaskManager>();

  g_craned_client = std::make_unique<Supervisor::CranedClient>();
  g_craned_client->InitChannelAndStub(g_config.CranedUnixSocketPath);

  if (g_config.Plugin.Enabled) {
    CRANE_INFO("[Plugin] Plugin module is enabled.");
    g_plugin_client = std::make_unique<plugin::PluginClient>();
    g_plugin_client->InitChannelAndStub(g_config.Plugin.PlugindSockPath);
  }

  g_cfored_manager = std::make_unique<Supervisor::CforedManager>();
  g_cfored_manager->Init();

  g_server = std::make_unique<Supervisor::SupervisorServer>();

  using google::protobuf::io::FileOutputStream;
  using google::protobuf::util::SerializeDelimitedToZeroCopyStream;
  auto ostream = FileOutputStream(STDOUT_FILENO);

  // Ready for grpc call
  crane::grpc::SupervisorReady msg;
  msg.set_ok(true);
  auto ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);
  ok &= ostream.Flush();
  if (!ok) std::abort();
}

void StartServer() {
  constexpr uint64_t file_max = 640000;
  if (!util::os::SetMaxFileDescriptorNumber(file_max)) {
    CRANE_ERROR("Unable to set file descriptor limits to {}", file_max);
    std::exit(1);
  }

  GlobalVariableInit();

  // Set FD_CLOEXEC on stdin, stdout, stderr
  util::os::SetCloseOnExecOnFdRange(STDIN_FILENO, STDERR_FILENO + 1);

  g_server->Wait();
  g_task_mgr->Wait();
  g_task_mgr.reset();

  // CforedManager MUST be destructed after TaskManager.
  g_cfored_manager.reset();
  g_craned_client.reset();
  g_plugin_client.reset();
  g_thread_pool->wait();
  g_thread_pool.reset();

  std::exit(0);
}

void InstallStackTraceHooks() {
  static backward::SignalHandling sh;
  if (!sh.loaded()) {
    CRANE_ERROR("Failed to install stacktrace hooks.");
    std::exit(1);
  }
}

int main(int argc, char** argv) {
  InitFromStdin(argc, argv);
  InstallStackTraceHooks();
  StartServer();
}
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

#include "CranedClient.h"
#include "SupervisorServer.h"
#include "TaskManager.h"
#include "crane/PasswordEntry.h"
#include "crane/PluginClient.h"
#include "crane/PublicHeader.h"
#include "crane/TracePluginExporter.h"

using Craned::Supervisor::g_config;

int InitFromStdin(int argc, char** argv) {
  auto supervisor_start_time = std::chrono::system_clock::now();
  cxxopts::Options options("CSupervisor");

  // clang-format off
  options.add_options()
      ("v,version", "Display version information")
      ("h,help", "Display help for CSupervisor")
      ("i,input-fd",
        "Input file descriptor for supervisor(For internal use only)",cxxopts::value<int>())
      ("o,output-fd",
        "Output file descriptor for supervisor(For internal use only)",cxxopts::value<int>())
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
    std::exit(0);
  }

  int grpc_input_fd = -1;
  int grpc_output_fd = -1;
  if (parsed_args.count("input-fd") > 0) {
    grpc_input_fd = parsed_args["input-fd"].as<int>();
  } else {
    fmt::print(stderr, "Input fd is required.\n");
    std::exit(1);
  }
  if (parsed_args.count("output-fd") > 0) {
    grpc_output_fd = parsed_args["output-fd"].as<int>();
  } else {
    fmt::print(stderr, "Output fd is required.\n");
    std::exit(1);
  }

  using google::protobuf::io::FileInputStream;
  using google::protobuf::util::ParseDelimitedFromZeroCopyStream;

  auto istream = FileInputStream(grpc_input_fd);
  crane::grpc::supervisor::InitSupervisorRequest msg;
  auto ok = ParseDelimitedFromZeroCopyStream(&msg, &istream, nullptr);
  if (!ok) {
    fmt::print(stderr,
               "[Supervisor pid#{}] Failed to recv message from Craned.\n",
               getpid());
    std::abort();
  }
  close(grpc_input_fd);
  auto recv_init_msg_time = std::chrono::system_clock::now();

  g_config.JobId = msg.job_id();
  g_config.JobName = msg.job_name();
  g_config.StepId = msg.step_id();
  g_config.StepSpec = msg.step_spec();
  g_config.CranedIdOfThisNode = msg.craned_id();
  g_config.TaskCount = 1;
  g_config.SupervisorDebugLevel = msg.debug_level();
  g_config.CranedUnixSocketPath = msg.craned_unix_socket_path();
  g_config.CraneBaseDir = msg.crane_base_dir();
  g_config.CraneScriptDir = msg.crane_script_dir();
  g_config.CforedListenConf.TlsConfig.Enabled =
      msg.cfored_listen_conf().use_tls();
  g_config.CforedListenConf.TlsConfig.TlsCerts.CertContent =
      msg.cfored_listen_conf().tls_certs().cert_content();
  g_config.CforedListenConf.TlsConfig.CaContent =
      msg.cfored_listen_conf().tls_certs().ca_content();
  g_config.CforedListenConf.TlsConfig.TlsCerts.KeyContent =
      msg.cfored_listen_conf().tls_certs().key_content();
  g_config.CforedListenConf.TlsConfig.DomainSuffix =
      msg.cfored_listen_conf().domain_suffix();

  // Environment from JobManager
  g_config.JobEnv.clear();
  for (const auto& [key, value] : msg.env()) {
    g_config.JobEnv.emplace(key, value);
  }

  // Cgroup path for OOM monitoring
  g_config.CgroupPath = msg.cgroup_path();

  // Container config
  g_config.Container.Enabled = msg.has_container_config();
  if (g_config.Container.Enabled) {
    g_config.Container.TempDir = msg.container_config().temp_dir();
    g_config.Container.RuntimeEndpoint =
        msg.container_config().runtime_endpoint();
    g_config.Container.ImageEndpoint = msg.container_config().image_endpoint();
    g_config.Container.BindFs.Enabled = msg.container_config().has_bindfs();
    if (g_config.Container.BindFs.Enabled) {
      const auto& bindfs_conf = msg.container_config().bindfs();
      g_config.Container.BindFs.BindfsBinary = bindfs_conf.bindfs_binary();
      g_config.Container.BindFs.FusermountBinary =
          bindfs_conf.fusermount_binary();
      g_config.Container.BindFs.MountBaseDir = bindfs_conf.mount_base_dir();
    }
    if (msg.container_config().has_subid()) {
      const auto& subid_conf = msg.container_config().subid();
      g_config.Container.SubId.Managed = subid_conf.managed();
      g_config.Container.SubId.RangeSize = subid_conf.range_size();
      g_config.Container.SubId.BaseOffset = subid_conf.base_offset();
    }
  }

  // Plugin config
  g_config.Plugin.Enabled = msg.has_plugin_config();
  if (g_config.Plugin.Enabled)
    g_config.Plugin.PlugindSockPath = msg.plugin_config().socket_path();

  g_config.SupervisorLogFile =
      std::filesystem::path(msg.log_dir()) /
      fmt::format("{}.{}.log", g_config.JobId, g_config.StepId);
  g_config.SupervisorMaxLogFileSize = msg.max_log_file_size();
  g_config.SupervisorMaxLogFileNum = msg.max_log_file_num();
  g_config.SupervisorUnixSockPath =
      std::filesystem::path(kDefaultSupervisorUnixSockDir) /
      fmt::format("step_{}.{}.sock", g_config.JobId, g_config.StepId);

  auto log_level = StrToLogLevel(g_config.SupervisorDebugLevel);
  if (log_level.has_value()) {
    InitLogger(log_level.value(), g_config.SupervisorLogFile, false,
               g_config.SupervisorMaxLogFileSize,
               g_config.SupervisorMaxLogFileNum);
  } else {
    fmt::print(stderr, "[Supervisor #{}.{}] Invalid debug level: {}\n",
               g_config.SupervisorDebugLevel, g_config.JobId, g_config.StepId);
    ok = false;
  }
  if (ok) {
    int null_fd = open("/dev/null", O_WRONLY);
    if (null_fd != -1) {
      if (dup2(null_fd, STDOUT_FILENO) == -1) {
        fmt::print(
            stderr,
            "[Supervisor #{}.{}] Stdout failed to redirect to /dev/null: {}\n",
            g_config.JobId, g_config.StepId, std::strerror(errno));
        ok = false;
      }
      if (dup2(null_fd, STDERR_FILENO) == -1) {
        fmt::print(
            stderr,
            "[Supervisor #{}.{}] Stderr failed to redirect to /dev/null: {}\n",
            g_config.JobId, g_config.StepId, std::strerror(errno));
        ok = false;
      }
      close(null_fd);
    } else {
      fmt::print(
          stderr,
          "[Supervisor #{}.{}] Failed to open /dev/null for stdout and stderr "
          "writing: {}\n",
          g_config.JobId, g_config.StepId, std::strerror(errno));
      ok = false;
    }
  }

  if (!ok) {
    using google::protobuf::io::FileOutputStream;
    using google::protobuf::util::SerializeDelimitedToZeroCopyStream;
    auto ostream = FileOutputStream(grpc_output_fd);
    crane::grpc::supervisor::SupervisorReady msg;
    msg.set_ok(ok);
    SerializeDelimitedToZeroCopyStream(msg, &ostream);
    ostream.Flush();
    std::abort();
  }
  CRANE_INFO("Supervisor start at: {}",
             std::chrono::current_zone()->to_local(supervisor_start_time));
  CRANE_INFO("Supervisor recv init msg at: {}",
             std::chrono::current_zone()->to_local(recv_init_msg_time));
  return grpc_output_fd;
}

bool CreateRequiredDirectories() {
  bool ok{true};
  ok = util::os::CreateFolders(g_config.CraneScriptDir);
  if (!ok) return ok;

  if (g_config.SupervisorDebugLevel != "off") {
    ok = util::os::CreateFoldersForFile(g_config.SupervisorLogFile);
    if (!ok) return ok;
  }

  ok = util::os::CreateFolders(kDefaultSupervisorUnixSockDir);
  return ok;
}

void GlobalVariableInit(int grpc_output_fd) {
  bool ok = CreateRequiredDirectories();
  using google::protobuf::io::FileOutputStream;
  using google::protobuf::util::SerializeDelimitedToZeroCopyStream;
  auto ostream = FileOutputStream(grpc_output_fd);

  // Ready for grpc call
  crane::grpc::supervisor::SupervisorReady msg;
  msg.set_ok(ok);
  if (!ok) {
    SerializeDelimitedToZeroCopyStream(msg, &ostream);
    ostream.Close();
    std::abort();
  }

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

  // Set OOM score adjustment for supervisor process
  std::filesystem::path oom_adj_file =
      fmt::format("/proc/{}/oom_score_adj", getpid());

  std::ofstream oom_adj_file_stream(oom_adj_file);
  if (!oom_adj_file_stream.is_open()) {
    std::exit(1);
  }

  oom_adj_file_stream << "-1000";
  oom_adj_file_stream.close();

  if (oom_adj_file_stream.fail()) {
    std::exit(1);
  }

  PasswordEntry::InitializeEntrySize();

#ifdef CRANE_ENABLE_TRACING
  std::filesystem::path trace_dir =
      "/nfs/home/interntwo/crane/output/supervisor";

  if (!std::filesystem::exists(trace_dir)) {
    try {
      std::filesystem::create_directories(trace_dir);
    } catch (const std::filesystem::filesystem_error& e) {
      fmt::print(stderr, "Failed to create trace directory {}: {}\n",
                 trace_dir.string(), e.what());
    }
  }

  auto trace_file = trace_dir / fmt::format("supervisor_{}_{}.trace",
                                            g_config.JobId, g_config.StepId);
  auto plugin_exporter = std::make_unique<crane::TracePluginExporter>(
      []() { return g_plugin_client.get(); },
      []() { return g_config.Plugin.Enabled; });

  if (crane::TracerManager::GetInstance().Initialize(
          trace_file.string(), "Supervisor", std::move(plugin_exporter))) {
    g_tracer = crane::TracerManager::GetInstance().GetTracer();
  }
#endif

  Craned::Common::CgroupManager::Init(
      StrToLogLevel(g_config.SupervisorDebugLevel).value());
  g_thread_pool = std::make_unique<BS::thread_pool>(
      std::thread::hardware_concurrency(),
      [] { util::SetCurrentThreadName("BsThreadPool"); });
  g_task_mgr = std::make_unique<Craned::Supervisor::TaskManager>();

  g_craned_client = std::make_unique<Craned::Supervisor::CranedClient>();
  g_craned_client->InitChannelAndStub(
      fmt::format("unix://{}", g_config.CranedUnixSocketPath.string()));

  if (g_config.Plugin.Enabled) {
    CRANE_INFO("[Plugin] Plugin module is enabled.");
    g_plugin_client = std::make_unique<plugin::PluginClient>();
    g_plugin_client->InitChannelAndStub(g_config.Plugin.PlugindSockPath);
  }

  g_server = std::make_unique<Craned::Supervisor::SupervisorServer>();

  // Make sure grpc server is ready to receive requests.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  ok = SerializeDelimitedToZeroCopyStream(msg, &ostream);

  ok &= ostream.Flush();
  if (!ok) std::abort();
  close(grpc_output_fd);
}

void StartServer(int grpc_output_fd) {
  using crane::grpc::StepType;
  using Craned::Supervisor::g_runtime_status;
  using Craned::Supervisor::StepStatus;

  constexpr uint64_t file_max = 640000;
  if (!util::os::SetMaxFileDescriptorNumber(file_max)) {
    CRANE_ERROR("Unable to set file descriptor limits to {}", file_max);
    std::exit(1);
  }

  GlobalVariableInit(grpc_output_fd);

  // Set FD_CLOEXEC on stdin, stdout, stderr
  util::os::SetCloseOnExecOnFdRange(STDIN_FILENO, STDERR_FILENO + 1);

  CRANE_INFO("Supervisor started for step type: {}.",
             static_cast<int>(g_config.StepSpec.step_type()));

  if (g_config.StepSpec.step_type() == StepType::DAEMON) {
    // For container jobs, the daemon step need to setup a pod per node,
    // then the following common steps will launch containers inside the pod.
    bool ready = true;
    if (g_config.StepSpec.has_pod_meta()) {
      if (!g_config.Container.Enabled) {
        CRANE_ERROR(
            "Container config is required for daemon step with pod spec.");
        ready = false;
      } else {
        // Just wait here for pod setup. if pod failed, daemon step failed.
        auto ok_prom = g_task_mgr->ExecuteTaskAsync();
        if (auto err = ok_prom.get(); err != CraneErrCode::SUCCESS) {
          CRANE_ERROR("Failed to start daemon step, code: {}",
                      static_cast<int>(err));
          ready = false;
        }
      }
    }

    // Daemon step is RUNNING after supervisor and related resources are ready.
    g_runtime_status.Status = ready ? StepStatus::Running : StepStatus::Failed;

  } else {
    // Common step is CONFIGURED after supervisor is ready.
    g_runtime_status.Status = StepStatus::Configured;
  }

  g_craned_client->StepStatusChangeAsync(g_runtime_status.Status, 0,
                                         std::nullopt);

  g_server->Wait();
  g_server.reset();
  g_task_mgr->Wait();
  g_task_mgr.reset();

  g_craned_client.reset();
  g_plugin_client.reset();

  g_thread_pool->wait();
  g_thread_pool.reset();

#ifdef CRANE_ENABLE_TRACING
  crane::TracerManager::GetInstance().Shutdown();
#endif

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
  auto grpc_output_fd = InitFromStdin(argc, argv);
  InstallStackTraceHooks();
  StartServer(grpc_output_fd);
}

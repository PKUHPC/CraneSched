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

#include "CranedPublicDefs.h"
// Precompiled header comes first.

#include <sys/file.h>
#include <sys/stat.h>
#include <yaml-cpp/yaml.h>

#include <ctime>
#include <cxxopts.hpp>

#include "CranedServer.h"
#include "CtldClient.h"
#include "DeviceManager.h"
#include "JobManager.h"
#include "crane/PluginClient.h"
#include "crane/String.h"

using Craned::g_config;
using Craned::Partition;

void ParseConfig(int argc, char** argv) {
  cxxopts::Options options("craned");

  // clang-format off
  options.add_options()
      ("C,config", "Path to configuration file",
      cxxopts::value<std::string>()->default_value(kDefaultConfigPath))
      ("l,listen", "Listening address, format: <IP>:<port>",
       cxxopts::value<std::string>()->default_value(fmt::format("0.0.0.0:{}", kCranedDefaultPort)))
      ("s,server-address", "CraneCtld address, format: <IP>:<port>",
       cxxopts::value<std::string>())
      ("L,log-file", "Path to Craned log file",
       cxxopts::value<std::string>()->default_value(fmt::format("{}{}",kDefaultCraneBaseDir, kDefaultCranedLogPath)))
      ("D,debug-level", "Logging level of Craned, format: <trace|debug|info|warn|error>",
       cxxopts::value<std::string>()->default_value("info"))
      ("v,version", "Display version information")
      ("h,help", "Display help for Craned")
      ;
  // clang-format on

  cxxopts::ParseResult parsed_args;
  try {
    parsed_args = options.parse(argc, argv);
  } catch (cxxopts::OptionException& e) {
    CRANE_ERROR("{}\n{}", e.what(), options.help());
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

  std::string config_path = parsed_args["config"].as<std::string>();
  std::unordered_map<std::string, std::vector<Craned::DeviceMetaInConfig>>
      each_node_device;
  if (std::filesystem::exists(config_path)) {
    try {
      YAML::Node config = YAML::LoadFile(config_path);

      if (config["CraneBaseDir"])
        g_config.CraneBaseDir = config["CraneBaseDir"].as<std::string>();
      else
        g_config.CraneBaseDir = kDefaultCraneBaseDir;

      if (parsed_args.count("log-file"))
        g_config.CranedLogFile = parsed_args["log-file"].as<std::string>();
      else if (config["CranedLogFile"])
        g_config.CranedLogFile =
            g_config.CraneBaseDir + config["CranedLogFile"].as<std::string>();
      else
        g_config.CranedLogFile = g_config.CraneBaseDir + kDefaultCranedLogPath;

      if (parsed_args.count("debug-level"))
        g_config.CranedDebugLevel =
            parsed_args["debug-level"].as<std::string>();
      else if (config["CranedDebugLevel"])
        g_config.CranedDebugLevel =
            config["CranedDebugLevel"].as<std::string>();
      else
        g_config.CranedDebugLevel = "info";

      // spdlog should be initialized as soon as possible
      std::optional log_level = StrToLogLevel(g_config.CranedDebugLevel);
      if (log_level.has_value()) {
        InitLogger(log_level.value(), g_config.CranedLogFile, true);
      } else {
        fmt::print(stderr, "Illegal debug-level format.");
        std::exit(1);
      }

#ifdef CRANE_ENABLE_BPF
      Craned::CgroupV2::SetBpfDebugLogLevel(
          static_cast<uint32_t>(log_level.value()));
#endif
      if (config["CranedUnixSockPath"])
        g_config.CranedUnixSockPath =
            g_config.CraneBaseDir +
            config["CranedUnixSockPath"].as<std::string>();
      else
        g_config.CranedUnixSockPath =
            g_config.CraneBaseDir + kDefaultCranedUnixSockPath;

      if (config["CranedScriptDir"])
        g_config.CranedScriptDir =
            g_config.CraneBaseDir + config["CranedScriptDir"].as<std::string>();
      else
        g_config.CranedScriptDir =
            g_config.CraneBaseDir + kDefaultCranedScriptDir;

      if (config["CranedMutexFilePath"])
        g_config.CranedMutexFilePath =
            g_config.CraneBaseDir +
            config["CranedMutexFilePath"].as<std::string>();
      else
        g_config.CranedMutexFilePath =
            g_config.CraneBaseDir + kDefaultCranedMutexFile;

      // Parsing node hostnames needs network functions, initialize it first.
      crane::InitializeNetworkFunctions();

      if (config["CranedListen"])
        g_config.ListenConf.CranedListenAddr =
            config["CranedListen"].as<std::string>();
      else
        g_config.ListenConf.CranedListenAddr = "0.0.0.0";

      if (config["CranedListenPort"])
        g_config.ListenConf.CranedListenPort =
            config["CranedListenPort"].as<std::string>();
      else
        g_config.ListenConf.CranedListenPort = kCtldDefaultPort;

      g_config.ListenConf.CranedListenPort = kCranedDefaultPort;

      g_config.ListenConf.UnixSocketListenAddr =
          fmt::format("unix://{}", g_config.CranedUnixSockPath);

      if (config["CompressedRpc"])
        g_config.CompressedRpc = config["CompressedRpc"].as<bool>();

      if (config["UseTls"] && config["UseTls"].as<bool>()) {
        g_config.ListenConf.UseTls = true;
        TlsCertificates& tls_certs = g_config.ListenConf.TlsCerts;

        if (config["DomainSuffix"])
          tls_certs.DomainSuffix = config["DomainSuffix"].as<std::string>();

        if (config["ServerCertFilePath"]) {
          tls_certs.ServerCertFilePath =
              config["ServerCertFilePath"].as<std::string>();

          try {
            tls_certs.ServerCertContent =
                util::ReadFileIntoString(tls_certs.ServerCertFilePath);
          } catch (const std::exception& e) {
            CRANE_ERROR("Read cert file error: {}", e.what());
            std::exit(1);
          }
          if (tls_certs.ServerCertContent.empty()) {
            CRANE_ERROR(
                "UseTls is true, but the file specified by ServerCertFilePath "
                "is empty");
          }
        } else {
          CRANE_ERROR("UseTls is true, but ServerCertFilePath is empty");
          std::exit(1);
        }

        if (config["ServerKeyFilePath"]) {
          tls_certs.ServerKeyFilePath =
              config["ServerKeyFilePath"].as<std::string>();

          try {
            tls_certs.ServerKeyContent =
                util::ReadFileIntoString(tls_certs.ServerKeyFilePath);
          } catch (const std::exception& e) {
            CRANE_ERROR("Read cert file error: {}", e.what());
            std::exit(1);
          }
          if (tls_certs.ServerKeyContent.empty()) {
            CRANE_ERROR(
                "UseTls is true, but the file specified by ServerKeyFilePath "
                "is empty");
          }
        } else {
          CRANE_ERROR("UseTls is true, but ServerKeyFilePath is empty");
          std::exit(1);
        }
      } else {
        g_config.ListenConf.UseTls = false;
      }

      if (config["ControlMachine"]) {
        g_config.ControlMachine = config["ControlMachine"].as<std::string>();
      }

      if (config["CraneCtldListenPort"])
        g_config.CraneCtldListenPort =
            config["CraneCtldListenPort"].as<std::string>();
      else
        g_config.CraneCtldListenPort = kCtldDefaultPort;

      if (config["Nodes"]) {
        for (auto it = config["Nodes"].begin(); it != config["Nodes"].end();
             ++it) {
          auto node = it->as<YAML::Node>();
          auto node_res = std::make_shared<ResourceInNode>();
          std::list<std::string> name_list;

          if (node["name"]) {
            if (!util::ParseHostList(node["name"].Scalar(), &name_list)) {
              CRANE_ERROR("Illegal node name string format.");
              std::exit(1);
            }
            CRANE_TRACE("node name list parsed: {}",
                        fmt::join(name_list, ", "));
          } else
            std::exit(1);

          if (node["cpu"])
            node_res->allocatable_res.cpu_count =
                cpu_t(std::stoul(node["cpu"].as<std::string>()));
          else
            std::exit(1);

          if (node["memory"]) {
            auto memory = node["memory"].as<std::string>();
            std::regex mem_regex(R"((\d+)([KMBG]))");
            std::smatch mem_group;
            if (!std::regex_search(memory, mem_group, mem_regex)) {
              CRANE_ERROR("Illegal memory format.");
              std::exit(1);
            }

            uint64_t memory_bytes = std::stoul(mem_group[1]);
            if (mem_group[2] == "K")
              memory_bytes *= 1024;
            else if (mem_group[2] == "M")
              memory_bytes *= 1024 * 1024;
            else if (mem_group[2] == "G")
              memory_bytes *= 1024 * 1024 * 1024;

            node_res->allocatable_res.memory_bytes = memory_bytes;
            node_res->allocatable_res.memory_sw_bytes = memory_bytes;
          } else
            std::exit(1);

          std::vector<Craned::DeviceMetaInConfig> devices;
          if (node["gres"]) {
            for (auto gres_it = node["gres"].begin();
                 gres_it != node["gres"].end(); ++gres_it) {
              const auto& gres_node = gres_it->as<YAML::Node>();
              const auto& device_name = gres_node["name"].as<std::string>();
              const auto& device_type = gres_node["type"].as<std::string>();
              bool device_file_configured = false;
              std::string env_injector;
              if (gres_node["EnvInjector"]) {
                env_injector = gres_node["EnvInjector"].as<std::string>();
              }
              if (gres_node["DeviceFileRegex"]) {
                device_file_configured = true;
                std::list<std::string> device_path_list;
                if (!util::ParseHostList(gres_node["DeviceFileRegex"].Scalar(),
                                         &device_path_list)) {
                  CRANE_ERROR(
                      "Illegal gres {}:{} DeviceFileRegex path string format.",
                      device_name, device_type);
                  std::exit(1);
                }
                for (const auto& device_path : device_path_list) {
                  devices.push_back(
                      {device_name, device_type, std::vector{device_path},
                       !env_injector.empty() ? std::optional(env_injector)
                                             : std::nullopt});
                }
              }
              if (gres_node["DeviceFileList"] &&
                  gres_node["DeviceFileList"].IsSequence()) {
                device_file_configured = true;
                for (const auto& file_regex :
                     gres_node["DeviceFileList"]
                         .as<std::vector<std::string>>()) {
                  std::list<std::string> device_path_list;
                  if (!util::ParseHostList(file_regex, &device_path_list)) {
                    CRANE_ERROR(
                        "Illegal gres {}:{} DeviceFileList path string format.",
                        device_name, device_type);
                    std::exit(1);
                  }
                  devices.push_back({device_name, device_type,
                                     std::vector(device_path_list.begin(),
                                                 device_path_list.end()),
                                     !env_injector.empty()
                                         ? std::optional(env_injector)
                                         : std::nullopt});
                }
              }
              if (!device_file_configured) {
                CRANE_ERROR(
                    "At least one of DeviceFileRegex or DeviceFileList must be "
                    "configured for GRES {}:{} device.",
                    device_name, device_type);
              }
            }
          }

          for (auto&& name : name_list) {
            for (auto& dev : devices) {
              each_node_device[name].push_back(dev);
            }

            ipv4_t ipv4;
            ipv6_t ipv6;
            switch (crane::GetIpAddrVer(name)) {
            case -1: {
              bool ip_resolved = false;
              if (crane::ResolveIpv4FromHostname(name, &ipv4)) {
                g_config.Ipv4ToCranedHostname[ipv4] = name;
                CRANE_INFO("Resolve hostname `{}` to `{}`", name,
                           crane::Ipv4ToStr(ipv4));
                ip_resolved = true;
              }

              if (crane::ResolveIpv6FromHostname(name, &ipv6)) {
                g_config.Ipv6ToCranedHostname[ipv6] = name;
                CRANE_INFO("Resolve hostname `{}` to `{}`", name,
                           crane::Ipv6ToStr(ipv6));
                ip_resolved = true;
              }

              if (!ip_resolved) {
                CRANE_ERROR("Init error: Cannot resolve hostname of `{}`",
                            name);
                std::exit(1);
              }
              break;
            }

            case 4: {
              CRANE_INFO(
                  "Node name `{}` is a valid ipv4 address and doesn't "
                  "need resolving.",
                  name);
              if (!crane::StrToIpv4(name, &ipv4)) {
                std::exit(1);
              }
              g_config.Ipv4ToCranedHostname[ipv4] = name;
              break;
            }

            case 6: {
              CRANE_INFO(
                  "Node name `{}` is a valid ipv6 address and doesn't "
                  "need resolving.",
                  name);
              if (!crane::StrToIpv6(name, &ipv6)) {
                std::exit(1);
              }
              g_config.Ipv6ToCranedHostname[ipv6] = name;
              break;
            }

            default:
              ABSL_UNREACHABLE();
            }
            g_config.CranedRes[name] = node_res;
          }
        }
      }

      if (config["Partitions"]) {
        for (auto it = config["Partitions"].begin();
             it != config["Partitions"].end(); ++it) {
          auto partition = it->as<YAML::Node>();
          std::string name;
          std::string nodes;
          Partition part;

          if (partition["name"]) {
            name.assign(partition["name"].Scalar());
          } else
            std::exit(1);

          if (partition["nodes"]) {
            nodes = partition["nodes"].as<std::string>();
          } else
            std::exit(1);

          std::list<std::string> name_list;
          if (!util::ParseHostList(nodes, &name_list)) {
            CRANE_ERROR("Illegal node name string format.");
            std::exit(1);
          }

          for (auto&& node : name_list) {
            std::string node_s{node};

            auto node_it = g_config.CranedRes.find(node_s);
            if (node_it != g_config.CranedRes.end()) {
              part.nodes.emplace(node_it->first);
              CRANE_INFO("Find node {} in partition {}", node_it->first, name);
            } else {
              CRANE_ERROR(
                  "Unknown node '{}' found in partition '{}'. It is ignored "
                  "and should be contained in the configuration file.",
                  node, name);
            }
          }

          g_config.Partitions.emplace(std::move(name), std::move(part));
        }

        if (config["CranedForeground"]) {
          auto val = config["CranedForeground"].as<std::string>();
          if (val == "true")
            g_config.CranedForeground = true;
          else
            g_config.CranedForeground = false;
        }

        if (config["Plugin"]) {
          const auto& plugin_config = config["Plugin"];

          if (plugin_config["Enabled"])
            g_config.Plugin.Enabled = plugin_config["Enabled"].as<bool>();

          if (plugin_config["PlugindSockPath"]) {
            g_config.Plugin.PlugindSockPath =
                fmt::format("unix://{}{}", g_config.CraneBaseDir,
                            plugin_config["PlugindSockPath"].as<std::string>());
          } else {
            g_config.Plugin.PlugindSockPath =
                fmt::format("unix://{}{}", g_config.CraneBaseDir,
                            kDefaultPlugindUnixSockPath);
          }
        }
      }
    } catch (YAML::BadFile& e) {
      CRANE_CRITICAL("Can't open config file {}: {}", kDefaultConfigPath,
                     e.what());
      std::exit(1);
    }
  } else {
    CRANE_CRITICAL("Config file '{}' not existed", config_path);
    std::exit(1);
  }

  if (parsed_args.count("listen")) {
    g_config.ListenConf.CranedListenAddr =
        parsed_args["listen"].as<std::string>();
    g_config.ListenConf.CranedListenPort = kCranedDefaultPort;
  }

  if (parsed_args.count("server-address") == 0) {
    if (g_config.ControlMachine.empty()) {
      CRANE_CRITICAL(
          "CraneCtld address must be specified in command line or config "
          "file.\n{}",
          options.help());
      std::exit(1);
    }
  } else {
    g_config.ControlMachine = parsed_args["server-address"].as<std::string>();
  }

  if (crane::GetIpAddrVer(g_config.ListenConf.CranedListenAddr) == -1) {
    CRANE_ERROR("Listening address is invalid.");
    std::exit(1);
  }

  std::regex regex_port(R"(^([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|)"
                        R"(65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$)");
  if (!std::regex_match(g_config.ListenConf.CranedListenPort, regex_port)) {
    CRANE_ERROR("Listening port is invalid.");
    std::exit(1);
  }

  char hostname[HOST_NAME_MAX + 1];
  int r = gethostname(hostname, HOST_NAME_MAX + 1);
  if (r != 0) {
    CRANE_ERROR("Error: get hostname.");
    std::exit(1);
  }
  g_config.Hostname.assign(hostname);

  if (!g_config.CranedRes.contains(g_config.Hostname)) {
    CRANE_ERROR("This machine {} is not contained in Nodes!",
                g_config.Hostname);
    std::exit(1);
  }

  CRANE_INFO("Found this machine {} in Nodes", g_config.Hostname);
  // get this node device info
  // Todo: Auto detect device
  {
    auto node_res = g_config.CranedRes.at(g_config.Hostname);
    auto& devices = each_node_device[g_config.Hostname];
    for (auto& dev_arg : devices) {
      auto& [name, type, path_vec, env_injector] = dev_arg;
      auto env_injector_enum =
          Craned::GetDeviceEnvInjectorFromStr(env_injector);
      if (env_injector_enum == Craned::InvalidInjector) {
        CRANE_ERROR("Invalid injector type:{} for device {}.",
                    env_injector.value_or("EmptyVal"), path_vec);
        std::exit(1);
      }

      std::unique_ptr dev = Craned::DeviceManager::ConstructDevice(
          name, type, path_vec, env_injector_enum);
      if (!dev->Init()) {
        CRANE_ERROR("Access Device {} failed.", static_cast<std::string>(*dev));
        std::exit(1);
      }

      dev->slot_id = dev->device_file_metas.front().path;
      node_res->dedicated_res.name_type_slots_map[dev->name][dev->type].emplace(
          dev->slot_id);
      Craned::g_this_node_device[dev->slot_id] = std::move(dev);
    }
    each_node_device.clear();
  }

  uint32_t part_id, node_index;
  std::string part_name;
  for (const auto& par : g_config.Partitions) {
    if (par.second.nodes.contains(g_config.Hostname)) {
      part_name = par.first;
      CRANE_INFO("Found this machine {} in partition {}", g_config.Hostname,
                 par.first);
      break;
    }
  }
  if (part_name.empty()) {
    CRANE_ERROR("This machine {} doesn't belong to any partition",
                g_config.Hostname);
    std::exit(1);
  }

  g_config.CranedIdOfThisNode = g_config.Hostname;
  CRANE_INFO("CranedId of this machine: {}", g_config.CranedIdOfThisNode);

  auto& meta = g_config.CranedMeta;
  if (bool ok = util::os::GetSystemReleaseInfo(&meta.SysInfo); !ok) {
    CRANE_ERROR("Error when get system release info");
  }

  g_config.CranedMeta.CranedStartTime = absl::Now();
  g_config.CranedMeta.SystemBootTime = util::os::GetSystemBootTime();
}

void CreateRequiredDirectories() {
  bool ok;
  ok = util::os::CreateFolders(g_config.CranedScriptDir);
  if (!ok) std::exit(1);

  ok = util::os::CreateFoldersForFile(g_config.CranedLogFile);
  if (!ok) std::exit(1);
}

void GlobalVariableInit() {
  CreateRequiredDirectories();

  // Mask SIGPIPE to prevent Craned from crushing due to
  // SIGPIPE while communicating with spawned task processes.
  signal(SIGPIPE, SIG_IGN);

  PasswordEntry::InitializeEntrySize();

  using Craned::CgroupManager;
  using Craned::CgroupConstant::Controller;
  g_cg_mgr = std::make_unique<Craned::CgroupManager>();
  g_cg_mgr->Init();
  if (g_cg_mgr->GetCgroupVersion() ==
          Craned::CgroupConstant::CgroupVersion::CGROUP_V1 &&
      (!g_cg_mgr->Mounted(Controller::CPU_CONTROLLER) ||
       !g_cg_mgr->Mounted(Controller::MEMORY_CONTROLLER) ||
       !g_cg_mgr->Mounted(Controller::DEVICES_CONTROLLER) ||
       !g_cg_mgr->Mounted(Controller::BLOCK_CONTROLLER))) {
    CRANE_ERROR(
        "Failed to initialize cpu,memory,devices,block cgroups controller.");
    std::exit(1);
  }
  if (g_cg_mgr->GetCgroupVersion() ==
          Craned::CgroupConstant::CgroupVersion::CGROUP_V2 &&
      (!g_cg_mgr->Mounted(Controller::CPU_CONTROLLER_V2) ||
       !g_cg_mgr->Mounted(Controller::MEMORY_CONTORLLER_V2) ||
       !g_cg_mgr->Mounted(Controller::IO_CONTROLLER_V2))) {
    CRANE_ERROR("Failed to initialize cpu,memory,IO cgroups controller.");
    std::exit(1);
  }

  g_thread_pool =
      std::make_unique<BS::thread_pool>(std::thread::hardware_concurrency());

  g_job_mgr = std::make_unique<Craned::JobManager>();

  g_ctld_client = std::make_unique<Craned::CtldClient>();
  g_ctld_client->SetCranedId(g_config.CranedIdOfThisNode);

  g_ctld_client->InitChannelAndStub(g_config.ControlMachine);

  if (g_config.Plugin.Enabled) {
    CRANE_INFO("[Plugin] Plugin module is enabled.");
    g_plugin_client = std::make_unique<plugin::PluginClient>();
    g_plugin_client->InitChannelAndStub(g_config.Plugin.PlugindSockPath);
  }
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
  util::os::CheckProxyEnvironmentVariable();
  g_server = std::make_unique<Craned::CranedServer>(g_config.ListenConf);
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  g_ctld_client->StartConnectingCtld();
  g_server->Wait();

  // Free global variables
  g_job_mgr->Wait();
  g_job_mgr.reset();
  // CforedManager MUST be destructed after JobManager.
  g_cfored_manager.reset();
  g_server.reset();
  g_ctld_client.reset();
  g_plugin_client.reset();

  g_thread_pool->wait();
  g_thread_pool.reset();

  std::exit(0);
}

void StartDaemon() {
  /* Our process ID and Session ID */
  pid_t pid, sid;

  /* Fork off the parent process */
  pid = fork();
  if (pid < 0) {
    CRANE_ERROR("Error: fork()");
    exit(1);
  }
  /* If we got a good PID, then
     we can exit the parent process. */
  if (pid > 0) {
    exit(0);
  }

  /* Change the file mode mask */
  umask(0);

  /* Open any logs here */

  /* Create a new SID for the child process */
  sid = setsid();
  if (sid < 0) {
    /* Log the failure */
    CRANE_ERROR("Error: setsid()");
    exit(1);
  }

  /* Change the current working directory */
  if ((chdir("/")) < 0) {
    CRANE_ERROR("Error: chdir()");
    /* Log the failure */
    exit(1);
  }

  /* Close out the standard file descriptors */
  close(STDIN_FILENO);
  close(STDOUT_FILENO);
  close(STDERR_FILENO);

  /* Daemon-specific initialization goes here */
  StartServer();

  exit(EXIT_SUCCESS);
}

void CheckSingleton() {
  bool ok = util::os::CreateFoldersForFile(g_config.CranedMutexFilePath);
  if (!ok) std::exit(1);

  int pid_file =
      open(g_config.CranedMutexFilePath.c_str(), O_CREAT | O_RDWR, 0666);
  int rc = flock(pid_file, LOCK_EX | LOCK_NB);
  if (rc) {
    if (EWOULDBLOCK == errno) {
      CRANE_CRITICAL("There is another Craned instance running. Exiting...");
      std::exit(1);
    } else {
      CRANE_CRITICAL("Failed to lock {}: {}. Exiting...",
                     g_config.CranedMutexFilePath, strerror(errno));
      std::exit(1);
    }
  }
}

void InstallStackTraceHooks() {
  static backward::SignalHandling sh;
  if (!sh.loaded()) {
    CRANE_ERROR("Failed to install stacktrace hooks.");
    std::exit(1);
  }
}

int main(int argc, char** argv) {
  // If config parsing fails, this function will not return and will call
  // std::exit(1) instead.
  ParseConfig(argc, argv);
  CheckSingleton();
  InstallStackTraceHooks();

  if (g_config.CranedForeground)
    StartServer();
  else
    StartDaemon();

  return 0;
}

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

#include "CranedPublicDefs.h"
// Precompiled header comes first.

#include <event2/thread.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <yaml-cpp/yaml.h>

#include <cxxopts.hpp>

#include "CforedClient.h"
#include "CranedServer.h"
#include "CtldClient.h"
#include "crane/Network.h"
#include "crane/OS.h"
#include "crane/PublicHeader.h"
#include "crane/String.h"

using Craned::CranedNode;
using Craned::g_config;
using Craned::Partition;

void ParseConfig(int argc, char** argv) {
  cxxopts::Options options("craned");

  // clang-format off
  options.add_options()
      ("C,config", "Path to configuration file",
      cxxopts::value<std::string>()->default_value(kDefaultConfigPath))
      ("l,listen", "Listen address format: <IP>:<port>",
       cxxopts::value<std::string>()->default_value(fmt::format("0.0.0.0:{}", kCranedDefaultPort)))
      ("s,server-address", "CraneCtld address format: <IP>:<port>",
       cxxopts::value<std::string>())
      ("L,log-file", "File path of craned log file",
       cxxopts::value<std::string>()->default_value(kDefaultCranedLogPath))
      ("D,debug-level", "<trace|debug|info|warn|error>", cxxopts::value<std::string>()->default_value("info"))
      ("h,help", "Show help")
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
    std::exit(1);
  }

  std::string config_path = parsed_args["config"].as<std::string>();
  if (std::filesystem::exists(config_path)) {
    try {
      YAML::Node config = YAML::LoadFile(config_path);

      if (config["CraneBaseDir"])
        g_config.CraneBaseDir = config["CraneBaseDir"].as<std::string>();
      else
        g_config.CraneBaseDir = kDefaultCraneBaseDir;

      if (config["CranedLogFile"])
        g_config.CranedLogFile =
            g_config.CraneBaseDir + config["CranedLogFile"].as<std::string>();
      else
        g_config.CranedLogFile = g_config.CraneBaseDir + kDefaultCranedLogPath;

      if (config["CranedDebugLevel"])
        g_config.CranedDebugLevel =
            config["CranedDebugLevel"].as<std::string>();
      else
        g_config.CranedDebugLevel = "info";

      // spdlog should be initialized as soon as possible
      spdlog::level::level_enum log_level;
      if (g_config.CranedDebugLevel == "trace") {
        log_level = spdlog::level::trace;
      } else if (g_config.CranedDebugLevel == "debug") {
        log_level = spdlog::level::debug;
      } else if (g_config.CranedDebugLevel == "info") {
        log_level = spdlog::level::info;
      } else if (g_config.CranedDebugLevel == "warn") {
        log_level = spdlog::level::warn;
      } else if (g_config.CranedDebugLevel == "error") {
        log_level = spdlog::level::err;
      } else {
        fmt::print(stderr, "Illegal debug-level format.");
        std::exit(1);
      }

      InitLogger(log_level, g_config.CranedLogFile);

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

      g_config.ListenConf.CranedListenPort = kCranedDefaultPort;

      g_config.ListenConf.UnixSocketListenAddr =
          fmt::format("unix://{}", g_config.CranedUnixSockPath);

      if (config["CompressedRpc"])
        g_config.CompressedRpc = config["CompressedRpc"].as<bool>();

      if (config["UseTls"] && config["UseTls"].as<bool>()) {
        g_config.ListenConf.UseTls = true;

        if (config["DomainSuffix"])
          g_config.ListenConf.DomainSuffix =
              config["DomainSuffix"].as<std::string>();

        if (config["ServerCertFilePath"]) {
          g_config.ListenConf.ServerCertFilePath =
              config["ServerCertFilePath"].as<std::string>();

          try {
            g_config.ListenConf.ServerCertContent = util::ReadFileIntoString(
                g_config.ListenConf.ServerCertFilePath);
          } catch (const std::exception& e) {
            CRANE_ERROR("Read cert file error: {}", e.what());
            std::exit(1);
          }
          if (g_config.ListenConf.ServerCertContent.empty()) {
            CRANE_ERROR(
                "UseTls is true, but the file specified by ServerCertFilePath "
                "is empty");
          }
        } else {
          CRANE_ERROR("UseTls is true, but ServerCertFilePath is empty");
          std::exit(1);
        }

        if (config["ServerKeyFilePath"]) {
          g_config.ListenConf.ServerKeyFilePath =
              config["ServerKeyFilePath"].as<std::string>();

          try {
            g_config.ListenConf.ServerKeyContent =
                util::ReadFileIntoString(g_config.ListenConf.ServerKeyFilePath);
          } catch (const std::exception& e) {
            CRANE_ERROR("Read cert file error: {}", e.what());
            std::exit(1);
          }
          if (g_config.ListenConf.ServerKeyContent.empty()) {
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
          auto node_ptr = std::make_shared<CranedNode>();
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
            node_ptr->cpu = std::stoul(node["cpu"].as<std::string>());
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

            node_ptr->memory_bytes = memory_bytes;
          } else
            std::exit(1);

          for (auto&& name : name_list) {
            if (crane::IsAValidIpv4Address(name)) {
              CRANE_INFO(
                  "Node name `{}` is a valid ipv4 address and doesn't "
                  "need resolving.",
                  name);
              g_config.Ipv4ToCranedHostname[name] = name;
              g_config.CranedNodes[name] = node_ptr;
            } else {
              std::string ipv4;
              if (!crane::ResolveIpv4FromHostname(name, &ipv4)) {
                CRANE_ERROR("Init error: Cannot resolve hostname of `{}`",
                            name);
                std::exit(1);
              }
              CRANE_INFO("Resolve hostname `{}` to `{}`", name, ipv4);
              g_config.Ipv4ToCranedHostname[ipv4] = name;

              g_config.CranedNodes[name] = node_ptr;
            }
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

            auto node_it = g_config.CranedNodes.find(node_s);
            if (node_it != g_config.CranedNodes.end()) {
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
  // Todo: Check static level setting.
  if (parsed_args.count("debug-level")) {
    g_config.CranedDebugLevel = parsed_args["debug-level"].as<std::string>();
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

  if (parsed_args.count("debug-level")) {
    g_config.CranedDebugLevel = parsed_args["debug-level"].as<std::string>();
  }

  if (parsed_args.count("log-file")) {
    g_config.CranedLogFile = parsed_args["log-file"].as<std::string>();
  }

  // Check the format of CranedListen
  std::regex ipv4_re(R"(^((25[0-5]|(2[0-4]|1\d|[1-9]|)\d)\.?\b){4}$)");
  std::smatch ivp4_match;
  if (!std::regex_match(g_config.ListenConf.CranedListenAddr, ivp4_match,
                        ipv4_re)) {
    CRANE_ERROR("Illegal Ctld address format.");
    std::exit(1);
  }

  char hostname[HOST_NAME_MAX + 1];
  int r = gethostname(hostname, HOST_NAME_MAX + 1);
  if (r != 0) {
    CRANE_ERROR("Error: get hostname.");
    std::exit(1);
  }
  g_config.Hostname.assign(hostname);

  if (!g_config.CranedNodes.contains(g_config.Hostname)) {
    CRANE_ERROR("This machine {} is not contained in Nodes!",
                g_config.Hostname);
    std::exit(1);
  }

  CRANE_INFO("Found this machine {} in Nodes", g_config.Hostname);

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

  // Enable inter-thread custom event notification.
  evthread_use_pthreads();

  PasswordEntry::InitializeEntrySize();

  util::CgroupUtil::Init();
  if (!util::CgroupUtil::Mounted(
          util::CgroupConstant::Controller::CPU_CONTROLLER) ||
      !util::CgroupUtil::Mounted(
          util::CgroupConstant::Controller::MEMORY_CONTROLLER)) {
    CRANE_ERROR("Failed to initialize cpu and memory cgroups controller.");
    std::exit(1);
  }

  g_thread_pool =
      std::make_unique<BS::thread_pool>(std::thread::hardware_concurrency());

  g_task_mgr = std::make_unique<Craned::TaskManager>();

  g_ctld_client = std::make_unique<Craned::CtldClient>();
  g_ctld_client->SetCranedId(g_config.CranedIdOfThisNode);

  g_ctld_client->InitChannelAndStub(g_config.ControlMachine);

  g_cfored_manager = std::make_unique<Craned::CforedManager>();
  g_cfored_manager->Init();
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

  g_server = std::make_unique<Craned::CranedServer>(g_config.ListenConf);
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  g_ctld_client->StartConnectingCtld();
  g_server->Wait();

  // Free global variables
  g_task_mgr.reset();
  g_server.reset();
  g_ctld_client.reset();
  g_cfored_manager.reset();

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

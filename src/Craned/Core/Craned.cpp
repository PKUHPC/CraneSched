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
#include <unistd.h>
#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <array>
#include <ctime>
#include <cxxopts.hpp>

#include "CranedForPamServer.h"
#include "CranedServer.h"
#include "CtldClient.h"
#include "DeviceManager.h"
#include "JobManager.h"
#include "SupervisorKeeper.h"
#include "crane/PluginClient.h"
#include "crane/String.h"

using Craned::g_config;
using Craned::Partition;

void ParseSupervisorConfig(const YAML::Node& supervisor_config) {
  using util::YamlValueOr;
  g_config.Supervisor.Path =
      YamlValueOr(supervisor_config["Path"], kDefaultSupervisorPath);
  if (!std::filesystem::exists(g_config.Supervisor.Path)) {
    fmt::print(stderr, "csupervisor {} does not exist\n",
               g_config.Supervisor.Path);
    std::exit(1);
  }
  g_config.Supervisor.DebugLevel =
      YamlValueOr(supervisor_config["DebugLevel"], "trace");
  if (!StrToLogLevel(g_config.Supervisor.DebugLevel).has_value()) {
    fmt::print(stderr,
               "Illegal Supervisor debug-level: {}, should be one of trace,"
               ", debug, info, warn, error, off.\n",
               g_config.Supervisor.DebugLevel);
    std::exit(1);
  }
  g_config.Supervisor.LogDir =
      g_config.CraneBaseDir /
      YamlValueOr(supervisor_config["LogDir"], "supervisor");
}

void ParseConfig(int argc, char** argv) {
  cxxopts::Options options("craned");

  // clang-format off
  options.add_options()
      ("f,config-file", "Path to configuration file",
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
      ("C,nodeinfo", "Print current node cpu and memory info")
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
    std::exit(0);
  }

  if (parsed_args.count("nodeinfo") > 0) {
    // Print CPU cores and memory information of current node.
    // Similar to `slurmd -C`.
    NodeSpecInfo node;
    if (!util::os::GetNodeInfo(&node)) {
      fmt::print(stderr, "Failed to get node info. Exiting...\n");
      std::exit(1);
    }

    fmt::print("Nodes:\n");
    fmt::print("  - name: {}\n", node.name);
    fmt::print("    cpu: {}\n", node.cpu);
    fmt::print("    memory: {:.2f}G\n", node.memory_gb);
    std::exit(0);
  }

  std::string config_path = parsed_args["config-file"].as<std::string>();
  std::unordered_map<std::string, std::vector<Craned::DeviceMetaInConfig>>
      each_node_device;
  if (std::filesystem::exists(config_path)) {
    try {
      using util::YamlValueOr;
      YAML::Node config = YAML::LoadFile(config_path);

      g_config.ConfigCrcVal = util::CalcConfigCRC32(config);

      g_config.CraneBaseDir =
          YamlValueOr(config["CraneBaseDir"], kDefaultCraneBaseDir);

      if (parsed_args.count("log-file"))
        g_config.CranedLogFile = parsed_args["log-file"].as<std::string>();
      else
        g_config.CranedLogFile =
            g_config.CraneBaseDir /
            YamlValueOr(config["CranedLogFile"], kDefaultCranedLogPath);

      if (parsed_args.count("debug-level"))
        g_config.CranedDebugLevel =
            parsed_args["debug-level"].as<std::string>();
      else
        g_config.CranedDebugLevel =
            YamlValueOr(config["CranedDebugLevel"], "info");

      if (config["Supervisor"]) {
        ParseSupervisorConfig(config["Supervisor"]);
      } else {
        fmt::print(stderr, "No Supervisor configuration found.\n");
        std::exit(1);
      }

      // spdlog should be initialized as soon as possible
      std::optional log_level = StrToLogLevel(g_config.CranedDebugLevel);
      if (log_level.has_value()) {
        InitLogger(log_level.value(), g_config.CranedLogFile, true);
      } else {
        fmt::print(stderr, "Illegal Craned debug-level format: {}.\n",
                   g_config.CranedDebugLevel);
        std::exit(1);
      }

      g_config.CranedUnixSockPath =
          g_config.CraneBaseDir /
          YamlValueOr(config["CranedUnixSockPath"], kDefaultCranedUnixSockPath);

      g_config.CranedForPamUnixSockPath =
          g_config.CraneBaseDir /
          YamlValueOr(config["CranedForPamUnixSockPath"],
                      kDefaultCranedForPamUnixSockPath);

      g_config.CranedScriptDir =
          g_config.CraneBaseDir /
          YamlValueOr(config["CranedScriptDir"], kDefaultCranedScriptDir);

      g_config.CranedMutexFilePath =
          g_config.CraneBaseDir /
          YamlValueOr(config["CranedMutexFilePath"], kDefaultCranedMutexFile);

      // Parsing node hostnames needs network functions, initialize it first.
      crane::InitializeNetworkFunctions();

      g_config.ListenConf.CranedListenAddr =
          YamlValueOr(config["CranedListen"], kDefaultHost);

      g_config.ListenConf.CranedListenPort =
          YamlValueOr(config["CranedListenPort"], kCranedDefaultPort);

      g_config.ListenConf.UnixSocketListenAddr =
          fmt::format("unix://{}", g_config.CranedUnixSockPath);

      g_config.ListenConf.UnixSocketForPamListenAddr =
          fmt::format("unix://{}", g_config.CranedForPamUnixSockPath);

      g_config.CompressedRpc =
          YamlValueOr<bool>(config["CompressedRpc"], false);

      if (config["TLS"]) {
        auto& g_tls_config = g_config.ListenConf.TlsConfig;

        TlsCertificates& tls_certs = g_tls_config.TlsCerts;

        const auto& tls_config = config["TLS"];

        if (tls_config["Enabled"])
          g_tls_config.Enabled = tls_config["Enabled"].as<bool>();

        if (g_tls_config.Enabled) {
          g_tls_config.DomainSuffix =
              YamlValueOr(tls_config["DomainSuffix"], "");

          if (auto result = util::ParseCertConfig(
                  "InternalCertFilePath", tls_config, &tls_certs.CertFilePath,
                  &tls_certs.CertContent);
              result) {
            CRANE_ERROR(result.value());
            std::exit(1);
          }
          if (auto result = util::ParseCertConfig(
                  "InternalKeyFilePath", tls_config, &tls_certs.KeyFilePath,
                  &tls_certs.KeyContent);
              result) {
            CRANE_ERROR(result.value());
            std::exit(1);
          }
          if (auto result = util::ParseCertConfig(
                  "InternalCaFilePath", tls_config, &tls_certs.CaFilePath,
                  &tls_certs.CaContent);
              result) {
            CRANE_ERROR(result.value());
            std::exit(1);
          }
        }
      }

      if (config["ControlMachine"]) {
        g_config.ControlMachine = config["ControlMachine"].as<std::string>();
      } else {
        CRANE_ERROR("ControlMachine is not configured.");
        std::exit(1);
      }

      g_config.CraneCtldForInternalListenPort =
          YamlValueOr(config["CraneCtldForInternalListenPort"],
                      kCtldForInternalDefaultPort);

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
          auto act_nodes_str = absl::StripAsciiWhitespace(nodes);
          if (act_nodes_str == "ALL") {
            for (const auto& [node, _] : g_config.CranedRes) {
              part.nodes.emplace(node);
              CRANE_INFO("Find node {} in partition {}", node, name);
            }
          } else {
            if (!util::ParseHostList(std::string(act_nodes_str), &name_list)) {
              CRANE_ERROR("Illegal node name string format.");
              std::exit(1);
            }
            if (name_list.empty()) {
              CRANE_WARN("No nodes in partition '{}'.", name);
            } else {
              for (auto&& node : name_list) {
                auto node_it = g_config.CranedRes.find(node);
                if (node_it != g_config.CranedRes.end()) {
                  part.nodes.emplace(node_it->first);
                  CRANE_INFO("Find node {} in partition {}", node_it->first,
                             name);
                } else {
                  CRANE_ERROR(
                      "Unknown node '{}' found in partition '{}'. It is "
                      "ignored "
                      "and should be contained in the configuration file.",
                      node, name);
                }
              }
            }
          }

          g_config.Partitions.emplace(std::move(name), std::move(part));
        }

        g_config.CranedForeground =
            YamlValueOr<bool>(config["CranedForeground"], false);

        if (config["Container"]) {
          const auto& container_config = config["Container"];

          g_config.Container.Enabled =
              YamlValueOr<bool>(container_config["Enabled"], false);

          if (g_config.Container.Enabled) {
            g_config.Container.TempDir =
                g_config.CraneBaseDir / YamlValueOr(container_config["TempDir"],
                                                    kDefaultContainerTempDir);

            if (container_config["RuntimeBin"]) {
              g_config.Container.RuntimeBin =
                  container_config["RuntimeBin"].as<std::string>();
            } else {
              CRANE_ERROR("RuntimeBin is not configured.");
              std::exit(1);
            }

            if (container_config["RuntimeState"]) {
              g_config.Container.RuntimeState =
                  container_config["RuntimeState"].as<std::string>();
            } else {
              CRANE_ERROR("RuntimeState is not configured.");
              std::exit(1);
            }

            if (container_config["RuntimeKill"]) {
              g_config.Container.RuntimeKill =
                  container_config["RuntimeKill"].as<std::string>();
            } else {
              CRANE_ERROR("RuntimeKill is not configured.");
              std::exit(1);
            }

            if (container_config["RuntimeDelete"]) {
              g_config.Container.RuntimeDelete =
                  container_config["RuntimeDelete"].as<std::string>();
            } else {
              CRANE_ERROR("RuntimeDelete is not configured.");
              std::exit(1);
            }

            if (container_config["RuntimeRun"]) {
              g_config.Container.RuntimeRun =
                  container_config["RuntimeRun"].as<std::string>();
            } else {
              CRANE_ERROR("RuntimeRun is not configured.");
              std::exit(1);
            }
          }
        }

        if (config["Plugin"]) {
          const auto& plugin_config = config["Plugin"];
          g_config.Plugin.Enabled =
              YamlValueOr<bool>(plugin_config["Enabled"], false);
          g_config.Plugin.PlugindSockPath =
              fmt::format("unix://{}{}", g_config.CraneBaseDir,
                          YamlValueOr(plugin_config["PlugindSockPath"],
                                      kDefaultPlugindUnixSockPath));
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

  std::array<char, HOST_NAME_MAX + 1> hostname{};
  int rc = gethostname(hostname.data(), hostname.size());
  if (rc != 0) {
    CRANE_ERROR("Error: get hostname.");
    std::exit(1);
  }
  g_config.Hostname.assign(hostname.data());

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
  g_config.CranedMeta.NetworkInterfaces = crane::GetNetworkInterfaces();
}

void CreateRequiredDirectories() {
  bool ok;
  ok = util::os::CreateFolders(g_config.CranedScriptDir);
  if (!ok) std::exit(1);

  ok = util::os::CreateFoldersForFile(g_config.CranedLogFile);
  if (!ok) std::exit(1);
}

void Recover(const crane::grpc::ConfigureCranedRequest& config_from_ctld) {
  // FIXME: Ctld cancel job after Configure Request sent, will keep invalid jobs
  // FIXME: Add API InitAndRetryToRecoverJobs(Expected Job List) -> Result

  using Craned::JobInD, Craned::StepInstance;

  std::vector ctld_job_ids = config_from_ctld.job_map() | std::views::keys |
                             std::ranges::to<std::vector<task_id_t>>();
  CRANE_DEBUG("CraneCtld claimed {} jobs are running on this node: [{}]",
              ctld_job_ids.size(), absl::StrJoin(ctld_job_ids, ","));

  CraneExpected<std::unordered_map<task_id_t, pid_t>> steps =
      g_supervisor_keeper->InitAndGetRecoveredMap();

  // JobId,supervisor pid
  std::unordered_map<task_id_t, pid_t> job_supv_pid_map;
  if (steps.has_value()) job_supv_pid_map = steps.value();

  // All job ids from supervisor
  auto supv_job_ids_view = job_supv_pid_map | std::views::keys;
  std::unordered_set<task_id_t> supv_job_ids(supv_job_ids_view.begin(),
                                             supv_job_ids_view.end());
  if (!supv_job_ids.empty()) {
    CRANE_TRACE("[Supervisor] job [{}] still running.",
                absl::StrJoin(supv_job_ids, ","));
  }

  std::unordered_map<task_id_t, JobInD> job_map(
      config_from_ctld.job_map().begin(), config_from_ctld.job_map().end());

  std::unordered_map<task_id_t, std::unique_ptr<StepInstance>> step_map;
  step_map.reserve(config_from_ctld.job_tasks_map_size());

  for (const auto& [job_id, step_to_d] : config_from_ctld.job_tasks_map()) {
    if (supv_job_ids.erase(job_id) > 0) {  // job_id is in supervisor recovery
      auto step_inst = std::make_unique<StepInstance>(
          step_to_d, job_supv_pid_map.at(job_id));
      step_map.emplace(job_id, std::move(step_inst));
    } else {
      // Remove lost step's invalid job
      job_map.erase(job_id);
    }
  }

  Craned::CgroupManager::TryToRecoverCgForJobs(job_map);
  g_job_mgr->Recover(std::move(job_map), std::move(step_map));

  if (!supv_job_ids.empty()) {
    CRANE_ERROR("[Supervisor] job {} is not recorded in Ctld.",
                absl::StrJoin(supv_job_ids, ","));
  }

  g_server->MarkSupervisorAsRecovered();
}

void GlobalVariableInit() {
  CreateRequiredDirectories();

  // Mask SIGPIPE to prevent Craned from crushing due to
  // SIGPIPE while communicating with spawned task processes.
  signal(SIGPIPE, SIG_IGN);

  PasswordEntry::InitializeEntrySize();

  // It is always ok to create thread pool first.
  g_thread_pool =
      std::make_unique<BS::thread_pool>(std::thread::hardware_concurrency());

  g_supervisor_keeper = std::make_unique<Craned::SupervisorKeeper>();

  using Craned::CgroupManager;
  using Craned::CgConstant::Controller;
  CgroupManager::Init();
  if (CgroupManager::GetCgroupVersion() ==
          Craned::CgConstant::CgroupVersion::CGROUP_V1 &&
      (!CgroupManager::IsMounted(Controller::CPU_CONTROLLER) ||
       !CgroupManager::IsMounted(Controller::MEMORY_CONTROLLER) ||
       !CgroupManager::IsMounted(Controller::DEVICES_CONTROLLER) ||
       !CgroupManager::IsMounted(Controller::BLOCK_CONTROLLER))) {
    CRANE_ERROR(
        "Failed to initialize cpu,memory,devices,block cgroups controller.");
    std::exit(1);
  }
  if (CgroupManager::GetCgroupVersion() ==
          Craned::CgConstant::CgroupVersion::CGROUP_V2 &&
      (!CgroupManager::IsMounted(Controller::CPU_CONTROLLER_V2) ||
       !CgroupManager::IsMounted(Controller::MEMORY_CONTROLLER_V2) ||
       !CgroupManager::IsMounted(Controller::IO_CONTROLLER_V2))) {
    CRANE_ERROR("Failed to initialize cpu,memory,IO cgroups controller.");
    std::exit(1);
  }

  g_server = std::make_unique<Craned::CranedServer>(g_config.ListenConf);

  g_job_mgr = std::make_unique<Craned::JobManager>();
  g_job_mgr->SetSigintCallback([] {
    g_server->Shutdown();
    g_craned_for_pam_server->Shutdown();
    CRANE_INFO("Grpc Server Shutdown() was called.");
  });

  g_ctld_client_sm = std::make_unique<Craned::CtldClientStateMachine>();
  g_ctld_client = std::make_unique<Craned::CtldClient>();

  g_ctld_client_sm->AddActionConfigureCb(
      [](const Craned::CtldClientStateMachine::ConfigureArg& arg) {
        Recover(arg.req);
      },
      Craned::CallbackInvokeMode::SYNC, true);

  g_ctld_client->Init();
  g_ctld_client->SetCranedId(g_config.CranedIdOfThisNode);

  g_ctld_client->InitGrpcChannel(g_config.ControlMachine);

  if (g_config.Plugin.Enabled) {
    CRANE_INFO("[Plugin] Plugin module is enabled.");
    g_plugin_client = std::make_unique<plugin::PluginClient>();
    g_plugin_client->InitChannelAndStub(g_config.Plugin.PlugindSockPath);
  }

  g_craned_for_pam_server =
      std::make_unique<Craned::CranedForPamServer>(g_config.ListenConf);

  // Make sure all grpc server is ready to receive requests.
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
}

void WaitForStopAndDoGvarFini() {
  /*
   * JobMgr is ending
   * g_server and g_craned_for_pam_server Shutdown() called
   */
  g_server->Wait();

  /*
   * No more status change from supervisor, clean up CtldClient, stop register
   * this craned
   */
  g_ctld_client->Shutdown();
  g_craned_for_pam_server->Wait();

  g_server.reset();
  g_craned_for_pam_server.reset();

  /* Called from
   * PAM_SERVER, G_SERVER
   * CtldClient: ActionConfigureCb
   */
  g_job_mgr->Wait();

  /*
   * Called from JobMgr and CtldClient, wait all thread pool task finish before
   * destruct.
   */
  g_thread_pool->wait();
  g_job_mgr.reset();

  g_ctld_client.reset();
  // After ctld client destroyed, it is ok to destroy ctld client state machine
  g_ctld_client_sm.reset();

  /*
   * Called from G_SERVER, JobMgr
   * CtldClient: ActionConfigureCb
   */
  g_supervisor_keeper.reset();

  g_thread_pool.reset();

  // Plugin client must be destroyed after the thread pool.
  // It may be called in the thread pool.
  g_plugin_client.reset();

  std::exit(0);
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

  g_ctld_client->StartGrpcCtldConnection();

  WaitForStopAndDoGvarFini();
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

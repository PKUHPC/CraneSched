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

#ifdef CRANE_ENABLE_BPF
#  include <bpf/bpf.h>
#  include <bpf/libbpf.h>
#  include <linux/bpf.h>
#endif

#include <array>
#include <cxxopts.hpp>

#include "CgroupManager.h"
#include "CranedForPamServer.h"
#include "CranedServer.h"
#include "CtldClient.h"
#include "DeviceManager.h"
#include "JobManager.h"
#include "SupervisorKeeper.h"
#include "crane/CriClient.h"
#include "crane/PluginClient.h"
#include "crane/String.h"

using namespace Craned::Common;
using Craned::g_config;
using Craned::JobInD;

CraneErrCode RecoverCgForJobs(
    std::unordered_map<job_id_t, Craned::JobInD>& rn_jobs_from_ctld) {
  using namespace Craned;
  using namespace Common::CgConstant;

  std::set<job_id_t> rn_job_ids_with_cg;
  if (CgroupManager::GetCgroupVersion() == CgroupVersion::CGROUP_V1) {
    // FIXME: What about the case of inconsistency?

    rn_job_ids_with_cg.merge(
        CgroupManager::GetJobIdsFromCgroupV1_(Controller::CPU_CONTROLLER));
    rn_job_ids_with_cg.merge(
        CgroupManager::GetJobIdsFromCgroupV1_(Controller::MEMORY_CONTROLLER));
    rn_job_ids_with_cg.merge(
        CgroupManager::GetJobIdsFromCgroupV1_(Controller::DEVICES_CONTROLLER));

  } else if (CgroupManager::GetCgroupVersion() == CgroupVersion::CGROUP_V2) {
    rn_job_ids_with_cg = CgroupManager::GetJobIdsFromCgroupV2_(
        kSystemCgPathPrefix / kRootCgNamePrefix);

#ifdef CRANE_ENABLE_BPF
    auto job_id_bpf_key_vec_map = CgroupManager::GetJobBpfMapCgroupsV2_(
        kSystemCgPathPrefix / kRootCgNamePrefix);
    if (!job_id_bpf_key_vec_map) {
      CRANE_ERROR("Failed to read job ebpf info, skip recovery.");
      return CraneErrCode::ERR_EBPF;
    }

    for (const auto& [job_id, bpf_key_vec] : job_id_bpf_key_vec_map.value()) {
      if (rn_jobs_from_ctld.contains(job_id)) continue;

      CRANE_DEBUG("Erase bpf map entry for rn job {} not in Ctld.", job_id);
      for (const auto& key : bpf_key_vec) {
        if (bpf_map__delete_elem(CgroupManager::bpf_runtime_info.BpfDevMap(),
                                 &key, sizeof(BpfKey), BPF_ANY) < 0) {
          CRANE_ERROR(
              "Failed to delete BPF map major {},minor {} in cgroup id {}",
              key.major, key.minor, key.cgroup_id);
        }
      }
    }
#endif
  } else {
    CRANE_WARN("Error Cgroup version is not supported");
    return CraneErrCode::ERR_CGROUP;
  }

  for (job_id_t job_id : rn_job_ids_with_cg) {
    if (rn_jobs_from_ctld.contains(job_id)) {
      // Job is found in both ctld and cgroup
      JobInD& job = rn_jobs_from_ctld.at(job_id);

      CRANE_DEBUG("Recover existing cgroup for job #{}", job_id);
      auto cg_expt = CgroupManager::AllocateAndGetCgroup(
          CgroupManager::CgroupStrByJobId(job_id), job.job_to_d.res(), true);
      if (cg_expt.has_value()) {
        // Job cgroup is recovered.
        job.cgroup = std::move(cg_expt.value());
      } else {
        // If the cgroup is found but is unrecoverable, just logging out.
        CRANE_ERROR("Cgroup for job #{} is found but not recoverable.", job_id);
      }

      continue;
    }

    // Job is found in cgroup but not in ctld. Cleaning up.
    CRANE_DEBUG("Removing cgroup for job #{} not in Ctld.", job_id);
    std::unique_ptr<CgroupInterface> cg_ptr;

    // Open cgroup and destroy it.
    auto cg_str = CgroupManager::CgroupStrByJobId(job_id);

    // NOLINTBEGIN(readability-suspicious-call-argument)
    if (CgroupManager::GetCgroupVersion() == CgroupVersion::CGROUP_V1)
      cg_ptr = CgroupManager::CreateOrOpen_(cg_str, CG_V1_REQUIRED_CONTROLLERS,
                                            NO_CONTROLLER_FLAG, true);
    else if (CgroupManager::GetCgroupVersion() == CgroupVersion::CGROUP_V2)
      cg_ptr = CgroupManager::CreateOrOpen_(cg_str, CG_V2_REQUIRED_CONTROLLERS,
                                            NO_CONTROLLER_FLAG, true);
    else
      std::unreachable();

    if (!cg_ptr) {
      // FIXME: Clean incomplete cgroups!
      CRANE_ERROR("Failed to reopen cgroup of job #{}.", job_id);
      continue;
    }

    cg_ptr->KillAllProcesses();
    cg_ptr->Destroy();
  }

  return CraneErrCode::SUCCESS;
}

void ParseCranedConfig(const YAML::Node& config) {
  Craned::Config::CranedConfig conf{};
  using util::YamlValueOr;
  conf.PingIntervalSec = kCranedPingIntervalSec;
  conf.CtldTimeoutSec = Craned::kCtldClientTimeoutSec;
  if (config["Craned"]) {
    auto craned_config = config["Craned"];
    if (craned_config["PingInterval"])
      conf.PingIntervalSec = craned_config["PingInterval"].as<uint32_t>();
    if (craned_config["CraneCtldTimeout"])
      conf.CtldTimeoutSec = craned_config["CraneCtldTimeout"].as<uint32_t>();
    if (craned_config["CranedMaxLogFileSize"]) {
      auto file_size = util::ParseMemory(
          craned_config["CranedMaxLogFileSize"].as<std::string>());
      conf.CranedMaxLogFileSize = file_size.has_value()
                                      ? file_size.value()
                                      : kDefaultCranedMaxLogFileSize;
    }
    conf.CranedMaxLogFileNum = YamlValueOr<uint64_t>(
        craned_config["CranedMaxLogFileNum"], kDefaultCranedMaxLogFileNum);
  }
  g_config.CranedConf = std::move(conf);
}

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

  if (supervisor_config["MaxLogFileSize"]) {
    auto file_size = util::ParseMemory(
        supervisor_config["MaxLogFileSize"].as<std::string>());
    g_config.Supervisor.MaxLogFileSize = file_size.has_value()
                                             ? file_size.value()
                                             : kDefaultSupervisorMaxLogFileSize;
  }

  g_config.Supervisor.MaxLogFileNum = YamlValueOr<uint64_t>(
      supervisor_config["MaxLogFileNum"], kDefaultSupervisorMaxLogFileNum);
}

void ParseConfig(int argc, char** argv) {
  cxxopts::Options options("craned");

  // clang-format off
  options.add_options()
      ("f,config-file", "Path to configuration file",
      cxxopts::value<std::string>()->default_value(kDefaultConfigPath))
      ("P,plugin-config", "Path to Plugin configuration file",
       cxxopts::value<std::string>()->default_value(kDefaultPluginConfigPath))
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
  std::string plugin_config_path =
      parsed_args["plugin-config"].as<std::string>();
  std::unordered_map<std::string,
                     std::vector<Craned::Common::DeviceMetaInConfig>>
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

      ParseCranedConfig(config);

      // spdlog should be initialized as soon as possible
      std::optional log_level = StrToLogLevel(g_config.CranedDebugLevel);
      if (log_level.has_value()) {
        InitLogger(log_level.value(), g_config.CranedLogFile, true,
                   g_config.CranedConf.CranedMaxLogFileSize,
                   g_config.CranedConf.CranedMaxLogFileNum);
        Craned::g_runtime_status.conn_logger =
            AddLogger("conn", log_level.value(), true);
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
          if (auto result = util::ParseCertConfig("CaFilePath", tls_config,
                                                  &g_tls_config.CaFilePath,
                                                  &g_tls_config.CaContent);
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
            auto memory_bytes =
                util::ParseMemory(node["memory"].as<std::string>());
            if (memory_bytes.has_value()) {
              node_res->allocatable_res.memory_bytes = memory_bytes.value();
              node_res->allocatable_res.memory_sw_bytes = memory_bytes.value();
            } else {
              CRANE_ERROR("Illegal memory format.");
              std::exit(1);
            }
          } else
            std::exit(1);

          std::vector<DeviceMetaInConfig> devices;
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
          Craned::Partition part;

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

            if (container_config["RuntimeEndpoint"]) {
              g_config.Container.RuntimeEndpoint =
                  container_config["RuntimeEndpoint"].as<std::string>();
            } else {
              CRANE_ERROR("RuntimeEndpoint is not configured.");
              std::exit(1);
            }

            // In most cases, ImageEndpoint is the same as RuntimeEndpoint.
            g_config.Container.ImageEndpoint =
                YamlValueOr(container_config["ImageEndpoint"],
                            g_config.Container.RuntimeEndpoint.string());

            // Prepend unix protocol
            g_config.Container.RuntimeEndpoint =
                fmt::format("unix://{}", g_config.Container.RuntimeEndpoint);
            g_config.Container.ImageEndpoint =
                fmt::format("unix://{}", g_config.Container.ImageEndpoint);
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

  // Load plugin configuration from separate plugin.yaml file
  if (std::filesystem::exists(plugin_config_path)) {
    try {
      using util::YamlValueOr;
      YAML::Node plugin_config = YAML::LoadFile(plugin_config_path);

      if (plugin_config["Enabled"])
        g_config.Plugin.Enabled = plugin_config["Enabled"].as<bool>();

      g_config.Plugin.PlugindSockPath =
          fmt::format("unix://{}{}", g_config.CraneBaseDir,
                      YamlValueOr(plugin_config["PlugindSockPath"],
                                  kDefaultPlugindUnixSockPath));

      CRANE_INFO("Plugin config loaded from {}", plugin_config_path);
    } catch (YAML::BadFile& e) {
      CRANE_WARN("Can't open plugin config file {}: {}. Plugin disabled.",
                 plugin_config_path, e.what());
      g_config.Plugin.Enabled = false;
    }
  } else {
    CRANE_INFO("Plugin config file '{}' not found. Plugin disabled.",
               plugin_config_path);
    g_config.Plugin.Enabled = false;
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
      auto env_injector_enum = GetDeviceEnvInjectorFromStr(env_injector);
      if (env_injector_enum == InvalidInjector) {
        CRANE_ERROR("Invalid injector type:{} for device {}.",
                    env_injector.value_or("EmptyVal"), path_vec);
        std::exit(1);
      }

      std::unique_ptr dev = DeviceManager::ConstructDevice(name, type, path_vec,
                                                           env_injector_enum);
      if (!dev->Init()) {
        CRANE_ERROR("Access Device {} failed.", static_cast<std::string>(*dev));
        std::exit(1);
      }

      dev->slot_id = dev->device_file_metas.front().path;
      node_res->dedicated_res.name_type_slots_map[dev->name][dev->type].emplace(
          dev->slot_id);
      g_this_node_device[dev->slot_id] = std::move(dev);
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
  std::map<job_id_t, std::set<step_id_t>> ctld_step_ids;

  for (const auto& [job_id, job_steps] : config_from_ctld.job_steps()) {
    ctld_step_ids[job_id] =
        job_steps.steps() | std::views::keys | std::ranges::to<std::set>();
  }

  CRANE_DEBUG("CraneCtld claimed [{}] are running on this node.",
              util::JobStepsToString(ctld_step_ids));

  CraneExpected<absl::flat_hash_map<std::pair<job_id_t, step_id_t>,
                                    std::pair<pid_t, Craned::StepStatus>>>
      steps = g_supervisor_keeper->InitAndGetRecoveredMap();

  // JobId,supervisor pid
  absl::flat_hash_map<std::pair<job_id_t, step_id_t>,
                      std::pair<pid_t, Craned::StepStatus>>
      step_supv_map;
  if (steps.has_value()) step_supv_map = steps.value();

  // All job ids from supervisor
  std::map<job_id_t, std::set<step_id_t>> supv_step_ids;
  for (const auto& [job_id, step_id] : step_supv_map | std::views::keys) {
    supv_step_ids[job_id].emplace(step_id);
  }

  if (!supv_step_ids.empty()) {
    CRANE_TRACE("[Supervisor] step [{}] still running.",
                util::JobStepsToString(supv_step_ids));
  }

  /* Filter valid job/steps (Intersection of job/steps form ctld/supervisor) */
  auto supv_jobs =
      supv_step_ids | std::views::keys | std::ranges::to<std::set<job_id_t>>();
  auto ctld_jobs =
      ctld_step_ids | std::views::keys | std::ranges::to<std::set<job_id_t>>();
  std::set<job_id_t> lost_jobs;
  std::set<job_id_t> valid_jobs;
  std::ranges::set_difference(ctld_jobs, supv_jobs,
                              std::inserter(lost_jobs, lost_jobs.end()));
  std::ranges::set_intersection(ctld_jobs, supv_jobs,
                                std::inserter(valid_jobs, valid_jobs.end()));
  CRANE_INFO("Job [{}] is lost when craned down. Valid jobs [{}].",
             absl::StrJoin(lost_jobs, ","), absl::StrJoin(valid_jobs, ","));

  std::unordered_map<job_id_t, std::set<step_id_t>> lost_steps{};
  std::unordered_map<job_id_t, std::set<step_id_t>> invalid_steps{};
  std::unordered_map<job_id_t, std::set<step_id_t>> valid_steps{};
  for (auto job_id : valid_jobs) {
    const auto& ctld_steps = ctld_step_ids.at(job_id);
    const auto& craned_steps = supv_step_ids.at(job_id);
    std::ranges::set_difference(
        ctld_steps, craned_steps,
        std::inserter(lost_steps[job_id], lost_steps[job_id].end()));
    std::ranges::set_difference(
        craned_steps, ctld_steps,
        std::inserter(invalid_steps[job_id], invalid_steps[job_id].end()));
    std::ranges::set_intersection(
        craned_steps, ctld_steps,
        std::inserter(valid_steps[job_id], valid_steps[job_id].end()));
  }

  // For lost jobs, all steps are lost
  for (auto job_id : lost_jobs) {
    lost_steps[job_id].merge(ctld_step_ids.at(job_id));
  }
  CRANE_INFO("Step [{}] is lost when craned down. Valid steps [{}].",
             util::JobStepsToString(lost_steps),
             util::JobStepsToString(valid_steps));

  /******************* Recover all JobInD and StepInstance *******************/
  absl::flat_hash_map<std::pair<job_id_t, step_id_t>,
                      std::unique_ptr<StepInstance>>
      step_map;
  step_map.reserve(supv_step_ids.size());

  for (const auto& [job_id, job_steps] : valid_steps) {
    const auto& proto_job_steps = config_from_ctld.job_steps().at(job_id);
    for (const auto& step_id : job_steps) {
      auto [pid, status] = step_supv_map.at(std::make_pair(job_id, step_id));
      auto step_inst = std::make_unique<StepInstance>(
          proto_job_steps.steps().at(step_id), pid, status);
      step_map.emplace(std::make_pair(job_id, step_id), std::move(step_inst));
    }
  }

  std::unordered_map<job_id_t, JobInD> job_map;
  job_map.reserve(valid_jobs.size());
  for (const auto& job_id : valid_jobs) {
    job_map.emplace(job_id,
                    JobInD{config_from_ctld.job_steps().at(job_id).job()});
  }

  /******************* Recover job cgroup info *******************/
  RecoverCgForJobs(job_map);

  /******************* Handle step status inconsistency *******************/
  using Craned::StepStatus;
  std::set<job_id_t> completing_jobs{};
  std::unordered_map<job_id_t, std::unordered_set<step_id_t>>
      completing_steps{};
  std::unordered_map<job_id_t, std::unordered_set<step_id_t>> failed_steps{};
  for (auto& [ids, step] : step_map) {
    auto [job_id, step_id] = ids;
    auto ctld_status =
        config_from_ctld.job_steps().at(job_id).step_status().at(step_id);
    auto supv_status = step_supv_map.at(std::make_pair(job_id, step_id)).second;
    CRANE_TRACE("[Step #{}.{}] Ctld status: {}, supervisor reported: {}",
                job_id, step_id, ctld_status, supv_status);

    // For ctld completing step, cleanup
    if (ctld_status == StepStatus::Completing) {
      if (step->IsDaemonStep()) {
        CRANE_TRACE("[Job #{}] is completing", job_id);
        completing_jobs.insert(job_id);
      } else {
        CRANE_TRACE("[Step #{}.{}] is completing", job_id, step_id);
        completing_steps[job_id].insert(step_id);
      }
      continue;
    }
    if (supv_status == StepStatus::Configured) {
      CRANE_ASSERT(!step->IsDaemonStep());
      // For configured but not running step, remove this, ctld will consider it
      // failed
      failed_steps[job_id].insert(step_id);
      CRANE_TRACE("[Step #{}.{}] is configured but not running, mark as failed",
                  job_id, step_id);
      continue;
    }
    if (supv_status != ctld_status) {
      CRANE_TRACE(
          "[Step #{}.{}] status inconsistency, ctld: {}, supervisor: {}, "
          "mark as failed.",
          job_id, step_id, ctld_status, supv_status);
      failed_steps[job_id].insert(step_id);
    }
  }
  for (auto [job_id, step_ids] : failed_steps) {
    for (auto step_id : step_ids) {
      auto stub = g_supervisor_keeper->GetStub(job_id, step_id);
      if (stub) {
        auto err = stub->TerminateTask(true, false);
        if (err != CraneErrCode::SUCCESS) {
          CRANE_ERROR("[Supervisor] Failed to terminate task #{}.{}", job_id,
                      step_id);
        }
      }
      step_map.erase({job_id, step_id});
    }
  }

  g_job_mgr->Recover(std::move(job_map), std::move(step_map));
  for (const auto& [job_id, step_ids] : invalid_steps)
    for (auto step_id : step_ids)
      g_supervisor_keeper->RemoveSupervisor(job_id, step_id);

  if (!invalid_steps.empty()) {
    CRANE_INFO("[Supervisor] step [{}] is not recorded in Ctld.",
               util::JobStepsToString(invalid_steps));
  }
  g_job_mgr->FreeJobs(std::move(completing_jobs));
  g_job_mgr->FreeSteps(std::move(completing_steps));
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

  using CgConstant::Controller;
  CgroupManager::Init(StrToLogLevel(g_config.CranedDebugLevel).value());
  if (CgroupManager::GetCgroupVersion() ==
          CgConstant::CgroupVersion::CGROUP_V1 &&
      (!CgroupManager::IsMounted(Controller::CPU_CONTROLLER) ||
       !CgroupManager::IsMounted(Controller::MEMORY_CONTROLLER) ||
       !CgroupManager::IsMounted(Controller::DEVICES_CONTROLLER) ||
       !CgroupManager::IsMounted(Controller::BLOCK_CONTROLLER))) {
    CRANE_ERROR(
        "Failed to initialize cpu,memory,devices,block cgroups controller.");
    std::exit(1);
  }
  if (CgroupManager::GetCgroupVersion() ==
          CgConstant::CgroupVersion::CGROUP_V2 &&
      (!CgroupManager::IsMounted(Controller::CPU_CONTROLLER_V2) ||
       !CgroupManager::IsMounted(Controller::MEMORY_CONTROLLER_V2) ||
       !CgroupManager::IsMounted(Controller::IO_CONTROLLER_V2))) {
    CRANE_ERROR("Failed to initialize cpu,memory,IO cgroups controller.");
    std::exit(1);
  }

  // If Container is enabled, connect to CRI runtime.
  if (g_config.Container.Enabled) {
    g_cri_client = std::make_unique<cri::CriClient>();
    g_cri_client->InitChannelAndStub(g_config.Container.RuntimeEndpoint,
                                     g_config.Container.ImageEndpoint);
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

  /*
   * Called in g_server.
   */
  g_cri_client.reset();

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

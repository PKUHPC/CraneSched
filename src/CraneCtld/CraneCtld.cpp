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

#include "CtldPreCompiledHeader.h"
// Precompiled header comes first!

#include <sys/file.h>
#include <sys/stat.h>
#include <yaml-cpp/yaml.h>

#include <cxxopts.hpp>
#include <filesystem>
#include <set>

#include "Account/AccountManager.h"
#include "Accounting/AccountMetaContainer.h"
#include "Accounting/LicenseManager.h"
#include "CtldPublicDefs.h"
#include "Database/DbClient.h"
#include "Database/EmbeddedDbClient.h"
#include "JobScheduler.h"
#include "Lua/LuaJobHandler.h"
#include "Node/CranedMetaContainer.h"
#include "Node/NodeManager.h"
#include "RpcService/CranedKeeper.h"
#include "RpcService/CtldGrpcServer.h"
#include "Security/VaultClient.h"
#include "crane/Network.h"
#include "crane/PluginClient.h"
#include "crane/String.h"
#include "crane/Tracing.h"
#ifdef CRANE_ENABLE_TRACING
#  include "crane/CraneSpanExporter.h"
#endif

void ParseCtldConfig(const YAML::Node& config) {
  using util::YamlValueOr;
  Ctld::Config::CraneCtldConf ctld_config{};
  ctld_config.CranedTimeout = kCranedTimeoutSec;
  ctld_config.MaxLogFileSize = kDefaultCraneCtldMaxLogFileSize;
  ctld_config.MaxLogFileNum = kDefaultCraneCtldMaxLogFileNum;

  if (config["CraneCtld"]) {
    auto ctld_cfg = config["CraneCtld"];
    if (ctld_cfg["CranedTimeout"])
      ctld_config.CranedTimeout = ctld_cfg["CranedTimeout"].as<uint32_t>();

    if (ctld_cfg["MaxLogFileSize"]) {
      auto file_size =
          util::ParseMemory(ctld_cfg["MaxLogFileSize"].as<std::string>());
      if (file_size.has_value()) {
        ctld_config.MaxLogFileSize = file_size.value();
      } else {
        CRANE_ERROR("Illegal memory format.");
        std::exit(1);
      }
    }

    if (ctld_cfg["MaxLogFileNum"]) {
      ctld_config.MaxLogFileNum = ctld_cfg["MaxLogFileNum"].as<uint64_t>();
    }

    ctld_config.ThreadPoolSize =
        YamlValueOr<uint32_t>(ctld_cfg["ThreadPoolSize"], 0);
    ctld_config.SchedulerRpcThreadPoolSize =
        YamlValueOr<uint32_t>(ctld_cfg["SchedulerRpcThreadPoolSize"], 0);
    ctld_config.SchedulerAllocJobsRpcTimeoutSeconds =
        YamlValueOr<uint32_t>(ctld_cfg["SchedulerAllocJobsRpcTimeoutSeconds"],
                              Ctld::kCtldRpcTimeoutSeconds);
    ctld_config.StatusChangeFlushTimeoutMs =
        YamlValueOr<uint32_t>(ctld_cfg["StatusChangeFlushTimeoutMs"],
                              Ctld::kJobStatusChangeTimeoutMS);
    ctld_config.StatusChangeBatchNum = YamlValueOr<uint32_t>(
        ctld_cfg["StatusChangeBatchNum"], Ctld::kJobStatusChangeBatchNum);
    ctld_config.StatusChangeMaxDrainPerTick =
        YamlValueOr<uint32_t>(ctld_cfg["StatusChangeMaxDrainPerTick"],
                              Ctld::kJobStatusChangeMaxDrainPerTick);
    ctld_config.StatusChangeDbCommitChunkSize =
        YamlValueOr<uint32_t>(ctld_cfg["StatusChangeDbCommitChunkSize"],
                              Ctld::kJobStatusChangeDbCommitChunkSize);

    ctld_config.JobRequeue =
        YamlValueOr<bool>(ctld_cfg["JobRequeue"], Ctld::kDefaultJobRequeue);
    ctld_config.MaxRequeueCount = YamlValueOr<int32_t>(
        ctld_cfg["MaxRequeueCount"], Ctld::kDefaultMaxRequeueCount);
    ctld_config.MaxNodeCount =
        YamlValueOr<uint32_t>(ctld_cfg["MaxNodeCount"], 0);

    if (ctld_cfg["DynamicNodes"]) {
      const auto& dynamic_cfg = ctld_cfg["DynamicNodes"];
      ctld_config.DynamicNodes.Enabled =
          YamlValueOr<bool>(dynamic_cfg["Enabled"], false);
      ctld_config.DynamicNodes.AutoCreate =
          YamlValueOr<bool>(dynamic_cfg["AutoCreate"], false);
      ctld_config.DynamicNodes.RegistrationLeaseSeconds =
          YamlValueOr<uint32_t>(dynamic_cfg["RegistrationLeaseSeconds"], 30);
      ctld_config.DynamicNodes.MaxAutoCreateNodes =
          YamlValueOr<uint32_t>(dynamic_cfg["MaxAutoCreateNodes"], 0);
      ctld_config.DynamicNodes.TombstoneRetentionSeconds =
          YamlValueOr<uint32_t>(dynamic_cfg["TombstoneRetentionSeconds"],
                                24 * 60 * 60);
      ctld_config.DynamicNodes.PowerActionTimeoutSeconds =
          YamlValueOr<uint32_t>(dynamic_cfg["PowerActionTimeoutSeconds"], 300);
      if (ctld_config.DynamicNodes.RegistrationLeaseSeconds == 0) {
        CRANE_ERROR(
            "CraneCtld.DynamicNodes.RegistrationLeaseSeconds must be greater "
            "than zero.");
        std::exit(1);
      }
      if (ctld_config.DynamicNodes.TombstoneRetentionSeconds == 0) {
        CRANE_ERROR(
            "CraneCtld.DynamicNodes.TombstoneRetentionSeconds must be greater "
            "than zero.");
        std::exit(1);
      }
      if (ctld_config.DynamicNodes.PowerActionTimeoutSeconds == 0) {
        CRANE_ERROR(
            "CraneCtld.DynamicNodes.PowerActionTimeoutSeconds must be greater "
            "than zero.");
        std::exit(1);
      }

      if (dynamic_cfg["AutoCreatePools"]) {
        if (!dynamic_cfg["AutoCreatePools"].IsSequence()) {
          CRANE_ERROR(
              "CraneCtld.DynamicNodes.AutoCreatePools must be a sequence.");
          std::exit(1);
        }

        std::unordered_set<std::string> pool_names;
        for (const auto& pool_cfg : dynamic_cfg["AutoCreatePools"]) {
          Ctld::Config::CraneCtldConf::DynamicNodeConfig::AutoCreatePool pool;
          pool.Name = YamlValueOr<std::string>(pool_cfg["Name"], "");
          pool.NodeNamePattern =
              YamlValueOr<std::string>(pool_cfg["NodeNamePattern"], "");
          if (pool.Name.empty() || !pool_names.emplace(pool.Name).second) {
            CRANE_ERROR("Dynamic node AutoCreate pool names must be unique.");
            std::exit(1);
          }
          if (pool.NodeNamePattern.empty()) {
            CRANE_ERROR("AutoCreate pool {} requires NodeNamePattern.",
                        pool.Name);
            std::exit(1);
          }
          try {
            static_cast<void>(std::regex(pool.NodeNamePattern));
          } catch (const std::regex_error&) {
            CRANE_ERROR("AutoCreate pool {} has an invalid NodeNamePattern.",
                        pool.Name);
            std::exit(1);
          }

          if (!pool_cfg["Partitions"] || !pool_cfg["Partitions"].IsSequence()) {
            CRANE_ERROR("AutoCreate pool {} requires Partitions.", pool.Name);
            std::exit(1);
          }
          std::unordered_set<std::string> partition_names;
          for (auto partition :
               pool_cfg["Partitions"].as<std::vector<std::string>>()) {
            if (partition.empty() ||
                !partition_names.emplace(partition).second) {
              CRANE_ERROR(
                  "AutoCreate pool {} contains an invalid partition list.",
                  pool.Name);
              std::exit(1);
            }
            pool.Partitions.emplace_back(std::move(partition));
          }
          if (pool.Partitions.empty()) {
            CRANE_ERROR("AutoCreate pool {} requires Partitions.", pool.Name);
            std::exit(1);
          }

          auto parse_features = [&](std::string_view key,
                                    std::unordered_set<std::string>* output) {
            if (!pool_cfg[std::string(key)]) return;
            if (!pool_cfg[std::string(key)].IsSequence()) {
              CRANE_ERROR("AutoCreate pool {} field {} must be a sequence.",
                          pool.Name, key);
              std::exit(1);
            }
            for (const auto& feature :
                 pool_cfg[std::string(key)].as<std::vector<std::string>>()) {
              if (feature.empty() || !output->emplace(feature).second) {
                CRANE_ERROR(
                    "AutoCreate pool {} field {} contains invalid features.",
                    pool.Name, key);
                std::exit(1);
              }
            }
          };
          parse_features("RequiredFeatures", &pool.RequiredFeatures);
          if (pool_cfg["AllowedFeatures"])
            parse_features("AllowedFeatures", &pool.AllowedFeatures);
          else
            pool.AllowedFeatures = pool.RequiredFeatures;
          if (!std::ranges::all_of(
                  pool.RequiredFeatures, [&](const auto& feature) {
                    return pool.AllowedFeatures.contains(feature);
                  })) {
            CRANE_ERROR("AutoCreate pool {} RequiredFeatures must be allowed.",
                        pool.Name);
            std::exit(1);
          }

          pool.MinCpu = YamlValueOr<uint32_t>(pool_cfg["MinCpu"], 0);
          pool.MaxCpu = YamlValueOr<uint32_t>(pool_cfg["MaxCpu"], 0);
          pool.MinSockets = YamlValueOr<uint32_t>(pool_cfg["MinSockets"], 0);
          pool.MaxSockets = YamlValueOr<uint32_t>(pool_cfg["MaxSockets"], 0);
          pool.MaxNodes = YamlValueOr<uint32_t>(pool_cfg["MaxNodes"], 0);
          if (!pool_cfg["MinMemory"] || !pool_cfg["MaxMemory"]) {
            CRANE_ERROR("AutoCreate pool {} requires MinMemory and MaxMemory.",
                        pool.Name);
            std::exit(1);
          }
          auto min_memory =
              util::ParseMemory(pool_cfg["MinMemory"].as<std::string>());
          auto max_memory =
              util::ParseMemory(pool_cfg["MaxMemory"].as<std::string>());
          if (!min_memory || !max_memory) {
            CRANE_ERROR("AutoCreate pool {} has invalid memory bounds.",
                        pool.Name);
            std::exit(1);
          }
          pool.MinMemoryBytes = *min_memory;
          pool.MaxMemoryBytes = *max_memory;
          if (pool.MinCpu == 0 || pool.MaxCpu < pool.MinCpu ||
              pool.MinMemoryBytes == 0 ||
              pool.MaxMemoryBytes < pool.MinMemoryBytes ||
              pool.MinSockets == 0 || pool.MaxSockets < pool.MinSockets ||
              pool.MaxSockets > pool.MaxCpu || pool.MaxNodes == 0) {
            CRANE_ERROR("AutoCreate pool {} has invalid resource bounds.",
                        pool.Name);
            std::exit(1);
          }

          if (pool_cfg["Gres"]) {
            if (!pool_cfg["Gres"].IsSequence()) {
              CRANE_ERROR("AutoCreate pool {} Gres must be a sequence.",
                          pool.Name);
              std::exit(1);
            }
            std::set<std::pair<std::string, std::string>> gres_keys;
            for (const auto& gres_cfg : pool_cfg["Gres"]) {
              Ctld::Config::CraneCtldConf::DynamicNodeConfig::GresRange range{
                  .Name = YamlValueOr<std::string>(gres_cfg["Name"], ""),
                  .Type = YamlValueOr<std::string>(gres_cfg["Type"], ""),
                  .Min = YamlValueOr<uint64_t>(gres_cfg["Min"], 0),
                  .Max = YamlValueOr<uint64_t>(gres_cfg["Max"], 0)};
              if (range.Name.empty() || range.Max == 0 ||
                  range.Max < range.Min ||
                  !gres_keys.emplace(range.Name, range.Type).second) {
                CRANE_ERROR("AutoCreate pool {} contains invalid GRES bounds.",
                            pool.Name);
                std::exit(1);
              }
              pool.Gres.emplace_back(std::move(range));
            }
          }
          ctld_config.DynamicNodes.AutoCreatePools.emplace_back(
              std::move(pool));
        }
      }
    }
  }

  g_config.CtldConf = std::move(ctld_config);
}

void ParseConfig(int argc, char** argv) {
  using util::YamlValueOr;
  cxxopts::Options options("cranectld");

  // clang-format off
  options.add_options()
      ("C,config", "Path to configuration file",
      cxxopts::value<std::string>()->default_value(kDefaultConfigPath))
      ("D,db-config", "Path to DB configuration file",
       cxxopts::value<std::string>()->default_value(kDefaultDbConfigPath))
      ("P,plugin-config", "Path to Plugin configuration file",
       cxxopts::value<std::string>()->default_value(kDefaultPluginConfigPath))
      ("l,listen", "Listening address, format: <IP>:<port>",
      cxxopts::value<std::string>()->default_value("0.0.0.0"))
      ("p,port", "Listening port, format: <IP>:<port>",
      cxxopts::value<std::string>()->default_value(kCtldDefaultPort))
      ("v,version", "Display version information")
      ("h,help", "Display help for CraneCtld")
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

  std::string config_path = parsed_args["config"].as<std::string>();
  std::string db_config_path = parsed_args["db-config"].as<std::string>();
  std::string plugin_config_path =
      parsed_args["plugin-config"].as<std::string>();
  if (std::filesystem::exists(config_path)) {
    try {
      YAML::Node config = YAML::LoadFile(config_path);

      if (config["ClusterName"]) {
        g_config.CraneClusterName = config["ClusterName"].as<std::string>();
        if (g_config.CraneClusterName.empty()) {
          CRANE_ERROR("ClusterName is empty.");
          std::exit(1);
        }
      } else {
        CRANE_ERROR("ClusterName is empty.");
        std::exit(1);
      }

      g_config.ConfigCrcVal = util::CalcConfigCRC32(config);

      g_config.CraneBaseDir =
          YamlValueOr(config["CraneBaseDir"], kDefaultCraneBaseDir);

      g_config.CraneCtldLogFile =
          g_config.CraneBaseDir /
          YamlValueOr(config["CraneCtldLogFile"], kDefaultCraneCtldLogPath);

      g_config.CraneCtldDebugLevel =
          YamlValueOr(config["CraneCtldDebugLevel"], "info");

      ParseCtldConfig(config);

      // spdlog should be initialized as soon as possible
      std::optional log_level = StrToLogLevel(g_config.CraneCtldDebugLevel);
      if (log_level.has_value()) {
        InitLogger(log_level.value(), g_config.CraneCtldLogFile, true,
                   g_config.CtldConf.MaxLogFileSize,
                   g_config.CtldConf.MaxLogFileNum);
      } else {
        fmt::print(stderr, "Illegal debug-level format.");
        std::exit(1);
      }

      // External configuration file path
      if (!parsed_args.count("db-config") && config["DbConfigPath"]) {
        db_config_path = config["DbConfigPath"].as<std::string>();
      }

      g_config.CraneCtldMutexFilePath =
          g_config.CraneBaseDir / YamlValueOr(config["CraneCtldMutexFilePath"],
                                              kDefaultCraneCtldMutexFile);

      g_config.ListenConf.CraneCtldListenAddr =
          YamlValueOr(config["CraneCtldListenAddr"], "0.0.0.0");

      g_config.ListenConf.CraneCtldListenPort =
          YamlValueOr(config["CraneCtldListenPort"], kCtldDefaultPort);

      g_config.ListenConf.CraneCtldForInternalListenPort =
          YamlValueOr(config["CraneCtldForInternalListenPort"],
                      kCtldForInternalDefaultPort);

      if (config["JobLifecycleHook"]) {
        const auto& hook_config = config["JobLifecycleHook"];

        util::ParsePrologEpilogHookPaths(
            YamlValueOr(hook_config["CranectldProlog"], ""), config_path,
            &g_config.JobLifecycleHook.CranectldPrologs);
        util::ParsePrologEpilogHookPaths(
            YamlValueOr(hook_config["CranectldEpilog"], ""), config_path,
            &g_config.JobLifecycleHook.CranectldEpilogs);

        g_config.JobLifecycleHook.PrologTimeout =
            YamlValueOr<uint32_t>(hook_config["PrologTimeout"], 60);
        g_config.JobLifecycleHook.EpilogTimeout =
            YamlValueOr<uint32_t>(hook_config["EpilogTimeout"], 60);
        g_config.JobLifecycleHook.PrologEpilogTimeout =
            YamlValueOr<uint32_t>(hook_config["PrologEpilogTimeout"], 0);
        g_config.JobLifecycleHook.MaxOutputSize = YamlValueOr<uint64_t>(
            hook_config["MaxOutputSize"], kDefaultPrologOutputSize);
      }

      if (config["CompressedRpc"])
        g_config.CompressedRpc = config["CompressedRpc"].as<bool>();

      // Keepalived
      if (config["Keepalived"]) {
        auto& g_keepalived_config = g_config.KeepalivedConfig;
        const auto& keepalived_config = config["Keepalived"];
        if (keepalived_config["CraneSharedBaseDir"]) {
          g_keepalived_config.CraneSharedBaseDir =
              keepalived_config["CraneSharedBaseDir"].as<std::string>();
        } else {
          CRANE_ERROR(
              "Keepalived.CraneSharedBaseDir is not set in configuration "
              "file.");
          exit(1);
        }
        g_keepalived_config.CraneCtldAliveFile =
            g_config.CraneBaseDir /
            YamlValueOr(keepalived_config["CraneCtldAliveFile"],
                        kDefaultCraneCtldAlivePath);
        // When keepalived is set, the mutex file directory is located in
        // CraneSharedBaseDir.
        g_config.CraneCtldMutexFilePath =
            g_keepalived_config.CraneSharedBaseDir /
            YamlValueOr(config["CraneCtldMutexFilePath"],
                        kDefaultCraneCtldMutexFile);
      }

      if (config["TLS"]) {
        auto& g_tls_config = g_config.ListenConf.TlsConfig;

        const auto& tls_config = config["TLS"];

        if (tls_config["Enabled"])
          g_tls_config.Enabled = tls_config["Enabled"].as<bool>();

        if (g_tls_config.Enabled) {
          if (tls_config["DomainSuffix"])
            g_tls_config.DomainSuffix =
                tls_config["DomainSuffix"].as<std::string>();

          if (tls_config["AllowedNodes"]) {
            std::string nodes = tls_config["AllowedNodes"].as<std::string>();
            std::list<std::string> name_list;
            if (!util::ParseHostList(absl::StripAsciiWhitespace(nodes).data(),
                                     &name_list)) {
              CRANE_ERROR("Illegal login node name string format.");
              std::exit(1);
            }
            for (const auto& name : name_list) {
              g_tls_config.AllowedNodes.insert(name);
              g_tls_config.AllowedNodes.insert(
                  fmt::format("{}.{}", name, g_tls_config.DomainSuffix));
            }
            // todo: localhost?
            g_tls_config.AllowedNodes.insert("localhost");
          }

          if (auto result = util::ParseCertConfig("CaFilePath", tls_config,
                                                  &g_tls_config.CaFilePath,
                                                  &g_tls_config.CaContent);
              result) {
            CRANE_ERROR(result.value());
            std::exit(1);
          }

          // internal
          if (auto result = util::ParseCertConfig(
                  "InternalCertFilePath", tls_config,
                  &g_tls_config.InternalCerts.CertFilePath,
                  &g_tls_config.InternalCerts.CertContent);
              result) {
            CRANE_ERROR(result.value());
            std::exit(1);
          }

          if (auto result =
                  util::ParseCertConfig("InternalKeyFilePath", tls_config,
                                        &g_tls_config.InternalCerts.KeyFilePath,
                                        &g_tls_config.InternalCerts.KeyContent);
              result) {
            CRANE_ERROR(result.value());
            std::exit(1);
          }

          // external
          if (auto result = util::ParseCertConfig(
                  "ExternalCertFilePath", tls_config,
                  &g_tls_config.ExternalCerts.CertFilePath,
                  &g_tls_config.ExternalCerts.CertContent);
              result) {
            CRANE_ERROR(result.value());
            std::exit(1);
          }
          if (auto result =
                  util::ParseCertConfig("ExternalKeyFilePath", tls_config,
                                        &g_tls_config.ExternalCerts.KeyFilePath,
                                        &g_tls_config.ExternalCerts.KeyContent);
              result) {
            CRANE_ERROR(result.value());
            std::exit(1);
          }
        }
      }

      if (config["CraneCtldForeground"]) {
        g_config.CraneCtldForeground = config["CraneCtldForeground"].as<bool>();
      }

      g_config.JobSubmitLuaScript =
          YamlValueOr(config["JobSubmitLuaScript"], "");

      g_config.CranedListenConf.CranedListenPort =
          YamlValueOr(config["CranedListenPort"], kCranedDefaultPort);

      g_config.PriorityConfig.MaxAge = kPriorityDefaultMaxAge;
      if (config["PriorityMaxAge"]) {
        std::string max_age = config["PriorityMaxAge"].as<std::string>();

        std::regex pattern_hour_min_sec(R"((\d+):(\d+):(\d+))");
        std::regex pattern_day_hour(R"((\d+)-(\d+))");
        std::regex pattern_min(R"((\d+))");
        std::regex pattern_day_hour_min_sec(R"((\d+)-(\d+):(\d+):(\d+))");
        std::smatch matches;

        uint64_t day, hour, minute, second;
        if (std::regex_match(max_age, matches, pattern_hour_min_sec)) {
          hour = std::stoi(matches[1]);
          minute = std::stoi(matches[2]);
          second = std::stoi(matches[3]);

          g_config.PriorityConfig.MaxAge = hour * 3600 + minute * 60 + second;
        } else if (std::regex_match(max_age, matches, pattern_day_hour)) {
          day = std::stoi(matches[1]);
          hour = std::stoi(matches[2]);

          g_config.PriorityConfig.MaxAge = day * 24 * 3600 + hour * 3600;
        } else if (std::regex_match(max_age, pattern_min)) {
          minute = std::stoi(max_age);

          g_config.PriorityConfig.MaxAge = minute * 60;
        } else if (std::regex_match(max_age, pattern_day_hour_min_sec)) {
          day = std::stoi(matches[1]);
          hour = std::stoi(matches[2]);
          minute = std::stoi(matches[3]);
          second = std::stoi(matches[4]);

          g_config.PriorityConfig.MaxAge =
              day * 24 * 3600 + hour * 3600 + minute * 60 + second;
        }

        g_config.PriorityConfig.MaxAge =
            std::min(g_config.PriorityConfig.MaxAge, kPriorityDefaultMaxAge);
      }

      if (config["PriorityType"]) {
        std::string priority_type = config["PriorityType"].as<std::string>();
        if (priority_type == "priority/multifactor")
          g_config.PriorityConfig.Type = Ctld::Config::Priority::MultiFactor;
        else
          g_config.PriorityConfig.Type = Ctld::Config::Priority::Basic;
      }

      if (config["PriorityFavorSmall"])
        g_config.PriorityConfig.FavorSmall =
            config["PriorityFavorSmall"].as<bool>();

      if (config["PriorityWeightAge"])
        g_config.PriorityConfig.WeightAge =
            config["PriorityWeightAge"].as<uint32_t>();
      else
        g_config.PriorityConfig.WeightAge = 1000;

      if (config["PriorityWeightFairShare"])
        g_config.PriorityConfig.WeightFairShare =
            config["PriorityWeightFairShare"].as<uint32_t>();
      else
        g_config.PriorityConfig.WeightFairShare = 0;

      if (config["PriorityWeightJobSize"])
        g_config.PriorityConfig.WeightJobSize =
            config["PriorityWeightJobSize"].as<uint32_t>();
      else
        g_config.PriorityConfig.WeightJobSize = 0;

      if (config["PriorityWeightPartition"])
        g_config.PriorityConfig.WeightPartition =
            config["PriorityWeightPartition"].as<uint32_t>();
      else
        g_config.PriorityConfig.WeightPartition = 0;

      if (config["PriorityWeightQoS"])
        g_config.PriorityConfig.WeightQoS =
            config["PriorityWeightQoS"].as<uint32_t>();
      else
        g_config.PriorityConfig.WeightQoS = 0;

      if (config["PendingQueueMaxSize"]) {
        g_config.PendingQueueMaxSize =
            config["PendingQueueMaxSize"].as<uint32_t>();
        if (g_config.PendingQueueMaxSize > Ctld::kPendingQueueMaxSize) {
          CRANE_WARN(
              "The value of 'PendingQueueMaxSize' set in config file "
              "is too high and has been reset to default value {}",
              Ctld::kPendingQueueMaxSize);
          g_config.PendingQueueMaxSize = Ctld::kPendingQueueMaxSize;
        }
      } else {
        g_config.PendingQueueMaxSize = Ctld::kPendingQueueMaxSize;
      }

      if (config["ScheduledBatchSize"]) {
        g_config.ScheduledBatchSize =
            std::min(config["ScheduledBatchSize"].as<uint32_t>(),
                     Ctld::kMaxScheduledBatchSize);
      } else {
        g_config.ScheduledBatchSize = Ctld::kDefaultScheduledBatchSize;
      }

      if (config["Licenses"]) {
        for (auto it = config["Licenses"].begin();
             it != config["Licenses"].end(); ++it) {
          auto license = it->as<YAML::Node>();
          std::string name;
          uint32_t quantity;
          if (license["name"]) {
            name = license["name"].as<std::string>();
          } else {
            CRANE_ERROR("Illegal licenses name format.");
            std::exit(1);
          }
          if (license["quantity"]) {
            quantity = license["quantity"].as<uint32_t>();
          } else {
            CRANE_ERROR("Illegal licenses quantity format.");
            std::exit(1);
          }
          g_config.lic_id_to_count_map.emplace(name, quantity);
        }
      }

      g_config.AllLicenseResourcesAbsolute =
          YamlValueOr<bool>(config["AllLicenseResourcesAbsolute"], false);

      g_config.RejectJobsBeyondCapacity =
          YamlValueOr<bool>(config["RejectJobsBeyondCapacity"],
                            Ctld::kDefaultRejectJobsBeyondCapacity);

      if (config["JobFileAppend"]) {
        g_config.JobFileOpenModeAppend = config["JobFileAppend"].as<bool>();
      } else {
        g_config.JobFileOpenModeAppend = Ctld::kDefaultJobFileOpenModeAppend;
      }

      if (config["Nodes"]) {
        for (auto it = config["Nodes"].begin(); it != config["Nodes"].end();
             ++it) {
          auto node = it->as<YAML::Node>();
          auto node_ptr = std::make_shared<Ctld::Config::Node>();
          std::list<std::string> node_id_list;

          if (node["name"]) {
            if (!util::ParseHostList(node["name"].Scalar(), &node_id_list)) {
              CRANE_ERROR("Illegal node name string format.");
              std::exit(1);
            }

            CRANE_TRACE("node name list parsed: {}",
                        fmt::join(node_id_list, ", "));
          } else
            std::exit(1);

          if (node["cpu"])
            node_ptr->cpu = std::stoul(node["cpu"].as<std::string>());
          else
            std::exit(1);

          if (node["memory"]) {
            auto mem = util::ParseMemory(node["memory"].as<std::string>());
            if (mem.has_value()) {
              node_ptr->memory_bytes = mem.value();
            } else {
              CRANE_ERROR("Illegal memory format.");
              std::exit(1);
            }
          } else
            std::exit(1);

          // Parse optional sockets field (number of physical CPU sockets).
          // Default value is 1 when not specified.
          if (node["sockets"]) {
            uint32_t sockets_val = node["sockets"].as<uint32_t>(1);
            if (sockets_val == 0) {
              CRANE_ERROR("Invalid sockets=0 for node '{}'. Resetting to 1.",
                          node["name"].Scalar());
              sockets_val = 1;
            } else if (sockets_val > node_ptr->cpu) {
              CRANE_WARN(
                  "Sockets={} for node '{}' exceeds cpu count={}. "
                  "Resetting to 1.",
                  sockets_val, node["name"].Scalar(), node_ptr->cpu);
              sockets_val = 1;
            }
            node_ptr->node_topo_info.sockets = sockets_val;
          }

          DedicatedResourceInNode resourceInNode;
          if (node["gres"]) {
            for (auto gres_it = node["gres"].begin();
                 gres_it != node["gres"].end(); ++gres_it) {
              const auto& gres_node = gres_it->as<YAML::Node>();
              const auto& device_name = gres_node["name"].as<std::string>();
              const auto& device_type = gres_node["type"].as<std::string>();
              if (gres_node["DeviceFileRegex"]) {
                std::list<std::string> device_path_list;
                if (!util::ParseHostList(gres_node["DeviceFileRegex"].Scalar(),
                                         &device_path_list)) {
                  CRANE_ERROR(
                      "Illegal gres {}:{} DeviceFileRegex path string format.",
                      device_name, device_type);
                  std::exit(1);
                }
                for (const auto& device_path : device_path_list) {
                  resourceInNode.name_type_slots_map[device_name][device_type]
                      .emplace(device_path);
                }
              }

              if (gres_node["DeviceFileList"]) {
                if (!gres_node["DeviceFileList"].IsSequence()) {
                  CRANE_ERROR(
                      "Illegal gres {}:{} DeviceFileList type. It must be a "
                      "YAML sequence.",
                      device_name, device_type);
                  std::exit(1);
                }

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
                  if (device_path_list.empty()) {
                    CRANE_ERROR(
                        "Illegal gres {}:{} DeviceFileList entry expands to "
                        "empty device file list.",
                        device_name, device_type);
                    std::exit(1);
                  }
                  resourceInNode.name_type_slots_map[device_name][device_type]
                      .emplace(device_path_list.front());
                }
              }
            }
          }

          for (auto&& node_id : node_id_list) {
            g_config.Nodes[node_id] = node_ptr;
            g_config.Nodes[node_id]->dedicated_resource = resourceInNode;
          }
        }
      }

      if (g_config.CtldConf.MaxNodeCount == 0)
        g_config.CtldConf.MaxNodeCount = g_config.Nodes.size();
      if (g_config.CtldConf.MaxNodeCount < g_config.Nodes.size()) {
        CRANE_ERROR("MaxNodeCount {} is smaller than static node count {}.",
                    g_config.CtldConf.MaxNodeCount, g_config.Nodes.size());
        std::exit(1);
      }

      std::unordered_set nodes_without_part = g_config.Nodes |
                                              ranges::views::keys |
                                              ranges::to<std::unordered_set>();
      const std::list<std::string> all_node_list =
          g_config.Nodes | ranges::views::keys |
          ranges::to<std::list<std::string>>();
      if (config["Partitions"]) {
        for (auto it = config["Partitions"].begin();
             it != config["Partitions"].end(); ++it) {
          auto partition = it->as<YAML::Node>();
          std::string name;
          std::string nodes;
          Ctld::Config::Partition part;

          if (partition["name"] && !partition["name"].IsNull()) {
            name.append(partition["name"].Scalar());
          } else {
            CRANE_ERROR("Partition name not found");
            std::exit(1);
          }

          if (partition["nodes"] && !partition["nodes"].IsNull()) {
            nodes = partition["nodes"].as<std::string>();
          } else {
            CRANE_ERROR("The node of the partition {} was not found",
                        partition["name"].Scalar());
            std::exit(1);
          }

          if (partition["priority"] && !partition["priority"].IsNull()) {
            part.priority = partition["priority"].as<uint32_t>();
          } else
            part.priority = 0;

          if (!util::PartitionNodesProcess(nodes, all_node_list, name, true,
                                           &part.nodes)) {
            std::exit(1);
          }
          part.nodelist_str = util::HostNameListToStr(part.nodes);

          for (const auto& node_name : part.nodes) {
            nodes_without_part.erase(node_name);
          }

          if (partition["AllowedAccounts"] &&
              !partition["AllowedAccounts"].IsNull()) {
            auto allowed_accounts_str =
                partition["AllowedAccounts"].as<std::string>();
            std::vector<std::string> allowed_accounts =
                absl::StrSplit(allowed_accounts_str.data(), ",");
            for (const auto& account_name : allowed_accounts) {
              part.allowed_accounts.insert(
                  absl::StripAsciiWhitespace(account_name).data());
            }
          }

          if (partition["DeniedAccounts"] &&
              !partition["DeniedAccounts"].IsNull()) {
            auto denied_accounts_str =
                partition["DeniedAccounts"].as<std::string>();
            std::vector<std::string> denied_accounts =
                absl::StrSplit(denied_accounts_str, ",");
            for (const auto& account_name : denied_accounts) {
              part.denied_accounts.insert(
                  absl::StripAsciiWhitespace(account_name).data());
            }

            if (partition["AllowedAccounts"] &&
                !partition["AllowedAccounts"].IsNull())
              CRANE_WARN(
                  "Hint: When using AllowedAccounts, DeniedAccounts will not "
                  "take effect.");
          }
          constexpr uint32_t B2MB = 1024 * 1024;

          bool has_default_mem_per_cpu =
              partition["DefaultMemPerCpu"] &&
              !partition["DefaultMemPerCpu"].IsNull();
          bool has_default_mem_per_node =
              partition["DefaultMemPerNode"] &&
              !partition["DefaultMemPerNode"].IsNull();

          if (has_default_mem_per_cpu && has_default_mem_per_node) {
            CRANE_ERROR(
                "Partition {}: DefaultMemPerCpu and DefaultMemPerNode "
                "are mutually exclusive.",
                name);
            std::exit(1);
          }

          if (has_default_mem_per_cpu) {
            part.default_mem_per_cpu =
                partition["DefaultMemPerCpu"].as<uint64_t>() * B2MB;
          }
          if (has_default_mem_per_node) {
            part.default_mem_per_node =
                partition["DefaultMemPerNode"].as<uint64_t>() * B2MB;
          }

          if (part.default_mem_per_cpu == 0 && part.default_mem_per_node == 0) {
            uint64_t part_mem = 0;
            uint32_t part_cpu = 0;
            for (const auto& node : part.nodes) {
              part_cpu += g_config.Nodes[node]->cpu;
              part_mem += g_config.Nodes[node]->memory_bytes;
            }
            if (part_cpu != 0) part.default_mem_per_cpu = part_mem / part_cpu;
          }

          if (partition["MaxMemPerCpu"] &&
              !partition["MaxMemPerCpu"].IsNull()) {
            part.max_mem_per_cpu =
                partition["MaxMemPerCpu"].as<uint64_t>() * B2MB;
          } else {
            part.max_mem_per_cpu = 0;
          }

          if (partition["MaxMemPerNode"] &&
              !partition["MaxMemPerNode"].IsNull()) {
            part.max_mem_per_node =
                partition["MaxMemPerNode"].as<uint64_t>() * B2MB;
          } else {
            part.max_mem_per_node = 0;
          }

          if (part.default_mem_per_cpu != 0 && part.max_mem_per_cpu != 0 &&
              part.max_mem_per_cpu < part.default_mem_per_cpu) {
            CRANE_ERROR(
                "The partition {} MaxMemPerCpu {}MB should not be "
                "less than DefaultMemPerCpu {}MB.",
                name, part.max_mem_per_cpu / B2MB,
                part.default_mem_per_cpu / B2MB);
            std::exit(1);
          }
          if (part.default_mem_per_node != 0 && part.max_mem_per_node != 0 &&
              part.max_mem_per_node < part.default_mem_per_node) {
            CRANE_ERROR(
                "The partition {} MaxMemPerNode {}MB should not be "
                "less than DefaultMemPerNode {}MB.",
                name, part.max_mem_per_node / B2MB,
                part.default_mem_per_node / B2MB);
            std::exit(1);
          }

          CRANE_TRACE(
              "Partition {} DefaultMemPerCpu {}MB, DefaultMemPerNode "
              "{}MB, MaxMemPerCpu {}MB, MaxMemPerNode {}MB.",
              name, part.default_mem_per_cpu / B2MB,
              part.default_mem_per_node / B2MB, part.max_mem_per_cpu / B2MB,
              part.max_mem_per_node / B2MB);

          g_config.Partitions.emplace(std::move(name), std::move(part));
        }
      }

      if (!nodes_without_part.empty()) {
        CRANE_ERROR("Nodes {} not belong to any partition",
                    ranges::views::join(nodes_without_part, ",") |
                        ranges::to<std::string>);
        std::exit(1);
      }

      if (config["DefaultPartition"] && !config["DefaultPartition"].IsNull()) {
        auto default_partition = config["DefaultPartition"].as<std::string>();
        std::vector<std::string> default_partition_vec =
            absl::StrSplit(default_partition, ',');
        g_config.DefaultPartition =
            absl::StripAsciiWhitespace(default_partition_vec[0]);
        if (default_partition_vec.size() > 1) {
          CRANE_ERROR(
              "Default partition contains multiple values. '{}' is used",
              g_config.DefaultPartition);
        }

        if (!std::any_of(g_config.Partitions.begin(), g_config.Partitions.end(),
                         [&](const auto& p) {
                           return p.first == g_config.DefaultPartition;
                         })) {
          CRANE_ERROR("Unknown default partition {}",
                      g_config.DefaultPartition);
          std::exit(1);
        }
      }

      g_config.WckeyValid =
          YamlValueOr<bool>(config["TrackWCKey"], Ctld::kDefaultTrackWCKey);

      if (config["IgnoreConfigInconsistency"] &&
          !config["IgnoreConfigInconsistency"].IsNull())
        g_config.IgnoreConfigInconsistency =
            config["IgnoreConfigInconsistency"].as<bool>();

      if (config["Container"]) {
        const auto& container_config = config["Container"];

        if (container_config["Enabled"])
          g_config.Container.Enabled = container_config["Enabled"].as<bool>();
      }

      if (config["Tracing"]) {
        const auto& tracing_config = config["Tracing"];
        if (tracing_config["Enabled"])
          g_config.Tracing.Enabled = tracing_config["Enabled"].as<bool>();
        if (tracing_config["Level"])
          g_config.Tracing.Level = crane::TraceLevelFromString(
              tracing_config["Level"].as<std::string>());
      }

      if (config["Preempt"]) {
        const auto& preempt_config = config["Preempt"];

        if (preempt_config["PreemptType"]) {
          const auto& preempt_type =
              preempt_config["PreemptType"].as<std::string>();
          if (preempt_type == "none")
            g_config.Preempt.PreemptType =
                crane::grpc::PreemptType::PREEMPT_NONE;
          else if (preempt_type == "qos")
            g_config.Preempt.PreemptType =
                crane::grpc::PreemptType::PREEMPT_QOS;
          else if (preempt_type == "partition")
            g_config.Preempt.PreemptType =
                crane::grpc::PreemptType::PREEMPT_PARTITION;
          else {
            CRANE_CRITICAL(
                "Unknown PreemptType '{}'. Valid: none|qos|partition",
                preempt_type);
            std::exit(1);
          }
        }

        if (preempt_config["PreemptMode"]) {
          const auto& preempt_mode =
              preempt_config["PreemptMode"].as<std::string>();
          // TODO(preempt): accept REQUEUE / SUSPEND once the scheduler knows
          // how to honour them. The proto enum already reserves those values.
          if (preempt_mode == "OFF")
            g_config.Preempt.PreemptMode =
                crane::grpc::PreemptMode::PREEMPT_MODE_OFF;
          else if (preempt_mode == "CANCEL")
            g_config.Preempt.PreemptMode =
                crane::grpc::PreemptMode::PREEMPT_MODE_CANCEL;
          else {
            CRANE_CRITICAL("Unknown PreemptMode '{}'. Valid: OFF|CANCEL",
                           preempt_mode);
            std::exit(1);
          }
        }

        const bool type_is_none = g_config.Preempt.PreemptType ==
                                  crane::grpc::PreemptType::PREEMPT_NONE;
        const bool mode_is_off = g_config.Preempt.PreemptMode ==
                                 crane::grpc::PreemptMode::PREEMPT_MODE_OFF;
        if (type_is_none != mode_is_off) {
          CRANE_CRITICAL(
              "Preempt config inconsistent: PreemptType={}, PreemptMode={}. "
              "Both must be 'none'/'OFF' or both must be set.",
              crane::grpc::PreemptType_Name(g_config.Preempt.PreemptType),
              crane::grpc::PreemptMode_Name(g_config.Preempt.PreemptMode));
          std::exit(1);
        }
      }

    } catch (YAML::BadFile& e) {
      CRANE_CRITICAL("Can't open config file {}: {}", config_path, e.what());
      std::exit(1);
    }
  } else {
    CRANE_CRITICAL("Config file '{}' not existed", config_path);
    std::exit(1);
  }

  if (std::filesystem::exists(db_config_path)) {
    try {
      YAML::Node config = YAML::LoadFile(db_config_path);

      if (config["CraneEmbeddedDbBackend"] &&
          !config["CraneEmbeddedDbBackend"].IsNull()) {
        g_config.CraneEmbeddedDbBackend =
            config["CraneEmbeddedDbBackend"].as<std::string>();
      } else {
        g_config.CraneEmbeddedDbBackend = "Unqlite";
      }

      if (!Ctld::IsValidCraneEmbeddedDbBackend(
              g_config.CraneEmbeddedDbBackend)) {
        CRANE_CRITICAL("Invalid CraneEmbeddedDbBackend '{}'. Valid values: {}",
                       g_config.CraneEmbeddedDbBackend,
                       fmt::join(Ctld::kCraneEmbeddedDbBackendValues, ", "));
        std::exit(1);
      }

      std::filesystem::path db_base_dir = g_config.CraneBaseDir;
      if (!g_config.KeepalivedConfig.CraneSharedBaseDir.empty())
        db_base_dir = g_config.KeepalivedConfig.CraneSharedBaseDir;

      if (config["CraneCtldDbPath"] && !config["CraneCtldDbPath"].IsNull()) {
        std::filesystem::path path(config["CraneCtldDbPath"].as<std::string>());
        if (path.is_absolute())
          g_config.CraneCtldDbPath = path;
        else
          g_config.CraneCtldDbPath = db_base_dir / path;
      } else
        g_config.CraneCtldDbPath = db_base_dir / kDefaultCraneCtldDbPath;

      if (config["RocksDb"]) {
        const auto& rocksdb_config = config["RocksDb"];
        g_config.RocksDb.SyncWrites =
            YamlValueOr<bool>(rocksdb_config["SyncWrites"], true);
        g_config.RocksDb.ManualWalSyncIntervalMs = YamlValueOr<uint32_t>(
            rocksdb_config["ManualWalSyncIntervalMs"], 1000);
        g_config.RocksDb.WriteBufferSizeMB =
            YamlValueOr<uint32_t>(rocksdb_config["WriteBufferSizeMB"], 64);
        g_config.RocksDb.MaxWriteBufferNumber =
            YamlValueOr<uint32_t>(rocksdb_config["MaxWriteBufferNumber"], 4);
        g_config.RocksDb.TargetFileSizeBaseMB =
            YamlValueOr<uint32_t>(rocksdb_config["TargetFileSizeBaseMB"], 64);
        g_config.RocksDb.MaxBackgroundJobs =
            YamlValueOr<uint32_t>(rocksdb_config["MaxBackgroundJobs"], 4);
        g_config.RocksDb.Compression =
            YamlValueOr(rocksdb_config["Compression"], "lz4");
      }

      if (config["DbUser"] && !config["DbUser"].IsNull()) {
        g_config.DbUser = config["DbUser"].as<std::string>();
        if (config["DbPassword"] && !config["DbPassword"].IsNull())
          g_config.DbPassword = config["DbPassword"].as<std::string>();
      }

      if (config["DbHost"] && !config["DbHost"].IsNull())
        g_config.DbHost = config["DbHost"].as<std::string>();
      else
        g_config.DbHost = "localhost";

      if (config["DbPort"] && !config["DbPort"].IsNull())
        g_config.DbPort = config["DbPort"].as<std::string>();
      else
        g_config.DbPort = "27017";  // default port 27017

      if (config["DbReplSetName"] && !config["DbReplSetName"].IsNull())
        g_config.DbRSName = config["DbReplSetName"].as<std::string>();
      else {
        CRANE_ERROR("Unknown Replica Set name");
        std::exit(1);
      }

      if (config["DbName"] && !config["DbName"].IsNull())
        g_config.DbName = config["DbName"].as<std::string>();
      else
        g_config.DbName = "crane_db";

      if (config["JobAggregationTimeoutMs"]) {
        g_config.JobAggregationTimeoutMs =
            config["JobAggregationTimeoutMs"].as<uint32_t>();
      }

      if (config["JobAggregationBatchSize"]) {
        g_config.JobAggregationBatchSize =
            config["JobAggregationBatchSize"].as<uint32_t>();
      }

      if (config["JobAggregationMode"]) {
        g_config.JobAggregationMode =
            config["JobAggregationMode"].as<std::string>();
        if (g_config.JobAggregationMode != "async" &&
            g_config.JobAggregationMode != "sync") {
          CRANE_ERROR("Unknown JobAggregationMode '{}'. Valid: async|sync",
                      g_config.JobAggregationMode);
          std::exit(1);
        }
      }

      g_config.JobAggregationWorkerBatchSize =
          YamlValueOr<uint32_t>(config["JobAggregationWorkerBatchSize"],
                                Ctld::kJobAggregationWorkerBatchSize);
      g_config.JobAggregationPollIntervalMs =
          YamlValueOr<uint32_t>(config["JobAggregationPollIntervalMs"],
                                Ctld::kJobAggregationPollIntervalMs);
      g_config.JobAggregationRetryBackoffMs =
          YamlValueOr<uint32_t>(config["JobAggregationRetryBackoffMs"],
                                Ctld::kJobAggregationRetryBackoffMs);
      g_config.JobAggregationMaxRetryBackoffMs =
          YamlValueOr<uint32_t>(config["JobAggregationMaxRetryBackoffMs"],
                                Ctld::kJobAggregationMaxRetryBackoffMs);

      if (config["Vault"]) {
        const auto& vault_config = config["Vault"];

        if (vault_config["Enabled"])
          g_config.VaultConf.Enabled = vault_config["Enabled"].as<bool>();

        g_config.VaultConf.Addr =
            YamlValueOr(vault_config["Addr"], "127.0.0.1");

        g_config.VaultConf.Port = YamlValueOr(vault_config["Port"], "8200");

        g_config.VaultConf.ExpirationMinutes = YamlValueOr<uint64_t>(
            vault_config["ExpirationMinutes"], kDefaultCertExpirationMinutes);

        if (vault_config["Username"] && !vault_config["Username"].IsNull())
          g_config.VaultConf.Username =
              vault_config["Username"].as<std::string>();
        else {
          CRANE_ERROR("Unknown Vault Username");
          std::exit(1);
        }

        if (vault_config["Password"] && !vault_config["Password"].IsNull())
          g_config.VaultConf.Password =
              vault_config["Password"].as<std::string>();
        else {
          CRANE_ERROR("Unknown Vault Password");
          std::exit(1);
        }

        if (vault_config["Tls"] && !vault_config["Tls"].IsNull())
          g_config.VaultConf.Tls = vault_config["Tls"].as<bool>();
        else
          g_config.VaultConf.Tls = false;
      }  // vault

    } catch (YAML::BadFile& e) {
      CRANE_CRITICAL("Can't open database config file {}: {}", db_config_path,
                     e.what());
      std::exit(1);
    }
  } else {
    CRANE_CRITICAL("Database config file '{}' not existed", db_config_path);
    std::exit(1);
  }

  // Load plugin configuration from separate plugin.yaml file
  if (std::filesystem::exists(plugin_config_path)) {
    try {
      YAML::Node plugin_config = YAML::LoadFile(plugin_config_path);

      if (plugin_config["Enabled"])
        g_config.Plugin.Enabled = plugin_config["Enabled"].as<bool>();

      g_config.Plugin.PlugindSockPath =
          fmt::format("unix://{}{}", g_config.CraneBaseDir,
                      YamlValueOr(plugin_config["PlugindSockPath"],
                                  kDefaultPlugindUnixSockPath));
      g_config.Plugin.TraceHookMaxRequestBytes =
          YamlValueOr<size_t>(plugin_config["TraceHookMaxRequestBytes"],
                              kDefaultTraceHookMaxRequestBytes);

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
    g_config.ListenConf.CraneCtldListenAddr =
        parsed_args["listen"].as<std::string>();
  }
  if (parsed_args.count("port")) {
    g_config.ListenConf.CraneCtldListenPort =
        parsed_args["port"].as<std::string>();
  }

  const auto& dynamic_nodes = g_config.CtldConf.DynamicNodes;
  if (dynamic_nodes.Enabled && !g_config.ListenConf.TlsConfig.Enabled) {
    CRANE_ERROR("DynamicNodes requires TLS to be enabled.");
    std::exit(1);
  }
  if (dynamic_nodes.AutoCreate) {
    if (!dynamic_nodes.Enabled) {
      CRANE_ERROR("Dynamic node AutoCreate requires DynamicNodes.Enabled.");
      std::exit(1);
    }
    if (dynamic_nodes.AutoCreatePools.empty()) {
      CRANE_ERROR("Dynamic node AutoCreate requires at least one pool.");
      std::exit(1);
    }
  }
  for (const auto& pool : dynamic_nodes.AutoCreatePools) {
    for (const auto& partition : pool.Partitions) {
      if (!g_config.Partitions.contains(partition)) {
        CRANE_ERROR("AutoCreate pool {} references unknown partition {}.",
                    pool.Name, partition);
        std::exit(1);
      }
    }
  }

  if (crane::GetIpAddrVer(g_config.ListenConf.CraneCtldListenAddr) == -1) {
    CRANE_ERROR("Listening address is invalid.");
    std::exit(1);
  }

  std::regex regex_port(R"(^([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|)"
                        R"(65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$)");
  if (!std::regex_match(g_config.ListenConf.CraneCtldListenPort, regex_port)) {
    CRANE_ERROR("Listening port is invalid.");
    std::exit(1);
  }
}

void DestroyCtldGlobalVariables() {
  using namespace Ctld;

#ifdef CRANE_ENABLE_TRACING
  crane::TracerManager::GetInstance().Shutdown();
#endif

  // Stop callback producers, then drain their tasks while every callback
  // dependency is still alive. CranedKeeper callbacks can query both the
  // scheduler and NodeManager.
  if (g_craned_keeper != nullptr) g_craned_keeper->Shutdown();
  if (g_thread_pool != nullptr) g_thread_pool->wait();
  g_job_scheduler.reset();
  if (g_thread_pool != nullptr) g_thread_pool->wait();
  g_craned_keeper.reset();
  if (g_plugin_client != nullptr) g_plugin_client->SetReconnectCallback({});
  g_node_manager.reset();

  // In case that spdlog is destructed before g_embedded_db_client->Close()
  // in which log function is called.
  g_embedded_db_client.reset();

  g_meta_container.reset();
  g_thread_pool.reset();
  g_plugin_client.reset();

  if (!g_config.KeepalivedConfig.CraneCtldAliveFile.empty()) {
    if (!util::os::DeleteFile(g_config.KeepalivedConfig.CraneCtldAliveFile))
      CRANE_ERROR("Failed to delete folders for CraneCtld alive file!");
  }
}

void InitializeCtldGlobalVariables() {
  using namespace Ctld;

  PasswordEntry::InitializeEntrySize();

  crane::InitializeNetworkFunctions();

  char hostname[HOST_NAME_MAX + 1];
  int err = gethostname(hostname, HOST_NAME_MAX + 1);
  if (err != 0) {
    CRANE_ERROR("Error: get hostname.");
    std::exit(1);
  }

  g_config.Hostname.assign(hostname);
  CRANE_INFO("Hostname of CraneCtld: {}", g_config.Hostname);

  {
    uint32_t pool_size = g_config.CtldConf.ThreadPoolSize > 0
                             ? g_config.CtldConf.ThreadPoolSize
                             : std::thread::hardware_concurrency();
    CRANE_INFO("CraneCtld thread pool size: {}", pool_size);
    g_thread_pool = std::make_unique<BS::thread_pool>(
        pool_size, [] { util::SetCurrentThreadName("BsThreadPool"); });
  }

  g_db_client = std::make_unique<MongodbClient>();
  if (!g_db_client->Connect()) {
    CRANE_ERROR("Error: MongoDb client connect fail");
    std::exit(1);
  }

  if (g_config.Plugin.Enabled) {
    CRANE_INFO("[Plugin] Plugin module is enabled.");
    g_plugin_client = std::make_unique<plugin::PluginClient>();
    g_plugin_client->InitChannelAndStub(
        g_config.Plugin.PlugindSockPath,
        g_config.Plugin.TraceHookMaxRequestBytes);
  }

#ifdef CRANE_ENABLE_TRACING
  if (g_config.Plugin.Enabled && g_plugin_client) {
    auto exporter =
        std::make_unique<crane::CraneSpanExporter>(*g_plugin_client);
    crane::TracerManager::GetInstance().Initialize("CraneCtld",
                                                   std::move(exporter));
  } else {
    crane::TracerManager::GetInstance().Initialize("CraneCtld");
  }
  auto trace_config = crane::ApplyRuntimeTraceConfig(g_config.Tracing.Enabled,
                                                     g_config.Tracing.Level);
  if (trace_config.clamped) {
    CRANE_WARN(
        "Tracing runtime level {} exceeds compiled max level {}; effective "
        "level is {}.",
        crane::TraceLevelToString(trace_config.runtime_level),
        crane::TraceLevelToString(trace_config.compiled_max_level),
        crane::TraceLevelToString(trace_config.effective_level));
  }
#endif

  if (g_config.VaultConf.Enabled) {
    g_vault_client = std::make_unique<Security::VaultClient>();
    if (!g_vault_client->InitFromConfig(g_config.VaultConf)) std::exit(1);
  } else if (g_config.ListenConf.TlsConfig.Enabled) {
    CRANE_ERROR("[Security] TLS is enabled but Vault is not enabled.");
    std::exit(1);
  }

  // Account manager must be initialized before Job Scheduler
  // since the recovery stage of the job scheduler will acquire
  // information from account manager.
  g_account_manager = std::make_unique<AccountManager>();

  g_license_manager = std::make_unique<LicenseManager>();
  g_license_manager->Init(g_config.lic_id_to_count_map);

  g_account_meta_container = std::make_unique<AccountMetaContainer>();

  if (!g_config.JobSubmitLuaScript.empty()) {
#ifdef HAVE_LUA
    g_lua_pool = std::make_unique<crane::LuaPool>();
    if (!g_lua_pool->Init()) std::exit(1);
#else
    CRANE_WARN(
        "JobSubmitLuaScript is configured but CraneCtld was built without Lua "
        "support. The Lua script will NOT be executed.");
#endif
  }

  bool ok;
  g_embedded_db_client =
      Ctld::MakeEmbeddedDbClient(g_config.CraneEmbeddedDbBackend);
  ok = g_embedded_db_client &&
       g_embedded_db_client->Init(g_config.CraneCtldDbPath);
  if (!ok) {
    CRANE_ERROR("Failed to initialize g_embedded_db_client.");

    DestroyCtldGlobalVariables();
    std::exit(1);
  }

  g_node_manager = std::make_unique<NodeManager>();
  if (!g_node_manager->Init()) {
    CRANE_ERROR("Failed to initialize dynamic node manager.");
    DestroyCtldGlobalVariables();
    std::exit(1);
  }

  g_meta_container = std::make_unique<CranedMetaContainer>();
  g_meta_container->InitFromConfig(g_config);
  g_node_manager->RestoreDynamicNodes();
  g_node_manager->StartReconcileThread();

  g_craned_keeper =
      std::make_unique<CranedKeeper>(g_config.CtldConf.MaxNodeCount);

  g_craned_keeper->SetCranedConnectedCb(
      [](const CranedId& craned_id, const google::protobuf::Timestamp& token) {
        CRANE_DEBUG("CranedNode #{} Connected.", craned_id);
        auto stub = g_craned_keeper->GetCranedStub(craned_id);
        if (stub == nullptr) {
          CRANE_ERROR("CranedNode #{} has no stub.", craned_id);
          return;
        }
        auto registration_lock = stub->AcquireRegistrationLock();
        if (!stub->Connected()) return;
        if (!stub->TryBeginRegistration(token)) {
          CRANE_TRACE(
              "Ignore stale registration callback for craned {} token {}.",
              craned_id, ProtoTimestampToString(token));
          return;
        }
        if (g_meta_container->CheckCranedOnline(craned_id)) {
          CRANE_TRACE(
              "Already online craned {} notified Ctld, consider it down.",
              craned_id);
          g_meta_container->CranedDown(craned_id);
        }
        stub->ConfigureCraned(craned_id, token);
      });

  g_craned_keeper->SetCranedDisconnectedCb([](const CranedId& craned_id) {
    CRANE_DEBUG("CranedNode #{} Disconnected.", craned_id);
    // No need to worry disconnect before job scheduler init
    if (g_node_manager) {
      auto result = g_node_manager->MarkDisconnectedIfUntracked(craned_id);
      if (!result)
        CRANE_WARN("Failed to persist dynamic node {} down state: {}",
                   craned_id, result.error());
    }
  });

  ok = g_db_client->Init();
  if (!ok) {
    CRANE_ERROR("The initialization of MongoDb client failed. Exiting...");
    DestroyCtldGlobalVariables();
    std::exit(1);
  }

  using namespace std::chrono_literals;

  g_job_scheduler = std::make_unique<JobScheduler>();

  ok = g_job_scheduler->Init();
  if (!ok) {
    CRANE_ERROR("The initialization of JobScheduler failed. Exiting...");
    DestroyCtldGlobalVariables();
    std::exit(1);
  }
  g_ctld_server = std::make_unique<Ctld::CtldServer>(g_config.ListenConf);

  g_runtime_status.srv_ready.store(true, std::memory_order_release);
  g_node_manager->ReconcilePluginState();
  if (g_plugin_client != nullptr) {
    g_plugin_client->SetReconnectCallback(
        [] { g_node_manager->ReconcilePluginState(); });
  }
  util::SetCurrentThreadName("CraneCtldMain");
}

void CreateFolders() {
  bool ok;
  ok = util::os::CreateFoldersForFile(g_config.CraneCtldLogFile);
  if (!ok) {
    CRANE_ERROR("Failed to create folders for CraneCtld log files!");
    std::exit(1);
  }

  ok = util::os::CreateFoldersForFile(g_config.CraneCtldDbPath);
  if (!ok) {
    CRANE_ERROR("Failed to create folders for CraneCtld db files!");
    std::exit(1);
  }

  if (!g_config.KeepalivedConfig.CraneCtldAliveFile.empty()) {
    ok = util::os::CreateFoldersForFile(
        g_config.KeepalivedConfig.CraneCtldAliveFile);
    if (!ok) {
      CRANE_ERROR("Failed to create folders for CraneCtld alive file!");
      std::exit(1);
    }
  }
}

int StartServer() {
  constexpr uint64_t file_max = 640000;
  if (!util::os::SetMaxFileDescriptorNumber(file_max)) {
    CRANE_WARN(
        "Unable to set file descriptor limits to {}. Please increase the hard "
        "limit if needed.",
        file_max);
  }
  util::os::CheckProxyEnvironmentVariable();

  CreateFolders();

  InitializeCtldGlobalVariables();
  CRANE_INFO("CraneCtld service ready.");

  if (!g_config.KeepalivedConfig.CraneCtldAliveFile.empty()) {
    if (!util::os::CreateFile(g_config.KeepalivedConfig.CraneCtldAliveFile)) {
      DestroyCtldGlobalVariables();
      std::exit(1);
    }
  }

  g_ctld_server->Wait();

  DestroyCtldGlobalVariables();

  return 0;
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
  bool ok = util::os::CreateFoldersForFile(g_config.CraneCtldMutexFilePath);
  if (!ok) std::exit(1);

  int pid_file =
      open(g_config.CraneCtldMutexFilePath.c_str(), O_CREAT | O_RDWR, 0666);
  int rc = flock(pid_file, LOCK_EX | LOCK_NB);
  if (rc) {
    if (EWOULDBLOCK == errno) {
      CRANE_CRITICAL("There is another CraneCtld instance running. Exiting...");
      std::exit(1);
    } else {
      CRANE_CRITICAL("Failed to lock {}: {}. Exiting...",
                     g_config.CraneCtldMutexFilePath, strerror(errno));
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
  ParseConfig(argc, argv);
  CheckSingleton();
  InstallStackTraceHooks();

  if (g_config.CraneCtldForeground)
    StartServer();
  else
    StartDaemon();

  return 0;
}

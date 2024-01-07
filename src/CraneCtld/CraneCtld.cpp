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

#include "CtldPreCompiledHeader.h"
// Precompiled header comes first!

#include <event2/thread.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <yaml-cpp/yaml.h>

#include <cxxopts.hpp>

#include "AccountManager.h"
#include "CranedKeeper.h"
#include "CranedMetaContainer.h"
#include "CtldGrpcServer.h"
#include "DbClient.h"
#include "EmbeddedDbClient.h"
#include "TaskScheduler.h"
#include "crane/FdFunctions.h"
#include "crane/Network.h"

void ParseConfig(int argc, char** argv) {
  cxxopts::Options options("cranectld");

  // clang-format off
  options.add_options()
      ("C,config", "Path to configuration file",
      cxxopts::value<std::string>()->default_value(kDefaultConfigPath))
      ("l,listen", "listening address",
      cxxopts::value<std::string>()->default_value("0.0.0.0"))
      ("p,port", "listening port",
      cxxopts::value<std::string>()->default_value(kCtldDefaultPort))
      ;
  // clang-format on

  auto parsed_args = options.parse(argc, argv);

  std::string config_path = parsed_args["config"].as<std::string>();
  std::string db_config_path = Ctld::kDefaultDbConfigPath;
  if (std::filesystem::exists(config_path)) {
    try {
      YAML::Node config = YAML::LoadFile(config_path);

      if (config["CraneCtldDebugLevel"])
        g_config.CraneCtldDebugLevel =
            config["CraneCtldDebugLevel"].as<std::string>();
      else
        g_config.CraneCtldDebugLevel = "info";

      if (config["CraneCtldLogFile"])
        g_config.CraneCtldLogFile =
            config["CraneCtldLogFile"].as<std::string>();
      else
        g_config.CraneCtldLogFile = Ctld::kCraneCtldDefaultLogPath;

      // spdlog should be initialized as soon as possible
      spdlog::level::level_enum log_level;
      if (g_config.CraneCtldDebugLevel == "trace") {
        log_level = spdlog::level::trace;
      } else if (g_config.CraneCtldDebugLevel == "debug") {
        log_level = spdlog::level::debug;
      } else if (g_config.CraneCtldDebugLevel == "info") {
        log_level = spdlog::level::info;
      } else if (g_config.CraneCtldDebugLevel == "warn") {
        log_level = spdlog::level::warn;
      } else if (g_config.CraneCtldDebugLevel == "error") {
        log_level = spdlog::level::err;
      } else {
        fmt::print(stderr, "Illegal debug-level format.");
        std::exit(1);
      }

      InitLogger(log_level, g_config.CraneCtldLogFile);

      if (config["CraneCtldListenAddr"])
        g_config.ListenConf.CraneCtldListenAddr =
            config["CraneCtldListenAddr"].as<std::string>();
      else
        g_config.ListenConf.CraneCtldListenAddr = "0.0.0.0";

      if (config["CraneCtldListenPort"])
        g_config.ListenConf.CraneCtldListenPort =
            config["CraneCtldListenPort"].as<std::string>();
      else
        g_config.ListenConf.CraneCtldListenPort = kCtldDefaultPort;

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

      if (config["DbConfigPath"]) {
        db_config_path = config["DbConfigPath"].as<std::string>();
      }

      if (config["CraneCtldForeground"]) {
        g_config.CraneCtldForeground = config["CraneCtldForeground"].as<bool>();
      }

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

      if (config["PriorityWeightQ0S"])
        g_config.PriorityConfig.WeightQOS =
            config["PriorityWeightQ0S"].as<uint32_t>();
      else
        g_config.PriorityConfig.WeightQOS = 0;

      if (config["Nodes"]) {
        for (auto it = config["Nodes"].begin(); it != config["Nodes"].end();
             ++it) {
          auto node = it->as<YAML::Node>();
          auto node_ptr = std::make_shared<Ctld::Config::Node>();
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
          for (auto&& name : name_list) g_config.Nodes[name] = node_ptr;
        }
      }

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

          part.nodelist_str = nodes;
          std::list<std::string> name_list;
          if (!util::ParseHostList(absl::StripAsciiWhitespace(nodes).data(),
                                   &name_list)) {
            CRANE_ERROR("Illegal node name string format.");
            std::exit(1);
          }

          for (auto&& node : name_list) {
            auto node_it = g_config.Nodes.find(node);
            if (node_it != g_config.Nodes.end()) {
              part.nodes.emplace(node_it->first);
              CRANE_TRACE("Set the partition of node {} to {}", node_it->first,
                          name);
            } else {
              CRANE_ERROR(
                  "Unknown node '{}' found in partition '{}'. It is ignored "
                  "and should be contained in the configuration file.",
                  node, name);
            }
          }

          g_config.Partitions.emplace(std::move(name), std::move(part));
        }
      }

      if (config["DefaultPartition"] && !config["DefaultPartition"].IsNull()) {
        auto default_partition = config["DefaultPartition"].as<std::string>();
        std::vector<std::string> default_partition_vec;
        boost::split(default_partition_vec, default_partition,
                     boost::is_any_of(","));
        g_config.DefaultPartition = default_partition_vec[0];
        boost::trim(g_config.DefaultPartition);
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
          !config["CraneEmbeddedDbBackend"].IsNull())
        g_config.CraneEmbeddedDbBackend =
            config["CraneEmbeddedDbBackend"].as<std::string>();
      else
        g_config.CraneEmbeddedDbBackend = "Unqlite";

      if (config["CraneCtldDbPath"] && !config["CraneCtldDbPath"].IsNull())
        g_config.CraneCtldDbPath = config["CraneCtldDbPath"].as<std::string>();
      else
        g_config.CraneCtldDbPath = Ctld::kDefaultDbPath;

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
    } catch (YAML::BadFile& e) {
      CRANE_CRITICAL("Can't open database config file {}: {}", db_config_path,
                     e.what());
      std::exit(1);
    }
  } else {
    CRANE_CRITICAL("Database config file '{}' not existed", db_config_path);
    std::exit(1);
  }

  if (parsed_args.count("listen")) {
    g_config.ListenConf.CraneCtldListenAddr =
        parsed_args["listen"].as<std::string>();
  }
  if (parsed_args.count("port")) {
    g_config.ListenConf.CraneCtldListenPort =
        parsed_args["port"].as<std::string>();
  }

  std::regex regex_addr(
      R"(^((25[0-5]|(2[0-4]|1[0-9]|[1-9]|)[0-9])(\.(?!$)|$)){4}$)");

  std::regex regex_port(R"(^([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|)"
                        R"(65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$)");

  if (!std::regex_match(g_config.ListenConf.CraneCtldListenAddr, regex_addr)) {
    fmt::print("Listening address is invalid.\n");
    std::exit(1);
  }

  if (!std::regex_match(g_config.ListenConf.CraneCtldListenPort, regex_port)) {
    fmt::print("Listening port is invalid.\n");
    std::exit(1);
  }
}

void DestroyCtldGlobalVariables() {
  using namespace Ctld;

  g_craned_keeper.reset();

  // In case that spdlog is destructed before g_embedded_db_client->Close()
  // in which log function is called.
  g_embedded_db_client.reset();
}

void InitializeCtldGlobalVariables() {
  using namespace Ctld;

  crane::InitializeNetworkFunctions();

  // Enable inter-thread custom event notification.
  evthread_use_pthreads();

  char hostname[HOST_NAME_MAX + 1];
  int err = gethostname(hostname, HOST_NAME_MAX + 1);
  if (err != 0) {
    CRANE_ERROR("Error: get hostname.");
    std::exit(1);
  }

  g_config.Hostname.assign(hostname);
  CRANE_INFO("Hostname of CraneCtld: {}", g_config.Hostname);

  g_thread_pool =
      std::make_unique<BS::thread_pool>(std::thread::hardware_concurrency());

  g_db_client = std::make_unique<MongodbClient>();
  if (!g_db_client->Connect()) {
    CRANE_ERROR("Error: MongoDb client connect fail");
    std::exit(1);
  }

  // Account manager must be initialized before Task Scheduler
  // since the recovery stage of the task scheduler will acquire
  // information from account manager.
  g_account_manager = std::make_unique<AccountManager>();

  g_meta_container = std::make_unique<CranedMetaContainerSimpleImpl>();
  g_meta_container->InitFromConfig(g_config);

  bool ok;
  g_embedded_db_client = std::make_unique<Ctld::EmbeddedDbClient>();
  ok = g_embedded_db_client->Init(g_config.CraneCtldDbPath);
  if (!ok) {
    CRANE_ERROR("Failed to initialize g_embedded_db_client.");

    DestroyCtldGlobalVariables();
    std::exit(1);
  }

  g_craned_keeper = std::make_unique<CranedKeeper>(g_config.Nodes.size());

  g_craned_keeper->SetCranedIsUpCb([](const CranedId& craned_id) {
    CRANE_TRACE(
        "A new node #{} is up now. Add its resource to the global resource "
        "pool.",
        craned_id);
    g_meta_container->CranedUp(craned_id);
  });

  g_craned_keeper->SetCranedIsDownCb([](const CranedId& craned_id) {
    CRANE_TRACE("CranedNode #{} is down now.", craned_id);
  });

  g_craned_keeper->SetCranedIsTempUpCb([](const CranedId& craned_id) {
    CRANE_TRACE(
        "CranedNode #{} is temporarily up now. "
        "Add its resource to the global resource pool.",
        craned_id);
    g_meta_container->CranedUp(craned_id);
  });

  g_craned_keeper->SetCranedIsTempDownCb([](const CranedId& craned_id) {
    CRANE_TRACE(
        "CranedNode #{} lost connection and is temporarily down now. "
        "Remove its resource from the global resource pool.",
        craned_id);
    g_meta_container->CranedDown(craned_id);
    g_task_scheduler->TerminateTasksOnCraned(craned_id,
                                             ExitCode::kExitCodeCranedDown);
  });

  std::list<CranedId> to_register_craned_list;
  for (auto&& kv : g_config.Nodes) {
    to_register_craned_list.emplace_back(kv.first);
  }

  using namespace std::chrono_literals;

  // TaskScheduler will always recovery pending or running tasks since last
  // failure, it might be reasonable to wait some time (1s) for all healthy
  // craned nodes (most of the time all the craned nodes are healthy) to be
  // online or to wait for the connections to some offline craned nodes to
  // time out. Otherwise, recovered pending or running tasks may always fail
  // to be re-queued.
  size_t to_registered_craneds_cnt = g_config.Nodes.size();
  // sigmoid function, where x=0 -> y=1.89, x=inf -> y=301
  // The time space is approximately [2s, 5min].
  int timeout =
      (int)(314.0 /
            (1.0 + std::exp(-0.0001 *
                            ((double)to_registered_craneds_cnt - 30000.0)))) -
      13;
  std::chrono::time_point<std::chrono::system_clock> wait_end_point =
      std::chrono::system_clock::now() + std::chrono::seconds(timeout);

  g_craned_keeper->InitAndRegisterCraneds(to_register_craned_list);
  while (true) {
    auto online_cnt = g_craned_keeper->AvailableCranedCount();
    if (online_cnt >= to_registered_craneds_cnt) {
      CRANE_INFO("All craned nodes are up.");
      break;
    }

    std::this_thread::sleep_for(
        std::chrono::microseconds(timeout * 1000 /*ms*/ / 100));
    if (std::chrono::system_clock::now() > wait_end_point) {
      CRANE_INFO(
          "Waiting all craned node to be online timed out. Continuing. "
          "{} craned is online. Total: {}.",
          online_cnt, to_registered_craneds_cnt);
      break;
    }
  }

  g_task_scheduler = std::make_unique<TaskScheduler>();
  ok = g_task_scheduler->Init();
  if (!ok) {
    CRANE_ERROR("The initialization of TaskScheduler failed. Exiting...");
    DestroyCtldGlobalVariables();
    std::exit(1);
  }

  g_ctld_server = std::make_unique<Ctld::CtldServer>(g_config.ListenConf);
}

void CreateFolders() {
  auto create_folders_for_path = [](std::string const& p) {
    try {
      std::filesystem::path log_path{p};
      auto log_dir = log_path.parent_path();
      if (!std::filesystem::exists(log_dir))
        std::filesystem::create_directories(log_dir);
    } catch (const std::exception& e) {
      CRANE_ERROR("Failed to create folder for {}: {}", p, e.what());
      std::exit(1);
    }
  };

  create_folders_for_path(g_config.CraneCtldLogFile);
  create_folders_for_path(g_config.CraneCtldDbPath);
}

int StartServer() {
  constexpr uint64_t file_max = 640000;
  if (!util::SetMaxFileDescriptorNumber(file_max)) {
    CRANE_ERROR("Unable to set file descriptor limits to {}", file_max);
    std::exit(1);
  }

  CreateFolders();

  InitializeCtldGlobalVariables();

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
  std::filesystem::path lock_path{kDefaultCraneCtldMutexFile};
  try {
    auto lock_dir = lock_path.parent_path();
    if (!std::filesystem::exists(lock_dir))
      std::filesystem::create_directories(lock_dir);
  } catch (const std::exception& e) {
    CRANE_ERROR("Invalid CraneCtldMutexFile path {}: {}",
                kDefaultCraneCtldMutexFile, e.what());
  }

  int pid_file = open(lock_path.c_str(), O_CREAT | O_RDWR, 0666);
  int rc = flock(pid_file, LOCK_EX | LOCK_NB);
  if (rc) {
    if (EWOULDBLOCK == errno) {
      CRANE_CRITICAL("There is another CraneCtld instance running. Exiting...");
      std::exit(1);
    } else {
      CRANE_CRITICAL("Failed to lock {}: {}. Exiting...", lock_path.string(),
                     strerror(errno));
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
  CheckSingleton();
  ParseConfig(argc, argv);
  InstallStackTraceHooks();

  if (g_config.CraneCtldForeground)
    StartServer();
  else
    StartDaemon();

  return 0;
}

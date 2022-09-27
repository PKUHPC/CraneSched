#include <absl/strings/ascii.h>
#include <absl/strings/str_split.h>
#include <event2/thread.h>
#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <sys/stat.h>
#include <yaml-cpp/yaml.h>

#include <boost/filesystem/string_file.hpp>
#include <condition_variable>
#include <cxxopts.hpp>
#include <filesystem>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "CtldGrpcServer.h"
#include "DbClient.h"
#include "TaskScheduler.h"
#include "CranedKeeper.h"
#include "CranedMetaContainer.h"
#include "crane/PublicHeader.h"
#include "crane/String.h"

void InitializeCtldGlobalVariables() {
  using namespace Ctld;

  // Enable inter-thread custom event notification.
  evthread_use_pthreads();

  auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
      g_config.CraneCtldLogFile, 1048576 * 5, 3);

  auto console_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();

  if (g_config.CraneCtldDebugLevel == "trace") {
    file_sink->set_level(spdlog::level::trace);
    console_sink->set_level(spdlog::level::trace);
  } else if (g_config.CraneCtldDebugLevel == "debug") {
    file_sink->set_level(spdlog::level::debug);
    console_sink->set_level(spdlog::level::debug);
  } else if (g_config.CraneCtldDebugLevel == "info") {
    file_sink->set_level(spdlog::level::info);
    console_sink->set_level(spdlog::level::info);
  } else if (g_config.CraneCtldDebugLevel == "warn") {
    file_sink->set_level(spdlog::level::warn);
    console_sink->set_level(spdlog::level::warn);
  } else if (g_config.CraneCtldDebugLevel == "error") {
    file_sink->set_level(spdlog::level::err);
    console_sink->set_level(spdlog::level::err);
  } else {
    CRANE_ERROR("Illegal debug-level format.");
    std::exit(1);
  }

  spdlog::init_thread_pool(256, 1);
  auto logger = std::make_shared<spdlog::async_logger>(
      "default", spdlog::sinks_init_list{file_sink, console_sink},
      spdlog::thread_pool(), spdlog::async_overflow_policy::block);

  spdlog::set_default_logger(logger);

  spdlog::flush_on(spdlog::level::err);
  spdlog::flush_every(std::chrono::seconds(1));

  spdlog::set_level(spdlog::level::trace);

  char hostname[HOST_NAME_MAX + 1];
  int err = gethostname(hostname, HOST_NAME_MAX + 1);
  if (err != 0) {
    CRANE_ERROR("Error: get hostname.");
    std::exit(1);
  }

  g_config.Hostname.assign(hostname);
  CRANE_INFO("Hostname of CraneCtld: {}", g_config.Hostname);

  g_db_client = std::make_unique<MongodbClient>();
  if (!g_db_client) {
    CRANE_ERROR("Error: MongoDb client Init failed");
    std::exit(1);
  }
  if (!g_db_client->Connect()) {
    CRANE_ERROR("Error: MongoDb client connect fail");
    std::exit(1);
  }
  g_db_client->Init();

  g_meta_container = std::make_unique<CranedMetaContainerSimpleImpl>();
  g_meta_container->InitFromConfig(g_config);

  g_craned_keeper = std::make_unique<CranedKeeper>();
  std::list<CranedAddrAndId> addr_and_id_list;
  for (auto& kv : g_config.Nodes) {
    CranedAddrAndId addr_and_id;
    addr_and_id.node_addr = kv.first;
    if (g_meta_container->GetCraneId(kv.first, &addr_and_id.node_id))
      addr_and_id_list.emplace_back(std::move(addr_and_id));
    else {
      CRANE_TRACE(
          "Node {} doesn't belong to any partition. It will not be "
          "registered.",
          kv.first);
    }
  }
  g_craned_keeper->RegisterCraneds(std::move(addr_and_id_list));

  g_task_scheduler =
      std::make_unique<TaskScheduler>(std::make_unique<MinLoadFirst>());
}

void DestroyCtldGlobalVariables() {
  using namespace Ctld;
  g_craned_keeper.reset();
}

int StartServer() {
  // Create log directory recursively.
  try {
    std::filesystem::path log_path{g_config.CraneCtldLogFile};
    auto log_dir = log_path.parent_path();
    if (!log_dir.empty()) std::filesystem::create_directories(log_dir);
  } catch (const std::exception& e) {
    CRANE_ERROR("Invalid CraneCtldLogFile path {}: {}",
                g_config.CraneCtldLogFile, e.what());
  }

  InitializeCtldGlobalVariables();

  g_ctld_server = std::make_unique<Ctld::CtldServer>(g_config.ListenConf);

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

int main(int argc, char** argv) {
// Todo: Add single program instance checking. The current program will freeze
//  when multiple instances are running.
#ifndef NDEBUG
  spdlog::set_level(spdlog::level::trace);
#endif

  if (std::filesystem::exists(kDefaultConfigPath)) {
    try {
      YAML::Node config = YAML::LoadFile(kDefaultConfigPath);

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

      if (config["UseTls"] && config["UseTls"].as<bool>()) {
        g_config.ListenConf.UseTls = true;
        if (config["CertFilePath"]) {
          g_config.ListenConf.CertFilePath =
              config["CertFilePath"].as<std::string>();

          try {
            boost::filesystem::load_string_file(
                g_config.ListenConf.CertFilePath,
                g_config.ListenConf.CertContent);
          } catch (const std::exception& e) {
            CRANE_ERROR("Read cert file error: {}", e.what());
            std::exit(1);
          }
          if (g_config.ListenConf.CertContent.empty()) {
            CRANE_ERROR(
                "UseTls is true, but the file specified by CertFilePath is "
                "empty");
          }
        } else {
          CRANE_ERROR("UseTls is true, but CertFilePath is empty");
          std::exit(1);
        }
        if (config["KeyFilePath"]) {
          g_config.ListenConf.KeyFilePath =
              config["KeyFilePath"].as<std::string>();

          try {
            boost::filesystem::load_string_file(g_config.ListenConf.KeyFilePath,
                                                g_config.ListenConf.KeyContent);
          } catch (const std::exception& e) {
            CRANE_ERROR("Read cert file error: {}", e.what());
            std::exit(1);
          }
          if (g_config.ListenConf.KeyContent.empty()) {
            CRANE_ERROR(
                "UseTls is true, but the file specified by KeyFilePath is "
                "empty");
          }
        } else {
          CRANE_ERROR("UseTls is true, but KeyFilePath is empty");
          std::exit(1);
        }
      } else {
        g_config.ListenConf.UseTls = false;
      }

      if (config["CraneCtldDebugLevel"])
        g_config.CraneCtldDebugLevel =
            config["CraneCtldDebugLevel"].as<std::string>();
      else
        std::exit(1);

      if (config["CraneCtldLogFile"])
        g_config.CraneCtldLogFile =
            config["CraneCtldLogFile"].as<std::string>();
      else
        g_config.CraneCtldLogFile = "/tmp/cranectld/cranectld.log";

      if (config["DbUser"]) {
        g_config.DbUser = config["DbUser"].as<std::string>();
        if (config["DbPassword"])
          g_config.DbPassword = config["DbPassword"].as<std::string>();
      }

      if (config["DbHost"])
        g_config.DbHost = config["DbHost"].as<std::string>();
      else
        g_config.DbHost = "localhost";

      if (config["DbPort"])
        g_config.DbPort = config["DbPort"].as<std::string>();
      else
        g_config.DbPort = "27017";  // default port 27017

      if (config["DbName"])
        g_config.DbName = config["DbName"].as<std::string>();
      else
        g_config.DbName = "crane_db";

      if (config["CraneCtldForeground"]) {
        auto val = config["CraneCtldForeground"].as<std::string>();
        if (val == "true")
          g_config.CraneCtldForeground = true;
        else
          g_config.CraneCtldForeground = false;
      }

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

          if (partition["name"]) {
            name.append(partition["name"].Scalar());
          } else
            std::exit(1);

          if (partition["nodes"]) {
            nodes = partition["nodes"].as<std::string>();
          } else
            std::exit(1);

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
              node_it->second->partition_name = name;
              part.nodes.emplace(node_it->first);
              CRANE_TRACE("Set the partition of node {} to {}", node_it->first,
                          name);
            }
          }

          g_config.Partitions.emplace(std::move(name), std::move(part));
        }
      }
    } catch (YAML::BadFile& e) {
      CRANE_ERROR("Can't open config file {}: {}", kDefaultConfigPath,
                  e.what());
      std::exit(1);
    }
  } else {
    cxxopts::Options options("craned");

    // clang-format off
    options.add_options()
        ("l,listen", "listening address",
         cxxopts::value<std::string>()->default_value("0.0.0.0"))
        ("p,port", "listening port",
         cxxopts::value<std::string>()->default_value(kCtldDefaultPort))
        ;
    // clang-format on

    auto parsed_args = options.parse(argc, argv);

    g_config.ListenConf.CraneCtldListenAddr =
        parsed_args["listen"].as<std::string>();
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

  if (g_config.CraneCtldForeground)
    StartServer();
  else
    StartDaemon();

  return 0;
}
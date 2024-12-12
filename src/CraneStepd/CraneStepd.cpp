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
#include "CraneStepdPublicDefs.h"
// Precompiled header comes first.

#include <cxxopts.hpp>

#include "crane/PasswordEntry.h"
#include "crane/PluginClient.h"

using CraneStepd::g_config;

void ParseConfig(int argc, char** argv) {
  cxxopts::Options options("craned");

  // clang-format off
  options.add_options()
      ("C,config", "Path to configuration file",
      cxxopts::value<std::string>()->default_value(kDefaultConfigPath))
      ("l,listen", "Listening address, format: <IP>:<port>",
       cxxopts::value<std::string>()->default_value(fmt::format("0.0.0.0:{}", kCranedDefaultPort)))
      ("L,log-file", "Path to Craned log file",
       cxxopts::value<std::string>()->default_value(fmt::format("{}{}",kDefaultCraneBaseDir, kDefaultCranedLogPath)))
      ("D,debug-level", "Logging level of Craned, format: <trace|debug|info|warn|error>",
       cxxopts::value<std::string>()->default_value("info"))
      ("v,version", "Display version information")
      ("h,help", "Display help for CraneStepd")
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
}

void CreateRequiredDirectories() {
  bool ok;
  ok = util::os::CreateFolders(g_config.CraneScriptDir);
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
       !g_cg_mgr->Mounted(Controller::DEVICES_CONTROLLER))) {
    CRANE_ERROR("Failed to initialize cpu,memory,devices cgroups controller.");
    std::exit(1);
  }
  if (g_cg_mgr->GetCgroupVersion() ==
          Craned::CgroupConstant::CgroupVersion::CGROUP_V2 &&
      (!g_cg_mgr->Mounted(Controller::CPU_CONTROLLER_V2) ||
       !g_cg_mgr->Mounted(Controller::MEMORY_CONTORLLER_V2))) {
    CRANE_ERROR("Failed to initialize cpu,memory cgroups controller.");
    std::exit(1);
  }

  g_thread_pool =
      std::make_unique<BS::thread_pool>(std::thread::hardware_concurrency());

  g_task_mgr = std::make_unique<Craned::TaskManager>();

  if (g_config.Plugin.Enabled) {
    CRANE_INFO("[Plugin] Plugin module is enabled.");
    g_plugin_client = std::make_unique<plugin::PluginClient>();
    g_plugin_client->InitChannelAndStub(g_config.Plugin.PlugindSockPath);
  }

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
  util::os::CheckProxyEnvironmentVariable();

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
  ParseConfig(argc, argv);
  InstallStackTraceHooks();
  StartServer();
}
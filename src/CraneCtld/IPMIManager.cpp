#include "IPMIManager.h"

#include "CranedKeeper.h"
#include "crane/Logger.h"
#include "CranedMetaContainer.h"

namespace Ctld {
constexpr int kMonitoringIntervalSeconds = 5;
constexpr int kIdleTimeoutSeconds = 30;
constexpr int kWakeupTimeoutSeconds = 180;
constexpr int kPowerUpTimeoutSeconds = 180;
constexpr int kShutdownTimeoutSeconds = 180;

constexpr char kPowerOnCmd[] = "on";
constexpr char kPowerOffCmd[] = "soft";
constexpr char kPowerStatusCmd[] = "status";

void IPMIManager::StartMonitoring() {
  should_stop_ = false;
  monitoring_thread_ = std::thread(&IPMIManager::MonitoringThread_, this);
  CRANE_DEBUG("IPMI monitoring thread started");
}

void IPMIManager::StopMonitoring() {
  should_stop_ = true;
  if (monitoring_thread_.joinable()) {
    monitoring_thread_.join();
    CRANE_DEBUG("IPMI monitoring thread stopped");
  }
}

NodeState IPMIManager::GetNodeState(const CranedId& craned_id) const {
  CRANE_DEBUG("Getting node state for {}", craned_id);
  std::lock_guard<std::mutex> lock(state_mutex_);
  auto it = node_states_.find(craned_id);
  if (it != node_states_.end()) {
    return it->second.state;
  }
  CRANE_DEBUG("Node {} state not found", craned_id);
  return NodeState::Unknown;
}

bool IPMIManager::SleepNode(const CranedId& craned_id) {
  std::lock_guard<std::mutex> lock(state_mutex_);
  
  auto& node_info = node_states_[craned_id];
  switch (node_info.state) {
    case NodeState::CranedRunning:
      node_info.state = NodeState::Sleeped;
      node_info.last_state_change = absl::Now();
      CRANE_DEBUG("Node {} entered sleep state", craned_id);
      return true;

    // 目前Sleepd状态只能从CranedRunning状态进入，如果能从Running状态进入Sleepd状态
    // 则应该也要能从Running状态进入CranedRunning状态，但是目前不行
    case NodeState::RunningWithNoCraned:
      CRANE_ERROR("Node {} is not running with Craned service, cannot sleep", craned_id);
      return false;

    case NodeState::Sleeped:
      CRANE_DEBUG("Node {} is already in sleep state", craned_id);
      return true;

    case NodeState::PoweringUp:
      CRANE_ERROR("Cannot sleep node {} while it's powering up", craned_id);
      return false;

    case NodeState::WakingUp:
      CRANE_ERROR("Cannot sleep node {} while it's waking up", craned_id);
      return false;

    case NodeState::ShuttingDown:
      CRANE_ERROR("Cannot sleep node {} while it's shutting down", craned_id);
      return false;

    case NodeState::Shutdown:
      CRANE_ERROR("Cannot sleep node {} while it's shut down", craned_id);
      return false;

    case NodeState::Unknown:
      CRANE_ERROR("Cannot sleep node {} in unknown state", craned_id);
      return false;
  }
  return false;
}

bool IPMIManager::WakeupNode(const CranedId& craned_id) {
  std::lock_guard<std::mutex> lock(state_mutex_);
  
  auto& node_info = node_states_[craned_id];
  switch (node_info.state) {
    case NodeState::Sleeped:
      node_info.state = NodeState::WakingUp;
      node_info.last_state_change = absl::Now();
      CRANE_DEBUG("Node {} is waking up from sleep", craned_id);
      return true;

    case NodeState::CranedRunning:
      CRANE_DEBUG("Node {} is already running with Craned service", craned_id);
      return true;

    case NodeState::RunningWithNoCraned:
      CRANE_DEBUG("Node {} is already powered on", craned_id);
      return true;

    case NodeState::WakingUp:
      CRANE_DEBUG("Node {} is already in wake up process", craned_id);
      return true;

    case NodeState::PoweringUp:
      CRANE_DEBUG("Node {} is in power up process", craned_id);
      return true;

    case NodeState::ShuttingDown:
      CRANE_ERROR("Cannot wake up node {} while it's shutting down", craned_id);
      return false;

    case NodeState::Shutdown:
      CRANE_DEBUG("Node {} is shut down, use PowerOnNode instead", craned_id);
      return false;

    case NodeState::Unknown:
      CRANE_ERROR("Cannot wake up node {} in unknown state", craned_id);
      return false;
  }
  return false;
}

bool IPMIManager::ShutdownNode(const CranedId& craned_id) {
  std::lock_guard<std::mutex> lock(state_mutex_);
  
  auto& node_info = node_states_[craned_id];
  switch (node_info.state) {
    case NodeState::CranedRunning:
    case NodeState::RunningWithNoCraned:
    case NodeState::Sleeped:
      CRANE_DEBUG("Shutting down node {}", craned_id);
      if (ExecutePowerCommand_(craned_id, kPowerOffCmd)) {
        node_info.state = NodeState::ShuttingDown;
        node_info.last_state_change = absl::Now();
        CRANE_DEBUG("Node {} is shutting down", craned_id);
        return true;
      }
      CRANE_ERROR("Failed to execute shutdown command for node {}", craned_id);
      return false;

    case NodeState::WakingUp:
      CRANE_ERROR("Cannot shutdown node {} while it's waking up", craned_id);
      return false;

    case NodeState::PoweringUp:
      CRANE_ERROR("Cannot shutdown node {} while it's powering up", craned_id);
      return false;

    case NodeState::Shutdown:
      CRANE_DEBUG("Node {} is already shut down", craned_id);
      return true;
    
    case NodeState::ShuttingDown:
      CRANE_DEBUG("Node {} is already in shutdown process", craned_id);
      return true;

    case NodeState::Unknown:
      CRANE_ERROR("Cannot shutdown node {} in unknown state", craned_id);
      return false;
  }
  
  return false;
}

bool IPMIManager::PowerOnNode(const CranedId& craned_id) {
  std::lock_guard<std::mutex> lock(state_mutex_);
  
  auto& node_info = node_states_[craned_id];
  switch (node_info.state) {
    case NodeState::Shutdown:
      if (ExecutePowerCommand_(craned_id, kPowerOnCmd)) {
        node_info.state = NodeState::PoweringUp;
        node_info.last_state_change = absl::Now();
        CRANE_DEBUG("Node {} is powering on", craned_id);
        return true;
      }
      CRANE_ERROR("Failed to execute power on command for node {}", craned_id);
      return false;

    case NodeState::CranedRunning:
      CRANE_DEBUG("Node {} is already running with Craned service", craned_id);
      return true;

    case NodeState::RunningWithNoCraned:
      CRANE_DEBUG("Node {} is powered on but Craned service not running", craned_id);
      return true;

    case NodeState::PoweringUp:
      CRANE_DEBUG("Node {} is already in power up process", craned_id);
      return true;

    case NodeState::WakingUp:
      CRANE_DEBUG("Node {} is in wake up process", craned_id);
      return true;

    case NodeState::Sleeped:
      CRANE_DEBUG("Node {} is in sleep state, use WakeupNode instead", craned_id);
      return false;

    case NodeState::ShuttingDown:
      CRANE_ERROR("Cannot power on node {} while it's shutting down", craned_id);
      return false;

    case NodeState::Unknown:
      CRANE_ERROR("Cannot power on node {} in unknown state", craned_id);
      return false;
  }
  
  return false;
}

void IPMIManager::MonitoringThread_() {
  while (!should_stop_) {
    auto craned_meta = g_meta_container->GetCranedMetaMapConstPtr();
    std::vector<CranedId> nodes_to_shutdown;
    
    {
      std::lock_guard<std::mutex> lock(state_mutex_);
      for (const auto& [craned_id, meta] : *craned_meta) {
        UpdateNodeState_(craned_id);
        
        if (node_states_[craned_id].state == NodeState::CranedRunning) {
          auto node_meta = meta.GetExclusivePtr();
          
          if (node_meta->running_task_resource_map.empty()) {
            absl::Time idle_start_time = node_meta->last_busy_time;
            if (idle_start_time == absl::Time()) {
              idle_start_time = node_states_[craned_id].last_state_change;
            }
            
            auto idle_time = absl::Now() - idle_start_time;
            if (idle_time > absl::Seconds(kIdleTimeoutSeconds)) {
              CRANE_DEBUG("Node {} has been idle for {}s, will initiate shutdown",
                         craned_id, absl::ToInt64Seconds(idle_time));
              nodes_to_shutdown.push_back(craned_id);
            }
          }
        }
      }
    }

    for (const auto& craned_id : nodes_to_shutdown) {
      ShutdownNode(craned_id);
    }

    std::this_thread::sleep_for(
        std::chrono::seconds(kMonitoringIntervalSeconds));
  }
}

void IPMIManager::UpdateNodeState_(const CranedId& craned_id) {
  auto& node_info = node_states_[craned_id];
  auto now = absl::Now();
  
  auto stub = g_craned_keeper->GetCranedStub(craned_id);
  bool is_connected = (stub && !stub->Invalid());
  
  switch (node_info.state) {
    case NodeState::PoweringUp:
      {
        PowerState power_status = GetPowerStatus_(craned_id);
        if (power_status == PowerState::On) {
          node_info.state = NodeState::RunningWithNoCraned;
          node_info.last_state_change = now;
          CRANE_DEBUG("Node {} powered up, waiting for Craned service", craned_id);
        } else if (now - node_info.last_state_change > 
                   absl::Seconds(kPowerUpTimeoutSeconds)) {
          node_info.state = NodeState::Unknown;
          node_info.last_state_change = now;
          CRANE_ERROR("Node {} power up timeout", craned_id);
          // 如果开机失败，则需要重试开机，不主动重试也行，反正会一直重试，就是效率低些
        }
      }
      break;

    case NodeState::RunningWithNoCraned:
      if (is_connected) {
        node_info.state = NodeState::CranedRunning;
        node_info.last_state_change = now;
        CRANE_DEBUG("Node {} is now running with Craned service", craned_id);
      } else {
        PowerState power_status = GetPowerStatus_(craned_id);
        if (power_status == PowerState::On) {
          if (StartCranedService_(craned_id)) {
            node_info.state = NodeState::CranedRunning;
            node_info.last_state_change = now;
            CRANE_DEBUG("Successfully started Craned service on node {}", craned_id);
          } else {
            CRANE_ERROR("Failed to start Craned service on node {}", craned_id);
          }
          // 这里也是，不主动重试也行，反正会一直重试，就是效率低些
        } else if (power_status == PowerState::Off) {
          node_info.state = NodeState::Shutdown;
          node_info.last_state_change = now;
          CRANE_DEBUG("Node {} is shut down", craned_id);
        } else {
          node_info.state = NodeState::Unknown;
          node_info.last_state_change = now;
          CRANE_ERROR("Node {} is in unknown state", craned_id);
        }
      }
      break;

    case NodeState::CranedRunning:
      if (!is_connected) {
        PowerState power_status = GetPowerStatus_(craned_id);
        if (power_status == PowerState::Off) {
          node_info.state = NodeState::Shutdown;
          node_info.last_state_change = now;
          CRANE_DEBUG("Node {} is shut down", craned_id);
        } else if (power_status == PowerState::On) {
          node_info.state = NodeState::RunningWithNoCraned;
          node_info.last_state_change = now;
          CRANE_DEBUG("Node {} is powered on but unreachable", craned_id);
        } else {
          node_info.state = NodeState::Unknown;
          node_info.last_state_change = now;
          CRANE_ERROR("Node {} is in unknown state", craned_id);
        }
      }
      break;

    case NodeState::Sleeped:
      // Sleeped状态是Craned服务主动进入的，不需要处理
      break;

    case NodeState::WakingUp:
      if (is_connected) {
        node_info.state = NodeState::CranedRunning;
        node_info.last_state_change = now;
        CRANE_DEBUG("Node {} has completed wake up", craned_id);
      } else if (now - node_info.last_state_change > 
                 absl::Seconds(kWakeupTimeoutSeconds)) {
        node_info.state = NodeState::Unknown;
        node_info.last_state_change = now;
        CRANE_ERROR("Node {} wake up timeout", craned_id);
      }
      break;

    case NodeState::ShuttingDown:
      {
        PowerState power_status = GetPowerStatus_(craned_id);
        if (power_status == PowerState::Off) {
          node_info.state = NodeState::Shutdown;
          node_info.last_state_change = now;
          CRANE_DEBUG("Node {} has completed shutdown", craned_id);
        } else if (now - node_info.last_state_change > 
                   absl::Seconds(kShutdownTimeoutSeconds)) {
          node_info.state = NodeState::Unknown;
          node_info.last_state_change = now;
          CRANE_ERROR("Node {} shutdown timeout", craned_id);
        }
        // 如果关机失败，则需要重试关机
      }
      break;

    case NodeState::Shutdown:
      // Shutdown状态是主动进入的，不需要处理
      break;

    case NodeState::Unknown:
      if (is_connected) {
        node_info.state = NodeState::CranedRunning;
        node_info.last_state_change = now;
        CRANE_DEBUG("Node {} recovered to running state with Craned service", craned_id);
      } else {
        PowerState power_status = GetPowerStatus_(craned_id);
        if (power_status == PowerState::On) {
          node_info.state = NodeState::RunningWithNoCraned;
          node_info.last_state_change = now;
          CRANE_DEBUG("Node {} is powered on but service not running", craned_id);
        } else if (power_status == PowerState::Off) {
          node_info.state = NodeState::Shutdown;
          node_info.last_state_change = now;
          CRANE_DEBUG("Node {} is shut down", craned_id);
        }
      }
      break;
  }
}

bool IPMIManager::ExecutePowerCommand_(
    const CranedId& craned_id, const std::string& command) {
  CRANE_DEBUG("Executing IPMI power {} command for node {}", command, craned_id);
  
  const auto& bmc = GetBMCConfig_(craned_id);
  if (bmc.ip.empty()) {
    CRANE_ERROR("No BMC address configured for node {}", craned_id);
    return false;
  }

  if (bmc.username.empty() || bmc.password.empty()) {
    CRANE_ERROR("Incomplete BMC credentials for node {}", craned_id);
    return false;
  }

  std::string ipmi_cmd = fmt::format(
      "ipmitool -I {} -H {} -p {} -U {} -P {} power {}",
      bmc.interface, bmc.ip, bmc.port, bmc.username, bmc.password, command);
  CRANE_DEBUG("Executing IPMI command: {}", ipmi_cmd);

  int ret = system(ipmi_cmd.c_str());
  if (ret != 0) {
    CRANE_ERROR("Failed to execute IPMI command for node {}: {} (return code: {})", 
                craned_id, command, ret);
    return false;
  }
  
  CRANE_DEBUG("Successfully executed IPMI power {} command for node {}", 
              command, craned_id);
  return true;
}

PowerState IPMIManager::GetPowerStatus_(const CranedId& craned_id) {
  CRANE_DEBUG("Getting IPMI status for node {}", craned_id);
  
  const auto& bmc = GetBMCConfig_(craned_id);
  if (bmc.ip.empty() || bmc.username.empty() || bmc.password.empty()) {
    CRANE_ERROR("Incomplete BMC configuration for node {}", craned_id);
    return PowerState::Unknown;
  }

  std::string ipmi_cmd = fmt::format(
      "ipmitool -I {} -H {} -p {} -U {} -P {} power {}",
      bmc.interface, bmc.ip, bmc.port, bmc.username, bmc.password, kPowerStatusCmd);

  FILE* pipe = popen(ipmi_cmd.c_str(), "r");
  if (!pipe) {
    CRANE_ERROR("Failed to execute IPMI command for node {}", craned_id);
    return PowerState::Unknown;
  }

  char buffer[128];
  std::string result;
  while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
    result += buffer;
  }
  pclose(pipe);

  CRANE_DEBUG("IPMI power status for node {}: {}", craned_id, result);
  
  if (result.find("Chassis Power is on") != std::string::npos) {
    return PowerState::On;
  } else if (result.find("Chassis Power is off") != std::string::npos) {
    return PowerState::Off;
  }

  return PowerState::Unknown;
}

BMCConfig IPMIManager::GetBMCConfig_(const CranedId& craned_id) const {
  auto craned_meta = g_meta_container->GetCranedMetaMapConstPtr();
  auto node_meta = craned_meta->at(craned_id).GetExclusivePtr();
  BMCConfig bmc_config;
  bmc_config.ip = node_meta->static_meta.bmc.ip;
  bmc_config.port = node_meta->static_meta.bmc.port;
  bmc_config.username = node_meta->static_meta.bmc.username;
  bmc_config.password = node_meta->static_meta.bmc.password;
  bmc_config.interface = node_meta->static_meta.bmc.interface;
  return bmc_config;
}

SSHConfig IPMIManager::GetSSHConfig_(const CranedId& craned_id) const {
  auto craned_meta = g_meta_container->GetCranedMetaMapConstPtr();
  auto node_meta = craned_meta->at(craned_id).GetExclusivePtr();
  SSHConfig ssh_config;
  ssh_config.host = node_meta->static_meta.ssh.ip;
  ssh_config.port = node_meta->static_meta.ssh.port;
  ssh_config.username = node_meta->static_meta.ssh.username;
  ssh_config.password = node_meta->static_meta.ssh.password;
  return ssh_config;
}

bool IPMIManager::StartCranedService_(const CranedId& craned_id) {
  const char* start_cmd = "systemctl start craned";
  return ExecuteRemoteCommand_(craned_id, start_cmd);
}

bool IPMIManager::ExecuteRemoteCommand_(const CranedId& craned_id, 
                                      const std::string& command) {
  ssh_session session = ssh_new();
  if (session == nullptr) {
    CRANE_ERROR("Failed to create SSH session for node {}", craned_id);
    return false;
  }

  try {
    auto config = GetSSHConfig_(craned_id);
    
    ssh_options_set(session, SSH_OPTIONS_HOST, config.host.c_str());
    ssh_options_set(session, SSH_OPTIONS_USER, config.username.c_str());
    ssh_options_set(session, SSH_OPTIONS_PORT, &config.port);

    int rc = ssh_connect(session);
    if (rc != SSH_OK) {
      CRANE_ERROR("Failed to connect to node {}: {}", 
                  craned_id, ssh_get_error(session));
      ssh_free(session);
      return false;
    }

    // 密码认证
    rc = ssh_userauth_password(session, nullptr, config.password.c_str());
    if (rc != SSH_AUTH_SUCCESS) {
      CRANE_ERROR("Failed to authenticate for node {}: {}", 
                  craned_id, ssh_get_error(session));
      ssh_disconnect(session);
      ssh_free(session);
      return false;
    }

    // 执行命令
    ssh_channel channel = ssh_channel_new(session);
    if (channel == nullptr) {
      CRANE_ERROR("Failed to create SSH channel for node {}", craned_id);
      ssh_disconnect(session);
      ssh_free(session);
      return false;
    }

    rc = ssh_channel_open_session(channel);
    if (rc != SSH_OK) {
      CRANE_ERROR("Failed to open SSH channel for node {}", craned_id);
      ssh_channel_free(channel);
      ssh_disconnect(session);
      ssh_free(session);
      return false;
    }

    rc = ssh_channel_request_exec(channel, command.c_str());
    if (rc != SSH_OK) {
      CRANE_ERROR("Failed to execute command on node {}: {}", 
                  craned_id, command);
      ssh_channel_close(channel);
      ssh_channel_free(channel);
      ssh_disconnect(session);
      ssh_free(session);
      return false;
    }

    ssh_channel_send_eof(channel);
    ssh_channel_close(channel);
    ssh_channel_free(channel);
    ssh_disconnect(session);
    ssh_free(session);

    CRANE_DEBUG("Successfully executed command on node {}: {}", 
                craned_id, command);
    return true;

  } catch (const std::exception& e) {
    CRANE_ERROR("Unexpected error executing command on node {}: {}", 
                craned_id, e.what());
    ssh_free(session);
    return false;
  }
}

} // namespace Ctld 
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

#include "IPMIManager.h"

#include "CranedKeeper.h"
#include "crane/Logger.h"

namespace Ctld {
constexpr int kMonitoringIntervalSeconds = 3;
constexpr int kIdleTimeoutSeconds = 60;
constexpr int kWakeupTimeoutSeconds = 180;
constexpr int kPowerUpTimeoutSeconds = 180;
constexpr int kShutdownTimeoutSeconds = 180;
constexpr int kMaxRetryCount = 3;

constexpr char kPowerOnCmd[] = "on";
constexpr char kPowerOffCmd[] = "soft";
constexpr char kPowerStatusCmd[] = "status";
constexpr char kStartCranedServiceCmd[] = "systemctl start craned";
constexpr char kSleepNodeCmd[] = "systemctl suspend";

void IPMIManager::StartMonitoring_() {
  should_stop_ = false;
  monitoring_thread_ = std::thread(&IPMIManager::MonitoringThread_, this);
  CRANE_DEBUG("IPMI monitoring thread started");
}

void IPMIManager::StopMonitoring_() {
  should_stop_ = true;
  if (monitoring_thread_.joinable()) {
    monitoring_thread_.join();
    CRANE_DEBUG("IPMI monitoring thread stopped");
  }
}

void IPMIManager::SetCranedState_(
    const CranedMetaContainer::CranedMetaPtr& craned_meta, CranedState state) {
  CRANE_DEBUG("Setting craned {} state to {}",
              craned_meta->static_meta.hostname, CranedStateToStr(state));
  craned_meta->state_info.state = state;
  craned_meta->state_info.last_state_change = absl::Now();
}

bool IPMIManager::SleepNode(const CranedId& craned_id) {
  auto craned_meta = g_meta_container->GetCranedMetaPtr(craned_id);
  if (!craned_meta) {
    CRANE_ERROR("Craned {} not found in craned_states_", craned_id);
    return false;
  }
  switch (craned_meta->state_info.state) {
  case CranedState::Running:
    if (ExecuteRemoteCommand_(craned_meta, kSleepNodeCmd)) {
      craned_meta->state_info.state = CranedState::Sleeped;
      craned_meta->state_info.last_state_change = absl::Now();
      CRANE_DEBUG("Craned {} entered sleep state via SSH", craned_id);
      return true;
    }

    CRANE_ERROR("Failed to put craned {} to sleep", craned_id);
    return false;

  case CranedState::Sleeped:
    CRANE_DEBUG("Craned {} is already in sleep state", craned_id);
    return true;

  case CranedState::PoweringUp:
    CRANE_ERROR("Cannot sleep craned {} while it's powering up", craned_id);
    return false;

  case CranedState::WakingUp:
    CRANE_ERROR("Cannot sleep craned {} while it's waking up", craned_id);
    return false;

  case CranedState::ShuttingDown:
    CRANE_ERROR("Cannot sleep craned {} while it's shutting down", craned_id);
    return false;

  case CranedState::Shutdown:
    CRANE_ERROR("Cannot sleep craned {} while it's shut down", craned_id);
    return false;

  case CranedState::Unknown:
    CRANE_ERROR("Cannot sleep craned {} in unknown state", craned_id);
    return false;
  }
  return false;
}

bool IPMIManager::WakeupNode(const CranedId& craned_id) {
  auto craned_meta = g_meta_container->GetCranedMetaPtr(craned_id);
  if (!craned_meta) {
    CRANE_ERROR("Craned {} not found in craned_states_", craned_id);
    return false;
  }

  switch (craned_meta->state_info.state) {
  case CranedState::Sleeped:
    if (ExecuteIPMICommand_(craned_meta, kPowerOnCmd)) {
      craned_meta->state_info.state = CranedState::WakingUp;
      craned_meta->state_info.last_state_change = absl::Now();
      CRANE_DEBUG("Craned {} is waking up via IPMI power on",
                  craned_meta->static_meta.hostname);
      return true;
    }

    CRANE_ERROR("Failed to wake up craned {}", craned_id);
    return false;

  case CranedState::Running:
    CRANE_DEBUG("Craned {} is already running with Craned service", craned_id);
    return true;

  case CranedState::WakingUp:
    CRANE_DEBUG("Craned {} is already in wake up process", craned_id);
    return true;

  case CranedState::PoweringUp:
    CRANE_DEBUG("Craned {} is in power up process", craned_id);
    return true;

  case CranedState::ShuttingDown:
    CRANE_ERROR("Cannot wake up craned {} while it's shutting down", craned_id);
    return false;

  case CranedState::Shutdown:
    CRANE_DEBUG("Craned {} is shut down, use PowerOnNode instead", craned_id);
    return false;

  case CranedState::Unknown:
    CRANE_ERROR("Cannot wake up craned {} in unknown state", craned_id);
    return false;
  }
  return false;
}

bool IPMIManager::ShutdownNode(const CranedId& craned_id) {
  auto craned_meta = g_meta_container->GetCranedMetaPtr(craned_id);
  if (!craned_meta) {
    CRANE_ERROR("Craned {} not found in craned_states_", craned_id);
    return false;
  }
  switch (craned_meta->state_info.state) {
  case CranedState::Running:
  case CranedState::Sleeped:
    CRANE_DEBUG("Shutting down craned {}", craned_id);
    if (ExecuteIPMICommand_(craned_meta, kPowerOffCmd)) {
      craned_meta->state_info.state = CranedState::ShuttingDown;
      craned_meta->state_info.last_state_change = absl::Now();
      CRANE_DEBUG("Craned {} is shutting down", craned_id);
      return true;
    }
    CRANE_ERROR("Failed to execute shutdown command for craned {}", craned_id);
    return false;

  case CranedState::WakingUp:
    CRANE_ERROR("Cannot shutdown craned {} while it's waking up", craned_id);
    return false;

  case CranedState::PoweringUp:
    CRANE_ERROR("Cannot shutdown  {} while it's powering up", craned_id);
    return false;

  case CranedState::Shutdown:
    CRANE_DEBUG("Craned {} is already shut down", craned_id);
    return true;

  case CranedState::ShuttingDown:
    CRANE_DEBUG("Craned {} is already in shutdown process", craned_id);
    return true;

  case CranedState::Unknown:
    CRANE_ERROR("Cannot shutdown craned {} in unknown state", craned_id);
    return false;
  }

  return false;
}

bool IPMIManager::PowerOnNode(const CranedId& craned_id) {
  auto craned_meta = g_meta_container->GetCranedMetaPtr(craned_id);
  if (!craned_meta) {
    CRANE_ERROR("Craned {} not found in craned_states_", craned_id);
    return false;
  }
  switch (craned_meta->state_info.state) {
  case CranedState::Shutdown:
    if (ExecuteIPMICommand_(craned_meta, kPowerOnCmd)) {
      craned_meta->state_info.state = CranedState::PoweringUp;
      craned_meta->state_info.last_state_change = absl::Now();
      CRANE_DEBUG("Craned {} is powering on", craned_id);
      return true;
    }
    CRANE_ERROR("Failed to execute power on command for craned {}", craned_id);
    return false;

  case CranedState::Running:
    CRANE_DEBUG("Craned {} is already running with Craned service", craned_id);
    return true;

  case CranedState::PoweringUp:
    CRANE_DEBUG("Craned {} is already in power up process", craned_id);
    return true;

  case CranedState::WakingUp:
    CRANE_DEBUG("Craned {} is in wake up process", craned_id);
    return true;

  case CranedState::Sleeped:
    CRANE_DEBUG("Craned {} is in sleep state, use WakeupNode instead",
                craned_id);
    return false;

  case CranedState::ShuttingDown:
    CRANE_ERROR("Cannot power on craned {} while it's shutting down",
                craned_id);
    return false;

  case CranedState::Unknown:
    CRANE_ERROR("Cannot power on craned {} in unknown state", craned_id);
    return false;
  }

  return false;
}

void IPMIManager::MonitoringThread_() {
  while (!should_stop_) {
    auto craned_meta_map = g_meta_container->GetCranedMetaMapConstPtr();
    std::vector<CranedId> nodes_to_shutdown;

    for (const auto& [craned_id, craned_meta] : *craned_meta_map) {
      UpdateNodeState_(craned_id);

      auto craned_meta_ptr = craned_meta.GetExclusivePtr();
      if (craned_meta_ptr->state_info.state == CranedState::Running) {
        if (craned_meta_ptr->running_task_resource_map.empty()) {
          absl::Time idle_start_time = craned_meta_ptr->last_busy_time;
          if (idle_start_time == absl::Time()) {
            idle_start_time = craned_meta_ptr->state_info.last_state_change;
          }

          auto idle_time = absl::Now() - idle_start_time;
          if (idle_time > absl::Seconds(kIdleTimeoutSeconds)) {
            CRANE_DEBUG(
                "Craned {} has been idle for {}s, will initiate shutdown",
                craned_id, absl::ToInt64Seconds(idle_time));
            nodes_to_shutdown.push_back(craned_id);
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
  auto craned_meta = g_meta_container->GetCranedMetaPtr(craned_id);
  if (!craned_meta) {
    CRANE_ERROR("Craned {} not found in craned_states_", craned_id);
    return;
  }
  auto now = absl::Now();

  auto stub = g_craned_keeper->GetCranedStub(craned_id);
  bool is_connected = (stub && !stub->Invalid());

  Config::BMC bmc = craned_meta->static_meta.bmc;
  PowerState power_status = GetPowerStatus_(craned_id, bmc);

  switch (craned_meta->state_info.state) {
  case CranedState::Running:
    if (!is_connected) {
      if (power_status == PowerState::Off) {
        // Manual power off
        SetCranedState_(craned_meta, CranedState::Shutdown);
        CRANE_DEBUG("Craned {} status changed to {} from {}", craned_id,
                    CranedStateToStr(craned_meta->state_info.state),
                    CranedStateToStr(CranedState::Running));
      } else if (power_status == PowerState::On) {
        // Manual sleep
        SetCranedState_(craned_meta, CranedState::Sleeped);
        CRANE_DEBUG("Craned {} status changed to {} from {}", craned_id,
                    CranedStateToStr(craned_meta->state_info.state),
                    CranedStateToStr(CranedState::Running));
      } else {
        // Craned abnormal
        SetCranedState_(craned_meta, CranedState::Unknown);
        CRANE_ERROR("Craned {} status changed to {} from {}", craned_id,
                    CranedStateToStr(craned_meta->state_info.state),
                    CranedStateToStr(CranedState::Running));
      }
    } else {
      CRANE_DEBUG("Craned {} is still in running state", craned_id);
    }
    break;

  case CranedState::Sleeped:
    if (is_connected) {
      // Manual wake up
      SetCranedState_(craned_meta, CranedState::Running);
      CRANE_DEBUG("Craned {} status changed to {} from {}", craned_id,
                  CranedStateToStr(craned_meta->state_info.state),
                  CranedStateToStr(CranedState::Sleeped));
    } else if (power_status == PowerState::Off) {
      // Manual power off
      SetCranedState_(craned_meta, CranedState::Shutdown);
      CRANE_DEBUG("Craned {} status changed to {} from {}", craned_id,
                  CranedStateToStr(craned_meta->state_info.state),
                  CranedStateToStr(CranedState::Sleeped));
    } else {
      CRANE_DEBUG("Craned {} is still in sleep state", craned_id);
    }
    break;

  case CranedState::Shutdown:
    if (power_status == PowerState::On) {
      // Manual power on
      SetCranedState_(craned_meta, CranedState::PoweringUp);
      CRANE_DEBUG("Craned {} status changed to {} from {}", craned_id,
                  CranedStateToStr(craned_meta->state_info.state),
                  CranedStateToStr(CranedState::Shutdown));
    } else {
      CRANE_DEBUG("Craned {} is still in shutdown state", craned_id);
    }
    break;

  case CranedState::PoweringUp: {
    if (power_status == PowerState::On) {
      if (is_connected) {
        SetCranedState_(craned_meta, CranedState::Running);
        CRANE_DEBUG("Craned {} powered up, Craned service is running",
                    craned_id);
      } else {
        CRANE_ERROR("Craned {} powered up, Craned service is not running",
                    craned_id);
      }
    } else if (now - craned_meta->state_info.last_state_change >
               absl::Seconds(kPowerUpTimeoutSeconds)) {
      CRANE_ERROR("Craned {} power up timeout, try to power up again",
                  craned_id);
      PowerOnNode(craned_id);
    }
  } break;

  case CranedState::WakingUp:
    if (is_connected) {
      SetCranedState_(craned_meta, CranedState::Running);
      CRANE_DEBUG("Craned {} has completed wake up", craned_id);
    } else if (now - craned_meta->state_info.last_state_change >
               absl::Seconds(kWakeupTimeoutSeconds)) {
      CRANE_ERROR("Craned {} wake up timeout, try to wake up again", craned_id);
      WakeupNode(craned_id);
    }
    break;

  case CranedState::ShuttingDown: {
    if (power_status == PowerState::Off) {
      SetCranedState_(craned_meta, CranedState::Shutdown);
      CRANE_DEBUG("Craned {} has completed shutdown", craned_id);
    } else if (now - craned_meta->state_info.last_state_change >
               absl::Seconds(kShutdownTimeoutSeconds)) {
      CRANE_ERROR("Craned {} shutdown timeout, try to shutdown again",
                  craned_id);
      ShutdownNode(craned_id);
    }
  } break;

  case CranedState::Unknown:
    if (is_connected) {
      SetCranedState_(craned_meta, CranedState::Running);
      CRANE_DEBUG("Craned {} recovered to running state with Craned service",
                  craned_id);
    } else {
      if (power_status == PowerState::On) {
        SetCranedState_(craned_meta, CranedState::Sleeped);
        CRANE_DEBUG("Craned {} is powered on but unreachable", craned_id);
      } else if (power_status == PowerState::Off) {
        SetCranedState_(craned_meta, CranedState::Shutdown);
        CRANE_DEBUG("Craned {} is shut down", craned_id);
      }
    }
    break;
  }
}

bool IPMIManager::ExecuteIPMICommand_(
    const CranedMetaContainer::CranedMetaPtr& craned_meta,
    const std::string& command) {
  CRANE_DEBUG("Executing IPMI power {} command for craned {}", command,
              craned_meta->static_meta.hostname);

  const auto& bmc = craned_meta->static_meta.bmc;
  if (bmc.ip.empty()) {
    CRANE_ERROR("No BMC address configured for craned {}",
                craned_meta->static_meta.hostname);
    return false;
  }

  if (bmc.username.empty() || bmc.password.empty()) {
    CRANE_ERROR("Incomplete BMC credentials for craned {}",
                craned_meta->static_meta.hostname);
    return false;
  }

  std::string ipmi_cmd = fmt::format(
      "ipmitool -I {} -H {} -p {} -U {} -P {} power {}", bmc.interface, bmc.ip,
      bmc.port, bmc.username, bmc.password, command);
  CRANE_DEBUG("Executing IPMI command: {}", ipmi_cmd);

  int ret = system(ipmi_cmd.c_str());
  if (ret != 0) {
    CRANE_ERROR(
        "Failed to execute IPMI command for craned {}: {} (return code: {})",
        craned_meta->static_meta.hostname, command, ret);
    return false;
  }

  CRANE_DEBUG("Successfully executed IPMI power {} command for craned {}",
              command, craned_meta->static_meta.hostname);
  return true;
}

PowerState IPMIManager::GetPowerStatus_(const CranedId& craned_id,
                                        const Config::BMC& bmc) {
  if (bmc.ip.empty() || bmc.username.empty() || bmc.password.empty()) {
    CRANE_ERROR("Incomplete BMC configuration for craned {}", craned_id);
    return PowerState::Unknown;
  }

  std::string ipmi_cmd = fmt::format(
      "ipmitool -I {} -H {} -p {} -U {} -P {} power {}", bmc.interface, bmc.ip,
      bmc.port, bmc.username, bmc.password, kPowerStatusCmd);

  FILE* pipe = popen(ipmi_cmd.c_str(), "r");
  if (!pipe) {
    CRANE_ERROR("Failed to execute IPMI command for craned {}", craned_id);
    return PowerState::Unknown;
  }

  char buffer[128];
  std::string result;
  while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
    result += buffer;
  }
  pclose(pipe);

  CRANE_DEBUG("IPMI power status for craned {}: {}", craned_id, result);

  if (result.find("Chassis Power is on") != std::string::npos) {
    return PowerState::On;
  } else if (result.find("Chassis Power is off") != std::string::npos) {
    return PowerState::Off;
  }

  return PowerState::Unknown;
}

bool IPMIManager::ExecuteRemoteCommand_(
    const CranedMetaContainer::CranedMetaPtr& craned_meta,
    const std::string& command) {
  ssh_session session = ssh_new();
  if (session == nullptr) {
    CRANE_ERROR("Failed to create SSH session for craned {}",
                craned_meta->static_meta.hostname);
    return false;
  }

  try {
    auto config = craned_meta->static_meta.ssh;

    ssh_options_set(session, SSH_OPTIONS_HOST, config.ip.c_str());
    ssh_options_set(session, SSH_OPTIONS_USER, config.username.c_str());
    ssh_options_set(session, SSH_OPTIONS_PORT, &config.port);

    int rc = ssh_connect(session);
    if (rc != SSH_OK) {
      CRANE_ERROR("Failed to connect to craned {}: {}",
                  craned_meta->static_meta.hostname, ssh_get_error(session));
      ssh_free(session);
      return false;
    }

    // 密码认证
    rc = ssh_userauth_password(session, nullptr, config.password.c_str());
    if (rc != SSH_AUTH_SUCCESS) {
      CRANE_ERROR("Failed to authenticate for craned {}: {}",
                  craned_meta->static_meta.hostname, ssh_get_error(session));
      ssh_disconnect(session);
      ssh_free(session);
      return false;
    }

    // 执行命令
    ssh_channel channel = ssh_channel_new(session);
    if (channel == nullptr) {
      CRANE_ERROR("Failed to create SSH channel for craned {}",
                  craned_meta->static_meta.hostname);
      ssh_disconnect(session);
      ssh_free(session);
      return false;
    }

    rc = ssh_channel_open_session(channel);
    if (rc != SSH_OK) {
      CRANE_ERROR("Failed to open SSH channel for craned {}",
                  craned_meta->static_meta.hostname);
      ssh_channel_free(channel);
      ssh_disconnect(session);
      ssh_free(session);
      return false;
    }

    rc = ssh_channel_request_exec(channel, command.c_str());
    if (rc != SSH_OK) {
      CRANE_ERROR("Failed to execute command on craned {}: {}",
                  craned_meta->static_meta.hostname, command);
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

    CRANE_DEBUG("Successfully executed command on craned {}: {}",
                craned_meta->static_meta.hostname, command);
    return true;

  } catch (const std::exception& e) {
    CRANE_ERROR("Unexpected error executing command on craned {}: {}",
                craned_meta->static_meta.hostname, e.what());
    ssh_free(session);
    return false;
  }
}

}  // namespace Ctld
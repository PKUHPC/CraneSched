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
#include "CtldPublicDefs.h"
#include "DbClient.h"
#include "crane/Logger.h"

namespace Ctld {

namespace {
constexpr int kMonitoringIntervalSeconds = 3;
constexpr int kIdleTimeoutSeconds = 6000000;
constexpr int kWakeupTimeoutSeconds = 180;
constexpr int kPowerUpTimeoutSeconds = 180;
constexpr int kShutdownTimeoutSeconds = 180;
constexpr int kSleepTimeoutSeconds = 600;

constexpr char kPowerOnCmd[] = "on";
constexpr char kShutdownCmd[] = "soft";
constexpr char kPowerStatusCmd[] = "status";
constexpr char kSuspendCmd[] = "systemctl suspend";
constexpr char kRestartCranedCmd[] = "systemctl restart craned";
}  // namespace

CranedStateMachine::CranedStateMachine() {
  std::vector<StateTransition> transitions = {
      // Running state transition
      {CranedState::Running, true, PowerState::On, CranedState::Running,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} is running normally", id);
       }},
      {CranedState::Running, false, PowerState::On, CranedState::PreparingSleep,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} lost connection, entering PreparingSleep state",
                     id);
       }},
      {CranedState::Running, false, PowerState::Off, CranedState::Shutdown,
       [](auto& meta, auto id) {
         CRANE_DEBUG(
             "Craned {} has been shut down, transition from Running to "
             "Shutdown",
             id);
       }},
      {CranedState::Running, true, PowerState::Off, CranedState::Shutdown,
       [](auto& meta, auto id) {
         CRANE_DEBUG(
             "Craned {} has been shut down, transition from Running to "
             "Shutdown",
             id);
       }},

      // Sleeped state transition
      {CranedState::Sleeped, true, PowerState::On, CranedState::Running,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} is running from sleep state", id);
       }},
      {CranedState::Sleeped, false, PowerState::On, CranedState::Sleeped,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} remains in sleep state", id);
       }},
      {CranedState::Sleeped, false, PowerState::Off, CranedState::Shutdown,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} has been shut down from sleep state", id);
       }},
      {CranedState::Sleeped, true, PowerState::Off, CranedState::Shutdown,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} has been shut down from sleep state", id);
       }},

      // Shutdown state transition
      {CranedState::Shutdown, true, PowerState::On, CranedState::Running,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} is running from shutdown", id);
       }},
      {CranedState::Shutdown, false, PowerState::On, CranedState::PoweringUp,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} is powering up from shutdown", id);
       }},
      {CranedState::Shutdown, false, PowerState::Off, CranedState::Shutdown,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} remains shutdown", id);
       }},
      {CranedState::Shutdown, true, PowerState::Off, CranedState::Shutdown,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} remains shutdown", id);
       }},

      // WakingUp state transition
      {CranedState::WakingUp, true, PowerState::On, CranedState::Running,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} is now running", id);
       }},
      {CranedState::WakingUp, false, PowerState::On, CranedState::WakingUp,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} is still waking up", id);
       }},
      {CranedState::WakingUp, true, PowerState::Off, CranedState::Shutdown,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} has been shut down from wake up state", id);
       }},
      {CranedState::WakingUp, true, PowerState::Off, CranedState::WakingUp,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} is still waking up", id);
       }},

      /** PreparingSleep state is only handled by a timeout check, see
      UpdateState_()
      PreparingSleep state transition
      {CranedState::PreparingSleep, true, PowerState::On,
      CranedState::Running,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} is running from preparing sleep", id);
       }},
      {CranedState::PreparingSleep, false, PowerState::On,
      CranedState::Sleeped,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} entered sleep state", id);
       }},
      {CranedState::PreparingSleep, false, PowerState::Off,
       CranedState::Shutdown,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} has been shut down from preparing sleep
         state",
                     id);
       }},
      {CranedState::PreparingSleep, true, PowerState::Off,
       CranedState::Shutdown,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} has been shut down from preparing sleep
         state",
                     id);
       }}, **/

      // PoweringUp state transition
      {CranedState::PoweringUp, true, PowerState::On, CranedState::Running,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} is now running", id);
       }},
      {CranedState::PoweringUp, false, PowerState::On, CranedState::PoweringUp,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} is still powering up", id);
       }},
      {CranedState::PoweringUp, true, PowerState::Off, CranedState::Shutdown,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} has been shut down from powering up", id);
       }},
      {CranedState::PoweringUp, false, PowerState::Off, CranedState::Shutdown,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} has been shut down from powering up", id);
       }},

      // ShuttingDown state transition
      {CranedState::ShuttingDown, true, PowerState::On, CranedState::Running,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} is running from shutting down", id);
       }},
      {CranedState::ShuttingDown, false, PowerState::On,
       CranedState::ShuttingDown,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} is still shutting down", id);
       }},
      {CranedState::ShuttingDown, false, PowerState::Off, CranedState::Shutdown,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} completed shutdown", id);
       }},
      {CranedState::ShuttingDown, true, PowerState::Off, CranedState::Shutdown,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} has been shut down from shutting down", id);
       }},

      // Unknown state transition
      {CranedState::Unknown, true, PowerState::On, CranedState::Running,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} is actually running", id);
       }},
      {CranedState::Unknown, false, PowerState::On, CranedState::Sleeped,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} is actually in sleep state", id);
       }},
      {CranedState::Unknown, false, PowerState::Off, CranedState::Shutdown,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} is actually shutdown", id);
       }},
      {CranedState::Unknown, true, PowerState::Off, CranedState::Shutdown,
       [](auto& meta, auto id) {
         CRANE_DEBUG("Craned {} is actually shutdown", id);
       }},
  };

  for (const auto& transition : transitions) {
    TransitionKey key{transition.current_state, transition.is_connected,
                      transition.power_status};
    transition_map_[key] = transition;
  }
}

std::optional<CranedStateMachine::StateTransition>
CranedStateMachine::NextTransition(CranedState current_state, bool is_connected,
                                   PowerState power_status) const {
  TransitionKey key{current_state, is_connected, power_status};
  auto it = transition_map_.find(key);

  if (it != transition_map_.end()) {
    return it->second;
  }

  return std::nullopt;
}

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

void IPMIManager::SetCranedInfo_(
    const CranedMetaContainer::CranedMetaPtr& craned_meta) {
  if (!craned_meta->remote_meta.nic.mac_address.empty()) {
    CRANE_DEBUG("Using cached NIC info for craned {}: interface {}, MAC {}",
                craned_meta->static_meta.hostname,
                craned_meta->remote_meta.nic.interface_name,
                craned_meta->remote_meta.nic.mac_address);
    return;
  }

  auto stub = g_craned_keeper->GetCranedStub(craned_meta->static_meta.hostname);
  if (!stub) {
    CRANE_ERROR("Failed to get stub for craned {}",
                craned_meta->static_meta.hostname);
    return;
  }

  craned_meta->static_meta.ssh.ip = stub->GetCranedIp();

  CraneErr err = stub->QueryCranedNICInfo(&craned_meta->remote_meta);
  if (err == CraneErr::kOk) {
    g_db_client->UpsertNodeNICInfo(craned_meta->static_meta.hostname,
                                   craned_meta->remote_meta.nic.interface_name,
                                   craned_meta->remote_meta.nic.mac_address);

    CRANE_DEBUG("Updated NIC info for craned {}: interface {}, MAC {}",
                craned_meta->static_meta.hostname,
                craned_meta->remote_meta.nic.interface_name,
                craned_meta->remote_meta.nic.mac_address);
  }
}

void IPMIManager::SetCranedState_(
    const CranedMetaContainer::CranedMetaPtr& craned_meta, CranedState state) {
  CRANE_DEBUG("Setting craned {} state to {}",
              craned_meta->static_meta.hostname, CranedStateToStr(state));
  if (state == CranedState::Running) {
    SetCranedInfo_(craned_meta);
  }
  craned_meta->state_info.state = state;
  craned_meta->state_info.last_state_change = absl::Now();
}

bool IPMIManager::SleepCraned(const CranedId& craned_id,
                              std::string* error_msg) {
  auto craned_meta = g_meta_container->GetCranedMetaPtr(craned_id);
  if (!craned_meta) {
    std::string msg =
        fmt::format("Craned {} not found in craned_states_", craned_id);
    CRANE_ERROR(msg);
    if (error_msg) *error_msg = msg;
    return false;
  }

  std::string msg;
  bool success = false;

  switch (craned_meta->state_info.state) {
  case CranedState::Running:
  case CranedState::PreparingSleep:
    SetCranedState_(craned_meta, CranedState::PreparingSleep);
    if (ExecuteRemoteCommand_(craned_meta, kSuspendCmd)) {
      msg = fmt::format("Craned {} entered preparing sleep state", craned_id);
      CRANE_DEBUG(msg);
      success = true;
    } else {
      msg = fmt::format("Failed to execute suspend command for craned {}",
                        craned_id);
      CRANE_ERROR(msg);
    }
    break;

  case CranedState::Sleeped:
    msg = fmt::format("Craned {} is already in sleep state", craned_id);
    CRANE_DEBUG(msg);
    break;

  case CranedState::PoweringUp:
    msg = fmt::format("Craned {} is powering up, cannot sleep", craned_id);
    CRANE_DEBUG(msg);
    break;

  case CranedState::WakingUp:
    msg = fmt::format("Craned {} is waking up, cannot sleep", craned_id);
    CRANE_DEBUG(msg);
    break;

  case CranedState::ShuttingDown:
    msg = fmt::format("Craned {} is shutting down, cannot sleep", craned_id);
    CRANE_DEBUG(msg);
    break;

  case CranedState::Shutdown:
    msg = fmt::format("Craned {} is shut down, cannot sleep", craned_id);
    CRANE_DEBUG(msg);
    break;

  case CranedState::Unknown:
    msg = fmt::format("Cannot sleep craned {} in unknown state", craned_id);
    CRANE_DEBUG(msg);
    break;
  }

  if (error_msg) *error_msg = msg;
  return success;
}

bool IPMIManager::WakeupCraned(const CranedId& craned_id,
                               std::string* error_msg) {
  auto craned_meta = g_meta_container->GetCranedMetaPtr(craned_id);
  if (!craned_meta) {
    std::string msg =
        fmt::format("Craned {} not found in craned_states_", craned_id);
    CRANE_ERROR(msg);
    if (error_msg) *error_msg = msg;
    return false;
  }

  std::string msg;
  bool success = false;

  switch (craned_meta->state_info.state) {
  case CranedState::Sleeped:
  case CranedState::WakingUp:
    if (craned_meta->remote_meta.nic.mac_address.empty()) {
      if (!g_db_client->GetNodeNICInfo(
              craned_meta->static_meta.hostname,
              &craned_meta->remote_meta.nic.interface_name,
              &craned_meta->remote_meta.nic.mac_address)) {
        msg = fmt::format(
            "No MAC address found for craned {} in memory or database",
            craned_id);
        CRANE_ERROR(msg);
        break;
      }
    }

    if (SendCranedWakeOnLanPacket_(craned_meta->remote_meta.nic.mac_address)) {
      SetCranedState_(craned_meta, CranedState::WakingUp);
      msg = fmt::format("Sent WoL packet to craned {} (interface: {}, MAC: {})",
                        craned_id, craned_meta->remote_meta.nic.interface_name,
                        craned_meta->remote_meta.nic.mac_address);
      CRANE_DEBUG(msg);
      success = true;
    } else {
      msg = fmt::format("Failed to wake up craned {} via WoL", craned_id);
      CRANE_ERROR(msg);
    }
    break;

  case CranedState::Running:
    msg = fmt::format("Craned {} is already running with Craned service",
                      craned_id);
    CRANE_DEBUG(msg);
    break;

  case CranedState::PreparingSleep:
    msg = fmt::format("Craned {} is preparing to sleep", craned_id);
    CRANE_DEBUG(msg);
    break;

  case CranedState::PoweringUp:
    msg = fmt::format("Craned {} is in power up process", craned_id);
    CRANE_DEBUG(msg);
    break;

  case CranedState::ShuttingDown:
    msg = fmt::format("Craned {} is shutting down, cannot wake up", craned_id);
    CRANE_DEBUG(msg);
    break;

  case CranedState::Shutdown:
    msg = fmt::format("Craned {} is shut down, use PowerOnNode instead",
                      craned_id);
    CRANE_DEBUG(msg);
    break;

  case CranedState::Unknown:
    msg = fmt::format("Cannot wake up craned {} in unknown state", craned_id);
    CRANE_ERROR(msg);
    break;
  }

  if (error_msg) *error_msg = msg;
  return success;
}

// Here the host needs to turn on UDP ports 7 and 9
bool IPMIManager::SendCranedWakeOnLanPacket_(const std::string& mac_str) {
  std::vector<uint8_t> mac(6);
  if (sscanf(mac_str.c_str(), "%hhx:%hhx:%hhx:%hhx:%hhx:%hhx", &mac[0], &mac[1],
             &mac[2], &mac[3], &mac[4], &mac[5]) != 6) {
    CRANE_ERROR("Invalid MAC address format: {}", mac_str);
    return false;
  }
  CRANE_DEBUG("Sending WoL packet to MAC: {}", mac_str);

  // construct magic packet
  std::vector<uint8_t> packet(102);
  // fill first 6 bytes with 0xFF
  std::fill_n(packet.begin(), 6, 0xFF);
  // repeat MAC address 16 times
  for (int i = 1; i <= 16; ++i) {
    std::copy(mac.begin(), mac.end(), packet.begin() + i * 6);
  }

  int sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
  if (sock < 0) {
    CRANE_ERROR("Failed to create socket for WoL");
    return false;
  }

  // set broadcast option
  int broadcast = 1;
  if (setsockopt(sock, SOL_SOCKET, SO_BROADCAST, &broadcast,
                 sizeof(broadcast)) < 0) {
    CRANE_ERROR("Failed to set broadcast option");
    close(sock);
    return false;
  }

  // set target address (broadcast address)
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(9);  // WoL uses port 9
  addr.sin_addr.s_addr = INADDR_BROADCAST;

  // send magic packet
  ssize_t sent = sendto(sock, packet.data(), packet.size(), 0,
                        (struct sockaddr*)&addr, sizeof(addr));
  close(sock);

  if (sent != packet.size()) {
    CRANE_ERROR("Failed to send WoL packet");
    return false;
  }

  return true;
}

bool IPMIManager::ShutdownCraned(const CranedId& craned_id,
                                 std::string* error_msg) {
  auto craned_meta = g_meta_container->GetCranedMetaPtr(craned_id);
  if (!craned_meta) {
    std::string msg =
        fmt::format("Craned {} not found in craned_states_", craned_id);
    CRANE_ERROR(msg);
    if (error_msg) *error_msg = msg;
    return false;
  }

  std::string msg;
  bool success = false;

  switch (craned_meta->state_info.state) {
  case CranedState::Running:
  case CranedState::Sleeped:
  case CranedState::Shutdown:
    msg = fmt::format("Shutting down craned {}", craned_id);
    CRANE_DEBUG(msg);
    if (ExecuteIPMICommand_(craned_meta, kShutdownCmd)) {
      SetCranedState_(craned_meta, CranedState::ShuttingDown);
      msg = fmt::format("Craned {} is shutting down", craned_id);
      CRANE_DEBUG(msg);
      success = true;
    } else {
      msg = fmt::format("Failed to execute shutdown command for craned {}",
                        craned_id);
      CRANE_ERROR(msg);
    }
    break;

  case CranedState::WakingUp:
    msg = fmt::format("Craned {} is waking up, cannot shutdown", craned_id);
    CRANE_DEBUG(msg);
    break;

  case CranedState::PreparingSleep:
    msg = fmt::format("Craned {} is preparing to sleep, cannot shutdown",
                      craned_id);
    CRANE_DEBUG(msg);
    break;

  case CranedState::PoweringUp:
    msg = fmt::format("Craned {} is powering up, cannot shutdown", craned_id);
    CRANE_DEBUG(msg);
    break;

  case CranedState::ShuttingDown:
    msg = fmt::format("Craned {} is already in shutdown process", craned_id);
    CRANE_DEBUG(msg);
    break;

  case CranedState::Unknown:
    msg = fmt::format("Cannot shutdown craned {} in unknown state", craned_id);
    CRANE_DEBUG(msg);
    break;
  }

  if (error_msg) *error_msg = msg;
  return success;
}

bool IPMIManager::PowerOnCraned(const CranedId& craned_id,
                                std::string* error_msg) {
  auto craned_meta = g_meta_container->GetCranedMetaPtr(craned_id);
  if (!craned_meta) {
    std::string msg =
        fmt::format("Craned {} not found in craned_states_", craned_id);
    CRANE_ERROR(msg);
    if (error_msg) *error_msg = msg;
    return false;
  }

  std::string msg;
  bool success = false;

  switch (craned_meta->state_info.state) {
  case CranedState::Shutdown:
  case CranedState::PoweringUp:
    if (ExecuteIPMICommand_(craned_meta, kPowerOnCmd)) {
      SetCranedState_(craned_meta, CranedState::PoweringUp);
      msg = fmt::format("Craned {} is powering on", craned_id);
      CRANE_DEBUG(msg);
      success = true;
    } else {
      msg = fmt::format("Failed to execute power on command for craned {}",
                        craned_id);
      CRANE_ERROR(msg);
    }
    break;

  case CranedState::Running:
    msg = fmt::format("Craned {} is already running with Craned service",
                      craned_id);
    CRANE_DEBUG(msg);
    break;

  case CranedState::WakingUp:
    msg = fmt::format("Craned {} is in wake up process", craned_id);
    CRANE_DEBUG(msg);
    break;

  case CranedState::PreparingSleep:
    msg = fmt::format("Craned {} is preparing to sleep, cannot power on",
                      craned_id);
    CRANE_DEBUG(msg);
    break;

  case CranedState::Sleeped:
    msg = fmt::format("Craned {} is in sleep state, use WakeupNode instead",
                      craned_id);
    CRANE_DEBUG(msg);
    break;

  case CranedState::ShuttingDown:
    msg = fmt::format("Craned {} is shutting down, cannot power on", craned_id);
    CRANE_DEBUG(msg);
    break;

  case CranedState::Unknown:
    msg = fmt::format("Cannot power on craned {} in unknown state", craned_id);
    CRANE_DEBUG(msg);
    break;
  }

  if (error_msg) *error_msg = msg;
  return success;
}

void IPMIManager::MonitoringThread_() {
  while (!should_stop_) {
    if (!g_meta_container) {
      CRANE_ERROR("Craned meta container not initialized");
      std::this_thread::sleep_for(
          std::chrono::seconds(kMonitoringIntervalSeconds));
      continue;
    }

    if (g_meta_container->GetCranedMetaMapConstPtr()->empty()) {
      CRANE_DEBUG("No craned found, sleep for {}s", kMonitoringIntervalSeconds);
      std::this_thread::sleep_for(
          std::chrono::seconds(kMonitoringIntervalSeconds));
      continue;
    }

    auto craned_meta_map = g_meta_container->GetCranedMetaMapConstPtr();
    std::vector<CranedId> nodes_to_shutdown;

    auto now = absl::Now();

    for (const auto& [craned_id, craned_meta] : *craned_meta_map) {
      UpdateCranedState_(craned_id);

      auto craned_meta_ptr = craned_meta.GetExclusivePtr();
      if (craned_meta_ptr->state_info.state == CranedState::Running) {
        if (craned_meta_ptr->running_task_resource_map.empty()) {
          // TODO: Use the shutdown model to predict the node idle time, and
          // sleep or shutdown when the idle time exceeds the specified
          // threshold
          absl::Time idle_start_time = craned_meta_ptr->last_busy_time;
          if (idle_start_time == absl::Time()) {
            idle_start_time = craned_meta_ptr->state_info.last_state_change;
          }

          if (idle_start_time > now) {
            CRANE_ERROR(
                "Invalid idle start time for craned {}: time is in future",
                craned_id);
            continue;
          }

          auto idle_time = now - idle_start_time;

          if (idle_time < absl::ZeroDuration() ||
              idle_time > absl::Hours(24 * 365)) {
            continue;
          }

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
      ShutdownCraned(craned_id);
    }

    std::this_thread::sleep_for(
        std::chrono::seconds(kMonitoringIntervalSeconds));
  }
}

void IPMIManager::UpdateCranedState_(const CranedId& craned_id) {
  CranedState craned_state;
  absl::Time last_state_change;

  {
    auto craned_meta = g_meta_container->GetCranedMetaPtr(craned_id);
    if (!craned_meta) {
      CRANE_ERROR("Craned {} not found in craned_states_", craned_id);
      return;
    }

    if (craned_meta->state_info.state == CranedState::PreparingSleep) {
      auto now = absl::Now();
      if (now - craned_meta->state_info.last_state_change >
          absl::Seconds(kSleepTimeoutSeconds)) {
        CRANE_DEBUG("Craned {} sleep timeout, transitioning to Sleeped state",
                    craned_id);
        SetCranedState_(craned_meta, CranedState::Sleeped);
        return;
      }
      CRANE_DEBUG(
          "Craned {} is still in PreparingSleep state, sleep timeout not "
          "reached",
          craned_id);
      return;
    }

    auto stub = g_craned_keeper->GetCranedStub(craned_id);
    bool is_connected = (stub && !stub->Invalid());
    PowerState power_status = GetCranedPowerStatus_(craned_meta);

    auto transition = node_state_machine_->NextTransition(
        craned_meta->state_info.state, is_connected, power_status);

    if (transition) {
      if (transition->next_state != craned_meta->state_info.state) {
        CRANE_DEBUG("State transition for Craned {}: {} -> {}", craned_id,
                    CranedStateToStr(craned_meta->state_info.state),
                    CranedStateToStr(transition->next_state));
        SetCranedState_(craned_meta, transition->next_state);
        transition->action(craned_meta, craned_id);
      }
    } else {
      CRANE_ERROR(
          "No valid state transition found for Craned {} in state {}, "
          "connected: {}, power: {}",
          craned_id, CranedStateToStr(craned_meta->state_info.state),
          is_connected, static_cast<int>(power_status));
    }

    craned_state = craned_meta->state_info.state;
    last_state_change = craned_meta->state_info.last_state_change;
  }

  auto now = absl::Now();
  auto CheckTimeout = [&](CranedState state, int timeout_seconds) -> bool {
    return craned_state == state &&
           now - last_state_change > absl::Seconds(timeout_seconds);
  };

  if (CheckTimeout(CranedState::WakingUp, kWakeupTimeoutSeconds)) {
    auto craned_meta = g_meta_container->GetCranedMetaPtr(craned_id);
    if (craned_meta) {
      CRANE_ERROR("Craned {} wake up timeout, attempting to restart service",
                  craned_id);
      if (ExecuteRemoteCommand_(craned_meta, kRestartCranedCmd)) {
        CRANE_DEBUG("Successfully restarted craned service on {}", craned_id);
      } else {
        CRANE_ERROR("Failed to restart craned service on {}", craned_id);
      }
      SetCranedState_(craned_meta, CranedState::WakingUp);
    }
  } else if (CheckTimeout(CranedState::PoweringUp, kPowerUpTimeoutSeconds)) {
    CRANE_ERROR("Craned {} power up timeout", craned_id);
    auto craned_meta = g_meta_container->GetCranedMetaPtr(craned_id);
    if (craned_meta) {
      SetCranedState_(craned_meta, CranedState::PoweringUp);
    }
  } else if (CheckTimeout(CranedState::ShuttingDown, kShutdownTimeoutSeconds)) {
    CRANE_ERROR("Craned {} shutdown timeout", craned_id);
    auto craned_meta = g_meta_container->GetCranedMetaPtr(craned_id);
    if (craned_meta) {
      SetCranedState_(craned_meta, CranedState::Shutdown);
    }
  }
}

bool IPMIManager::ExecuteIPMICommand_(
    const CranedMetaContainer::CranedMetaPtr& craned_meta,
    const std::string& command, std::string* output) {
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

  FILE* pipe = popen(ipmi_cmd.c_str(), "r");
  if (!pipe) {
    CRANE_ERROR("Failed to execute IPMI command for craned {}",
                craned_meta->static_meta.hostname);
    return false;
  }

  char buffer[128];
  std::string result;
  while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
    result += buffer;
  }
  int ret = pclose(pipe);

  if (ret != 0) {
    CRANE_ERROR(
        "Failed to execute IPMI command for craned {}: {} (return code: {})",
        craned_meta->static_meta.hostname, command, ret);
    return false;
  }

  if (output) {
    *output = result;
  }

  CRANE_DEBUG("Successfully executed IPMI power {} command for craned {}: {}",
              command, craned_meta->static_meta.hostname, result);
  return true;
}

PowerState IPMIManager::GetCranedPowerStatus_(
    const CranedMetaContainer::CranedMetaPtr& craned_meta) {
  std::string output;

  if (!ExecuteIPMICommand_(craned_meta, kPowerStatusCmd, &output)) {
    return PowerState::Unknown;
  }

  if (output.find("Chassis Power is on") != std::string::npos) {
    return PowerState::On;
  } else if (output.find("Chassis Power is off") != std::string::npos) {
    return PowerState::Off;
  }

  CRANE_ERROR("Unexpected IPMI power status output for craned {}: {}",
              craned_meta->static_meta.hostname, output);
  return PowerState::Unknown;
}

bool IPMIManager::ExecuteRemoteCommand_(
    const CranedMetaContainer::CranedMetaPtr& craned_meta,
    const std::string& command) {
  const auto& config = craned_meta->static_meta.ssh;

  if (config.ip.empty()) {
    CRANE_ERROR("SSH IP address not configured for craned {}",
                craned_meta->static_meta.hostname);
    return false;
  }

  if (config.username.empty()) {
    CRANE_ERROR("SSH username not configured for craned {}",
                craned_meta->static_meta.hostname);
    return false;
  }

  if (config.password.empty()) {
    CRANE_ERROR("SSH password not configured for craned {}",
                craned_meta->static_meta.hostname);
    return false;
  }

  if (config.port <= 0 || config.port > 65535) {
    CRANE_ERROR("Invalid SSH port {} configured for craned {}", config.port,
                craned_meta->static_meta.hostname);
    return false;
  }

  ssh_session session = ssh_new();
  if (session == nullptr) {
    CRANE_ERROR("Failed to create SSH session for craned {}",
                craned_meta->static_meta.hostname);
    return false;
  }

  try {
    CRANE_DEBUG("Connecting to {}@{}:{}", config.username, config.ip,
                config.port);

    int rc;
    rc = ssh_options_set(session, SSH_OPTIONS_HOST, config.ip.c_str());
    if (rc != SSH_OK) {
      CRANE_ERROR("Failed to set SSH host for craned {}: {}",
                  craned_meta->static_meta.hostname, ssh_get_error(session));
      ssh_free(session);
      return false;
    }

    rc = ssh_options_set(session, SSH_OPTIONS_USER, config.username.c_str());
    if (rc != SSH_OK) {
      CRANE_ERROR("Failed to set SSH username for craned {}: {}",
                  craned_meta->static_meta.hostname, ssh_get_error(session));
      ssh_free(session);
      return false;
    }

    rc = ssh_options_set(session, SSH_OPTIONS_PORT, &config.port);
    if (rc != SSH_OK) {
      CRANE_ERROR("Failed to set SSH port for craned {}: {}",
                  craned_meta->static_meta.hostname, ssh_get_error(session));
      ssh_free(session);
      return false;
    }

    long timeout = 10 * 1000000;
    rc = ssh_options_set(session, SSH_OPTIONS_TIMEOUT_USEC, &timeout);
    if (rc != SSH_OK) {
      CRANE_ERROR("Failed to set SSH timeout for craned {}: {}",
                  craned_meta->static_meta.hostname, ssh_get_error(session));
      ssh_free(session);
      return false;
    }

    rc = ssh_connect(session);
    if (rc != SSH_OK) {
      CRANE_ERROR("Failed to connect to craned {} ({}:{}): {}",
                  craned_meta->static_meta.hostname, config.ip, config.port,
                  ssh_get_error(session));
      ssh_free(session);
      return false;
    }

    rc = ssh_userauth_password(session, nullptr, config.password.c_str());
    if (rc != SSH_AUTH_SUCCESS) {
      CRANE_ERROR("Failed to authenticate for craned {}: {}",
                  craned_meta->static_meta.hostname, ssh_get_error(session));
      ssh_disconnect(session);
      ssh_free(session);
      return false;
    }

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
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

#pragma once

#include <libssh/libssh.h>

#include <atomic>
#include <functional>
#include <string>
#include <thread>
#include <unordered_map>

#include "CranedMetaContainer.h"
#include "CtldPublicDefs.h"
#include "crane/PublicHeader.h"
namespace Ctld {

enum class PowerState { On, Off, Unknown };

inline std::string PowerStateToStr(PowerState power_state) {
  switch (power_state) {
  case PowerState::On:
    return "On";
  case PowerState::Off:
    return "Off";
  case PowerState::Unknown:
    return "Unknown";
  }
  return "Unknown";
}

class CranedStateMachine {
 public:
  struct StateTransition {
    CranedState current_state;
    bool is_connected;
    PowerState power_status;
    CranedState next_state;
    std::function<void(CranedMetaContainer::CranedMetaPtr&, const CranedId&)>
        action;
  };

  struct TransitionKey {
    CranedState current_state;
    bool is_connected;
    PowerState power_status;

    bool operator==(const TransitionKey& other) const {
      return current_state == other.current_state &&
             is_connected == other.is_connected &&
             power_status == other.power_status;
    }
  };

  struct TransitionKeyHash {
    std::size_t operator()(const TransitionKey& key) const {
      return std::hash<int>()(static_cast<int>(key.current_state)) ^
             (std::hash<bool>()(key.is_connected) << 1) ^
             (std::hash<int>()(static_cast<int>(key.power_status)) << 2);
    }
  };

  CranedStateMachine();

  std::optional<StateTransition> NextTransition(CranedState current_state,
                                                bool is_connected,
                                                PowerState power_status) const;

 private:
  std::unordered_map<TransitionKey, StateTransition, TransitionKeyHash>
      transition_map_;
};

class IPMIManager {
 public:
  IPMIManager() : node_state_machine_(std::make_unique<CranedStateMachine>()) {
    StartMonitoring_();
  }

  ~IPMIManager() { StopMonitoring_(); }

  bool ShutdownCraned(const CranedId& craned_id,
                      std::string* error_msg = nullptr);
  bool PowerOnCraned(const CranedId& craned_id,
                     std::string* error_msg = nullptr);

  bool SleepCraned(const CranedId& craned_id, std::string* error_msg = nullptr);
  bool WakeupCraned(const CranedId& craned_id,
                    std::string* error_msg = nullptr);

 private:
  void StartMonitoring_();
  void StopMonitoring_();

  void MonitoringThread_();
  void UpdateCranedState_(const CranedId& craned_id);

  void SetCranedState_(const CranedMetaContainer::CranedMetaPtr& craned_meta,
                       CranedState state);
  void SetCranedInfo_(const CranedMetaContainer::CranedMetaPtr& craned_meta);

  PowerState GetCranedPowerStatus_(
      const CranedMetaContainer::CranedMetaPtr& craned_meta);

  bool SendCranedWakeOnLanPacket_(const std::string& mac_str);

  bool ExecuteIPMICommand_(
      const CranedMetaContainer::CranedMetaPtr& craned_meta,
      const std::string& command, std::string* output = nullptr);
  bool ExecuteRemoteCommand_(
      const CranedMetaContainer::CranedMetaPtr& craned_meta,
      const std::string& command);

 private:
  std::thread monitoring_thread_;
  std::atomic<bool> should_stop_{false};

  std::unique_ptr<CranedStateMachine> node_state_machine_;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::IPMIManager> g_ipmi_manager;
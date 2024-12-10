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
#include <string>
#include <thread>

#include "CranedMetaContainer.h"
#include "CtldPublicDefs.h"
#include "crane/PublicHeader.h"
namespace Ctld {

enum class PowerState { On, Off, Unknown };

class IPMIManager {
 public:
  // IPMIManager(const IPMIManager&) = delete;
  // IPMIManager& operator=(const IPMIManager&) = delete;

  // static IPMIManager& GetInstance() {
  //   static IPMIManager instance;
  //   return instance;
  // }

  IPMIManager() { StartMonitoring_(); }

  ~IPMIManager() { StopMonitoring_(); }

  bool SleepCraned(const CranedId& craned_id);
  bool WakeupCraned(const CranedId& craned_id);
  bool ShutdownCraned(const CranedId& craned_id);
  bool PowerOnCraned(const CranedId& craned_id);

 private:

  void SetCranedState_(const CranedMetaContainer::CranedMetaPtr& craned_meta,
                       CranedState state);

  void StartMonitoring_();
  void StopMonitoring_();

  void MonitoringThread_();
  void UpdateNodeState_(const CranedId& craned_id);

  PowerState GetPowerStatus_(const CranedId& craned_id, const Config::BMC& bmc);

  bool ExecuteIPMICommand_(
      const CranedMetaContainer::CranedMetaPtr& craned_meta,
      const std::string& command);

  bool ExecuteRemoteCommand_(
      const CranedMetaContainer::CranedMetaPtr& craned_meta,
      const std::string& command);

  std::thread monitoring_thread_;
  std::atomic<bool> should_stop_{false};
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::IPMIManager> g_ipmi_manager;
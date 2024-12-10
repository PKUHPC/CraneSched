#pragma once

#include <string>
#include <thread>
#include <atomic>
#include <unordered_map>
#include <mutex>
#include <libssh/libssh.h>

#include "crane/PublicHeader.h"
#include "absl/time/time.h"

namespace Ctld {
enum class NodeState {
  CranedRunning,
  Sleeped,
  Shutdown,
  WakingUp,
  PoweringUp,
  ShuttingDown,
  Unknown
};

enum class PowerState {
  On,
  Off,
  Unknown
};

struct BMCConfig {
  std::string interface;
  std::string ip;
  uint32_t port;
  std::string username;
  std::string password;
};
struct SSHConfig {
  std::string host;
  std::string username;
  std::string password;
  int port{22};
};
class IPMIManager {
 public:
  IPMIManager() {
    StartMonitoring();
  }

  ~IPMIManager() {
    StopMonitoring();
  }

  bool SleepNode(const CranedId& craned_id); 
  bool WakeupNode(const CranedId& craned_id);
  bool ShutdownNode(const CranedId& craned_id);
  bool PowerOnNode(const CranedId& craned_id);

  NodeState GetNodeState(const CranedId& craned_id) const;
 private:
  struct NodeStateInfo {
    NodeState state{NodeState::Unknown};
    absl::Time last_state_change;
  };

  void StartMonitoring();
  void StopMonitoring();

  void MonitoringThread_();
  void UpdateNodeState_(const CranedId& craned_id);

  PowerState GetPowerStatus_(const CranedId& craned_id);
  bool ExecuteIPMICommand_(const CranedId& craned_id, const std::string& command);
  BMCConfig GetBMCConfig_(const CranedId& craned_id) const;

  bool StartCranedService_(const CranedId& craned_id);
  bool ExecuteRemoteCommand_(const CranedId& craned_id, const std::string& command);
  SSHConfig GetSSHConfig_(const CranedId& craned_id) const;

  std::thread monitoring_thread_;
  std::atomic<bool> should_stop_{false};
  
  mutable std::mutex state_mutex_;
  std::unordered_map<CranedId, NodeStateInfo> node_states_;
};

} // namespace Ctld 
/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneCtld is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include "TaskScheduler.h"
#include "CtldPublicDefs.h"
#include "CranedMetaContainer.h"
#include "InfluxDBClient.h"

namespace Ctld {

class EnergyAwarePriority : public IPrioritySorter {
 public:
  explicit EnergyAwarePriority(const std::string& db_url = "http://localhost:8086", const std::string& token = "", const std::string& org = "crane  ")
      : m_influx_client(db_url, token, org) {}

  std::vector<task_id_t> GetOrderedTaskIdList(
      const OrderedTaskMap& pending_task_map, 
      const UnorderedTaskMap& running_task_map, 
        size_t limit) override;

 private:
  double CalculateTaskEnergyScore_(TaskInCtld* task) const;
  double GetHistoricalTaskEnergyEfficiency_(const TaskInCtld* task) const;

  InfluxDBClient m_influx_client;
};

class EnergyAwareNodeSelect : public INodeSelectionAlgo {
 public:
  enum class NodePowerState {
    Running,
    Sleeping,
    WakingUp,
    GoingToSleep,
    Unknown
  };

  struct NodeEnergyInfo {
    struct Stats {
      double current_power{0.0};    
      double avg_power{0.0};        
      double total_energy{0.0};     
      double cpu_util{0.0};         
      double mem_util{0.0};         
      double gpu_util{0.0};         
      absl::Duration time_span;     
    };
    
    Stats recent_stats;             
    Stats historical_stats;         
    size_t task_count{0};          
    NodePowerState power_state{NodePowerState::Unknown};
    absl::Time last_task_end_time;
    absl::Time last_state_change;
  };

  struct Config {
    absl::Duration recent_stats_window;
    absl::Duration historical_stats_window;
    absl::Duration idle_sleep_threshold;
    double power_sleep_threshold;
    double weight_current_power;
    double weight_historical_power;
    double weight_task_count;
    std::string db_url;
    std::string token;
    std::string org;
    Config()
        : recent_stats_window(absl::Minutes(5)),
          historical_stats_window(absl::Hours(1)),
          idle_sleep_threshold(absl::Minutes(5)),
          power_sleep_threshold(50.0),
          weight_current_power(0.4),
          weight_historical_power(0.3),
          weight_task_count(0.3),
          db_url("http://localhost:8086"),
          token("") {}
  };

  explicit EnergyAwareNodeSelect(
      IPrioritySorter* priority_sorter, 
      const Config& config = Config{})
      : m_priority_sorter_(priority_sorter), 
        m_config(config),
        m_influx_client(config.db_url, config.token, config.org) {}

  ~EnergyAwareNodeSelect() override = default;

  void NodeSelect(
      const absl::flat_hash_map<task_id_t, std::unique_ptr<TaskInCtld>>& running_tasks,
      absl::btree_map<task_id_t, std::unique_ptr<TaskInCtld>>* pending_task_map,
      std::list<NodeSelectionResult>* selection_result_list) override;

 private:
  static constexpr bool kAlgoTraceOutput = false;

  void UpdateNodeEnergyInfo_(
      const std::unordered_set<CranedId>& craned_ids,
      std::unordered_map<CranedId, NodeEnergyInfo>* energy_info);

  bool UpdateSingleNodeEnergyInfo_(
      const CranedId& node_id,
      NodeEnergyInfo* info);

  std::list<CranedId> SelectBestNodes_(
      TaskInCtld* task,
      const std::unordered_map<CranedId, NodeEnergyInfo>& energy_info);

  double CalculateNodeScore_(const NodeEnergyInfo& info) const;

  bool ShouldSleepNode_(
      const CranedId& craned_id,
      const NodeEnergyInfo& energy_info) const;

  bool SleepNode_(const CranedId& craned_id);

  bool WakeupNode_(const CranedId& craned_id);

  bool ExecuteIpmiPowerCommand_(const CranedId& craned_id, const std::string& command) const;
  
  std::string GetNodeIpmiStatus_(const CranedId& craned_id) const;

  IPrioritySorter* m_priority_sorter_;
  Config m_config;
  InfluxDBClient m_influx_client;
};

}  // namespace Ctld 
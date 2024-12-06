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
#include "influxdb.hpp"

namespace Ctld {

class EnergyAwarePriority : public IPrioritySorter {
 public:
  explicit EnergyAwarePriority(const std::string& db_url = "http://localhost:8086")
      : m_server_info("localhost", 8086, "crane") {
  }
  
  std::vector<task_id_t> GetOrderedTaskIdList(
      const OrderedTaskMap& pending_task_map,
      const UnorderedTaskMap& running_task_map, 
      size_t limit) override;

 private:
  // 计算任务的能耗优先级分数
  double CalculateTaskEnergyScore_(TaskInCtld* task) const;
  
  // 从InfluxDB获取任务的历史能耗数据
  double GetHistoricalTaskEnergyEfficiency_(const TaskInCtld* task) const;
  
  influxdb_cpp::server_info m_server_info;
};

class EnergyAwareNodeSelect : public INodeSelectionAlgo {
 public:
  enum class NodePowerState {
    Running,      // 正常运行
    Sleeping,     // 休眠中
    WakingUp,     // 正在唤醒
    GoingToSleep, // 正在进入休眠
    Unknown       // 未知状态
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
    absl::Time last_state_change;   // 记录最后一次状态变更时间
  };

  struct Config {
    // 能耗统计配置
    absl::Duration recent_stats_window;    // 最近统计窗口
    absl::Duration historical_stats_window;  // 历史统计窗口
    
    // 节点休眠配置
    absl::Duration idle_sleep_threshold;   // 空闲休眠阈值
    double power_sleep_threshold;          // 功率休眠阈值(W)
    
    // 节点评分权重
    double weight_current_power;     // 当前功率权重
    double weight_historical_power;   // 历史功率权重
    double weight_task_count;        // 任务数量权重

    // InfluxDB配置
    std::string db_url;                  // InfluxDB URL

    // 默认构造函数
    Config()
        : recent_stats_window(absl::Minutes(5)),
          historical_stats_window(absl::Hours(1)),
          idle_sleep_threshold(absl::Minutes(5)),
          power_sleep_threshold(50.0),
          weight_current_power(0.4),
          weight_historical_power(0.3),
          weight_task_count(0.3),
          db_url("http://localhost:8086") {}
  };

  explicit EnergyAwareNodeSelect(
      IPrioritySorter* priority_sorter, 
      const Config& config = Config{})
      : m_priority_sorter_(priority_sorter), 
        m_config(config),
        m_server_info("localhost", 8086, "crane") {
  }

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
  
  // 获取节点的IPMI电源状态
  std::string GetNodeIpmiStatus_(const CranedId& craned_id) const;

  IPrioritySorter* m_priority_sorter_;
  Config m_config;
  influxdb_cpp::server_info m_server_info;
};

}  // namespace Ctld 
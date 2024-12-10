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
#include "InfluxDBClient.h"
#include "IPMIManager.h"

namespace Ctld {
struct NodeEnergyInfo {
  NodeStats recent_stats;             
  NodeStats historical_stats;         
  size_t task_count{0};          
  NodeState state;
};

class EnergyAwarePriority : public IPrioritySorter {
 public:
  EnergyAwarePriority(){}

  std::vector<task_id_t> GetOrderedTaskIdList(
      const OrderedTaskMap& pending_task_map, 
      const UnorderedTaskMap& running_task_map, 
        size_t limit) override;

 private:
  double CalculateTaskEnergyScore_(TaskInCtld* task) const;
  double GetHistoricalTaskEnergyEfficiency_(const TaskInCtld* task) const;

  InfluxDBClient m_influx_client_;
};

struct EnergyAwareConfig {
  absl::Duration recent_stats_window{absl::Minutes(5)};
  absl::Duration historical_stats_window{absl::Hours(1)};
  double weight_current_power{0.4};
  double weight_historical_power{0.3};
  double weight_task_count{0.3};
};

class EnergyAwareNodeSelect : public INodeSelectionAlgo {
 public:
  explicit EnergyAwareNodeSelect(
      IPrioritySorter* priority_sorter, 
      const EnergyAwareConfig& config = EnergyAwareConfig{})
      : m_priority_sorter_(priority_sorter), 
        m_config_(config) {}


  ~EnergyAwareNodeSelect() override = default;

  void NodeSelect(
      const absl::flat_hash_map<task_id_t, std::unique_ptr<TaskInCtld>>& running_tasks,
      absl::btree_map<task_id_t, std::unique_ptr<TaskInCtld>>* pending_task_map,
      std::list<NodeSelectionResult>* selection_result_list) override;

 private:
  void UpdateNodeEnergyInfo_(
      const std::unordered_set<CranedId>& craned_ids,
      std::unordered_map<CranedId, NodeEnergyInfo>* energy_info);

  bool UpdateSingleNodeEnergyInfo_(
      const CranedId& node_id,
      NodeEnergyInfo* info);

  std::list<CranedId> SelectBestNodes_(
      TaskInCtld* task,
      const std::unordered_map<CranedId, NodeEnergyInfo>& energy_info);

  void SelectNodesFromCandidates_(
      const std::vector<std::pair<double, CranedId>>& candidates,
      const std::string& node_type,
      TaskInCtld* task,
      const CranedMetaContainer::CranedMetaMapConstPtr& craned_meta,
      std::list<CranedId>* selected_nodes,
      ResourceV2* allocated_res);

  double CalculateNodeScore_(const NodeEnergyInfo& info) const;

  IPrioritySorter* m_priority_sorter_;
  
  EnergyAwareConfig m_config_;
  InfluxDBClient m_influx_client_;
  IPMIManager m_ipmi_manager_;
};

}  // namespace Ctld 
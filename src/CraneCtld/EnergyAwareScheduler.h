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

#include "CtldPublicDefs.h"
#include "InfluxDBClient.h"
#include "TaskScheduler.h"
namespace Ctld {
struct NodeEnergyInfo {
  NodeStats recent_stats;
  NodeStats historical_stats;
};

class EnergyAwarePriority : public IPrioritySorter {
 public:
  EnergyAwarePriority() {}

  std::vector<task_id_t> GetOrderedTaskIdList(
      const OrderedTaskMap& pending_task_map,
      const UnorderedTaskMap& running_task_map, size_t limit) override;

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
      : m_priority_sorter_(priority_sorter), m_config_(config) {}

  ~EnergyAwareNodeSelect() override = default;

  void NodeSelect(
      const absl::flat_hash_map<task_id_t, std::unique_ptr<TaskInCtld>>&
          running_tasks,
      absl::btree_map<task_id_t, std::unique_ptr<TaskInCtld>>* pending_task_map,
      std::list<NodeSelectionResult>* selection_result_list) override;

 private:
  void UpdateNodeEnergyInfo_(
      const std::unordered_set<CranedId>& craned_ids,
      std::unordered_map<CranedId, NodeEnergyInfo>* energy_info);

  bool UpdateSingleNodeEnergyInfo_(const CranedId& node_id,
                                   NodeEnergyInfo* info);

  std::list<CranedId> SelectBestNodes_(
      TaskInCtld* task,
      const std::unordered_map<CranedId, NodeEnergyInfo>& energy_info);

  void SelectNodesFromCandidates_(
      const std::vector<std::pair<double, CranedId>>& candidates,
      CranedState node_state, TaskInCtld* task,
      const CranedMetaContainer::CranedMetaMapConstPtr& craned_meta,
      std::list<CranedId>* selected_nodes, ResourceV2* allocated_res);

  double CalculateNodeScore_(const NodeEnergyInfo& info) const;

  IPrioritySorter* m_priority_sorter_;

  EnergyAwareConfig m_config_;
  InfluxDBClient m_influx_client_;
};

}  // namespace Ctld
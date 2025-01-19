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

#include "EnergyAwareScheduler.h"

#include <bsoncxx/json.hpp>

#include "IPMIManager.h"
#include "crane/Logger.h"

namespace Ctld {

void EnergyAwareScheduler::NodeSelect(
    const absl::flat_hash_map<task_id_t, std::unique_ptr<TaskInCtld>>&
        running_tasks,
    absl::btree_map<task_id_t, std::unique_ptr<TaskInCtld>>* pending_task_map,
    std::list<NodeSelectionResult>* selection_result_list) {
  CRANE_DEBUG("Starting node selection for {} pending tasks",
              pending_task_map->size());

  std::vector<task_id_t> ordered_task_ids =
      m_priority_sorter_->GetOrderedTaskIdList(*pending_task_map, running_tasks,
                                               g_config.ScheduledBatchSize);

  CRANE_DEBUG("Got {} ordered tasks for processing", ordered_task_ids.size());

  auto craned_meta_map = g_meta_container->GetCranedMetaMapConstPtr();
  std::unordered_set<CranedId> all_craned_ids;
  for (const auto& [craned_id, _] : *craned_meta_map) {
    all_craned_ids.insert(craned_id);
  }

  CRANE_DEBUG("Found {} total nodes in system", all_craned_ids.size());

  std::unordered_map<CranedId, NodeStatsInfo> energy_info;
  UpdateNodeStatsInfo_(all_craned_ids, &energy_info);

  for (task_id_t task_id : ordered_task_ids) {
    auto task_it = pending_task_map->find(task_id);
    if (task_it == pending_task_map->end()) {
      continue;
    }

    TaskInCtld* task = task_it->second.get();
    std::list<CranedId> selected_nodes = SelectBestNodes_(task, energy_info);

    if (!selected_nodes.empty()) {
      bool all_nodes_ready = true;
      std::vector<CranedId> nodes_to_wake;
      std::vector<CranedId> nodes_to_power_on;

      for (const CranedId& craned_id : selected_nodes) {
        auto node_meta = craned_meta_map->at(craned_id).GetExclusivePtr();

        switch (node_meta->state_info.state) {
        case CranedState::Running:
          CRANE_DEBUG("Node {} is running", craned_id);
          break;

        case CranedState::Sleeped:
          CRANE_DEBUG("Node {} is sleeping, will wake up for Task #{}",
                      craned_id, task->TaskId());
          nodes_to_wake.push_back(craned_id);
          all_nodes_ready = false;
          break;

        case CranedState::Shutdown:
          CRANE_DEBUG("Node {} is shut down, will power on for Task #{}",
                      craned_id, task->TaskId());
          nodes_to_power_on.push_back(craned_id);
          all_nodes_ready = false;
          break;

        case CranedState::WakingUp:
        case CranedState::PreparingSleep:
        case CranedState::PoweringUp:
        case CranedState::ShuttingDown:
          CRANE_DEBUG("Node {} is in transition state ({})", craned_id,
                      CranedStateToStr(node_meta->state_info.state));
          all_nodes_ready = false;
          break;

        case CranedState::Unknown:
          CRANE_ERROR("Node {} is in unknown state", craned_id);
          all_nodes_ready = false;
          break;
        }
      }

      if (!nodes_to_wake.empty() || !nodes_to_power_on.empty()) {
        for (const CranedId& craned_id : nodes_to_wake) {
          if (g_ipmi_manager->WakeupCraned(craned_id)) {
            CRANE_DEBUG("Wake up command sent to node {}", craned_id);
          } else {
            CRANE_ERROR("Failed to send wake up command to node {}", craned_id);
          }
        }

        for (const CranedId& craned_id : nodes_to_power_on) {
          if (g_ipmi_manager->PowerOnCraned(craned_id)) {
            CRANE_DEBUG("Power on command sent to node {}", craned_id);
          } else {
            CRANE_ERROR("Failed to send power on command to node {}",
                        craned_id);
          }
        }
        continue;
      }

      if (all_nodes_ready) {
        absl::Time now = absl::Now();
        task->SetStartTime(now);
        task->SetEndTime(now + task->time_limit);

        for (const CranedId& node_id : selected_nodes) {
          g_meta_container->MallocResourceFromNode(node_id, task->TaskId(),
                                                   task->Resources());
        }

        std::unique_ptr<TaskInCtld> moved_task;
        moved_task.swap(task_it->second);
        selection_result_list->emplace_back(
            std::make_pair(std::move(moved_task), selected_nodes));
        pending_task_map->erase(task_it);

        CRANE_DEBUG("Successfully allocated Task #{} to {} nodes",
                    task->TaskId(), selected_nodes.size());
      }
    } else {
      CRANE_DEBUG("No suitable nodes found for Task #{}", task->TaskId());
    }
  }
}

void EnergyAwareScheduler::UpdateNodeStatsInfo_(
    const std::unordered_set<CranedId>& craned_ids,
    std::unordered_map<CranedId, NodeStatsInfo>* energy_info) {
  CRANE_DEBUG("Updating energy info for {} nodes", craned_ids.size());

  try {
    for (const CranedId& node_id : craned_ids) {
      NodeStatsInfo& info = (*energy_info)[node_id];
      UpdateSingleNodeStatsInfo_(node_id, &info);
    }
  } catch (const std::exception& e) {
    CRANE_ERROR("Failed to update node energy info: {}", e.what());
  }
}

bool EnergyAwareScheduler::UpdateSingleNodeStatsInfo_(const CranedId& node_id,
                                                        NodeStatsInfo* info) {
  CRANE_DEBUG("Updating energy info for node {}", node_id);

  bool recent_ok = m_influx_client_.QueryNodeEnergyInfo(
      node_id, absl::ToInt64Seconds(m_config_.recent_stats_window),
      &info->recent_stats);

  bool history_ok = m_influx_client_.QueryNodeEnergyInfo(
      node_id, absl::ToInt64Seconds(m_config_.historical_stats_window),
      &info->historical_stats);

  return recent_ok && history_ok;
}

// TODO: Training the scoring model
double EnergyAwareScheduler::CalculateNodeScore_(
    const NodeStatsInfo& info) const {
  double recent_score = 1.0 - (info.recent_stats.total_energy / 300.0);
  double history_score = 1.0 - (info.historical_stats.total_energy / 300.0);

  double final_score = m_config_.weight_current * recent_score +
                       m_config_.weight_historical * history_score;

  CRANE_DEBUG(
      "Score components - Recent: {:.3f}, History: {:.3f}, Final: {:.3f}",
      recent_score, history_score, final_score);

  return final_score;
}

void EnergyAwareScheduler::SelectNodesFromCandidates_(
    const std::vector<std::pair<double, CranedId>>& candidates,
    CranedState node_state, TaskInCtld* task,
    const CranedMetaContainer::CranedMetaMapConstPtr& craned_meta,
    std::list<CranedId>* selected_nodes, ResourceV2* allocated_res) {
  for (const auto& [score, node_id] : candidates) {
    if (selected_nodes->size() >= task->node_num) break;

    auto node_meta = craned_meta->at(node_id).GetExclusivePtr();
    ResourceInNode feasible_res;

    bool ok;
    switch (node_state) {
    case CranedState::Running:
      ok = task->requested_node_res_view.GetFeasibleResourceInNode(
          node_meta->res_total, &feasible_res);
      break;

    case CranedState::Shutdown:
    case CranedState::Sleeped:
      ok = task->requested_node_res_view.GetFeasibleResourceInNode(
          node_meta->static_meta.res, &feasible_res);
      break;

    default:
      CRANE_DEBUG("Node {} is in unexpected state {} for task #{}", node_id,
                  CranedStateToStr(node_state), task->TaskId());
      continue;
    }

    if (!ok) {
      CRANE_DEBUG(
          "Failed to get feasible resource from node {} (state: {}) for task "
          "#{}",
          node_id, CranedStateToStr(node_state), task->TaskId());
      continue;
    }

    selected_nodes->push_back(node_id);
    allocated_res->AddResourceInNode(node_id, feasible_res);
    CRANE_DEBUG("Selected node {} (state: {}, score: {:.3f}) for Task #{}",
                node_id, CranedStateToStr(node_state), score, task->TaskId());
  }
}

std::list<CranedId> EnergyAwareScheduler::SelectBestNodes_(
    TaskInCtld* task,
    const std::unordered_map<CranedId, NodeStatsInfo>& stats_info) {
  CRANE_DEBUG("Selecting nodes for Task #{}, requires {} nodes", task->TaskId(),
              task->node_num);

  auto craned_meta_map = g_meta_container->GetCranedMetaMapConstPtr();

  std::vector<std::pair<double, CranedId>> running_candidates;
  std::vector<std::pair<double, CranedId>> sleeped_candidates;
  std::vector<std::pair<double, CranedId>> shutdown_candidates;
  int waking_up_candidates_count = 0;
  int preparing_sleep_candidates_count = 0;
  int powering_up_candidates_count = 0;
  int shutting_down_candidates_count = 0;

  for (const auto& [craned_id, info] : stats_info) {
    auto node_meta = craned_meta_map->at(craned_id).GetExclusivePtr();

    if (!(task->requested_node_res_view <= node_meta->static_meta.res)) {
      CRANE_TRACE("Node {} has insufficient resources for task #{}", craned_id,
                  task->TaskId());
      continue;
    }

    if (!task->included_nodes.empty() &&
        !task->included_nodes.contains(craned_id)) {
      CRANE_DEBUG("Node {} not in included nodes list for task #{}", craned_id,
                  task->TaskId());
      continue;
    }

    if (!task->excluded_nodes.empty() &&
        task->excluded_nodes.contains(craned_id)) {
      CRANE_DEBUG("Node {} in excluded nodes list for task #{}", craned_id,
                  task->TaskId());
      continue;
    }

    double score = CalculateNodeScore_(info);

    switch (node_meta->state_info.state) {
    case CranedState::Running:
      running_candidates.emplace_back(score, craned_id);
      CRANE_DEBUG("Added running node {} to active candidates, score: {:.3f}",
                  craned_id, score);
      break;
    case CranedState::Sleeped:
      sleeped_candidates.emplace_back(score, craned_id);
      CRANE_DEBUG(
          "Added sleeping node {} to sleeping candidates, score: {:.3f}",
          craned_id, score);
      break;
    case CranedState::Shutdown:
      shutdown_candidates.emplace_back(score, craned_id);
      CRANE_DEBUG(
          "Added shutdown node {} to shutdown candidates, score: {:.3f}",
          craned_id, score);
      break;
    case CranedState::PreparingSleep:
      preparing_sleep_candidates_count++;
      CRANE_DEBUG("Preparing sleep node {}, score: {:.3f}", craned_id, score);
      break;
    case CranedState::WakingUp:
      waking_up_candidates_count++;
      CRANE_DEBUG("Waking up node {}, score: {:.3f}", craned_id, score);
      break;
    case CranedState::PoweringUp:
      powering_up_candidates_count++;
      CRANE_DEBUG("Powering up node {}, score: {:.3f}", craned_id, score);
      break;
    case CranedState::ShuttingDown:
      shutting_down_candidates_count++;
      CRANE_DEBUG("Shutting down node {}, score: {:.3f}", craned_id, score);
      break;
    case CranedState::Unknown:
      CRANE_WARN("Node {} in unknown state, skipping", craned_id);
      break;
    }
  }

  std::sort(running_candidates.begin(), running_candidates.end(),
            std::greater<std::pair<double, CranedId>>());
  std::sort(sleeped_candidates.begin(), sleeped_candidates.end(),
            std::greater<std::pair<double, CranedId>>());
  std::sort(shutdown_candidates.begin(), shutdown_candidates.end(),
            std::greater<std::pair<double, CranedId>>());

  std::list<CranedId> selected_nodes;
  ResourceV2 allocated_res;

  CRANE_DEBUG(
      "Found {} running, {} sleeped, {} shutdown, {} waking, {} powering, {} "
      "shutting candidates for Task #{}",
      running_candidates.size(), sleeped_candidates.size(),
      shutdown_candidates.size(), waking_up_candidates_count, powering_up_candidates_count,
      shutting_down_candidates_count, task->TaskId());

  if (!running_candidates.empty()) {
    CRANE_DEBUG("Selecting nodes from active candidates for task #{}",
                task->TaskId());
    SelectNodesFromCandidates_(running_candidates, CranedState::Running, task,
                               craned_meta_map, &selected_nodes,
                               &allocated_res);
  }

  if (selected_nodes.size() < task->node_num && !sleeped_candidates.empty()) {
    CRANE_DEBUG(
        "Active nodes insufficient ({}/{}), considering sleeping nodes for "
        "task #{}",
        selected_nodes.size(), task->node_num, task->TaskId());

    SelectNodesFromCandidates_(sleeped_candidates, CranedState::Sleeped, task,
                               craned_meta_map, &selected_nodes,
                               &allocated_res);
  }

  if (selected_nodes.size() < task->node_num && !shutdown_candidates.empty()) {
    CRANE_DEBUG(
        "Sleeping nodes insufficient ({}/{}), considering shutdown nodes for "
        "task #{}",
        selected_nodes.size(), task->node_num, task->TaskId());

    SelectNodesFromCandidates_(shutdown_candidates, CranedState::Shutdown, task,
                               craned_meta_map, &selected_nodes,
                               &allocated_res);
  }

  if (selected_nodes.size() == task->node_num) {
    task->SetResources(std::move(allocated_res));
    return selected_nodes;
  }

  CRANE_DEBUG("Failed to select enough nodes for task #{} ({}/{})",
              task->TaskId(), selected_nodes.size(), task->node_num);
  return {};
}

}  // namespace Ctld
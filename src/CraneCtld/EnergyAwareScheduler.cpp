#include "EnergyAwareScheduler.h"

#include "IPMIManager.h"
#include "crane/Logger.h"
#include <bsoncxx/json.hpp>

namespace Ctld {

std::vector<task_id_t> EnergyAwarePriority::GetOrderedTaskIdList(
    const OrderedTaskMap& pending_task_map,
    const UnorderedTaskMap& running_task_map, 
    size_t limit) {
  
  CRANE_DEBUG("Starting task prioritization for {} pending tasks, limit={}", 
              pending_task_map.size(), limit);
  
  std::vector<std::pair<TaskInCtld*, double>> task_scores;
  task_scores.reserve(pending_task_map.size());

  for (const auto& [task_id, task] : pending_task_map) {
    if (task->Held()) {
      task->pending_reason = "Held";
      CRANE_DEBUG("Task #{} is held, skipping", task_id);
      continue;
    }

    double score = CalculateTaskEnergyScore_(task.get());
    task->cached_priority = score;
    task->pending_reason = "Priority";
    task_scores.emplace_back(task.get(), score);
    
    CRANE_DEBUG("Task #{} received score: {:.6f}", task_id, score);
  }

  std::sort(task_scores.begin(), task_scores.end(),
            [](const auto& a, const auto& b) {
              return a.second > b.second;
            });

  size_t result_size = std::min(limit, task_scores.size());
  std::vector<task_id_t> result;
  result.reserve(result_size);

  CRANE_DEBUG("Selected top {} tasks from {} candidates", result_size, task_scores.size());

  for (size_t i = 0; i < result_size; i++) {
    result.push_back(task_scores[i].first->TaskId());
    CRANE_DEBUG("Position {}: Task #{} (score: {:.6f})", 
                i + 1, task_scores[i].first->TaskId(), task_scores[i].second);
  }

  return result;
}

double EnergyAwarePriority::CalculateTaskEnergyScore_(TaskInCtld* task) const {
  CRANE_DEBUG("Calculating energy score for Task #{}", task->TaskId());
  
  double historical_efficiency = GetHistoricalTaskEnergyEfficiency_(task);
  double resource_demand = task->requested_node_res_view.CpuCount() + 
                         task->requested_node_res_view.MemoryBytes() / (1024.0 * 1024.0 * 1024.0);
  
  double score = historical_efficiency / resource_demand;
  
  CRANE_DEBUG("Task #{}: efficiency={:.6f}, resource_demand={:.2f}, final_score={:.6f}",
              task->TaskId(), historical_efficiency, resource_demand, score);
              
  return score;
}

double EnergyAwarePriority::GetHistoricalTaskEnergyEfficiency_(
    const TaskInCtld* task) const {
  double requested_cpu = task->requested_node_res_view.CpuCount();
  double requested_mem_gb = task->requested_node_res_view.MemoryBytes() / (1024.0 * 1024.0 * 1024.0);
    
  return m_influx_client.QueryTaskEnergyEfficiency(requested_cpu, requested_mem_gb);
}

void EnergyAwareNodeSelect::NodeSelect(
    const absl::flat_hash_map<task_id_t, std::unique_ptr<TaskInCtld>>& running_tasks,
    absl::btree_map<task_id_t, std::unique_ptr<TaskInCtld>>* pending_task_map,
    std::list<NodeSelectionResult>* selection_result_list) {
  
  CRANE_DEBUG("Starting node selection for {} pending tasks", pending_task_map->size());
  
  std::vector<task_id_t> ordered_task_ids = 
      m_priority_sorter_->GetOrderedTaskIdList(*pending_task_map, running_tasks, 100);
  
  CRANE_DEBUG("Got {} ordered tasks for processing", ordered_task_ids.size());
  
  auto craned_meta_map = g_meta_container->GetCranedMetaMapConstPtr();
  std::unordered_set<CranedId> all_craned_ids;
  for (const auto& [craned_id, _] : *craned_meta_map) {
    all_craned_ids.insert(craned_id);
  }
  
  CRANE_DEBUG("Found {} total nodes in system", all_craned_ids.size());

  std::unordered_map<CranedId, NodeEnergyInfo> energy_info;
  UpdateNodeEnergyInfo_(all_craned_ids, &energy_info);

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
      
      for (const CranedId& node_id : selected_nodes) {
        auto& node_info = energy_info[node_id];
        
        switch (node_info.state) {
          case NodeState::CranedRunning:
            CRANE_DEBUG("Node {} is ready (CranedRunning state)", node_id);
            break;
            
          case NodeState::RunningWithNoCraned:
            CRANE_DEBUG("Node {} is powered on but waiting for Craned service", node_id);
            all_nodes_ready = false;
            break;
            
          case NodeState::Sleeped:
            CRANE_DEBUG("Node {} is sleeping, will wake up for Task #{}", 
                       node_id, task->TaskId());
            nodes_to_wake.push_back(node_id);
            all_nodes_ready = false;
            break;
            
          case NodeState::Shutdown:
            CRANE_DEBUG("Node {} is shut down, will power on for Task #{}", 
                       node_id, task->TaskId());
            nodes_to_power_on.push_back(node_id);
            all_nodes_ready = false;
            break;
            
          case NodeState::WakingUp:
          case NodeState::PoweringUp:
            CRANE_DEBUG("Node {} is in transition state ({})", 
                       node_id, node_info.state == NodeState::WakingUp ? "waking up" : "powering up");
            all_nodes_ready = false;
            break;
            
          case NodeState::ShuttingDown:
            CRANE_DEBUG("Node {} is shutting down, cannot be used", node_id);
            all_nodes_ready = false;
            break;
            
          case NodeState::Unknown:
            CRANE_ERROR("Node {} is in unknown state", node_id);
            all_nodes_ready = false;
            break;
        }
      }

      if (!nodes_to_wake.empty() || !nodes_to_power_on.empty()) {
        for (const CranedId& node_id : nodes_to_wake) {
          if (m_ipmi_manager_.WakeupNode(node_id)) {
            CRANE_DEBUG("Wake up command sent to node {}", node_id);
          } else {
            CRANE_ERROR("Failed to send wake up command to node {}", node_id);
          }
        }
        
        for (const CranedId& node_id : nodes_to_power_on) {
          if (m_ipmi_manager_.PowerOnNode(node_id)) {
            CRANE_DEBUG("Power on command sent to node {}", node_id);
          } else {
            CRANE_ERROR("Failed to send power on command to node {}", node_id);
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

        for (const CranedId& node_id : selected_nodes) {
          energy_info[node_id].task_count++;
        }

        // 将任务从pending移动到selection result
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
  
  CRANE_DEBUG("Node selection completed. Processed {} tasks", ordered_task_ids.size());
}

void EnergyAwareNodeSelect::UpdateNodeEnergyInfo_(
    const std::unordered_set<CranedId>& craned_ids,
    std::unordered_map<CranedId, NodeEnergyInfo>* energy_info) {
  CRANE_DEBUG("Updating energy info for {} nodes", craned_ids.size());
  
  try {
    for (const CranedId& node_id : craned_ids) {
      NodeEnergyInfo& info = (*energy_info)[node_id];
      auto now = absl::Now();
      
      info.state = m_ipmi_manager_.GetNodeState(node_id);
      
      CRANE_DEBUG("Node {}: current_state={}", 
                  node_id, static_cast<int>(info.state));

      if (info.state == NodeState::CranedRunning) {
        CRANE_DEBUG("Updating energy stats for running node {}", node_id);
        UpdateSingleNodeEnergyInfo_(node_id, &info);
      }
    }
  } catch (const std::exception& e) {
    CRANE_ERROR("Failed to update node energy info: {}", e.what());
  }
}

bool EnergyAwareNodeSelect::UpdateSingleNodeEnergyInfo_(
    const CranedId& node_id,
    NodeEnergyInfo* info) {
  CRANE_DEBUG("Updating energy info for node {}", node_id);
  
  bool recent_ok = m_influx_client_.QueryNodeEnergyInfo(
      node_id, 
      absl::ToInt64Seconds(m_config_.recent_stats_window),
      &info->recent_stats);

  bool history_ok = m_influx_client_.QueryNodeEnergyInfo(
      node_id, 
      absl::ToInt64Seconds(m_config_.historical_stats_window),
      &info->historical_stats);

  // 这里多次一举，直接查询窗口内累计能耗就可以了
  if (history_ok) {
    info->historical_stats.total_energy = 
        info->historical_stats.avg_power * 
        absl::ToDoubleSeconds(m_config_.historical_stats_window);
  }

  return recent_ok && history_ok;
}

double EnergyAwareNodeSelect::CalculateNodeScore_(
    const NodeEnergyInfo& info) const {
  CRANE_DEBUG("Calculating node score - Current Power: {:.2f}W, Historical Power: {:.2f}W, Tasks: {}",
              info.recent_stats.current_power,
              info.historical_stats.avg_power,
              info.task_count);
              
  double power_score = 1.0 - (info.recent_stats.current_power / 300.0);
  double history_score = 1.0 - (info.historical_stats.avg_power / 300.0);
  double task_score = 1.0 - (info.task_count / 10.0);

  double final_score = m_config_.weight_current_power * power_score +
                      m_config_.weight_historical_power * history_score +
                      m_config_.weight_task_count * task_score;
                      
  CRANE_DEBUG("Score components - Power: {:.3f}, History: {:.3f}, Tasks: {:.3f}, Final: {:.3f}",
              power_score, history_score, task_score, final_score);
              
  return final_score;
}

void EnergyAwareNodeSelect::SelectNodesFromCandidates_(
    const std::vector<std::pair<double, CranedId>>& candidates,
    const std::string& node_type,
    TaskInCtld* task,
    const CranedMetaContainer::CranedMetaMapConstPtr& craned_meta,
    std::list<CranedId>* selected_nodes,
    ResourceV2* allocated_res) {
  
  for (const auto& [score, node_id] : candidates) {
    if (selected_nodes->size() >= task->node_num) break;

    auto node_meta = craned_meta->at(node_id).GetExclusivePtr();
    ResourceInNode feasible_res;
    
    bool ok = task->requested_node_res_view.GetFeasibleResourceInNode(
        node_meta->res_total, &feasible_res);
    
    if (!ok) {
      CRANE_DEBUG("Failed to get feasible resource from node {} for task #{}", 
                  node_id, task->TaskId());
      continue;
    }

    selected_nodes->push_back(node_id);
    allocated_res->AddResourceInNode(node_id, feasible_res);
    CRANE_DEBUG("Selected {} node {} (score: {:.3f}) for Task #{}", 
                node_type, node_id, score, task->TaskId());
  }
}

std::list<CranedId> EnergyAwareNodeSelect::SelectBestNodes_(
    TaskInCtld* task,
    const std::unordered_map<CranedId, NodeEnergyInfo>& energy_info) {
  
  CRANE_DEBUG("Selecting nodes for Task #{}, requires {} nodes", 
              task->TaskId(), task->node_num);
  
  auto craned_meta = g_meta_container->GetCranedMetaMapConstPtr();
  
  std::vector<std::pair<double, CranedId>> active_candidates;  
  std::vector<std::pair<double, CranedId>> sleeping_candidates;  
  std::vector<std::pair<double, CranedId>> shutdown_candidates;  
  
  active_candidates.reserve(energy_info.size());
  sleeping_candidates.reserve(energy_info.size());
  shutdown_candidates.reserve(energy_info.size());

  for (const auto& [craned_id, info] : energy_info) {
    auto node_meta = craned_meta->at(craned_id).GetExclusivePtr();
    
    if (!(task->requested_node_res_view <= node_meta->res_total)) {
      CRANE_TRACE("Node {} has insufficient resources for task #{}", 
                  craned_id, task->TaskId());
      continue;
    }
    
    if (!task->included_nodes.empty() && 
        !task->included_nodes.contains(craned_id)) {
      CRANE_DEBUG("Node {} not in included nodes list for task #{}", 
                  craned_id, task->TaskId());
      continue;
    }
    
    if (!task->excluded_nodes.empty() && 
        task->excluded_nodes.contains(craned_id)) {
      CRANE_DEBUG("Node {} in excluded nodes list for task #{}", 
                  craned_id, task->TaskId());
      continue;
    }

    double score = CalculateNodeScore_(info);
    auto power_state = m_ipmi_manager_.GetNodeState(craned_id);
    
    switch (power_state) {
      case NodeState::CranedRunning:
        active_candidates.emplace_back(score, craned_id);
        CRANE_DEBUG("Added running node {} to active candidates, score: {:.3f}", 
                   craned_id, score);
        break;
      case NodeState::Sleeped:
        sleeping_candidates.emplace_back(score, craned_id);
        CRANE_DEBUG("Added sleeping node {} to sleeping candidates, score: {:.3f}", 
                   craned_id, score);
        break;
      case NodeState::Shutdown:
        shutdown_candidates.emplace_back(score, craned_id);
        CRANE_DEBUG("Added shutdown node {} to shutdown candidates, score: {:.3f}", 
                   craned_id, score);
        break;
      case NodeState::RunningWithNoCraned:
      case NodeState::WakingUp:
      case NodeState::PoweringUp:
      case NodeState::ShuttingDown:
        CRANE_DEBUG("Node {} is in transition state ({}), skipping for now", 
                   craned_id, static_cast<int>(power_state));
        break;
      case NodeState::Unknown:
        CRANE_WARN("Node {} in unknown state, skipping", craned_id);
        break;
    }
  }

  std::sort(active_candidates.begin(), active_candidates.end(),
            std::greater<std::pair<double, CranedId>>());
  std::sort(sleeping_candidates.begin(), sleeping_candidates.end(),
            std::greater<std::pair<double, CranedId>>());
  std::sort(shutdown_candidates.begin(), shutdown_candidates.end(),
            std::greater<std::pair<double, CranedId>>());
            
  std::list<CranedId> selected_nodes;
  ResourceV2 allocated_res;
  
  CRANE_DEBUG("Found {} active, {} sleeping, {} shutdown candidates for Task #{}", 
              active_candidates.size(), sleeping_candidates.size(), 
              shutdown_candidates.size(), task->TaskId());
  
  if (!active_candidates.empty()) {
    CRANE_DEBUG("Selecting nodes from active candidates for task #{}", task->TaskId());
    SelectNodesFromCandidates_(active_candidates, "active", task, craned_meta,
                              &selected_nodes, &allocated_res);
  }

  if (selected_nodes.size() < task->node_num && !sleeping_candidates.empty()) {
    CRANE_DEBUG("Active nodes insufficient ({}/{}), considering sleeping nodes for task #{}", 
                selected_nodes.size(), task->node_num, task->TaskId());
    
    SelectNodesFromCandidates_(sleeping_candidates, "sleeping", task, craned_meta,
                              &selected_nodes, &allocated_res);
  }

  if (selected_nodes.size() < task->node_num && !shutdown_candidates.empty()) {
    CRANE_DEBUG("Sleeping nodes insufficient ({}/{}), considering shutdown nodes for task #{}", 
                selected_nodes.size(), task->node_num, task->TaskId());
    
    SelectNodesFromCandidates_(shutdown_candidates, "shutdown", task, craned_meta,
                              &selected_nodes, &allocated_res);
  }

  if (selected_nodes.size() == task->node_num) {
    task->SetResources(std::move(allocated_res));
    return selected_nodes;
  }

  CRANE_DEBUG("Failed to select enough nodes for task #{} ({}/{})", 
              task->TaskId(), 
              selected_nodes.size(), 
              task->node_num);
  return {};
}

}  // namespace Ctld 
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

#include "EnergyAwareScheduler.h"
#include "CranedKeeper.h"
#include "crane/Logger.h"

#include <bsoncxx/json.hpp>

namespace Ctld {

std::vector<task_id_t> EnergyAwarePriority::GetOrderedTaskIdList(
    const OrderedTaskMap& pending_task_map,
    const UnorderedTaskMap& running_task_map, 
    size_t limit) {
  
  std::vector<std::pair<TaskInCtld*, double>> task_scores;
  task_scores.reserve(pending_task_map.size());

  for (const auto& [task_id, task] : pending_task_map) {
    if (task->Held()) {
      task->pending_reason = "Held";
      continue;
    }

    double score = CalculateTaskEnergyScore_(task.get());
    task->cached_priority = score;
    task->pending_reason = "Priority";
    task_scores.emplace_back(task.get(), score);
  }

  std::sort(task_scores.begin(), task_scores.end(),
            [](const auto& a, const auto& b) {
              return a.second > b.second;
            });

  size_t result_size = std::min(limit, task_scores.size());
  std::vector<task_id_t> result;
  result.reserve(result_size);

  for (size_t i = 0; i < result_size; i++) {
    result.push_back(task_scores[i].first->TaskId());
  }

  return result;
}

double EnergyAwarePriority::CalculateTaskEnergyScore_(TaskInCtld* task) const {
  double historical_efficiency = GetHistoricalTaskEnergyEfficiency_(task);
  double resource_demand = task->requested_node_res_view.CpuCount() + 
                         task->requested_node_res_view.MemoryBytes() / (1024.0 * 1024.0 * 1024.0);
  return historical_efficiency / resource_demand;
}

double EnergyAwarePriority::GetHistoricalTaskEnergyEfficiency_(
    const TaskInCtld* task) const {
  try {
    std::string resp;
    std::string query = fmt::format(
        "SELECT mean(\"total_energy_j\") / mean(\"duration\") as efficiency "
        "FROM task_stats WHERE user = '{}' AND partition = '{}' "
        "GROUP BY task_id LIMIT 1",
        task->Username(), task->partition_id);
        
    int ret = influxdb_cpp::query(resp, query, m_server_info);
    if (ret != 0) {
      CRANE_ERROR("InfluxDB query failed with code: {}", ret);
      return 1.0;
    }

    if (!resp.empty()) {
      try {
        auto doc = bsoncxx::from_json(resp);
        auto results = doc.view()["results"].get_array().value;
        if (!results.empty() && !results[0]["series"].get_array().value.empty()) {
          auto values = results[0]["series"][0]["values"].get_array().value[0].get_array().value;
          return values[1].get_double().value;
        }
      } catch (const std::exception& e) {
        CRANE_ERROR("Failed to parse InfluxDB response: {}", e.what());
      }
    }
    return 1.0;
  } catch (const std::exception& e) {
    CRANE_ERROR("Failed to query InfluxDB: {}", e.what());
    return 1.0;
  }
}

void EnergyAwareNodeSelect::NodeSelect(
    const absl::flat_hash_map<task_id_t, std::unique_ptr<TaskInCtld>>& running_tasks,
    absl::btree_map<task_id_t, std::unique_ptr<TaskInCtld>>* pending_task_map,
    std::list<NodeSelectionResult>* selection_result_list) {
  
  std::vector<task_id_t> ordered_task_ids = 
      m_priority_sorter_->GetOrderedTaskIdList(*pending_task_map, running_tasks, 100);
  
  // 获取所有节点ID
  std::unordered_set<CranedId> all_craned_ids;
  auto craned_meta_map = g_meta_container->GetCranedMetaMapConstPtr();
  for (const auto& [craned_id, _] : *craned_meta_map) {
    all_craned_ids.insert(craned_id);
  }

  // 更新所有节点的能源信息和状态
  std::unordered_map<CranedId, NodeEnergyInfo> energy_info;
  UpdateNodeEnergyInfo_(all_craned_ids, &energy_info);

  // 检查是否有节点需要休眠
  for (const auto& [craned_id, info] : energy_info) {
    if (info.power_state == NodePowerState::Running && 
        ShouldSleepNode_(craned_id, info)) {
      CRANE_DEBUG("Node {} meets sleep criteria, executing SleepNode_", craned_id);
      if (SleepNode_(craned_id)) {
        energy_info[craned_id].power_state = NodePowerState::GoingToSleep;
        energy_info[craned_id].last_state_change = absl::Now();
      }
    }
  }

  // 处理每个待处理任务
  for (task_id_t task_id : ordered_task_ids) {
    auto task_it = pending_task_map->find(task_id);
    if (task_it == pending_task_map->end()) continue;

    TaskInCtld* task = task_it->second.get();
    
    // 为任务选择最佳节点
    std::list<CranedId> selected_nodes = SelectBestNodes_(task, energy_info);
    
    if (!selected_nodes.empty()) {
      bool all_nodes_ready = true;
      
      // 检查所有选中的节点，必要时唤醒
      for (const CranedId& node_id : selected_nodes) {
        auto& node_info = energy_info[node_id];
        
        switch (node_info.power_state) {
          case NodePowerState::Sleeping:
          case NodePowerState::GoingToSleep:
            CRANE_DEBUG("Node {} is in {} state, attempting to wake up", 
                       node_id, 
                       node_info.power_state == NodePowerState::Sleeping ? 
                           "Sleeping" : "GoingToSleep");
            
            if (!WakeupNode_(node_id)) {
              all_nodes_ready = false;
              CRANE_ERROR("Failed to wake up node {} for task #{}", 
                         node_id, task->TaskId());
              break;
            }
            node_info.power_state = NodePowerState::WakingUp;
            node_info.last_state_change = absl::Now();
            break;
            
          case NodePowerState::WakingUp:
            CRANE_DEBUG("Node {} is already waking up", node_id);
            // 检查唤醒是否超时
            if (absl::Now() - node_info.last_state_change > absl::Minutes(2)) {
              CRANE_ERROR("Node {} wake up timeout", node_id);
              all_nodes_ready = false;
            }
            break;
            
          case NodePowerState::Running:
            // 节点已经在运行，无需操作
            break;
            
          case NodePowerState::Unknown:
            CRANE_ERROR("Cannot use node {} with unknown state for task #{}", 
                       node_id, task->TaskId());
            all_nodes_ready = false;
            break;
        }
        
        if (!all_nodes_ready) break;
      }

      if (!all_nodes_ready) {
        CRANE_ERROR("Not all nodes are ready for task #{}", task->TaskId());
        continue;
      }

      // 所有节点就绪，分配任务
      selection_result_list->emplace_back(
          std::make_pair(std::move(task_it->second), selected_nodes));
      pending_task_map->erase(task_it);
      
      if (kAlgoTraceOutput) {
        std::string node_list;
        for (const auto& node : selected_nodes) {
          if (!node_list.empty()) node_list += ",";
          node_list += node;
        }
        CRANE_TRACE("Task #{} allocated to nodes: {}", task->TaskId(), node_list);
      }
    }
  }
}

void EnergyAwareNodeSelect::UpdateNodeEnergyInfo_(
    const std::unordered_set<CranedId>& craned_ids,
    std::unordered_map<CranedId, NodeEnergyInfo>* energy_info) {
  try {
    for (const CranedId& node_id : craned_ids) {
      NodeEnergyInfo& info = (*energy_info)[node_id];
      auto now = absl::Now();
      
      // 检查节点连接状态
      auto stub = g_craned_keeper->GetCranedStub(node_id);
      bool is_connected = (stub && !stub->Invalid());

      // 根据当前状态和连接状态更新节点状态
      switch (info.power_state) {
        case NodePowerState::WakingUp:
          if (is_connected) {
            info.power_state = NodePowerState::Running;
            info.last_state_change = now;
            CRANE_DEBUG("Node {} successfully woke up", node_id);
          } else if (now - info.last_state_change > absl::Minutes(2)) {
            info.power_state = NodePowerState::Unknown;
            CRANE_ERROR("Node {} wake up timeout", node_id);
          }
          break;

        case NodePowerState::GoingToSleep:
          if (!is_connected) {
            info.power_state = NodePowerState::Sleeping;
            info.last_state_change = now;
            CRANE_DEBUG("Node {} entered sleep state", node_id);
          } else if (now - info.last_state_change > absl::Minutes(2)) {
            info.power_state = NodePowerState::Unknown;
            CRANE_ERROR("Node {} sleep transition timeout", node_id);
          }
          break;

        default:
          // 对于其他状态，根据连接状态直接更新
          if (is_connected) {
            if (info.power_state != NodePowerState::Running) {
              info.power_state = NodePowerState::Running;
              info.last_state_change = now;
            }
          } else {
            std::string status = GetNodeIpmiStatus_(node_id);
            if (status == "on") {
              info.power_state = NodePowerState::WakingUp;
              info.last_state_change = now;
              CRANE_DEBUG("Node {} is powered on but not connected", node_id);
            } else {
              info.power_state = NodePowerState::Sleeping;
              info.last_state_change = now;
            }
          }
          break;
      }

      // 如果节点在运行状态，更新能源信息
      if (info.power_state == NodePowerState::Running) {
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
  try {
    // 1. 查询最近的能源统计数据
    std::string resp;
    std::string recent_query = fmt::format(
        "SELECT last(\"ipmi_power_w\") as current_power, "
        "mean(\"ipmi_power_w\") as avg_power, "
        "last(\"cpu_utilization\") as cpu_util, "
        "last(\"memory_utilization\") as mem_util, "
        "last(\"gpu_utilization\") as gpu_util "
        "FROM node_stats WHERE node_id = '{}' AND "
        "time > now() - {}s GROUP BY node_id",
        node_id, 
        absl::ToInt64Seconds(m_config.recent_stats_window));
    
    int ret = influxdb_cpp::query(resp, recent_query, m_server_info);
    if (ret != 0) {
      CRANE_ERROR("InfluxDB recent query failed for node {}: {}", node_id, ret);
      return false;
    }

    try {
      auto doc = bsoncxx::from_json(resp);
      auto results = doc.view()["results"].get_array().value;
      if (!results.empty() && !results[0]["series"].get_array().value.empty()) {
        auto values = results[0]["series"][0]["values"].get_array().value[0].get_array().value;
        info->recent_stats.current_power = values[1].get_double().value;
        info->recent_stats.avg_power = values[2].get_double().value;
        info->recent_stats.cpu_util = values[3].get_double().value;
        info->recent_stats.mem_util = values[4].get_double().value;
        info->recent_stats.gpu_util = values[5].get_double().value;
        info->recent_stats.time_span = m_config.recent_stats_window;
      }
    } catch (const std::exception& e) {
      CRANE_ERROR("Failed to parse recent stats for node {}: {}", node_id, e.what());
      return false;
    }

    // 2. 查询历史能源统计数据
    std::string history_query = fmt::format(
        "SELECT mean(\"ipmi_power_w\") as avg_power, "
        "sum(\"total_energy_j\") as total_energy "
        "FROM node_stats WHERE node_id = '{}' AND "
        "time > now() - {}s GROUP BY node_id",
        node_id,
        absl::ToInt64Seconds(m_config.historical_stats_window));
    
    ret = influxdb_cpp::query(resp, history_query, m_server_info);
    if (ret != 0) {
      CRANE_ERROR("InfluxDB historical query failed for node {}: {}", node_id, ret);
      return false;
    }

    try {
      auto doc = bsoncxx::from_json(resp);
      auto results = doc.view()["results"].get_array().value;
      if (!results.empty() && !results[0]["series"].get_array().value.empty()) {
        auto values = results[0]["series"][0]["values"].get_array().value[0].get_array().value;
        info->historical_stats.avg_power = values[1].get_double().value;
        info->historical_stats.total_energy = values[2].get_double().value;
        info->historical_stats.time_span = m_config.historical_stats_window;
      }
    } catch (const std::exception& e) {
      CRANE_ERROR("Failed to parse historical stats for node {}: {}", node_id, e.what());
      return false;
    }

    // 3. 更新任务计数和时间信息
    auto craned_meta = g_meta_container->GetCranedMetaMapConstPtr();
    auto node_meta = craned_meta->at(node_id).GetExclusivePtr();
    info->task_count = node_meta->running_task_resource_map.size();
    
    if (info->task_count == 0) {
      info->last_task_end_time = node_meta->last_busy_time;
    }

    CRANE_TRACE("Updated energy info for node {}: current_power={:.2f}W, avg_power={:.2f}W, "
                "cpu_util={:.1f}%, mem_util={:.1f}%, tasks={}",
                node_id,
                info->recent_stats.current_power,
                info->historical_stats.avg_power,
                info->recent_stats.cpu_util,
                info->recent_stats.mem_util,
                info->task_count);

    return true;
  } catch (const std::exception& e) {
    CRANE_ERROR("Unexpected error updating energy info for node {}: {}", 
                node_id, e.what());
    return false;
  }
}

std::string EnergyAwareNodeSelect::GetNodeIpmiStatus_(const CranedId& craned_id) const {
  auto craned_meta = g_meta_container->GetCranedMetaMapConstPtr();
  auto node_meta = craned_meta->at(craned_id).GetExclusivePtr();
  const auto& bmc = node_meta->static_meta.bmc;

  if (bmc.ip.empty() || bmc.username.empty() || bmc.password.empty()) {
    CRANE_ERROR("Incomplete BMC configuration for node {}", craned_id);
    return "unknown";
  }

  std::string ipmi_cmd = fmt::format(
      "ipmitool -I {} -H {} -p {} -U {} -P {} power status",
      bmc.interface,
      bmc.ip,
      bmc.port,
      bmc.username,
      bmc.password);

  FILE* pipe = popen(ipmi_cmd.c_str(), "r");
  if (!pipe) {
    CRANE_ERROR("Failed to execute IPMI command for node {}", craned_id);
    return "unknown";
  }

  char buffer[128];
  std::string result;
  while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
    result += buffer;
  }
  pclose(pipe);

  if (result.find("Power is on") != std::string::npos) {
    return "on";
  } else if (result.find("Power is off") != std::string::npos) {
    return "off";
  }

  return "unknown";
}

bool EnergyAwareNodeSelect::ShouldSleepNode_(
    const CranedId& craned_id,
    const NodeEnergyInfo& energy_info) const {
  if (energy_info.task_count > 0) return false;
  if (energy_info.power_state != NodePowerState::Running) return false;
  
  auto now = absl::Now();
  auto idle_time = now - energy_info.last_task_end_time;
  
  if (idle_time > m_config.idle_sleep_threshold && 
      energy_info.recent_stats.current_power < m_config.power_sleep_threshold) {
    CRANE_DEBUG("Node {} has been idle for {} seconds, current power {} W, sending sleep command", 
                craned_id, 
                absl::ToInt64Seconds(idle_time),
                energy_info.recent_stats.current_power);
    return true;
  }
  return false;
}

double EnergyAwareNodeSelect::CalculateNodeScore_(
    const NodeEnergyInfo& info) const {
  double power_score = 1.0 - (info.recent_stats.current_power / 300.0);
  double history_score = 1.0 - (info.historical_stats.avg_power / 300.0);
  double task_score = 1.0 - (info.task_count / 10.0);

  return m_config.weight_current_power * power_score +
         m_config.weight_historical_power * history_score +
         m_config.weight_task_count * task_score;
}

std::list<CranedId> EnergyAwareNodeSelect::SelectBestNodes_(
    TaskInCtld* task,
    const std::unordered_map<CranedId, NodeEnergyInfo>& energy_info) {
  
  auto craned_meta = g_meta_container->GetCranedMetaMapConstPtr();
  
  // 分为活跃和非活跃节点
  std::vector<std::pair<double, CranedId>> active_candidates;
  std::vector<std::pair<double, CranedId>> inactive_candidates;
  active_candidates.reserve(energy_info.size());
  inactive_candidates.reserve(energy_info.size());

  // 1. 筛选并评分所有符合条件的节点
  for (const auto& [craned_id, info] : energy_info) {
    auto node_meta = craned_meta->at(craned_id).GetExclusivePtr();
    
    // 基本条件检查
    if (!(task->requested_node_res_view <= node_meta->res_total)) {
      CRANE_TRACE("Node {} has insufficient resources for task #{}", 
                  craned_id, task->TaskId());
      continue;
    }
    
    if (!task->included_nodes.empty() && 
        !task->included_nodes.contains(craned_id)) {
      continue;
    }
    
    if (!task->excluded_nodes.empty() && 
        task->excluded_nodes.contains(craned_id)) {
      continue;
    }

    // 计算节点得分
    double score = CalculateNodeScore_(info);
    
    // 根据节点状态分类
    switch (info.power_state) {
      case NodePowerState::Running:
        active_candidates.emplace_back(score, craned_id);
        break;
      case NodePowerState::Sleeping:
      case NodePowerState::GoingToSleep:
        inactive_candidates.emplace_back(score, craned_id);
        break;
      case NodePowerState::WakingUp:
        // 正在唤醒的节点暂时不考虑，避免重复操作
        CRANE_DEBUG("Node {} is waking up, skipping for now", craned_id);
        break;
      case NodePowerState::Unknown:
        CRANE_WARN("Node {} state is unknown, skipping", craned_id);
        break;
    }
  }

  // 2. 首先尝试只从活跃节点中选择
  std::sort(active_candidates.begin(), active_candidates.end(),
            std::greater<std::pair<double, CranedId>>());
            
  std::list<CranedId> selected_nodes;
  ResourceV2 allocated_res;
  
  // 尝试从活跃节点中分配
  for (const auto& [score, node_id] : active_candidates) {
    if (selected_nodes.size() >= task->node_num) break;

    auto node_meta = craned_meta->at(node_id).GetExclusivePtr();
    ResourceInNode feasible_res;
    
    bool ok = task->requested_node_res_view.GetFeasibleResourceInNode(
        node_meta->res_total, &feasible_res);
    
    if (!ok) continue;

    selected_nodes.push_back(node_id);
    allocated_res.AddResourceInNode(node_id, feasible_res);
  }

  // 3. 如果活跃节点不够，考虑使用休眠节点
  if (selected_nodes.size() < task->node_num && !inactive_candidates.empty()) {
    CRANE_DEBUG("Active nodes insufficient ({}/{}), considering sleeping nodes for task #{}", 
                selected_nodes.size(), task->node_num, task->TaskId());
                
    std::sort(inactive_candidates.begin(), inactive_candidates.end(),
              std::greater<std::pair<double, CranedId>>());
              
    for (const auto& [score, node_id] : inactive_candidates) {
      if (selected_nodes.size() >= task->node_num) break;

      auto node_meta = craned_meta->at(node_id).GetExclusivePtr();
      ResourceInNode feasible_res;
      
      bool ok = task->requested_node_res_view.GetFeasibleResourceInNode(
          node_meta->res_total, &feasible_res);
      
      if (!ok) continue;

      selected_nodes.push_back(node_id);
      allocated_res.AddResourceInNode(node_id, feasible_res);
    }
  }

  // 4. 确认是否选择了足够的节点
  if (selected_nodes.size() == task->node_num) {
    task->SetResources(std::move(allocated_res));
    
    std::string node_list;
    for (const auto& node : selected_nodes) {
      if (!node_list.empty()) node_list += ",";
      node_list += node;
    }
    CRANE_TRACE("Task #{} allocated to nodes: {}", task->TaskId(), node_list);
    
    return selected_nodes;
  }

  CRANE_DEBUG("Failed to select enough nodes for task #{} ({}/{})", 
              task->TaskId(), 
              selected_nodes.size(), 
              task->node_num);
  return {};
}

bool EnergyAwareNodeSelect::ExecuteIpmiPowerCommand_(
    const CranedId& craned_id, const std::string& command) const {
  auto craned_meta = g_meta_container->GetCranedMetaMapConstPtr();
  auto node_meta = craned_meta->at(craned_id).GetExclusivePtr();
  
  const auto& bmc = node_meta->static_meta.bmc;
  if (bmc.ip.empty()) {
    CRANE_ERROR("No BMC address configured for node {}", craned_id);
    return false;
  }

  if (bmc.username.empty() || bmc.password.empty()) {
    CRANE_ERROR("Incomplete BMC credentials for node {}", craned_id);
    return false;
  }

  std::string ipmi_cmd = fmt::format(
      "ipmitool -I {} -H {} -p {} -U {} -P {} power {}",
      bmc.interface,
      bmc.ip,
      bmc.port,
      bmc.username,
      bmc.password,
      command);

  CRANE_DEBUG("Executing IPMI command for node {}: power {}", craned_id, command);
  
  int ret = system(ipmi_cmd.c_str());
  if (ret != 0) {
    CRANE_ERROR("Failed to execute IPMI command for node {}: {} (return code: {})", 
                craned_id, command, ret);
    return false;
  }
  
  return true;
}

bool EnergyAwareNodeSelect::WakeupNode_(const CranedId& craned_id) {
  CRANE_DEBUG("Attempting to wake up node {}", craned_id);
  
  const int max_retries = 3;
  const int check_interval_seconds = 10;
  
  if (!ExecuteIpmiPowerCommand_(craned_id, "on")) {
    CRANE_ERROR("Failed to wake up node {}", craned_id);
    return false;
  }

  // 等待并检查节点状态
  for (int i = 0; i < max_retries; i++) {
    std::this_thread::sleep_for(std::chrono::seconds(check_interval_seconds));
    
    auto stub = g_craned_keeper->GetCranedStub(craned_id);
    if (stub && !stub->Invalid()) {
      CRANE_DEBUG("Node {} is now online", craned_id);
      return true;
    }
    
    CRANE_WARN("Node {} still not ready, retry {}/{}", craned_id, i + 1, max_retries);
  }

  CRANE_ERROR("Node {} failed to wake up after {} seconds", 
              craned_id, 
              max_retries * check_interval_seconds);
  return false;
}

bool EnergyAwareNodeSelect::SleepNode_(const CranedId& craned_id) {
  CRANE_DEBUG("Attempting to sleep node {}", craned_id);
  
  if (!ExecuteIpmiPowerCommand_(craned_id, "soft")) {
    CRANE_ERROR("Failed to sleep node {}", craned_id);
    return false;
  }

  CRANE_DEBUG("Node {} sleep command sent successfully", craned_id);
  return true;
}

}  // namespace Ctld 
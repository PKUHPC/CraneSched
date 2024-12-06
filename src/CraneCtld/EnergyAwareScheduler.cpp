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
    double requested_cpu = task->requested_node_res_view.CpuCount();
    double requested_mem_gb = task->requested_node_res_view.MemoryBytes() / (1024.0 * 1024.0 * 1024.0);
    
    double cpu_lower = requested_cpu * 0.8;
    double cpu_upper = requested_cpu * 1.2;
    double mem_lower = requested_mem_gb * 0.8;
    double mem_upper = requested_mem_gb * 1.2;

    std::string query = fmt::format(
        "from(bucket: \"energy_task\") "
        "|> range(start: -24h) "
        "|> filter(fn: (r) => r[\"cpu_time_usage_seconds\"] >= {} and r[\"cpu_time_usage_seconds\"] <= {})"
        "|> filter(fn: (r) => r[\"memory_usage_mb\"] / 1024.0 >= {} and r[\"memory_usage_mb\"] / 1024.0 <= {})"
        "|> mean(column: \"total_energy_j\")",
        cpu_lower, cpu_upper, mem_lower, mem_upper);
    
    std::string resp = m_influx_client.Query(query);
    if (!resp.empty()) {
      auto doc = bsoncxx::from_json(resp);
      auto results = doc.view()["results"].get_array().value;
      if (!results.empty() && !results[0]["series"].get_array().value.empty()) {
        auto values = results[0]["series"][0]["values"].get_array().value[0].get_array().value;
        double total_energy = values[1].get_double().value;
        return 1.0 / total_energy;
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
  
  std::unordered_set<CranedId> all_craned_ids;
  auto craned_meta_map = g_meta_container->GetCranedMetaMapConstPtr();
  for (const auto& [craned_id, _] : *craned_meta_map) {
    all_craned_ids.insert(craned_id);
  }

  std::unordered_map<CranedId, NodeEnergyInfo> energy_info;
  UpdateNodeEnergyInfo_(all_craned_ids, &energy_info);

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

  for (task_id_t task_id : ordered_task_ids) {
    auto task_it = pending_task_map->find(task_id);
    if (task_it == pending_task_map->end()) continue;

    TaskInCtld* task = task_it->second.get();
    
    std::list<CranedId> selected_nodes = SelectBestNodes_(task, energy_info);
    
    if (!selected_nodes.empty()) {
      bool all_nodes_ready = true;
      
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
            if (absl::Now() - node_info.last_state_change > absl::Minutes(2)) {
              CRANE_ERROR("Node {} wake up timeout", node_id);
              all_nodes_ready = false;
            }
            break;
            
          case NodePowerState::Running:
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
      
      auto stub = g_craned_keeper->GetCranedStub(node_id);
      bool is_connected = (stub && !stub->Invalid());

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
    std::string recent_query = fmt::format(
        "from(bucket: \"energy_node\") |> range(start: -{}s) |> filter(fn: (r) => r[\"node_id\"] == \"{}\") |> last(column: \"ipmi_power_w\") |> mean(column: \"ipmi_power_w\") |> last(column: \"cpu_utilization\") |> last(column: \"memory_utilization\") |> last(column: \"gpu_utilization\")",
        absl::ToInt64Seconds(m_config.recent_stats_window), node_id);
    
    std::string resp = m_influx_client.Query(recent_query);
    if (!resp.empty()) {
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
    }

    std::string history_query = fmt::format(
        "from(bucket: \"energy_node\") |> range(start: -{}s) |> filter(fn: (r) => r[\"node_id\"] == \"{}\") |> mean(column: \"ipmi_power_w\") |> sum(column: \"total_energy_j\")",
        absl::ToInt64Seconds(m_config.historical_stats_window), node_id);
    
    resp = m_influx_client.Query(history_query);
    if (!resp.empty()) {
      auto doc = bsoncxx::from_json(resp);
      auto results = doc.view()["results"].get_array().value;
      if (!results.empty() && !results[0]["series"].get_array().value.empty()) {
        auto values = results[0]["series"][0]["values"].get_array().value[0].get_array().value;
        info->historical_stats.avg_power = values[1].get_double().value;
        info->historical_stats.total_energy = values[2].get_double().value;
        info->historical_stats.time_span = m_config.historical_stats_window;
      }
    }

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
  
  std::vector<std::pair<double, CranedId>> active_candidates;
  std::vector<std::pair<double, CranedId>> inactive_candidates;
  active_candidates.reserve(energy_info.size());
  inactive_candidates.reserve(energy_info.size());

  for (const auto& [craned_id, info] : energy_info) {
    auto node_meta = craned_meta->at(craned_id).GetExclusivePtr();
    
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

    double score = CalculateNodeScore_(info);
    
    switch (info.power_state) {
      case NodePowerState::Running:
        active_candidates.emplace_back(score, craned_id);
        break;
      case NodePowerState::Sleeping:
      case NodePowerState::GoingToSleep:
        inactive_candidates.emplace_back(score, craned_id);
        break;
      case NodePowerState::WakingUp:
        CRANE_DEBUG("Node {} is waking up, skipping for now", craned_id);
        break;
      case NodePowerState::Unknown:
        CRANE_WARN("Node {} state is unknown, skipping", craned_id);
        break;
    }
  }

  std::sort(active_candidates.begin(), active_candidates.end(),
            std::greater<std::pair<double, CranedId>>());
            
  std::list<CranedId> selected_nodes;
  ResourceV2 allocated_res;
  
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
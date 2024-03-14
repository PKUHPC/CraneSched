//
// Created by root on 3/12/24.
//

#include "XGBoostEstimator.h"

namespace Predictor {

/* Features:
 * 0: time_limit
 * 1: submit_time
 * 2: cpu_count
 * 3: memory_bytes
 * 4: node_num
 * 5: ntasks_per_node
 * 6: cpus_per_task
 * 7 ... 7 + kMaxUserRecentJob: recent_job_time, mean_recent_job_time
 */
void TaskInPredictor::BuildFeature(
    Predictor::HistoryTaskAnalyser *history_task_analyser) {
  feature.clear();
  feature.push_back(static_cast<float>(time_limit));
  feature.push_back(static_cast<float>(submit_time));
  feature.push_back(
      static_cast<float>(resources.allocatable_resource.cpu_count));
  feature.push_back(
      static_cast<float>(resources.allocatable_resource.memory_bytes));
  feature.push_back(static_cast<float>(node_num));
  feature.push_back(static_cast<float>(ntasks_per_node));
  feature.push_back(static_cast<float>(cpus_per_task));

  std::vector<int64_t> recent_job_time;
  history_task_analyser->GetRecentJobTime(uid, &recent_job_time);
  if (recent_job_time.empty()) {
    recent_job_time.push_back(0);
  }
  while (recent_job_time.size() < kMaxUserRecentJob) {
    recent_job_time.push_back(recent_job_time.back());
  }

  for (const auto &time : recent_job_time) {
    feature.push_back(static_cast<float>(time));
  }
  feature.push_back(
      std::accumulate(recent_job_time.begin(), recent_job_time.end(), 0.0f) /
      recent_job_time.size());
}

void HistoryTaskAnalyser::AddTaskInfo(
    std::unique_ptr<TaskInPredictor> task_info) {
  task_id_t task_id = task_info->task_id;

  task_info_[task_id] = std::move(task_info);
}

void HistoryTaskAnalyser::AddRealTimeInfo(
    task_id_t task_id, const google::protobuf::Duration &real_time) {
  if (!CheckTaskExist(task_id)) {
    return;
  }
  auto task_info = task_info_[task_id].get();
  task_info->SetRealTime(real_time.seconds());
  uid_t uid = task_info->uid;
  user_recent_finished_job_map_[uid].insert(task_id);
  if (user_recent_finished_job_map_[uid].size() > kMaxUserRecentJob) {
    RemoveTaskInfo(*user_recent_finished_job_map_[uid].begin());
  }
}

void HistoryTaskAnalyser::GetRecentJobTime(
    uid_t uid, std::vector<int64_t> *recent_job_time) {
  auto iter = user_recent_finished_job_map_.find(uid);
  if (iter == user_recent_finished_job_map_.end()) {
    return;
  }

  for (const auto &task_id : iter->second) {
    recent_job_time->push_back(task_info_[task_id].get()->real_time);
  }
}

void HistoryTaskAnalyser::RemoveTaskInfo(task_id_t task_id) {
  if (!CheckTaskExist(task_id)) {
    return;
  }
  uid_t uid = task_info_[task_id].get()->uid;
  user_recent_finished_job_map_[uid].erase(task_id);

  task_info_.erase(task_id);
}

bool HistoryTaskAnalyser::CheckTaskExist(task_id_t task_id) {
  if (task_info_.find(task_id) == task_info_.end()) {
    std::cout << "Task " << task_id << " not exist" << std::endl;
    return false;
  }
  return true;
}

void XGBoostEstimator::Predict(
    const crane::grpc::TaskEstimationRequest *request,
    crane::grpc::TaskEstimationReply *reply) {
  for (const auto &task : request->tasks()) {
    std::unique_ptr<TaskInPredictor> task_info =
        std::make_unique<TaskInPredictor>();

    std::cout << "task_id: " << task.task_id() << std::endl;
    std::cout << "time_limit: " << task.time_limit() << std::endl;
    std::cout << "submit_time: " << task.submit_time() << std::endl;
    std::cout << "resources: "
              << static_cast<double>(
                     task.resources().allocatable_resource().cpu_core_limit())
              << std::endl;
    std::cout << "node_num: " << task.node_num() << std::endl;
    std::cout << "ntasks_per_node: " << task.ntasks_per_node() << std::endl;
    std::cout << "cpus_per_task: " << task.cpus_per_task() << std::endl;
    std::cout << "uid: " << task.uid() << std::endl;
    std::cout << "account: " << task.account() << std::endl;
    std::cout << "partition: " << task.partition() << std::endl;
    std::cout << "name: " << task.name() << std::endl;
    std::cout << "qos: " << task.qos() << std::endl;
    std::cout << "cwd: " << task.cwd() << std::endl;
    std::cout << "cmd_line: " << task.cmd_line() << std::endl;
    std::cout << "get_user_env: " << task.get_user_env() << std::endl;
    std::cout << "sh_script: " << task.batch_meta().sh_script() << std::endl;
    std::cout << "output_file_pattern: "
              << task.batch_meta().output_file_pattern() << std::endl;

    task_info->SetFeildsByTaskToPredictor(task);
    task_info->BuildFeature(&history_task_analyser_);

    model_.AddRow(task_info->feature);

    history_task_analyser_.AddTaskInfo(std::move(task_info));
  }

  for (const auto &estimation : reply->estimations()) {
    std::cout << "Task " << estimation.task_id()
              << " estimated time: " << estimation.estimated_time().seconds()
              << " seconds" << std::endl;
  }
}

void XGBoostEstimator::Record(
    const crane::grpc::TaskExecutionTimeAck *request) {
  for (const auto &execution_time : request->execution_times()) {
    history_task_analyser_.AddRealTimeInfo(execution_time.task_id(),
                                           execution_time.execution_time());
  }
}

}  // namespace Predictor
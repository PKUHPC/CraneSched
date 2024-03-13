//
// Created by root on 3/12/24.
//

#include "XGBoostEstimator.h"
// #include <xgboost/c_api.h>

namespace Predictor {

void TaskInPredictor::BuildFeature(
    Predictor::HistoryTaskAnalyser *history_task_analyser) {
  feature.time_limit = time_limit;
  feature.submit_time = submit_time;
  feature.cpu_count =
      static_cast<double>(resources.allocatable_resource.cpu_count);
  feature.memory_bytes = resources.allocatable_resource.memory_bytes;
  feature.memory_sw_bytes = resources.allocatable_resource.memory_sw_bytes;
  feature.node_num = node_num;
  feature.ntasks_per_node = ntasks_per_node;
  feature.cpus_per_task = cpus_per_task;
  feature.uid = uid;
  feature.account = account;
  feature.partition = partition;
  feature.name = name;

  history_task_analyser->GetRecentJobTime(uid, &feature.recent_job_time);

  if (feature.recent_job_time.empty()) {
    feature.mean_recent_job_time = 0;
  } else {
    feature.mean_recent_job_time =
        std::accumulate(feature.recent_job_time.begin(),
                        feature.recent_job_time.end(), 0.0) /
        feature.recent_job_time.size();
  }
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

    auto estimation = reply->add_estimations();
    estimation->set_task_id(task.task_id());

    // use the mean of recent job time as the estimation
    estimation->mutable_estimated_time()->set_seconds(
        std::round(task_info->feature.mean_recent_job_time));

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
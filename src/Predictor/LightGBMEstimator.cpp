//
// Created by root on 3/12/24.
//

#include "LightGBMEstimator.h"

namespace Predictor {

void GetTimeInfo(absl::Time time, int *year, int *quarter, int *month, int *day,
                 int *hour, int *minute, int *second, int *day_of_year,
                 int *day_of_month, int *day_of_week) {
  struct tm tm {};
  time_t t = ToUnixSeconds(time);
  localtime_r(&t, &tm);
  *year = tm.tm_year + 1900;
  *quarter = (tm.tm_mon / 3) + 1;
  *month = tm.tm_mon + 1;
  *day = tm.tm_mday;
  *hour = tm.tm_hour;
  *minute = tm.tm_min;
  *second = tm.tm_sec;
  *day_of_year = tm.tm_yday + 1;
  *day_of_month = tm.tm_mday;
  *day_of_week = tm.tm_wday;
}

/*
 feature_names=
 id_user id_qos cpus_req nodes_alloc timelimit time_submit
 sub_year sub_quarter sub_month sub_day sub_hour sub_day_of_year
 sub_day_of_month sub_day_of_week top1_time top2_time top2_mean
 */
void TaskInPredictor::BuildFeature(
    Predictor::HistoryTaskAnalyser *history_task_analyser) {
  feature.clear();
  feature.push_back(static_cast<double>(uid));
  feature.push_back(static_cast<double>(qos));
  feature.push_back(
      static_cast<double>(resources.allocatable_resource.cpu_count));
  feature.push_back(static_cast<double>(node_num));
  feature.push_back(static_cast<double>(ToInt64Seconds(time_limit)));
  feature.push_back(static_cast<double>(ToUnixSeconds(submit_time)));

  int sub_year, sub_quarter, sub_month, sub_day, sub_hour, sub_minute,
      sub_second, sub_day_of_year, sub_day_of_month, sub_day_of_week;
  GetTimeInfo(submit_time, &sub_year, &sub_quarter, &sub_month, &sub_day,
              &sub_hour, &sub_minute, &sub_second, &sub_day_of_year,
              &sub_day_of_month, &sub_day_of_week);

  feature.push_back(static_cast<double>(sub_year));
  feature.push_back(static_cast<double>(sub_quarter));
  feature.push_back(static_cast<double>(sub_month));
  feature.push_back(static_cast<double>(sub_day));
  feature.push_back(static_cast<double>(sub_hour));
  feature.push_back(static_cast<double>(sub_day_of_year));
  feature.push_back(static_cast<double>(sub_day_of_month));
  feature.push_back(static_cast<double>(sub_day_of_week));

  std::vector<int64_t> recent_job_time;
  history_task_analyser->GetRecentJobTime(uid, &recent_job_time);
  if (recent_job_time.empty()) {
    recent_job_time.push_back(0);
  }
  while (recent_job_time.size() < kMaxUserRecentJob) {
    recent_job_time.push_back(recent_job_time.back());
  }

  for (const auto &time : recent_job_time) {
    feature.push_back(static_cast<double>(time));
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
    CRANE_INFO("Task {} not exist", task_id);
    return false;
  }
  return true;
}

void LightGBMEstimator::Predict(
    const crane::grpc::TaskEstimationRequest *request,
    crane::grpc::TaskEstimationReply *reply) {
  CRANE_INFO("received {} tasks", request->tasks_size());
  for (const auto &task : request->tasks()) {
    std::unique_ptr<TaskInPredictor> task_info =
        std::make_unique<TaskInPredictor>();
    task_info->SetFeildsByTaskToPredictor(task);
    task_info->BuildFeature(&history_task_analyser_);

    model_.AddRow(task_info->feature);

    history_task_analyser_.AddTaskInfo(std::move(task_info));
  }
  std::vector<int64_t> result;
  model_.Predict(&result);
  for (int i = 0; i < result.size(); i++) {
    reply->add_estimations()->set_task_id(request->tasks(i).task_id());
    reply->mutable_estimations(i)->mutable_estimated_time()->set_seconds(
        result[i]);
  }
  CRANE_INFO("finish {} tasks", request->tasks_size());
}

void LightGBMEstimator::Record(
    const crane::grpc::TaskExecutionTimeAck *request) {
  for (const auto &execution_time : request->execution_times()) {
    history_task_analyser_.AddRealTimeInfo(execution_time.task_id(),
                                           execution_time.execution_time());
  }
}

}  // namespace Predictor
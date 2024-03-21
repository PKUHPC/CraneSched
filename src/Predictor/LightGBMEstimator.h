//
// Created by root on 3/12/24.
//

#pragma once

#include <LightGBM/c_api.h>

#include "TimeEstimator.h"
#include "protos/Crane.pb.h"

namespace Predictor {

constexpr int kMaxUserRecentJob = 2;

using TaskFeature = std::vector<double>;

struct BatchMetaInTask {
  std::string sh_script;
  std::string output_file_pattern;
};

class HistoryTaskAnalyser;

struct TaskInPredictor {
  task_id_t task_id;
  int64_t real_time;

  /* -------- Fields directly used for estimating ------- */
  absl::Duration time_limit;
  absl::Time submit_time;

  Resources resources;
  uint32_t node_num{0};
  uint32_t ntasks_per_node{0};
  cpu_t cpus_per_task{0};

  uid_t uid;
  std::string account;
  std::string partition;

  std::string name;

  /* -------- Fields used to calculate job correlation ------- */
  std::string qos;
  std::string cwd;

  std::string cmd_line;

  std::unordered_set<std::string> excludes;
  std::unordered_set<std::string> nodelist;

  bool get_user_env{false};
  std::unordered_map<std::string, std::string> env;

  BatchMetaInTask meta;
  /* -------- Fields of preprocessed feature ------- */
  TaskFeature feature;

  void SetFeildsByTaskToPredictor(const crane::grpc::TaskToPredictor &val) {
    task_id = val.task_id();
    time_limit = absl::Seconds(val.time_limit().seconds());
    submit_time = absl::FromUnixSeconds(val.submit_time().seconds());

    resources.allocatable_resource = val.resources().allocatable_resource();

    node_num = val.node_num();
    ntasks_per_node = val.ntasks_per_node();
    cpus_per_task = cpu_t(val.cpus_per_task());

    uid = val.uid();
    account = val.account();
    partition = val.partition();

    name = val.name();

    qos = val.qos();
    cwd = val.cwd();

    cmd_line = val.cmd_line();

    for (const auto &exclude : val.excludes()) {
      excludes.insert(exclude);
    }

    for (const auto &node : val.nodelist()) {
      nodelist.insert(node);
    }

    get_user_env = val.get_user_env();
    for (const auto &env_pair : val.env()) {
      env[env_pair.first] = env_pair.second;
    }

    meta.sh_script = val.batch_meta().sh_script();
    meta.output_file_pattern = val.batch_meta().output_file_pattern();
  }

  void SetRealTime(int64_t val) { real_time = val; }

  void BuildFeature(HistoryTaskAnalyser *history_task_analyser);
};

class HistoryTaskAnalyser {
 public:
  HistoryTaskAnalyser() = default;
  ~HistoryTaskAnalyser() = default;

  void AddTaskInfo(std::unique_ptr<TaskInPredictor> task_info);

  void AddRealTimeInfo(task_id_t task_id,
                       const google::protobuf::Duration &real_time);

  void GetRecentJobTime(uid_t uid, std::vector<int64_t> *recent_job_time);

 private:
  std::unordered_map<task_id_t, std::unique_ptr<TaskInPredictor>> task_info_;
  std::unordered_map<uid_t, std::set<task_id_t>>
      user_recent_finished_job_map_;  // keep kMaxUserRecentJob recent jobs of
                                      // specific user
  bool CheckTaskExist(task_id_t task_id);
  void RemoveTaskInfo(task_id_t task_id);
};

class LightGBMModel {
 public:
  LightGBMModel(const std::string &model_path) {
    if (LGBM_BoosterCreateFromModelfile(model_path.c_str(), &num_iterations,
                                        &booster) != 0) {
      std::cerr << "Could not load model." << std::endl;
      throw std::runtime_error("Could not load model.");
    }
  }

  ~LightGBMModel() { LGBM_BoosterFree(booster); }

  void AddRow(const TaskFeature &row) {
    if (!matrix.empty() && matrix[0].size() != row.size()) {
      throw std::runtime_error("Row size does not match.");
    }
    matrix.emplace_back(row);
  }

  void Predict(std::vector<int64_t> *result) {
    result->clear();
    if (matrix.empty()) {  // No data to predict
      return;
    }
    int num_rows = matrix.size();
    int num_cols = matrix[0].size();
    std::vector<double> data;
    for (const auto &row : matrix) {
      data.insert(data.end(), row.begin(), row.end());
    }

    std::vector<double> predictions(num_rows);
    int64_t out_len = 0;
    if (LGBM_BoosterPredictForMat(booster, data.data(), C_API_DTYPE_FLOAT64,
                                  num_rows, num_cols, 1, C_API_PREDICT_NORMAL,
                                  0, num_iterations, nullptr, &out_len,
                                  predictions.data()) != 0) {
      throw std::runtime_error("Prediction failed.");
    }

    for (const auto &pred : predictions) {
      result->push_back(static_cast<int64_t>(pred));
    }
  }

 private:
  BoosterHandle booster = nullptr;
  int num_iterations;
  std::vector<TaskFeature> matrix;
};

class LightGBMEstimator : public ITimeEstimator {
 public:
  explicit LightGBMEstimator(const std::string &model_path)
      : model_(model_path) {}
  ~LightGBMEstimator() = default;

  void Predict(const crane::grpc::TaskEstimationRequest *request,
               crane::grpc::TaskEstimationReply *reply) override;

  void Record(const crane::grpc::TaskExecutionTimeAck *request) override;

 private:
  HistoryTaskAnalyser history_task_analyser_;
  LightGBMModel model_;
};

}  // namespace Predictor
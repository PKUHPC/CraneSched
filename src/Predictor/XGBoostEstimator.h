//
// Created by root on 3/12/24.
//

#pragma once

#include <xgboost/c_api.h>

#include "TimeEstimator.h"
#include "protos/Crane.pb.h"

namespace Predictor {

constexpr int kMaxUserRecentJob = 2;

using TaskFeature = std::vector<float>;

struct BatchMetaInTask {
  std::string sh_script;
  std::string output_file_pattern;
};

class HistoryTaskAnalyser;

struct TaskInPredictor {
  task_id_t task_id;
  int64_t real_time;

  /* -------- Fields directly used for estimating ------- */
  int64_t time_limit;
  int64_t submit_time;

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
    time_limit = val.time_limit().seconds();
    submit_time = val.submit_time().seconds();

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

class XGBoostModel {
 public:
  XGBoostModel(const std::string &model_path) {
    if (XGBoosterCreate(nullptr, 0, &booster) != 0) {
      throw std::runtime_error("Failed to create XGBooster.");
    }
    if (XGBoosterLoadModel(booster, model_path.c_str()) != 0) {
      throw std::runtime_error("Failed to load model.");
    }
  }

  ~XGBoostModel() { XGBoosterFree(booster); }

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
    int nrow = matrix.size();
    int ncol = matrix[0].size();
    std::vector<float> data;
    for (const auto &row : matrix) {
      data.insert(data.end(), row.begin(), row.end());
    }

    DMatrixHandle dmat;
    if (XGDMatrixCreateFromMat(data.data(), nrow, ncol, NAN, &dmat) != 0) {
      throw std::runtime_error("Failed to create DMatrix.");
    }

    bst_ulong out_len;
    const float *out_result;
    if (XGBoosterPredict(booster, dmat, 0, 0, 0, &out_len, &out_result) != 0) {
      XGDMatrixFree(dmat);
      throw std::runtime_error("Prediction failed.");
    }

    for (bst_ulong i = 0; i < out_len; ++i) {
      result->push_back((int64_t)std::round(out_result[i]));
    }

    XGDMatrixFree(dmat);
    matrix.clear();
  }

 private:
  BoosterHandle booster;
  std::vector<TaskFeature> matrix;
};

class XGBoostEstimator : public ITimeEstimator {
 public:
  explicit XGBoostEstimator(const std::string &model_path)
      : model_(model_path) {}
  ~XGBoostEstimator() = default;

  void Predict(const crane::grpc::TaskEstimationRequest *request,
               crane::grpc::TaskEstimationReply *reply) override;

  void Record(const crane::grpc::TaskExecutionTimeAck *request) override;

 private:
  HistoryTaskAnalyser history_task_analyser_;
  XGBoostModel model_;
};

}  // namespace Predictor
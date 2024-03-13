//
// Created by root on 3/12/24.
//

#pragma once

#include "TimeEstimator.h"
#include "protos/Crane.pb.h"

namespace Predictor {

constexpr int kMaxUserRecentJob = 10;

struct TaskFeature {
  int64_t time_limit;
  int64_t submit_time;

  double cpu_count;
  uint64_t memory_bytes;
  uint64_t memory_sw_bytes;

  uint32_t node_num;
  uint32_t ntasks_per_node;
  cpu_t cpus_per_task;

  uid_t uid;
  std::string account;
  std::string partition;

  std::string name;

  std::vector<int64_t> recent_job_time;
  double mean_recent_job_time;
};

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

class XGBoostEstimator : public ITimeEstimator {
 public:
  XGBoostEstimator() = default;
  ~XGBoostEstimator() = default;

  void SetModelPath(const std::string &model_path);

  void Predict(const crane::grpc::TaskEstimationRequest *request,
               crane::grpc::TaskEstimationReply *reply) override;

  void Record(const crane::grpc::TaskExecutionTimeAck *request) override;

 private:
  HistoryTaskAnalyser history_task_analyser_;
};

}  // namespace Predictor
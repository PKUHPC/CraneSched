//
// Created by root on 3/12/24.
//

#pragma once

#include "TimeEstimator.h"

namespace Predictor {

class XGBoostEstimator : public ITimeEstimator {
 public:
  XGBoostEstimator(std::string model_path);
  ~XGBoostEstimator() = default;

  void Predict(const crane::grpc::TaskEstimationRequest *request,
               crane::grpc::TaskEstimationReply *reply) override;

  void Record(const crane::grpc::TaskExecutionTimeAck *request) override;

 private:
  //  std::unordered_map<int, std::unique_ptr<crane::grpc::TaskToPredictor>>
};

}  // namespace Predictor
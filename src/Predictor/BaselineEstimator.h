//
// Created by root on 3/12/24.
//

#pragma once

#include "TimeEstimator.h"

namespace Predictor {

class BaselineEstimator : public ITimeEstimator {
 public:
  BaselineEstimator() = default;
  ~BaselineEstimator() = default;

  void Predict(const crane::grpc::TaskEstimationRequest *request,
               crane::grpc::TaskEstimationReply *reply) override;

  void Record(const crane::grpc::TaskExecutionTimeAck *request) override;
};

}  // namespace Predictor
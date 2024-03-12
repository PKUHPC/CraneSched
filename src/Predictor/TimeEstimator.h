//
// Created by root on 3/12/24.
//

#pragma once

#include "PredictorPublicDefs.h"
#include "protos/Crane.grpc.pb.h"

namespace Predictor {

class ITimeEstimator {
 public:
  virtual void Predict(const crane::grpc::TaskEstimationRequest *request,
                       crane::grpc::TaskEstimationReply *reply) = 0;

  virtual void Record(const crane::grpc::TaskExecutionTimeAck *request) = 0;
};

}  // namespace Predictor
//
// Created by root on 3/12/24.
//

#include "XGBoostEstimator.h"

namespace Predictor {

void XGBoostEstimator::Predict(
    const crane::grpc::TaskEstimationRequest *request,
    crane::grpc::TaskEstimationReply *reply) {}

void XGBoostEstimator::Record(
    const crane::grpc::TaskExecutionTimeAck *request) {}

}  // namespace Predictor
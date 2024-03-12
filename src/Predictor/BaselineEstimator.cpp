//
// Created by root on 3/12/24.
//

#include "BaselineEstimator.h"

namespace Predictor {

void BaselineEstimator::Predict(
    const crane::grpc::TaskEstimationRequest *request,
    crane::grpc::TaskEstimationReply *reply) {
  for (const auto &task : request->tasks()) {
    auto *estimation = reply->add_estimations();
    estimation->set_task_id(task.task_id());

    estimation->mutable_estimated_time()->CopyFrom(task.time_limit());

    // estimated time should be greater than 0 to affect the TimeAvailResMap.
    if (estimation->estimated_time().seconds() <= 0) {
      estimation->mutable_estimated_time()->set_seconds(1);
    }
  }
}

void BaselineEstimator::Record(
    const crane::grpc::TaskExecutionTimeAck *request) {
}

}  // namespace Predictor
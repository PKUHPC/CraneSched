//
// Created by root on 3/12/24.
//

#include "PredGrpcServer.h"

#include "LightGBMEstimator.h"

namespace Predictor {

grpc::Status CranePredServiceImpl::TaskEstimation(
    grpc::ServerContext *context,
    const crane::grpc::TaskEstimationRequest *request,
    crane::grpc::TaskEstimationReply *reply) {
  m_predictor_server_->EstimateRunTime(request, reply);
  return grpc::Status::OK;
}

grpc::Status CranePredServiceImpl::ReportExecutionTime(
    grpc::ServerContext *context,
    const crane::grpc::TaskExecutionTimeAck *request,
    ::google::protobuf::Empty *reply) {
  CRANE_INFO("received task execution time of {} tasks",
             request->execution_times().size());
  m_predictor_server_->RecordRunTime(request);

  return grpc::Status::OK;
}

PredServer::PredServer() {
  CRANE_INFO("Loading model...");
  m_time_estimator_ =
      std::make_unique<LightGBMEstimator>("/home/nameless/Desktop/model.txt");
  CRANE_INFO("Model loaded");

  m_service_impl_ = std::make_unique<CranePredServiceImpl>(this);

  std::string listen_addr_port("0.0.0.0:51890");

  grpc::ServerBuilder builder;
  builder.AddListeningPort(listen_addr_port, grpc::InsecureServerCredentials());
  builder.RegisterService(m_service_impl_.get());

  m_server_ = builder.BuildAndStart();

  CRANE_INFO("Server listening on {}", listen_addr_port);
}

void PredServer::EstimateRunTime(
    const crane::grpc::TaskEstimationRequest *request,
    crane::grpc::TaskEstimationReply *reply) {
  m_time_estimator_->Predict(request, reply);
}

void PredServer::RecordRunTime(
    const crane::grpc::TaskExecutionTimeAck *request) {
  m_time_estimator_->Record(request);
}

}  // namespace Predictor
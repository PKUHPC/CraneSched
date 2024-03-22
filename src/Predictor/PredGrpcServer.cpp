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
  std::cout << "received task execution time of "
            << request->execution_times().size() << " tasks" << std::endl;

  m_predictor_server_->RecordRunTime(request);

  return grpc::Status::OK;
}

PredServer::PredServer() {
  std::cerr << "Loading model..." << std::endl;
  m_time_estimator_ =
      std::make_unique<LightGBMEstimator>("/home/nameless/Desktop/model.txt");
  std::cerr << "Model loaded" << std::endl;

  m_service_impl_ = std::make_unique<CranePredServiceImpl>(this);

  std::string listen_addr_port("0.0.0.0:51890");

  grpc::ServerBuilder builder;
  builder.AddListeningPort(listen_addr_port, grpc::InsecureServerCredentials());
  builder.RegisterService(m_service_impl_.get());

  m_server_ = builder.BuildAndStart();

  std::cout << "Server listening on " << listen_addr_port << std::endl;
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
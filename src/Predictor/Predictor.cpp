//
// Created by root on 3/9/24.
//

#include "Predictor.h"

class CranePredServiceImpl final : public crane::grpc::CranePred::Service {
 public:
  grpc::Status TaskEstimation(
      grpc::ServerContext* context,
      const crane::grpc::TaskEstimationRequest* request,
      crane::grpc::TaskEstimationReply* reply) override {
    for (const auto& task : request->tasks()) {
      auto* estimation = reply->add_estimations();
      estimation->set_task_id(task.task_id());
      estimation->mutable_estimated_time()->CopyFrom(task.time_limit());
      std::cout << "Task " << task.task_id() << " estimated time: "
                << task.time_limit().seconds() << " seconds" << std::endl;
    }
    return grpc::Status::OK;
  }
};

void RunServer() {
  std::string server_address("0.0.0.0:51890");
  CranePredServiceImpl service;

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  server->Wait();
}

int main(int argc, char** argv) {
  RunServer();

  return 0;
}

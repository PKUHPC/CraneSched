#include "SharedTestImpl/greeter_service_impl.h"

#include <thread>

#include "crane/PublicHeader.h"

grpc::Status GreeterServiceImpl::SayHello(
    grpc::ServerContext* context, const grpc_example::HelloRequest* request,
    grpc_example::HelloReply* reply) {
  std::string prefix("Hello ");
  reply->set_message(prefix + request->name());
  return grpc::Status::OK;
}

grpc::Status GreeterServiceImpl::SleepSeconds(
    grpc::ServerContext* context, const grpc_example::SleepRequest* request,
    grpc_example::SleepReply* response) {
  response->set_ok(true);

  std::this_thread::sleep_for(std::chrono::seconds(request->seconds()));

  return grpc::Status::OK;
}

GreeterSyncServer::GreeterSyncServer(const std::string& server_address) {
  grpc::ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&m_service_impl_);
  // Finally assemble the server.
  m_server_ = builder.BuildAndStart();

  CRANE_INFO("Server listening on {}", server_address);
}

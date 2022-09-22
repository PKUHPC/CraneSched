#pragma once

#include <grpc++/grpc++.h>

#include "protos/greeter.grpc.pb.h"
#include "protos/greeter.pb.h"

class GreeterServiceImpl final : public grpc_example::Greeter::Service {
  grpc::Status SayHello(grpc::ServerContext* context,
                        const grpc_example::HelloRequest* request,
                        grpc_example::HelloReply* reply) override;

  grpc::Status SleepSeconds(grpc::ServerContext* context,
                            const grpc_example::SleepRequest* request,
                            grpc_example::SleepReply* response) override;
};

class GreeterSyncServer {
 public:
  GreeterSyncServer(const std::string& server_address);

  inline void Shutdown() { m_server_->Shutdown(); }

  inline void Wait() { m_server_->Wait(); }

 private:
  std::unique_ptr<grpc::Server> m_server_;
  GreeterServiceImpl m_service_impl_;
};
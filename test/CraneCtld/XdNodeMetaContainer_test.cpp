#include <gmock/gmock.h>
#include <grpc++/grpc++.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <thread>

#include "SharedTestImpl/GlobalDefs.h"
#include "SharedTestImpl/greeter_service_impl.h"
#include "protos/greeter.grpc.pb.h"
#include "protos/greeter.pb.h"
#include "crane/FdFunctions.h"

using grpc::Channel;
using grpc_example::Greeter;

TEST(Draft, FreeStubWhenCallingRpc) {
  using namespace std::chrono_literals;

  std::string server_addr("localhost:50011");

  pid_t child_pid = fork();
  if (child_pid == 0) {
    auto greeter_server = std::make_unique<GreeterSyncServer>(server_addr);

    std::this_thread::sleep_for(3s);

    (void)greeter_server.get();
    util::CloseFdFrom(0);

    std::terminate();
  } else {
    auto channel =
        grpc::CreateChannel(server_addr, grpc::InsecureChannelCredentials());
    auto stub = Greeter::NewStub(channel);

    using grpc::ClientContext;
    using grpc_example::SleepReply;
    using grpc_example::SleepRequest;

    ClientContext context;
    SleepRequest request;
    SleepReply reply;

    request.set_seconds(10);
    grpc::Status status = stub->SleepSeconds(&context, request, &reply);

    EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAVAILABLE);

    int stat;
    wait(&stat);
  }
}
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string_view>
#include <thread>

#include "SharedTestImpl/greeter_service_impl.h"
#include "crane/PublicHeader.h"

using grpc::Channel;

using grpc_example::Greeter;
using grpc_example::HelloReply;
using grpc_example::HelloRequest;

using namespace std::chrono_literals;

std::string_view GrpcConnStateStr(grpc_connectivity_state state) {
  switch (state) {
    case grpc_connectivity_state::GRPC_CHANNEL_IDLE:
      return "IDLE";
    case grpc_connectivity_state::GRPC_CHANNEL_CONNECTING:
      return "CONNECTING";
    case grpc_connectivity_state::GRPC_CHANNEL_READY:
      return "READY";
    case grpc_connectivity_state::GRPC_CHANNEL_TRANSIENT_FAILURE:
      return "TRANSIENT_FAILURE";
    case grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN:
      return "SHUTDOWN";
    default:
      return "UNKNOWN";
  }
}

/**
 * A simple class as the demo of a node state machine.
 */
class KeepAliveGreeterClient {
 public:
  KeepAliveGreeterClient(
      const std::string& server_addr,
      std::function<void(grpc_connectivity_state, grpc_connectivity_state)>
          state_change_cb)
      : m_cq_closed_(false),
        m_state_change_cb_(state_change_cb),
        m_failure_retry_times_(0) {
    // These KeepAlive args seem useless right now, so we do not merge the
    // following lines into the actual code.
    grpc::ChannelArguments channel_args;
    channel_args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 5 /*s*/ * 1000 /*ms*/);
    channel_args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 10 /*s*/ * 1000 /*ms*/);
    channel_args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1 /*true*/);

    m_channel_ = grpc::CreateCustomChannel(
        server_addr, grpc::InsecureChannelCredentials(), channel_args);

    m_last_grpc_conn_state_ = m_channel_->GetState(true);
    CRANE_INFO("Channel initial conn state: {}",
               GrpcConnStateStr(m_last_grpc_conn_state_));

    using namespace std::chrono_literals;
    m_channel_->NotifyOnStateChange(m_last_grpc_conn_state_,
                                    std::chrono::system_clock::now() + 3s,
                                    &m_cq_, nullptr);

    m_stub_ = Greeter::NewStub(m_channel_);

    m_state_monitor_thread_ =
        std::thread(&KeepAliveGreeterClient::StateMonitorThreadFunc, this);
  }

  std::string SayHello(const std::string& user) {
    HelloRequest request;
    request.set_name(user);

    HelloReply reply;

    grpc::ClientContext context;

    grpc::Status status = m_stub_->SayHello(&context, request, &reply);

    if (status.ok()) {
      return reply.message();
    } else {
      CRANE_INFO("{}:{}", status.error_code(), status.error_message());
      return "RPC failed";
    }
  }

  ~KeepAliveGreeterClient() {
    m_cq_mtx_.lock();

    m_cq_.Shutdown();
    m_cq_closed_ = true;

    m_cq_mtx_.unlock();

    m_state_monitor_thread_.join();
  }

 private:
  void StateMonitorThreadFunc() {
    CRANE_INFO("StateMonitorThread started...");

    bool ok;
    void* tag;

    while (true) {
      if (m_cq_.Next(&tag, &ok)) {
        grpc_connectivity_state new_state = m_channel_->GetState(true);

        if (m_last_grpc_conn_state_ != new_state) {
          if (m_state_change_cb_) {
            m_state_change_cb_(m_last_grpc_conn_state_, new_state);
          }
          m_last_grpc_conn_state_ = new_state;
        }

        m_cq_mtx_.lock();
        if (!m_cq_closed_)  // When cq is closed, do not register more callbacks
                            // on it.
          m_channel_->NotifyOnStateChange(m_last_grpc_conn_state_,
                                          std::chrono::system_clock::now() + 3s,
                                          &m_cq_, nullptr);
        m_cq_mtx_.unlock();
      } else
        break;
    }

    CRANE_INFO("StateMonitorThread is exiting...");
  }

  grpc_connectivity_state m_last_grpc_conn_state_;

  grpc::CompletionQueue m_cq_;
  std::mutex m_cq_mtx_;
  bool m_cq_closed_;

  uint32_t m_failure_retry_times_;

  std::thread m_state_monitor_thread_;

  std::function<void(grpc_connectivity_state, grpc_connectivity_state)>
      m_state_change_cb_;

  std::shared_ptr<Channel> m_channel_;
  std::unique_ptr<Greeter::Stub> m_stub_;
};

TEST(KeepAlive, ServerNormallyExit) {
  std::string server_addr("localhost:50022");

  auto state_change_cb = [](grpc_connectivity_state old_state,
                            grpc_connectivity_state new_state) {
    CRANE_INFO("Underlying conn state changed: {} -> {}",
               GrpcConnStateStr(old_state), GrpcConnStateStr(new_state));
  };

  auto server = std::make_unique<GreeterSyncServer>(server_addr);
  auto client =
      std::make_unique<KeepAliveGreeterClient>(server_addr, state_change_cb);

  std::string reply = client->SayHello("Riley");
  CRANE_INFO("SayHello(Riley) -> {}", reply);

  std::this_thread::sleep_for(10s);

  server->Shutdown();
  server->Wait();

  std::this_thread::sleep_for(20s);

  client.reset();
}

TEST(KeepAlive, ServerAborts) {
  std::string server_addr("localhost:50022");

  pid_t pid = fork();

  if (pid > 0) {
    auto state_change_cb = [](grpc_connectivity_state old_state,
                              grpc_connectivity_state new_state) {
      CRANE_INFO("Underlying conn state changed: {} -> {}",
                 GrpcConnStateStr(old_state), GrpcConnStateStr(new_state));
    };

    auto client =
        std::make_unique<KeepAliveGreeterClient>(server_addr, state_change_cb);

    std::this_thread::sleep_for(6s);

    std::string reply = client->SayHello("Riley");
    CRANE_INFO("SayHello(Riley) -> {}", reply);

    EXPECT_EQ(reply, "RPC failed");

    std::this_thread::sleep_for(6s);

    client.reset();

    int stat, wait_pid;
    wait_pid = wait(&stat);

    EXPECT_EQ(WIFSIGNALED(stat), 1);
    EXPECT_EQ(wait_pid, pid);
  } else {
    auto server = std::make_unique<GreeterSyncServer>(server_addr);

    std::this_thread::sleep_for(3s);

    std::abort();
  }
}

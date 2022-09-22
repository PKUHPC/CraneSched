#include <fcntl.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/util/delimited_message_util.h>
#include <grpc++/grpc++.h>
#include <gtest/gtest.h>
#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>
#include <sys/socket.h>
#include <sys/stat.h>

#include <atomic>
#include <filesystem>
#include <queue>
#include <thread>

#include "SharedTestImpl/greeter_service_impl.h"
#include "concurrentqueue/concurrentqueue.h"
#include "protos/math.grpc.pb.h"
#include "protos/math.pb.h"
#include "crane/PublicHeader.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using grpc_example::Greeter;
using grpc_example::HelloReply;
using grpc_example::HelloRequest;

class GreeterClient {
 public:
  explicit GreeterClient(std::shared_ptr<Channel> channel)
      : stub_(Greeter::NewStub(channel)) {}

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  std::string SayHello(const std::string& user) {
    // Data we are sending to the server.
    HelloRequest request;
    request.set_name(user);

    // Container for the data we expect from the server.
    HelloReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = stub_->SayHello(&context, request, &reply);

    // Act upon its status.
    if (status.ok()) {
      return reply.message();
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

 private:
  std::unique_ptr<Greeter::Stub> stub_;
};

TEST(GrpcExample, Simple) {
  GreeterSyncServer server("localhost:50051");

  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  GreeterClient greeter(grpc::CreateChannel(
      "localhost:50051", grpc::InsecureChannelCredentials()));
  std::string user("world");
  std::string reply = greeter.SayHello(user);
  SPDLOG_INFO("Greeter received: {}", reply);

  EXPECT_EQ(reply, "Hello world");

  // This method is thread-safe.
  server.Shutdown();

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server.Wait();
}

using grpc::CompletionQueue;
using grpc::ServerAsyncReaderWriter;
using grpc::ServerCompletionQueue;
using grpc_example::Math;
using grpc_example::MaxRequest;
using grpc_example::MaxResponse;

// NOTE: This is a complex example for an asynchronous, bidirectional streaming
// server.

// Most of the logic is similar to AsyncBidiGreeterClient, so follow that class
// for detailed comments. Two main differences between the server and the client
// are: (a) Server cannot initiate a connection, so it first waits for a
// 'connection'. (b) Server can handle multiple streams at the same time, so
// the completion queue/server have a longer lifetime than the client(s).
class AsyncBidiMathServer {
 public:
  AsyncBidiMathServer(std::string server_address)
      : m_server_address_(std::move(server_address)) {
    // In general avoid setting up the server in the main thread (specifically,
    // in a constructor-like function such as this). We ignore this in the
    // context of an example.
    ServerBuilder builder;
    builder.AddListeningPort(m_server_address_,
                             grpc::InsecureServerCredentials());
    builder.RegisterService(&m_async_serv_);
    m_server_cq_ = builder.AddCompletionQueue();
    m_server_ = builder.BuildAndStart();

    // Call RequestMax in the constructor.
    auto new_conn = std::make_unique<ClientConn>(
        &m_async_serv_, m_server_cq_.get(), m_next_client_index_,
        &m_to_reap_conn_queue_);
    m_client_conns_.emplace(m_next_client_index_, std::move(new_conn));
    m_next_client_index_++;

    m_server_cq_thread_ =
        std::thread(&AsyncBidiMathServer::m_server_cq_func_, this);

    m_conn_reaping_thread_ =
        std::thread(&AsyncBidiMathServer::m_conn_reap_func_, this);
  }

  void WaitStop() {
    m_thread_should_stop_.store(true, std::memory_order_release);

    m_server_->Shutdown();

    // Always shutdown the completion queue after the server.
    m_server_cq_->Shutdown();

    m_server_cq_thread_.join();
    m_conn_reaping_thread_.join();
  }

  enum class cq_tag_t : uint8_t {
    NEW_RPC_ESTAB = 0,
    WRITE,
    READ,
    SHUTDOWN,
  };

 private:
  class ClientConn;

  static constexpr size_t cq_tag_n_bit = 3;

  void m_conn_reap_func_() {
    SPDLOG_INFO("[Server] reap thread started.");
    uint64_t index;
    while (!m_thread_should_stop_.load(std::memory_order_acquire)) {
      if (!m_to_reap_conn_queue_.try_dequeue(index))
        std::this_thread::yield();
      else {
        SPDLOG_INFO("[Server] Reaping conn: {}", index);
        std::lock_guard<std::mutex> guard(m_conn_map_lock_);
        m_client_conns_.erase(index);
      }
    }
    SPDLOG_INFO("[Server] reap thread ended.");
  }

  void m_server_cq_func_() {
    SPDLOG_INFO("m_server_cq_func_ started.");

    while (true) {
      void* got_tag = nullptr;
      bool ok = false;
      if (!m_server_cq_->Next(&got_tag, &ok)) {
        SPDLOG_INFO(
            "[ServerCq] Server Completion Queue closed. Thread for server_cq "
            "is "
            "ending...");
        break;
      }

      if (ok) {
        uint64_t index = index_from_tag(got_tag);
        cq_tag_t status = status_from_tag(got_tag);

        SPDLOG_INFO(
            "[ServerCq] Client {} | Status: {}", index,
            (status == cq_tag_t::READ)
                ? "READ"
                : ((status == cq_tag_t::WRITE)
                       ? "WRITE"
                       : (status == cq_tag_t::NEW_RPC_ESTAB ? "NEW_RPC_ESTAB"
                                                            : "DONE")));

        GPR_ASSERT(status == cq_tag_t::WRITE || status == cq_tag_t::READ ||
                   status == cq_tag_t::SHUTDOWN ||
                   status == cq_tag_t::NEW_RPC_ESTAB);

        auto iter = m_client_conns_.find(index);
        if (GPR_UNLIKELY(iter == m_client_conns_.end())) {
          SPDLOG_ERROR("[ServerCq] Client {} doesn't exist!", index);
        } else {
          ClientConn* conn = iter->second.get();

          if (status == cq_tag_t::NEW_RPC_ESTAB) {
            GPR_ASSERT(index_from_tag(got_tag) == m_next_client_index_ - 1);
            GPR_ASSERT(status_from_tag(got_tag) == cq_tag_t::NEW_RPC_ESTAB);

            SPDLOG_INFO(
                "[ServerCq] RPC for client {} established. Requesting read...",
                m_next_client_index_ - 1);
            conn->RequestRead();
            conn->ConnEstablished();

            // Prepare next incoming RPC.
            auto new_conn = std::make_unique<ClientConn>(
                &m_async_serv_, m_server_cq_.get(), m_next_client_index_,
                &m_to_reap_conn_queue_);

            {
              std::lock_guard<std::mutex> guard(m_conn_map_lock_);
              m_client_conns_.emplace(m_next_client_index_,
                                      std::move(new_conn));
            }

            m_next_client_index_++;
          } else {
            SPDLOG_ERROR("[ServerCq] Unexpected status {} of Client {}!",
                         status, index);
          }
        }
      } else {
        SPDLOG_ERROR("[ServerCq] server_cq_.Next() returned with ok false!");
      }
    }
  }

  const std::string m_server_address_;
  std::unique_ptr<Server> m_server_;

  Math::AsyncService m_async_serv_ = {};

  std::unique_ptr<ServerCompletionQueue> m_server_cq_;

  std::mutex m_conn_map_lock_;
  std::unordered_map<uint64_t, std::unique_ptr<ClientConn>> m_client_conns_;

  std::thread m_server_cq_thread_;

  moodycamel::ConcurrentQueue<uint64_t> m_to_reap_conn_queue_;

  std::thread m_conn_reaping_thread_;

  std::atomic_bool m_thread_should_stop_ = false;

  uint64_t m_next_client_index_ = 0;

 public:
  static enum cq_tag_t status_from_tag(void* tag) {
    constexpr std::uintptr_t zero{};
    constexpr size_t pointer_n_bit = sizeof(std::uintptr_t) * 8;
    constexpr std::uintptr_t status_mask = (~zero)
                                           << (pointer_n_bit - cq_tag_n_bit);
    return static_cast<cq_tag_t>(
        (reinterpret_cast<uintptr_t>(tag) & status_mask) >>
        (pointer_n_bit - cq_tag_n_bit));
  }

  static uint64_t index_from_tag(void* tag) {
    constexpr std::uintptr_t zero{};
    constexpr std::uintptr_t index_mask = (~zero) >> cq_tag_n_bit;
    return reinterpret_cast<uintptr_t>(tag) & index_mask;
  }

  static void* tag_from_index_status(uint64_t index, cq_tag_t status) {
    constexpr std::uintptr_t zero{};
    constexpr size_t pointer_n_bit = sizeof(std::uintptr_t) * 8;
    std::uintptr_t status_part = zero | (static_cast<std::uintptr_t>(status)
                                         << (pointer_n_bit - cq_tag_n_bit));
    std::uintptr_t index_part =
        static_cast<std::uintptr_t>(index) & ((~zero) >> cq_tag_n_bit);

    return reinterpret_cast<void*>(status_part | index_part);
  }

 private:
  class ClientConn {
   public:
    // Take in the "service" instance (in this case representing an asynchronous
    // server) and the completion queue "cq" used for asynchronous communication
    // with the gRPC runtime.
    ClientConn(Math::AsyncService* service, ServerCompletionQueue* server_cq,
               uint64_t index,
               moodycamel::ConcurrentQueue<uint64_t>* to_reap_conn_queue_)
        : m_stream_(&m_rpc_ctx_),
          m_index_(index),
          m_to_reap_conn_queue_(to_reap_conn_queue_),
          m_initialized(false) {
      service->RequestMax(
          &m_rpc_ctx_, &m_stream_, &m_conn_cq_, server_cq,
          tag_from_index_status(index, cq_tag_t::NEW_RPC_ESTAB));

      // This is important as the server should know when the client is done.
      m_rpc_ctx_.AsyncNotifyWhenDone(
          tag_from_index_status(index, cq_tag_t::SHUTDOWN));

      m_conn_cq_thread_ = std::thread(&ClientConn::m_conn_cq_func_, this);
    }

    ~ClientConn() {
      // For those who has established the connection with the client, exit only
      // after all the pending writes have flushed.
      //
      // Also, the established connection may be ended from the client side.
      // In such case, conn_cq_thread_ will receive a SHUTDOWN tag and will call
      // m_conn_cq_.Shutdown(). The destructor shall not call any API on
      // m_stream_ and m_conn_cq_ and should wait for the conn_cq_thread_ to
      // quit directly.
      //
      // For those who hasn't established the connection, shutdown the
      // completion queue directly (m_stream is still not associated with the
      // completion queue)
      if (m_initialized && !m_cq_shutdown_called_) {
        MarkConnEnd();
        while (m_is_writing_) {
          // Wait for pending writes to be flushed.
          std::this_thread::yield();
        }

        MaxResponse resp;
        if (m_write_queue_.try_dequeue(resp)) {
          // Flush the possible trailing pending writes. See WriteFinished().
          m_stream_.Write(resp,
                          tag_from_index_status(m_index_, cq_tag_t::WRITE));
          m_is_writing_ = true;
        }
        while (m_is_writing_) {
          // Wait for pending writes to be flushed.
          std::this_thread::yield();
        }

        // Inform the conn_cq_thread_ to shutdown the queue.
        m_stream_.Finish(Status::OK,
                         tag_from_index_status(m_index_, cq_tag_t::SHUTDOWN));
      } else {
        m_conn_cq_.Shutdown();
      }
      m_conn_cq_thread_.join();
    }

    void ConnEstablished() { m_initialized = true; }

    void RequestRead() {
      if (!m_end_conn_)
        m_stream_.Read(&m_req_,
                       tag_from_index_status(m_index_, cq_tag_t::READ));
    }

    const MaxRequest& GetRequest() const { return m_req_; }

    // This function is thread-safe
    void RequestWrite(MaxResponse&& resp) {
      bool expected = false;
      if (!m_end_conn_) {
        if (m_is_writing_ ||
            !m_is_writing_.compare_exchange_strong(expected, true)) {
          m_write_queue_.enqueue(std::forward<MaxResponse>(resp));
        } else {
          // Nobody is writing and nobody is trying to read at the same time.
          m_stream_.Write(resp,
                          tag_from_index_status(m_index_, cq_tag_t::WRITE));
        }
      }
    }

    // It's ok to call this function from multiple thread more than one time.
    void MarkConnEnd() { m_end_conn_ = true; }

   private:
    void WriteFinished() {
      MaxResponse resp;
      if (m_write_queue_.try_dequeue(resp)) {
        m_stream_.Write(resp, tag_from_index_status(m_index_, cq_tag_t::WRITE));
      } else {
        // A slight chance that new pending write in enqueued before
        // is_writing is set to false. The trailing writes are handle in
        // destructor.
        m_is_writing_ = false;
      }
    }

    void m_conn_cq_func_() {
      SPDLOG_INFO("[Server | Client {}] conn_cq_thread started.", m_index_);

      while (true) {
        void* got_tag = nullptr;
        bool ok = false;
        if (!m_conn_cq_.Next(&got_tag, &ok)) {
          SPDLOG_INFO(
              "[Server | Client {}] Completion Queue has been shutdown. "
              "Exiting "
              "conn_cq_ thread...",
              m_index_);
          break;
        }

        if (ok) {
          uint64_t index = index_from_tag(got_tag);
          cq_tag_t status = status_from_tag(got_tag);

          SPDLOG_INFO(
              "[Server | Client {}] Completion Queue Received: {}", index,
              (status == cq_tag_t::READ)
                  ? "READ"
                  : ((status == cq_tag_t::WRITE)
                         ? "WRITE"
                         : (status == cq_tag_t::NEW_RPC_ESTAB ? "NEW_RPC_ESTAB"
                                                              : "SHUTDOWN")));

          if (status == cq_tag_t::READ) {
            const MaxRequest& req = GetRequest();

            if (req.a() != 0 || req.b() != 0) {
              SPDLOG_INFO(
                  "[Server | Client {}] Receive Request MAX({},{}) from ",
                  index, req.a(), req.b());

              MaxResponse resp;
              resp.set_result(std::max(req.a(), req.b()));

              RequestWrite(std::move(resp));

              // Request Next Read.
              RequestRead();
            } else {
              MarkConnEnd();
              m_to_reap_conn_queue_->enqueue(m_index_);
            }
          } else if (status == cq_tag_t::WRITE) {
            SPDLOG_INFO("[Server | Client {}] Write response to successfully.",
                        index);
            WriteFinished();
          } else if (status == cq_tag_t::SHUTDOWN) {
            SPDLOG_INFO(
                "[Server | Client {}] SHUTDOWN is called from the destructor. "
                "Stopping "
                "the completion queue...",
                index);
            m_cq_shutdown_called_ = true;
            m_conn_cq_.Shutdown();
          } else {
            SPDLOG_ERROR("[Server | Client {}] Unexpected status {}!", index,
                         status);
          }

        } else {
          SPDLOG_ERROR(
              "[Server | Client {}] CompletionQueue.Next() returned with "
              "\"ok\": "
              "false!");
        }
      }

      SPDLOG_INFO("[Server | Client {}] conn_cq_thread ended.", m_index_);
    }

    std::atomic_bool m_initialized;
    uint64_t m_index_;

    MaxRequest m_req_;

    // Context for the rpc, allowing to tweak aspects of it such as the use
    // of compression, authentication, as well as to send metadata back to the
    // client.
    ServerContext m_rpc_ctx_;

    // The context, request and m_stream_ are ready once the tag is retrieved
    // from m_cq_->Next().
    ServerAsyncReaderWriter<MaxResponse, MaxRequest> m_stream_;

    CompletionQueue m_conn_cq_;

    std::thread m_conn_cq_thread_;

    moodycamel::ConcurrentQueue<uint64_t>* m_to_reap_conn_queue_;

    // When set true, the ClientConn class will not submit any more write
    // requests and will keep flushing all pending writes.
    std::atomic_bool m_end_conn_ = false;

    // Indicate whether m_conn_cq.Shutdown() has been called. When set true,
    // NO MORE operations should be carried out on m_conn_cq_ and m_stream_.
    std::atomic_bool m_cq_shutdown_called_ = false;

    moodycamel::ConcurrentQueue<MaxResponse> m_write_queue_;
    std::atomic_bool m_is_writing_ = false;
  };
};

class BidiMathClient {
 public:
  BidiMathClient(std::shared_ptr<Channel> channel, uint64_t index)
      : m_stub_(Math::NewStub(channel)), m_index_(index) {
    m_stream_ = m_stub_->Max(&m_ctx_);
    m_recv_thread_ = std::thread(&BidiMathClient::ReceiveRespThread, this);
  }

  ~BidiMathClient() { Wait(); }

  void Wait() {
    if (m_recv_thread_.joinable()) m_recv_thread_.join();
  }

  void ReceiveRespThread() {
    SPDLOG_INFO("[Client {}] Recv Thread Started...", m_index_);

    MaxResponse resp;
    while (m_stream_->Read(&resp)) {
      SPDLOG_INFO("[Client {}] Received the response: {}", m_index_,
                  resp.result());
    }

    SPDLOG_INFO("[Client {}] Recv Thread Exiting...", m_index_);
  }

  void FindMultipleMax(const std::vector<std::pair<int32_t, int32_t>>& pairs) {
    for (auto&& pair : pairs) {
      MaxRequest req;
      req.set_a(pair.first);
      req.set_b(pair.second);
      m_stream_->Write(req);
    }

    m_stream_->Finish();
  }

 private:
  ClientContext m_ctx_;
  std::unique_ptr<grpc::ClientReaderWriter<MaxRequest, MaxResponse>> m_stream_;
  std::unique_ptr<Math::Stub> m_stub_;
  std::thread m_recv_thread_;
  uint64_t m_index_;
};

TEST(GrpcExample, BidirectionalStream) {
  using tag_t = AsyncBidiMathServer::cq_tag_t;

  void* tag = AsyncBidiMathServer::tag_from_index_status(3, tag_t::READ);
  ASSERT_EQ(AsyncBidiMathServer::index_from_tag(tag), 3);
  ASSERT_EQ(AsyncBidiMathServer::status_from_tag(tag), tag_t::READ);

  std::string server_address{"localhost:50051"};
  AsyncBidiMathServer server{server_address};

  BidiMathClient client0(
      grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()),
      0);

  BidiMathClient client1(
      grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()),
      1);

  BidiMathClient client2(
      grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()),
      2);

  std::vector<std::pair<int32_t, int32_t>> reqs_1{{1, 2},
                                                  {3, 4},
                                                  {5, 6},
                                                  {7, 8},
                                                  {0, 0}};

  client0.FindMultipleMax(reqs_1);
  client1.FindMultipleMax(reqs_1);
  client2.FindMultipleMax(reqs_1);

  client0.Wait();
  client1.Wait();
  client2.Wait();

  server.WaitStop();
}

TEST(GrpcExample, BidirectionalStream_ClientAbort) {
  std::string server_address{"localhost:50051"};

  signal(SIGCHLD, SIG_IGN);
  pid_t child_pid = fork();
  if (child_pid == 0) {
    int devNull = open("/dev/null", O_WRONLY);

    dup2(devNull, STDOUT_FILENO);
    dup2(devNull, STDERR_FILENO);

    BidiMathClient client0(
        grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()),
        0);

    abort();
  } else {
    AsyncBidiMathServer server{server_address};
    std::this_thread::sleep_for(std::chrono::seconds(1));
    server.WaitStop();
  }
}

TEST(Protobuf, InterprocessPipe) {
  using google::protobuf::io::FileInputStream;
  using google::protobuf::io::FileOutputStream;
  using google::protobuf::util::ParseDelimitedFromZeroCopyStream;
  using google::protobuf::util::SerializeDelimitedToZeroCopyStream;

  int socket_pair[2];

  if (socketpair(AF_UNIX, SOCK_DGRAM, 0, socket_pair) != 0) {
    FAIL() << fmt::format("Failed to create socket pair: {}", strerror(errno));
  }
  signal(SIGCHLD, SIG_IGN);

  std::thread t1([=] {
    int fd = socket_pair[1];
    bool ok;

    FileInputStream istream(fd);
    FileOutputStream ostream(fd);

    HelloRequest request;
    ok = ParseDelimitedFromZeroCopyStream(&request, &istream, nullptr);
    ASSERT_TRUE(ok) << "request.ParseFromZeroCopyStream(&istream)";
    GTEST_LOG_(INFO) << fmt::format("Child receive HelloRequest: {}",
                                    request.name());

    HelloReply reply;
    reply.set_message("OK");
    ok = SerializeDelimitedToZeroCopyStream(reply, &ostream);
    ASSERT_TRUE(ok) << "reply.SerializeToZeroCopyStream(&ostream)";
    ostream.Flush();
    ASSERT_TRUE(ok) << "ostream.Flush()";
  });

  {
    int fd = socket_pair[0];
    bool ok;

    FileInputStream istream(fd);
    FileOutputStream ostream(fd);

    HelloRequest request;
    request.set_name("Parent");
    ok = SerializeDelimitedToZeroCopyStream(request, &ostream);
    GTEST_LOG_(INFO) << fmt::format("ostream.ByteCount(): {}",
                                    ostream.ByteCount());
    ASSERT_TRUE(ok) << "request.SerializeToZeroCopyStream(&ostream)";
    ok = ostream.Flush();
    ASSERT_TRUE(ok) << "ostream.Flush()";

    HelloReply reply;
    ok = ParseDelimitedFromZeroCopyStream(&reply, &istream, nullptr);
    ASSERT_TRUE(ok) << "reply.ParseFromZeroCopyStream(&istream)";
    GTEST_LOG_(INFO) << fmt::format("Parent receive HelloReply: {}",
                                    reply.message());
  }

  t1.join();
}

TEST(GrpcExample, UnixSocket) {
  std::filesystem::create_directories(kDefaultCraneTempDir);

  std::string server_address =
      fmt::format("{}{}", "unix://", kDefaultCranedUnixSockPath);
  SPDLOG_INFO("Unix Server Address: {}", server_address);

  GreeterSyncServer server(server_address);

  GreeterClient greeter(
      grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()));
  std::string user("world");
  std::string reply = greeter.SayHello(user);
  SPDLOG_INFO("Greeter received: {}", reply);

  EXPECT_EQ(reply, "Hello world");

  // This method is thread-safe.
  server.Shutdown();

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server.Wait();
}
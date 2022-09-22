#include "CranedKeeper.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <boost/fiber/barrier.hpp>
#include <boost/interprocess/anonymous_shared_memory.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string_view>
#include <thread>

#include "SharedTestImpl/GlobalDefs.h"
#include "CranedServer.h"
#include "crane/BoostInterprocessBarrier.h"
#include "crane/FdFunctions.h"

using testing::_;
using testing::AnyOf;
using testing::AtLeast;
using testing::Invoke;
using testing::InvokeWithoutArgs;
using testing::Sequence;

namespace bi = boost::interprocess;

#define RED "\033[0;31m"
#define RESET "\033[0m"

/** Outdated test code.
 *  Keep it for reference.

class MockCtldServer {
 public:
  MOCK_METHOD(void, CranedIsUpCb, (CranedId, void*));
  MOCK_METHOD(void, CranedIsDownCb, (CranedId, void*));
  MOCK_METHOD(void, CranedIsTempDownCb, (CranedId, void*));
  MOCK_METHOD(void, CranedRecFromTempFailureCb, (CranedId, void*));
};

class CranedKeeperTest : public ::testing::Test {
 public:
  void SetUp() override {
    g_task_mgr = std::make_unique<Craned::TaskManager>();

    m_mock_ctld_server_ = std::make_unique<MockCtldServer>();

    m_keeper_ = std::make_unique<Ctld::CranedKeeper>();
    m_keeper_->SetCranedIsUpCb(
        std::bind(&MockCtldServer::CranedIsUpCb, m_mock_ctld_server_.get(),
                  std::placeholders::_1, std::placeholders::_2));
    m_keeper_->SetCranedIsDownCb(
        std::bind(&MockCtldServer::CranedIsDownCb, m_mock_ctld_server_.get(),
                  std::placeholders::_1, std::placeholders::_2));
    m_keeper_->SetCranedTempDownCb(std::bind(
        &MockCtldServer::CranedIsTempDownCb, m_mock_ctld_server_.get(),
        std::placeholders::_1, std::placeholders::_2));
    m_keeper_->SetCranedRecFromTempFailureCb(
        std::bind(&MockCtldServer::CranedRecFromTempFailureCb,
                  m_mock_ctld_server_.get(), std::placeholders::_1,
                  std::placeholders::_2));
  }

  void TearDown() override {
    if (m_keeper_) m_keeper_.reset();
    m_mock_ctld_server_.reset();
    g_task_mgr->Shutdown();
    g_task_mgr.reset();
  }

  std::unique_ptr<MockCtldServer> m_mock_ctld_server_;
  std::unique_ptr<Ctld::CranedKeeper> m_keeper_;
};

TEST_F(CranedKeeperTest, FailToConnect) {
  std::string server_addr{"127.0.0.1:50011"};

  AllocatableResource res;
  res.cpu_count = 10;
  res.memory_bytes = 1024 * 1024 * 1024;
  res.memory_sw_bytes = 1024 * 1024 * 1024;

  auto result_future =
      m_keeper_->RegisterCraned(server_addr, CranedId{0, 0}, &res, nullptr);
  Ctld::RegisterNodeResult result = result_future.get();
  EXPECT_EQ(result.node_id.has_value(), false);
}

TEST_F(CranedKeeperTest, OneStub_OneAbortedServer) {
  using Craned::CranedServer;
  using namespace std::chrono_literals;

  std::string server_addr{"127.0.0.1:50011"};

  AllocatableResource res;
  res.cpu_count = 10;
  res.memory_bytes = 1024 * 1024 * 1024;
  res.memory_sw_bytes = 1024 * 1024 * 1024;

  std::atomic_bool has_exited = false;

  auto xd_server = std::make_unique<CranedServer>(server_addr);
  std::this_thread::sleep_for(1s);

  ON_CALL((*m_mock_ctld_server_), CranedIsUpCb(CranedId{0, 0}, _))
      .WillByDefault(Invoke([&](CranedId node_id, void*) {
        CRANE_TRACE("Node {} is Up", node_id);
      }));

  ON_CALL((*m_mock_ctld_server_), CranedIsTempDownCb(CranedId{0, 0}, _))
      .WillByDefault(Invoke([&](CranedId node_id, void*) {
        CRANE_TRACE("Node {} is Temp Down", node_id);
      }));

  ON_CALL((*m_mock_ctld_server_), CranedIsDownCb(CranedId{0, 0}, _))
      .WillByDefault(Invoke([&](CranedId node_id, void*) {
        CRANE_TRACE("Node {} is Down", node_id);
        has_exited = true;
      }));

  Sequence seq;
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsUpCb(CranedId{0, 0}, _))
      .Times(1)
      .InSequence(seq);
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsTempDownCb(CranedId{0, 0}, _))
      .Times(1)
      .InSequence(seq);
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsDownCb(CranedId{0, 0}, _))
      .Times(1)
      .InSequence(seq);

  auto result_future =
      m_keeper_->RegisterCraned(server_addr, CranedId{0, 0}, &res, nullptr);
  result_future.wait();
  ASSERT_EQ(result_future.valid(), true);

  Ctld::RegisterNodeResult result = result_future.get();
  EXPECT_EQ(result.node_id.has_value(), true);
  if (result.node_id.has_value()) {
    EXPECT_EQ(result.node_id.value(), (CranedId{0, 0}));
  }

  std::this_thread::sleep_for(3s);
  xd_server->Shutdown();
  xd_server.reset();

  while (!has_exited) {
    std::this_thread::yield();
  }
}

// Note: we use sleep() here to provide synchronization. However, valgrind may
// slow the execution down too much and cause the test to fail.
//
// Todo: Consider to use barrier or condition variable.
//
// The server is gracefully shut down. In such case, a GOAWAY message will be
// send. READY -> IDLE transition is expected.
// e.g. Shutdown by the user command.
TEST_F(CranedKeeperTest, OneStub_OneTempDownServer) {
  using Craned::CranedServer;
  using namespace std::chrono_literals;

  std::atomic_bool has_exited = false;

  std::string server_addr{"127.0.0.1:50011"};

  AllocatableResource res;
  res.cpu_count = 10;
  res.memory_bytes = 1024 * 1024 * 1024;
  res.memory_sw_bytes = 1024 * 1024 * 1024;

  ON_CALL((*m_mock_ctld_server_), CranedIsUpCb(CranedId{0, 0}, _))
      .WillByDefault(Invoke([&](CranedId node_id, void*) {
        CRANE_TRACE("Node {} is Up", node_id);
      }));

  ON_CALL((*m_mock_ctld_server_), CranedIsTempDownCb(CranedId{0, 0}, _))
      .WillByDefault(Invoke([&](CranedId node_id, void*) {
        CRANE_TRACE("Node {} is Temp Down", node_id);
      }));

  ON_CALL((*m_mock_ctld_server_),
          CranedRecFromTempFailureCb(CranedId{0, 0}, _))
      .WillByDefault(Invoke([&](CranedId node_id, void*) {
        CRANE_TRACE("Node {} recovered from temporary failure", node_id);
      }));

  ON_CALL((*m_mock_ctld_server_), CranedIsDownCb(CranedId{0, 0}, _))
      .WillByDefault(Invoke([&](CranedId node_id, void*) {
        CRANE_TRACE("Node {} is Down", node_id);
        has_exited = true;
      }));

  Sequence seq;
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsUpCb(CranedId{0, 0}, _))
      .Times(1)
      .InSequence(seq);
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsTempDownCb(CranedId{0, 0}, _))
      .Times(1)
      .InSequence(seq);
  EXPECT_CALL((*m_mock_ctld_server_),
              CranedRecFromTempFailureCb(CranedId{0, 0}, _))
      .Times(1)
      .InSequence(seq);
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsTempDownCb(CranedId{0, 0}, _))
      .Times(1)
      .InSequence(seq);
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsDownCb(CranedId{0, 0}, _))
      .Times(1)
      .InSequence(seq);

  auto xd_server = std::make_unique<CranedServer>(server_addr);
  std::this_thread::sleep_for(1s);

  auto result_future =
      m_keeper_->RegisterCraned(server_addr, CranedId{0, 0}, &res, nullptr);
  result_future.wait();
  ASSERT_EQ(result_future.valid(), true);

  Ctld::RegisterNodeResult result = result_future.get();
  EXPECT_EQ(result.node_id.has_value(), true);
  if (result.node_id.has_value()) {
    EXPECT_EQ(result.node_id.value(), (CranedId{0, 0}));
  }

  std::this_thread::sleep_for(2s);
  xd_server->Shutdown();
  xd_server.reset();

  xd_server = std::make_unique<CranedServer>(server_addr);
  std::this_thread::sleep_for(2s);
  xd_server->Shutdown();
  xd_server.reset();

  while (!has_exited) {
    std::this_thread::yield();
  }
}

// Note: Valgrind will conflict with pthread_barrier in such an abort case.
//  It may be caused by the bugs of pthread library or this test.
//  This test SHOULD be disabled when using valgrind.
//  See https://valgrind.org/docs/manual/hg-manual.html
//
// Todo: Check whether this case is buggy on pthread_barrier misuse. (using
//  Helgrind)
//
// The server is aborted. In such case, a GOAWAY message will never be
// send.
// e.g. Shutdown by power failure.
TEST_F(CranedKeeperTest, OneStub_OneTempAbortedServer) {
  using Craned::CranedServer;
  using namespace std::chrono_literals;

  util::SetCloseOnExecFromFd(STDERR_FILENO + 1);

  std::atomic_bool has_exited = false;

  boost::fibers::barrier xd_rec_barrier(2);

  ON_CALL((*m_mock_ctld_server_), CranedIsUpCb(CranedId{0, 0}, _))
      .WillByDefault(Invoke([&](CranedId node_id, void*) {
        CRANE_TRACE("Node {} is Up", node_id);
      }));

  ON_CALL((*m_mock_ctld_server_), CranedIsTempDownCb(CranedId{0, 0}, _))
      .WillByDefault(Invoke([&](CranedId node_id, void*) {
        CRANE_TRACE("Node {} is Temp Down", node_id);
      }));

  ON_CALL((*m_mock_ctld_server_),
          CranedRecFromTempFailureCb(CranedId{0, 0}, _))
      .WillByDefault(Invoke([&](CranedId node_id, void*) {
        CRANE_TRACE("Node {} recovered from temporary failure", node_id);
        xd_rec_barrier.wait();
      }));

  ON_CALL((*m_mock_ctld_server_), CranedIsDownCb(CranedId{0, 0}, _))
      .WillByDefault(Invoke([&](CranedId node_id, void*) {
        CRANE_TRACE("Node {} is Down", node_id);
        has_exited = true;
      }));

  Sequence seq;
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsUpCb(CranedId{0, 0}, _))
      .Times(1)
      .InSequence(seq);
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsTempDownCb(CranedId{0, 0}, _))
      .Times(1)
      .InSequence(seq);
  EXPECT_CALL((*m_mock_ctld_server_),
              CranedRecFromTempFailureCb(CranedId{0, 0}, _))
      .Times(1)
      .InSequence(seq);
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsTempDownCb(CranedId{0, 0}, _))
      .Times(1)
      .InSequence(seq);
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsDownCb(CranedId{0, 0}, _))
      .Times(1)
      .InSequence(seq);

  std::string server_addr{"127.0.0.1:50011"};

  AllocatableResource res;
  res.cpu_count = 10;
  res.memory_bytes = 1024 * 1024 * 1024;
  res.memory_sw_bytes = 1024 * 1024 * 1024;

  // Start server and client connects it.
  {
    struct TestIpc {
      TestIpc() : init_barrier(2), terminate_barrier(2) {}

      bi::barrier init_barrier;
      bi::barrier terminate_barrier;
    };

    bi::mapped_region region(bi::anonymous_shared_memory(sizeof(TestIpc)));
    TestIpc* ipc = new (region.get_address()) TestIpc;

    pid_t child_pid = fork();
    if (child_pid == 0) {  // Child
      auto xd_server = std::make_unique<CranedServer>(server_addr);
      ipc->init_barrier.wait();

      ipc->terminate_barrier.wait();
      std::terminate();
    } else {
      ipc->init_barrier.wait();
      auto result_future =
          m_keeper_->RegisterCraned(server_addr, CranedId{0, 0}, &res, nullptr);
      result_future.wait();
      ASSERT_EQ(result_future.valid(), true);

      Ctld::RegisterNodeResult result = result_future.get();
      EXPECT_EQ(result.node_id.has_value(), true);
      if (result.node_id.has_value()) {
        EXPECT_EQ(result.node_id.value(), (CranedId{0, 0}));
      }

      ipc->terminate_barrier.wait();
      int stat;
      wait(&stat);
    }
    // ipc is destructed here.
  }

  // Restart server and wait for the client to reconnect.
  {
    struct TestIpc {
      TestIpc() : terminate_barrier(2) {}

      bi::barrier terminate_barrier;
    };

    bi::mapped_region region(bi::anonymous_shared_memory(sizeof(TestIpc)));
    TestIpc* ipc = new (region.get_address()) TestIpc;

    pid_t child_pid = fork();
    if (child_pid == 0) {  // Child
      auto xd_server = std::make_unique<CranedServer>(server_addr);

      (void)xd_server.get();
      ipc->terminate_barrier.wait();
      std::terminate();
    } else {
      // Wait for the client to reconnect the server.
      xd_rec_barrier.wait();

      // Let child process terminate.
      ipc->terminate_barrier.wait();

      int stat;
      wait(&stat);
    }
    // ipc is destructed here.
  }

  while (!has_exited) {
    std::this_thread::yield();
  }
}

TEST_F(CranedKeeperTest, TwoStubs_TwoTempDownServers) {
  using Craned::CranedServer;
  using namespace std::chrono_literals;
  using testing::AnyOf;

  std::string server_addr_0{"127.0.0.1:50011"};
  std::string server_addr_1{"127.0.0.1:50012"};

  AllocatableResource res;
  res.cpu_count = 10;
  res.memory_bytes = 1024 * 1024 * 1024;
  res.memory_sw_bytes = 1024 * 1024 * 1024;

  std::unique_ptr<boost::fibers::barrier> init_barrier_0;
  std::unique_ptr<boost::fibers::barrier> terminate_barrier_0;
  std::unique_ptr<boost::fibers::barrier> terminate_barrier_1;
  std::unique_ptr<boost::fibers::barrier> exit_barrier_all;

  std::atomic_uint disconnected_count = 0;
  std::atomic_uint exit_count = 0;

  init_barrier_0 = std::make_unique<boost::fibers::barrier>(2);
  terminate_barrier_0 = std::make_unique<boost::fibers::barrier>(2);
  terminate_barrier_1 = std::make_unique<boost::fibers::barrier>(2);

  ON_CALL((*m_mock_ctld_server_),
          CranedIsUpCb(AnyOf(CranedId{0, 0}, CranedId{0, 1}), _))
      .WillByDefault(Invoke([&](CranedId node_id, void*) {
        CRANE_TRACE(RED "Node {} is Up" RESET, node_id);

        if (node_id == CranedId{0, 0}) {
          terminate_barrier_0->wait();
        } else {
          terminate_barrier_1->wait();
        }
      }));

  ON_CALL((*m_mock_ctld_server_),
          CranedIsTempDownCb(AnyOf(CranedId{0, 0}, CranedId{0, 1}), _))
      .WillByDefault(Invoke([&](CranedId node_id, void*) {
        CRANE_TRACE(RED "Node {} is Temp Down" RESET, node_id);
        disconnected_count++;
      }));

  ON_CALL((*m_mock_ctld_server_),
          CranedRecFromTempFailureCb(CranedId{0, 0}, _))
      .WillByDefault(Invoke([&](CranedId node_id, void*) {
        CRANE_TRACE(RED "Node {} recovered from temporary failure" RESET,
                     node_id);
        terminate_barrier_0->wait();
      }));

  ON_CALL((*m_mock_ctld_server_),
          CranedIsDownCb(AnyOf(CranedId{0, 0}, CranedId{0, 1}), _))
      .WillByDefault(Invoke([&](CranedId node_id, void*) {
        CRANE_TRACE(RED "Node {} is Down" RESET, node_id);
        exit_count++;
      }));

  Sequence seq_0;
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsUpCb(CranedId{0, 0}, _))
      .Times(1)
      .InSequence(seq_0);
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsTempDownCb(CranedId{0, 0}, _))
      .Times(1)
      .InSequence(seq_0);
  EXPECT_CALL((*m_mock_ctld_server_),
              CranedRecFromTempFailureCb(CranedId{0, 0}, _))
      .Times(1)
      .InSequence(seq_0);
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsTempDownCb(CranedId{0, 0}, _))
      .Times(1)
      .InSequence(seq_0);
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsDownCb(CranedId{0, 0}, _))
      .Times(1)
      .InSequence(seq_0);

  Sequence seq_1;
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsUpCb(CranedId{0, 1}, _))
      .Times(1)
      .InSequence(seq_1);
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsTempDownCb(CranedId{0, 1}, _))
      .Times(1)
      .InSequence(seq_1);
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsDownCb(CranedId{0, 1}, _))
      .Times(1)
      .InSequence(seq_1);

  std::thread t0([&] {
    auto xd_server = std::make_unique<CranedServer>(server_addr_0);
    init_barrier_0->wait();

    // Wait for client stub 0 to connect.
    terminate_barrier_0->wait();
    xd_server->Shutdown();
  });

  // Wait for server 0 initialization.
  init_barrier_0->wait();

  auto result_future =
      m_keeper_->RegisterCraned(server_addr_0, CranedId{0, 0}, &res, nullptr);
  result_future.wait();
  ASSERT_EQ(result_future.valid(), true);

  // Wait for Xd Node 0 registration result.
  Ctld::RegisterNodeResult result = result_future.get();
  EXPECT_EQ(result.node_id.has_value(), true);
  if (result.node_id.has_value()) {
    EXPECT_EQ(result.node_id.value(), (CranedId{0, 0}));
  }

  t0.join();

  std::thread t1([&] {
    auto xd_server = std::make_unique<CranedServer>(server_addr_1);

    // Wait for client stub 1 to connect.
    terminate_barrier_1->wait();
    xd_server->Shutdown();
  });

  result_future =
      m_keeper_->RegisterCraned(server_addr_1, CranedId{0, 1}, &res, nullptr);
  result_future.wait();
  ASSERT_EQ(result_future.valid(), true);

  // Wait for Xd Node 1 registration result.
  result = result_future.get();
  EXPECT_EQ(result.node_id.has_value(), true);
  if (result.node_id.has_value()) {
    EXPECT_EQ(result.node_id.value(), (CranedId{0, 1}));
  }

  t1.join();

  // Wait for Xd Node 0,1 to encounter temporary failure.
  while (disconnected_count < 2) std::this_thread::yield();
  terminate_barrier_0 = std::make_unique<boost::fibers::barrier>(2);

  std::thread t0_restart([&] {
    auto xd_server = std::make_unique<CranedServer>(server_addr_0);

    // Wait for client stub 0 to re-connect.
    terminate_barrier_0->wait();
    xd_server->Shutdown();
  });
  t0_restart.join();

  while (exit_count < 2) std::this_thread::yield();
}

TEST_F(CranedKeeperTest, CheckReuseOfSlot) {
  using Craned::CranedServer;
  using namespace std::chrono_literals;

  std::string server_addr_0{"127.0.0.1:50011"};
  std::string server_addr_1{"127.0.0.1:50012"};
  std::string server_addr_2{"127.0.0.1:50013"};

  AllocatableResource res;
  res.cpu_count = 10;
  res.memory_bytes = 1024 * 1024 * 1024;
  res.memory_sw_bytes = 1024 * 1024 * 1024;

  std::vector<std::unique_ptr<boost::fibers::barrier>> terminate_barriers;
  for (int i = 0; i < 3; i++)
    terminate_barriers.emplace_back(
        std::make_unique<boost::fibers::barrier>(2));

  auto restart_barrier_1 = std::make_unique<boost::fibers::barrier>(2);
  bool has_restarted_1 = false;
  uint start_count = 0;

  std::atomic_uint exit_count = 0;

  ON_CALL((*m_mock_ctld_server_), CranedIsUpCb(_, _))
      .WillByDefault(Invoke([&](CranedId node_id, void*) {
        ASSERT_THAT(node_id,
                    AnyOf(CranedId{0, 0}, CranedId{0, 1}, CranedId{0, 2}));

        CRANE_TRACE(RED "Node {} is Up" RESET, node_id);

        start_count++;
        if (start_count >= 3 && !has_restarted_1) {
          CRANE_TRACE(RED "Terminate Node #1 ..." RESET);
          terminate_barriers[1]->wait();
        } else if (has_restarted_1) {
          for (auto&& i : {0, 1, 2}) terminate_barriers[i]->wait();
        }
      }));

  ON_CALL((*m_mock_ctld_server_), CranedIsTempDownCb(_, _))
      .WillByDefault(Invoke([&](CranedId node_id, void*) {
        ASSERT_THAT(node_id,
                    AnyOf(CranedId{0, 0}, CranedId{0, 1}, CranedId{0, 2}));
        CRANE_TRACE(RED "Node {} is Temp Down" RESET, node_id);
      }));

  ON_CALL((*m_mock_ctld_server_), CranedRecFromTempFailureCb(_, _))
      .WillByDefault(Invoke([&](CranedId node_id, void*) {
        ASSERT_THAT(node_id,
                    AnyOf(CranedId{0, 0}, CranedId{0, 1}, CranedId{0, 2}));
        CRANE_TRACE(RED "Node {} recovered from temporary failure" RESET,
                     node_id);
      }));

  ON_CALL((*m_mock_ctld_server_), CranedIsDownCb(_, _))
      .WillByDefault(Invoke([&](CranedId node_id, void*) {
        ASSERT_THAT(node_id,
                    AnyOf(CranedId{0, 0}, CranedId{0, 1}, CranedId{0, 2}));
        CRANE_TRACE(RED "Node #{} is Down" RESET, node_id);
        if (!has_restarted_1 && node_id == CranedId{0, 1}) {
          CRANE_TRACE(RED "Restarting Node (0,1) ..." RESET);
          restart_barrier_1->wait();
          has_restarted_1 = true;
        } else if (has_restarted_1) {
          exit_count++;
        }
      }));

  Sequence seq_0;
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsUpCb(CranedId{0, 0}, _))
      .Times(1)
      .InSequence(seq_0);
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsTempDownCb(CranedId{0, 0}, _))
      .Times(1)
      .InSequence(seq_0);
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsDownCb(CranedId{0, 0}, _))
      .Times(1)
      .InSequence(seq_0);

  Sequence seq_2;
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsUpCb(CranedId{0, 2}, _))
      .Times(1)
      .InSequence(seq_2);
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsTempDownCb(CranedId{0, 2}, _))
      .Times(1)
      .InSequence(seq_2);
  EXPECT_CALL((*m_mock_ctld_server_), CranedIsDownCb(CranedId{0, 2}, _))
      .Times(1)
      .InSequence(seq_2);

  Sequence seq_1;
  for (int i = 0; i < 2; i++) {
    EXPECT_CALL((*m_mock_ctld_server_), CranedIsUpCb(CranedId{0, 1}, _))
        .Times(1)
        .InSequence(seq_1);
    EXPECT_CALL((*m_mock_ctld_server_), CranedIsTempDownCb(CranedId{0, 1}, _))
        .Times(1)
        .InSequence(seq_1);
    EXPECT_CALL((*m_mock_ctld_server_), CranedIsDownCb(CranedId{0, 1}, _))
        .Times(1)
        .InSequence(seq_1);
  }

  std::thread t0([&] {
    auto xd_server = std::make_unique<CranedServer>(server_addr_0);
    terminate_barriers[0]->wait();
    xd_server->Shutdown();
  });

  // Server 0 and 2 serve as the slot occupier and they will be shut down at the
  // same time.
  std::thread t2([&] {
    auto xd_server = std::make_unique<CranedServer>(server_addr_2);
    terminate_barriers[2]->wait();
    xd_server->Shutdown();
  });

  std::thread t1;
  t1 = std::thread([&] {
    auto xd_server = std::make_unique<CranedServer>(server_addr_1);
    terminate_barriers[1]->wait();
    xd_server->Shutdown();
  });

  std::future<Ctld::RegisterNodeResult> result_future;
  Ctld::RegisterNodeResult result;

  result_future =
      m_keeper_->RegisterCraned(server_addr_0, CranedId{0, 0}, &res, nullptr);
  result_future.wait();
  ASSERT_EQ(result_future.valid(), true);

  result = result_future.get();
  EXPECT_EQ(result.node_id.has_value(), true);
  if (result.node_id.has_value()) {
    EXPECT_EQ(result.node_id.value(), (CranedId{0, 0}));
  }

  result_future =
      m_keeper_->RegisterCraned(server_addr_1, CranedId{0, 1}, &res, nullptr);
  result_future.wait();
  ASSERT_EQ(result_future.valid(), true);

  result = result_future.get();
  EXPECT_EQ(result.node_id.has_value(), true);
  if (result.node_id.has_value()) {
    EXPECT_EQ(result.node_id.value(), (CranedId{0, 1}));
  }

  result_future =
      m_keeper_->RegisterCraned(server_addr_2, CranedId{0, 2}, &res, nullptr);
  result_future.wait();
  ASSERT_EQ(result_future.valid(), true);

  result = result_future.get();
  EXPECT_EQ(result.node_id.has_value(), true);
  if (result.node_id.has_value()) {
    EXPECT_EQ(result.node_id.value(), (CranedId{0, 2}));
  }

  t1.join();
  restart_barrier_1->wait();

  terminate_barriers[1] = std::make_unique<boost::fibers::barrier>(2);
  t1 = std::thread([&] {
    auto xd_server = std::make_unique<CranedServer>(server_addr_1);
    terminate_barriers[1]->wait();
    xd_server->Shutdown();
  });

  result_future =
      m_keeper_->RegisterCraned(server_addr_1, CranedId{0, 1}, &res, nullptr);
  result_future.wait();
  ASSERT_EQ(result_future.valid(), true);

  result = result_future.get();
  EXPECT_EQ(result.node_id.has_value(), true);
  if (result.node_id.has_value()) {
    EXPECT_EQ(result.node_id.value(), (CranedId{0, 1}));
  }

  while (exit_count < 3) {
    std::this_thread::yield();
  }

  t0.join();
  t1.join();
  t2.join();
}

 */
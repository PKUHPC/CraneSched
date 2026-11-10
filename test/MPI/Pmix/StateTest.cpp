/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <barrier>
#include <thread>

#include "Callbacks.h"
#include "Collective/CollectiveManager.h"
#include "Modex/ModexManager.h"
#include "Server.h"
#include "Transport/PmixClient.h"

namespace {
class Stub final : public pmix::PmixStub {
 public:
  crane::grpc::mpi::pmix::PmixDModexRequestReq request;
  pmix::AsyncCallback completion;
  void PmixDModexRequestNoBlock(
      const crane::grpc::mpi::pmix::PmixDModexRequestReq& value,
      pmix::AsyncCallback callback) override {
    request = value;
    completion = std::move(callback);
  }
  void SendPmixRingMsgNoBlock(const crane::grpc::mpi::pmix::SendPmixRingMsgReq&,
                              pmix::AsyncCallback callback) override {
    callback(true);
  }
  void PmixTreeUpwardForwardNoBlock(
      const crane::grpc::mpi::pmix::PmixTreeUpwardForwardReq&,
      pmix::AsyncCallback callback) override {
    callback(true);
  }
  void PmixTreeDownwardForwardNoBlock(
      const crane::grpc::mpi::pmix::PmixTreeDownwardForwardReq&,
      pmix::AsyncCallback callback) override {
    callback(true);
  }
  void PmixDModexResponseNoBlock(
      const crane::grpc::mpi::pmix::PmixDModexResponseReq&,
      pmix::AsyncCallback callback) override {
    callback(true);
  }
};
class Transport final : public pmix::PmixClient {
 public:
  std::shared_ptr<Stub> stub = std::make_shared<Stub>();
  void EmplacePmixStub(const CranedId&, const std::string&) override {}
  std::shared_ptr<pmix::PmixStub> GetPmixStub(const CranedId&) override {
    return stub;
  }
  uint64_t GetChannelCount() const override { return 1; }
  bool WaitAllStubReady() override { return true; }
};
pmix::StepInfo Step() {
  pmix::StepInfo step;
  step.nspace = "42.3";
  step.craned_id = "node0";
  step.node_list = {"node0", "node1"};
  step.task_map = {0, 1};
  step.task_num = 2;
  step.node_id = 0;
  return step;
}
struct Completion {
  int calls{0};
  pmix_status_t status{};
  std::string data;
  std::function<void()> reenter;
  static void Callback(pmix_status_t status, const char* data, size_t size,
                       void* context, pmix_release_cbfunc_t release,
                       void* release_data) {
    auto& completion = *static_cast<Completion*>(context);
    ++completion.calls;
    completion.status = status;
    if (data) completion.data.assign(data, size);
    if (completion.reenter) completion.reenter();
    if (release) release(release_data);
  }
};
TEST(PmixModex, ResponseCompletesOnceAndPreservesBinaryPayload) {
  Transport transport;
  Completion completion;
  pmix::ModexManager manager(Step(), &transport, std::chrono::seconds(5));
  ASSERT_TRUE(manager.Get("42.3", 1, Completion::Callback, &completion));
  auto seq = transport.stub->request.seq_num();
  manager.HandleResponse(seq, "outsider", "wrong", PMIX_SUCCESS);
  EXPECT_EQ(completion.calls, 0);
  const std::string value("a\0b", 3);
  manager.HandleResponse(seq, "node1", value, PMIX_SUCCESS);
  manager.HandleResponse(seq, "node1", value, PMIX_SUCCESS);
  transport.stub->completion(false);
  EXPECT_EQ(completion.calls, 1);
  EXPECT_EQ(completion.data, value);
}
TEST(PmixModex, TimeoutCallbackCanReenterAndLateFailureDoesNotCompleteAgain) {
  Transport transport;
  Completion completion;
  pmix::ModexManager manager(Step(), &transport, std::chrono::seconds::zero());
  completion.reenter = [&] { manager.CleanupTimeoutRequests(); };
  ASSERT_TRUE(manager.Get("42.3", 1, Completion::Callback, &completion));
  manager.CleanupTimeoutRequests();
  transport.stub->completion(false);
  EXPECT_EQ(completion.calls, 1);
  EXPECT_EQ(completion.status, PMIX_ERR_TIMEOUT);
}
TEST(PmixModex, TransportFailureCompletesOnceBeforeAnyLateResponse) {
  Transport transport;
  Completion completion;
  pmix::ModexManager manager(Step(), &transport, std::chrono::seconds(5));
  ASSERT_TRUE(manager.Get("42.3", 1, Completion::Callback, &completion));
  transport.stub->completion(false);
  manager.HandleResponse(transport.stub->request.seq_num(), "node1", "late",
                         PMIX_SUCCESS);
  EXPECT_EQ(completion.calls, 1);
  EXPECT_EQ(completion.status, PMIX_ERROR);
}
TEST(PmixModex, DestructorDrainsAndLateTransportCallbackDoesNotAccessManager) {
  Transport transport;
  Completion completion;
  {
    pmix::ModexManager manager(Step(), &transport, std::chrono::seconds(5));
    ASSERT_TRUE(manager.Get("42.3", 1, Completion::Callback, &completion));
  }
  transport.stub->completion(false);
  EXPECT_EQ(completion.calls, 1);
}
TEST(PmixModex, RejectedRequestDoesNotTransferCallbackOwnership) {
  Transport transport;
  Completion completion;
  pmix::ModexManager manager(Step(), &transport, std::chrono::seconds(5));
  EXPECT_FALSE(manager.Get("other", 1, Completion::Callback, &completion));
  EXPECT_FALSE(manager.Get("42.3", -1, Completion::Callback, &completion));
  EXPECT_FALSE(manager.Get("42.3", 3, Completion::Callback, &completion));
  manager.DrainAllRequests();
  EXPECT_FALSE(manager.Get("42.3", 1, Completion::Callback, &completion));
  EXPECT_EQ(completion.calls, 0);
}
TEST(PmixModex, ConcurrentResponseTimeoutAndFailureCompleteOnce) {
  for (int attempt = 0; attempt < 50; ++attempt) {
    Transport transport;
    std::atomic<int> calls{0};
    pmix::ModexManager manager(Step(), &transport,
                               std::chrono::seconds::zero());
    auto callback = [](pmix_status_t, const char*, size_t, void* context,
                       pmix_release_cbfunc_t release, void* release_data) {
      ++*static_cast<std::atomic<int>*>(context);
      if (release) release(release_data);
    };
    ASSERT_TRUE(manager.Get("42.3", 1, callback, &calls));
    const auto sequence = transport.stub->request.seq_num();
    std::barrier start(4);
    {
      std::jthread response([&] {
        start.arrive_and_wait();
        manager.HandleResponse(sequence, "node1", "value", PMIX_SUCCESS);
      });
      std::jthread timeout([&] {
        start.arrive_and_wait();
        manager.CleanupTimeoutRequests();
      });
      std::jthread failure([&] {
        start.arrive_and_wait();
        transport.stub->completion(false);
      });
      start.arrive_and_wait();
    }
    manager.DrainAllRequests();
    EXPECT_EQ(calls.load(), 1);
  }
}
TEST(PmixModex, ServerRejectsInvalidRequestsWithoutCallingCompletion) {
  Completion completion;
  pmix::PmixServer server;
  EXPECT_EQ(
      server.DModex(nullptr, nullptr, 0, Completion::Callback, &completion),
      PMIX_ERR_BAD_PARAM);
  pmix_proc_t process;
  PMIX_LOAD_PROCID(&process, "42.3", 1);
  EXPECT_EQ(
      server.DModex(&process, nullptr, 0, Completion::Callback, &completion),
      PMIX_ERR_INIT);
  EXPECT_EQ(completion.calls, 0);
}
TEST(PmixCallbacks, MissingServerRejectsWithoutTakingCallbackOwnership) {
  ASSERT_EQ(pmix::PmixServer::GetInstance(), nullptr);
  auto* module = pmix::PmixServerCallbacks::Module();
  Completion completion;
  auto operation_callback = [](pmix_status_t, void* context) {
    ++static_cast<Completion*>(context)->calls;
  };
  EXPECT_EQ(module->abort(nullptr, nullptr, 1, "abort", nullptr, 0,
                          operation_callback, &completion),
            PMIX_ERR_INIT);
  EXPECT_EQ(module->fence_nb(nullptr, 0, nullptr, 0, nullptr, 0,
                             Completion::Callback, &completion),
            PMIX_ERR_INIT);
  EXPECT_EQ(module->direct_modex(nullptr, nullptr, 0, Completion::Callback,
                                 &completion),
            PMIX_ERR_INIT);
  EXPECT_EQ(completion.calls, 0);
}
TEST(PmixCollective, AbortPreventsLateMessagesFromRecreatingState) {
  Transport transport;
  auto step = Step();
  pmix::CollectiveManager manager(step, &transport, nullptr);
  pmix_proc_t process;
  PMIX_LOAD_PROCID(&process, "42.3", PMIX_RANK_WILDCARD);
  std::vector<pmix_proc_t> processes{process};
  EXPECT_TRUE(manager.GetCollective(pmix::CollectiveType::kTree, processes));
  manager.AbortAll();
  EXPECT_FALSE(manager.GetCollective(pmix::CollectiveType::kTree, processes));
  EXPECT_FALSE(manager.GetCollective(pmix::CollectiveType::kRing, processes));
}
}  // namespace

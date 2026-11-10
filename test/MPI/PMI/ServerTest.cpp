/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#include <fcntl.h>
#include <gtest/gtest.h>
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "Protocol/Pmi1Codec.h"
#include "Server/Server.h"
#include "protos/Crane.grpc.pb.h"

extern char** environ;

namespace {
using namespace std::chrono_literals;

// Spawn without running allocation/setenv after fork: gRPC already has threads
// whose allocator and environment locks must not be inherited by test code.
class Clients {
 public:
  ~Clients() {
    for (auto pid : pids_) {
      kill(pid, SIGKILL);
      waitpid(pid, nullptr, 0);
    }
  }
  bool Start(mpi::LaunchInfo launch, const char* mapping,
             const char* mode = "normal",
             const char* client = PMI2_TEST_CLIENT) {
    constexpr int kClientFd = 198;
    launch.env["PMI_FD"] = std::to_string(kClientFd);
    launch.env["EXPECTED_MAPPING"] = mapping;
    std::vector<std::string> env;
    for (char** e = environ; *e; ++e) {
      std::string entry(*e);
      if (!launch.env.contains(entry.substr(0, entry.find('='))))
        env.push_back(std::move(entry));
    }
    for (const auto& [key, value] : launch.env)
      env.push_back(key + '=' + value);
    std::vector<char*> envp;
    for (auto& entry : env) envp.push_back(entry.data());
    envp.push_back(nullptr);
    posix_spawn_file_actions_t actions;
    if (posix_spawn_file_actions_init(&actions) != 0) return false;
    int rc = posix_spawn_file_actions_adddup2(&actions, launch.fd, kClientFd);
    char* argv[] = {const_cast<char*>(client), const_cast<char*>(mode),
                    nullptr};
    pid_t pid;
    if (rc == 0)
      rc = posix_spawn(&pid, client, &actions, nullptr, argv, envp.data());
    posix_spawn_file_actions_destroy(&actions);
    close(launch.fd);
    if (rc != 0) return false;
    pids_.push_back(pid);
    return true;
  }
  void ExpectExit(int expected = 0) {
    auto deadline = std::chrono::steady_clock::now() + 10s;
    for (auto it = pids_.begin(); it != pids_.end();) {
      int status{};
      auto pid = waitpid(*it, &status, WNOHANG);
      if (pid == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(10ms);
        continue;
      }
      ASSERT_EQ(pid, *it) << "PMI-2 client did not finish in time";
      it = pids_.erase(it);
      EXPECT_TRUE(WIFEXITED(status));
      if (WIFEXITED(status)) EXPECT_EQ(WEXITSTATUS(status), expected);
    }
  }
  void ExpectFailure() {
    auto deadline = std::chrono::steady_clock::now() + 10s;
    for (auto it = pids_.begin(); it != pids_.end();) {
      int status{};
      auto pid = waitpid(*it, &status, WNOHANG);
      if (pid == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(10ms);
        continue;
      }
      ASSERT_EQ(pid, *it) << "PMI client did not fail in time";
      it = pids_.erase(it);
      ASSERT_TRUE(WIFEXITED(status));
      EXPECT_NE(WEXITSTATUS(status), 0);
    }
  }

 private:
  std::vector<pid_t> pids_;
};

mpi::Config Config() {
  mpi::Config config;
  config.CranedIdOfThisNode = "node0";
  config.CranedIdToNodeHostname = {{"node0", "localhost"}};
  config.CranedIdToNodeAddr = {{"node0", "127.0.0.1"}};
  return config;
}
crane::grpc::StepToD Step(uint32_t id = 3) {
  crane::grpc::StepToD step;
  step.set_job_id(42);
  step.set_step_id(id);
  step.add_nodelist("node0");
  step.add_task_node_list(0);
  return step;
}
bool WaitFor(const std::atomic<unsigned>& count, unsigned expected) {
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (count.load() < expected && std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(10ms);
  return count.load() == expected;
}
std::string Read(int fd) {
  pollfd pfd{fd, POLLIN, 0};
  if (poll(&pfd, 1, 2000) <= 0) return {};
  char buf[4096];
  auto n = recv(fd, buf, sizeof(buf), 0);
  return n > 0 ? std::string(buf, n) : std::string{};
}
void Send(int fd, std::string_view message) {
  ASSERT_EQ(send(fd, message.data(), message.size(), MSG_NOSIGNAL),
            message.size());
}
}  // namespace

TEST(Pmi1Codec, ParsesAndEncodesLinesAndRejectsMalformedFields) {
  auto fields =
      pmi::pmi1::Codec::ParseLine("cmd=put kvsname=42.3 key=rank value=0\n");
  ASSERT_TRUE(fields);
  EXPECT_EQ(fields->at("cmd"), "put");
  EXPECT_EQ(fields->at("value"), "0");
  auto line = pmi::pmi1::Codec::EncodeLine("put_result",
                                           {{"rc", "0"}, {"value", "a=b"}});
  ASSERT_TRUE(line);
  EXPECT_EQ(*line, "cmd=put_result rc=0 value=a=b\n");
  EXPECT_FALSE(pmi::pmi1::Codec::ParseLine("cmd=put key=x key=y value=z\n"));
  EXPECT_FALSE(pmi::pmi1::Codec::ParseLine("cmd=put key\n"));
  EXPECT_FALSE(pmi::pmi1::Codec::ParseLine("cmd=put key=x\nextra\n"));
}

TEST(Pmi1Codec, PreservesTerminalKvsValuesAndEscapesReplies) {
  // Open MPI s1 replaces base64 padding with spaces before appending '-'.
  auto fields = pmi::pmi1::Codec::ParseLine(
      "cmd=put kvsname=42.3 key=modex value=cG1peA  -\n");
  ASSERT_TRUE(fields);
  EXPECT_EQ(fields->at("value"), "cG1peA  -");
  auto reply = pmi::pmi1::Codec::EncodeLine(
      "get_result", {{"rc", "0"}, {"value", "cG1peA  -"}});
  ASSERT_TRUE(reply);
  EXPECT_EQ(*reply, "cmd=get_result rc=0 value=cG1peA\\ \\ -\n");
  auto decoded = pmi::pmi1::Codec::ParseLine(*reply);
  ASSERT_TRUE(decoded);
  EXPECT_EQ(decoded->at("value"), fields->at("value"));
  EXPECT_FALSE(pmi::pmi1::Codec::ParseLine(
      "cmd=put key=x value=" + std::string(1024, ' ') + "\n"));
  EXPECT_FALSE(pmi::pmi1::Codec::ParseLine("cmd=put key=x value=a\rb\n"));
}

TEST(Pmi2Server, RealClientInitializesExchangesDataAndFinalizes) {
  pmi::Server server;
  ASSERT_TRUE(server.Init(Config(), Step(), {}, 3s));
  auto launch = server.PrepareTask(0);
  ASSERT_TRUE(launch);
  EXPECT_NE(fcntl(launch->fd, F_GETFD) & FD_CLOEXEC, 0);
  EXPECT_EQ(launch->env.at("PMI_JOBID"), "42.3");
  Clients clients;
  ASSERT_TRUE(clients.Start(std::move(*launch), "(vector,(0,1,1))"));
  clients.ExpectExit();
  server.TaskExited(0);
}

TEST(Pmi2Server, Pmi1ClientInitializesAndExchangesKvsAcrossRanks) {
  auto step = Step(8);
  step.add_task_node_list(0);
  step.add_task_node_list(0);
  pmi::Server server;
  ASSERT_TRUE(server.Init(Config(), step, {}, 3s));
  Clients clients;
  for (uint32_t rank = 0; rank < 3; ++rank) {
    auto launch = server.PrepareTask(rank);
    ASSERT_TRUE(launch);
    ASSERT_TRUE(clients.Start(std::move(*launch), "unused", "normal",
                              PMI1_TEST_CLIENT));
  }
  clients.ExpectExit();
  for (uint32_t rank = 0; rank < 3; ++rank) server.TaskExited(rank);
}

TEST(Pmi2Server, RejectsMixedProtocolsOnOneNode) {
  auto step = Step(9);
  step.add_task_node_list(0);
  std::atomic<unsigned> failures{};
  pmi::Server server;
  ASSERT_TRUE(server.Init(Config(), step, [&] { ++failures; }, 3s));
  auto pmi1_launch = server.PrepareTask(0);
  auto pmi2_launch = server.PrepareTask(1);
  ASSERT_TRUE(pmi1_launch);
  ASSERT_TRUE(pmi2_launch);
  Send(pmi1_launch->fd, "cmd=init pmi_version=1 pmi_subversion=1\n");
  Send(pmi2_launch->fd, "cmd=init pmi_version=2 pmi_subversion=0\n");
  EXPECT_TRUE(WaitFor(failures, 1));
}

TEST(Pmi2Server, RejectsMixedProtocolsAcrossNodesAtFence) {
  auto step = Step(10);
  step.add_nodelist("node1");
  step.add_task_node_list(1);
  mpi::Config config = Config();
  config.CranedIdToNodeHostname["node1"] = "localhost";
  config.CranedIdToNodeAddr["node1"] = "127.0.0.1";
  std::vector<std::unique_ptr<pmi::Server>> servers;
  for (const auto& id : step.nodelist()) {
    config.CranedIdOfThisNode = id;
    auto server = std::make_unique<pmi::Server>();
    ASSERT_TRUE(server->Init(config, step, {}, 3s));
    servers.push_back(std::move(server));
  }
  for (size_t i = 0; i < servers.size(); ++i) {
    std::vector<std::pair<CranedId, std::string>> peers;
    for (size_t j = 0; j < servers.size(); ++j)
      if (i != j) peers.emplace_back(step.nodelist(j), servers[j]->Port());
    ASSERT_TRUE(servers[i]->ReceivePeers(peers));
  }
  auto pmi1_launch = servers[0]->PrepareTask(0);
  auto pmi2_launch = servers[1]->PrepareTask(1);
  ASSERT_TRUE(pmi1_launch);
  ASSERT_TRUE(pmi2_launch);
  Clients clients;
  ASSERT_TRUE(clients.Start(std::move(*pmi1_launch), "unused", "normal",
                            PMI1_TEST_CLIENT));
  ASSERT_TRUE(
      clients.Start(std::move(*pmi2_launch), "(vector,(0,1,1),(1,1,1))"));
  clients.ExpectFailure();
}

TEST(Pmi2Runtime, AnnouncesEndpointThroughCranedUnixSocket) {
  struct Craned final : crane::grpc::Craned::Service {
    std::filesystem::path directory;
    std::unique_ptr<grpc::Server> server;
    std::atomic<bool> announced{false};
    ~Craned() override {
      if (server) server->Shutdown();
      std::error_code error;
      if (!directory.empty()) std::filesystem::remove_all(directory, error);
    }
    grpc::Status BroadcastMpiEndpoint(
        grpc::ServerContext*,
        const crane::grpc::mpi::BroadcastMpiEndpointRequest* request,
        crane::grpc::mpi::BroadcastMpiEndpointReply* reply) override {
      bool valid = request->job_id() == 42 && request->step_id() == 3 &&
                   request->endpoint().craned_id() == "node0" &&
                   request->protocol() == crane::grpc::mpi::PROTOCOL_PMI &&
                   request->craned_ids_size() == 2 &&
                   !request->endpoint().address().empty();
      announced = valid;
      reply->set_ok(valid);
      return grpc::Status::OK;
    }
  } craned;
  char directory[] = "/tmp/crane-pmi2-runtime-XXXXXX";
  ASSERT_NE(mkdtemp(directory), nullptr);
  craned.directory = directory;
  auto config = Config();
  config.CranedUnixSocketPath = craned.directory / "craned.sock";
  grpc::ServerBuilder builder;
  builder.AddListeningPort("unix://" + config.CranedUnixSocketPath.string(),
                           grpc::InsecureServerCredentials());
  builder.RegisterService(&craned);
  craned.server = builder.BuildAndStart();
  ASSERT_TRUE(craned.server);
  auto step = Step();
  step.add_nodelist("node1");
  step.add_task_node_list(1);
  auto runtime = mpi::CreateRuntime("pmi2");
  ASSERT_TRUE(runtime);
  ASSERT_TRUE(runtime->Init(config, step));
  EXPECT_TRUE(craned.announced);
}

TEST(Pmi2Server, ThreeNodeTreeExchangesNonuniformRankData) {
  const std::vector<uint32_t> rank_nodes{0, 1, 2, 0};
  crane::grpc::StepToD step;
  step.set_job_id(43);
  step.set_step_id(4);
  for (const auto& id : {"node0", "node1", "node2"}) step.add_nodelist(id);
  for (auto node : rank_nodes) step.add_task_node_list(node);
  mpi::Config config;
  for (const auto& id : step.nodelist()) {
    config.CranedIdToNodeHostname[id] = "localhost";
    config.CranedIdToNodeAddr[id] = "127.0.0.1";
  }
  std::vector<std::unique_ptr<pmi::Server>> servers;
  for (const auto& id : step.nodelist()) {
    config.CranedIdOfThisNode = id;
    auto server = std::make_unique<pmi::Server>();
    ASSERT_TRUE(server->Init(config, step, {}, std::chrono::seconds(3)));
    servers.push_back(std::move(server));
  }
  for (size_t i = 0; i < servers.size(); ++i) {
    std::vector<std::pair<CranedId, std::string>> peers;
    for (size_t j = 0; j < servers.size(); ++j)
      if (i != j) peers.emplace_back(step.nodelist(j), servers[j]->Port());
    ASSERT_TRUE(servers[i]->ReceivePeers(peers));
    EXPECT_TRUE(servers[i]->ReceivePeers(peers));
    auto changed = peers;
    changed.front().second = changed.front().second == "1" ? "2" : "1";
    EXPECT_FALSE(servers[i]->ReceivePeers(changed));
  }
  Clients clients;
  for (uint32_t rank = 0; rank < rank_nodes.size(); ++rank) {
    auto launch = servers[rank_nodes[rank]]->PrepareTask(rank);
    ASSERT_TRUE(launch);
    ASSERT_TRUE(clients.Start(std::move(*launch), "(vector,(0,3,1),(0,1,1))"));
  }
  clients.ExpectExit();
}

TEST(Pmi2Server, PartialFramesAndPmi1Negotiation) {
  pmi::Server server;
  ASSERT_TRUE(server.Init(Config(), Step(), {}, 3s));
  auto launch = server.PrepareTask(0);
  ASSERT_TRUE(launch);
  Send(launch->fd, "cmd=init pmi_version=2 ");
  Send(launch->fd, "pmi_subversion=0\n");
  EXPECT_NE(Read(launch->fd).find("rc=0"), std::string::npos);
  std::string body = "cmd=fullinit;pmirank=0;";
  std::string frame = std::to_string(body.size());
  frame.resize(6, ' ');
  frame += body;
  Send(launch->fd, frame.substr(0, 2));
  Send(launch->fd, frame.substr(2, 7));
  Send(launch->fd, frame.substr(9));
  EXPECT_NE(Read(launch->fd).find("size=1;"), std::string::npos);
  Send(launch->fd, "13    cmd=finalize;");
  EXPECT_NE(Read(launch->fd).find("finalize-response"), std::string::npos);

  pmi::Server legacy;
  ASSERT_TRUE(legacy.Init(Config(), Step(4), {}, 3s));
  auto old = legacy.PrepareTask(0);
  ASSERT_TRUE(old);
  Send(old->fd, "cmd=init pmi_version=1 pmi_subversion=1\n");
  auto init = Read(old->fd);
  EXPECT_NE(init.find("cmd=response_to_init"), std::string::npos);
  EXPECT_NE(init.find("rc=0"), std::string::npos);
  Send(old->fd, "cmd=get_appnum\n");
  auto appnum = Read(old->fd);
  EXPECT_NE(appnum.find("cmd=appnum"), std::string::npos);
  EXPECT_NE(appnum.find("appnum=-1"), std::string::npos);
  Send(old->fd, "cmd=finalize\n");
  EXPECT_NE(Read(old->fd).find("cmd=finalize_ack"), std::string::npos);
}

TEST(Pmi2Server, IncompleteInitializationHasBoundedLifetime) {
  std::atomic<unsigned> failures{};
  pmi::Server server;
  ASSERT_TRUE(server.Init(Config(), Step(), [&] { ++failures; }, 1s));
  auto launch = server.PrepareTask(0);
  ASSERT_TRUE(launch);
  Send(launch->fd, "cmd=init pmi_version=2 pmi_subversion=0\n");
  EXPECT_NE(Read(launch->fd).find("rc=0"), std::string::npos);
  EXPECT_TRUE(WaitFor(failures, 1));
  EXPECT_FALSE(server.PrepareTask(0));
}

TEST(Pmi2Server, FailedStepDoesNotPoisonAnotherStep) {
  std::atomic<unsigned> failures{};
  pmi::Server failing, healthy;
  ASSERT_TRUE(failing.Init(Config(), Step(3), [&] { ++failures; }, 1s));
  ASSERT_TRUE(healthy.Init(Config(), Step(4), {}, 3s));
  Clients clients;
  auto stalled = failing.PrepareTask(0);
  auto normal = healthy.PrepareTask(0);
  ASSERT_TRUE(stalled);
  ASSERT_TRUE(normal);
  ASSERT_TRUE(
      clients.Start(std::move(*stalled), "(vector,(0,1,1))", "wait-timeout"));
  // Exercise the healthy step after failure has propagated, not just before
  // the timeout when both servers would still appear usable.
  EXPECT_TRUE(WaitFor(failures, 1));
  ASSERT_TRUE(clients.Start(std::move(*normal), "(vector,(0,1,1))"));
  clients.ExpectExit();
  EXPECT_TRUE(WaitFor(failures, 1));
}

TEST(Pmi2Server, DisconnectPropagatesAcrossTree) {
  auto step = Step();
  step.add_nodelist("node1");
  step.add_nodelist("node2");
  step.add_task_node_list(1);
  step.add_task_node_list(2);
  auto config = Config();
  std::atomic<unsigned> failures{};
  for (const auto& id : step.nodelist()) {
    config.CranedIdToNodeHostname[id] = "localhost";
    config.CranedIdToNodeAddr[id] = "127.0.0.1";
  }
  std::vector<std::unique_ptr<pmi::Server>> servers;
  for (const auto& id : step.nodelist()) {
    config.CranedIdOfThisNode = id;
    auto server = std::make_unique<pmi::Server>();
    ASSERT_TRUE(server->Init(config, step, [&] { ++failures; }, 2s));
    servers.push_back(std::move(server));
  }
  for (size_t i = 0; i < servers.size(); ++i) {
    std::vector<std::pair<CranedId, std::string>> peers;
    for (size_t j = 0; j < servers.size(); ++j)
      if (i != j) peers.emplace_back(step.nodelist(j), servers[j]->Port());
    ASSERT_TRUE(servers[i]->ReceivePeers(peers));
  }
  auto disconnected = servers[2]->PrepareTask(2);
  ASSERT_TRUE(disconnected);
  disconnected.reset();
  EXPECT_TRUE(WaitFor(failures, 3));
}

TEST(Pmi2Server, LargeUniformAllocationHasCompactProcessMapping) {
  auto step = Step();
  for (uint32_t node = 1; node < 1000; ++node) {
    step.add_nodelist("node" + std::to_string(node));
    step.add_task_node_list(node);
  }
  pmi::Server server;
  ASSERT_TRUE(server.Init(Config(), step, {}, 3s));
  auto launch = server.PrepareTask(0);
  ASSERT_TRUE(launch);
  Clients clients;
  ASSERT_TRUE(
      clients.Start(std::move(*launch), "(vector,(0,1000,1))", "mapping"));
  clients.ExpectExit();
}

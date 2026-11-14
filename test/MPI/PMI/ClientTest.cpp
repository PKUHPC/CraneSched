/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 */

#include <gtest/gtest.h>
#include <pthread.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <string>
#include <thread>

#include "Pmi2Codec.h"
#include "Pmi2Session.h"
#include "Pmi2Transport.h"

namespace {

using crane::pmi2::Codec;
using crane::pmi2::Fields;
using crane::pmi2::Transport;

TEST(Pmi2Codec, EscapesAndUnescapesSemicolons) {
  Fields fields{{"cmd", "kvs-put"}, {"key", "escaped"}, {"value", "a;b=c;;d"}};
  auto body = Codec::EncodeFrame(fields);
  ASSERT_TRUE(body);
  EXPECT_NE(body->find("a;;b=c;;;;d"), std::string::npos);
  auto parsed = Codec::ParseFrame(*body);
  ASSERT_TRUE(parsed);
  EXPECT_EQ(parsed->at("value"), "a;b=c;;d");
}

TEST(Pmi2Codec, RejectsDuplicateAndMalformedFields) {
  EXPECT_FALSE(Codec::ParseFrame("cmd=kvs-put;key=a;key=b;"));
  EXPECT_FALSE(Codec::ParseFrame("cmd=kvs-put;malformed;"));
  EXPECT_FALSE(Codec::ParseFrame("key=a;"));
}

TEST(Pmi2Transport, HandlesPartialFrameWrites) {
  int sockets[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
  Transport sender(sockets[0], std::chrono::seconds(1));
  Transport receiver(sockets[1], std::chrono::seconds(1));
  std::atomic send_ok{false};
  std::thread writer(
      [&] { send_ok = sender.SendFrame("cmd=finalize-response;rc=0;"); });
  auto body = receiver.ReceiveFrame();
  writer.join();
  EXPECT_TRUE(send_ok);
  ASSERT_TRUE(body);
  EXPECT_EQ(*body, "cmd=finalize-response;rc=0;");
}

}  // namespace

TEST(Pmi2Session, SingletonRefillsOutputsAndRejectsInitAfterFinalize) {
  const char* previous = getenv("PMI_FD");
  const std::optional<std::string> saved =
      previous ? std::optional<std::string>(previous) : std::nullopt;
  unsetenv("PMI_FD");
  crane::pmi2::Session session;
  for (int i = 0; i < 2; ++i) {
    int spawned = -1, size = -1, rank = -1, appnum = 99;
    EXPECT_EQ(session.Initialize(&spawned, &size, &rank, &appnum),
              PMI2_SUCCESS);
    EXPECT_EQ(spawned, 0);
    EXPECT_EQ(size, 1);
    EXPECT_EQ(rank, 0);
    EXPECT_EQ(appnum, -1);
  }
  EXPECT_EQ(session.Finalize(), PMI2_SUCCESS);
  int a;
  int b;
  int c;
  int d;
  EXPECT_EQ(session.Initialize(&a, &b, &c, &d), PMI2_ERR_INIT);
  if (saved) setenv("PMI_FD", saved->c_str(), 1);
}

TEST(Pmi2Session, SingletonAbortExitsNonzeroAndAllowsExitCallbackReentry) {
  EXPECT_EXIT(
      {
        unsetenv("PMI_FD");
        auto& session = crane::pmi2::GetSession();
        int a;
        int b;
        int c;
        int d;
        if (session.Initialize(&a, &b, &c, &d) != PMI2_SUCCESS) _exit(20);
        atexit([] { crane::pmi2::GetSession().Finalize(); });
        session.Abort(0, "singleton abort test");
      },
      ::testing::ExitedWithCode(EXIT_FAILURE), "");
}

namespace {
class InterruptPoll {
 public:
  InterruptPoll() {
    struct sigaction action{};
    action.sa_handler = [](int) {};
    sigemptyset(&action.sa_mask);
    sigaction(SIGUSR1, &action, &previous_);
  }
  ~InterruptPoll() { sigaction(SIGUSR1, &previous_, nullptr); }

 private:
  struct sigaction previous_{};
};
}  // namespace

TEST(Pmi2Transport, RetriesInterruptedReadsAndWrites) {
  InterruptPoll signals;
  for (bool write : {false, true}) {
    int sockets[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    int capacity = 1024;
    ASSERT_EQ(setsockopt(sockets[0], SOL_SOCKET, SO_SNDBUF, &capacity,
                         sizeof(capacity)),
              0);
    Transport sender(sockets[0], std::chrono::seconds(2));
    Transport receiver(sockets[1], std::chrono::seconds(2));
    const std::string body = "cmd=reply;value=" + std::string(32000, 'x') + ';';
    std::optional<std::string> received;
    bool sent = false;
    std::atomic<bool> started{};
    std::thread interrupted([&] {
      started = true;
      if (write)
        sent = sender.SendFrame(body);
      else
        received = receiver.ReceiveFrame();
    });
    while (!started) std::this_thread::yield();
    for (int i = 0; i < 10; ++i) {
      pthread_kill(interrupted.native_handle(), SIGUSR1);
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    if (write)
      received = receiver.ReceiveFrame();
    else
      sent = sender.SendFrame(body);
    interrupted.join();
    EXPECT_TRUE(sent);
    ASSERT_TRUE(received);
    EXPECT_EQ(*received, body);
  }
}

TEST(Pmi2Transport, RepeatedInterruptionsDoNotExtendTheDeadline) {
  InterruptPoll signals;
  int sockets[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
  Transport receiver(sockets[0], std::chrono::seconds(1));
  Transport peer(sockets[1], std::chrono::seconds(1));
  std::atomic<bool> done{};
  bool received = true;
  auto start = std::chrono::steady_clock::now();
  std::thread interrupted([&] {
    received = receiver.ReceiveFrame().has_value();
    done = true;
  });
  while (!done &&
         std::chrono::steady_clock::now() - start < std::chrono::seconds(2)) {
    pthread_kill(interrupted.native_handle(), SIGUSR1);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(done);
  peer.Close();
  interrupted.join();
  EXPECT_FALSE(received);
  EXPECT_LT(std::chrono::steady_clock::now() - start,
            std::chrono::milliseconds(1800));
}

TEST(Pmi2Transport, StalledWriteRespectsTheDeadline) {
  int sockets[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
  int capacity = 1024;
  ASSERT_EQ(setsockopt(sockets[0], SOL_SOCKET, SO_SNDBUF, &capacity,
                       sizeof(capacity)),
            0);
  Transport sender(sockets[0], std::chrono::seconds(1));
  Transport peer(sockets[1], std::chrono::seconds(1));
  std::atomic<bool> done{};
  bool sent = true;
  const auto start = std::chrono::steady_clock::now();
  std::thread writer([&] {
    sent = sender.SendFrame("cmd=reply;value=" + std::string(32000, 'x') + ';');
    done = true;
  });
  while (!done && std::chrono::steady_clock::now() - start <
                      std::chrono::milliseconds(1500))
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  EXPECT_TRUE(done);
  peer.Close();
  writer.join();
  EXPECT_FALSE(sent);
}

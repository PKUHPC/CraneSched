/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 */

#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <string>
#include <thread>

#include "Pmi2Codec.h"
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

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

#include "EndpointRendezvous.h"

namespace {
using namespace crane::grpc::mpi;
BroadcastMpiEndpointRequest Report(Protocol protocol, const std::string& node,
                                   const std::string& address) {
  BroadcastMpiEndpointRequest request;
  request.set_job_id(42);
  request.set_step_id(3);
  request.set_protocol(protocol);
  request.add_craned_ids("node0");
  request.add_craned_ids("node1");
  request.mutable_endpoint()->set_craned_id(node);
  request.mutable_endpoint()->set_address(address);
  return request;
}
TEST(MpiEndpoints, CompletesOnlyAfterEveryAllocatedNodeAndKeepsOrder) {
  mpi::EndpointRendezvous rendezvous(42, 3, PROTOCOL_PMI, {"node0", "node1"});
  auto first = rendezvous.Accept(Report(PROTOCOL_PMI, "node1", "1234"));
  ASSERT_TRUE(first);
  EXPECT_FALSE(*first);
  auto second = rendezvous.Accept(Report(PROTOCOL_PMI, "node0", "5678"));
  ASSERT_TRUE(second);
  ASSERT_TRUE(*second);
  EXPECT_EQ((**second).endpoints(0).craned_id(), "node0");
  EXPECT_EQ((**second).endpoints(1).address(), "1234");
}
TEST(MpiEndpoints, AcceptsIdenticalRetriesAndRejectsConflictingAnnouncements) {
  mpi::EndpointRendezvous rendezvous(42, 3, PROTOCOL_PMI, {"node0", "node1"});
  auto request = Report(PROTOCOL_PMI, "node0", "1234");
  ASSERT_TRUE(rendezvous.Accept(request));
  EXPECT_TRUE(rendezvous.Accept(request));
  request.mutable_endpoint()->set_address("5678");
  EXPECT_FALSE(rendezvous.Accept(request));
  auto completed = rendezvous.Accept(Report(PROTOCOL_PMI, "node1", "1111"));
  ASSERT_TRUE(completed && *completed);
  EXPECT_EQ((**completed).endpoints(0).address(), "1234");
}
TEST(MpiEndpoints, RejectsWrongStepProtocolAndMembersWithoutRecordingThem) {
  mpi::EndpointRendezvous rendezvous(42, 3, PROTOCOL_PMI, {"node0", "node1"});
  auto request = Report(PROTOCOL_PMIX, "node0", "1234");
  EXPECT_FALSE(rendezvous.Accept(request));
  request.set_protocol(PROTOCOL_PMI);
  request.set_step_id(4);
  EXPECT_FALSE(rendezvous.Accept(request));
  request.set_step_id(3);
  request.set_job_id(43);
  EXPECT_FALSE(rendezvous.Accept(request));
  request.set_job_id(42);
  request.mutable_endpoint()->set_craned_id("outsider");
  EXPECT_FALSE(rendezvous.Accept(request));
  request.mutable_endpoint()->set_craned_id("node0");
  request.set_craned_ids(1, "node0");
  EXPECT_FALSE(rendezvous.Accept(request));
  auto valid = rendezvous.Accept(Report(PROTOCOL_PMI, "node1", "1234"));
  ASSERT_TRUE(valid);
  EXPECT_FALSE(*valid);
}
TEST(MpiEndpoints, RejectsMalformedPmiPorts) {
  mpi::EndpointRendezvous rendezvous(42, 3, PROTOCOL_PMI, {"node0", "node1"});
  for (const std::string port : {"", "0", "65536", "-1", "1x", "1\n"})
    EXPECT_FALSE(rendezvous.Accept(Report(PROTOCOL_PMI, "node0", port)));
}
TEST(MpiEndpoints, PreservesOpaqueUcxBytes) {
  mpi::EndpointRendezvous rendezvous(42, 3, PROTOCOL_PMIX, {"node0", "node1"});
  const std::string address("\0\xff\x80tcp", 6);
  ASSERT_TRUE(rendezvous.Accept(Report(PROTOCOL_PMIX, "node0", address)));
  auto completed = rendezvous.Accept(Report(PROTOCOL_PMIX, "node1", address));
  ASSERT_TRUE(completed && *completed);
  std::string serialized = (**completed).SerializeAsString();
  ReceiveMpiEndpointsRequest decoded;
  ASSERT_TRUE(decoded.ParseFromString(serialized));
  EXPECT_EQ(decoded.endpoints(0).address(), address);
}
}  // namespace

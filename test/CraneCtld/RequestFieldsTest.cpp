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

#include <gtest/gtest.h>

#include <bsoncxx/builder/basic/kvp.hpp>

#include "Database/RequestFields.h"

namespace {
using bsoncxx::builder::basic::document;
using bsoncxx::builder::basic::kvp;
using namespace Ctld::accounting;

TEST(RequestFields, RoundTripPreservesRequestWithoutChangingAllocation) {
  crane::grpc::GresMap request;
  auto& gpu = (*request.mutable_name_gres_map())["gpu"];
  gpu.set_total(6);
  (*gpu.mutable_specified())["a100"] = 4;
  auto total = RecoverRequestedGres(request, 2, nullptr);
  ASSERT_TRUE(total);
  document doc;
  AppendRequestFields(&doc, 3, &*total, request);

  crane::grpc::JobInfo job;
  auto* allocated = job.mutable_allocated_res_view()->mutable_gres_map();
  (*allocated->mutable_name_gres_map())["gpu"].set_total(20);
  job.mutable_req_total_res_view()->set_cpu_count(12);
  RestoreRequestFields(doc.view(), &job);
  EXPECT_TRUE(job.has_ntasks());
  EXPECT_EQ(job.ntasks(), 3);
  EXPECT_EQ(job.req_total_res_view().cpu_count(), 12);
  const auto& result =
      job.req_total_res_view().gres_map().name_gres_map().at("gpu");
  EXPECT_EQ(result.total(), 12);
  EXPECT_EQ(result.specified().at("a100"), 8);
  EXPECT_EQ(allocated->name_gres_map().at("gpu").total(), 20);

  crane::grpc::StepInfo step;
  RestoreRequestFields(doc.view(), &step);
  EXPECT_TRUE(step.has_ntasks());
  EXPECT_EQ(step.ntasks(),
            3);  // Do not reinterpret a PRIMARY request as layout=1.
  EXPECT_EQ(
      step.req_total_res_view().gres_map().name_gres_map().at("gpu").total(),
      12);
}

TEST(RequestFields, MissingHistoricalFieldsDifferFromKnownZeroAndEmpty) {
  document legacy;
  legacy.append(kvp("cpus_req", int64_t{12}));
  crane::grpc::JobInfo job;
  job.set_ntasks(9);
  job.mutable_req_total_res_view()->mutable_gres_map();
  RestoreRequestFields(legacy.view(), &job);
  EXPECT_FALSE(job.has_ntasks());
  EXPECT_FALSE(job.req_total_res_view().has_gres_map());

  document current;
  crane::grpc::GresMap empty;
  AppendRequestFields(&current, 0, &empty, empty);
  RestoreRequestFields(current.view(), &job);
  EXPECT_TRUE(job.has_ntasks());
  EXPECT_EQ(job.ntasks(), 0);
  EXPECT_TRUE(job.req_total_res_view().has_gres_map());
  EXPECT_TRUE(job.req_total_res_view().gres_map().name_gres_map().empty());
}

TEST(RequestFields, RecoveryUsesSnapshotAndDoesNotGuessElasticTotals) {
  crane::grpc::GresMap per_node;
  (*per_node.mutable_name_gres_map())["gpu"].set_total(2);
  auto snapshot = RecoverRequestedGres(per_node, 3, nullptr);
  ASSERT_TRUE(snapshot);
  auto recovered = RecoverRequestedGres(per_node, 1, &*snapshot);
  ASSERT_TRUE(recovered);
  EXPECT_EQ(recovered->name_gres_map().at("gpu").total(), 6);
  EXPECT_FALSE(RecoverRequestedGres(per_node, 0, nullptr));
  EXPECT_TRUE(RecoverRequestedGres({}, 0, nullptr));

  document unknown_total;
  AppendRequestFields(&unknown_total, 3, nullptr, per_node);
  crane::grpc::StepInfo step;
  RestoreRequestFields(unknown_total.view(), &step);
  EXPECT_EQ(step.ntasks(), 3);
  EXPECT_FALSE(step.req_total_res_view().has_gres_map());
}
}  // namespace

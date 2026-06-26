/**
 * Copyright (c) 2024 Peking University and Peking University
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

#include <filesystem>

#include "CtldPublicDefs.h"
#include "Database/EmbeddedDbClient.h"

namespace {

google::protobuf::Timestamp TimestampAt(int64_t seconds) {
  google::protobuf::Timestamp timestamp;
  timestamp.set_seconds(seconds);
  return timestamp;
}

void ExpectStepFreeRequested(const Ctld::StepStatusChangeContext& context,
                             const CranedId& craned_id, job_id_t job_id,
                             step_id_t step_id) {
  auto craned_it = context.craned_step_free_map.find(craned_id);
  ASSERT_NE(craned_it, context.craned_step_free_map.end());

  auto job_it = craned_it->second.find(job_id);
  ASSERT_NE(job_it, craned_it->second.end());
  EXPECT_TRUE(job_it->second.contains(step_id));
}

void ExpectStepExecRequested(const Ctld::StepStatusChangeContext& context,
                             const CranedId& craned_id, job_id_t job_id,
                             step_id_t step_id) {
  auto craned_it = context.craned_step_exec_map.find(craned_id);
  ASSERT_NE(craned_it, context.craned_step_exec_map.end());

  auto job_it = craned_it->second.find(job_id);
  ASSERT_NE(job_it, craned_it->second.end());
  EXPECT_TRUE(job_it->second.contains(step_id));
}

void ExpectStepCancelRequested(const Ctld::StepStatusChangeContext& context,
                               const CranedId& craned_id, job_id_t job_id,
                               step_id_t step_id) {
  auto craned_it = context.craned_cancel_steps.find(craned_id);
  ASSERT_NE(craned_it, context.craned_cancel_steps.end());

  auto job_it = craned_it->second.find(job_id);
  ASSERT_NE(job_it, craned_it->second.end());
  EXPECT_TRUE(job_it->second.contains(step_id));
}

std::unique_ptr<Ctld::DaemonStepInCtld> MakeDaemonStep(
    Ctld::JobInCtld* job, const std::vector<CranedId>& craned_ids) {
  auto daemon_step = std::make_unique<Ctld::DaemonStepInCtld>();
  auto craned_set =
      std::unordered_set<CranedId>{craned_ids.begin(), craned_ids.end()};

  daemon_step->job = job;
  daemon_step->type = crane::grpc::JobType::Batch;
  daemon_step->job_id = job->JobId();
  daemon_step->SetStepType(crane::grpc::StepType::DAEMON);
  daemon_step->SetStepId(kDaemonStepId);
  daemon_step->SetStepDbId(kDaemonStepId);
  daemon_step->SetCranedIds(craned_ids);
  daemon_step->SetExecutionNodes(craned_set);
  daemon_step->SetConfiguringNodes(craned_set);
  daemon_step->SetRunningNodes(craned_set);
  daemon_step->SetStatus(crane::grpc::JobStatus::Configuring);
  daemon_step->SetErrorStatus(crane::grpc::JobStatus::Invalid);
  daemon_step->SetErrorExitCode(0U);

  return daemon_step;
}

std::unique_ptr<Ctld::CommonStepInCtld> MakePrimaryStep(
    Ctld::JobInCtld* job, const std::vector<CranedId>& craned_ids) {
  auto primary_step = std::make_unique<Ctld::CommonStepInCtld>();
  auto craned_set =
      std::unordered_set<CranedId>{craned_ids.begin(), craned_ids.end()};

  primary_step->job = job;
  primary_step->type = crane::grpc::JobType::Batch;
  primary_step->job_id = job->JobId();
  primary_step->SetStepType(crane::grpc::StepType::PRIMARY);
  primary_step->SetStepId(Ctld::kPrimaryStepId);
  primary_step->SetStepDbId(Ctld::kPrimaryStepId);
  primary_step->SetExecutionNodes(craned_set);
  primary_step->SetConfiguringNodes(craned_set);
  primary_step->SetRunningNodes(craned_set);
  primary_step->SetStatus(crane::grpc::JobStatus::Configuring);
  primary_step->SetErrorStatus(crane::grpc::JobStatus::Invalid);
  primary_step->SetErrorExitCode(0U);

  return primary_step;
}

void SetupJobAllocation(Ctld::JobInCtld& job,
                        const std::vector<CranedId>& craned_ids) {
  job.SetCranedIds(std::vector<CranedId>{craned_ids});
  job.executing_craned_ids = {craned_ids.begin(), craned_ids.end()};
  ResourceV3 alloc_res;
  for (const auto& node : craned_ids) {
    crane::grpc::ResourceInNodeV3 proto_res;
    proto_res.set_cpu_count(1.0);
    proto_res.set_memory_bytes(1024ULL * 1024 * 1024);
    alloc_res.AddResourceInNode(node, ResourceInNodeV3{proto_res});
  }
  job.SetAllocatedRes(std::move(alloc_res));
}

}  // namespace

TEST(CtldStepStateMachineTest,
     DaemonConfigureFailureCleansAndReturnsFailureAfterTerminalReports) {
  constexpr job_id_t kJobId = 42;
  const std::vector<CranedId> craned_ids{"node-a", "node-b"};

  Ctld::JobInCtld job;
  job.type = crane::grpc::JobType::Batch;
  job.SetJobId(kJobId);
  job.SetStatus(crane::grpc::JobStatus::Configuring);
  job.SetPrimaryStepStatus(crane::grpc::JobStatus::Invalid);
  job.SetDaemonStep(MakeDaemonStep(&job, craned_ids));

  Ctld::StepStatusChangeContext context;
  auto* daemon_step = job.DaemonStep();

  auto result =
      daemon_step->StepStatusChange(crane::grpc::JobStatus::Running, 0U, "",
                                    "node-a", TimestampAt(100), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(daemon_step->Status(), crane::grpc::JobStatus::Configuring);
  EXPECT_TRUE(context.craned_step_free_map.empty());

  result = daemon_step->StepStatusChange(crane::grpc::JobStatus::Failed, 7U,
                                         "configure failed", "node-b",
                                         TimestampAt(101), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(daemon_step->Status(), crane::grpc::JobStatus::Completing);
  EXPECT_EQ(daemon_step->PrevErrorStatus(), crane::grpc::JobStatus::Failed);
  EXPECT_EQ(daemon_step->PrevErrorExitCode(), 7U);
  ExpectStepFreeRequested(context, "node-a", kJobId, kDaemonStepId);
  ExpectStepFreeRequested(context, "node-b", kJobId, kDaemonStepId);

  result =
      daemon_step->StepStatusChange(crane::grpc::JobStatus::Completed, 0U, "",
                                    "node-a", TimestampAt(102), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_NE(job.DaemonStep(), nullptr);

  result =
      daemon_step->StepStatusChange(crane::grpc::JobStatus::Completed, 0U, "",
                                    "node-b", TimestampAt(103), &context);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, crane::grpc::JobStatus::Failed);
  EXPECT_EQ(result->second, 7U);
  EXPECT_EQ(job.DaemonStep(), nullptr);
  EXPECT_TRUE(context.step_raw_ptrs.contains(daemon_step));
}

TEST(CtldStepStateMachineTest,
     DaemonCancelRequestedDuringConfiguringCleansAndReturnsCancelled) {
  constexpr job_id_t kJobId = 43;
  const std::vector<CranedId> craned_ids{"node-a", "node-b"};

  Ctld::JobInCtld job;
  job.type = crane::grpc::JobType::Batch;
  job.SetJobId(kJobId);
  job.SetStatus(crane::grpc::JobStatus::Configuring);
  job.SetPrimaryStepStatus(crane::grpc::JobStatus::Invalid);
  job.SetCancelRequested(true);
  job.SetDaemonStep(MakeDaemonStep(&job, craned_ids));

  Ctld::StepStatusChangeContext context;
  auto* daemon_step = job.DaemonStep();

  auto result =
      daemon_step->StepStatusChange(crane::grpc::JobStatus::Running, 0U, "",
                                    "node-a", TimestampAt(110), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(daemon_step->Status(), crane::grpc::JobStatus::Configuring);
  EXPECT_TRUE(context.craned_step_free_map.empty());

  result =
      daemon_step->StepStatusChange(crane::grpc::JobStatus::Running, 0U, "",
                                    "node-b", TimestampAt(111), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(daemon_step->Status(), crane::grpc::JobStatus::Completing);
  EXPECT_EQ(daemon_step->PrevErrorStatus(), crane::grpc::JobStatus::Cancelled);
  EXPECT_EQ(daemon_step->PrevErrorExitCode(), ExitCode::EC_TERMINATED);
  ExpectStepFreeRequested(context, "node-a", kJobId, kDaemonStepId);
  ExpectStepFreeRequested(context, "node-b", kJobId, kDaemonStepId);

  result =
      daemon_step->StepStatusChange(crane::grpc::JobStatus::Completed, 0U, "",
                                    "node-a", TimestampAt(112), &context);
  EXPECT_FALSE(result.has_value());

  result =
      daemon_step->StepStatusChange(crane::grpc::JobStatus::Completed, 0U, "",
                                    "node-b", TimestampAt(113), &context);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, crane::grpc::JobStatus::Cancelled);
  EXPECT_EQ(result->second, ExitCode::EC_TERMINATED);
  EXPECT_EQ(job.DaemonStep(), nullptr);
}

TEST(CtldStepStateMachineTest,
     PrimaryStepCompletingWaitsForAllNodesAndThenReleasesStep) {
  constexpr job_id_t kJobId = 44;
  const std::vector<CranedId> craned_ids{"node-a", "node-b"};

  Ctld::JobInCtld job;
  job.type = crane::grpc::JobType::Batch;
  job.SetJobId(kJobId);
  job.SetStatus(crane::grpc::JobStatus::Configuring);
  job.SetPrimaryStepStatus(crane::grpc::JobStatus::Invalid);
  job.SetDaemonStep(MakeDaemonStep(&job, craned_ids));
  job.SetPrimaryStep(MakePrimaryStep(&job, craned_ids));

  Ctld::StepStatusChangeContext context;
  auto* primary_step = job.PrimaryStep();

  auto result =
      primary_step->StepStatusChange(crane::grpc::JobStatus::Starting, 0U, "",
                                     "node-a", TimestampAt(200), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(primary_step->Status(), crane::grpc::JobStatus::Configuring);
  EXPECT_TRUE(context.craned_step_exec_map.empty());

  result =
      primary_step->StepStatusChange(crane::grpc::JobStatus::Starting, 0U, "",
                                     "node-b", TimestampAt(201), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(primary_step->Status(), crane::grpc::JobStatus::Running);
  EXPECT_EQ(job.Status(), crane::grpc::JobStatus::Running);
  ExpectStepExecRequested(context, "node-a", kJobId, Ctld::kPrimaryStepId);
  ExpectStepExecRequested(context, "node-b", kJobId, Ctld::kPrimaryStepId);
  EXPECT_TRUE(context.rn_step_raw_ptrs.contains(primary_step));
  EXPECT_TRUE(context.rn_job_raw_ptrs.contains(&job));

  result =
      primary_step->StepStatusChange(crane::grpc::JobStatus::Completing, 0U, "",
                                     "node-a", TimestampAt(202), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(primary_step->Status(), crane::grpc::JobStatus::Running);
  EXPECT_FALSE(context.craned_step_free_map.contains("node-a"));

  result =
      primary_step->StepStatusChange(crane::grpc::JobStatus::Completing, 0U, "",
                                     "node-b", TimestampAt(203), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(primary_step->Status(), crane::grpc::JobStatus::Completing);
  ExpectStepFreeRequested(context, "node-a", kJobId, Ctld::kPrimaryStepId);
  ExpectStepFreeRequested(context, "node-b", kJobId, Ctld::kPrimaryStepId);

  result =
      primary_step->StepStatusChange(crane::grpc::JobStatus::Completed, 0U, "",
                                     "node-a", TimestampAt(204), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_NE(job.PrimaryStep(), nullptr);

  result =
      primary_step->StepStatusChange(crane::grpc::JobStatus::Completed, 0U, "",
                                     "node-b", TimestampAt(205), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(job.PrimaryStep(), nullptr);
  EXPECT_EQ(job.PrimaryStepStatus(), crane::grpc::JobStatus::Completed);
  EXPECT_EQ(job.PrimaryStepExitCode(), 0U);
  EXPECT_TRUE(context.step_raw_ptrs.contains(primary_step));
  ExpectStepFreeRequested(context, "node-a", kJobId, kDaemonStepId);
  ExpectStepFreeRequested(context, "node-b", kJobId, kDaemonStepId);
}

TEST(CtldStepStateMachineTest,
     PrimaryConfigureFailureCancelsReadyNodesAndFinishesAsFailed) {
  constexpr job_id_t kJobId = 45;
  const std::vector<CranedId> craned_ids{"node-a", "node-b"};

  Ctld::JobInCtld job;
  job.type = crane::grpc::JobType::Batch;
  job.SetJobId(kJobId);
  job.SetStatus(crane::grpc::JobStatus::Configuring);
  job.SetPrimaryStepStatus(crane::grpc::JobStatus::Invalid);
  job.SetDaemonStep(MakeDaemonStep(&job, craned_ids));
  job.SetPrimaryStep(MakePrimaryStep(&job, craned_ids));

  Ctld::StepStatusChangeContext context;
  auto* primary_step = job.PrimaryStep();

  auto result =
      primary_step->StepStatusChange(crane::grpc::JobStatus::Starting, 0U, "",
                                     "node-a", TimestampAt(300), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(primary_step->Status(), crane::grpc::JobStatus::Configuring);

  result = primary_step->StepStatusChange(crane::grpc::JobStatus::Failed, 9U,
                                          "configure failed", "node-b",
                                          TimestampAt(301), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(primary_step->Status(), crane::grpc::JobStatus::Completing);
  EXPECT_EQ(primary_step->PrevErrorStatus(), crane::grpc::JobStatus::Failed);
  EXPECT_EQ(primary_step->PrevErrorExitCode(), 9U);
  ExpectStepCancelRequested(context, "node-a", kJobId, Ctld::kPrimaryStepId);
  EXPECT_TRUE(context.craned_step_free_map.empty());

  result =
      primary_step->StepStatusChange(crane::grpc::JobStatus::Completing, 0U, "",
                                     "node-a", TimestampAt(302), &context);
  EXPECT_FALSE(result.has_value());
  ExpectStepFreeRequested(context, "node-a", kJobId, Ctld::kPrimaryStepId);
  ExpectStepFreeRequested(context, "node-b", kJobId, Ctld::kPrimaryStepId);

  result =
      primary_step->StepStatusChange(crane::grpc::JobStatus::Completed, 0U, "",
                                     "node-a", TimestampAt(303), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_NE(job.PrimaryStep(), nullptr);

  result =
      primary_step->StepStatusChange(crane::grpc::JobStatus::Completed, 0U, "",
                                     "node-b", TimestampAt(304), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(job.PrimaryStep(), nullptr);
  EXPECT_EQ(job.PrimaryStepStatus(), crane::grpc::JobStatus::Failed);
  EXPECT_EQ(job.PrimaryStepExitCode(), 9U);
  ExpectStepFreeRequested(context, "node-a", kJobId, kDaemonStepId);
  ExpectStepFreeRequested(context, "node-b", kJobId, kDaemonStepId);
}

TEST(CtldStepStateMachineTest,
     PrimaryTerminalFailurePreservesFailureExitCodeAfterCleanup) {
  constexpr job_id_t kJobId = 46;
  const std::vector<CranedId> craned_ids{"node-a", "node-b"};

  Ctld::JobInCtld job;
  job.type = crane::grpc::JobType::Batch;
  job.SetJobId(kJobId);
  job.SetStatus(crane::grpc::JobStatus::Running);
  job.SetPrimaryStepStatus(crane::grpc::JobStatus::Invalid);
  job.SetDaemonStep(MakeDaemonStep(&job, craned_ids));
  job.DaemonStep()->SetStatus(crane::grpc::JobStatus::Running);
  job.SetPrimaryStep(MakePrimaryStep(&job, craned_ids));

  Ctld::StepStatusChangeContext context;
  auto* primary_step = job.PrimaryStep();
  primary_step->SetConfiguringNodes({});
  primary_step->SetStatus(crane::grpc::JobStatus::Running);

  auto result =
      primary_step->StepStatusChange(crane::grpc::JobStatus::Completing, 0U, "",
                                     "node-a", TimestampAt(400), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(primary_step->Status(), crane::grpc::JobStatus::Running);

  result =
      primary_step->StepStatusChange(crane::grpc::JobStatus::Completing, 0U, "",
                                     "node-b", TimestampAt(401), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(primary_step->Status(), crane::grpc::JobStatus::Completing);
  ExpectStepFreeRequested(context, "node-a", kJobId, Ctld::kPrimaryStepId);
  ExpectStepFreeRequested(context, "node-b", kJobId, Ctld::kPrimaryStepId);

  result =
      primary_step->StepStatusChange(crane::grpc::JobStatus::Completed, 0U, "",
                                     "node-a", TimestampAt(402), &context);
  EXPECT_FALSE(result.has_value());

  result = primary_step->StepStatusChange(crane::grpc::JobStatus::Failed, 137U,
                                          "process failed", "node-b",
                                          TimestampAt(403), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(job.PrimaryStep(), nullptr);
  EXPECT_EQ(job.PrimaryStepStatus(), crane::grpc::JobStatus::Failed);
  EXPECT_EQ(job.PrimaryStepExitCode(), 137U);
  ExpectStepFreeRequested(context, "node-a", kJobId, kDaemonStepId);
  ExpectStepFreeRequested(context, "node-b", kJobId, kDaemonStepId);
}

// --- ShouldRequeue tests ---

TEST(ShouldRequeueTest, InteractiveJobNeverRequeues) {
  Ctld::JobInCtld job;
  job.type = crane::grpc::Interactive;
  job.SetRequeueRequested(true);
  EXPECT_FALSE(job.ShouldRequeue());
}

TEST(ShouldRequeueTest, BatchRequeueRequestedAlwaysRequeues) {
  Ctld::JobInCtld job;
  job.type = crane::grpc::Batch;
  job.SetRequeueRequested(true);
  job.SetStatus(crane::grpc::Completed);
  EXPECT_TRUE(job.ShouldRequeue());
}

TEST(ShouldRequeueTest, NoRequeueFlagPreventsRequeue) {
  Ctld::JobInCtld job;
  job.type = crane::grpc::Batch;
  job.MutableJobToCtld()->set_no_requeue(true);
  job.SetExitCode(ExitCode::EC_CRANED_DOWN);
  EXPECT_FALSE(job.ShouldRequeue());
}

TEST(ShouldRequeueTest, SystemFailureRequeuesWithoutRequeueFlag) {
  Ctld::JobInCtld job;
  job.type = crane::grpc::Batch;
  job.requeue_if_failed = false;
  job.SetExitCode(ExitCode::EC_CRANED_DOWN);
  EXPECT_TRUE(job.ShouldRequeue());

  job.SetExitCode(ExitCode::EC_RPC_ERR);
  EXPECT_TRUE(job.ShouldRequeue());
}

TEST(ShouldRequeueTest, RequeueIfFailedOnlyForNonSuccessNonCancel) {
  Ctld::JobInCtld job;
  job.type = crane::grpc::Batch;
  job.requeue_if_failed = true;

  job.SetStatus(crane::grpc::Failed);
  job.SetExitCode(1U);
  EXPECT_TRUE(job.ShouldRequeue());

  job.SetStatus(crane::grpc::Completed);
  EXPECT_FALSE(job.ShouldRequeue());

  job.SetStatus(crane::grpc::Cancelled);
  EXPECT_FALSE(job.ShouldRequeue());
}

TEST(ShouldRequeueTest, NoRequeueByDefault) {
  Ctld::JobInCtld job;
  job.type = crane::grpc::Batch;
  job.requeue_if_failed = false;
  job.SetStatus(crane::grpc::Failed);
  job.SetExitCode(1U);
  EXPECT_FALSE(job.ShouldRequeue());
}

// --- ResetForRequeue tests ---

TEST(ResetForRequeueTest, IncrementsCountAndResetsState) {
  Ctld::JobInCtld job;
  job.type = crane::grpc::Batch;
  job.SetJobId(100);
  job.SetStatus(crane::grpc::Failed);
  job.SetExitCode(1U);
  job.SetRequeueRequested(true);
  job.SetCancelRequested(true);

  auto daemon = MakeDaemonStep(&job, {"node-a"});
  job.SetDaemonStep(std::move(daemon));
  auto primary = MakePrimaryStep(&job, {"node-a"});
  job.SetPrimaryStep(std::move(primary));

  EXPECT_EQ(job.RequeueCount(), 0);

  job.ResetForRequeue();

  EXPECT_EQ(job.RequeueCount(), 1);
  EXPECT_EQ(job.Status(), crane::grpc::Pending);
  EXPECT_EQ(job.ExitCode(), 0U);
  EXPECT_FALSE(job.RequeueRequested());
  EXPECT_FALSE(job.CancelRequested());
  EXPECT_EQ(job.DaemonStep(), nullptr);
  EXPECT_EQ(job.PrimaryStep(), nullptr);

  job.ResetForRequeue();
  EXPECT_EQ(job.RequeueCount(), 2);
}

// --- Daemon AllNodesCompleting tests ---

TEST(CtldStepStateMachineTest,
     DaemonAllNodesCompletingTriggersCleanup) {
  constexpr job_id_t kJobId = 50;
  const std::vector<CranedId> craned_ids{"node-a", "node-b"};

  Ctld::JobInCtld job;
  job.type = crane::grpc::Batch;
  job.SetJobId(kJobId);
  job.SetStatus(crane::grpc::Running);
  job.SetPrimaryStepStatus(crane::grpc::JobStatus::Invalid);
  job.SetDaemonStep(MakeDaemonStep(&job, craned_ids));
  job.DaemonStep()->SetConfiguringNodes({});
  job.DaemonStep()->SetStatus(crane::grpc::JobStatus::Running);

  Ctld::StepStatusChangeContext context;
  auto* daemon_step = job.DaemonStep();

  auto result =
      daemon_step->StepStatusChange(crane::grpc::JobStatus::Completing, 0U, "",
                                    "node-a", TimestampAt(500), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(daemon_step->Status(), crane::grpc::JobStatus::Running);
  EXPECT_TRUE(context.craned_step_free_map.empty());

  result =
      daemon_step->StepStatusChange(crane::grpc::JobStatus::Completing, 0U, "",
                                    "node-b", TimestampAt(501), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(daemon_step->Status(), crane::grpc::JobStatus::Completing);
  ExpectStepFreeRequested(context, "node-a", kJobId, kDaemonStepId);
  ExpectStepFreeRequested(context, "node-b", kJobId, kDaemonStepId);
}

TEST(CtldStepStateMachineTest,
     DaemonPartialCompletingDoesNotTriggerCleanup) {
  constexpr job_id_t kJobId = 51;
  const std::vector<CranedId> craned_ids{"node-a", "node-b", "node-c"};

  Ctld::JobInCtld job;
  job.type = crane::grpc::Batch;
  job.SetJobId(kJobId);
  job.SetStatus(crane::grpc::Running);
  job.SetPrimaryStepStatus(crane::grpc::JobStatus::Invalid);
  job.SetDaemonStep(MakeDaemonStep(&job, craned_ids));
  job.DaemonStep()->SetConfiguringNodes({});
  job.DaemonStep()->SetStatus(crane::grpc::JobStatus::Running);

  Ctld::StepStatusChangeContext context;
  auto* daemon_step = job.DaemonStep();

  auto result =
      daemon_step->StepStatusChange(crane::grpc::JobStatus::Completing, 0U, "",
                                    "node-a", TimestampAt(510), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(daemon_step->Status(), crane::grpc::JobStatus::Running);
  EXPECT_TRUE(context.craned_step_free_map.empty());

  result =
      daemon_step->StepStatusChange(crane::grpc::JobStatus::Completing, 0U, "",
                                    "node-b", TimestampAt(511), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(daemon_step->Status(), crane::grpc::JobStatus::Running);
  EXPECT_TRUE(context.craned_step_free_map.empty());
}

// --- Primary finished but daemon still pending ---

TEST(CtldStepStateMachineTest,
     PrimaryFinishedDoesNotFinishJobWhenDaemonStillRunning) {
  constexpr job_id_t kJobId = 52;
  const std::vector<CranedId> craned_ids{"node-a"};

  Ctld::JobInCtld job;
  job.type = crane::grpc::Batch;
  job.SetJobId(kJobId);
  job.SetStatus(crane::grpc::Running);
  job.SetPrimaryStepStatus(crane::grpc::JobStatus::Invalid);
  job.SetDaemonStep(MakeDaemonStep(&job, craned_ids));
  job.DaemonStep()->SetConfiguringNodes({});
  job.DaemonStep()->SetStatus(crane::grpc::JobStatus::Running);
  job.SetPrimaryStep(MakePrimaryStep(&job, craned_ids));

  auto* primary_step = job.PrimaryStep();
  primary_step->SetConfiguringNodes({});
  primary_step->SetStatus(crane::grpc::JobStatus::Running);

  Ctld::StepStatusChangeContext context;

  auto result =
      primary_step->StepStatusChange(crane::grpc::JobStatus::Completing, 0U, "",
                                     "node-a", TimestampAt(520), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(primary_step->Status(), crane::grpc::JobStatus::Completing);

  result =
      primary_step->StepStatusChange(crane::grpc::JobStatus::Completed, 0U, "",
                                     "node-a", TimestampAt(521), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(job.PrimaryStep(), nullptr);
  EXPECT_EQ(job.PrimaryStepStatus(), crane::grpc::JobStatus::Completed);
  EXPECT_EQ(job.PrimaryStepExitCode(), 0U);
  ExpectStepFreeRequested(context, "node-a", kJobId, kDaemonStepId);

  EXPECT_NE(job.DaemonStep(), nullptr);
}

// --- Tests requiring EmbeddedDbClient (daemon Configuring→Running path) ---

class StepLifecycleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    tmp_dir_ = std::filesystem::temp_directory_path() /
               ("crane_test_" + std::to_string(::getpid()));
    std::filesystem::create_directories(tmp_dir_);
    g_config.CraneEmbeddedDbBackend = "Unqlite";
    g_embedded_db_client =
        std::make_unique<Ctld::EmbeddedDbClient>();
    ASSERT_TRUE(g_embedded_db_client->Init(tmp_dir_.string()));
  }
  void TearDown() override {
    g_embedded_db_client.reset();
    std::filesystem::remove_all(tmp_dir_);
  }
  std::filesystem::path tmp_dir_;
};

TEST_F(StepLifecycleTest,
       DaemonNormalConfigureToRunningCreatesPrimaryStep) {
  constexpr job_id_t kJobId = 60;
  const std::vector<CranedId> craned_ids{"node-a"};

  Ctld::JobInCtld job;
  job.type = crane::grpc::Batch;
  job.SetJobId(kJobId);
  job.SetStatus(crane::grpc::JobStatus::Configuring);
  job.SetPrimaryStepStatus(crane::grpc::JobStatus::Invalid);
  job.time_limit = absl::Hours(1);
  SetupJobAllocation(job, craned_ids);
  job.SetDaemonStep(MakeDaemonStep(&job, craned_ids));
  ASSERT_TRUE(g_embedded_db_client->AppendSteps({job.DaemonStep()}));

  Ctld::StepStatusChangeContext context;
  auto* daemon_step = job.DaemonStep();

  auto result =
      daemon_step->StepStatusChange(crane::grpc::JobStatus::Running, 0U, "",
                                    "node-a", TimestampAt(600), &context);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(daemon_step->Status(), crane::grpc::JobStatus::Running);
  EXPECT_NE(job.PrimaryStep(), nullptr);
  EXPECT_EQ(job.PrimaryStep()->StepType(), crane::grpc::StepType::PRIMARY);
  EXPECT_FALSE(context.craned_step_alloc_map.empty());
}

TEST_F(StepLifecycleTest, FullJobLifecycleDaemonAndPrimary) {
  constexpr job_id_t kJobId = 61;
  const std::vector<CranedId> craned_ids{"node-a"};

  Ctld::JobInCtld job;
  job.type = crane::grpc::Batch;
  job.SetJobId(kJobId);
  job.SetStatus(crane::grpc::JobStatus::Configuring);
  job.SetPrimaryStepStatus(crane::grpc::JobStatus::Invalid);
  job.time_limit = absl::Hours(1);
  SetupJobAllocation(job, craned_ids);
  job.SetDaemonStep(MakeDaemonStep(&job, craned_ids));
  ASSERT_TRUE(g_embedded_db_client->AppendSteps({job.DaemonStep()}));

  // Phase 1: Daemon Configuring → Running (creates primary step)
  {
    Ctld::StepStatusChangeContext ctx;
    auto result =
        job.DaemonStep()->StepStatusChange(crane::grpc::JobStatus::Running, 0U,
                                           "", "node-a", TimestampAt(700), &ctx);
    ASSERT_FALSE(result.has_value());
    ASSERT_NE(job.PrimaryStep(), nullptr);
  }

  auto* daemon_step = job.DaemonStep();
  auto* primary_step = job.PrimaryStep();

  // Phase 2: Primary Configuring → Running
  {
    Ctld::StepStatusChangeContext ctx;
    auto result = primary_step->StepStatusChange(
        crane::grpc::JobStatus::Starting, 0U, "", "node-a", TimestampAt(701),
        &ctx);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(primary_step->Status(), crane::grpc::JobStatus::Running);
    ExpectStepExecRequested(ctx, "node-a", kJobId, Ctld::kPrimaryStepId);
  }

  // Phase 3: Primary Completing → Terminal
  {
    Ctld::StepStatusChangeContext ctx;
    auto result = primary_step->StepStatusChange(
        crane::grpc::JobStatus::Completing, 0U, "", "node-a", TimestampAt(702),
        &ctx);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(primary_step->Status(), crane::grpc::JobStatus::Completing);

    result = primary_step->StepStatusChange(
        crane::grpc::JobStatus::Completed, 0U, "", "node-a", TimestampAt(703),
        &ctx);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(job.PrimaryStep(), nullptr);
    EXPECT_EQ(job.PrimaryStepStatus(), crane::grpc::JobStatus::Completed);
    ExpectStepFreeRequested(ctx, "node-a", kJobId, kDaemonStepId);
  }

  // Phase 4: Daemon Completing → Terminal → Job finished
  {
    Ctld::StepStatusChangeContext ctx;
    auto result = daemon_step->StepStatusChange(
        crane::grpc::JobStatus::Completing, 0U, "", "node-a", TimestampAt(704),
        &ctx);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(daemon_step->Status(), crane::grpc::JobStatus::Completing);
    ExpectStepFreeRequested(ctx, "node-a", kJobId, kDaemonStepId);

    result = daemon_step->StepStatusChange(crane::grpc::JobStatus::Completed,
                                           0U, "", "node-a", TimestampAt(705),
                                           &ctx);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->first, crane::grpc::JobStatus::Completed);
    EXPECT_EQ(result->second, 0U);
    EXPECT_EQ(job.DaemonStep(), nullptr);
  }
}

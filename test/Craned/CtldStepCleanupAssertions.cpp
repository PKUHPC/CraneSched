#include "CtldPublicDefs.h"
// Precompiled header comes first.

#include <gtest/gtest.h>

#include "protos/Crane.pb.h"

namespace Craned {
// Keep Ctld and Craned globals in separate translation units while testing the
// actual state machine against the status sequence emitted by JobManager.
void ExpectCascadeResultInCtld(
    job_id_t job_id,
    const std::vector<crane::grpc::StepStatusChangeRequest>& changes,
    crane::grpc::JobStatus expected_status, uint32_t expected_exit_code) {
  Ctld::JobInCtld job;
  job.type = crane::grpc::Batch;
  job.SetJobId(job_id);
  job.SetStatus(crane::grpc::Running);
  job.SetPrimaryStepStatus(crane::grpc::Invalid);
  auto daemon = std::make_unique<Ctld::DaemonStepInCtld>();
  auto primary = std::make_unique<Ctld::CommonStepInCtld>();
  for (Ctld::StepInCtld* step :
       {static_cast<Ctld::StepInCtld*>(daemon.get()),
        static_cast<Ctld::StepInCtld*>(primary.get())}) {
    step->job = &job;
    step->job_id = job_id;
    step->type = crane::grpc::Batch;
    step->SetExecutionNodes({"test-node"});
    step->SetRunningNodes({"test-node"});
    step->SetPendingFinalStatus(crane::grpc::Invalid);
    step->SetPendingFinalExitCode(0);
  }
  daemon->SetStepType(crane::grpc::DAEMON);
  daemon->SetStepId(kDaemonStepId);
  daemon->SetStatus(crane::grpc::Running);
  primary->SetStepType(crane::grpc::PRIMARY);
  primary->SetStepId(Ctld::kPrimaryStepId);
  primary->SetStatus(crane::grpc::Configuring);
  primary->SetConfiguringNodes({"test-node"});
  job.SetDaemonStep(std::move(daemon));
  job.SetPrimaryStep(std::move(primary));

  Ctld::StepStatusChangeContext context;
  std::optional<std::pair<crane::grpc::JobStatus, uint32_t>> job_result;
  for (const auto& change : changes) {
    auto apply = [&](auto* step) {
      ASSERT_NE(step, nullptr);
      auto result = step->StepStatusChange(
          change.new_status(), change.exit_code(), change.reason(), "test-node",
          change.timestamp(), &context);
      if (result) {
        EXPECT_FALSE(job_result.has_value());
        job_result = result;
      }
    };
    if (change.step_id() == kDaemonStepId)
      apply(job.DaemonStep());
    else
      apply(job.PrimaryStep());
  }
  EXPECT_TRUE(job.AllStepsFinished());
  EXPECT_EQ(job.PrimaryStepStatus(), expected_status);
  EXPECT_EQ(job.PrimaryStepExitCode(), expected_exit_code);
  ASSERT_TRUE(job_result.has_value());
  EXPECT_EQ(job_result->first, expected_status);
  EXPECT_EQ(job_result->second, expected_exit_code);
  EXPECT_TRUE(context.craned_step_free_map.at("test-node")
                  .at(job_id)
                  .contains(Ctld::kPrimaryStepId));
}
}  // namespace Craned

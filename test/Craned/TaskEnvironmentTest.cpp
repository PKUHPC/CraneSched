// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include "cri/api.pb.h"
// CRI signal enums must be included before system signal macros.

#include <gtest/gtest.h>

#include "../../src/Craned/Supervisor/TaskManager.h"

namespace Craned::Supervisor {
namespace {

class TaskEnvironmentTest : public ::testing::Test {
 protected:
  void SetUp() override {
    original_node_ = g_config.CranedIdOfThisNode;
    original_env_ = g_config.JobEnv;
    original_slurm_ = g_config.EnableSlurmCompatibleEnv;
    auto hostname = util::os::GetHostname();
    ASSERT_TRUE(hostname);
    g_config.CranedIdOfThisNode = hostname.value() + "-scheduler-alias";
    g_config.JobEnv = {};
    g_config.EnableSlurmCompatibleEnv = false;

    spec_.set_type(crane::grpc::Batch);
    spec_.set_step_type(crane::grpc::PRIMARY);
    spec_.add_nodelist(g_config.CranedIdOfThisNode);
    spec_.add_task_node_list(0);
    (*spec_.mutable_env())["HOSTNAME"] = "submission-host";
    (*spec_.mutable_env())["ENVIRONMENT"] = "submission-environment";
  }

  void TearDown() override {
    g_config.CranedIdOfThisNode = std::move(original_node_);
    g_config.JobEnv = std::move(original_env_);
    g_config.EnableSlurmCompatibleEnv = original_slurm_;
  }

  StepToSupv spec_;

 private:
  std::string original_node_;
  EnvMap original_env_;
  bool original_slurm_{};
};

TEST_F(TaskEnvironmentTest, BatchHostnameUsesTheOperatingSystemName) {
  auto hostname = util::os::GetHostname();
  ASSERT_TRUE(hostname);
  ASSERT_NE(hostname.value(), g_config.CranedIdOfThisNode);
  for (bool pod : {false, true}) {
    spec_.set_type(pod ? crane::grpc::Container : crane::grpc::Batch);
    if (pod) spec_.mutable_batch_meta();
    StepInstance step(spec_);
    const auto env = step.GetStepProcessEnv();
    EXPECT_EQ(env.at("HOSTNAME"), hostname.value());
    EXPECT_EQ(env.at("ENVIRONMENT"), "BATCH");
  }
}

TEST_F(TaskEnvironmentTest, OtherStepsPreserveSubmittedHostname) {
  for (auto type :
       {crane::grpc::DAEMON, crane::grpc::PRIMARY, crane::grpc::COMMON}) {
    spec_.set_step_type(type);
    spec_.set_type(crane::grpc::Interactive);
    spec_.mutable_interactive_meta()->set_interactive_type(
        type == crane::grpc::PRIMARY ? crane::grpc::Calloc : crane::grpc::Crun);
    StepInstance step(spec_);
    const auto env = step.GetStepProcessEnv();
    EXPECT_EQ(env.at("HOSTNAME"), "submission-host");
    EXPECT_EQ(env.at("ENVIRONMENT"), "submission-environment");
    step.GetMutableStep().mutable_env()->clear();
    const auto empty_env = step.GetStepProcessEnv();
    EXPECT_FALSE(empty_env.contains("HOSTNAME"));
    EXPECT_FALSE(empty_env.contains("ENVIRONMENT"));
  }
}

}  // namespace
}  // namespace Craned::Supervisor

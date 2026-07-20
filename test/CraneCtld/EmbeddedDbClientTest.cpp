/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <unistd.h>

#include "Database/EmbeddedDbClient.h"
#include "crane/Logger.h"

namespace fs = std::filesystem;

namespace Ctld {

void JobInCtld::SetJobId(job_id_t id) {
  job_id = id;
  runtime_attr.set_job_id(id);
}

void JobInCtld::SetJobDbId(job_db_id_t id) {
  job_db_id = id;
  runtime_attr.set_job_db_id(id);
}

void JobInCtld::SetUsername(std::string const& val) {
  username = val;
  runtime_attr.set_username(val);
}

void JobInCtld::SetStatus(crane::grpc::JobStatus val) {
  status = val;
  runtime_attr.set_status(val);
}

void StepInCtld::SetStepType(crane::grpc::StepType type) {
  step_type = type;
  m_runtime_attr_.set_step_type(type);
}

const crane::grpc::StepToCtld& StepInCtld::StepToCtld() const {
  return m_step_to_ctld_;
}

crane::grpc::StepToCtld* StepInCtld::MutableStepToCtld() {
  return &m_step_to_ctld_;
}

void StepInCtld::SetStepId(step_id_t id) {
  m_step_id_ = id;
  m_runtime_attr_.set_step_id(id);
}

void StepInCtld::SetStepDbId(step_db_id_t id) {
  m_step_db_id_ = id;
  m_runtime_attr_.set_step_db_id(id);
}

void StepInCtld::SetStatus(crane::grpc::JobStatus new_status) {
  m_status_ = new_status;
  m_runtime_attr_.set_status(new_status);
}

void StepInCtld::RecoverFromDb(
    const JobInCtld& /*job*/,
    const crane::grpc::StepInEmbeddedDb& /*step_in_db*/) {}

void StepInCtld::SetFieldsOfStepInfo(
    crane::grpc::StepInfo* /*step_info*/) const noexcept {}

}  // namespace Ctld

namespace {

class TestStepInCtld final : public Ctld::StepInCtld {
 public:
  [[nodiscard]] crane::grpc::StepToD GetStepToD(
      const CranedId& /*craned_id*/) const override {
    return {};
  }
};

std::string DbBasePath(std::string_view backend) {
  return fmt::format("{}/embedded_db_client_test_{}_{}", CRANE_BUILD_DIRECTORY,
                     backend, ::getpid());
}

void CleanupDbPath(const std::string& db_path) {
  fs::remove_all(db_path);
  fs::remove_all(db_path + ".rocksdb");
  fs::remove(db_path + "var");
  fs::remove(db_path + "fix");
  fs::remove(db_path + "resv");
  fs::remove(db_path + "step_var");
  fs::remove(db_path + "step_fix");
}

std::unique_ptr<Ctld::JobInCtld> MakeJob(const std::string& name,
                                         crane::grpc::JobStatus status) {
  auto job = std::make_unique<Ctld::JobInCtld>();
  job->MutableJobToCtld()->set_name(name);
  job->MutableJobToCtld()->set_type(crane::grpc::Batch);
  job->SetUsername("crane-test");
  job->SetStatus(status);
  return job;
}

std::unique_ptr<TestStepInCtld> MakeStep(Ctld::JobInCtld* job,
                                         const std::string& name) {
  auto step = std::make_unique<TestStepInCtld>();
  step->job = job;
  step->job_id = job->JobId();
  step->type = crane::grpc::Batch;
  step->SetStepType(crane::grpc::COMMON);
  step->MutableStepToCtld()->set_job_id(job->JobId());
  step->MutableStepToCtld()->set_type(crane::grpc::Batch);
  step->MutableStepToCtld()->set_name(name);
  step->SetStatus(crane::grpc::Pending);
  return step;
}

void RunEmbeddedDbSmoke(const std::string& backend) {
  g_config.CraneEmbeddedDbBackend = backend;
  g_config.RocksDb.SyncWrites = true;

  const std::string db_path = DbBasePath(backend);
  CleanupDbPath(db_path);

  auto client = std::make_unique<Ctld::EmbeddedDbClient>();
  ASSERT_TRUE(client->Init(db_path));

  auto pending = MakeJob("pending-job", crane::grpc::Pending);
  auto running = MakeJob("running-job", crane::grpc::Running);

  std::vector<Ctld::JobInCtld*> jobs{pending.get(), running.get()};
  ASSERT_TRUE(client->AppendJobsToPendingAndAdvanceJobIds(jobs));
  EXPECT_EQ(pending->JobId(), 1);
  EXPECT_EQ(pending->JobDbId(), 1);
  EXPECT_EQ(running->JobId(), 2);
  EXPECT_EQ(running->JobDbId(), 2);

  crane::grpc::JobInEmbeddedDb job_in_db;
  ASSERT_TRUE(client->FetchJobDataInDb(0, pending->JobDbId(), &job_in_db));
  EXPECT_EQ(job_in_db.job_to_ctld().name(), "pending-job");
  EXPECT_EQ(job_in_db.runtime_attr().job_id(), pending->JobId());
  EXPECT_EQ(job_in_db.runtime_attr().job_db_id(), pending->JobDbId());
  EXPECT_EQ(job_in_db.runtime_attr().status(), crane::grpc::Pending);

  pending->SetStatus(crane::grpc::Completed);
  Ctld::txn_id_t txn_id = 0;
  ASSERT_TRUE(client->BeginVariableDbTransaction(&txn_id));
  ASSERT_TRUE(client->UpdateRuntimeAttrOfJob(txn_id, pending->JobDbId(),
                                             pending->RuntimeAttr()));
  ASSERT_TRUE(client->CommitVariableDbTransaction(txn_id));
  job_in_db.Clear();
  ASSERT_TRUE(client->FetchJobDataInDb(0, pending->JobDbId(), &job_in_db));
  EXPECT_EQ(job_in_db.runtime_attr().status(), crane::grpc::Completed);

  auto step = MakeStep(running.get(), "running-step");
  std::vector<Ctld::StepInCtld*> steps{step.get()};
  ASSERT_TRUE(client->AppendSteps(steps));
  EXPECT_EQ(step->StepId(), 0);
  EXPECT_EQ(step->StepDbId(), 1);

  crane::grpc::StepInEmbeddedDb step_in_db;
  ASSERT_TRUE(client->FetchStepDataInDb(0, step->StepDbId(), &step_in_db));
  EXPECT_EQ(step_in_db.step_to_ctld().job_id(), running->JobId());
  EXPECT_EQ(step_in_db.step_to_ctld().name(), "running-step");
  EXPECT_EQ(step_in_db.runtime_attr().step_id(), step->StepId());
  EXPECT_EQ(step_in_db.runtime_attr().step_db_id(), step->StepDbId());

  ASSERT_TRUE(client->PurgeEndedJobs({{pending->JobId(), pending->JobDbId()}}));
  job_in_db.Clear();
  EXPECT_FALSE(client->FetchJobDataInDb(0, pending->JobDbId(), &job_in_db));

  client.reset();

  auto recovered_client = std::make_unique<Ctld::EmbeddedDbClient>();
  ASSERT_TRUE(recovered_client->Init(db_path));

  Ctld::EmbeddedDbClient::DbSnapshot snapshot;
  ASSERT_TRUE(recovered_client->RetrieveLastSnapshot(&snapshot));
  EXPECT_TRUE(snapshot.pending_queue.empty());
  EXPECT_TRUE(snapshot.final_queue.empty());
  ASSERT_EQ(snapshot.running_queue.size(), 1);
  ASSERT_TRUE(snapshot.running_queue.contains(running->JobDbId()));
  EXPECT_EQ(snapshot.running_queue.at(running->JobDbId()).job_to_ctld().name(),
            "running-job");

  Ctld::EmbeddedDbClient::StepDbSnapshot step_snapshot;
  ASSERT_TRUE(recovered_client->RetrieveStepInfo(&step_snapshot));
  ASSERT_TRUE(step_snapshot.steps.contains(running->JobId()));
  ASSERT_EQ(step_snapshot.steps.at(running->JobId()).size(), 1);
  EXPECT_EQ(step_snapshot.steps.at(running->JobId())[0].step_to_ctld().name(),
            "running-step");

  ASSERT_TRUE(recovered_client->PurgeEndedSteps({step->StepDbId()}));
  step_in_db.Clear();
  EXPECT_FALSE(
      recovered_client->FetchStepDataInDb(0, step->StepDbId(), &step_in_db));

  recovered_client.reset();
  CleanupDbPath(db_path);
}

}  // namespace

TEST(EmbeddedDbClientTest, UnqliteCurrentApiSmoke) {
  RunEmbeddedDbSmoke("Unqlite");
}

#ifdef CRANE_HAVE_ROCKSDB
TEST(EmbeddedDbClientTest, RocksDbCurrentApiSmoke) {
  RunEmbeddedDbSmoke("RocksDB");
}
#endif

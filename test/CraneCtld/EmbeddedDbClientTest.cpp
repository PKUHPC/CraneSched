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

#define private public
#include "EmbeddedDbClient.h"
#undef private

#include "crane/Logger.h"

namespace fs = std::filesystem;
using Ctld::EmbeddedDbClient;

class EmbeddedDbClientTest : public ::testing::Test {
  void SetUp() override {
    if (fs::exists(db_file)) {
      fs::remove(db_file);
    }

    g_embedded_db_client = std::make_unique<Ctld::EmbeddedDbClient>();
    ASSERT_TRUE(g_embedded_db_client->Init(db_file));
  }

  void TearDown() override {
    g_embedded_db_client.reset();
    fs::remove(db_file);
  }

  std::string db_dir{CRANE_BUILD_DIRECTORY};
  std::string db_file = fmt::format("{}/unqlite.db", db_dir);
};

TEST_F(EmbeddedDbClientTest, Simple) {
  int rc;

  uint32_t next_job_id;
  rc = g_embedded_db_client->FetchTypeFromDb_(
      EmbeddedDbClient::s_next_job_id_str_, &next_job_id);
  EXPECT_EQ(rc, UNQLITE_OK);
  EXPECT_EQ(next_job_id, 0);

  EmbeddedDbClient::db_id_t next_job_db_id;
  rc = g_embedded_db_client->FetchTypeFromDb_(
      EmbeddedDbClient::s_next_job_db_id_str_, &next_job_db_id);
  EXPECT_EQ(rc, UNQLITE_OK);
  EXPECT_EQ(next_job_db_id, 0);
}

TEST_F(EmbeddedDbClientTest, LinkList) {
  using crane::grpc::JobInEmbeddedDb;
  using crane::grpc::JobToCtld;

  int rc;
  bool ok;
  JobToCtld job_to_ctld;

  job_to_ctld.set_name("Job0");
  ok = g_embedded_db_client->AppendJobToPendingAndAdvanceJobIds(
      job_to_ctld);
  ASSERT_TRUE(ok);

  job_to_ctld.set_name("Job1");
  ok = g_embedded_db_client->AppendJobToPendingAndAdvanceJobIds(
      job_to_ctld);
  ASSERT_TRUE(ok);

  job_to_ctld.set_name("Job2");
  ok = g_embedded_db_client->AppendJobToPendingAndAdvanceJobIds(
      job_to_ctld);
  ASSERT_TRUE(ok);

  job_to_ctld.set_name("Job3");
  ok = g_embedded_db_client->AppendJobToPendingAndAdvanceJobIds(
      job_to_ctld);
  ASSERT_TRUE(ok);

  ok = g_embedded_db_client->MoveJobFromPendingToRunning(1);
  ASSERT_TRUE(ok);

  ok = g_embedded_db_client->MoveJobFromPendingToRunning(3);
  ASSERT_TRUE(ok);

  int i;

  i = 0;
  std::list<JobInEmbeddedDb> pending_list;
  ok = g_embedded_db_client->GetPendingQueueCopy(&pending_list);
  ASSERT_TRUE(ok);
  ASSERT_EQ(pending_list.size(), 2);
  std::for_each(g_embedded_db_client->m_pending_queue_.begin(),
                g_embedded_db_client->m_pending_queue_.end(),
                [&i](decltype(g_embedded_db_client
                                  ->m_pending_queue_)::value_type const& kv) {
                  // Pending Queue: head -> 2 -> 0 -> tail
                  constexpr std::array<Ctld::job_db_id_t, 2> arr{2, 0};
                  EXPECT_EQ(kv.second.db_id, arr[i++]);
                });
  ASSERT_EQ(i, 2);

  i = 0;
  std::list<JobInEmbeddedDb> running_list;
  ok = g_embedded_db_client->GetRunningQueueCopy(&running_list);
  ASSERT_TRUE(ok);
  ASSERT_EQ(running_list.size(), 2);
  std::for_each(g_embedded_db_client->m_running_queue_.begin(),
                g_embedded_db_client->m_running_queue_.end(),
                [&i](decltype(g_embedded_db_client
                                  ->m_running_queue_)::value_type const& kv) {
                  // Running Queue: head -> 1 -> 3 -> tail
                  constexpr std::array<Ctld::job_db_id_t, 2> arr{3, 1};
                  EXPECT_EQ(kv.second.db_id, arr[i++]);
                });
  ASSERT_EQ(i, 2);

  i = 0;
  rc = g_embedded_db_client->ForEachInDbQueueNoLock_(
      g_embedded_db_client->s_pending_queue_head_,
      g_embedded_db_client->s_pending_queue_tail_,
      [&i](EmbeddedDbClient::DbQueueNode const& node) {
        // Pending Queue: head -> 2 -> 0 -> tail
        constexpr std::array<Ctld::job_db_id_t, 2> arr{2, 0};
        EXPECT_EQ(node.db_id, arr[i++]);
      });
  ASSERT_EQ(rc, UNQLITE_OK);
  ASSERT_EQ(i, 2);

  i = 0;
  rc = g_embedded_db_client->ForEachInDbQueueNoLock_(
      g_embedded_db_client->s_running_queue_head_,
      g_embedded_db_client->s_running_queue_tail_,
      [&i](EmbeddedDbClient::DbQueueNode const& node) {
        // Running Queue: head -> 3 -> 1 -> tail
        constexpr std::array<Ctld::job_db_id_t, 2> arr{3, 1};
        EXPECT_EQ(node.db_id, arr[i++]);
      });
  ASSERT_EQ(rc, UNQLITE_OK);
  ASSERT_EQ(i, 2);

  JobInEmbeddedDb job_in_embedded_db;
  rc = g_embedded_db_client->FetchJobDataInDbAtomic_(3, &job_in_embedded_db);
  ASSERT_EQ(rc, UNQLITE_OK);
  ASSERT_EQ(job_in_embedded_db.persisted_part().job_id(), 3);
  ASSERT_EQ(job_in_embedded_db.persisted_part().job_db_id(), 3);
  ASSERT_EQ(job_in_embedded_db.job_to_ctld().name(), "Job3");

  job_in_embedded_db.Clear();
  ok = g_embedded_db_client->MovePendingOrRunningJobToEnded(3);
  ASSERT_TRUE(ok);
  rc = g_embedded_db_client->FetchJobDataInDbAtomic_(3, &job_in_embedded_db);
  ASSERT_EQ(rc, UNQLITE_OK);
  ASSERT_EQ(job_in_embedded_db.persisted_part().job_id(), 3);
  ASSERT_EQ(job_in_embedded_db.persisted_part().job_db_id(), 3);
  ASSERT_EQ(job_in_embedded_db.job_to_ctld().name(), "Job3");

  job_in_embedded_db.Clear();
  ok = g_embedded_db_client->MovePendingOrRunningJobToEnded(1);
  ASSERT_TRUE(ok);
  ok = g_embedded_db_client->FetchJobDataInDb(1, &job_in_embedded_db);
  ASSERT_TRUE(ok);
  ASSERT_EQ(job_in_embedded_db.persisted_part().job_id(), 1);
  ASSERT_EQ(job_in_embedded_db.persisted_part().job_db_id(), 1);
  ASSERT_EQ(job_in_embedded_db.job_to_ctld().name(), "Job1");

  job_in_embedded_db.Clear();
  ok = g_embedded_db_client->MovePendingOrRunningJobToEnded(2);
  ASSERT_TRUE(ok);
  ok = g_embedded_db_client->FetchJobDataInDb(2, &job_in_embedded_db);
  ASSERT_TRUE(ok);
  ASSERT_EQ(job_in_embedded_db.persisted_part().job_id(), 2);
  ASSERT_EQ(job_in_embedded_db.persisted_part().job_db_id(), 2);
  ASSERT_EQ(job_in_embedded_db.job_to_ctld().name(), "Job2");

  rc = g_embedded_db_client->ForEachInDbQueueNoLock_(
      g_embedded_db_client->s_running_queue_head_,
      g_embedded_db_client->s_running_queue_tail_,
      [](EmbeddedDbClient::DbQueueNode const& node) {
        // Running Queue: head -> tail
        FAIL() << "No element should exist in Pending Queue.";
      });
  ASSERT_EQ(rc, UNQLITE_OK);

  std::list<JobInEmbeddedDb> ended_list;

  i = 0;
  rc = g_embedded_db_client->ForEachInDbQueueNoLock_(
      g_embedded_db_client->s_ended_queue_head_,
      g_embedded_db_client->s_ended_queue_tail_,
      [&i](EmbeddedDbClient::DbQueueNode const& node) {
        // Ended Queue: head -> 2 -> 1 -> 3 -> tail
        constexpr std::array<Ctld::job_db_id_t, 3> arr{2, 1, 3};
        EXPECT_EQ(node.db_id, arr[i++]);
      });
  ASSERT_EQ(rc, UNQLITE_OK);
  ASSERT_EQ(i, 3);

  ok = g_embedded_db_client->PurgeEndedJobs(1);
  ASSERT_TRUE(ok);

  i = 0;
  rc = g_embedded_db_client->ForEachInDbQueueNoLock_(
      g_embedded_db_client->s_ended_queue_head_,
      g_embedded_db_client->s_ended_queue_tail_,
      [&i](EmbeddedDbClient::DbQueueNode const& node) {
        // Ended Queue: head -> 2 -> 3 -> tail
        constexpr std::array<Ctld::job_db_id_t, 2> arr{2, 3};
        EXPECT_EQ(node.db_id, arr[i++]);
      });
  ASSERT_EQ(rc, UNQLITE_OK);
  ASSERT_EQ(i, 2);

  i = 0;
  ok = g_embedded_db_client->GetEndedQueueCopy(&ended_list);
  ASSERT_TRUE(ok);
  ASSERT_EQ(ended_list.size(), 2);
  std::for_each(g_embedded_db_client->m_ended_queue_.begin(),
                g_embedded_db_client->m_ended_queue_.end(),
                [&i](decltype(g_embedded_db_client
                                  ->m_ended_queue_)::value_type const& kv) {
                  // Ended Queue: head -> 2 -> 3 -> tail
                  constexpr std::array<Ctld::job_db_id_t, 2> arr{2, 3};
                  EXPECT_EQ(kv.second.db_id, arr[i++]);
                });
  ASSERT_EQ(i, 2);

  job_in_embedded_db.Clear();
  ok = g_embedded_db_client->FetchJobDataInDb(1, &job_in_embedded_db);
  ASSERT_FALSE(ok);
}
#include <gtest/gtest.h>

#include <filesystem>

#define private public
#include "EmbeddedDbClient.h"
#undef private

#include "crane/Logger.h"

namespace fs = std::filesystem;
using Ctld::EmbeddedDbClient;
using Ctld::task_db_id_t;

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

  uint32_t next_task_id;
  rc = g_embedded_db_client->FetchTypeFromDb_(
      EmbeddedDbClient::s_next_task_id_str_, &next_task_id);
  EXPECT_EQ(rc, UNQLITE_OK);
  EXPECT_EQ(next_task_id, 0);

  EmbeddedDbClient::db_id_t next_task_db_id;
  rc = g_embedded_db_client->FetchTypeFromDb_(
      EmbeddedDbClient::s_next_task_db_id_str_, &next_task_db_id);
  EXPECT_EQ(rc, UNQLITE_OK);
  EXPECT_EQ(next_task_db_id, 0);
}

TEST_F(EmbeddedDbClientTest, LinkList) {
  using crane::grpc::PersistedPartOfTaskInCtld;
  using crane::grpc::TaskInEmbeddedDb;
  using crane::grpc::TaskToCtld;

  int rc;
  bool ok;
  TaskToCtld task_to_ctld;
  PersistedPartOfTaskInCtld persisted_part;

  task_to_ctld.set_name("Task0");
  ok = g_embedded_db_client->AppendTaskToPendingAndAdvanceTaskIds(
      task_to_ctld, &persisted_part);
  ASSERT_TRUE(ok);
  EXPECT_EQ(persisted_part.task_id(), 0);
  EXPECT_EQ(persisted_part.task_db_id(), 0);

  task_to_ctld.set_name("Task1");
  ok = g_embedded_db_client->AppendTaskToPendingAndAdvanceTaskIds(
      task_to_ctld, &persisted_part);
  ASSERT_TRUE(ok);
  EXPECT_EQ(persisted_part.task_id(), 1);
  EXPECT_EQ(persisted_part.task_db_id(), 1);

  task_to_ctld.set_name("Task2");
  ok = g_embedded_db_client->AppendTaskToPendingAndAdvanceTaskIds(
      task_to_ctld, &persisted_part);
  ASSERT_TRUE(ok);
  EXPECT_EQ(persisted_part.task_id(), 2);
  EXPECT_EQ(persisted_part.task_db_id(), 2);

  task_to_ctld.set_name("Task3");
  ok = g_embedded_db_client->AppendTaskToPendingAndAdvanceTaskIds(
      task_to_ctld, &persisted_part);
  ASSERT_TRUE(ok);
  EXPECT_EQ(persisted_part.task_id(), 3);
  EXPECT_EQ(persisted_part.task_db_id(), 3);

  ok = g_embedded_db_client->MoveTaskFromPendingToRunning(1);
  ASSERT_TRUE(ok);

  ok = g_embedded_db_client->MoveTaskFromPendingToRunning(3);
  ASSERT_TRUE(ok);

  int i;

  i = 0;
  std::list<TaskInEmbeddedDb> pending_list;
  ok = g_embedded_db_client->GetPendingQueueCopy(&pending_list);
  ASSERT_TRUE(ok);
  ASSERT_EQ(pending_list.size(), 2);
  std::for_each(g_embedded_db_client->m_pending_queue_.begin(),
                g_embedded_db_client->m_pending_queue_.end(),
                [&i](decltype(g_embedded_db_client
                                  ->m_pending_queue_)::value_type const& kv) {
                  // Pending Queue: head -> 2 -> 0 -> tail
                  constexpr std::array<Ctld::task_db_id_t, 2> arr{2, 0};
                  EXPECT_EQ(kv.second.db_id, arr[i++]);
                });

  i = 0;
  std::list<TaskInEmbeddedDb> running_list;
  ok = g_embedded_db_client->GetRunningQueueCopy(&running_list);
  ASSERT_TRUE(ok);
  ASSERT_EQ(running_list.size(), 2);
  std::for_each(g_embedded_db_client->m_running_queue_.begin(),
                g_embedded_db_client->m_running_queue_.end(),
                [&i](decltype(g_embedded_db_client
                                  ->m_running_queue_)::value_type const& kv) {
                  // Running Queue: head -> 1 -> 3 -> tail
                  constexpr std::array<Ctld::task_db_id_t, 2> arr{3, 1};
                  EXPECT_EQ(kv.second.db_id, arr[i++]);
                });

  i = 0;
  rc = g_embedded_db_client->ForEachInDbQueueNoLockAndTxn_(
      g_embedded_db_client->s_pending_queue_head_,
      g_embedded_db_client->s_pending_queue_tail_,
      [&i](EmbeddedDbClient::DbQueueNode const& node) {
        // Pending Queue: head -> 2 -> 0 -> tail
        constexpr std::array<Ctld::task_db_id_t, 2> arr{2, 0};
        EXPECT_EQ(node.db_id, arr[i++]);
      });
  ASSERT_EQ(rc, UNQLITE_OK);

  i = 0;
  rc = g_embedded_db_client->ForEachInDbQueueNoLockAndTxn_(
      g_embedded_db_client->s_running_queue_head_,
      g_embedded_db_client->s_running_queue_tail_,
      [&i](EmbeddedDbClient::DbQueueNode const& node) {
        // Running Queue: head -> 3 -> 1 -> tail
        constexpr std::array<Ctld::task_db_id_t, 2> arr{3, 1};
        EXPECT_EQ(node.db_id, arr[i++]);
      });
  ASSERT_EQ(rc, UNQLITE_OK);

  task_db_id_t db_id;
  ok = g_embedded_db_client->GetMarkedDbId(&db_id);
  ASSERT_FALSE(ok);

  ok = g_embedded_db_client->SetMarkedDbId(1);
  ASSERT_TRUE(ok);

  ok = g_embedded_db_client->GetMarkedDbId(&db_id);
  ASSERT_TRUE(ok);
  ASSERT_EQ(db_id, 1);

  ok = g_embedded_db_client->UnsetMarkedDbId();
  ASSERT_TRUE(ok);

  ok = g_embedded_db_client->GetMarkedDbId(&db_id);
  ASSERT_FALSE(ok);

  TaskInEmbeddedDb task_in_embedded_db;
  rc = g_embedded_db_client->FetchTaskDataInDbAtomic_(3, &task_in_embedded_db);
  ASSERT_EQ(rc, UNQLITE_OK);
  ASSERT_EQ(task_in_embedded_db.persisted_part().task_id(), 3);
  ASSERT_EQ(task_in_embedded_db.persisted_part().task_db_id(), 3);
  ASSERT_EQ(task_in_embedded_db.task_to_ctld().name(), "Task3");

  task_in_embedded_db.Clear();
  ok = g_embedded_db_client->DeleteTaskByDbId(3);
  ASSERT_TRUE(ok);
  rc = g_embedded_db_client->FetchTaskDataInDbAtomic_(3, &task_in_embedded_db);
  ASSERT_EQ(rc, UNQLITE_NOTFOUND);

  task_in_embedded_db.Clear();
  ok = g_embedded_db_client->DeleteTaskByDbId(1);
  ASSERT_TRUE(ok);
  ok = g_embedded_db_client->FetchTaskDataInDb(1, &task_in_embedded_db);
  ASSERT_FALSE(ok);

  rc = g_embedded_db_client->ForEachInDbQueueNoLockAndTxn_(
      g_embedded_db_client->s_running_queue_head_,
      g_embedded_db_client->s_running_queue_tail_,
      [](EmbeddedDbClient::DbQueueNode const& node) {
        // Running Queue: head -> tail
        FAIL() << "No element should exist in Pending Queue.";
      });
  ASSERT_EQ(rc, UNQLITE_OK);
}
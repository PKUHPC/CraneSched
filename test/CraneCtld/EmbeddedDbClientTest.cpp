#include <gtest/gtest.h>

#include <filesystem>

#define private public

#include "EmbeddedDbClient.h"
#include "crane/Logger.h"

namespace fs = std::filesystem;
using Ctld::EmbeddedDbClient;

TEST(EmbeddedDbClient, Simple) {
  int rc;

  std::string db_dir{CRANE_BUILD_DIRECTORY};
  std::string db_file = fmt::format("{}/unqlite.db", db_dir);

  g_embedded_db_client = std::make_unique<Ctld::EmbeddedDbClient>();

  if (fs::exists(db_file)) {
    fs::remove(db_file);
  }

  g_embedded_db_client->Init(db_file);

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

  g_embedded_db_client.reset();
}
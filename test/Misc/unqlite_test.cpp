#include <gtest/gtest.h>
#include <spdlog/fmt/fmt.h>
#include <unqlite.h>

TEST(Unqlite, Simple) {
  std::string db_dir{CRANE_BUILD_DIRECTORY};
  std::string db_file = fmt::format("{}/unqlite.db", db_dir);
  int i, rc;
  unqlite *pDb;

  // Open our database;
  rc = unqlite_open(&pDb, db_file.c_str(), UNQLITE_OPEN_CREATE);
  if (rc != UNQLITE_OK) {
    GTEST_FAIL();
  }

  // Store some records
  rc = unqlite_kv_store(pDb, "test", -1, "Hello World",
                        11);  // test => 'Hello World'
  if (rc != UNQLITE_OK) {
    // Insertion fail, Hande error (See below)
    GTEST_FAIL();
  }
  // A small formatted string
  rc = unqlite_kv_store_fmt(pDb, "date", -1, "Current date: %d:%d:%d", 2013, 06,
                            07);
  if (rc != UNQLITE_OK) {
    // Insertion fail, Hande error (See below)
    GTEST_FAIL();
  }

  // Switch to the append interface
  rc = unqlite_kv_append(pDb, "msg", -1, "Hello, ", 7);  // msg => 'Hello, '
  if (rc == UNQLITE_OK) {
    // The second chunk
    rc = unqlite_kv_append(pDb, "msg", -1, "Current time is: ",
                           17);  // msg => 'Hello, Current time is: '
    if (rc == UNQLITE_OK) {
      // The last formatted chunk
      rc = unqlite_kv_append_fmt(
          pDb, "msg", -1, "%d:%d:%d", 10, 16,
          53);  // msg => 'Hello, Current time is: 10:16:53'
    }
  }

  // Delete a record
  unqlite_kv_delete(pDb, "test", -1);

  // Store 20 random records.
  for (i = 0; i < 20; ++i) {
    char zKey[12];   // Random generated key
    char zData[34];  // Dummy data

    // generate the random key
    unqlite_util_random_string(pDb, zKey, sizeof(zKey));

    // Perform the insertion
    rc = unqlite_kv_store(pDb, zKey, sizeof(zKey), zData, sizeof(zData));
    if (rc != UNQLITE_OK) {
      break;
    }
  }

  if (rc != UNQLITE_OK) {
    // Insertion fail, Handle error
    const char *zBuf;
    int iLen;
    /* Something goes wrong, extract the database error log */
    unqlite_config(pDb, UNQLITE_CONFIG_ERR_LOG, &zBuf, &iLen);
    if (iLen > 0) {
      puts(zBuf);
    }
    if (rc != UNQLITE_BUSY && rc != UNQLITE_NOTIMPLEMENTED) {
      /* Rollback */
      unqlite_rollback(pDb);
    }
  }

  // Auto-commit the transaction and close our handle.
  unqlite_close(pDb);
}
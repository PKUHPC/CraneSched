#include <google/protobuf/io/coded_stream.h>
#include <gtest/gtest.h>
#include <spdlog/fmt/fmt.h>
#include <unqlite.h>

#include "protos/math.pb.h"

std::string db_dir{CRANE_BUILD_DIRECTORY};
std::string db_file = fmt::format("{}/unqlite.db", db_dir);

TEST(Unqlite, Simple) {
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

TEST(Unqlite, Protobuf) {
  int i, rc;
  unqlite *pDb;

  using google::protobuf::io::CodedOutputStream;
  using google::protobuf::io::StringOutputStream;
  using grpc_example::MaxRequest;

  std::string buf;
  StringOutputStream stringOutputStream(&buf);
  CodedOutputStream codedOutputStream(&stringOutputStream);

  MaxRequest request1;
  request1.set_a(1);
  request1.set_b(2);

  MaxRequest request2;
  request2.set_a(3);
  request2.set_b(4);

  std::size_t TotalSize{0};

  codedOutputStream.WriteLittleEndian32(request1.ByteSizeLong());
  request1.SerializeToCodedStream(&codedOutputStream);
  GTEST_LOG_(INFO) << "request1 size: " << request1.GetCachedSize();
  codedOutputStream.WriteLittleEndian32(request2.ByteSizeLong());
  request2.SerializeToCodedStream(&codedOutputStream);
  GTEST_LOG_(INFO) << "request2 size: " << request2.GetCachedSize();

  TotalSize += 2 * sizeof(int32_t);
  TotalSize += request1.GetCachedSize();
  TotalSize += request2.GetCachedSize();
  GTEST_LOG_(INFO) << "Total size: " << TotalSize;

  // Open our database;
  rc = unqlite_open(&pDb, db_file.c_str(), UNQLITE_OPEN_CREATE);
  if (rc != UNQLITE_OK) {
    GTEST_FAIL();
  }

  // Store some records
  rc = unqlite_kv_store(pDb, "protobuf", -1, buf.data(),
                        static_cast<unqlite_int64>(TotalSize));
  if (rc != UNQLITE_OK) {
    // Insertion fail, Hande error (See below)
    GTEST_FAIL();
  }

  unqlite_int64 nBytes;

  // Extract data size first
  rc = unqlite_kv_fetch(pDb, "protobuf", -1, nullptr, &nBytes);
  if (rc != UNQLITE_OK) {
    return;
  }
  GTEST_LOG_(INFO) << "Total size in DB: " << nBytes;

  std::string r_buf;
  r_buf.resize(nBytes);

  // Copy record content in our buffer
  unqlite_kv_fetch(pDb, "protobuf", -1, r_buf.data(), &nBytes);

  using google::protobuf::io::ArrayInputStream;
  using google::protobuf::io::CodedInputStream;

  ArrayInputStream arrayInputStream(r_buf.data(), nBytes);
  CodedInputStream codedInputStream(&arrayInputStream);
  uint32_t usz;
  int sz;
  const void *p;

  GTEST_LOG_(INFO) << "Current Pos: " << codedInputStream.CurrentPosition();
  codedInputStream.ReadLittleEndian32(&usz);
  GTEST_LOG_(INFO) << "Current Pos: " << codedInputStream.CurrentPosition();
  GTEST_LOG_(INFO) << "usz: " << usz;
  sz = static_cast<int>(usz);
  ASSERT_TRUE(codedInputStream.GetDirectBufferPointer(&p, &sz));
  GTEST_LOG_(INFO) << "Sz: " << sz;
  MaxRequest request1_r;
  EXPECT_TRUE(request1_r.ParseFromArray(p, usz));
  codedInputStream.Skip(usz);
  GTEST_LOG_(INFO) << "Current Pos: " << codedInputStream.CurrentPosition();

  codedInputStream.ReadLittleEndian32(&usz);
  sz = static_cast<int>(usz);
  ASSERT_TRUE(codedInputStream.GetDirectBufferPointer(&p, &sz));
  MaxRequest request2_r;
  EXPECT_TRUE(request2_r.ParseFromArray(p, usz));
  codedInputStream.Skip(usz);
  GTEST_LOG_(INFO) << "Current Pos: " << codedInputStream.CurrentPosition();

  // Reverse order
  EXPECT_EQ(request1_r.a(), 1);
  EXPECT_EQ(request1_r.b(), 2);
  EXPECT_EQ(request2_r.a(), 3);
  EXPECT_EQ(request2_r.b(), 4);

  // Auto-commit the transaction and close our handle.
  unqlite_close(pDb);
}
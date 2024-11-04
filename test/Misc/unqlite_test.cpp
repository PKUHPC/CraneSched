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

#include <google/protobuf/io/coded_stream.h>
#include <gtest/gtest.h>
#include <spdlog/fmt/fmt.h>
#include <unqlite.h>

#include "protos/math.pb.h"

std::string db_dir{CRANE_BUILD_DIRECTORY};
std::string db_file = std::format("{}/unqlite.db", db_dir);

void PrintErrorAndRollback(unqlite *pDb, int rc) {
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
}

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

  // Auto-commit the transaction and close our handle.
  unqlite_close(pDb);
}

TEST(Unqlite, Transaction) {
  int i, rc;
  unqlite *pDb;
  unqlite_int64 nBytes;
  std::string r_buf;

  // Open our database;
  rc = unqlite_open(&pDb, db_file.c_str(), UNQLITE_OPEN_CREATE);
  if (rc != UNQLITE_OK) {
    GTEST_FAIL();
  }

  rc = unqlite_begin(pDb);
  if (rc != UNQLITE_OK) {
    PrintErrorAndRollback(pDb, rc);
  }

  rc = unqlite_kv_store(pDb, "test", -1, "Hello World",
                        11);  // test => 'Hello World'
  if (rc != UNQLITE_OK) {
    PrintErrorAndRollback(pDb, rc);
    goto FailTransaction;
  }

  rc = unqlite_kv_store(pDb, "test1", -1, "Hello World",
                        11);  // test => 'Hello World'
  if (rc != UNQLITE_OK) {
    PrintErrorAndRollback(pDb, rc);
    goto FailTransaction;
  }

  rc = unqlite_commit(pDb);
  if (rc != UNQLITE_OK) {
    PrintErrorAndRollback(pDb, rc);
    goto FailTransaction;
  }

  // Extract data size first
  rc = unqlite_kv_fetch(pDb, "test", -1, nullptr, &nBytes);
  if (rc != UNQLITE_OK) {
    return;
  }
  GTEST_LOG_(INFO) << "Total size in DB: " << nBytes;

  r_buf.resize(nBytes);

  // Copy record content in our buffer
  unqlite_kv_fetch(pDb, "test", -1, r_buf.data(), &nBytes);

  EXPECT_EQ(r_buf, "Hello World");

  // Auto-commit the transaction and close our handle.
  unqlite_close(pDb);
  return;

FailTransaction:
  GTEST_FAIL();
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
  GTEST_LOG_(INFO) << "request1 size: " << request1.GetCachedSize();
  request1.SerializeToCodedStream(&codedOutputStream);

  codedOutputStream.WriteLittleEndian32(request2.ByteSizeLong());
  GTEST_LOG_(INFO) << "request2 size: " << request2.GetCachedSize();
  request2.SerializeToCodedStream(&codedOutputStream);

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

  std::string append_buf;
  StringOutputStream AppendStringOutputStream(&append_buf);
  CodedOutputStream AppendCodedOutputStream(&AppendStringOutputStream);

  MaxRequest r3;
  r3.set_a(5);
  r3.set_b(6);

  unqlite_int64 nAppendBytes{sizeof(int32_t)};
  AppendCodedOutputStream.WriteLittleEndian32(r3.ByteSizeLong());
  nAppendBytes += r3.GetCachedSize();
  r3.SerializeToCodedStream(&AppendCodedOutputStream);

  rc = unqlite_kv_append(pDb, "protobuf", -1, append_buf.data(),
                         static_cast<unqlite_int64>(nAppendBytes));
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

  codedInputStream.ReadLittleEndian32(&usz);
  sz = static_cast<int>(usz);
  ASSERT_TRUE(codedInputStream.GetDirectBufferPointer(&p, &sz));
  MaxRequest request3_r;
  EXPECT_TRUE(request3_r.ParseFromArray(p, usz));
  codedInputStream.Skip(usz);
  GTEST_LOG_(INFO) << "Current Pos: " << codedInputStream.CurrentPosition();

  // Reverse order
  EXPECT_EQ(request1_r.a(), 1);
  EXPECT_EQ(request1_r.b(), 2);
  EXPECT_EQ(request2_r.a(), 3);
  EXPECT_EQ(request2_r.b(), 4);
  EXPECT_EQ(request3_r.a(), 5);
  EXPECT_EQ(request3_r.b(), 6);

  // Auto-commit the transaction and close our handle.
  unqlite_close(pDb);
}

class Jx9Test : public testing::Test {
  void SetUp() override {
    int rc;

    // Open our database;
    rc = unqlite_open(&pDb, db_file.c_str(), UNQLITE_OPEN_CREATE);
    if (rc != UNQLITE_OK) {
      GTEST_FAIL();
    }
  }

  void TearDown() override { unqlite_close(pDb); }

 protected:
  void Jx9Print(std::string_view output) {
    GTEST_LOG_(INFO) << "[JX9] " << output;
  }

  static int Jx9OutputCallback(const void *pOutput, unsigned int nLen,
                               void *pThisVoid) {
    auto *pThis = reinterpret_cast<Jx9Test *>(pThisVoid);

    std::string_view output(reinterpret_cast<const char *>(pOutput), nLen);
    pThis->Jx9Print(output);
    return UNQLITE_OK;
  }

  std::string GetJx9CompileErrorString(int rc) {
    if (rc == UNQLITE_COMPILE_ERR) {
      const char *zBuf;
      int iLen;

      /* Compile-time error, extract the compiler error log */
      unqlite_config(pDb, UNQLITE_CONFIG_JX9_ERR_LOG, &zBuf, &iLen);

      if (iLen > 0) {
        return std::string{zBuf};
      }
    }
    return {};
  }

  unqlite *pDb;

  std::string m_jx9_store_src_{
      R"(
$db_name = $argv[0];
print($db_name);

// foreach($arg_kvs as $k, $v) {
//    $o = sprintf("%s=>%s", $k, $v);
//    print($o);
// }

if ( !db_exists( $db_name ) ) {
  /* Try to create it */
  $rc = db_create($db_name);

  if ( !$rc ) {
    // Handle error
    print db_errlog();
    return;
  }
}
db_begin();
$rc = db_store($db_name, $arg_kvs);
db_commit();

$result = db_fetch_all($db_name);
)"};
};

TEST_F(Jx9Test, StoreAndFetch) {
  std::string collection_name{"test"};

  int rc;
  unqlite_vm *pVm;
  rc = unqlite_compile(pDb, m_jx9_store_src_.c_str(), m_jx9_store_src_.size(),
                       &pVm);
  if (rc != UNQLITE_OK) {
    GTEST_FAIL() << "Failed to compile jx9 src: "
                 << GetJx9CompileErrorString(rc);
  }

  rc =
      unqlite_vm_config(pVm, UNQLITE_VM_CONFIG_OUTPUT, Jx9OutputCallback, this);
  if (rc != UNQLITE_OK) {
    GTEST_FAIL() << "Failed to set output callback";
  }

  rc = unqlite_vm_config(pVm, UNQLITE_VM_CONFIG_ARGV_ENTRY,
                         collection_name.c_str());
  if (rc != UNQLITE_OK) {
    GTEST_FAIL() << "Failed to set argv[0]: " << collection_name;
  }

  std::vector<std::pair<std::string, std::string>> kv_vec{
      {"k1", "v1"},
      {"k2", "v2"},
  };

  unqlite_value *kv_list = unqlite_vm_new_array(pVm);
  unqlite_value *pValueStr = unqlite_vm_new_scalar(pVm);

  for (const auto &[k, v] : kv_vec) {
    unqlite_value_string(pValueStr, v.c_str(), v.size());
    unqlite_array_add_strkey_elem(kv_list, k.c_str(), pValueStr);
    unqlite_value_reset_string_cursor(pValueStr);
  }

  rc = unqlite_vm_config(pVm, UNQLITE_VM_CONFIG_CREATE_VAR, "arg_kvs", kv_list);
  if (rc != UNQLITE_OK) {
    GTEST_FAIL() << "Failed to create var";
  }

  unqlite_vm_release_value(pVm, pValueStr);
  rc = unqlite_vm_release_value(pVm, kv_list);
  if (rc != UNQLITE_OK) {
    GTEST_FAIL() << "Failed to release kv_list";
  }

  rc = unqlite_vm_exec(pVm);
  if (rc != UNQLITE_OK) {
    GTEST_FAIL() << "Failed to execute jx9 vm";
  }

  unqlite_value *result = unqlite_vm_extract_variable(pVm, "result");
  if (result == nullptr) {
    GTEST_FAIL() << "Failed to extract jx9 vm result";
  }

  GTEST_LOG_(INFO) << "result is json object: "
                   << bool(unqlite_value_is_json_object(result));
  GTEST_LOG_(INFO) << "result is json array: "
                   << bool(unqlite_value_is_json_array(result));

  GTEST_LOG_(INFO) << "len of result: " << unqlite_array_count(result);

  rc = unqlite_vm_reset(pVm);
  if (rc != UNQLITE_OK) {
    GTEST_FAIL() << "Failed to reset jx9 vm";
  }
}
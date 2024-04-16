/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "EmbeddedDbClient.h"

namespace Ctld {

#ifdef CRANE_HAVE_UNQLITE

result::result<void, DbErrorCode> UnqliteDb::Init(const std::string& path) {
  int rc;

  m_db_path_ = path;

  rc = unqlite_open(&m_db_, m_db_path_.c_str(), UNQLITE_OPEN_CREATE);
  if (rc != UNQLITE_OK) {
    m_db_ = nullptr;
    CRANE_ERROR("Failed to open unqlite db file {}: {}", m_db_path_,
                GetInternalErrorStr_());
    return result::failure(DbErrorCode::kOther);
  }

  // Unqlite does not roll back and clear WAL after crashing.
  // Call rollback function to prevent DB writing from error due to
  // possible remaining WAL.
  rc = unqlite_rollback(m_db_);
  if (rc != UNQLITE_OK) {
    m_db_ = nullptr;
    CRANE_ERROR("Failed to rollback the undone transaction: {}",
                GetInternalErrorStr_());
    return result::failure(DbErrorCode::kOther);
  }

  return {};
}

result::result<void, DbErrorCode> UnqliteDb::Close() {
  int rc;
  if (m_db_ != nullptr) {
    CRANE_TRACE("Closing unqlite...");
    rc = unqlite_close(m_db_);
    m_db_ = nullptr;

    if (rc != UNQLITE_OK) {
      CRANE_ERROR("Failed to close unqlite: {}", GetInternalErrorStr_());
      return result::failure(DbErrorCode::kOther);
    }
  }

  return {};
}

result::result<void, DbErrorCode> UnqliteDb::Store(txn_id_t txn_id,
                                                   const std::string& key,
                                                   const void* data,
                                                   size_t len) {
  int rc;
  while (true) {
    rc = unqlite_kv_store(m_db_, key.c_str(), key.size(), data, len);
    if (rc == UNQLITE_OK) return {};
    if (rc == UNQLITE_BUSY) {
      std::this_thread::yield();
      continue;
    }

    CRANE_ERROR("Failed to store key {} into db: {}", key,
                GetInternalErrorStr_());
    if (rc != UNQLITE_NOTIMPLEMENTED) unqlite_rollback(m_db_);
    return result::failure(DbErrorCode::kOther);
  }
}

result::result<size_t, DbErrorCode> UnqliteDb::Fetch(txn_id_t txn_id,
                                                     const std::string& key,
                                                     void* buf, size_t* len) {
  int rc;

  void* buf_arg = (*len == 0) ? nullptr : buf;
  auto n_bytes = static_cast<unqlite_int64>(*len);

  while (true) {
    rc = unqlite_kv_fetch(m_db_, key.c_str(), key.size(), buf_arg, &n_bytes);

    if (rc == UNQLITE_OK) break;
    if (rc == UNQLITE_BUSY) {
      std::this_thread::yield();
      continue;
    }
    if (rc == UNQLITE_NOTFOUND) return result::failure(DbErrorCode::kNotFound);

    CRANE_ERROR("Failed to get value size for key {}: {}", key,
                GetInternalErrorStr_());
    unqlite_rollback(m_db_);
    return result::failure(DbErrorCode::kOther);
  }

  if (*len == 0) {
    *len = n_bytes;
    return {0};
  } else if (*len < n_bytes) {
    *len = n_bytes;
    return result::failure(DbErrorCode::kBufferSmall);
  }

  return {n_bytes};
}

result::result<void, DbErrorCode> UnqliteDb::Delete(txn_id_t txn_id,
                                                    const std::string& key) {
  int rc;
  while (true) {
    rc = unqlite_kv_delete(m_db_, key.c_str(), key.size());
    if (rc == UNQLITE_OK) return {};
    if (rc == UNQLITE_BUSY) {
      std::this_thread::yield();
      continue;
    }
    if (rc == UNQLITE_NOTFOUND) return result::failure(DbErrorCode::kNotFound);

    CRANE_ERROR("Failed to delete key {} from db: {}", key,
                GetInternalErrorStr_());
    if (rc != UNQLITE_NOTIMPLEMENTED) unqlite_rollback(m_db_);

    return result::failure(DbErrorCode::kOther);
  }
}

result::result<txn_id_t, DbErrorCode> UnqliteDb::Begin() {
  int rc;
  while (true) {
    rc = unqlite_begin(m_db_);
    if (rc == UNQLITE_OK) return {s_fixed_txn_id_};
    if (rc == UNQLITE_BUSY) {
      std::this_thread::yield();
      continue;
    }
    CRANE_ERROR("Failed to begin transaction: {}", GetInternalErrorStr_());
    return result::failure(DbErrorCode::kOther);
  }
}

result::result<void, DbErrorCode> UnqliteDb::Commit(txn_id_t txn_id) {
  if (txn_id <= 0 || txn_id > s_fixed_txn_id_) return {};

  int rc;
  while (true) {
    rc = unqlite_commit(m_db_);
    if (rc == UNQLITE_OK) return {};
    if (rc == UNQLITE_BUSY) {
      std::this_thread::yield();
      continue;
    }
    CRANE_ERROR("Failed to commit: {}", GetInternalErrorStr_());
    return result::failure(kOther);
  }
}

result::result<void, DbErrorCode> UnqliteDb::Abort(txn_id_t txn_id) {
  if (txn_id <= 0 || txn_id > s_fixed_txn_id_) return {};

  int rc;
  while (true) {
    rc = unqlite_rollback(m_db_);
    if (rc == UNQLITE_OK) return {};
    if (rc == UNQLITE_BUSY) {
      std::this_thread::yield();
      continue;
    }
    CRANE_ERROR("Failed to abort: {}", GetInternalErrorStr_());
    return result::failure(kOther);
  }
}

result::result<void, DbErrorCode> UnqliteDb::IterateAllKv(KvIterFunc func) {
  int rc;
  unqlite_kv_cursor* cursor;
  rc = unqlite_kv_cursor_init(m_db_, &cursor);
  if (rc != UNQLITE_OK) return result::failure(kOther);

  for (unqlite_kv_cursor_first_entry(cursor);
       unqlite_kv_cursor_valid_entry(cursor);
       unqlite_kv_cursor_next_entry(cursor)) {
    int key_len;
    unqlite_int64 value_len;
    unqlite_kv_cursor_key(cursor, nullptr, &key_len);
    unqlite_kv_cursor_data(cursor, nullptr, &value_len);

    std::string key;
    key.resize(key_len);

    std::vector<uint8_t> value;
    value.resize(value_len);

    unqlite_kv_cursor_key(cursor, key.data(), &key_len);
    unqlite_kv_cursor_data(cursor, value.data(), &value_len);

    if (!func(std::move(key), std::move(value))) {
      unqlite_kv_cursor_delete_entry(cursor);
    }
  }

  unqlite_kv_cursor_release(m_db_, cursor);
  return {};
}

std::string UnqliteDb::GetInternalErrorStr_() {
  // Insertion fail, Handle error
  const char* zBuf;
  int iLen;
  /* Something goes wrong, extract the database error log */
  unqlite_config(m_db_, UNQLITE_CONFIG_ERR_LOG, &zBuf, &iLen);
  if (iLen > 0) {
    return {zBuf};
  }
  return {};
}

#endif

#ifdef CRANE_HAVE_BERKELEY_DB

result::result<void, DbErrorCode> BerkeleyDb::Init(const std::string& path) {
  try {
    std::filesystem::path db_dir{path};
    std::filesystem::path env_dir{path + "Env"};
    if (!std::filesystem::exists(env_dir))
      std::filesystem::create_directories(env_dir);

    m_env_home_ = env_dir.string();
  } catch (const std::exception& e) {
    CRANE_CRITICAL("Invalid berkeley db env home path {}: {}", m_env_home_,
                   e.what());
    return result::failure(DbErrorCode::kOther);
  }

  m_db_path_ = path;

  u_int32_t env_flags = DB_CREATE |      // If the environment does not
                                         // exist, create it.
                        DB_INIT_LOCK |   // Initialize locking
                        DB_INIT_LOG |    // Initialize logging
                        DB_INIT_MPOOL |  // Initialize the cache
                        DB_INIT_TXN |    // Initialize transactions
                        DB_RECOVER;

  u_int32_t o_flags = DB_CREATE | DB_AUTO_COMMIT;  // Open flags;

  try {
    m_env_ = std::make_unique<DbEnv>(0);
    m_env_->open(m_env_home_.c_str(), env_flags, 0);
    m_db_ = std::make_unique<Db>(m_env_.get(), 0);  // Instantiate the Db object

    // Open the database
    m_db_->open(nullptr,       // Transaction pointer
                path.c_str(),  // Database file name
                nullptr,       // Optional logical database name
                DB_HASH,       // Database access method
                o_flags,       // Open flags
                0);            // File mode (using defaults)
  } catch (DbException& e) {
    if (e.get_errno() == ENOENT) {
      CRANE_ERROR("Failed to open berkeley db file {}: {}", m_db_path_,
                  e.what());
    } else {
      CRANE_ERROR("Failed to init berkeley db: {}", e.what());
    }

    return result::failure(DbErrorCode::kOther);
  } catch (std::exception& e) {
    CRANE_ERROR("Failed to init berkeley db: {}", e.what());
    return result::failure(DbErrorCode::kOther);
  }

  return {};
}

result::result<void, DbErrorCode> BerkeleyDb::Close() {
  if (m_db_ != nullptr && m_env_ != nullptr) {
    CRANE_TRACE("Closing berkeley db...");
    try {
      // close all databases before closing environment
      m_db_->close(0);
      m_env_->close(0);
    } catch (DbException& e) {
      CRANE_ERROR("Failed to close berkeley db and environment: {}, {}, {}",
                  m_db_path_, m_env_home_, e.what());
      return result::failure(DbErrorCode::kOther);
    }
  }

  return {};
}

result::result<void, DbErrorCode> BerkeleyDb::Store(txn_id_t txn_id,
                                                    const std::string& key,
                                                    const void* data,
                                                    size_t len) {
  DbTxn* txn = GetDbTxnFromId_(txn_id);

  Dbt key_dbt((void*)key.c_str(), key.length() + 1);
  Dbt data_dbt((void*)data, len);

  try {
    m_db_->put(txn, &key_dbt, &data_dbt, 0);
  } catch (DbException& e) {
    CRANE_ERROR("Failed to store key {} into db: {}", key, e.what());
    std::ignore = Abort(txn_id);
    return result::failure(DbErrorCode::kOther);
  }

  return {};
}

result::result<size_t, DbErrorCode> BerkeleyDb::Fetch(txn_id_t txn_id,
                                                      const std::string& key,
                                                      void* buf, size_t* len) {
  int rc;
  DbTxn* txn = GetDbTxnFromId_(txn_id);
  Dbt key_dbt, data_dbt;

  key_dbt.set_data((void*)key.c_str());
  key_dbt.set_size(key.length() + 1);

  void* buf_arg = (*len == 0) ? nullptr : buf;
  data_dbt.set_data(buf_arg);
  data_dbt.set_ulen(*len);
  data_dbt.set_flags(DB_DBT_USERMEM);

  try {
    rc = m_db_->get(txn, &key_dbt, &data_dbt, 0);
  } catch (DbException& e) {
    if (e.get_errno() == DB_BUFFER_SMALL) {
      *len = data_dbt.get_size();
      return result::failure(DbErrorCode::kBufferSmall);
    } else {
      CRANE_ERROR("Failed to get value size for key {}. {}", key, e.what());
      std::ignore = Abort(txn_id);
      return result::failure(DbErrorCode::kOther);
    }
  }

  if (rc != 0) {
    if (rc == DB_NOTFOUND)
      return result::failure(DbErrorCode::kNotFound);
    else {
      std::ignore = Abort(txn_id);
      return result::failure(DbErrorCode::kOther);
    }
  }

  return {data_dbt.get_size()};
}

result::result<void, DbErrorCode> BerkeleyDb::Delete(txn_id_t txn_id,
                                                     const std::string& key) {
  DbTxn* txn = GetDbTxnFromId_(txn_id);

  Dbt key_dbt((void*)key.c_str(), key.length() + 1);

  int rc = m_db_->del(txn, &key_dbt, 0);
  if (rc != 0) {
    CRANE_ERROR("Failed to delete key {} from db.", key);
    if (rc == DB_NOTFOUND) return result::failure(DbErrorCode::kNotFound);
    std::ignore = Abort(txn_id);
    return result::failure(DbErrorCode::kOther);
  }

  return {};
}

result::result<txn_id_t, DbErrorCode> BerkeleyDb::Begin() {
  DbTxn* txn = nullptr;

  try {
    m_env_->txn_begin(
        nullptr, &txn,
        DB_TXN_BULK  // Enable transactional bulk insert optimization.
    );
  } catch (DbException& e) {
    CRANE_ERROR("Failed to begin a transaction: {}", e.what());
    return result::failure(DbErrorCode::kOther);
  }

  m_txn_map_[txn->id()] = txn;
  return {txn->id()};
}

result::result<void, DbErrorCode> BerkeleyDb::Commit(txn_id_t txn_id) {
  DbTxn* txn = GetDbTxnFromId_(txn_id);

  try {
    if (txn) txn->commit(0);
  } catch (DbException& e) {
    CRANE_ERROR("Failed to commit a transaction: {}", e.what());
    std::ignore = Abort(txn_id);
    return result::failure(DbErrorCode::kOther);
  }

  m_txn_map_.erase(txn_id);
  return {};
}

result::result<void, DbErrorCode> BerkeleyDb::Abort(txn_id_t txn_id) {
  DbTxn* txn = GetDbTxnFromId_(txn_id);

  try {
    if (txn) txn->abort();
  } catch (DbException& e) {
    CRANE_ERROR("Failed to abort a transaction: {}", e.what());
    m_txn_map_.erase(txn_id);
    return result::failure(DbErrorCode::kOther);
  }

  m_txn_map_.erase(txn_id);
  return {};
}

result::result<void, DbErrorCode> BerkeleyDb::IterateAllKv(KvIterFunc func) {
  int rc;
  Dbc* cursor;
  if ((rc = m_db_->cursor(nullptr, &cursor, 0)) != 0) {
    return result::failure(DbErrorCode::kOther);
  }

  Dbt key, value;
  while ((rc = cursor->get(&key, &value, DB_NEXT)) == 0) {
    std::string key_buf;
    key_buf.assign((char*)key.get_data(), key.get_size());

    std::vector<uint8_t> value_buf;
    value_buf.assign((uint8_t*)value.get_data(),
                     (uint8_t*)value.get_data() + value.get_size());

    if (!func(std::move(key_buf), std::move(value_buf))) {
      cursor->del(0 /*NULL flags*/);
    }
  }

  rc = cursor->close();
  if (rc) return result::failure(DbErrorCode::kOther);

  return {};
}

DbTxn* BerkeleyDb::GetDbTxnFromId_(txn_id_t txn_id) {
  // Do not use database transactions
  if (txn_id == 0) return nullptr;

  auto it = m_txn_map_.find(txn_id);
  if (it == m_txn_map_.end()) {
    CRANE_ERROR("Try to obtain a non-existent DbTxn, txn_id : {}", txn_id);
    return nullptr;
  } else {
    return it->second;
  }
}

#endif

EmbeddedDbClient::~EmbeddedDbClient() {
  if (m_variable_db_) {
    auto result = m_variable_db_->Close();
    if (result.has_error())
      CRANE_ERROR(
          "Error occurred when closing the embedded db of variable data!");
  }

  if (m_fixed_db_) {
    auto result = m_fixed_db_->Close();
    if (result.has_error())
      CRANE_ERROR("Error occurred when closing the embedded db of fixed data!");
  }
}

bool EmbeddedDbClient::Init(const std::string& db_path) {
  if (g_config.CraneEmbeddedDbBackend == "Unqlite") {
#ifdef CRANE_HAVE_UNQLITE
    m_variable_db_ = std::make_unique<UnqliteDb>();
    m_fixed_db_ = std::make_unique<UnqliteDb>();
#else
    CRANE_ERROR(
        "Select unqlite as the embedded db but it's not been compiled.");
    return false;
#endif

  } else if (g_config.CraneEmbeddedDbBackend == "BerkeleyDB") {
#ifdef CRANE_HAVE_BERKELEY_DB
    m_variable_db_ = std::make_unique<BerkeleyDb>();
    m_fixed_db_ = std::make_unique<BerkeleyDb>();
#else
    CRANE_ERROR(
        "Select Berkeley DB as the embedded db but it's not been compiled.");
    return false;
#endif

  } else {
    CRANE_ERROR("Invalid embedded database backend: {}",
                g_config.CraneEmbeddedDbBackend);
    return false;
  }

  auto result = m_variable_db_->Init(db_path + "var");
  if (result.has_error()) return false;
  result = m_fixed_db_->Init(db_path + "fix");
  if (result.has_error()) return false;

  bool ok;

  // There is no race during Init stage.
  // No lock is needed.
  ok = FetchTypeFromVarDbOrInitWithValueNoLockAndTxn_(0, s_next_task_id_str_,
                                                      &s_next_task_id_, 1u);
  if (!ok) return false;

  ok = FetchTypeFromVarDbOrInitWithValueNoLockAndTxn_(0, s_next_task_db_id_str_,
                                                      &s_next_task_db_id_, 1L);
  if (!ok) return false;

  return true;
}

bool EmbeddedDbClient::RetrieveLastSnapshot(DbSnapshot* snapshot) {
  using TaskToCtld = crane::grpc::TaskToCtld;

  std::unordered_map<db_id_t, TaskToCtld> task_fixed_data_map;

  auto result = m_fixed_db_->IterateAllKv(
      [&](std::string&& key, std::vector<uint8_t>&& value) {
        task_db_id_t id = ExtractDbIdFromEntry_(key);

        TaskToCtld task_to_ctld;
        task_to_ctld.ParseFromArray(value.data(), value.size());

        // Record all task_id here and don't delete any key,
        // so true is returned.
        return true;
      });

  if (result.has_error()) {
    CRANE_ERROR("Failed to restore fixed data into queues!");
    return false;
  }

  result = m_variable_db_->IterateAllKv(
      [&](std::string&& key, std::vector<uint8_t>&& value) {
        // Skip if not RuntimeAttr
        if (!IsVariableDbTaskDataEntry_(key)) return true;

        task_db_id_t id = ExtractDbIdFromEntry_(key);

        // Delete incomplete task data,
        // where fixed data are stored but variable data are missing.
        auto fixed_data_it = task_fixed_data_map.find(id);
        if (fixed_data_it == task_fixed_data_map.end()) return false;

        // Assemble TaskInEmbeddedDb here.
        crane::grpc::TaskInEmbeddedDb task_proto;
        task_proto.mutable_runtime_attr()->ParseFromArray(value.data(),
                                                          value.size());

        *task_proto.mutable_task_to_ctld() = std::move(fixed_data_it->second);

        // Dispatch to different queues by status.
        switch (task_proto.runtime_attr().status()) {
          case crane::grpc::Pending:
            snapshot->pending_queue.emplace(id, std::move(task_proto));
            break;
          case crane::grpc::Running:
            snapshot->running_queue.emplace(id, std::move(task_proto));
            break;
          default:
            snapshot->final_queue.emplace(id, std::move(task_proto));
            break;
        }
        return true;
      });

  if (result.has_error()) {
    CRANE_ERROR("Failed to restore the variable data into queues");
    return false;
  }

  return true;
}

bool EmbeddedDbClient::AppendTasksToPendingAndAdvanceTaskIds(
    const std::vector<TaskInCtld*>& tasks) {
  txn_id_t txn_id;
  result::result<void, DbErrorCode> result;

  // Note: In current implementation, this function is called by only
  // one single thread and the lock here is actually useless.
  // However, it costs little and prevents race condition,
  // so we just leave it here.
  absl::MutexLock lock_ids(&s_task_id_and_db_id_mtx_);

  uint32_t task_id{s_next_task_id_};
  db_id_t task_db_id{s_next_task_db_id_};

  if (!BeginDbTransaction_(m_fixed_db_.get(), &txn_id)) return false;

  for (const auto& task : tasks) {
    task->SetTaskId(task_id++);
    task->SetTaskDbId(task_db_id++);

    result = StoreTypeIntoDb_(m_fixed_db_.get(), txn_id,
                              GetFixedDbEntryName_(task->TaskDbId()),
                              &task->TaskToCtld());
    if (result.has_error()) {
      CRANE_ERROR(
          "Failed to store the fixed data of task id: {} / task db id: {}.",
          task->TaskId(), task->TaskDbId());
      return false;
    }
  }

  if (!CommitDbTransaction_(m_fixed_db_.get(), txn_id)) return false;

  if (!BeginDbTransaction_(m_variable_db_.get(), &txn_id)) return false;

  for (const auto& task : tasks) {
    if (task->TaskId() == 0)
      continue;  // skip the task which failed to store the fix data

    result = StoreTypeIntoDb_(m_variable_db_.get(), txn_id,
                              GetVariableDbEntryName_(task->TaskDbId()),
                              &task->RuntimeAttr());
    if (result.has_error()) {
      CRANE_ERROR(
          "Failed to store the variable data of task id: {} / task db id: "
          "{}.",
          task->TaskId(), task->TaskDbId());
      return false;
    }
  }

  result = StoreTypeIntoDb_(m_variable_db_.get(), txn_id, s_next_task_id_str_,
                            &task_id);
  if (result.has_error()) {
    CRANE_ERROR("Failed to store next_task_id.");
    return false;
  }

  result = StoreTypeIntoDb_(m_variable_db_.get(), txn_id,
                            s_next_task_db_id_str_, &task_db_id);
  if (result.has_error()) {
    CRANE_ERROR("Failed to store next_task_db_id.");
    return false;
  }
  if (!CommitDbTransaction_(m_variable_db_.get(), txn_id)) return false;

  s_next_task_id_ = task_id;
  s_next_task_db_id_ = task_db_id;

  return true;
}

bool EmbeddedDbClient::PurgeEndedTasks(const std::vector<db_id_t>& db_ids) {
  // To ensure consistency of both fixed data db and variable data db under
  // failure, we must ensure that:
  // 1. when inserting task data, fixed data db is written before variable db;
  // 2. when erasing task data, fixed data db is erased after variable db;

  txn_id_t txn_id;
  result::result<void, DbErrorCode> res;

  if (!BeginDbTransaction_(m_variable_db_.get(), &txn_id)) return false;
  for (const auto& id : db_ids) {
    res = m_variable_db_->Delete(txn_id, GetVariableDbEntryName_(id));
    if (res.has_error()) {
      CRANE_ERROR(
          "Failed to delete embedded variable data entry.Error code: {}",
          res.error());
      return false;
    }
  }
  if (!CommitDbTransaction_(m_variable_db_.get(), txn_id)) return false;

  if (!BeginDbTransaction_(m_fixed_db_.get(), &txn_id)) return false;
  for (const auto& id : db_ids) {
    res = m_fixed_db_->Delete(txn_id, GetFixedDbEntryName_(id));
    if (res.has_error()) {
      CRANE_ERROR("Failed to delete embedded fixed data entry. Error code: {}",
                  res.error());
      return false;
    }
  }
  if (!CommitDbTransaction_(m_fixed_db_.get(), txn_id)) return false;

  return true;
}

}  // namespace Ctld

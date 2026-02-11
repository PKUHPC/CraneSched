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

#include "EmbeddedDbClient.h"

namespace Ctld {

#ifdef CRANE_HAVE_UNQLITE

std::expected<void, DbErrorCode> UnqliteDb::Init(const std::string& path) {
  int rc;

  m_db_path_ = path;

  rc = unqlite_open(&m_db_, m_db_path_.c_str(), UNQLITE_OPEN_CREATE);
  if (rc != UNQLITE_OK) {
    m_db_ = nullptr;
    CRANE_ERROR("Failed to open unqlite db file {}: {}", m_db_path_,
                GetInternalErrorStr_());
    return std::unexpected(DbErrorCode::OTHER);
  }

  // Unqlite does not roll back and clear WAL after crashing.
  // Call rollback function to prevent DB writing from error due to
  // possible remaining WAL.
  rc = unqlite_rollback(m_db_);
  if (rc != UNQLITE_OK) {
    m_db_ = nullptr;
    CRANE_ERROR("Failed to rollback the undone transaction: {}",
                GetInternalErrorStr_());
    return std::unexpected(DbErrorCode::OTHER);
  }

  return {};
}

std::expected<void, DbErrorCode> UnqliteDb::Close() {
  int rc;
  if (m_db_ != nullptr) {
    CRANE_TRACE("Closing unqlite...");
    rc = unqlite_close(m_db_);
    m_db_ = nullptr;

    if (rc != UNQLITE_OK) {
      CRANE_ERROR("Failed to close unqlite: {}", GetInternalErrorStr_());
      return std::unexpected(DbErrorCode::OTHER);
    }
  }

  return {};
}

std::expected<void, DbErrorCode> UnqliteDb::Store(txn_id_t txn_id,
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
    return std::unexpected(DbErrorCode::OTHER);
  }
}

std::expected<size_t, DbErrorCode> UnqliteDb::Fetch(txn_id_t txn_id,
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
    if (rc == UNQLITE_NOTFOUND) return std::unexpected(DbErrorCode::NOT_FOUND);

    CRANE_ERROR("Failed to get value size for key {}: {}", key,
                GetInternalErrorStr_());
    unqlite_rollback(m_db_);
    return std::unexpected(DbErrorCode::OTHER);
  }

  if (*len == 0) {
    *len = n_bytes;
    return {0};
  } else if (*len < n_bytes) {
    *len = n_bytes;
    return std::unexpected(DbErrorCode::BUFFER_SMALL);
  }

  return {n_bytes};
}

std::expected<void, DbErrorCode> UnqliteDb::Delete(txn_id_t txn_id,
                                                   const std::string& key) {
  int rc;
  while (true) {
    rc = unqlite_kv_delete(m_db_, key.c_str(), key.size());
    if (rc == UNQLITE_OK) return {};
    if (rc == UNQLITE_BUSY) {
      std::this_thread::yield();
      continue;
    }
    if (rc == UNQLITE_NOTFOUND) return std::unexpected(DbErrorCode::NOT_FOUND);

    CRANE_ERROR("Failed to delete key {} from db: {}", key,
                GetInternalErrorStr_());
    if (rc != UNQLITE_NOTIMPLEMENTED) unqlite_rollback(m_db_);

    return std::unexpected(DbErrorCode::OTHER);
  }
}

std::expected<txn_id_t, DbErrorCode> UnqliteDb::Begin() {
  int rc;
  while (true) {
    rc = unqlite_begin(m_db_);
    if (rc == UNQLITE_OK) return {s_fixed_txn_id_};
    if (rc == UNQLITE_BUSY) {
      std::this_thread::yield();
      continue;
    }
    CRANE_ERROR("Failed to begin transaction: {}", GetInternalErrorStr_());
    return std::unexpected(DbErrorCode::OTHER);
  }
}

std::expected<void, DbErrorCode> UnqliteDb::Commit(txn_id_t txn_id) {
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
    return std::unexpected(DbErrorCode::OTHER);
  }
}

std::expected<void, DbErrorCode> UnqliteDb::Abort(txn_id_t txn_id) {
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
    return std::unexpected(DbErrorCode::OTHER);
  }
}

std::expected<void, DbErrorCode> UnqliteDb::IterateAllKv(KvIterFunc func) {
  int rc;
  unqlite_kv_cursor* cursor;
  rc = unqlite_kv_cursor_init(m_db_, &cursor);
  if (rc != UNQLITE_OK) return std::unexpected(DbErrorCode::OTHER);

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

std::expected<void, DbErrorCode> BerkeleyDb::Init(const std::string& path) {
  try {
    std::filesystem::path db_dir{path};
    std::filesystem::path env_dir{path + "Env"};
    if (!std::filesystem::exists(env_dir))
      std::filesystem::create_directories(env_dir);

    m_env_home_ = env_dir.string();
  } catch (const std::exception& e) {
    CRANE_CRITICAL("Invalid berkeley db env home path {}: {}", m_env_home_,
                   e.what());
    return std::unexpected(DbErrorCode::kOther);
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

    // Must be called before DB_ENV->open()!
    // Set max transaction number to 1 to avoid deadlock handling.
    if (m_env_->set_tx_max(1) != 0)
      CRANE_ERROR("Error when set_tx_max(1) for BDB {}!", m_db_path_);

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

    return std::unexpected(DbErrorCode::kOther);
  } catch (std::exception& e) {
    CRANE_ERROR("Failed to init berkeley db: {}", e.what());
    return std::unexpected(DbErrorCode::kOther);
  }

  return {};
}

std::expected<void, DbErrorCode> BerkeleyDb::Close() {
  if (m_db_ != nullptr && m_env_ != nullptr) {
    CRANE_TRACE("Closing berkeley db...");
    try {
      // close all databases before closing environment
      m_db_->close(0);
      m_env_->close(0);
    } catch (DbException& e) {
      CRANE_ERROR("Failed to close berkeley db and environment: {}, {}, {}",
                  m_db_path_, m_env_home_, e.what());
      return std::unexpected(DbErrorCode::kOther);
    }
  }

  return {};
}

std::expected<void, DbErrorCode> BerkeleyDb::Store(txn_id_t txn_id,
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
    return std::unexpected(DbErrorCode::kOther);
  }

  return {};
}

std::expected<size_t, DbErrorCode> BerkeleyDb::Fetch(txn_id_t txn_id,
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
      return std::unexpected(DbErrorCode::kBufferSmall);
    } else {
      CRANE_ERROR("Failed to get value size for key {}. {}", key, e.what());
      std::ignore = Abort(txn_id);
      return std::unexpected(DbErrorCode::kOther);
    }
  }

  if (rc != 0) {
    if (rc == DB_NOTFOUND)
      return std::unexpected(DbErrorCode::kNotFound);
    else {
      std::ignore = Abort(txn_id);
      return std::unexpected(DbErrorCode::kOther);
    }
  }

  return {data_dbt.get_size()};
}

std::expected<void, DbErrorCode> BerkeleyDb::Delete(txn_id_t txn_id,
                                                    const std::string& key) {
  DbTxn* txn = GetDbTxnFromId_(txn_id);

  Dbt key_dbt((void*)key.c_str(), key.length() + 1);

  int rc = m_db_->del(txn, &key_dbt, 0);
  if (rc != 0) {
    CRANE_ERROR("Failed to delete key {} from db.", key);
    if (rc == DB_NOTFOUND) return std::unexpected(DbErrorCode::kNotFound);
    std::ignore = Abort(txn_id);
    return std::unexpected(DbErrorCode::kOther);
  }

  return {};
}

std::expected<txn_id_t, DbErrorCode> BerkeleyDb::Begin() {
  DbTxn* txn = nullptr;

  try {
    m_env_->txn_begin(
        nullptr, &txn,
        DB_TXN_BULK  // Enable transactional bulk insert optimization.
    );
  } catch (DbException& e) {
    CRANE_ERROR("Failed to begin a transaction: {}", e.what());
    return std::unexpected(DbErrorCode::kOther);
  }

  m_txn_map_[txn->id()] = txn;
  return {txn->id()};
}

std::expected<void, DbErrorCode> BerkeleyDb::Commit(txn_id_t txn_id) {
  DbTxn* txn = GetDbTxnFromId_(txn_id);

  try {
    if (txn) txn->commit(0);
  } catch (DbException& e) {
    CRANE_ERROR("Failed to commit a transaction: {}", e.what());
    std::ignore = Abort(txn_id);
    return std::unexpected(DbErrorCode::kOther);
  }

  m_txn_map_.erase(txn_id);
  return {};
}

std::expected<void, DbErrorCode> BerkeleyDb::Abort(txn_id_t txn_id) {
  DbTxn* txn = GetDbTxnFromId_(txn_id);

  try {
    if (txn) txn->abort();
  } catch (DbException& e) {
    CRANE_ERROR("Failed to abort a transaction: {}", e.what());
    m_txn_map_.erase(txn_id);
    return std::unexpected(DbErrorCode::kOther);
  }

  m_txn_map_.erase(txn_id);
  return {};
}

std::expected<void, DbErrorCode> BerkeleyDb::IterateAllKv(KvIterFunc func) {
  int rc;
  Dbc* cursor;
  if ((rc = m_db_->cursor(nullptr, &cursor, 0)) != 0) {
    return std::unexpected(DbErrorCode::kOther);
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
  if (rc) return std::unexpected(DbErrorCode::kOther);

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
    if (!result)
      CRANE_ERROR(
          "Error occurred when closing the embedded db of variable data!");
  }

  if (m_fixed_db_) {
    auto result = m_fixed_db_->Close();
    if (!result)
      CRANE_ERROR("Error occurred when closing the embedded db of fixed data!");
  }

  if (m_step_var_db_) {
    auto result = m_step_var_db_->Close();
    if (!result)
      CRANE_ERROR(
          "Error occurred when closing the embedded db of step variable data!");
  }

  if (m_step_fixed_db_) {
    auto result = m_step_fixed_db_->Close();
    if (!result)
      CRANE_ERROR(
          "Error occurred when closing the embedded db of step fixed data!");
  }

  if (m_resv_db_) {
    auto result = m_resv_db_->Close();
    if (!result)
      CRANE_ERROR(
          "Error occurred when closing the embedded db of reservation data!");
  }
}

bool EmbeddedDbClient::Init(const std::string& db_path) {
  if (g_config.CraneEmbeddedDbBackend == "Unqlite") {
#ifdef CRANE_HAVE_UNQLITE
    m_variable_db_ = std::make_unique<UnqliteDb>();
    m_fixed_db_ = std::make_unique<UnqliteDb>();
    m_resv_db_ = std::make_unique<UnqliteDb>();

    m_step_var_db_ = std::make_unique<UnqliteDb>();
    m_step_fixed_db_ = std::make_unique<UnqliteDb>();
#else
    CRANE_ERROR(
        "Select unqlite as the embedded db but it's not been compiled.");
    return false;
#endif

  } else if (g_config.CraneEmbeddedDbBackend == "BerkeleyDB") {
#ifdef CRANE_HAVE_BERKELEY_DB
    m_variable_db_ = std::make_unique<BerkeleyDb>();
    m_fixed_db_ = std::make_unique<BerkeleyDb>();
    m_resv_db_ = std::make_unique<BerkeleyDb>();

    m_step_var_db_ = std::make_unique<BerkeleyDb>();
    m_step_fixed_db_ = std::make_unique<BerkeleyDb>();
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
  if (!result) return false;
  result = m_fixed_db_->Init(db_path + "fix");
  if (!result) return false;
  result = m_resv_db_->Init(db_path + "resv");
  if (!result) return false;

  result = m_step_var_db_->Init(db_path + "step_var");
  if (!result) return false;
  result = m_step_fixed_db_->Init(db_path + "step_fix");
  if (!result) return false;

  bool ok;

  // There is no race during Init stage.
  // No lock is needed.
  ok = FetchTypeFromDbOrInitWithValueNoLockAndTxn_(
      0, m_variable_db_.get(), s_next_task_id_str_, &s_next_task_id_, 1u);
  if (!ok) return false;

  ok = FetchTypeFromDbOrInitWithValueNoLockAndTxn_(
      0, m_variable_db_.get(), s_next_task_db_id_str_, &s_next_task_db_id_, 1L);
  if (!ok) return false;

  ok = FetchTypeFromDbOrInitWithValueNoLockAndTxn_(
      0, m_step_var_db_.get(), s_next_step_db_id_str_, &s_next_step_db_id_, 1L);
  if (!ok) return false;

  ok = FetchTypeFromDbOrInitWithValueNoLockAndTxn_(
      0, m_step_var_db_.get(), s_next_step_id_str_, &s_next_step_id_map_,
      crane::grpc::StepNextIdInEmbeddedDb{});
  if (!ok) return false;

  return true;
}

bool EmbeddedDbClient::ResetNextTaskId(task_id_t next_task_id,
                                       db_id_t next_task_db_id) {
  txn_id_t txn_id;
  std::expected<void, DbErrorCode> result;

  absl::MutexLock lock_ids(&s_task_id_and_db_id_mtx_);

  // Reset task ID counters in variable_db (0 = skip, >0 = reset to value)
  if (!BeginDbTransaction_(m_variable_db_.get(), &txn_id)) return false;

  if (next_task_id > 0) {
    result = StoreTypeIntoDb_(m_variable_db_.get(), txn_id,
                              s_next_task_id_str_, &next_task_id);
    if (!result) {
      CRANE_ERROR("Failed to reset next_task_id.");
      return false;
    }
  }

  if (next_task_db_id > 0) {
    result = StoreTypeIntoDb_(m_variable_db_.get(), txn_id,
                              s_next_task_db_id_str_, &next_task_db_id);
    if (!result) {
      CRANE_ERROR("Failed to reset next_task_db_id.");
      return false;
    }
  }

  if (!CommitDbTransaction_(m_variable_db_.get(), txn_id)) return false;

  // Also reset step counters when task IDs are reset
  if (next_task_id > 0) {
    absl::MutexLock lock_steps(&s_step_id_mtx_);

    if (!BeginDbTransaction_(m_step_var_db_.get(), &txn_id)) return false;

    db_id_t new_step_db_id = 1;
    result = StoreTypeIntoDb_(m_step_var_db_.get(), txn_id,
                              s_next_step_db_id_str_, &new_step_db_id);
    if (!result) {
      CRANE_ERROR("Failed to reset next_step_db_id.");
      return false;
    }

    crane::grpc::StepNextIdInEmbeddedDb new_step_id_map;
    result = StoreTypeIntoDb_(m_step_var_db_.get(), txn_id,
                              s_next_step_id_str_, &new_step_id_map);
    if (!result) {
      CRANE_ERROR("Failed to reset next_step_id_map.");
      return false;
    }

    if (!CommitDbTransaction_(m_step_var_db_.get(), txn_id)) return false;

    s_next_step_db_id_ = new_step_db_id;
    s_next_step_id_map_ = new_step_id_map;
  }

  // Update in-memory values
  if (next_task_id > 0) s_next_task_id_ = next_task_id;
  if (next_task_db_id > 0) s_next_task_db_id_ = next_task_db_id;

  CRANE_INFO("Task ID counters reset: next_task_id={}, next_task_db_id={}.",
             s_next_task_id_, s_next_task_db_id_);
  return true;
}

bool EmbeddedDbClient::RetrieveLastSnapshot(DbSnapshot* snapshot) {
  using TaskStatus = crane::grpc::TaskStatus;
  using RuntimeAttr = crane::grpc::RuntimeAttrOfTask;

  std::expected<void, DbErrorCode> result;
  std::unordered_map<db_id_t, RuntimeAttr> db_id_runtime_attr_map;

  result = m_variable_db_->IterateAllKv(
      [&](std::string&& key, std::vector<uint8_t>&& value) {
        // Skip if not RuntimeAttr
        if (!IsVariableDbTaskDataEntry_(key)) return true;

        task_db_id_t id = ExtractDbIdFromEntry_(key);

        RuntimeAttr runtime_attr;
        runtime_attr.ParseFromArray(value.data(), value.size());

        db_id_runtime_attr_map.emplace(id, std::move(runtime_attr));

        // Record all task_id here and don't delete any key,
        // so true is returned.
        return true;
      });

  if (!result) {
    CRANE_ERROR("Failed to restore the variable data into queues");
    return false;
  }

  result = m_fixed_db_->IterateAllKv(
      [&](std::string&& key, std::vector<uint8_t>&& value) {
        task_db_id_t id = ExtractDbIdFromEntry_(key);

        // Delete incomplete task fixed data,
        // where fixed data are stored but variable data are missing.
        auto runtime_attr_it = db_id_runtime_attr_map.find(id);
        if (runtime_attr_it == db_id_runtime_attr_map.end()) return false;

        TaskStatus status = runtime_attr_it->second.status();

        // Assemble TaskInEmbeddedDb here.
        TaskInEmbeddedDb task;
        *task.mutable_runtime_attr() = std::move(runtime_attr_it->second);
        task.mutable_task_to_ctld()->ParseFromArray(value.data(), value.size());

        // Dispatch to different queues by status.
        switch (status) {
        case crane::grpc::Pending:
          snapshot->pending_queue.emplace(id, std::move(task));
          break;
        case crane::grpc::Running:
        case crane::grpc::Configured:
        case crane::grpc::Configuring:
        case crane::grpc::Completing:
          snapshot->running_queue.emplace(id, std::move(task));
          break;
        default:
          snapshot->final_queue.emplace(id, std::move(task));
          break;
        }
        return true;
      });

  if (!result) {
    CRANE_ERROR("Failed to restore fixed data into queues!");
    return false;
  }

  return true;
}

bool EmbeddedDbClient::RetrieveStepInfo(StepDbSnapshot* snapshot) {
  using TaskStatus = crane::grpc::TaskStatus;
  using RuntimeAttr = crane::grpc::RuntimeAttrOfStep;

  std::expected<void, DbErrorCode> result;
  std::unordered_map<db_id_t, RuntimeAttr> db_id_runtime_attr_map;

  result = m_step_var_db_->IterateAllKv(
      [&](std::string&& key, std::vector<uint8_t>&& value) {
        // Skip if not RuntimeAttr
        if (!IsVariableDbStepDataEntry_(key)) return true;

        step_db_id_t id = ExtractStepDbIdFromEntry_(key);

        RuntimeAttr runtime_attr;
        runtime_attr.ParseFromArray(value.data(), value.size());

        db_id_runtime_attr_map.emplace(id, std::move(runtime_attr));

        // Record all task_id here and don't delete any key,
        // so true is returned.
        return true;
      });

  if (!result) {
    CRANE_ERROR("Failed to restore the variable data of steps");
    return false;
  }

  result = m_step_fixed_db_->IterateAllKv([&](std::string&& key,
                                              std::vector<uint8_t>&& value) {
    step_db_id_t id = ExtractStepDbIdFromEntry_(key);

    // Delete incomplete task fixed data,
    // where fixed data are stored but variable data are missing.
    auto runtime_attr_it = db_id_runtime_attr_map.find(id);
    if (runtime_attr_it == db_id_runtime_attr_map.end()) return false;

    TaskStatus status = runtime_attr_it->second.status();

    // Assemble TaskInEmbeddedDb here.
    StepInEmbeddedDb step;
    *step.mutable_runtime_attr() = std::move(runtime_attr_it->second);
    step.mutable_step_to_ctld()->ParseFromArray(value.data(), value.size());

    snapshot->steps[step.step_to_ctld().job_id()].push_back(std::move(step));
    // Dispatch to different queues by status.
    return true;
  });

  if (!result) {
    CRANE_ERROR("Failed to restore fixed data of steps!");
    return false;
  }
  CRANE_INFO(
      "Restored [{}] steps from embedded db.",
      fmt::join(snapshot->steps | std::views::transform([](auto& kv) {
                  auto& [job_id, steps] = kv;
                  return steps |
                         std::views::transform(
                             [job_id](const StepInEmbeddedDb& step_in_db) {
                               return std::make_pair(
                                   job_id, step_in_db.runtime_attr().step_id());
                             });
                }) | std::views::join |
                    std::views::transform(util::StepIdPairToString) |
                    std::views::common,
                ","));

  return true;
}

bool EmbeddedDbClient::RetrieveReservationInfo(
    std::unordered_map<ResvId, crane::grpc::CreateReservationRequest>*
        reservation_info) {
  std::expected<void, DbErrorCode> result;

  result = m_resv_db_->IterateAllKv(
      [&](std::string&& key, std::vector<uint8_t>&& value) {
        crane::grpc::CreateReservationRequest info;
        info.ParseFromArray(value.data(), value.size());

        reservation_info->emplace(key, std::move(info));

        return true;
      });

  if (!result) {
    CRANE_ERROR("Failed to restore the reservation info into queues");
    return false;
  }

  return true;
}

bool EmbeddedDbClient::AppendTasksToPendingAndAdvanceTaskIds(
    const std::vector<TaskInCtld*>& tasks) {
  txn_id_t txn_id;
  std::expected<void, DbErrorCode> result;

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
    if (!result) {
      CRANE_ERROR(
          "Failed to store the fixed data of task id: {} / task db id: {}.",
          task->TaskId(), task->TaskDbId());

      // Just drop this batch if any of them failed.
      return false;
    }
  }

  if (!CommitDbTransaction_(m_fixed_db_.get(), txn_id)) return false;

  if (!BeginDbTransaction_(m_variable_db_.get(), &txn_id)) return false;

  for (const auto& task : tasks) {
    result = StoreTypeIntoDb_(m_variable_db_.get(), txn_id,
                              GetVariableDbEntryName_(task->TaskDbId()),
                              &task->RuntimeAttr());
    if (!result) {
      CRANE_ERROR(
          "Failed to store the variable data of "
          "task id: {} / task db id: {}.",
          task->TaskId(), task->TaskDbId());
      return false;
    }
  }

  result = StoreTypeIntoDb_(m_variable_db_.get(), txn_id, s_next_task_id_str_,
                            &task_id);
  if (!result) {
    CRANE_ERROR("Failed to store next_task_id.");
    return false;
  }

  result = StoreTypeIntoDb_(m_variable_db_.get(), txn_id,
                            s_next_task_db_id_str_, &task_db_id);
  if (!result) {
    CRANE_ERROR("Failed to store next_task_db_id.");
    return false;
  }
  if (!CommitDbTransaction_(m_variable_db_.get(), txn_id)) return false;

  s_next_task_id_ = task_id;
  s_next_task_db_id_ = task_db_id;

  return true;
}

bool EmbeddedDbClient::PurgeEndedTasks(
    const std::unordered_map<job_id_t, task_db_id_t>& job_ids) {
  // To ensure consistency of both fixed data db and variable data db under
  // failure, we must ensure that:
  // 1. when inserting task data, fixed data db is written before variable db;
  // 2. when erasing task data, fixed data db is erased after variable db;

  txn_id_t txn_id;
  std::expected<void, DbErrorCode> res;

  if (!BeginDbTransaction_(m_variable_db_.get(), &txn_id)) return false;
  for (const auto& id : job_ids | std::views::values) {
    res = m_variable_db_->Delete(txn_id, GetVariableDbEntryName_(id));
    if (!res) {
      CRANE_ERROR(
          "Failed to delete embedded variable data entry. Error code: {}",
          int(res.error()));
      return false;
    }
  }
  if (!CommitDbTransaction_(m_variable_db_.get(), txn_id)) return false;

  if (!BeginDbTransaction_(m_fixed_db_.get(), &txn_id)) return false;
  for (const auto& id : job_ids | std::views::values) {
    res = m_fixed_db_->Delete(txn_id, GetFixedDbEntryName_(id));
    if (!res) {
      CRANE_ERROR("Failed to delete embedded fixed data entry. Error code: {}",
                  int(res.error()));
      return false;
    }
  }
  if (!CommitDbTransaction_(m_fixed_db_.get(), txn_id)) return false;

  absl::MutexLock lock_ids(&s_step_id_mtx_);
  auto next_step_id_map{s_next_step_id_map_.job_id_next_step_id_map()};
  for (const auto& job_id : job_ids | std::views::keys) {
    next_step_id_map.erase(job_id);
  }
  if (!BeginDbTransaction_(m_step_var_db_.get(), &txn_id)) return false;
  crane::grpc::StepNextIdInEmbeddedDb db_next_step_id_map;
  *db_next_step_id_map.mutable_job_id_next_step_id_map() = next_step_id_map;
  res = StoreTypeIntoDb_(m_step_var_db_.get(), txn_id, s_next_step_id_str_,
                         &db_next_step_id_map);
  if (!res) {
    CRANE_ERROR("Failed to store next_step_id_map.");
    return false;
  }
  if (!CommitDbTransaction_(m_step_var_db_.get(), txn_id)) return false;

  return true;
}

bool EmbeddedDbClient::AppendSteps(const std::vector<StepInCtld*>& steps) {
  txn_id_t txn_id;
  std::expected<void, DbErrorCode> result;

  // Note: In current implementation, this function is called by only
  // one single thread and the lock here is actually useless.
  // However, it costs little and prevents race condition,
  // so we just leave it here.
  absl::MutexLock lock_ids(&s_step_id_mtx_);

  db_id_t step_db_id{s_next_step_db_id_};
  auto next_step_id_map{s_next_step_id_map_.job_id_next_step_id_map()};

  if (!BeginDbTransaction_(m_step_fixed_db_.get(), &txn_id)) return false;

  for (const auto& step : steps) {
    if (!next_step_id_map.contains(step->job_id)) {
      next_step_id_map[step->job_id] = 0;
    }
    step->SetStepId(next_step_id_map[step->job_id]++);
    step->SetStepDbId(step_db_id++);

    result = StoreTypeIntoDb_(m_step_fixed_db_.get(), txn_id,
                              GetStepFixedDbEntryName_(step->StepDbId()),
                              &step->StepToCtld());
    if (!result) {
      CRANE_ERROR(
          "Failed to store the fixed data of step id: {} / step db id: {}.",
          step->StepId(), step->StepDbId());

      // Just drop this batch if any of them failed.
      return false;
    }
  }

  if (!CommitDbTransaction_(m_step_fixed_db_.get(), txn_id)) return false;

  if (!BeginDbTransaction_(m_step_var_db_.get(), &txn_id)) return false;

  for (const auto& step : steps) {
    result = StoreTypeIntoDb_(m_step_var_db_.get(), txn_id,
                              GetStepVariableDbEntryName_(step->StepDbId()),
                              &step->RuntimeAttr());
    if (!result) {
      CRANE_ERROR(
          "Failed to store the variable data of "
          "step id: {} / step db id: {}.",
          step->StepId(), step->StepDbId());
      return false;
    }
  }
  crane::grpc::StepNextIdInEmbeddedDb db_next_step_id_map;
  *db_next_step_id_map.mutable_job_id_next_step_id_map() = next_step_id_map;
  result = StoreTypeIntoDb_(m_step_var_db_.get(), txn_id, s_next_step_id_str_,
                            &db_next_step_id_map);
  if (!result) {
    CRANE_ERROR("Failed to store next_step_id.");
    return false;
  }

  result = StoreTypeIntoDb_(m_step_var_db_.get(), txn_id,
                            s_next_step_db_id_str_, &step_db_id);
  if (!result) {
    CRANE_ERROR("Failed to store next_step_db_id.");
    return false;
  }
  if (!CommitDbTransaction_(m_step_var_db_.get(), txn_id)) return false;

  s_next_step_id_map_ = db_next_step_id_map;
  s_next_step_db_id_ = step_db_id;

  return true;
}

bool EmbeddedDbClient::PurgeEndedSteps(
    const std::vector<step_db_id_t>& db_ids) {
  // To ensure consistency of both fixed data db and variable data db under
  // failure, we must ensure that:
  // 1. when inserting step data, fixed data db is written before variable db;
  // 2. when erasing step data, fixed data db is erased after variable db;

  txn_id_t txn_id;
  std::expected<void, DbErrorCode> res;

  if (!BeginDbTransaction_(m_step_var_db_.get(), &txn_id)) return false;
  for (const auto& id : db_ids) {
    res = m_step_var_db_->Delete(txn_id, GetStepVariableDbEntryName_(id));
    if (!res) {
      CRANE_ERROR(
          "Failed to delete embedded variable data entry. Error code: {}",
          int(res.error()));
      return false;
    }
  }
  if (!CommitDbTransaction_(m_step_var_db_.get(), txn_id)) return false;

  if (!BeginDbTransaction_(m_step_fixed_db_.get(), &txn_id)) return false;
  for (const auto& id : db_ids) {
    res = m_step_fixed_db_->Delete(txn_id, GetStepFixedDbEntryName_(id));
    if (!res) {
      CRANE_ERROR("Failed to delete embedded fixed data entry. Error code: {}",
                  int(res.error()));
      return false;
    }
  }
  if (!CommitDbTransaction_(m_step_fixed_db_.get(), txn_id)) return false;

  return true;
}

}  // namespace Ctld

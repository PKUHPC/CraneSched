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
  unqlite_int64 n_bytes = static_cast<unqlite_int64>(*len);

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
    return rc;
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

    CRANE_ERROR("Failed to delete key {} from db: {}", key,
                GetInternalErrorStr_());
    if (rc != UNQLITE_NOTIMPLEMENTED) unqlite_rollback(m_db_);

    if (rc == UNQLITE_NOTFOUND) return result::failure(DbErrorCode::kNotFound);
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
    std::filesystem::path env_dir{m_env_home_};
    if (!std::filesystem::exists(env_dir))
      std::filesystem::create_directories(env_dir);
  } catch (const std::exception& e) {
    CRANE_CRITICAL("Invalid berkeley db env home path {}: {}", m_env_home_,
                   e.what());
    std::exit(1);
  }

  m_db_path_ = path;

  u_int32_t env_flags = DB_CREATE |      // If the environment does not
                                         // exist, create it.
                        DB_INIT_LOCK |   // Initialize locking
                        DB_INIT_LOG |    // Initialize logging
                        DB_INIT_MPOOL |  // Initialize the cache
                        DB_INIT_TXN |    // Initialize transactions
                        DB_RECOVER;

  u_int32_t oFlags = DB_CREATE | DB_AUTO_COMMIT;  // Open flags;

  try {
    m_env_ = std::make_unique<DbEnv>(0);
    m_env_->open(m_env_home_.c_str(), env_flags, 0);
    m_db_ = std::make_unique<Db>(m_env_.get(), 0);  // Instantiate the Db object

    // Open the database
    m_db_->open(nullptr,       // Transaction pointer
                path.c_str(),  // Database file name
                nullptr,       // Optional logical database name
                DB_HASH,       // Database access method
                oFlags,        // Open flags
                0);            // File mode (using defaults)

    // DbException is not subclassed from std::exception, so
    // need to catch both of these.
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
      CRANE_ERROR("Failed to get value size for key {}. {}", key, rc);
      return result::failure(DbErrorCode::kOther);
    }
  }

  if (rc != 0) {
    if (rc == DB_NOTFOUND)
      return result::failure(DbErrorCode::kNotFound);
    else
      return result::failure(DbErrorCode::kOther);
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
  if (m_embedded_db_) {
    auto result = m_embedded_db_->Close();
    if (result.has_error())
      CRANE_ERROR("Error occurred when closing the embedded db!");
  }
}

bool EmbeddedDbClient::Init(const std::string& db_path) {
  if (g_config.CraneEmbeddedDbBackend == "Unqlite") {
#ifdef CRANE_HAVE_UNQLITE
    m_embedded_db_ = std::make_unique<UnqliteDb>();
#else
    CRANE_ERROR(
        "Select unqlite as the embedded db but it's not been compiled.");
    return false;
#endif

  } else if (g_config.CraneEmbeddedDbBackend == "BerkeleyDB") {
#ifdef CRANE_HAVE_BERKELEY_DB
    m_embedded_db_ = std::make_unique<BerkeleyDb>();
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

  auto result = m_embedded_db_->Init(db_path);
  if (result.has_error()) return false;

  bool ok;

  // There is no race during Init stage.
  // No lock is needed.
  ok = FetchTypeFromDbOrInitWithValueNoLockAndTxn_(0, s_next_task_id_str_,
                                                   &s_next_task_id_, 1u);
  if (!ok) return false;

  ok = FetchTypeFromDbOrInitWithValueNoLockAndTxn_(0, s_next_task_db_id_str_,
                                                   &s_next_task_db_id_, 1l);
  if (!ok) return false;

  std::string pd_head_next_name =
      GetDbQueueNodeNextName_(s_pending_queue_head_.db_id);
  ok = FetchTypeFromDbOrInitWithValueNoLockAndTxn_(
      0, pd_head_next_name, &s_pending_queue_head_.next_db_id,
      s_pending_tail_db_id_);
  if (!ok) return false;

  std::string pd_tail_priv_name =
      GetDbQueueNodePrevName_(s_pending_queue_tail_.db_id);
  ok = FetchTypeFromDbOrInitWithValueNoLockAndTxn_(
      0, pd_tail_priv_name, &s_pending_queue_tail_.prev_db_id,
      s_pending_head_db_id_);
  if (!ok) return false;

  std::string r_head_next_name =
      GetDbQueueNodeNextName_(s_running_queue_head_.db_id);
  ok = FetchTypeFromDbOrInitWithValueNoLockAndTxn_(
      0, r_head_next_name, &s_running_queue_head_.next_db_id,
      s_running_tail_db_id_);
  if (!ok) return false;

  std::string r_tail_priv_name =
      GetDbQueueNodePrevName_(s_running_queue_tail_.db_id);
  ok = FetchTypeFromDbOrInitWithValueNoLockAndTxn_(
      0, r_tail_priv_name, &s_running_queue_tail_.prev_db_id,
      s_running_head_db_id_);
  if (!ok) return false;

  ok = FetchTypeFromDbOrInitWithValueNoLockAndTxn_(
      0, GetDbQueueNodeNextName_(s_ended_queue_head_.db_id),
      &s_ended_queue_head_.next_db_id, s_ended_tail_db_id_);
  if (!ok) return false;

  ok = FetchTypeFromDbOrInitWithValueNoLockAndTxn_(
      0, GetDbQueueNodePrevName_(s_ended_queue_tail_.db_id),
      &s_ended_queue_tail_.prev_db_id, s_ended_head_db_id_);
  if (!ok) return false;

  // Reconstruct the running queue and the pending queue.
  ok = ForEachInDbQueueNoLock_(0, s_pending_queue_head_, s_pending_queue_tail_,
                               [this](DbQueueNode const& node) {
                                 m_pending_queue_.emplace(node.db_id, node);
                               });
  if (!ok) {
    CRANE_ERROR("Failed to reconstruct pending queue.");
    return false;
  }

  ok = ForEachInDbQueueNoLock_(0, s_running_queue_head_, s_running_queue_tail_,
                               [this](DbQueueNode const& node) {
                                 m_running_queue_.emplace(node.db_id, node);
                               });
  if (!ok) {
    CRANE_ERROR("Failed to reconstruct running queue.");
    return false;
  }

  ok = ForEachInDbQueueNoLock_(0, s_ended_queue_head_, s_ended_queue_tail_,
                               [this](DbQueueNode const& node) {
                                 m_ended_queue_.emplace(node.db_id, node);
                               });
  if (!ok) {
    CRANE_ERROR("Failed to reconstruct ended queue.");
    return false;
  }

  return true;
}

bool EmbeddedDbClient::AppendTaskToPendingAndAdvanceTaskIds(txn_id_t txn_id,
                                                            TaskInCtld* task) {
  absl::MutexLock lock_queue(&m_queue_mtx_);
  absl::MutexLock lock_ids(&s_task_id_and_db_id_mtx_);

  uint32_t task_id{s_next_task_id_};
  db_id_t task_db_id{s_next_task_db_id_};

  bool ok, outer_txn = txn_id > 0;
  result::result<void, DbErrorCode> result;

  if (!outer_txn) {
    if (!BeginTransaction(&txn_id)) return false;
  }

  db_id_t pos = s_pending_queue_head_.next_db_id;
  ok = InsertBeforeDbQueueNodeNoLock_(txn_id, task_db_id, pos,
                                      &m_pending_queue_, &s_pending_queue_head_,
                                      &s_pending_queue_tail_);
  if (!ok) {
    if (!outer_txn) AbortTransaction(txn_id);
    return false;
  }

  result = StoreTypeIntoDb_(txn_id, GetDbQueueNodeTaskToCtldName_(task_db_id),
                            &task->TaskToCtld());
  if (result.has_error()) {
    if (!outer_txn) AbortTransaction(txn_id);
    return false;
  }

  task->SetTaskId(task_id);
  task->SetTaskDbId(task_db_id);

  result =
      StoreTypeIntoDb_(txn_id, GetDbQueueNodePersistedPartName_(task_db_id),
                       &task->PersistedPart());
  if (result.has_error()) {
    CRANE_ERROR("Failed to store the data of task id: {} / task db id: {}.",
                task_id, task_db_id);
    if (!outer_txn) AbortTransaction(txn_id);
    return false;
  }

  uint32_t next_task_id_to_store = s_next_task_id_ + 1;
  result =
      StoreTypeIntoDb_(txn_id, s_next_task_id_str_, &next_task_id_to_store);
  if (result.has_error()) {
    CRANE_ERROR("Failed to store next_task_id + 1.");
    if (!outer_txn) AbortTransaction(txn_id);
    return false;
  }

  db_id_t next_task_db_id_to_store = s_next_task_db_id_ + 1;
  result = StoreTypeIntoDb_(txn_id, s_next_task_db_id_str_,
                            &next_task_db_id_to_store);
  if (result.has_error()) {
    CRANE_ERROR("Failed to store next_task_db_id + 1.");
    if (!outer_txn) AbortTransaction(txn_id);
    return false;
  }

  if (!outer_txn) {
    ok = CommitTransaction(txn_id);
    if (!ok) {
      AbortTransaction(txn_id);
      return false;
    }
  }

  s_next_task_id_++;
  s_next_task_db_id_++;

  return true;
}

bool EmbeddedDbClient::MovePendingOrRunningTaskToEnded(
    txn_id_t txn_id, EmbeddedDbClient::db_id_t db_id) {
  absl::MutexLock l(&m_queue_mtx_);

  bool ok, outer_txn = txn_id > 0;

  if (!outer_txn) {
    if (!BeginTransaction(&txn_id)) return false;
  }

  ok = DeleteDbQueueNodeNoLock_(txn_id, db_id, &m_pending_queue_,
                                &s_pending_queue_head_, &s_pending_queue_tail_);
  if (!ok) {
    ok = DeleteDbQueueNodeNoLock_(txn_id, db_id, &m_running_queue_,
                                  &s_running_queue_head_,
                                  &s_running_queue_tail_);
    if (!ok) {
      if (!outer_txn) AbortTransaction(txn_id);
      return false;
    }
  }

  ok = InsertBeforeDbQueueNodeNoLock_(
      txn_id, db_id, s_ended_queue_head_.next_db_id, &m_ended_queue_,
      &s_ended_queue_head_, &s_ended_queue_tail_);
  if (!ok) {
    if (!outer_txn) AbortTransaction(txn_id);
    return false;
  }

  if (!outer_txn) {
    ok = CommitTransaction(txn_id);
    if (!ok) {
      AbortTransaction(txn_id);
      return false;
    }
  }

  return true;
}

bool EmbeddedDbClient::MoveTaskFromPendingToRunning(
    txn_id_t txn_id, EmbeddedDbClient::db_id_t db_id) {
  absl::MutexLock l(&m_queue_mtx_);

  bool ok, outer_txn = txn_id > 0;

  if (!outer_txn) {
    if (!BeginTransaction(&txn_id)) return false;
  }

  ok = DeleteDbQueueNodeNoLock_(txn_id, db_id, &m_pending_queue_,
                                &s_pending_queue_head_, &s_pending_queue_tail_);
  if (!ok) {
    if (!outer_txn) AbortTransaction(txn_id);
    return false;
  }

  ok = InsertBeforeDbQueueNodeNoLock_(
      txn_id, db_id, s_running_queue_head_.next_db_id, &m_running_queue_,
      &s_running_queue_head_, &s_running_queue_tail_);
  if (!ok) {
    if (!outer_txn) AbortTransaction(txn_id);
    return false;
  }

  if (!outer_txn) {
    ok = CommitTransaction(txn_id);
    if (!ok) {
      AbortTransaction(txn_id);
      return false;
    }
  }

  return true;
}

bool EmbeddedDbClient::MoveTaskFromRunningToPending(
    txn_id_t txn_id, EmbeddedDbClient::db_id_t db_id) {
  absl::MutexLock l(&m_queue_mtx_);

  bool ok, outer_txn = txn_id > 0;
  result::result<void, DbErrorCode> result;

  if (!outer_txn) {
    if (!BeginTransaction(&txn_id)) return false;
  }

  ok = DeleteDbQueueNodeNoLock_(txn_id, db_id, &m_running_queue_,
                                &s_running_queue_head_, &s_running_queue_tail_);
  if (!ok) {
    if (!outer_txn) AbortTransaction(txn_id);
    return false;
  }

  ok = InsertBeforeDbQueueNodeNoLock_(
      txn_id, db_id, s_pending_queue_head_.next_db_id, &m_pending_queue_,
      &s_pending_queue_head_, &s_pending_queue_tail_);
  if (!ok) {
    if (!outer_txn) AbortTransaction(txn_id);
    return false;
  }

  if (!outer_txn) {
    ok = CommitTransaction(txn_id);
    if (!ok) {
      AbortTransaction(txn_id);
      return false;
    }
  }

  return true;
}

bool EmbeddedDbClient::PurgeTaskFromEnded(txn_id_t txn_id,
                                          EmbeddedDbClient::db_id_t db_id) {
  absl::MutexLock l(&m_queue_mtx_);

  bool ok, outer_txn = txn_id > 0;
  result::result<void, DbErrorCode> result;

  if (!outer_txn) {
    if (!BeginTransaction(&txn_id)) return false;
  }

  if (!DeleteDbQueueNodeNoLock_(txn_id, db_id, &m_ended_queue_,
                                &s_ended_queue_head_, &s_ended_queue_tail_)) {
    if (!outer_txn) AbortTransaction(txn_id);
    return false;
  }

  result = m_embedded_db_->Delete(txn_id, GetDbQueueNodeTaskToCtldName_(db_id));
  if (result.has_error()) {
    if (!outer_txn) AbortTransaction(txn_id);
    return false;
  }

  result =
      m_embedded_db_->Delete(txn_id, GetDbQueueNodePersistedPartName_(db_id));
  if (result.has_error()) {
    if (!outer_txn) AbortTransaction(txn_id);
    return false;
  }

  if (!outer_txn) {
    ok = CommitTransaction(txn_id);
    if (!ok) {
      AbortTransaction(txn_id);
      return false;
    }
  }

  return true;
}

bool EmbeddedDbClient::InsertBeforeDbQueueNodeNoLock_(
    txn_id_t txn_id, db_id_t db_id, db_id_t pos,
    std::unordered_map<db_id_t, DbQueueNode>* q, DbQueueDummyHead* q_head,
    DbQueueDummyTail* q_tail) {
  bool outer_txn = txn_id > 0;
  result::result<void, DbErrorCode> result;
  db_id_t prev_db_id, next_db_id{pos};

  if (!outer_txn) {
    if (!BeginTransaction(&txn_id)) return false;
  }

  if (pos == q_head->db_id) return false;

  auto it = q->find(pos);
  if (it == q->end()) {
    if (pos == q_tail->db_id)
      prev_db_id = q_tail->prev_db_id;
    else
      return false;
  } else
    prev_db_id = it->second.prev_db_id;

  result =
      StoreTypeIntoDb_(txn_id, GetDbQueueNodeNextName_(db_id), &next_db_id);
  if (result.has_error()) {
    if (!outer_txn) AbortTransaction(txn_id);
    return false;
  }

  result =
      StoreTypeIntoDb_(txn_id, GetDbQueueNodePrevName_(db_id), &prev_db_id);
  if (result.has_error()) {
    if (!outer_txn) AbortTransaction(txn_id);
    return false;
  }

  result =
      StoreTypeIntoDb_(txn_id, GetDbQueueNodeNextName_(prev_db_id), &db_id);
  if (result.has_error()) {
    if (!outer_txn) AbortTransaction(txn_id);
    return false;
  }

  result =
      StoreTypeIntoDb_(txn_id, GetDbQueueNodePrevName_(next_db_id), &db_id);
  if (result.has_error()) {
    if (!outer_txn) AbortTransaction(txn_id);
    return false;
  }

  if (prev_db_id == q_head->db_id) {
    q_head->next_db_id = db_id;
  } else {
    q->at(prev_db_id).next_db_id = db_id;
  }

  if (next_db_id == q_tail->db_id) {
    q_tail->prev_db_id = db_id;
  } else {
    q->at(next_db_id).prev_db_id = db_id;
  }

  q->emplace(db_id, DbQueueNode{db_id, prev_db_id, next_db_id});

  if (!outer_txn) {
    return CommitTransaction(txn_id);
  }

  return true;
}

bool EmbeddedDbClient::DeleteDbQueueNodeNoLock_(
    txn_id_t txn_id, db_id_t db_id, std::unordered_map<db_id_t, DbQueueNode>* q,
    DbQueueDummyHead* q_head, DbQueueDummyTail* q_tail) {
  bool outer_txn = txn_id > 0;
  result::result<void, DbErrorCode> result;
  db_id_t prev_db_id, next_db_id;

  if (!outer_txn) {
    if (!BeginTransaction(&txn_id)) return false;
  }

  auto it = q->find(db_id);
  if (it != q->end()) {
    prev_db_id = it->second.prev_db_id;
    next_db_id = it->second.next_db_id;
  } else
    return false;

  result = StoreTypeIntoDb_(txn_id, GetDbQueueNodeNextName_(prev_db_id),
                            &next_db_id);
  if (result.has_error()) {
    if (!outer_txn) AbortTransaction(txn_id);
    return false;
  }

  result = StoreTypeIntoDb_(txn_id, GetDbQueueNodePrevName_(next_db_id),
                            &prev_db_id);
  if (result.has_error()) {
    if (!outer_txn) AbortTransaction(txn_id);
    return false;
  }

  if (prev_db_id == q_head->db_id)
    q_head->next_db_id = next_db_id;
  else
    q->at(prev_db_id).next_db_id = next_db_id;

  if (next_db_id == q_tail->db_id)
    q_tail->prev_db_id = prev_db_id;
  else
    q->at(next_db_id).prev_db_id = prev_db_id;

  q->erase(it);

  if (!outer_txn) return CommitTransaction(txn_id);

  return true;
}

bool EmbeddedDbClient::ForEachInDbQueueNoLock_(txn_id_t txn_id,
                                               DbQueueDummyHead dummy_head,
                                               DbQueueDummyTail dummy_tail,
                                               const ForEachInQueueFunc& func) {
  bool outer_txn = txn_id > 0;

  if (!outer_txn) {
    if (!BeginTransaction(&txn_id)) return false;
  }

  db_id_t prev_pos = dummy_head.db_id;
  db_id_t pos = dummy_head.next_db_id;
  while (pos != dummy_tail.db_id) {
    db_id_t next_pos;

    // Assert "<db_id>Next" exists in DB. If not so, the callback should not
    // be called.
    auto result =
        FetchTypeFromDb_(txn_id, GetDbQueueNodeNextName_(pos), &next_pos);
    if (result.has_error()) {
      if (!outer_txn) AbortTransaction(txn_id);
      return false;
    }

    func(DbQueueNode{pos, prev_pos, next_pos});

    prev_pos = pos;
    pos = next_pos;
  }

  if (!outer_txn) return CommitTransaction(txn_id);

  return true;
}

bool EmbeddedDbClient::GetQueueCopyNoLock_(
    txn_id_t txn_id, const std::unordered_map<db_id_t, DbQueueNode>& q,
    std::list<crane::grpc::TaskInEmbeddedDb>* list) {
  result::result<size_t, DbErrorCode> result;
  bool outer_txn = txn_id > 0;

  if (!outer_txn) {
    if (!BeginTransaction(&txn_id)) return false;
  }

  for (const auto& [key, value] : q) {
    crane::grpc::TaskInEmbeddedDb task_proto;
    result = FetchTypeFromDb_(txn_id, GetDbQueueNodeTaskToCtldName_(key),
                              task_proto.mutable_task_to_ctld());
    if (result.has_error()) {
      CRANE_ERROR("Failed to fetch task_to_ctld for task id {}.", key);
      if (!outer_txn) AbortTransaction(txn_id);
      return false;
    }

    result = FetchTypeFromDb_(txn_id, GetDbQueueNodePersistedPartName_(key),
                              task_proto.mutable_persisted_part());
    if (result.has_error()) {
      CRANE_ERROR("Failed to fetch persisted_part for task id {}.", key);
      if (!outer_txn) AbortTransaction(txn_id);
      return false;
    }
    list->emplace_back(std::move(task_proto));
  }

  if (!outer_txn) return CommitTransaction(txn_id);
  return true;
}

}  // namespace Ctld

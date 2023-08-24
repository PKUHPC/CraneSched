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
  unqlite_int64 n_bytes;

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

  return result::result<void, DbErrorCode>();
}

result::result<txn_id_t, DbErrorCode> UnqliteDb::Begin() {
  int rc;
  while (true) {
    rc = unqlite_begin(m_db_);
    if (rc == UNQLITE_OK) return rc;
    if (rc == UNQLITE_BUSY) {
      std::this_thread::yield();
      continue;
    }
    CRANE_ERROR("Failed to begin transaction: {}", GetInternalErrorStr_());
    return result::failure(DbErrorCode::kOther);
  }

  return {s_fixed_txn_id_};
};

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
    if (rc != UNQLITE_NOTIMPLEMENTED) unqlite_rollback(m_db_);
    return result::failure(kOther);
  }
};

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

EmbeddedDbClient::~EmbeddedDbClient() {
  auto result = m_embedded_db_->Close();
  if (result.has_error())
    CRANE_ERROR("Error occurred when closing the embedded db!");
}

bool EmbeddedDbClient::Init(const std::string& db_path) {
  m_embedded_db_ = std::make_unique<UnqliteDb>();
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
  ok = ForEachInDbQueueNoLockAndTxn_(
      0, s_pending_queue_head_, s_pending_queue_tail_,
      [this](DbQueueNode const& node) {
        m_pending_queue_.emplace(node.db_id, node);
      });
  if (!ok) {
    CRANE_ERROR("Failed to reconstruct pending queue.");
    return false;
  }

  ok = ForEachInDbQueueNoLockAndTxn_(
      0, s_running_queue_head_, s_running_queue_tail_,
      [this](DbQueueNode const& node) {
        m_running_queue_.emplace(node.db_id, node);
      });
  if (!ok) {
    CRANE_ERROR("Failed to reconstruct running queue.");
    return false;
  }

  ok =
      ForEachInDbQueueNoLockAndTxn_(0, s_ended_queue_head_, s_ended_queue_tail_,
                                    [this](DbQueueNode const& node) {
                                      m_ended_queue_.emplace(node.db_id, node);
                                    });
  if (!ok) {
    CRANE_ERROR("Failed to reconstruct ended queue.");
    return false;
  }

  return true;
}

bool EmbeddedDbClient::AppendTaskToPendingAndAdvanceTaskIds(TaskInCtld* task) {
  int rc;

  absl::MutexLock lock_queue(&m_queue_mtx_);
  absl::MutexLock lock_ids(&s_task_id_and_db_id_mtx_);

  uint32_t task_id{s_next_task_id_};
  db_id_t task_db_id{s_next_task_db_id_};

  rc = BeginTransaction_();
  if (rc != UNQLITE_OK) return false;

  db_id_t pos = s_pending_queue_head_.next_db_id;
  rc = InsertBeforeDbQueueNodeNoLockAndTxn_(task_db_id, pos, &m_pending_queue_,
                                            &s_pending_queue_head_,
                                            &s_pending_queue_tail_);
  if (rc != UNQLITE_OK) return false;

  rc = StoreTypeIntoDb_(GetDbQueueNodeTaskToCtldName_(task_db_id),
                        &task->TaskToCtld());
  if (rc != UNQLITE_OK) return false;

  task->SetTaskId(task_id);
  task->SetTaskDbId(task_db_id);

  rc = StoreTypeIntoDb_(GetDbQueueNodePersistedPartName_(task_db_id),
                        &task->PersistedPart());
  if (rc != UNQLITE_OK) {
    CRANE_ERROR("Failed to store the data of task id: {} / task db id: {}. {}",
                task_id, task_db_id, GetInternalErrorStr_());
    return false;
  }

  uint32_t next_task_id_to_store = s_next_task_id_ + 1;
  rc = StoreTypeIntoDb_(s_next_task_id_str_, &next_task_id_to_store);
  if (rc != UNQLITE_OK) {
    CRANE_ERROR("Failed to store next_task_id + 1: {}", GetInternalErrorStr_());
    return false;
  }

  db_id_t next_task_db_id_to_store = s_next_task_db_id_ + 1;
  rc = StoreTypeIntoDb_(s_next_task_db_id_str_, &next_task_db_id_to_store);
  if (rc != UNQLITE_OK) {
    CRANE_ERROR("Failed to store next_task_db_id + 1: {}",
                GetInternalErrorStr_());
    return false;
  }

  rc = Commit_();
  if (rc != UNQLITE_OK) {
    CRANE_ERROR("Commit error: {}", GetInternalErrorStr_());
    return false;
  }

  s_next_task_id_++;
  s_next_task_db_id_++;

  return true;
}

int EmbeddedDbClient::DeleteDbQueueNodeNoLockAndTxn_(
    EmbeddedDbClient::db_id_t db_id,
    std::unordered_map<db_id_t, DbQueueNode>* q, DbQueueDummyHead* q_head,
    DbQueueDummyTail* q_tail) {
  db_id_t prev_db_id, next_db_id;

  int rc;
  auto it = q->find(db_id);
  if (it != q->end()) {
    prev_db_id = it->second.prev_db_id;
    next_db_id = it->second.next_db_id;
  } else
    return UNQLITE_NOTFOUND;

  rc = StoreTypeIntoDb_(GetDbQueueNodeNextName_(prev_db_id), &next_db_id);
  if (rc != UNQLITE_OK) return rc;

  rc = StoreTypeIntoDb_(GetDbQueueNodePrevName_(next_db_id), &prev_db_id);
  if (rc != UNQLITE_OK) return rc;

  if (prev_db_id == q_head->db_id)
    q_head->next_db_id = next_db_id;
  else
    q->at(prev_db_id).next_db_id = next_db_id;

  if (next_db_id == q_tail->db_id)
    q_tail->prev_db_id = prev_db_id;
  else
    q->at(next_db_id).prev_db_id = prev_db_id;

  q->erase(it);

  return rc;
}

int EmbeddedDbClient::DeleteKeyFromDbAtomic_(const std::string& key) {
  int rc;
  while (true) {
    rc = unqlite_kv_delete(m_db_, key.c_str(), key.size());
    if (rc == UNQLITE_OK) return rc;
    if (rc == UNQLITE_BUSY) {
      std::this_thread::yield();
      continue;
    }

    CRANE_ERROR("Failed to delete key {} from db: {}", key,
                GetInternalErrorStr_());
    if (rc != UNQLITE_NOTIMPLEMENTED) unqlite_rollback(m_db_);
    return rc;
  }
}

bool EmbeddedDbClient::MovePendingOrRunningTaskToEnded(
    EmbeddedDbClient::db_id_t db_id) {
  absl::MutexLock l(&m_queue_mtx_);
  int rc;

  rc = BeginTransaction_();
  if (rc != UNQLITE_OK) return false;

  rc = DeleteDbQueueNodeNoLockAndTxn_(
      db_id, &m_pending_queue_, &s_pending_queue_head_, &s_pending_queue_tail_);
  if (rc != UNQLITE_OK) {
    rc = DeleteDbQueueNodeNoLockAndTxn_(db_id, &m_running_queue_,
                                        &s_running_queue_head_,
                                        &s_running_queue_tail_);
    if (rc != UNQLITE_OK) return false;
  }

  rc = InsertBeforeDbQueueNodeNoLockAndTxn_(
      db_id, s_ended_queue_head_.next_db_id, &m_ended_queue_,
      &s_ended_queue_head_, &s_ended_queue_tail_);
  if (rc != UNQLITE_OK) return false;

  rc = Commit_();
  if (rc != UNQLITE_OK) return false;

  return true;
}

int EmbeddedDbClient::InsertBeforeDbQueueNodeNoLockAndTxn_(
    EmbeddedDbClient::db_id_t db_id, EmbeddedDbClient::db_id_t pos,
    std::unordered_map<db_id_t, DbQueueNode>* q, DbQueueDummyHead* q_head,
    DbQueueDummyTail* q_tail) {
  int rc;
  db_id_t prev_db_id, next_db_id{pos};

  if (pos == q_head->db_id) return UNQLITE_INVALID;

  auto it = q->find(pos);
  if (it == q->end()) {
    if (pos == q_tail->db_id)
      prev_db_id = q_tail->prev_db_id;
    else
      return UNQLITE_NOTFOUND;
  } else
    prev_db_id = it->second.prev_db_id;

  rc = StoreTypeIntoDb_(GetDbQueueNodeNextName_(db_id), &next_db_id);
  if (rc != UNQLITE_OK) return rc;

  rc = StoreTypeIntoDb_(GetDbQueueNodePrevName_(db_id), &prev_db_id);
  if (rc != UNQLITE_OK) return rc;

  rc = StoreTypeIntoDb_(GetDbQueueNodeNextName_(prev_db_id), &db_id);
  if (rc != UNQLITE_OK) return rc;

  rc = StoreTypeIntoDb_(GetDbQueueNodePrevName_(next_db_id), &db_id);
  if (rc != UNQLITE_OK) return rc;

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

  return UNQLITE_OK;
}

bool EmbeddedDbClient::MoveTaskFromPendingToRunning(
    EmbeddedDbClient::db_id_t db_id) {
  absl::MutexLock l(&m_queue_mtx_);
  int rc;

  rc = BeginTransaction_();
  if (rc != UNQLITE_OK) return false;

  rc = DeleteDbQueueNodeNoLockAndTxn_(
      db_id, &m_pending_queue_, &s_pending_queue_head_, &s_pending_queue_tail_);
  if (rc != UNQLITE_OK) return false;

  rc = InsertBeforeDbQueueNodeNoLockAndTxn_(
      db_id, s_running_queue_head_.next_db_id, &m_running_queue_,
      &s_running_queue_head_, &s_running_queue_tail_);
  if (rc != UNQLITE_OK) return false;

  rc = Commit_();
  if (rc != UNQLITE_OK) return false;

  return true;
}

bool EmbeddedDbClient::MoveTaskFromRunningToPending(
    EmbeddedDbClient::db_id_t db_id) {
  absl::MutexLock l(&m_queue_mtx_);
  int rc;

  rc = BeginTransaction_();
  if (rc != UNQLITE_OK) return false;

  rc = DeleteDbQueueNodeNoLockAndTxn_(
      db_id, &m_running_queue_, &s_running_queue_head_, &s_running_queue_tail_);
  if (rc != UNQLITE_OK) return false;

  rc = InsertBeforeDbQueueNodeNoLockAndTxn_(
      db_id, s_pending_queue_head_.next_db_id, &m_pending_queue_,
      &s_pending_queue_head_, &s_pending_queue_tail_);
  if (rc != UNQLITE_OK) return false;

  rc = Commit_();
  if (rc != UNQLITE_OK) return false;

  return true;
}

int EmbeddedDbClient::GetQueueCopyNoLock_(
    const std::unordered_map<db_id_t, DbQueueNode>& q,
    std::list<crane::grpc::TaskInEmbeddedDb>* list) {
  int rc = UNQLITE_OK;

  for (const auto& [key, value] : q) {
    crane::grpc::TaskInEmbeddedDb task_proto;
    rc = FetchTypeFromDb_(GetDbQueueNodeTaskToCtldName_(key),
                          task_proto.mutable_task_to_ctld());
    if (rc != UNQLITE_OK) {
      CRANE_ERROR("Failed to fetch task_to_ctld for task id {}: {}", key,
                  GetInternalErrorStr_());
      return rc;
    }
    rc = FetchTypeFromDb_(GetDbQueueNodePersistedPartName_(key),
                          task_proto.mutable_persisted_part());
    if (rc != UNQLITE_OK) {
      CRANE_ERROR("Failed to fetch persisted_part for task id {}: {}", key,
                  GetInternalErrorStr_());
      return rc;
    }
    list->emplace_back(std::move(task_proto));
  }

  return rc;
}

bool EmbeddedDbClient::ForEachInDbQueueNoLockAndTxn_(
    txn_id_t txn_id, EmbeddedDbClient::DbQueueDummyHead dummy_head,
    EmbeddedDbClient::DbQueueDummyTail dummy_tail,
    const EmbeddedDbClient::ForEachInQueueFunc& func) {
  db_id_t prev_pos = dummy_head.db_id;
  db_id_t pos = dummy_head.next_db_id;
  while (pos != dummy_tail.db_id) {
    db_id_t next_pos;

    // Assert "<db_id>Next" exists in DB. If not so, the callback should not
    // be called.
    auto result = FetchTypeFromDb_(0, GetDbQueueNodeNextName_(pos), &next_pos);
    if (result.has_error()) return false;

    func(DbQueueNode{pos, prev_pos, next_pos});

    prev_pos = pos;
    pos = next_pos;
  }

  return true;
}

bool EmbeddedDbClient::FetchTaskDataInDb(
    EmbeddedDbClient::db_id_t db_id,
    EmbeddedDbClient::TaskInEmbeddedDb* task_in_db) {
  int rc;
  rc = FetchTaskDataInDbAtomic_(db_id, task_in_db);
  return rc == UNQLITE_OK;
}

bool EmbeddedDbClient::PurgeTaskFromEnded(EmbeddedDbClient::db_id_t db_id) {
  absl::MutexLock l(&m_queue_mtx_);
  int rc;

  rc = BeginTransaction_();
  if (rc != UNQLITE_OK) return false;

  rc = DeleteDbQueueNodeNoLockAndTxn_(
      db_id, &m_ended_queue_, &s_ended_queue_head_, &s_ended_queue_tail_);
  if (rc != UNQLITE_OK) return false;

  rc = DeleteKeyFromDbAtomic_(GetDbQueueNodeTaskToCtldName_(db_id));
  if (rc != UNQLITE_OK) return false;

  rc = DeleteKeyFromDbAtomic_(GetDbQueueNodePersistedPartName_(db_id));
  if (rc != UNQLITE_OK) return false;

  rc = Commit_();
  if (rc != UNQLITE_OK) return false;

  return true;
}

}  // namespace Ctld

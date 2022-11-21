#include "EmbeddedDbClient.h"

namespace Ctld {

EmbeddedDbClient::~EmbeddedDbClient() {
  int rc;
  if (m_db_ != nullptr) {
    rc = unqlite_close(m_db_);
    if (rc != UNQLITE_OK) {
      CRANE_ERROR("Failed to close unqlite: {}", GetInternalErrorStr_());
    }
  }
}

bool EmbeddedDbClient::Init(const std::string& db_path) {
  int rc;
  rc = unqlite_open(&m_db_, m_db_path_.c_str(), UNQLITE_OPEN_CREATE);
  if (rc != UNQLITE_OK) {
    m_db_ = nullptr;
    CRANE_ERROR("Failed to open unqlite db file {}: {}", m_db_path_,
                GetInternalErrorStr_());
    return false;
  }

  // There is no race during Init stage.
  // No lock is needed.
  rc = FetchTypeFromDb_(s_next_task_id_str_, &s_next_task_id_);
  if (rc != UNQLITE_OK) {
    if (rc == UNQLITE_NOTFOUND) {
      s_next_task_id_ = 0;
      rc = StoreTypeIntoDb_(s_next_task_id_str_, &s_next_task_id_);
      if (rc != UNQLITE_OK) {
        CRANE_ERROR("Failed to init {} in db: {}", s_next_task_id_str_,
                    GetInternalErrorStr_());
      }
    } else
      CRANE_ERROR("Failed to fetch {} from db: {}", s_next_task_id_str_,
                  GetInternalErrorStr_());
  }

  rc = FetchTypeFromDb_(s_next_task_db_id_str_, &s_next_task_db_id_);
  if (rc != UNQLITE_OK) {
    if (rc == UNQLITE_NOTFOUND) {
      s_next_task_db_id_ = 0;
      rc = StoreTypeIntoDb_(s_next_task_db_id_str_, &s_next_task_db_id_);
      if (rc != UNQLITE_OK) {
        CRANE_ERROR("Failed to init {} in db: {}", s_next_task_db_id_str_,
                    GetInternalErrorStr_());
      }
    } else
      CRANE_ERROR("Failed to fetch {} from db: {}", s_next_task_db_id_str_,
                  GetInternalErrorStr_());
  }

  std::string pd_head_next_name =
      GetDbQueueNodeNextName_(s_pending_queue_head_.db_id);
  rc = FetchTypeFromDb_(pd_head_next_name, &s_pending_queue_head_.next_db_id);
  if (rc != UNQLITE_OK) {
    if (rc == UNQLITE_NOTFOUND) {
      s_pending_queue_head_.next_db_id = s_pending_tail_db_id_;
      rc = StoreTypeIntoDb_(pd_head_next_name,
                            &s_pending_queue_head_.next_db_id);
      if (rc != UNQLITE_OK) {
        CRANE_ERROR("Failed to init pending_queue_head_.next_db_id: {}",
                    GetInternalErrorStr_());
        return false;
      }
    }
  }

  std::string pd_tail_priv_name =
      GetDbQueueNodePrevName_(s_pending_queue_tail_.db_id);
  rc = FetchTypeFromDb_(pd_tail_priv_name, &s_pending_queue_tail_.prev_db_id);
  if (rc != UNQLITE_OK) {
    if (rc == UNQLITE_NOTFOUND) {
      s_pending_queue_tail_.prev_db_id = s_pending_head_db_id_;
      rc = StoreTypeIntoDb_(pd_tail_priv_name,
                            &s_pending_queue_tail_.prev_db_id);
      if (rc != UNQLITE_OK) {
        CRANE_ERROR("Failed to init pending_queue_tail_.priv_db_id: {}",
                    GetInternalErrorStr_());
        return false;
      }
    }
  }

  std::string r_head_next_name =
      GetDbQueueNodeNextName_(s_running_queue_head_.db_id);
  rc = FetchTypeFromDb_(r_head_next_name, &s_running_queue_head_.next_db_id);
  if (rc != UNQLITE_OK) {
    if (rc == UNQLITE_NOTFOUND) {
      s_running_queue_head_.next_db_id = s_running_tail_db_id_;
      rc =
          StoreTypeIntoDb_(r_head_next_name, &s_running_queue_head_.next_db_id);
      if (rc != UNQLITE_OK) {
        CRANE_ERROR("Failed to init running_queue_head_.next_db_id: {}",
                    GetInternalErrorStr_());
        return false;
      }
    }
  }

  std::string r_tail_priv_name =
      GetDbQueueNodePrevName_(s_running_queue_tail_.db_id);
  rc = FetchTypeFromDb_(r_tail_priv_name, &s_running_queue_tail_.prev_db_id);
  if (rc != UNQLITE_OK) {
    if (rc == UNQLITE_NOTFOUND) {
      s_running_queue_tail_.prev_db_id = s_running_head_db_id_;
      rc =
          StoreTypeIntoDb_(r_tail_priv_name, &s_running_queue_tail_.prev_db_id);
      if (rc != UNQLITE_OK) {
        CRANE_ERROR("Failed to init running_queue_tail_.priv_db_id: {}",
                    GetInternalErrorStr_());
        return false;
      }
    }
  }

  // Reconstruct the running queue and the pending queue.
  rc = ForEachInDbQueueNoLockAndTxn_(
      s_pending_queue_head_, s_pending_queue_tail_,
      [this](DbQueueNode const& node) {
        m_pending_queue_.emplace(node.db_id, node);
      });
  if (rc != UNQLITE_OK) {
    CRANE_ERROR("Failed to reconstruct pending queue.");
    return false;
  }

  rc = ForEachInDbQueueNoLockAndTxn_(
      s_pending_queue_head_, s_pending_queue_tail_,
      [this](DbQueueNode const& node) {
        m_running_queue_.emplace(node.db_id, node);
      });
  if (rc != UNQLITE_OK) {
    CRANE_ERROR("Failed to reconstruct running queue.");
    return false;
  }

  return true;
}

std::string EmbeddedDbClient::GetInternalErrorStr_() {
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

bool EmbeddedDbClient::AppendTaskToPendingAndAdvanceTaskIds(
    crane::grpc::TaskToCtld const& task_to_ctld,
    crane::grpc::PersistedPartOfTaskInCtld* persisted_part) {
  int rc;

  absl::MutexLock lock_queue(&m_queue_mtx_);
  absl::MutexLock lock_ids(&s_task_id_and_db_id_mtx_);

  uint32_t task_id{s_next_task_id_};
  db_id_t task_db_id{s_next_task_db_id_};

  rc = BeginTransaction_();
  if (rc != UNQLITE_OK) return rc;

  db_id_t pos = s_pending_queue_head_.next_db_id;
  rc = InsertBeforeDbQueueNodeNoLockAndTxn_(task_db_id, pos);
  if (rc != UNQLITE_OK) return rc;

  crane::grpc::TaskInEmbeddedDb task_in_embedded_db;
  *task_in_embedded_db.mutable_task_to_ctld() = task_to_ctld;
  *task_in_embedded_db.mutable_persisted_part() = *persisted_part;
  task_in_embedded_db.mutable_persisted_part()->set_task_id(task_id);
  task_in_embedded_db.mutable_persisted_part()->set_task_db_id(task_db_id);
  rc = StoreTypeIntoDb_(GetDbQueueNodeDataName_(task_db_id),
                        &task_in_embedded_db);
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

  persisted_part->set_task_id(task_id);
  persisted_part->set_task_db_id(task_db_id);

  s_next_task_id_++;
  s_next_task_db_id_++;

  return true;
}

std::unordered_map<EmbeddedDbClient::db_id_t,
                   EmbeddedDbClient::DbQueueNode>::iterator
EmbeddedDbClient::FindDbQueueNodeInRamQueueNoLock_(
    EmbeddedDbClient::db_id_t db_id,
    std::unordered_map<db_id_t, DbQueueNode>& q) {
  auto db_queue_eq =
      [db_id](std::unordered_map<db_id_t, DbQueueNode>::value_type const& it) {
        return it.second.db_id == db_id;
      };

  return std::find_if(q.begin(), q.end(), db_queue_eq);
}

int EmbeddedDbClient::DeleteDbQueueNodeNoLockAndTxn_(
    EmbeddedDbClient::db_id_t db_id) {
  bool found_pending{false};
  bool found_running{false};
  db_id_t prev_db_id, next_db_id;

  int rc;
  auto pending_it = FindDbQueueNodeInRamQueueNoLock_(db_id, m_pending_queue_);
  if (pending_it != m_pending_queue_.end()) {
    found_pending = true;
    prev_db_id = pending_it->second.prev_db_id;
    next_db_id = pending_it->second.next_db_id;
  }

  std::unordered_map<db_id_t, DbQueueNode>::iterator running_it;
  if (!found_pending) {
    running_it = FindDbQueueNodeInRamQueueNoLock_(db_id, m_running_queue_);
    if (running_it != m_running_queue_.end()) {
      found_running = true;
      prev_db_id = running_it->second.prev_db_id;
      next_db_id = running_it->second.next_db_id;
    }
  }

  if (!(found_pending || found_running)) return UNQLITE_NOTFOUND;

  rc = StoreTypeIntoDb_(GetDbQueueNodeNextName_(prev_db_id), &next_db_id);
  if (rc != UNQLITE_OK) return rc;

  rc = StoreTypeIntoDb_(GetDbQueueNodePrevName_(next_db_id), &prev_db_id);
  if (rc != UNQLITE_OK) return rc;

  if (found_pending) {
    if (prev_db_id == s_pending_head_db_id_)
      s_pending_queue_head_.next_db_id = next_db_id;
    else
      m_pending_queue_.at(prev_db_id).next_db_id = next_db_id;

    if (next_db_id == s_pending_tail_db_id_)
      s_pending_queue_tail_.prev_db_id = prev_db_id;
    else
      m_pending_queue_.at(next_db_id).prev_db_id = prev_db_id;

    m_pending_queue_.erase(pending_it);
  }

  if (found_running) {
    if (prev_db_id == s_running_head_db_id_)
      s_running_queue_head_.next_db_id = next_db_id;
    else
      m_running_queue_.at(prev_db_id).next_db_id = next_db_id;

    if (next_db_id == s_running_tail_db_id_)
      s_running_queue_tail_.prev_db_id = prev_db_id;
    else
      m_running_queue_.at(next_db_id).prev_db_id = prev_db_id;

    m_running_queue_.erase(running_it);
  }

  return rc;
}

int EmbeddedDbClient::Commit_() {
  int rc;
  while (true) {
    rc = unqlite_commit(m_db_);
    if (rc == UNQLITE_OK) return rc;
    if (rc == UNQLITE_BUSY) {
      std::this_thread::yield();
      continue;
    }
    CRANE_ERROR("Failed to commit: {}", GetInternalErrorStr_());
    if (rc != UNQLITE_NOTIMPLEMENTED) unqlite_rollback(m_db_);
    return rc;
  }
}

int EmbeddedDbClient::BeginTransaction_() {
  int rc;
  while (true) {
    rc = unqlite_begin(m_db_);
    if (rc == UNQLITE_OK) return rc;
    if (rc == UNQLITE_BUSY) {
      std::this_thread::yield();
      continue;
    }
    CRANE_ERROR("Failed to begin transaction: {}", GetInternalErrorStr_());
    return rc;
  }
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

bool EmbeddedDbClient::DeleteTaskByDbId(EmbeddedDbClient::db_id_t db_id) {
  absl::MutexLock l(&m_queue_mtx_);
  int rc;

  rc = BeginTransaction_();
  if (rc != UNQLITE_OK) return false;

  rc = DeleteDbQueueNodeNoLockAndTxn_(db_id);
  if (rc != UNQLITE_OK) return false;

  rc = DeleteKeyFromDbAtomic_(GetDbQueueNodeDataName_(db_id));
  if (rc != UNQLITE_OK) return rc;

  rc = Commit_();
  if (rc != UNQLITE_OK) return false;

  return true;
}

int EmbeddedDbClient::InsertBeforeDbQueueNodeNoLockAndTxn_(
    EmbeddedDbClient::db_id_t db_id, EmbeddedDbClient::db_id_t pos) {
  int rc;
  db_id_t prev_db_id, next_db_id{pos};
  bool found_pending{false};
  bool found_running{false};

  if (pos == s_pending_head_db_id_ || pos == s_running_head_db_id_)
    return UNQLITE_INVALID;

  if (pos != s_pending_tail_db_id_ && pos != s_running_tail_db_id_) {
    auto pending_it = FindDbQueueNodeInRamQueueNoLock_(pos, m_pending_queue_);
    if (pending_it == m_pending_queue_.end()) {
      auto running_it = FindDbQueueNodeInRamQueueNoLock_(pos, m_running_queue_);
      if (running_it == m_running_queue_.end())
        return UNQLITE_NOTFOUND;
      else {
        found_running = true;
        prev_db_id = running_it->second.prev_db_id;
      }
    } else {
      found_pending = true;
      prev_db_id = pending_it->second.prev_db_id;
    }
  } else {
    if (pos == s_pending_tail_db_id_) {
      found_pending = true;
      prev_db_id = s_pending_queue_tail_.prev_db_id;
    } else {
      found_running = true;
      prev_db_id = s_running_queue_tail_.prev_db_id;
    }
  }

  rc = StoreTypeIntoDb_(GetDbQueueNodeNextName_(db_id), &next_db_id);
  if (rc != UNQLITE_OK) return rc;

  rc = StoreTypeIntoDb_(GetDbQueueNodePrevName_(db_id), &prev_db_id);
  if (rc != UNQLITE_OK) return rc;

  rc = StoreTypeIntoDb_(GetDbQueueNodeNextName_(prev_db_id), &db_id);
  if (rc != UNQLITE_OK) return rc;

  rc = StoreTypeIntoDb_(GetDbQueueNodePrevName_(next_db_id), &db_id);
  if (rc != UNQLITE_OK) return rc;

  if (prev_db_id == s_pending_head_db_id_ ||
      prev_db_id == s_running_head_db_id_) {
    if (prev_db_id == s_pending_head_db_id_)
      s_pending_queue_head_.next_db_id = db_id;
    else
      s_running_queue_head_.next_db_id = db_id;
  } else {
    if (found_pending)
      m_pending_queue_.at(prev_db_id).next_db_id = db_id;
    else
      m_running_queue_.at(prev_db_id).next_db_id = db_id;
  }

  if (next_db_id == s_pending_tail_db_id_ ||
      next_db_id == s_running_tail_db_id_) {
    if (next_db_id == s_pending_tail_db_id_)
      s_pending_queue_tail_.prev_db_id = db_id;
    else
      s_running_queue_tail_.prev_db_id = db_id;
  } else {
    if (found_pending)
      m_pending_queue_.at(next_db_id).prev_db_id = db_id;
    else
      m_running_queue_.at(next_db_id).prev_db_id = db_id;
  }

  if (found_pending)
    m_pending_queue_.emplace(db_id, DbQueueNode{db_id, prev_db_id, next_db_id});
  else
    m_running_queue_.emplace(db_id, DbQueueNode{db_id, prev_db_id, next_db_id});

  return UNQLITE_OK;
}

bool EmbeddedDbClient::MoveTaskFromPendingToRunning(
    EmbeddedDbClient::db_id_t task_db_id) {
  absl::MutexLock l(&m_queue_mtx_);
  int rc;

  rc = BeginTransaction_();
  if (rc != UNQLITE_OK) return false;

  rc = DeleteDbQueueNodeNoLockAndTxn_(task_db_id);
  if (rc != UNQLITE_OK) return false;

  rc = InsertBeforeDbQueueNodeNoLockAndTxn_(task_db_id,
                                            s_running_queue_head_.next_db_id);
  if (rc != UNQLITE_OK) return false;

  rc = Commit_();
  if (rc != UNQLITE_OK) return false;

  return true;
}

int EmbeddedDbClient::GetQueueCopyNoLock_(
    const std::unordered_map<db_id_t, DbQueueNode>& q,
    std::list<crane::grpc::TaskInEmbeddedDb>* list) {
  int rc;

  for (const auto& [key, value] : q) {
    crane::grpc::TaskInEmbeddedDb task_proto;
    rc = FetchTypeFromDb_(GetDbQueueNodeDataName_(key), &task_proto);
    if (rc != UNQLITE_OK) {
      CRANE_ERROR("Failed to fetch data for task id {}: {}", key,
                  GetInternalErrorStr_());
      return rc;
    }
    list->emplace_back(std::move(task_proto));
  }

  return rc;
}

int EmbeddedDbClient::ForEachInDbQueueNoLockAndTxn_(
    EmbeddedDbClient::DbQueueDummyHead dummy_head,
    EmbeddedDbClient::DbQueueDummyTail dummy_tail,
    const EmbeddedDbClient::ForEachInQueueFunc& func) {
  int rc;

  db_id_t prev_pos = dummy_head.db_id;
  db_id_t pos = dummy_head.next_db_id;
  while (pos != dummy_tail.db_id) {
    db_id_t next_pos;

    // Assert "<db_id>Next" exists in DB. If not so, the callback should not
    // be called.
    rc = FetchTypeFromDb_(GetDbQueueNodeNextName_(pos), &next_pos);
    if (rc != UNQLITE_OK) return rc;

    func(DbQueueNode{pos, prev_pos, next_pos});

    prev_pos = pos;
    pos = next_pos;
  }

  return UNQLITE_OK;
}

bool EmbeddedDbClient::GetMarkedDbId(EmbeddedDbClient::db_id_t* db_id) {
  absl::MutexLock l(&m_marked_db_id_mtx_);
  if (m_marked_db_id_ < 0) return false;

  *db_id = m_marked_db_id_;
  return true;
}

bool EmbeddedDbClient::SetMarkedDbId(EmbeddedDbClient::db_id_t db_id) {
  absl::MutexLock l(&m_marked_db_id_mtx_);
  if (m_marked_db_id_ < 0) {
    m_marked_db_id_ = db_id;

    int rc = StoreTypeIntoDb_(s_marked_db_id_str_, &m_marked_db_id_);
    if (rc != UNQLITE_OK) return false;
  } else
    return false;

  return true;
}

bool EmbeddedDbClient::UnsetMarkedDbId() {
  absl::MutexLock l(&m_marked_db_id_mtx_);

  m_marked_db_id_ = -1;

  int rc;
  rc = StoreTypeIntoDb_(s_marked_db_id_str_, &m_marked_db_id_);
  if (rc != UNQLITE_OK) return false;

  return true;
}

bool EmbeddedDbClient::FetchTaskDataInDb(
    EmbeddedDbClient::db_id_t db_id,
    EmbeddedDbClient::TaskInEmbeddedDb* task_in_db) {
  int rc;
  rc = FetchTaskDataInDbAtomic_(db_id, task_in_db);
  return rc == UNQLITE_OK;
}

}  // namespace Ctld

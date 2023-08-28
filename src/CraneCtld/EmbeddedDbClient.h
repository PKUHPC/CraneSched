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

#pragma once

#include "CtldPublicDefs.h"
// Precompiled header comes first!

#include <unqlite.h>

#include "protos/Crane.pb.h"

namespace Ctld {

using txn_id_t = int64_t;

enum DbErrorCode {
  kNotFound,
  kBufferSmall,
  kParsingError,
  kOther,
};

class IEmbeddedDb {
 public:
  virtual ~IEmbeddedDb() = default;

  virtual result::result<void, DbErrorCode> Init(std::string const& path) = 0;

  virtual result::result<void, DbErrorCode> Close() = 0;

  virtual result::result<void, DbErrorCode> Store(txn_id_t txn_id,
                                                  std::string const& key,
                                                  const void* data,
                                                  size_t len) = 0;

  virtual result::result<size_t, DbErrorCode> Fetch(txn_id_t txn_id,
                                                    std::string const& key,
                                                    void* buf, size_t* len) = 0;

  virtual result::result<void, DbErrorCode> Delete(txn_id_t txn_id,
                                                   std::string const& key) = 0;

  virtual result::result<txn_id_t, DbErrorCode> Begin() = 0;

  virtual result::result<void, DbErrorCode> Commit(txn_id_t txn_id) = 0;

  virtual result::result<void, DbErrorCode> Abort(txn_id_t txn_id) = 0;

  virtual std::string const& DbPath() = 0;
};

class UnqliteDb : public IEmbeddedDb {
 public:
  result::result<void, DbErrorCode> Init(const std::string& path) override;

  result::result<void, DbErrorCode> Close() override;

  result::result<void, DbErrorCode> Store(txn_id_t txn_id,
                                          const std::string& key,
                                          const void* data,
                                          size_t len) override;

  result::result<size_t, DbErrorCode> Fetch(txn_id_t txn_id,
                                            const std::string& key, void* buf,
                                            size_t* len) override;

  result::result<void, DbErrorCode> Delete(txn_id_t txn_id,
                                           const std::string& key) override;

  result::result<txn_id_t, DbErrorCode> Begin() override;

  result::result<void, DbErrorCode> Commit(txn_id_t txn_id) override;

  result::result<void, DbErrorCode> Abort(txn_id_t txn_id) override;

  const std::string& DbPath() override { return m_db_path_; };

  std::string GetInternalErrorStr_();

 private:
  static constexpr txn_id_t s_fixed_txn_id_ = 1;

  std::string m_db_path_;
  unqlite* m_db_{nullptr};
};

class EmbeddedDbClient {
 private:
  using db_id_t = task_db_id_t;
  using TaskInEmbeddedDb = crane::grpc::TaskInEmbeddedDb;

  struct DbQueueDummyHead {
    db_id_t db_id;
    db_id_t next_db_id;
  };

  struct DbQueueDummyTail {
    db_id_t db_id;
    db_id_t prev_db_id;
  };

  struct DbQueueNode {
    db_id_t db_id{0};
    db_id_t prev_db_id{0};
    db_id_t next_db_id{0};
  };

  using ForEachInQueueFunc = std::function<void(DbQueueNode const&)>;

  inline static constexpr db_id_t s_pending_head_db_id_ =
      std::numeric_limits<db_id_t>::max() - 0;
  inline static constexpr db_id_t s_pending_tail_db_id_ =
      std::numeric_limits<db_id_t>::max() - 1;

  inline static constexpr db_id_t s_running_head_db_id_ =
      std::numeric_limits<db_id_t>::max() - 2;
  inline static constexpr db_id_t s_running_tail_db_id_ =
      std::numeric_limits<db_id_t>::max() - 3;

  inline static constexpr db_id_t s_ended_head_db_id_ =
      std::numeric_limits<db_id_t>::max() - 4;
  inline static constexpr db_id_t s_ended_tail_db_id_ =
      std::numeric_limits<db_id_t>::max() - 5;

  inline static DbQueueDummyHead s_pending_queue_head_{
      .db_id = s_pending_head_db_id_};
  inline static DbQueueDummyTail s_pending_queue_tail_{
      .db_id = s_pending_tail_db_id_};

  inline static DbQueueDummyHead s_running_queue_head_{
      .db_id = s_running_head_db_id_};
  inline static DbQueueDummyTail s_running_queue_tail_{
      .db_id = s_running_tail_db_id_};

  inline static DbQueueDummyHead s_ended_queue_head_{.db_id =
                                                         s_ended_head_db_id_};
  inline static DbQueueDummyTail s_ended_queue_tail_{.db_id =
                                                         s_ended_tail_db_id_};

 public:
  EmbeddedDbClient() = default;
  ~EmbeddedDbClient();

  bool Init(std::string const& db_path);

  bool BeginTransaction(txn_id_t* txn_id) {
    auto result = m_embedded_db_->Begin();
    if (result.has_value()) {
      *txn_id = result.value();
      return true;
    }
    CRANE_ERROR("Failed to begin a transaction.");
    return false;
  }

  bool CommitTransaction(txn_id_t txn_id) {
    if (txn_id <= 0) {
      CRANE_ERROR("Commit a transaction with id {} <= 0", txn_id);
      return false;
    }

    return m_embedded_db_->Commit(txn_id).has_value();
  }

  bool AbortTransaction(txn_id_t txn_id) {
    if (txn_id <= 0) {
      CRANE_ERROR("Abort a transaction with id {} <= 0", txn_id);
      return false;
    }

    return m_embedded_db_->Abort(txn_id).has_value();
  }

  bool AppendTaskToPendingAndAdvanceTaskIds(txn_id_t txn_id, TaskInCtld* task);

  bool MovePendingOrRunningTaskToEnded(txn_id_t txn_id, db_id_t db_id);

  bool MoveTaskFromPendingToRunning(txn_id_t txn_id, db_id_t db_id);

  bool MoveTaskFromRunningToPending(txn_id_t txn_id, db_id_t db_id);

  bool PurgeTaskFromEnded(txn_id_t txn_id, db_id_t db_id);

  bool GetPendingQueueCopy(txn_id_t txn_id,
                           std::list<crane::grpc::TaskInEmbeddedDb>* list) {
    absl::MutexLock l(&m_queue_mtx_);
    return GetQueueCopyNoLock_(txn_id, m_pending_queue_, list);
  }

  bool GetRunningQueueCopy(txn_id_t txn_id,
                           std::list<crane::grpc::TaskInEmbeddedDb>* list) {
    absl::MutexLock l(&m_queue_mtx_);
    return GetQueueCopyNoLock_(txn_id, m_running_queue_, list);
  }

  bool GetEndedQueueCopy(txn_id_t txn_id,
                         std::list<crane::grpc::TaskInEmbeddedDb>* list) {
    absl::MutexLock l(&m_queue_mtx_);
    return GetQueueCopyNoLock_(txn_id, m_ended_queue_, list);
  }

  bool UpdatePersistedPartOfTask(
      txn_id_t txn_id, db_id_t db_id,
      crane::grpc::PersistedPartOfTaskInCtld const& persisted_part) {
    return StoreTypeIntoDb_(txn_id, GetDbQueueNodePersistedPartName_(db_id),
                            &persisted_part)
        .has_value();
  }

  bool UpdateTaskToCtld(txn_id_t txn_id, db_id_t db_id,
                        crane::grpc::TaskToCtld const& task_to_ctld) {
    return StoreTypeIntoDb_(txn_id, GetDbQueueNodeTaskToCtldName_(db_id),
                            &task_to_ctld)
        .has_value();
  }

  bool FetchTaskDataInDb(txn_id_t txn_id, db_id_t db_id,
                         TaskInEmbeddedDb* task_in_db) {  // Only used in test
    return FetchTaskDataInDbAtomic_(txn_id, db_id, task_in_db).has_value();
  }

 private:
  inline static std::string GetDbQueueNodeTaskToCtldName_(db_id_t db_id) {
    return fmt::format("{}TaskToCtld", db_id);
  }

  inline static std::string GetDbQueueNodePersistedPartName_(db_id_t db_id) {
    return fmt::format("{}Persisted", db_id);
  }

  inline static std::string GetDbQueueNodeNextName_(db_id_t db_id) {
    return fmt::format("{}Next", db_id);
  }

  inline static std::string GetDbQueueNodePrevName_(db_id_t db_id) {
    return fmt::format("{}Prev", db_id);
  }

  // Helper functions for the queue structure in the embedded db.

  bool InsertBeforeDbQueueNodeNoLock_(
      txn_id_t txn_id, db_id_t db_id, db_id_t pos,
      std::unordered_map<db_id_t, DbQueueNode>* q, DbQueueDummyHead* q_head,
      DbQueueDummyTail* q_tail);

  bool DeleteDbQueueNodeNoLock_(txn_id_t txn_id, db_id_t db_id,
                                std::unordered_map<db_id_t, DbQueueNode>* q,
                                DbQueueDummyHead* q_head,
                                DbQueueDummyTail* q_tail);

  bool ForEachInDbQueueNoLock_(txn_id_t txn_id, DbQueueDummyHead dummy_head,
                               DbQueueDummyTail dummy_tail,
                               const ForEachInQueueFunc& func);

  bool GetQueueCopyNoLock_(txn_id_t txn_id,
                           std::unordered_map<db_id_t, DbQueueNode> const& q,
                           std::list<crane::grpc::TaskInEmbeddedDb>* list);

  // -------------------

  // Helper functions for basic embedded db operations

  inline result::result<size_t, DbErrorCode> FetchTaskDataInDbAtomic_(
      txn_id_t txn_id, db_id_t db_id, TaskInEmbeddedDb* task_in_db) {
    auto result = FetchTypeFromDb_(txn_id, GetDbQueueNodeTaskToCtldName_(db_id),
                                   task_in_db->mutable_task_to_ctld());
    if (result.has_error()) return result;

    return FetchTypeFromDb_(txn_id, GetDbQueueNodePersistedPartName_(db_id),
                            task_in_db->mutable_persisted_part());
  }

  template <std::integral T>
  bool FetchTypeFromDbOrInitWithValueNoLockAndTxn_(txn_id_t txn_id,
                                                   std::string const& key,
                                                   T* buf, T value) {
    result::result<size_t, DbErrorCode> fetch_result =
        FetchTypeFromDb_(txn_id, key, buf);
    if (fetch_result.has_value()) return true;

    if (fetch_result.error() == DbErrorCode::kNotFound) {
      CRANE_TRACE(
          "Key {} not found in embedded db. Initialize it with value {}", key,
          value);

      result::result<void, DbErrorCode> store_result =
          StoreTypeIntoDb_(txn_id, key, &value);
      if (store_result.has_error()) {
        CRANE_ERROR("Failed to init key '{}' in db.", key);
        return false;
      }

      *buf = value;
      return true;
    } else {
      CRANE_ERROR("Failed to fetch key '{}' from db.", key);
      return false;
    }
  }

  result::result<size_t, DbErrorCode> FetchTypeFromDb_(txn_id_t txn_id,
                                                       std::string const& key,
                                                       std::string* buf) {
    size_t n_bytes{0};

    auto result = m_embedded_db_->Fetch(txn_id, key, nullptr, &n_bytes);
    if (result.has_error()) {
      CRANE_ERROR("Unexpected error when fetching the size of string key '{}'",
                  key);
      return result;
    }

    buf->resize(n_bytes);
    result = m_embedded_db_->Fetch(txn_id, key, buf->data(), &n_bytes);
    if (result.has_error()) {
      CRANE_ERROR("Unexpected error when fetching the data of string key '{}'",
                  key);
      return result;
    }

    return {n_bytes};
  }

  result::result<size_t, DbErrorCode> FetchTypeFromDb_(
      txn_id_t txn_id, std::string const& key,
      google::protobuf::MessageLite* value) {
    size_t n_bytes{0};
    std::string buf;

    auto result = m_embedded_db_->Fetch(txn_id, key, nullptr, &n_bytes);
    if (result.has_error()) {
      CRANE_ERROR("Unexpected error when fetching the size of proto key '{}'",
                  key);
      return result;
    }

    buf.resize(n_bytes);
    result = m_embedded_db_->Fetch(txn_id, key, buf.data(), &n_bytes);
    if (result.has_error()) {
      CRANE_ERROR("Unexpected error when fetching the data of proto key '{}'",
                  key);
      return result;
    }

    bool ok = value->ParseFromArray(buf.data(), n_bytes);
    if (!ok) {
      CRANE_ERROR("Failed to parse protobuf data of key {}", key);
      return result::failure(DbErrorCode::kParsingError);
    }

    return {n_bytes};
  }

  template <std::integral T>
  result::result<size_t, DbErrorCode> FetchTypeFromDb_(txn_id_t txn_id,
                                                       std::string const& key,
                                                       T* buf) {
    size_t n_bytes{sizeof(T)};
    auto result = m_embedded_db_->Fetch(txn_id, key, buf, &n_bytes);
    if (result.has_error() && result.error() != DbErrorCode::kNotFound)
      CRANE_ERROR("Unexpected error when fetching scalar key '{}'.", key);
    return result;
  }

  template <typename T>
  result::result<void, DbErrorCode> StoreTypeIntoDb_(txn_id_t txn_id,
                                                     std::string const& key,
                                                     const T* value)
    requires std::derived_from<T, google::protobuf::MessageLite>
  {
    using google::protobuf::io::CodedOutputStream;
    using google::protobuf::io::StringOutputStream;

    std::string buf;
    StringOutputStream stringOutputStream(&buf);
    CodedOutputStream codedOutputStream(&stringOutputStream);

    size_t n_bytes{value->ByteSizeLong()};
    value->SerializeToCodedStream(&codedOutputStream);

    return m_embedded_db_->Store(txn_id, key, buf.data(), n_bytes);
  }

  template <std::integral T>
  result::result<void, DbErrorCode> StoreTypeIntoDb_(txn_id_t txn_id,
                                                     std::string const& key,
                                                     const T* value) {
    return m_embedded_db_->Store(txn_id, key, value, sizeof(T));
  }

  // -----------

  inline static std::string const s_next_task_db_id_str_{"NextTaskDbId"};
  inline static std::string const s_next_task_id_str_{"NextTaskId"};
  inline static std::string const s_marked_db_id_str_{"MarkedDbId"};

  inline static uint32_t s_next_task_id_;
  inline static db_id_t s_next_task_db_id_;
  inline static absl::Mutex s_task_id_and_db_id_mtx_;

  std::unordered_map<db_id_t, DbQueueNode> m_pending_queue_;
  std::unordered_map<db_id_t, DbQueueNode> m_running_queue_;
  std::unordered_map<db_id_t, DbQueueNode> m_ended_queue_;
  absl::Mutex m_queue_mtx_;

  std::unique_ptr<UnqliteDb> m_embedded_db_;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::EmbeddedDbClient> g_embedded_db_client;

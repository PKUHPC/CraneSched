#pragma once

#include <absl/synchronization/mutex.h>
#include <unqlite.h>

#include <string>
#include <thread>
#include <unordered_map>

#include "CtldPublicDefs.h"
#include "crane/PublicHeader.h"
#include "protos/Crane.pb.h"

namespace Ctld {

class EmbeddedDbClient {
 private:
  using db_id_t = task_db_id_t;
  using TaskInEmbeddedDb = crane::grpc::TaskInEmbeddedDb;

  inline static constexpr db_id_t s_pending_head_db_id_ =
      std::numeric_limits<db_id_t>::max() - 0;
  inline static constexpr db_id_t s_pending_tail_db_id_ =
      std::numeric_limits<db_id_t>::max() - 1;
  inline static constexpr db_id_t s_running_head_db_id_ =
      std::numeric_limits<db_id_t>::max() - 2;
  inline static constexpr db_id_t s_running_tail_db_id_ =
      std::numeric_limits<db_id_t>::max() - 3;

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

 public:
  EmbeddedDbClient() = default;
  ~EmbeddedDbClient();

  bool Init(std::string const& db_path);

  bool AppendTaskToPendingAndAdvanceTaskIds(
      const crane::grpc::TaskToCtld& task_to_ctld,
      crane::grpc::PersistedPartOfTaskInCtld* persisted_part);

  bool DeleteTaskByDbId(db_id_t db_id);

  bool MoveTaskFromPendingToRunning(db_id_t task_db_id);

  bool GetPendingQueueCopy(std::list<crane::grpc::TaskInEmbeddedDb>* list) {
    absl::MutexLock l(&m_queue_mtx_);
    return GetQueueCopyNoLock_(m_pending_queue_, list) == UNQLITE_OK;
  }

  bool GetRunningQueueCopy(std::list<crane::grpc::TaskInEmbeddedDb>* list) {
    absl::MutexLock l(&m_queue_mtx_);
    return GetQueueCopyNoLock_(m_running_queue_, list) == UNQLITE_OK;
  }

  bool GetMarkedDbId(db_id_t* db_id);

  bool SetMarkedDbId(db_id_t db_id);

  bool UnsetMarkedDbId();

  bool FetchTaskDataInDb(db_id_t db_id, TaskInEmbeddedDb* task_in_db);

 private:
  std::string GetInternalErrorStr_();

  inline static std::string GetDbQueueNodeDataName_(db_id_t db_id) {
    return fmt::format("{}Data", db_id);
  }

  inline static std::string GetDbQueueNodeNextName_(db_id_t db_id) {
    return fmt::format("{}Next", db_id);
  }

  inline static std::string GetDbQueueNodePrevName_(db_id_t db_id) {
    return fmt::format("{}Prev", db_id);
  }

  int BeginTransaction_();

  int Commit_();

  // Helper functions for the queue structure in the embedded db.

  static std::unordered_map<db_id_t, DbQueueNode>::iterator
  FindDbQueueNodeInRamQueueNoLock_(db_id_t db_id,
                                   std::unordered_map<db_id_t, DbQueueNode>& q);

  // -----------

  // Helper functions for the queue structure in the embedded db.

  int InsertBeforeDbQueueNodeNoLockAndTxn_(db_id_t db_id, db_id_t pos);

  int DeleteDbQueueNodeNoLockAndTxn_(db_id_t db_id);

  int ForEachInDbQueueNoLockAndTxn_(DbQueueDummyHead dummy_head,
                                    DbQueueDummyTail dummy_tail,
                                    ForEachInQueueFunc const& func);

  int GetQueueCopyNoLock_(std::unordered_map<db_id_t, DbQueueNode> const& q,
                          std::list<crane::grpc::TaskInEmbeddedDb>* list);

  // -------------------

  // Helper functions for basic embedded db operations

  inline int FetchTaskDataInDbAtomic_(db_id_t db_id,
                                      TaskInEmbeddedDb* task_in_db) {
    return FetchTypeFromDb_(GetDbQueueNodeDataName_(db_id), task_in_db);
  }

  int FetchTypeFromDb_(std::string const& key, std::string* buf) {
    int rc;
    unqlite_int64 n_bytes;
    while (true) {
      rc = unqlite_kv_fetch(m_db_, key.c_str(), key.size(), nullptr, &n_bytes);
      if (rc == UNQLITE_OK) break;
      if (rc == UNQLITE_BUSY) {
        std::this_thread::yield();
        continue;
      }

      CRANE_ERROR("Failed to get value size for key {}: {}", key,
                  GetInternalErrorStr_());
      return rc;
    }

    buf->resize(n_bytes);
    while (true) {
      rc = unqlite_kv_fetch(m_db_, key.c_str(), key.size(), buf->data(),
                            &n_bytes);
      if (rc == UNQLITE_OK) return rc;
      if (rc == UNQLITE_BUSY) {
        std::this_thread::yield();
        continue;
      }

      CRANE_ERROR("Failed to fetch value for key {}: {}", key,
                  GetInternalErrorStr_());
      return rc;
    }
  }

  int FetchTypeFromDb_(std::string const& key,
                       google::protobuf::MessageLite* value) {
    int rc;
    unqlite_int64 n_bytes;
    std::string buf;
    while (true) {
      rc = unqlite_kv_fetch(m_db_, key.c_str(), key.size(), nullptr, &n_bytes);
      if (rc == UNQLITE_OK) break;
      if (rc == UNQLITE_BUSY) {
        std::this_thread::yield();
        continue;
      }

      CRANE_ERROR("Failed to get value size for key {}: {}", key,
                  GetInternalErrorStr_());
      return rc;
    }

    buf.resize(n_bytes);
    while (true) {
      rc = unqlite_kv_fetch(m_db_, key.c_str(), key.size(), buf.data(),
                            &n_bytes);
      if (rc == UNQLITE_OK) {
        bool ok = value->ParseFromArray(buf.data(), n_bytes);
        if (!ok) {
          CRANE_ERROR("Failed to parse protobuf data of key {}", key);
          return UNQLITE_IOERR;
        }
        return UNQLITE_OK;
      }
      if (rc == UNQLITE_BUSY) {
        std::this_thread::yield();
        continue;
      }

      CRANE_ERROR("Failed to fetch value for key {}: {}", key,
                  GetInternalErrorStr_());
      return rc;
    }
  }

  template <typename T, std::enable_if_t<std::is_arithmetic_v<T>, bool> = true>
  int FetchTypeFromDb_(std::string const& key, T* buf) {
    int rc;
    unqlite_int64 n_bytes{sizeof(T)};
    while (true) {
      rc = unqlite_kv_fetch(m_db_, key.c_str(), key.size(), buf, &n_bytes);
      if (rc == UNQLITE_OK) {
        if (n_bytes != sizeof(T)) {
          CRANE_ERROR("Fetch {} ({} bytes) from db. However, {} was retrieved",
                      key, sizeof(T), n_bytes);
        }
        return rc;
      }

      if (rc == UNQLITE_BUSY) {
        std::this_thread::yield();
        continue;
      }

      return rc;
    }
  }

  template <typename T, std::enable_if_t<
                            std::is_base_of_v<google::protobuf::MessageLite, T>,
                            bool> = true>
  int StoreTypeIntoDb_(std::string const& key, const T* value) {
    using google::protobuf::io::CodedOutputStream;
    using google::protobuf::io::StringOutputStream;

    int rc;

    std::string buf;
    StringOutputStream stringOutputStream(&buf);
    CodedOutputStream codedOutputStream(&stringOutputStream);

    unqlite_int64 n_bytes{static_cast<unqlite_int64>(value->ByteSizeLong())};
    value->SerializeToCodedStream(&codedOutputStream);

    while (true) {
      rc =
          unqlite_kv_store(m_db_, key.c_str(), key.size(), buf.data(), n_bytes);
      if (rc == UNQLITE_OK) return rc;
      if (rc == UNQLITE_BUSY) {
        std::this_thread::yield();
        continue;
      }

      CRANE_ERROR("Failed to store protobuf for key {}: {}", key,
                  GetInternalErrorStr_());
      return rc;
    }
  }

  template <typename T, std::enable_if_t<std::is_arithmetic_v<T>, bool> = true>
  int StoreTypeIntoDb_(std::string const& key, const T* value) {
    int rc;
    while (true) {
      rc = unqlite_kv_store(m_db_, key.c_str(), key.size(), value, sizeof(T));
      if (rc == UNQLITE_OK) return rc;
      if (rc == UNQLITE_BUSY) {
        std::this_thread::yield();
        continue;
      }

      CRANE_ERROR("Failed to store {} ({}) into db: {}", key, *value,
                  GetInternalErrorStr_());
      if (rc != UNQLITE_NOTIMPLEMENTED) unqlite_rollback(m_db_);
      return rc;
    }
  }

  int DeleteKeyFromDbAtomic_(std::string const& key);

  // -----------

  inline static std::string const s_next_task_db_id_str_{"NextTaskDbId"};
  inline static std::string const s_next_task_id_str_{"NextTaskId"};
  inline static std::string const s_marked_db_id_str_{"MarkedDbId"};

  inline static uint32_t s_next_task_id_;
  inline static db_id_t s_next_task_db_id_;
  inline static absl::Mutex s_task_id_and_db_id_mtx_;

  DbQueueDummyHead m_pending_queue_head_{.db_id = s_pending_head_db_id_};
  DbQueueDummyTail m_pending_queue_tail_{.db_id = s_pending_tail_db_id_};

  DbQueueDummyHead m_running_queue_head_{.db_id = s_running_head_db_id_};
  DbQueueDummyTail m_running_queue_tail_{.db_id = s_running_tail_db_id_};

  std::unordered_map<db_id_t, DbQueueNode> m_pending_queue_;
  std::unordered_map<db_id_t, DbQueueNode> m_running_queue_;
  absl::Mutex m_queue_mtx_;

  task_db_id_t m_marked_db_id_{-1};
  absl::Mutex m_marked_db_id_mtx_;

  std::string m_db_path_;
  unqlite* m_db_{nullptr};
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::EmbeddedDbClient> g_embedded_db_client;

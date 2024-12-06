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

#pragma once

#include "CtldPublicDefs.h"
// Precompiled header comes first!

#ifdef CRANE_HAVE_BERKELEY_DB
#  include <db_cxx.h>
#endif

#ifdef CRANE_HAVE_UNQLITE
#  include <unqlite.h>
#endif

#include "protos/Crane.pb.h"

namespace Ctld {

using txn_id_t = uint32_t;

enum DbErrorCode {
  kNotFound,
  kBufferSmall,
  kParsingError,
  kOther,
};

class IEmbeddedDb {
 public:
  virtual ~IEmbeddedDb() = default;

  virtual std::expected<void, DbErrorCode> Init(std::string const& path) = 0;

  virtual std::expected<void, DbErrorCode> Close() = 0;

  virtual std::expected<void, DbErrorCode> Store(txn_id_t txn_id,
                                                 std::string const& key,
                                                 const void* data,
                                                 size_t len) = 0;

  virtual std::expected<size_t, DbErrorCode> Fetch(txn_id_t txn_id,
                                                   std::string const& key,
                                                   void* buf, size_t* len) = 0;

  virtual std::expected<void, DbErrorCode> Delete(txn_id_t txn_id,
                                                  std::string const& key) = 0;

  virtual std::expected<txn_id_t, DbErrorCode> Begin() = 0;

  virtual std::expected<void, DbErrorCode> Commit(txn_id_t txn_id) = 0;

  virtual std::expected<void, DbErrorCode> Abort(txn_id_t txn_id) = 0;

  using KvIterFunc =
      std::function<bool(std::string&& key, std::vector<uint8_t>&& value)>;

  /// @param func if the return value of func is true, continue to next KV.
  ///             Otherwise, continue to next KV and delete current KV.
  virtual std::expected<void, DbErrorCode> IterateAllKv(KvIterFunc func) = 0;

  virtual std::string const& DbPath() = 0;
};

#ifdef CRANE_HAVE_UNQLITE

class UnqliteDb : public IEmbeddedDb {
 public:
  std::expected<void, DbErrorCode> Init(const std::string& path) override;

  std::expected<void, DbErrorCode> Close() override;

  std::expected<void, DbErrorCode> Store(txn_id_t txn_id,
                                         const std::string& key,
                                         const void* data, size_t len) override;

  std::expected<size_t, DbErrorCode> Fetch(txn_id_t txn_id,
                                           const std::string& key, void* buf,
                                           size_t* len) override;

  std::expected<void, DbErrorCode> Delete(txn_id_t txn_id,
                                          const std::string& key) override;

  std::expected<txn_id_t, DbErrorCode> Begin() override;

  std::expected<void, DbErrorCode> Commit(txn_id_t txn_id) override;

  std::expected<void, DbErrorCode> Abort(txn_id_t txn_id) override;

  std::expected<void, DbErrorCode> IterateAllKv(KvIterFunc func) override;

  const std::string& DbPath() override { return m_db_path_; };

 private:
  std::string GetInternalErrorStr_();

  static constexpr txn_id_t s_fixed_txn_id_ = 1;

  std::string m_db_path_;
  unqlite* m_db_{nullptr};
};

#endif

#ifdef CRANE_HAVE_BERKELEY_DB

class BerkeleyDb : public IEmbeddedDb {
 public:
  std::expected<void, DbErrorCode> Init(const std::string& path) override;

  std::expected<void, DbErrorCode> Close() override;

  std::expected<void, DbErrorCode> Store(txn_id_t txn_id,
                                         const std::string& key,
                                         const void* data, size_t len) override;

  std::expected<size_t, DbErrorCode> Fetch(txn_id_t txn_id,
                                           const std::string& key, void* buf,
                                           size_t* len) override;

  std::expected<void, DbErrorCode> Delete(txn_id_t txn_id,
                                          const std::string& key) override;

  std::expected<txn_id_t, DbErrorCode> Begin() override;

  std::expected<void, DbErrorCode> Commit(txn_id_t txn_id) override;

  std::expected<void, DbErrorCode> Abort(txn_id_t txn_id) override;

  std::expected<void, DbErrorCode> IterateAllKv(KvIterFunc func) override;

  const std::string& DbPath() override { return m_db_path_; };

 private:
  DbTxn* GetDbTxnFromId_(txn_id_t txn_id);

  std::string m_db_path_, m_env_home_;

  std::unique_ptr<Db> m_db_;

  std::unique_ptr<DbEnv> m_env_;

  std::unordered_map<txn_id_t, DbTxn*> m_txn_map_;
};

#endif

class EmbeddedDbClient {
 private:
  using db_id_t = task_db_id_t;
  using TaskInEmbeddedDb = crane::grpc::TaskInEmbeddedDb;

 public:
  struct DbSnapshot {
    std::unordered_map<db_id_t, TaskInEmbeddedDb> pending_queue;
    std::unordered_map<db_id_t, TaskInEmbeddedDb> running_queue;
    std::unordered_map<db_id_t, TaskInEmbeddedDb> final_queue;
  };

  EmbeddedDbClient() = default;
  ~EmbeddedDbClient();

  bool Init(std::string const& db_path);

  bool RetrieveLastSnapshot(DbSnapshot* snapshot);

  bool BeginVariableDbTransaction(txn_id_t* txn_id) {
    return BeginDbTransaction_(m_variable_db_.get(), txn_id);
  }

  bool CommitVariableDbTransaction(txn_id_t txn_id) {
    return CommitDbTransaction_(m_variable_db_.get(), txn_id);
  }

  bool BeginFixedDbTransaction(txn_id_t* txn_id) {
    return BeginDbTransaction_(m_fixed_db_.get(), txn_id);
  }

  bool CommitFixedDbTransaction(txn_id_t txn_id) {
    return CommitDbTransaction_(m_fixed_db_.get(), txn_id);
  }

  // Note: All operations in transaction will abort or rollback automatically if
  // some operation fails, so we don't need anything like AbortTransaction here!

  bool AppendTasksToPendingAndAdvanceTaskIds(
      const std::vector<TaskInCtld*>& tasks);

  bool PurgeEndedTasks(const std::vector<db_id_t>& db_ids);

  bool UpdateRuntimeAttrOfTask(
      txn_id_t txn_id, db_id_t db_id,
      crane::grpc::RuntimeAttrOfTask const& runtime_attr) {
    return StoreTypeIntoDb_(m_variable_db_.get(), txn_id,
                            GetVariableDbEntryName_(db_id), &runtime_attr)
        .has_value();
  }

  bool UpdateTaskToCtld(txn_id_t txn_id, db_id_t db_id,
                        crane::grpc::TaskToCtld const& task_to_ctld) {
    return StoreTypeIntoDb_(m_fixed_db_.get(), txn_id,
                            GetFixedDbEntryName_(db_id), &task_to_ctld)
        .has_value();
  }

  bool UpdateRuntimeAttrOfTaskIfExists(
      txn_id_t txn_id, db_id_t db_id,
      crane::grpc::RuntimeAttrOfTask const& runtime_attr) {
    return StoreTypeIntoDbIfExists_(m_variable_db_.get(), txn_id,
                                    GetVariableDbEntryName_(db_id),
                                    &runtime_attr)
        .has_value();
  }

  bool UpdateTaskToCtldIfExists(txn_id_t txn_id, db_id_t db_id,
                                crane::grpc::TaskToCtld const& task_to_ctld) {
    return StoreTypeIntoDbIfExists_(m_fixed_db_.get(), txn_id,
                                    GetFixedDbEntryName_(db_id), &task_to_ctld)
        .has_value();
  }

  bool FetchTaskDataInDb(txn_id_t txn_id, db_id_t db_id,
                         TaskInEmbeddedDb* task_in_db) {  // Only used in test
    return FetchTaskDataInDbAtomic_(txn_id, db_id, task_in_db).has_value();
  }

 private:
  inline static std::string GetFixedDbEntryName_(db_id_t db_id) {
    return fmt::format("{}T", db_id);
  }

  inline static std::string GetVariableDbEntryName_(db_id_t db_id) {
    return fmt::format("{}S", db_id);
  }

  inline static bool IsVariableDbTaskDataEntry_(std::string const& key) {
    return key.back() == 'S';
  }

  inline static task_db_id_t ExtractDbIdFromEntry_(std::string const& key) {
    return std::stol(key.substr(0, key.size() - 1));
  }

  bool BeginDbTransaction_(IEmbeddedDb* db, txn_id_t* txn_id) {
    auto result = db->Begin();
    if (result.has_value()) {
      *txn_id = result.value();
      return true;
    }

    CRANE_ERROR("Failed to begin a transaction.");
    return false;
  }

  bool CommitDbTransaction_(IEmbeddedDb* db, txn_id_t txn_id) {
    if (txn_id <= 0) {
      CRANE_ERROR("Commit a transaction with id {} <= 0", txn_id);
      return false;
    }

    return db->Commit(txn_id).has_value();
  }

  // -------------------

  // Helper functions for basic embedded db operations

  inline std::expected<size_t, DbErrorCode> FetchTaskDataInDbAtomic_(
      txn_id_t txn_id, db_id_t db_id, TaskInEmbeddedDb* task_in_db) {
    auto result =
        FetchTypeFromDb_(m_fixed_db_.get(), txn_id, GetFixedDbEntryName_(db_id),
                         task_in_db->mutable_task_to_ctld());
    if (!result) return result;

    return FetchTypeFromDb_(m_variable_db_.get(), txn_id,
                            GetVariableDbEntryName_(db_id),
                            task_in_db->mutable_runtime_attr());
  }

  template <std::integral T>
  bool FetchTypeFromVarDbOrInitWithValueNoLockAndTxn_(txn_id_t txn_id,
                                                      std::string const& key,
                                                      T* buf, T value) {
    std::expected<size_t, DbErrorCode> fetch_result =
        FetchTypeFromDb_(m_variable_db_.get(), txn_id, key, buf);
    if (fetch_result.has_value()) return true;

    if (fetch_result.error() == DbErrorCode::kNotFound) {
      CRANE_TRACE(
          "Key {} not found in embedded db. Initialize it with value {}", key,
          value);

      std::expected store_result =
          StoreTypeIntoDb_(m_variable_db_.get(), txn_id, key, &value);
      if (!store_result) {
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

  std::expected<size_t, DbErrorCode> FetchTypeFromDb_(
      txn_id_t txn_id, const std::shared_ptr<IEmbeddedDb>& db,
      std::string const& key, std::string* buf) {
    size_t n_bytes{0};

    auto result = db->Fetch(txn_id, key, nullptr, &n_bytes);
    if (!result) {
      CRANE_ERROR("Unexpected error when fetching the size of string key '{}'",
                  key);
      return result;
    }

    buf->resize(n_bytes);
    result = db->Fetch(txn_id, key, buf->data(), &n_bytes);
    if (!result) {
      CRANE_ERROR("Unexpected error when fetching the data of string key '{}'",
                  key);
      return result;
    }

    return {n_bytes};
  }

  std::expected<size_t, DbErrorCode> FetchTypeFromDb_(
      IEmbeddedDb* db, txn_id_t txn_id, std::string const& key,
      google::protobuf::MessageLite* value) {
    size_t n_bytes{0};
    std::string buf;

    auto result = db->Fetch(txn_id, key, nullptr, &n_bytes);
    if (!result && result.error() != kBufferSmall) {
      CRANE_ERROR("Unexpected error when fetching the size of proto key '{}'",
                  key);
      return result;
    }

    buf.resize(n_bytes);
    result = db->Fetch(txn_id, key, buf.data(), &n_bytes);
    if (!result) {
      CRANE_ERROR("Unexpected error when fetching the data of proto key '{}'",
                  key);
      return result;
    }

    bool ok = value->ParseFromArray(buf.data(), n_bytes);
    if (!ok) {
      CRANE_ERROR("Failed to parse protobuf data of key {}", key);
      return std::unexpected(DbErrorCode::kParsingError);
    }

    return {n_bytes};
  }

  template <std::integral T>
  std::expected<size_t, DbErrorCode> FetchTypeFromDb_(IEmbeddedDb* db,
                                                      txn_id_t txn_id,
                                                      std::string const& key,
                                                      T* buf) {
    size_t n_bytes{sizeof(T)};
    auto result = db->Fetch(txn_id, key, buf, &n_bytes);
    if (!result && result.error() != DbErrorCode::kNotFound)
      CRANE_ERROR("Unexpected error when fetching scalar key '{}'.", key);
    return result;
  }

  template <typename T>
  std::expected<void, DbErrorCode> StoreTypeIntoDb_(IEmbeddedDb* db,
                                                    txn_id_t txn_id,
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

    return db->Store(txn_id, key, buf.data(), n_bytes);
  }

  template <std::integral T>
  std::expected<void, DbErrorCode> StoreTypeIntoDb_(IEmbeddedDb* db,
                                                    txn_id_t txn_id,
                                                    std::string const& key,
                                                    const T* value) {
    return db->Store(txn_id, key, value, sizeof(T));
  }

  template <typename T>
  std::expected<void, DbErrorCode> StoreTypeIntoDbIfExists_(
      IEmbeddedDb* db, txn_id_t txn_id, const std::string& key, const T* value)
    requires std::derived_from<T, google::protobuf::MessageLite>
  {
    using google::protobuf::io::CodedOutputStream;
    using google::protobuf::io::StringOutputStream;

    std::string buf;
    StringOutputStream stringOutputStream(&buf);
    CodedOutputStream codedOutputStream(&stringOutputStream);

    size_t n_bytes{value->ByteSizeLong()};
    value->SerializeToCodedStream(&codedOutputStream);

    if (!BeginDbTransaction_(db, &txn_id))
      return std::unexpected(DbErrorCode::kOther);

    size_t len = 0;
    auto fetch_result = db->Fetch(txn_id, key, nullptr, &len);
    if (!fetch_result) {
      if (fetch_result.error() == DbErrorCode::kNotFound) {
        CommitDbTransaction_(db, txn_id);
        return {};
      }
      return std::unexpected(fetch_result.error());
    }

    auto store_result = StoreTypeIntoDb_(db, txn_id, key, value);
    if (!store_result) return std::unexpected(store_result.error());

    CommitDbTransaction_(db, txn_id);
    return {};
  }

  template <std::integral T>
  std::expected<void, DbErrorCode> StoreTypeIntoDbIfExists_(
      IEmbeddedDb* db, txn_id_t txn_id, const std::string& key,
      const T* value) {
    if (!BeginDbTransaction_(db, &txn_id))
      return std::unexpected(DbErrorCode::kOther);

    size_t len = 0;
    auto fetch_result = db->Fetch(txn_id, key, nullptr, &len);
    if (!fetch_result) {
      if (fetch_result.error() == DbErrorCode::kNotFound) {
        CommitDbTransaction_(db, txn_id);
        return {};
      }
      return std::unexpected(fetch_result.error());
    }

    auto store_result = StoreTypeIntoDb_(db, txn_id, key, value);
    if (!store_result) return std::unexpected(store_result.error());

    CommitDbTransaction_(db, txn_id);
    return {};
  }

  // -----------

  inline static std::string const s_next_task_db_id_str_{"NDI"};
  inline static std::string const s_next_task_id_str_{"NI"};

  inline static task_id_t s_next_task_id_;
  inline static db_id_t s_next_task_db_id_;
  inline static absl::Mutex s_task_id_and_db_id_mtx_;

  std::unique_ptr<IEmbeddedDb> m_variable_db_;
  std::unique_ptr<IEmbeddedDb> m_fixed_db_;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::EmbeddedDbClient> g_embedded_db_client;

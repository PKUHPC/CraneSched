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

#ifdef CRANE_WITH_RAFT

std::expected<void, DbErrorCode> NuRaftMemoryDb::Store(txn_id_t txn_id,
                                                       const std::string& key,
                                                       const void* data,
                                                       size_t len) {
  CraneStateMachine::CraneCtldOpType type;
  if (db_index_ == 0) {
    type = CraneStateMachine::OP_VAR_STORE;
  } else if (db_index_ == 1) {
    type = CraneStateMachine::OP_FIX_STORE;
  } else {
    return std::unexpected(DbErrorCode::kOther);
  }

  g_raft_server->AppendLog(CraneStateMachine::enc_log(type, key, data, len));
  return {};
}

std::expected<size_t, DbErrorCode> NuRaftMemoryDb::Fetch(txn_id_t txn_id,
                                                         const std::string& key,
                                                         void* buf,
                                                         size_t* len) {
  auto* value_map =
      g_raft_server->GetStateMachine()->GetValueMapInstance(db_index_);

  if (!value_map->contains(key)) return std::unexpected(DbErrorCode::kNotFound);

  if (*len == 0) {
    *len = value_map->at(key).size();
    return {0};
  } else if (*len < value_map->at(key).size()) {
    *len = value_map->at(key).size();
    return std::unexpected(DbErrorCode::kBufferSmall);
  }

  memcpy(buf, value_map->at(key).data(), value_map->at(key).size());
  return {value_map->at(key).size()};
}

std::expected<void, DbErrorCode> NuRaftMemoryDb::Delete(
    txn_id_t txn_id, const std::string& key) {
  CraneStateMachine::CraneCtldOpType type;
  if (db_index_ == 0) {
    type = CraneStateMachine::OP_VAR_DELETE;
  } else if (db_index_ == 1) {
    type = CraneStateMachine::OP_FIX_DELETE;
  } else {
    return std::unexpected(DbErrorCode::kOther);
  }
  g_raft_server->AppendLog(CraneStateMachine::enc_log(type, key));
  return {};
}

std::expected<void, DbErrorCode> NuRaftMemoryDb::IterateAllKv(
    IEmbeddedDb::KvIterFunc func) {
  auto value_map =
      g_raft_server->GetStateMachine()->GetValueMapInstance(db_index_);
  for (auto [k, v] : *value_map) {
    std::string key = k;
    if (!func(std::move(key), std::move(v))) {
      value_map->erase(k);
    }
  }
  return {};
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
}

bool EmbeddedDbClient::Init(const std::string& db_path) {
#ifdef CRANE_WITH_RAFT
  m_variable_db_ = std::make_unique<NuRaftMemoryDb>(0);
  m_fixed_db_ = std::make_unique<NuRaftMemoryDb>(1);
#else

  if (g_config.CraneEmbeddedDbBackend == "Unqlite") {
    # ifdef CRANE_HAVE_UNQLITE
    m_variable_db_ = std::make_unique<UnqliteDb>();
    m_fixed_db_ = std::make_unique<UnqliteDb>();
    m_resv_db_ = std::make_unique<UnqliteDb>();
# else
    CRANE_ERROR(
        "Select unqlite as the embedded db but it's not been compiled.");
    return false;
# endif

  } else if (g_config.CraneEmbeddedDbBackend == "BerkeleyDB") {
# ifdef CRANE_HAVE_BERKELEY_DB
    m_variable_db_ = std::make_unique<BerkeleyDb>();
    m_fixed_db_ = std::make_unique<BerkeleyDb>();
    m_resv_db_ = std::make_unique<BerkeleyDb>();
# else
    CRANE_ERROR(
        "Select Berkeley DB as the embedded db but it's not been compiled.");
    return false;
# endif

  } else {
    CRANE_ERROR("Invalid embedded database backend: {}",
                g_config.CraneEmbeddedDbBackend);
    return false;
  }

  #endif

  auto result = m_variable_db_->Init(db_path + "var");
  if (!result) return false;
  result = m_fixed_db_->Init(db_path + "fix");
  if (!result) return false;
  result = m_resv_db_->Init(db_path + "resv");
  if (!result) return false;

  bool ok;

  // There is no race during Init stage.
  // No lock is needed.
  ok = FetchTypeFromVarDbOrInitWithValueNoLockAndTxn_(0, s_next_task_id_str_,
                                                      &s_next_task_id_, 1u);
  if (!ok) return false;

  ok = FetchTypeFromVarDbOrInitWithValueNoLockAndTxn_(0, s_next_task_db_id_str_,
                                                      &s_next_task_db_id_, 1L);
  if (!ok) return false;

  return RestoreTaskID();
}

bool EmbeddedDbClient::RestoreTaskID() {
  bool ok;
  // Note: Normally, there is no race during the Init phase and leader switching
  // phase, as there will be no data access until this function is completed.
  // However, it costs little and prevents race condition, so we just leave it
  // here.
  absl::MutexLock lock_ids(&s_task_id_and_db_id_mtx_);
  ok = FetchTypeFromVarDbOrInitWithValueNoLockAndTxn_(0, s_next_task_id_str_,
                                                      &s_next_task_id_, 1u);
  if (!ok) return false;

  ok = FetchTypeFromVarDbOrInitWithValueNoLockAndTxn_(0, s_next_task_db_id_str_,
                                                      &s_next_task_db_id_, 1L);
  if (!ok) return false;

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
    CRANE_ERROR("Failed to restore variable data into queues!");
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

  if (!BeginDbTransaction(m_fixed_db_.get(), &txn_id)) return false;

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

  if (!CommitDbTransaction(m_fixed_db_.get(), txn_id)) return false;

  if (!BeginDbTransaction(m_variable_db_.get(), &txn_id)) return false;

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
  if (!CommitDbTransaction(m_variable_db_.get(), txn_id)) return false;

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
  std::expected<void, DbErrorCode> res;

  if (!BeginDbTransaction(m_variable_db_.get(), &txn_id)) return false;
  for (const auto& id : db_ids) {
    res = m_variable_db_->Delete(txn_id, GetVariableDbEntryName_(id));
    if (!res) {
      CRANE_ERROR(
          "Failed to delete embedded variable data entry. Error code: {}",
          int(res.error()));
      return false;
    }
  }
  if (!CommitDbTransaction(m_variable_db_.get(), txn_id)) return false;

  if (!BeginDbTransaction(m_fixed_db_.get(), &txn_id)) return false;
  for (const auto& id : db_ids) {
    res = m_fixed_db_->Delete(txn_id, GetFixedDbEntryName_(id));
    if (!res) {
      CRANE_ERROR("Failed to delete embedded fixed data entry. Error code: {}",
                  int(res.error()));
      return false;
    }
  }
  if (!CommitDbTransaction(m_fixed_db_.get(), txn_id)) return false;

  return true;
}

}  // namespace Ctld

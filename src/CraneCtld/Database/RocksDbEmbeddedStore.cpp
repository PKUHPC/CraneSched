/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

#include "RocksDbEmbeddedStore.h"

#include <rocksdb/cache.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/slice_transform.h>

#include <unordered_set>

namespace Ctld {

namespace {

constexpr std::array<std::string_view, 6> kCfNames{
    "job_fixed", "job_var", "step_fixed", "step_var", "reservation", "meta"};

constexpr std::string_view kJobFixedPrefix = "job_fixed/";
constexpr std::string_view kJobVarPrefix = "job_var/";
constexpr std::string_view kStepFixedPrefix = "step_fixed/";
constexpr std::string_view kStepVarPrefix = "step_var/";
constexpr std::string_view kNextStepIdPrefix = "meta/next_step_id/";

constexpr std::string_view kNextJobIdKey = "meta/next_job_id";
constexpr std::string_view kNextJobDbIdKey = "meta/next_job_db_id";
constexpr std::string_view kNextStepDbIdKey = "meta/next_step_db_id";

constexpr uint64_t MiB(uint32_t value) { return uint64_t{value} * 1024 * 1024; }

}  // namespace

RocksDbEmbeddedStore::~RocksDbEmbeddedStore() { Close(); }

bool RocksDbEmbeddedStore::Init(std::string const& db_path) {
  db_path_ = db_path + ".rocksdb";
  std::error_code ec;
  std::filesystem::create_directories(db_path_, ec);
  if (ec) {
    CRANE_ERROR("Failed to create RocksDB directory {}: {}", db_path_.string(),
                ec.message());
    return false;
  }

  rocksdb::Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  options.IncreaseParallelism(g_config.RocksDb.MaxBackgroundJobs);
  options.write_buffer_size = MiB(g_config.RocksDb.WriteBufferSizeMB);
  options.max_write_buffer_number = g_config.RocksDb.MaxWriteBufferNumber;
  options.target_file_size_base = MiB(g_config.RocksDb.TargetFileSizeBaseMB);
  options.max_background_jobs = g_config.RocksDb.MaxBackgroundJobs;
  options.compression = ParseCompression_(g_config.RocksDb.Compression);

  std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs;
  auto add_cf = [&](std::string const& name) {
    cf_descs.emplace_back(name, rocksdb::ColumnFamilyOptions(options));
  };

  add_cf(rocksdb::kDefaultColumnFamilyName);
  for (auto name : kCfNames) add_cf(std::string{name});

  std::vector<rocksdb::ColumnFamilyHandle*> opened_handles;
  std::unique_ptr<rocksdb::DB> db;
  auto status = rocksdb::DB::Open(options, db_path_.string(), cf_descs,
                                  &opened_handles, &db);
  if (!status.ok()) {
    CRANE_ERROR("Failed to open RocksDB at {}: {}", db_path_.string(),
                ToString_(status));
    return false;
  }
  db_ = std::move(db);

  default_cf_handle_ = opened_handles[0];
  for (size_t i = 0; i < kCfNames.size(); ++i) {
    cf_handles_[i] = opened_handles[i + 1];
  }

  if (!InitMeta_()) return false;
  if (!LoadNextStepIdCache_()) return false;

  if (!g_config.RocksDb.SyncWrites &&
      g_config.RocksDb.ManualWalSyncIntervalMs > 0) {
    manual_wal_thread_stop_ = false;
    manual_wal_thread_ = std::thread([this] {
      while (!manual_wal_thread_stop_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(
            g_config.RocksDb.ManualWalSyncIntervalMs));
        if (manual_wal_thread_stop_ || db_ == nullptr) break;
        auto status = db_->SyncWAL();
        if (!status.ok())
          CRANE_WARN("RocksDB SyncWAL failed: {}", ToString_(status));
      }
    });
  }

  CRANE_INFO(
      "Embedded DB backend RocksDB initialized at {}, sync_writes={}, "
      "manual_wal_sync_ms={}, compression={}",
      db_path_.string(), g_config.RocksDb.SyncWrites,
      g_config.RocksDb.ManualWalSyncIntervalMs, g_config.RocksDb.Compression);
  return true;
}

void RocksDbEmbeddedStore::Close() {
  manual_wal_thread_stop_ = true;
  if (manual_wal_thread_.joinable()) manual_wal_thread_.join();

  if (db_ != nullptr && !g_config.RocksDb.SyncWrites) {
    auto status = db_->SyncWAL();
    if (!status.ok()) CRANE_WARN("RocksDB final SyncWAL failed: {}", ToString_(status));
  }

  for (auto*& handle : cf_handles_) {
    if (handle != nullptr && db_ != nullptr) {
      auto status = db_->DestroyColumnFamilyHandle(handle);
      if (!status.ok())
        CRANE_WARN("Failed to destroy RocksDB CF handle: {}", ToString_(status));
      handle = nullptr;
    }
  }

  if (default_cf_handle_ != nullptr && db_ != nullptr) {
    auto status = db_->DestroyColumnFamilyHandle(default_cf_handle_);
    if (!status.ok())
      CRANE_WARN("Failed to destroy RocksDB default CF handle: {}",
                 ToString_(status));
    default_cf_handle_ = nullptr;
  }

  db_.reset();
}

bool RocksDbEmbeddedStore::ResetNextJobId(job_id_t next_job_id,
                                          db_id_t next_job_db_id) {
  absl::MutexLock lock_jobs(&job_id_mu_);
  rocksdb::WriteBatch batch;
  if (next_job_id > 0 &&
      !PutScalar_(&batch, Cf::Meta, MetaKey_(kNextJobIdKey), next_job_id))
    return false;
  if (next_job_db_id > 0 &&
      !PutScalar_(&batch, Cf::Meta, MetaKey_(kNextJobDbIdKey),
                  next_job_db_id))
    return false;

  if (next_job_id > 0) {
    absl::MutexLock lock_steps(&step_id_mu_);
    db_id_t next_step_db_id = 1;
    if (!DeleteNextStepIdKeys_(&batch)) return false;
    if (!PutScalar_(&batch, Cf::Meta, MetaKey_(kNextStepDbIdKey),
                    next_step_db_id))
      return false;
  }

  if (!WriteBatch_(&batch, "rocksdb_reset_next_job_id")) return false;

  if (next_job_id > 0) next_job_id_ = next_job_id;
  if (next_job_db_id > 0) next_job_db_id_ = next_job_db_id;
  if (next_job_id > 0) {
    absl::MutexLock lock_steps(&step_id_mu_);
    next_step_id_map_.clear();
    next_step_db_id_ = 1;
  }
  return true;
}

bool RocksDbEmbeddedStore::ResetNextStepDbId() {
  absl::MutexLock lock_steps(&step_id_mu_);
  rocksdb::WriteBatch batch;
  db_id_t next_step_db_id = 1;
  if (!DeleteNextStepIdKeys_(&batch)) return false;
  if (!PutScalar_(&batch, Cf::Meta, MetaKey_(kNextStepDbIdKey),
                  next_step_db_id))
    return false;
  if (!WriteBatch_(&batch, "rocksdb_reset_next_step_db_id")) return false;
  next_step_id_map_.clear();
  next_step_db_id_ = next_step_db_id;
  return true;
}

bool RocksDbEmbeddedStore::PurgeAllJobHistory() {
  rocksdb::WriteBatch batch;
  auto delete_all = [&](Cf cf) -> bool {
    std::unique_ptr<rocksdb::Iterator> it(
        db_->NewIterator(ReadOptions_(), Handle_(cf)));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      batch.Delete(Handle_(cf), it->key());
    }
    if (!it->status().ok()) {
      CRANE_ERROR("RocksDB iterator failed during purge all: {}",
                  ToString_(it->status()));
      return false;
    }
    return true;
  };

  if (!delete_all(Cf::JobFixed) || !delete_all(Cf::JobVar) ||
      !delete_all(Cf::StepFixed) || !delete_all(Cf::StepVar))
    return false;

  {
    absl::MutexLock lock_steps(&step_id_mu_);
    if (!DeleteNextStepIdKeys_(&batch)) return false;
  }

  if (!WriteBatch_(&batch, "rocksdb_purge_all_job_history")) return false;
  {
    absl::MutexLock lock_steps(&step_id_mu_);
    next_step_id_map_.clear();
  }
  return true;
}

bool RocksDbEmbeddedStore::RetrieveLastSnapshot(DbSnapshot* snapshot) {
  using JobStatus = crane::grpc::JobStatus;
  using RuntimeAttr = crane::grpc::RuntimeAttrOfJob;

  std::unordered_map<db_id_t, RuntimeAttr> runtime_attrs;
  {
    std::unique_ptr<rocksdb::Iterator> it(
        db_->NewIterator(ReadOptions_(), Handle_(Cf::JobVar)));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      db_id_t id;
      if (!ParseDbId_(it->key().ToString(), kJobVarPrefix, &id)) continue;
      RuntimeAttr attr;
      if (!ParseProto_(it->value().ToString(), &attr, it->key().ToString()))
        return false;
      runtime_attrs.emplace(id, std::move(attr));
    }
    if (!it->status().ok()) {
      CRANE_ERROR("Failed to scan RocksDB job_var: {}", ToString_(it->status()));
      return false;
    }
  }

  rocksdb::WriteBatch cleanup_batch;
  {
    std::unique_ptr<rocksdb::Iterator> it(
        db_->NewIterator(ReadOptions_(), Handle_(Cf::JobFixed)));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      db_id_t id;
      if (!ParseDbId_(it->key().ToString(), kJobFixedPrefix, &id)) continue;
      auto attr_it = runtime_attrs.find(id);
      if (attr_it == runtime_attrs.end()) {
        cleanup_batch.Delete(Handle_(Cf::JobFixed), it->key());
        continue;
      }

      JobInEmbeddedDb job_data;
      *job_data.mutable_runtime_attr() = std::move(attr_it->second);
      if (!ParseProto_(it->value().ToString(),
                       job_data.mutable_job_to_ctld(), it->key().ToString()))
        return false;

      JobStatus status = job_data.runtime_attr().status();
      switch (status) {
      case crane::grpc::Pending:
        snapshot->pending_queue.emplace(id, std::move(job_data));
        break;
      case crane::grpc::Running:
      case crane::grpc::Suspended:
      case crane::grpc::Starting:
      case crane::grpc::Configuring:
      case crane::grpc::Completing:
        snapshot->running_queue.emplace(id, std::move(job_data));
        break;
      default:
        snapshot->final_queue.emplace(id, std::move(job_data));
        break;
      }
    }
    if (!it->status().ok()) {
      CRANE_ERROR("Failed to scan RocksDB job_fixed: {}",
                  ToString_(it->status()));
      return false;
    }
  }

  if (cleanup_batch.Count() > 0 &&
      !WriteBatch_(&cleanup_batch, "rocksdb_retrieve_job_cleanup"))
    return false;
  return true;
}

bool RocksDbEmbeddedStore::RetrieveStepInfo(StepDbSnapshot* snapshot) {
  using RuntimeAttr = crane::grpc::RuntimeAttrOfStep;

  std::unordered_map<db_id_t, RuntimeAttr> runtime_attrs;
  std::unordered_map<job_id_t, uint32_t> max_step_id_by_job;
  {
    std::unique_ptr<rocksdb::Iterator> it(
        db_->NewIterator(ReadOptions_(), Handle_(Cf::StepVar)));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      db_id_t id;
      if (!ParseDbId_(it->key().ToString(), kStepVarPrefix, &id)) continue;
      RuntimeAttr attr;
      if (!ParseProto_(it->value().ToString(), &attr, it->key().ToString()))
        return false;
      runtime_attrs.emplace(id, std::move(attr));
    }
    if (!it->status().ok()) {
      CRANE_ERROR("Failed to scan RocksDB step_var: {}",
                  ToString_(it->status()));
      return false;
    }
  }

  rocksdb::WriteBatch cleanup_batch;
  {
    std::unique_ptr<rocksdb::Iterator> it(
        db_->NewIterator(ReadOptions_(), Handle_(Cf::StepFixed)));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      db_id_t id;
      if (!ParseDbId_(it->key().ToString(), kStepFixedPrefix, &id)) continue;
      auto attr_it = runtime_attrs.find(id);
      if (attr_it == runtime_attrs.end()) {
        cleanup_batch.Delete(Handle_(Cf::StepFixed), it->key());
        continue;
      }

      StepInEmbeddedDb step;
      *step.mutable_runtime_attr() = std::move(attr_it->second);
      if (!ParseProto_(it->value().ToString(),
                       step.mutable_step_to_ctld(), it->key().ToString()))
        return false;

      auto job_id = step.step_to_ctld().job_id();
      max_step_id_by_job[job_id] =
          std::max<uint32_t>(max_step_id_by_job[job_id],
                             step.runtime_attr().step_id());
      snapshot->steps[job_id].push_back(std::move(step));
    }
    if (!it->status().ok()) {
      CRANE_ERROR("Failed to scan RocksDB step_fixed: {}",
                  ToString_(it->status()));
      return false;
    }
  }

  if (cleanup_batch.Count() > 0 &&
      !WriteBatch_(&cleanup_batch, "rocksdb_retrieve_step_cleanup"))
    return false;

  return RebuildMissingNextStepIds_(max_step_id_by_job);
}

bool RocksDbEmbeddedStore::RetrieveReservationInfo(
    std::unordered_map<ResvId, crane::grpc::CreateReservationRequest>*
        reservation_info_map) {
  std::unique_ptr<rocksdb::Iterator> it(
      db_->NewIterator(ReadOptions_(), Handle_(Cf::Reservation)));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    crane::grpc::CreateReservationRequest info;
    if (!ParseProto_(it->value().ToString(), &info, it->key().ToString()))
      return false;
    reservation_info_map->emplace(it->key().ToString(), std::move(info));
  }
  if (!it->status().ok()) {
    CRANE_ERROR("Failed to scan RocksDB reservation CF: {}",
                ToString_(it->status()));
    return false;
  }
  return true;
}

bool RocksDbEmbeddedStore::BeginTransaction(RocksStoreKind kind,
                                            rocks_txn_id_t* txn_id) {
  absl::MutexLock lock(&txn_mu_);
  auto id = next_txn_id_++;
  auto [it, inserted] = pending_batches_.try_emplace(id);
  it->second.kind = kind;
  *txn_id = id;
  return true;
}

bool RocksDbEmbeddedStore::CommitTransaction(rocks_txn_id_t txn_id) {
  PendingBatch batch;
  {
    absl::MutexLock lock(&txn_mu_);
    auto it = pending_batches_.find(txn_id);
    if (it == pending_batches_.end()) {
      CRANE_ERROR("RocksDB commit references unknown txn id {}", txn_id);
      return false;
    }
    batch = std::move(it->second);
    pending_batches_.erase(it);
  }
  if (batch.batch.Count() == 0) return true;
  return WriteBatch_(&batch.batch, "rocksdb_txn_commit");
}

bool RocksDbEmbeddedStore::UpdateRuntimeAttrOfJob(
    rocks_txn_id_t txn_id, db_id_t db_id,
    crane::grpc::RuntimeAttrOfJob const& attr) {
  return StoreProto_(txn_id, RocksStoreKind::JobVar, JobVarKey(db_id), attr);
}

bool RocksDbEmbeddedStore::UpdateJobToCtld(
    rocks_txn_id_t txn_id, db_id_t db_id,
    crane::grpc::JobToCtld const& job_to_ctld) {
  return StoreProto_(txn_id, RocksStoreKind::JobFixed, JobFixedKey(db_id),
                     job_to_ctld);
}

bool RocksDbEmbeddedStore::UpdateRuntimeAttrOfJobIfExists(
    rocks_txn_id_t txn_id, db_id_t db_id,
    crane::grpc::RuntimeAttrOfJob const& attr) {
  return StoreProtoIfExists_(txn_id, RocksStoreKind::JobVar, JobVarKey(db_id),
                             attr);
}

bool RocksDbEmbeddedStore::UpdateJobToCtldIfExists(
    rocks_txn_id_t txn_id, db_id_t db_id,
    crane::grpc::JobToCtld const& job_to_ctld) {
  return StoreProtoIfExists_(txn_id, RocksStoreKind::JobFixed,
                             JobFixedKey(db_id), job_to_ctld);
}

bool RocksDbEmbeddedStore::FetchJobDataInDb(rocks_txn_id_t txn_id,
                                            db_id_t db_id,
                                            JobInEmbeddedDb* job_in_db) {
  return GetProto_(Cf::JobFixed, JobFixedKey(db_id),
                   job_in_db->mutable_job_to_ctld()) &&
         GetProto_(Cf::JobVar, JobVarKey(db_id),
                   job_in_db->mutable_runtime_attr());
}

bool RocksDbEmbeddedStore::AppendJobsToPendingAndAdvanceJobIds(
    const std::vector<JobInCtld*>& jobs,
    const std::vector<ExtraVariableWrite>& extra_variable_writes) {
  absl::MutexLock lock_jobs(&job_id_mu_);
  job_id_t job_id{next_job_id_};
  db_id_t job_db_id{next_job_db_id_};
  rocksdb::WriteBatch batch;

  for (auto* job : jobs) {
    job->SetJobId(job_id++);
    job->SetJobDbId(job_db_id++);
    if (!PutProto_(&batch, Cf::JobFixed, JobFixedKey(job->JobDbId()),
                   job->JobToCtld()) ||
        !PutProto_(&batch, Cf::JobVar, JobVarKey(job->JobDbId()),
                   job->RuntimeAttr()))
      return false;
  }

  for (const auto& extra : extra_variable_writes) {
    if (!PutProto_(&batch, Cf::JobVar, JobVarKey(extra.db_id),
                   *extra.runtime_attr))
      return false;
  }

  if (!PutScalar_(&batch, Cf::Meta, MetaKey_(kNextJobIdKey), job_id) ||
      !PutScalar_(&batch, Cf::Meta, MetaKey_(kNextJobDbIdKey), job_db_id))
    return false;

  if (!WriteBatch_(&batch, "rocksdb_append_jobs")) return false;
  next_job_id_ = job_id;
  next_job_db_id_ = job_db_id;
  return true;
}

bool RocksDbEmbeddedStore::PurgeEndedJobs(
    const std::unordered_map<job_id_t, job_db_id_t>& job_ids) {
  rocksdb::WriteBatch batch;
  for (const auto& [job_id, db_id] : job_ids) {
    batch.Delete(Handle_(Cf::JobVar), JobVarKey(db_id));
    batch.Delete(Handle_(Cf::JobFixed), JobFixedKey(db_id));
    batch.Delete(Handle_(Cf::Meta), NextStepIdKey(job_id));
  }

  auto begin = std::chrono::steady_clock::now();
  if (!WriteBatch_(&batch, "rocksdb_purge_jobs")) return false;
  {
    absl::MutexLock lock_steps(&step_id_mu_);
    for (const auto& job_id : job_ids | std::views::keys)
      next_step_id_map_.erase(job_id);
  }
  auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - begin)
                        .count();
  CRANE_TRACE(
      "EmbeddedDbRocksDBDiag embedded_backend=RocksDB op=purge_jobs "
      "rocksdb_purge_job_count={} rocksdb_write_batch_ops={} "
      "rocksdb_write_ms={} rocksdb_sync={}",
      job_ids.size(), batch.Count(), elapsed_ms, g_config.RocksDb.SyncWrites);
  return true;
}

bool RocksDbEmbeddedStore::AppendSteps(const std::vector<StepInCtld*>& steps) {
  absl::MutexLock lock_steps(&step_id_mu_);
  db_id_t step_db_id{next_step_db_id_};
  auto next_step_id_map{next_step_id_map_};
  std::unordered_set<job_id_t> touched_job_ids;
  rocksdb::WriteBatch batch;

  for (auto* step : steps) {
    if (!next_step_id_map.contains(step->job_id))
      next_step_id_map[step->job_id] = 0;
    touched_job_ids.insert(step->job_id);
    step->SetStepId(next_step_id_map[step->job_id]++);
    step->SetStepDbId(step_db_id++);
    if (!PutProto_(&batch, Cf::StepFixed, StepFixedKey(step->StepDbId()),
                   step->StepToCtld()) ||
        !PutProto_(&batch, Cf::StepVar, StepVarKey(step->StepDbId()),
                   step->RuntimeAttr()))
      return false;
  }

  for (const auto& job_id : touched_job_ids) {
    if (!PutScalar_(&batch, Cf::Meta, NextStepIdKey(job_id),
                    next_step_id_map[job_id]))
      return false;
  }
  if (!PutScalar_(&batch, Cf::Meta, MetaKey_(kNextStepDbIdKey), step_db_id))
    return false;

  auto old_map = std::move(next_step_id_map_);
  next_step_id_map_ = next_step_id_map;
  if (!WriteBatch_(&batch, "rocksdb_append_steps")) {
    next_step_id_map_ = std::move(old_map);
    return false;
  }
  next_step_db_id_ = step_db_id;
  return true;
}

bool RocksDbEmbeddedStore::PurgeEndedSteps(
    const std::vector<step_db_id_t>& db_ids) {
  rocksdb::WriteBatch batch;
  for (const auto& id : db_ids) {
    batch.Delete(Handle_(Cf::StepVar), StepVarKey(id));
    batch.Delete(Handle_(Cf::StepFixed), StepFixedKey(id));
  }
  auto begin = std::chrono::steady_clock::now();
  if (!WriteBatch_(&batch, "rocksdb_purge_steps")) return false;
  auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - begin)
                        .count();
  CRANE_TRACE(
      "EmbeddedDbRocksDBDiag embedded_backend=RocksDB op=purge_steps "
      "rocksdb_purge_step_count={} rocksdb_write_batch_ops={} "
      "rocksdb_write_ms={} rocksdb_sync={}",
      db_ids.size(), batch.Count(), elapsed_ms, g_config.RocksDb.SyncWrites);
  return true;
}

bool RocksDbEmbeddedStore::UpdateRuntimeAttrOfStep(
    rocks_txn_id_t txn_id, db_id_t db_id,
    crane::grpc::RuntimeAttrOfStep const& attr) {
  return StoreProto_(txn_id, RocksStoreKind::StepVar, StepVarKey(db_id), attr);
}

bool RocksDbEmbeddedStore::UpdateStepToCtld(
    rocks_txn_id_t txn_id, db_id_t db_id,
    crane::grpc::StepToCtld const& step_to_ctld) {
  return StoreProto_(txn_id, RocksStoreKind::StepFixed, StepFixedKey(db_id),
                     step_to_ctld);
}

bool RocksDbEmbeddedStore::UpdateRuntimeAttrOfStepIfExists(
    rocks_txn_id_t txn_id, db_id_t db_id,
    crane::grpc::RuntimeAttrOfStep const& attr) {
  return StoreProtoIfExists_(txn_id, RocksStoreKind::StepVar,
                             StepVarKey(db_id), attr);
}

bool RocksDbEmbeddedStore::UpdateStepToCtldIfExists(
    rocks_txn_id_t txn_id, db_id_t db_id,
    crane::grpc::StepToCtld const& step_to_ctld) {
  return StoreProtoIfExists_(txn_id, RocksStoreKind::StepFixed,
                             StepFixedKey(db_id), step_to_ctld);
}

bool RocksDbEmbeddedStore::FetchStepDataInDb(rocks_txn_id_t txn_id,
                                             db_id_t db_id,
                                             StepInEmbeddedDb* step_in_db) {
  return GetProto_(Cf::StepFixed, StepFixedKey(db_id),
                   step_in_db->mutable_step_to_ctld()) &&
         GetProto_(Cf::StepVar, StepVarKey(db_id),
                   step_in_db->mutable_runtime_attr());
}

bool RocksDbEmbeddedStore::UpdateReservationInfo(
    rocks_txn_id_t txn_id, const ResvId& name,
    const crane::grpc::CreateReservationRequest& reservation_req) {
  return StoreProto_(txn_id, RocksStoreKind::Reservation, name,
                     reservation_req);
}

bool RocksDbEmbeddedStore::DeleteReservationInfo(rocks_txn_id_t txn_id,
                                                 const ResvId& name) {
  return DeleteKey_(txn_id, RocksStoreKind::Reservation, name);
}

std::string RocksDbEmbeddedStore::JobFixedKey(db_id_t db_id) {
  return fmt::format("{}{}", kJobFixedPrefix, db_id);
}

std::string RocksDbEmbeddedStore::JobVarKey(db_id_t db_id) {
  return fmt::format("{}{}", kJobVarPrefix, db_id);
}

std::string RocksDbEmbeddedStore::StepFixedKey(step_db_id_t db_id) {
  return fmt::format("{}{}", kStepFixedPrefix, db_id);
}

std::string RocksDbEmbeddedStore::StepVarKey(step_db_id_t db_id) {
  return fmt::format("{}{}", kStepVarPrefix, db_id);
}

std::string RocksDbEmbeddedStore::NextStepIdKey(job_id_t job_id) {
  return fmt::format("{}{}", kNextStepIdPrefix, job_id);
}

rocksdb::ColumnFamilyHandle* RocksDbEmbeddedStore::Handle_(Cf cf) const {
  return cf_handles_[static_cast<size_t>(cf)];
}

rocksdb::ColumnFamilyHandle* RocksDbEmbeddedStore::Handle_(
    RocksStoreKind kind) const {
  switch (kind) {
  case RocksStoreKind::JobVar:
    return Handle_(Cf::JobVar);
  case RocksStoreKind::JobFixed:
    return Handle_(Cf::JobFixed);
  case RocksStoreKind::StepVar:
    return Handle_(Cf::StepVar);
  case RocksStoreKind::StepFixed:
    return Handle_(Cf::StepFixed);
  case RocksStoreKind::Reservation:
    return Handle_(Cf::Reservation);
  }
  return nullptr;
}

rocksdb::WriteOptions RocksDbEmbeddedStore::WriteOptions_() const {
  rocksdb::WriteOptions opts;
  opts.sync = g_config.RocksDb.SyncWrites;
  return opts;
}

rocksdb::ReadOptions RocksDbEmbeddedStore::ReadOptions_() const {
  rocksdb::ReadOptions opts;
  return opts;
}

bool RocksDbEmbeddedStore::WriteBatch_(rocksdb::WriteBatch* batch,
                                       std::string_view op_name) {
  auto begin = std::chrono::steady_clock::now();
  auto status = db_->Write(WriteOptions_(), batch);
  auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - begin)
                        .count();
  if (!status.ok()) {
    CRANE_ERROR(
        "RocksDB write failed: op={} ops={} elapsed_ms={} sync={} error={}",
        op_name, batch->Count(), elapsed_ms, g_config.RocksDb.SyncWrites,
        ToString_(status));
    return false;
  }
  CRANE_TRACE(
      "EmbeddedDbRocksDBDiag embedded_backend=RocksDB op={} "
      "rocksdb_write_batch_ops={} rocksdb_write_bytes={} rocksdb_write_ms={} "
      "rocksdb_sync={}",
      op_name, batch->Count(), batch->GetDataSize(), elapsed_ms,
      g_config.RocksDb.SyncWrites);
  return true;
}

bool RocksDbEmbeddedStore::PutProto_(
    rocksdb::WriteBatch* batch, Cf cf, std::string const& key,
    google::protobuf::MessageLite const& value) {
  batch->Put(Handle_(cf), key, SerializeProto_(value));
  return true;
}

bool RocksDbEmbeddedStore::PutScalar_(rocksdb::WriteBatch* batch, Cf cf,
                                      std::string const& key,
                                      const void* value, size_t size) {
  batch->Put(Handle_(cf), key, ScalarValue_(value, size));
  return true;
}

bool RocksDbEmbeddedStore::GetProto_(
    Cf cf, std::string const& key,
    google::protobuf::MessageLite* value) const {
  std::string data;
  auto status = db_->Get(ReadOptions_(), Handle_(cf), key, &data);
  if (!status.ok()) {
    if (status.IsNotFound()) return false;
    CRANE_ERROR("RocksDB failed to fetch proto key {}: {}", key,
                ToString_(status));
    return false;
  }
  return ParseProto_(data, value, key);
}

bool RocksDbEmbeddedStore::GetScalar_(Cf cf, std::string const& key,
                                      void* value, size_t size) const {
  std::string data;
  auto status = db_->Get(ReadOptions_(), Handle_(cf), key, &data);
  if (!status.ok()) {
    if (status.IsNotFound()) return false;
    CRANE_ERROR("RocksDB failed to fetch scalar key {}: {}", key,
                ToString_(status));
    return false;
  }
  if (data.size() != size) {
    CRANE_ERROR("RocksDB scalar key {} size mismatch: expect {}, got {}", key,
                size, data.size());
    return false;
  }
  std::memcpy(value, data.data(), size);
  return true;
}

bool RocksDbEmbeddedStore::KeyExists_(Cf cf, std::string const& key) const {
  std::string data;
  auto status = db_->Get(ReadOptions_(), Handle_(cf), key, &data);
  return status.ok();
}

bool RocksDbEmbeddedStore::StoreProto_(
    rocks_txn_id_t txn_id, RocksStoreKind kind, std::string const& key,
    google::protobuf::MessageLite const& value) {
  if (txn_id == 0) {
    rocksdb::WriteBatch batch;
    if (!PutProto_(&batch, kind == RocksStoreKind::JobVar       ? Cf::JobVar
                          : kind == RocksStoreKind::JobFixed    ? Cf::JobFixed
                          : kind == RocksStoreKind::StepVar     ? Cf::StepVar
                          : kind == RocksStoreKind::StepFixed   ? Cf::StepFixed
                                                                  : Cf::Reservation,
                   key, value))
      return false;
    return WriteBatch_(&batch, "rocksdb_store_proto");
  }

  absl::MutexLock lock(&txn_mu_);
  auto it = pending_batches_.find(txn_id);
  if (it == pending_batches_.end()) {
    CRANE_ERROR("RocksDB store references unknown txn id {}", txn_id);
    return false;
  }
  it->second.batch.Put(Handle_(kind), key, SerializeProto_(value));
  return true;
}

bool RocksDbEmbeddedStore::StoreProtoIfExists_(
    rocks_txn_id_t txn_id, RocksStoreKind kind, std::string const& key,
    google::protobuf::MessageLite const& value) {
  Cf cf = kind == RocksStoreKind::JobVar       ? Cf::JobVar
          : kind == RocksStoreKind::JobFixed   ? Cf::JobFixed
          : kind == RocksStoreKind::StepVar    ? Cf::StepVar
          : kind == RocksStoreKind::StepFixed  ? Cf::StepFixed
                                                : Cf::Reservation;
  if (!KeyExists_(cf, key)) return true;
  return StoreProto_(txn_id, kind, key, value);
}

bool RocksDbEmbeddedStore::DeleteKey_(rocks_txn_id_t txn_id,
                                      RocksStoreKind kind,
                                      std::string const& key) {
  if (txn_id == 0) {
    rocksdb::WriteBatch batch;
    batch.Delete(Handle_(kind), key);
    return WriteBatch_(&batch, "rocksdb_delete_key");
  }

  absl::MutexLock lock(&txn_mu_);
  auto it = pending_batches_.find(txn_id);
  if (it == pending_batches_.end()) {
    CRANE_ERROR("RocksDB delete references unknown txn id {}", txn_id);
    return false;
  }
  it->second.batch.Delete(Handle_(kind), key);
  return true;
}

bool RocksDbEmbeddedStore::InitMeta_() {
  rocksdb::WriteBatch batch;
  job_id_t next_job_id = 1;
  db_id_t next_job_db_id = 1;
  db_id_t next_step_db_id = 1;

  if (!GetScalar_(Cf::Meta, MetaKey_(kNextJobIdKey), &next_job_id)) {
    if (!PutScalar_(&batch, Cf::Meta, MetaKey_(kNextJobIdKey), next_job_id))
      return false;
  }
  if (!GetScalar_(Cf::Meta, MetaKey_(kNextJobDbIdKey), &next_job_db_id)) {
    if (!PutScalar_(&batch, Cf::Meta, MetaKey_(kNextJobDbIdKey),
                    next_job_db_id))
      return false;
  }
  if (!GetScalar_(Cf::Meta, MetaKey_(kNextStepDbIdKey), &next_step_db_id)) {
    if (!PutScalar_(&batch, Cf::Meta, MetaKey_(kNextStepDbIdKey),
                    next_step_db_id))
      return false;
  }

  if (batch.Count() > 0 && !WriteBatch_(&batch, "rocksdb_init_meta"))
    return false;

  {
    absl::MutexLock lock(&job_id_mu_);
    next_job_id_ = next_job_id;
    next_job_db_id_ = next_job_db_id;
  }
  {
    absl::MutexLock lock(&step_id_mu_);
    next_step_db_id_ = next_step_db_id;
  }
  return true;
}

bool RocksDbEmbeddedStore::LoadNextStepIdCache_() {
  absl::MutexLock lock(&step_id_mu_);
  next_step_id_map_.clear();

  std::unique_ptr<rocksdb::Iterator> it(
      db_->NewIterator(ReadOptions_(), Handle_(Cf::Meta)));
  for (it->Seek(std::string{kNextStepIdPrefix}); it->Valid(); it->Next()) {
    auto key = it->key().ToString();
    if (!key.starts_with(kNextStepIdPrefix)) break;
    job_id_t job_id = std::stoul(
        key.substr(std::string{kNextStepIdPrefix}.size()));
    uint32_t next_step_id;
    auto data = it->value().ToString();
    if (data.size() != sizeof(next_step_id)) {
      CRANE_ERROR("Invalid RocksDB next_step_id entry size for key {}", key);
      return false;
    }
    std::memcpy(&next_step_id, data.data(), sizeof(next_step_id));
    next_step_id_map_[job_id] = next_step_id;
  }
  if (!it->status().ok()) {
    CRANE_ERROR("Failed to scan RocksDB next_step_id keys: {}",
                ToString_(it->status()));
    return false;
  }
  return true;
}

bool RocksDbEmbeddedStore::RebuildMissingNextStepIds_(
    const std::unordered_map<job_id_t, uint32_t>& max_step_id_by_job) {
  absl::MutexLock lock(&step_id_mu_);
  rocksdb::WriteBatch batch;
  std::vector<std::pair<job_id_t, uint32_t>> rebuilt_entries;
  for (const auto& [job_id, max_step_id] : max_step_id_by_job) {
    if (!next_step_id_map_.contains(job_id)) {
      auto next_step_id = max_step_id + 1;
      if (!PutScalar_(&batch, Cf::Meta, NextStepIdKey(job_id), next_step_id))
        return false;
      rebuilt_entries.emplace_back(job_id, next_step_id);
    }
  }
  if (batch.Count() > 0 &&
      !WriteBatch_(&batch, "rocksdb_rebuild_next_step_id"))
    return false;
  for (const auto& [job_id, next_step_id] : rebuilt_entries)
    next_step_id_map_[job_id] = next_step_id;
  return true;
}

bool RocksDbEmbeddedStore::DeleteNextStepIdKeys_(rocksdb::WriteBatch* batch) {
  std::unique_ptr<rocksdb::Iterator> it(
      db_->NewIterator(ReadOptions_(), Handle_(Cf::Meta)));
  for (it->Seek(std::string{kNextStepIdPrefix}); it->Valid(); it->Next()) {
    auto key = it->key().ToString();
    if (!key.starts_with(kNextStepIdPrefix)) break;
    batch->Delete(Handle_(Cf::Meta), key);
  }
  if (!it->status().ok()) {
    CRANE_ERROR("Failed to scan RocksDB next_step_id keys for deletion: {}",
                ToString_(it->status()));
    return false;
  }
  return true;
}

rocksdb::CompressionType RocksDbEmbeddedStore::ParseCompression_(
    std::string compression) {
  absl::AsciiStrToLower(&compression);
  if (compression == "lz4") return rocksdb::kLZ4Compression;
  if (compression == "snappy") return rocksdb::kSnappyCompression;
  if (compression == "zlib") return rocksdb::kZlibCompression;
  if (compression == "zstd") return rocksdb::kZSTD;
  if (compression == "none" || compression == "no")
    return rocksdb::kNoCompression;
  CRANE_WARN("Unknown RocksDB compression '{}', fallback to lz4", compression);
  return rocksdb::kLZ4Compression;
}

bool RocksDbEmbeddedStore::IsNotFound_(rocksdb::Status const& status) {
  return status.IsNotFound() || status.IsInvalidArgument();
}

std::string RocksDbEmbeddedStore::ToString_(rocksdb::Status const& status) {
  return status.ToString();
}

bool RocksDbEmbeddedStore::ParseProto_(
    std::string const& data, google::protobuf::MessageLite* value,
    std::string_view key) {
  if (!value->ParseFromArray(data.data(), static_cast<int>(data.size()))) {
    CRANE_ERROR("Failed to parse RocksDB proto key {}", key);
    return false;
  }
  return true;
}

std::string RocksDbEmbeddedStore::SerializeProto_(
    google::protobuf::MessageLite const& value) {
  std::string data;
  data.resize(value.ByteSizeLong());
  value.SerializeToArray(data.data(), static_cast<int>(data.size()));
  return data;
}

std::string RocksDbEmbeddedStore::ScalarValue_(const void* value,
                                               size_t size) {
  return std::string(static_cast<const char*>(value), size);
}

bool RocksDbEmbeddedStore::ParseDbId_(std::string const& key,
                                      std::string_view prefix, db_id_t* id) {
  if (!key.starts_with(prefix)) return false;
  *id = std::stol(key.substr(prefix.size()));
  return true;
}

std::string RocksDbEmbeddedStore::MetaKey_(std::string_view name) {
  return std::string{name};
}

}  // namespace Ctld

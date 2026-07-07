/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

#pragma once

#include "Database/EmbeddedDbClient.h"

namespace Ctld {

using rocks_txn_id_t = txn_id_t;

enum class RocksStoreKind : uint8_t {
  JobVar,
  JobFixed,
  StepVar,
  StepFixed,
  Reservation,
};

class RocksDbEmbeddedStore final : public EmbeddedDbClient {
 public:
  ~RocksDbEmbeddedStore() override;

  bool Init(std::string const& db_path) override;
  void Close();

  bool ResetNextJobId(job_id_t next_job_id, db_id_t next_job_db_id) override;
  bool ResetNextStepDbId() override;
  bool ResetJobStepIdCounter(job_id_t job_id) override;
  bool PurgeAllJobHistory() override;

  bool RetrieveLastSnapshot(DbSnapshot* snapshot) override;
  bool RetrieveStepInfo(StepDbSnapshot* snapshot) override;
  bool RetrieveReservationInfo(
      std::unordered_map<ResvId, crane::grpc::CreateReservationRequest>*
          reservation_info_map) override;

  bool BeginVariableDbTransaction(txn_id_t* txn_id) override {
    return BeginTransaction(RocksStoreKind::JobVar, txn_id);
  }

  bool CommitVariableDbTransaction(txn_id_t txn_id) override {
    return CommitTransaction(txn_id);
  }

  bool BeginFixedDbTransaction(txn_id_t* txn_id) override {
    return BeginTransaction(RocksStoreKind::JobFixed, txn_id);
  }

  bool CommitFixedDbTransaction(txn_id_t txn_id) override {
    return CommitTransaction(txn_id);
  }

  bool BeginStepVarDbTransaction(txn_id_t* txn_id) override {
    return BeginTransaction(RocksStoreKind::StepVar, txn_id);
  }

  bool CommitStepVarDbTransaction(txn_id_t txn_id) override {
    return CommitTransaction(txn_id);
  }

  bool BeginStepFixedDbTransaction(txn_id_t* txn_id) override {
    return BeginTransaction(RocksStoreKind::StepFixed, txn_id);
  }

  bool CommitStepFixedDbTransaction(txn_id_t txn_id) override {
    return CommitTransaction(txn_id);
  }

  bool BeginReservationDbTransaction(txn_id_t* txn_id) override {
    return BeginTransaction(RocksStoreKind::Reservation, txn_id);
  }

  bool CommitReservationDbTransaction(txn_id_t txn_id) override {
    return CommitTransaction(txn_id);
  }

  bool UpdateRuntimeAttrOfJob(
      rocks_txn_id_t txn_id, db_id_t db_id,
      crane::grpc::RuntimeAttrOfJob const& attr) override;
  bool UpdateJobToCtld(rocks_txn_id_t txn_id, db_id_t db_id,
                       crane::grpc::JobToCtld const& job_to_ctld) override;
  bool UpdateRuntimeAttrOfJobIfExists(
      rocks_txn_id_t txn_id, db_id_t db_id,
      crane::grpc::RuntimeAttrOfJob const& attr) override;
  bool UpdateJobToCtldIfExists(
      rocks_txn_id_t txn_id, db_id_t db_id,
      crane::grpc::JobToCtld const& job_to_ctld) override;
  bool FetchJobDataInDb(rocks_txn_id_t txn_id, db_id_t db_id,
                        JobInEmbeddedDb* job_in_db) override;

  bool AppendJobsToPendingAndAdvanceJobIds(
      const std::vector<JobInCtld*>& jobs,
      const std::vector<ExtraVariableWrite>& extra_variable_writes) override;
  bool PurgeEndedJobs(
      const std::unordered_map<job_id_t, job_db_id_t>& job_ids) override;

  bool AppendSteps(const std::vector<StepInCtld*>& steps) override;
  bool PurgeEndedSteps(const std::vector<step_db_id_t>& db_ids) override;

  bool UpdateRuntimeAttrOfStep(
      rocks_txn_id_t txn_id, db_id_t db_id,
      crane::grpc::RuntimeAttrOfStep const& attr) override;
  bool UpdateStepToCtld(rocks_txn_id_t txn_id, db_id_t db_id,
                        crane::grpc::StepToCtld const& step_to_ctld) override;
  bool UpdateRuntimeAttrOfStepIfExists(
      rocks_txn_id_t txn_id, db_id_t db_id,
      crane::grpc::RuntimeAttrOfStep const& attr) override;
  bool UpdateStepToCtldIfExists(
      rocks_txn_id_t txn_id, db_id_t db_id,
      crane::grpc::StepToCtld const& step_to_ctld) override;
  bool FetchStepDataInDb(rocks_txn_id_t txn_id, db_id_t db_id,
                         StepInEmbeddedDb* step_in_db) override;

  bool UpdateReservationInfo(
      rocks_txn_id_t txn_id, const ResvId& name,
      const crane::grpc::CreateReservationRequest& reservation_req) override;
  bool DeleteReservationInfo(rocks_txn_id_t txn_id,
                             const ResvId& name) override;

  static std::string JobFixedKey(db_id_t db_id);
  static std::string JobVarKey(db_id_t db_id);
  static std::string StepFixedKey(step_db_id_t db_id);
  static std::string StepVarKey(step_db_id_t db_id);
  static std::string NextStepIdKey(job_id_t job_id);

 private:
  bool BeginTransaction(RocksStoreKind kind, rocks_txn_id_t* txn_id);
  bool CommitTransaction(rocks_txn_id_t txn_id);

  enum class Cf : uint8_t {
    JobFixed,
    JobVar,
    StepFixed,
    StepVar,
    Reservation,
    Meta,
    Count,
  };

  struct PendingBatch {
    RocksStoreKind kind{RocksStoreKind::JobVar};
    rocksdb::WriteBatch batch;
  };

  rocksdb::ColumnFamilyHandle* Handle_(Cf cf) const;
  rocksdb::ColumnFamilyHandle* Handle_(RocksStoreKind kind) const;
  static rocksdb::WriteOptions WriteOptions_();
  static rocksdb::ReadOptions ReadOptions_();

  bool WriteBatch_(rocksdb::WriteBatch* batch, std::string_view op_name);
  bool PutProto_(rocksdb::WriteBatch* batch, Cf cf, std::string const& key,
                 google::protobuf::MessageLite const& value);
  bool PutScalar_(rocksdb::WriteBatch* batch, Cf cf, std::string const& key,
                  const void* value, size_t size);
  template <std::integral T>
  bool PutScalar_(rocksdb::WriteBatch* batch, Cf cf, std::string const& key,
                  T value) {
    return PutScalar_(batch, cf, key, &value, sizeof(T));
  }

  bool GetProto_(Cf cf, std::string const& key,
                 google::protobuf::MessageLite* value) const;
  bool GetScalar_(Cf cf, std::string const& key, void* value,
                  size_t size) const;
  template <std::integral T>
  bool GetScalar_(Cf cf, std::string const& key, T* value) const {
    return GetScalar_(cf, key, value, sizeof(T));
  }
  bool KeyExists_(Cf cf, std::string const& key) const;

  bool StoreProto_(rocks_txn_id_t txn_id, RocksStoreKind kind,
                   std::string const& key,
                   google::protobuf::MessageLite const& value);
  bool StoreProtoIfExists_(rocks_txn_id_t txn_id, RocksStoreKind kind,
                           std::string const& key,
                           google::protobuf::MessageLite const& value);
  bool DeleteKey_(rocks_txn_id_t txn_id, RocksStoreKind kind,
                  std::string const& key);

  bool InitMeta_();
  bool LoadNextStepIdCache_();
  bool RebuildMissingNextStepIds_(
      const std::unordered_map<job_id_t, uint32_t>& max_step_id_by_job);
  bool DeleteNextStepIdKeys_(rocksdb::WriteBatch* batch);

  static rocksdb::CompressionType ParseCompression_(std::string compression);
  static bool IsNotFound_(rocksdb::Status const& status);
  static std::string ToString_(rocksdb::Status const& status);
  static bool ParseProto_(std::string const& data,
                          google::protobuf::MessageLite* value,
                          std::string_view key);
  static std::string SerializeProto_(
      google::protobuf::MessageLite const& value);
  static std::string ScalarValue_(const void* value, size_t size);
  static bool ParseDbId_(std::string const& key, std::string_view prefix,
                         db_id_t* id);
  static std::string MetaKey_(std::string_view name);
  static Cf CfFromStoreKind_(RocksStoreKind kind);

  std::filesystem::path db_path_;
  std::unique_ptr<rocksdb::DB> db_;
  rocksdb::ColumnFamilyHandle* default_cf_handle_{nullptr};
  std::array<rocksdb::ColumnFamilyHandle*, static_cast<size_t>(Cf::Count)>
      cf_handles_{};

  absl::Mutex job_id_mu_;
  job_id_t next_job_id_ ABSL_GUARDED_BY(job_id_mu_){1};
  db_id_t next_job_db_id_ ABSL_GUARDED_BY(job_id_mu_){1};

  absl::Mutex step_id_mu_;
  db_id_t next_step_db_id_ ABSL_GUARDED_BY(step_id_mu_){1};
  std::unordered_map<job_id_t, uint32_t> next_step_id_map_
      ABSL_GUARDED_BY(step_id_mu_);

  absl::Mutex txn_mu_;
  rocks_txn_id_t next_txn_id_ ABSL_GUARDED_BY(txn_mu_){1};
  std::unordered_map<rocks_txn_id_t, PendingBatch> pending_batches_
      ABSL_GUARDED_BY(txn_mu_);

  bool manual_wal_thread_stop_{false};
  std::thread manual_wal_thread_;
};

}  // namespace Ctld

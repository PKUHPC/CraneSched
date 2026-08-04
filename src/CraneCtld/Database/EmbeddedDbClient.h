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

#include "Account/AccountDefs.h"
#include "protos/Crane.pb.h"

namespace Ctld {

using txn_id_t = uint32_t;

inline constexpr std::array<std::string_view, 3> kCraneEmbeddedDbBackendValues{
    "Unqlite", "BerkeleyDB", "RocksDB"};

inline bool IsValidCraneEmbeddedDbBackend(std::string_view backend) {
  return std::ranges::contains(kCraneEmbeddedDbBackendValues, backend);
}

// Key space of the dynamic node storage, shared by all backends: records are
// keyed by their bare node name; generation high watermarks live under this
// prefix, whose leading control byte cannot appear in a valid node name.
inline constexpr std::string_view kDynamicNodeGenerationPrefix =
    "\x01generation/";

class EmbeddedDbClient {
 public:
  using db_id_t = job_db_id_t;
  using JobInEmbeddedDb = crane::grpc::JobInEmbeddedDb;
  using StepInEmbeddedDb = crane::grpc::StepInEmbeddedDb;
  using DynamicNodeRecord = crane::grpc::DynamicNodeRecord;
  using DynamicNodeGenerationMap = std::unordered_map<CranedId, uint64_t>;

  struct DbSnapshot {
    std::unordered_map<db_id_t, JobInEmbeddedDb> pending_queue;
    std::unordered_map<db_id_t, JobInEmbeddedDb> running_queue;
    std::unordered_map<db_id_t, JobInEmbeddedDb> final_queue;
  };

  struct StepDbSnapshot {
    std::unordered_map<job_id_t, std::vector<StepInEmbeddedDb>> steps;
  };

  // Extra variable-db writes that should be committed atomically together
  // with the new jobs' variable-db entries (e.g. updating the runtime attr
  // of an array parent when materializing its children).
  struct ExtraVariableWrite {
    db_id_t db_id;
    crane::grpc::RuntimeAttrOfJob const* runtime_attr;
  };

  EmbeddedDbClient() = default;
  virtual ~EmbeddedDbClient() = default;

  virtual bool Init(std::string const& db_path) = 0;

  virtual bool ResetNextJobId(job_id_t next_job_id, db_id_t next_job_db_id) = 0;

  virtual bool ResetNextStepDbId() = 0;

  virtual bool ResetJobStepIdCounter(job_id_t job_id) = 0;

  virtual bool PurgeAllJobHistory() = 0;

  virtual bool RetrieveLastSnapshot(DbSnapshot* snapshot) = 0;

  virtual bool RetrieveStepInfo(StepDbSnapshot* snapshot) = 0;

  virtual bool RetrieveReservationInfo(
      std::unordered_map<ResvId, crane::grpc::CreateReservationRequest>*
          reservation_info_map) = 0;

  // Loads all dynamic node records keyed by node name, together with the
  // per-name generation high watermarks (see kDynamicNodeGenerationPrefix).
  virtual bool RetrieveDynamicNodeRecords(
      std::unordered_map<CranedId, DynamicNodeRecord>* records,
      DynamicNodeGenerationMap* generation_high_watermarks) = 0;

  // Upserts the records and raises their generation high watermarks in one
  // transaction.
  virtual bool StoreDynamicNodeRecords(
      const std::vector<DynamicNodeRecord>& records) = 0;

  // Deletes the records but intentionally KEEPS (and updates) the generation
  // high watermark of each name: a later re-creation of the same name must
  // get a strictly larger generation so that stale craneds cannot re-attach.
  virtual bool DeleteDynamicNodeRecords(
      const std::vector<DynamicNodeRecord>& records) = 0;

  virtual bool BeginVariableDbTransaction(txn_id_t* txn_id) = 0;

  virtual bool CommitVariableDbTransaction(txn_id_t txn_id) = 0;

  virtual bool BeginFixedDbTransaction(txn_id_t* txn_id) = 0;

  virtual bool CommitFixedDbTransaction(txn_id_t txn_id) = 0;

  virtual bool BeginStepVarDbTransaction(txn_id_t* txn_id) = 0;

  virtual bool CommitStepVarDbTransaction(txn_id_t txn_id) = 0;

  virtual bool BeginStepFixedDbTransaction(txn_id_t* txn_id) = 0;

  virtual bool CommitStepFixedDbTransaction(txn_id_t txn_id) = 0;

  virtual bool BeginReservationDbTransaction(txn_id_t* txn_id) = 0;

  virtual bool CommitReservationDbTransaction(txn_id_t txn_id) = 0;

  // Note: All operations in transaction will abort or rollback automatically if
  // some operation fails, so we don't need anything like AbortTransaction here!

  // Assign fresh IDs to jobs and persist them into embedded DB atomically.
  //
  // On success, all fixed-db entries, all variable-db entries, the persisted
  // next-id counters, and any `extra_variable_writes` are committed, and the
  // in-memory next-id counters are advanced.
  //
  // On failure, the in-memory next-id counters are left untouched. Any
  // fixed-db entries that were committed before a subsequent failure will be
  // reclaimed on the next successful call (their db_ids are reused) or by
  // crash-recovery cleanup (orphan fixed entries without a variable-db
  // counterpart are deleted at startup).
  virtual bool AppendJobsToPendingAndAdvanceJobIds(
      const std::vector<JobInCtld*>& jobs,
      const std::vector<ExtraVariableWrite>& extra_variable_writes = {}) = 0;

  virtual bool PurgeEndedJobs(
      const std::unordered_map<job_id_t, job_db_id_t>& job_ids) = 0;

  virtual bool UpdateRuntimeAttrOfJob(
      txn_id_t txn_id, db_id_t db_id,
      crane::grpc::RuntimeAttrOfJob const& runtime_attr) = 0;

  virtual bool UpdateJobToCtld(
      txn_id_t txn_id, db_id_t db_id,
      crane::grpc::JobToCtld const& job_to_ctld_ref) = 0;

  virtual bool UpdateRuntimeAttrOfJobIfExists(
      txn_id_t txn_id, db_id_t db_id,
      crane::grpc::RuntimeAttrOfJob const& runtime_attr) = 0;

  virtual bool UpdateJobToCtldIfExists(
      txn_id_t txn_id, db_id_t db_id,
      crane::grpc::JobToCtld const& job_to_ctld_ref) = 0;

  virtual bool FetchJobDataInDb(txn_id_t txn_id, db_id_t db_id,
                                JobInEmbeddedDb* job_in_db) = 0;

  virtual bool AppendSteps(const std::vector<StepInCtld*>& steps) = 0;

  virtual bool PurgeEndedSteps(const std::vector<step_db_id_t>& db_ids) = 0;

  virtual bool UpdateRuntimeAttrOfStep(
      txn_id_t txn_id, db_id_t db_id,
      crane::grpc::RuntimeAttrOfStep const& runtime_attr) = 0;

  virtual bool UpdateStepToCtld(
      txn_id_t txn_id, db_id_t db_id,
      crane::grpc::StepToCtld const& step_to_ctld) = 0;

  virtual bool UpdateRuntimeAttrOfStepIfExists(
      txn_id_t txn_id, db_id_t db_id,
      crane::grpc::RuntimeAttrOfStep const& runtime_attr) = 0;

  virtual bool UpdateStepToCtldIfExists(
      txn_id_t txn_id, db_id_t db_id,
      crane::grpc::StepToCtld const& step_to_ctld) = 0;

  virtual bool FetchStepDataInDb(txn_id_t txn_id, db_id_t db_id,
                                 StepInEmbeddedDb* step_in_db) = 0;

  virtual bool UpdateReservationInfo(
      txn_id_t txn_id, const ResvId& name,
      const crane::grpc::CreateReservationRequest& reservation_req) = 0;

  virtual bool DeleteReservationInfo(txn_id_t txn_id, const ResvId& name) = 0;
};

std::unique_ptr<EmbeddedDbClient> MakeEmbeddedDbClient(
    std::string_view backend);

}  // namespace Ctld

inline std::unique_ptr<Ctld::EmbeddedDbClient> g_embedded_db_client;

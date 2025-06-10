#pragma once

#include <atomic>
#include <cassert>
#include <chrono>
#include <iostream>
#include <libnuraft/nuraft.hxx>
#include <mutex>

#include "EmbeddedDbClient.h"
#include "crane/EmbeddedDb.h"
#include "crane/Lock.h"
#include "crane/Pointer.h"

namespace Ctld {

using nuraft::async_result;
using nuraft::buffer;
using nuraft::cluster_config;
using nuraft::log_entry;
using nuraft::snapshot;

class CraneStateMachine : public nuraft::state_machine {
 public:
  using ValueMapType = std::unordered_map<std::string, std::vector<uint8_t>>;
  using ValueMapConstPtr =
      util::ScopeConstSharedPtr<ValueMapType, util::rw_mutex>;
  using ValueMapExclusivePtr =
      util::ScopeExclusivePtr<ValueMapType, util::rw_mutex>;

#ifdef CRANE_HAVE_UNQLITE
  using UnqliteDb = crane::Internal::UnqliteDb;
#endif
#ifdef CRANE_HAVE_BERKELEY_DB
  using BerkeleyDb = crane::Internal::BerkeleyDb;
#endif

  enum CraneCtldOpType : uint8_t {
    OP_UNKNOWN = 0,
    OP_VAR_STORE = 1,
    OP_VAR_DELETE = 2,
    OP_FIX_STORE = 3,
    OP_FIX_DELETE = 4,
    OP_RESV_STORE = 5,
    OP_RESV_DELETE = 6,
  };

  explicit CraneStateMachine(bool async_snapshot = false)
      : last_committed_idx_(0), async_snapshot_(async_snapshot) {}

  ~CraneStateMachine() override;

  static std::shared_ptr<buffer> enc_log(CraneCtldOpType type,
                                         const std::string &key,
                                         const void *data = nullptr,
                                         size_t len = 0);

  static bool dec_log(buffer &log, CraneCtldOpType *type, std::string *key,
                      std::vector<uint8_t> *value);

  bool Init(const std::string &db_path,
            std::vector<std::shared_ptr<log_entry>> logs);

  std::shared_ptr<buffer> pre_commit(const uint64_t log_idx,
                                     buffer &data) override {
    return nullptr;
  }

  std::shared_ptr<buffer> commit(const uint64_t log_idx, buffer &data) override;

  void commit_config(const uint64_t log_idx,
                     std::shared_ptr<cluster_config> &new_conf) override {
    // Nothing to do with configuration change. Just update committed index.
    last_committed_idx_ = log_idx;
    StoreValueToDB_(s_last_commit_idx_key_str_, last_committed_idx_);
  }

  void rollback(const uint64_t log_idx, buffer &data) override {
    // Nothing to do with rollback, as it doesn't do anything on pre-commit.
    // Rollback the state machine to given Raft log number. It will be called
    // for uncommitted Raft logs only
  }

  int read_logical_snp_obj(snapshot &s, void *&user_snp_ctx, uint64_t obj_id,
                           std::shared_ptr<buffer> &data_out,
                           bool &is_last_obj) override;

  void save_logical_snp_obj(snapshot &s, uint64_t &obj_id, buffer &data,
                            bool is_first_obj, bool is_last_obj) override;

  bool apply_snapshot(snapshot &s) override;

  void free_user_snp_ctx(void *&user_snp_ctx) override {
    if (user_snp_ctx)
      delete static_cast<ValueMapType::iterator *>(user_snp_ctx);
  }

  std::shared_ptr<snapshot> last_snapshot() override;

  uint64_t last_commit_index() override { return last_committed_idx_; }

  void create_snapshot(snapshot &s,
                       async_result<bool>::handler_type &when_done) override;

  // Not allowed to transfer the leadership to another member
  bool allow_leadership_transfer() override { return false; }

  ValueMapConstPtr GetValueMapConstPtr(uint8_t db_index);

  ValueMapExclusivePtr GetValueMapExclusivePtr(uint8_t db_index);

 private:
  struct snapshot_ctx {
    snapshot_ctx() {};
    snapshot_ctx(std::shared_ptr<nuraft::snapshot> &s, uint64_t i)
        : snapshot(s), log_index(i) {};
    uint64_t log_index;
    std::shared_ptr<nuraft::snapshot> snapshot;

    ValueMapType var_value_map, fix_value_map, resv_value_map;
  };

  inline bool StoreValueToDB_(const std::string &key, uint64_t value) {
    auto res = m_variable_db_->Begin();
    if (res.has_value()) {
      m_variable_db_->Store(res.value(), key, &value, sizeof(uint64_t));
      return m_variable_db_->Commit(res.value()).has_value();
    } else {
      CRANE_ERROR("Failed to begin a transaction.");
      return false;
    }
  }

  bool RestoreFromDB_();

  void Apply_(buffer &data);

  bool OnStore_(ValueMapType &map, const std::string &key,
                std::vector<uint8_t> &&data);

  bool OnDelete_(ValueMapType &map, const std::string &key);

  void CreateSnapshotInternalNoLock_(std::shared_ptr<snapshot> ss);

  void PersistSnapshotNoLock_();

  void CreateSnapshotSync_(snapshot &s,
                           async_result<bool>::handler_type &when_done);

  void CreateSnapshotAsync_(snapshot &s,
                            async_result<bool>::handler_type &when_done);
  // Last committed Raft log number.
  std::atomic<uint64_t> last_committed_idx_;

  inline static std::string const s_last_commit_idx_key_str_{"LC"};
  inline static std::string const s_saved_snapshot_obj_key_str_{"SO"};

  // snapshot
  std::unique_ptr<snapshot_ctx> m_snapshot_;

  //   Mutex for `snapshots_`.
  util::mutex snapshots_lock_;

  // If `true`, snapshot will be created asynchronously.
  bool async_snapshot_;

  util::rw_mutex var_map_lock_, fix_map_lock_, resv_map_lock_;

  ValueMapType var_value_map_ ABSL_GUARDED_BY(var_map_lock_),
      fix_value_map_ ABSL_GUARDED_BY(fix_map_lock_),
      resv_value_map_ ABSL_GUARDED_BY(resv_map_lock_);
  std::unique_ptr<crane::Internal::IEmbeddedDb> m_variable_db_, m_fixed_db_,
      m_resv_db_;
};

};  // namespace Ctld

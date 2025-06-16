#include "CraneStateMachine.h"

#include <bit>

namespace Ctld {

CraneStateMachine::~CraneStateMachine() {
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

  if (m_resv_db_) {
    auto result = m_resv_db_->Close();
    if (!result)
      CRANE_ERROR(
          "Error occurred when closing the embedded db of reservation data!");
  }
}

std::shared_ptr<buffer> CraneStateMachine::enc_log(CraneCtldOpType type,
                                                   const std::string &key,
                                                   const void *data,
                                                   size_t len) {
  // Encode from op_payload to Raft log.
  size_t total_size = sizeof(CraneCtldOpType) /*type seat*/ +
                      sizeof(uint32_t) /*key len seat*/ +
                      key.size() /*key seat*/ +
                      sizeof(uint64_t) /*value len seat*/ + len /*value seat*/;
  std::shared_ptr<buffer> ret = buffer::alloc(total_size);
  buffer_serializer::endianness endian = buffer_serializer::LITTLE;
  if constexpr (std::endian::native == std::endian::big)
    endian = buffer_serializer::BIG;
  buffer_serializer bs(ret, endian);

  bs.put_u8(type);
  bs.put_str(key);
  bs.put_u64(len);
  if (data) bs.put_raw(data, len);

  return ret;
}

bool CraneStateMachine::dec_log(buffer &log, CraneCtldOpType *type,
                                std::string *key, std::vector<uint8_t> *value) {
  // Decode from Raft log
  buffer_serializer::endianness endian = buffer_serializer::LITTLE;
  if constexpr (std::endian::native == std::endian::big)
    endian = buffer_serializer::BIG;
  buffer_serializer bs(log, endian);
  *type = static_cast<CraneCtldOpType>(bs.get_u8());
  *key = bs.get_str();
  uint64_t len = bs.get_u64();
  if (len > 0) {
    value->resize(len);
    memcpy(value->data(), bs.get_raw(len), len);
  }

  return true;
}

bool CraneStateMachine::Init(const std::string &db_path,
                             std::vector<std::shared_ptr<log_entry>> logs) {
  if (g_config.CraneEmbeddedDbBackend == "Unqlite") {
#ifdef CRANE_HAVE_UNQLITE
    m_variable_db_ = std::make_unique<UnqliteDb>();
    m_fixed_db_ = std::make_unique<UnqliteDb>();
    m_resv_db_ = std::make_unique<UnqliteDb>();
#else
    CRANE_ERROR(
        "Select unqlite as the embedded db but it's not been compiled.");
    return false;
#endif

  } else if (g_config.CraneEmbeddedDbBackend == "BerkeleyDB") {
#ifdef CRANE_HAVE_BERKELEY_DB
    m_variable_db_ = std::make_unique<BerkeleyDb>();
    m_fixed_db_ = std::make_unique<BerkeleyDb>();
    m_resv_db_ = std::make_unique<BerkeleyDb>();
#else
    CRANE_ERROR(
        "Select Berkeley DB as the embedded db but it's not been compiled.");
    return false;
#endif

  } else {
    CRANE_ERROR("Invalid embedded database backend: {}",
                g_config.CraneEmbeddedDbBackend);
    return false;
  }

  auto result = m_variable_db_->Init(db_path + "var_raft");
  if (!result) return false;
  result = m_fixed_db_->Init(db_path + "fix_raft");
  if (!result) return false;
  result = m_resv_db_->Init(db_path + "resv_raft");
  if (!result) return false;

  RestoreFromDB_();

  for (const auto &l : logs) {
    if (l->get_val_type() == nuraft::log_val_type::app_log &&
        l->get_term() > 0) {
      Apply_(l->get_buf());
    }
  }

  return true;
}

std::shared_ptr<buffer> CraneStateMachine::commit(const uint64_t log_idx,
                                                  buffer &data) {
  Apply_(data);

  last_committed_idx_ = log_idx;
  StoreValueToDB_(s_last_commit_idx_key_str_, last_committed_idx_);

  // Return Raft log number as a return result.
  std::shared_ptr<buffer> ret = buffer::alloc(sizeof(log_idx));
  buffer_serializer bs(ret);
  bs.put_u64(log_idx);
  return ret;
}

int CraneStateMachine::read_logical_snp_obj(snapshot &s, void *&user_snp_ctx,
                                            uint64_t obj_id,
                                            std::shared_ptr<buffer> &data_out,
                                            bool &is_last_obj) {
  util::lock_guard lg(snapshots_lock_);
  if (!m_snapshot_ || m_snapshot_->log_index <= 0) {
    // Snapshot doesn't exist.
    //
    // NOTE:
    //   This should not happen in real use cases.
    //   The below code is an example about how to handle
    //   this case without aborting the server.
    data_out = nullptr;
    is_last_obj = true;
    return -1;
  }

  ValueMapType::iterator *it;
  if (user_snp_ctx) it = static_cast<ValueMapType::iterator *>(user_snp_ctx);

  if (obj_id == 0) {
    // Object ID == 0: first object, put dummy data.
    it = new ValueMapType::iterator(m_snapshot_->var_value_map.begin());
    user_snp_ctx = it;

    data_out = buffer::alloc(sizeof(CraneCtldOpType));
    buffer_serializer bs(data_out);
    bs.put_u8(OP_UNKNOWN);
    is_last_obj = false;
  }
  /** The snapshot reading process cycles through three phases using obj_id % 3:
   * - obj_id % 3 == 1: Read from var_value_map
   * - obj_id % 3 == 2: Read from fix_value_map
   * - obj_id % 3 == 0: Read from resv_value_map (last phase)
   * Empty log entries with type switches are used to transition between phases
   */
  else if (obj_id % 3 == 1) {
    // Object ID %= 1: start read var_value_map_
    if (*it == m_snapshot_->var_value_map.end()) {
      *it = m_snapshot_->fix_value_map.begin();  // switch value map
      data_out = enc_log(OP_FIX_STORE, "", nullptr, 0);
      is_last_obj = false;
      return 0;
    }

    data_out = enc_log(OP_VAR_STORE, (*it)->first, (*it)->second.data(),
                       (*it)->second.size());
    is_last_obj = false;
    ++(*it);
  } else if (obj_id % 3 == 2) {
    // Object ID %= 2: start read fix_value_map_
    if (*it == m_snapshot_->fix_value_map.end()) {
      *it = m_snapshot_->resv_value_map.begin();  // switch value map
      data_out = enc_log(OP_RESV_STORE, "", nullptr, 0);
      is_last_obj = false;
      return 0;
    }

    data_out = enc_log(OP_FIX_STORE, (*it)->first, (*it)->second.data(),
                       (*it)->second.size());
    is_last_obj = false;
    ++(*it);
  } else if (obj_id % 3 == 0) {
    // Object ID %= 3: start read resv_value_map_
    if (*it == m_snapshot_->resv_value_map.end()) {
      data_out = enc_log(OP_RESV_STORE, "", nullptr, 0);
      is_last_obj = true;
      return 0;
    }

    data_out = enc_log(OP_RESV_STORE, (*it)->first, (*it)->second.data(),
                       (*it)->second.size());
    is_last_obj = false;
    ++(*it);
  } else {
    return -1;
  }
  return 0;
}

void CraneStateMachine::save_logical_snp_obj(snapshot &s, uint64_t &obj_id,
                                             buffer &data, bool is_first_obj,
                                             bool is_last_obj) {
  static ValueMapType var_value_map, fix_value_map, resv_value_map;

  if (obj_id == 0) {
    util::lock_guard lg(snapshots_lock_);
    // Object ID == 0: it contains dummy value, create snapshot context.
    std::shared_ptr<buffer> snp_buf = s.serialize();
    auto ss_ptr = snapshot::deserialize(*snp_buf);
    m_snapshot_ = std::make_unique<snapshot_ctx>(ss_ptr, s.get_last_log_idx());
    var_value_map.clear();
    fix_value_map.clear();
    resv_value_map.clear();
    obj_id = 1;
  } else if (obj_id % 3 == 1) {
    // Object ID %= 1: start write var_value_map_
    CraneCtldOpType type;
    std::string key;
    std::vector<uint8_t> value;

    dec_log(data, &type, &key, &value);
    if (type == OP_FIX_STORE) {
      obj_id = 2;
      return;
    }
    var_value_map[key] = std::move(value);
    obj_id += 3;
  } else if (obj_id % 3 == 2) {
    // Object ID %= 2: start write fix_value_map_
    CraneCtldOpType type;
    std::string key;
    std::vector<uint8_t> value;

    dec_log(data, &type, &key, &value);
    if (type == OP_RESV_STORE) {
      obj_id = 3;
      return;
    }
    fix_value_map[key] = std::move(value);
    obj_id += 3;
  } else if (obj_id % 3 == 0) {
    // Object ID = 3: start write resv_value_map_
    if (is_last_obj) {
      {
        util::lock_guard lg(snapshots_lock_);

        assert(m_snapshot_->log_index == s.get_last_log_idx());
        m_snapshot_->var_value_map.swap(var_value_map);
        m_snapshot_->fix_value_map.swap(fix_value_map);
        m_snapshot_->resv_value_map.swap(resv_value_map);
        CRANE_INFO("Snapshot ({}, {}) has been created logically",
                   s.get_last_log_term(), s.get_last_log_idx());

        PersistSnapshotNoLock_();
      }
      return;
    }

    CraneCtldOpType type;
    std::string key;
    std::vector<uint8_t> value;

    dec_log(data, &type, &key, &value);
    resv_value_map[key] = std::move(value);
    obj_id += 3;
  }
}

bool CraneStateMachine::apply_snapshot(snapshot &s) {
  util::lock_guard lg(snapshots_lock_);

  if (!m_snapshot_ || s.get_last_log_idx() != m_snapshot_->log_index)
    return false;

  util::write_lock_guard lg1(var_map_lock_);
  util::write_lock_guard lg2(fix_map_lock_);
  util::write_lock_guard lg3(resv_map_lock_);

  var_value_map_ = m_snapshot_->var_value_map;
  fix_value_map_ = m_snapshot_->fix_value_map;
  resv_value_map_ = m_snapshot_->resv_value_map;

  return true;
}

std::shared_ptr<snapshot> CraneStateMachine::last_snapshot() {
  // Just return the latest snapshot.
  util::lock_guard lg(snapshots_lock_);
  if (!m_snapshot_)
    return nullptr;
  else
    return m_snapshot_->snapshot;
}

void CraneStateMachine::create_snapshot(
    snapshot &s, async_result<bool>::handler_type &when_done) {
  if (!async_snapshot_) {
    // Create a snapshot in a synchronous way (blocking the thread).
    CreateSnapshotSync_(s, when_done);
  } else {
    // Create a snapshot in an asynchronous way (in a different thread).
    CreateSnapshotAsync_(s, when_done);
  }
}

CraneStateMachine::ValueMapConstPtr CraneStateMachine::GetValueMapConstPtr(
    uint8_t db_index) {
  if (db_index == kVarValueMapTableIndex) {
    var_map_lock_.lock_shared();
    return ValueMapConstPtr{&var_value_map_, &var_map_lock_};
  } else if (db_index == kFixValueMapTableIndex) {
    fix_map_lock_.lock_shared();
    return ValueMapConstPtr{&fix_value_map_, &fix_map_lock_};
  } else if (db_index == kResvValueMapTableIndex) {
    resv_map_lock_.lock_shared();
    return ValueMapConstPtr{&resv_value_map_, &resv_map_lock_};
  } else {
    return ValueMapConstPtr{nullptr};
  }
}
CraneStateMachine::ValueMapExclusivePtr
CraneStateMachine::GetValueMapExclusivePtr(uint8_t db_index) {
  if (db_index == kVarValueMapTableIndex) {
    var_map_lock_.lock();
    return ValueMapExclusivePtr{&var_value_map_, &var_map_lock_};
  } else if (db_index == kFixValueMapTableIndex) {
    fix_map_lock_.lock();
    return ValueMapExclusivePtr{&fix_value_map_, &fix_map_lock_};
  } else if (db_index == kResvValueMapTableIndex) {
    resv_map_lock_.lock();
    return ValueMapExclusivePtr{&resv_value_map_, &resv_map_lock_};
  } else {
    return ValueMapExclusivePtr{nullptr};
  }
}

bool CraneStateMachine::RestoreFromDB_() {
  // restore last_committed_idx_
  size_t size = sizeof(last_committed_idx_);
  auto fetch_result = m_variable_db_->Fetch(0, s_last_commit_idx_key_str_,
                                            &last_committed_idx_, &size);

  if (fetch_result) {
    CRANE_TRACE("Found last_committed_idx_ = {}", last_committed_idx_.load());
  } else if (fetch_result.error() == DbErrorCode::kNotFound) {
    CRANE_TRACE(
        "last_committed_idx_ not found in embedded db. Initialize it with "
        "value 0.");
  } else {
    CRANE_ERROR("Unexpected error when fetching scalar key '{}'.",
                s_last_commit_idx_key_str_);
  }

  // restore the snapshot object
  // no need to lock
  size_t n_bytes{0};

  fetch_result = m_variable_db_->Fetch(0, s_saved_snapshot_obj_key_str_,
                                       nullptr, &n_bytes);
  if (!fetch_result) {
    if (fetch_result.error() == DbErrorCode::kNotFound) {
      CRANE_TRACE("Snapshot not found in embedded db. Initialize it by emtpy.");
      m_snapshot_ = std::make_unique<snapshot_ctx>();
      return true;
    } else {
      CRANE_ERROR("Unexpected error when fetching scalar key '{}'.",
                  s_saved_snapshot_obj_key_str_);
    }
  } else {
    std::shared_ptr<buffer> buf = buffer::alloc(n_bytes);

    fetch_result = m_variable_db_->Fetch(0, s_saved_snapshot_obj_key_str_,
                                         buf->data(), &n_bytes);
    if (!fetch_result) {
      CRANE_ERROR("Unexpected error when fetching the data of string key '{}'",
                  s_saved_snapshot_obj_key_str_);
    } else {
      auto ss = snapshot::deserialize(*buf);
      m_snapshot_ = std::make_unique<snapshot_ctx>(ss, ss->get_last_log_idx());
    }
  }

  std::expected<void, DbErrorCode> result;

  result = m_variable_db_->IterateAllKv(
      [&](std::string &&key, std::vector<uint8_t> &&value) {
        if (key == s_last_commit_idx_key_str_ ||
            key == s_saved_snapshot_obj_key_str_)
          return true;

        m_snapshot_->var_value_map[key] = std::move(value);
        return true;
      });

  if (!result) {
    CRANE_ERROR("Failed to apply snapshots from variable data!");
    return false;
  }

  result = m_fixed_db_->IterateAllKv(
      [&](std::string &&key, std::vector<uint8_t> &&value) {
        m_snapshot_->fix_value_map[key] = std::move(value);
        return true;
      });

  if (!result) {
    CRANE_ERROR("Failed to apply snapshots from fixed data!");
    return false;
  }

  result = m_resv_db_->IterateAllKv(
      [&](std::string &&key, std::vector<uint8_t> &&value) {
        m_snapshot_->resv_value_map[key] = std::move(value);
        return true;
      });

  if (!result) {
    CRANE_ERROR("Failed to apply snapshots from reservation data!");
    return false;
  }

  apply_snapshot(*m_snapshot_->snapshot);
  return true;
}

void CraneStateMachine::Apply_(buffer &data) {
  CraneCtldOpType type;
  std::string key;
  std::vector<uint8_t> value;
  dec_log(data, &type, &key, &value);

  switch (type) {
  case OP_VAR_STORE: {
    util::write_lock_guard lg(var_map_lock_);
    OnStore_(var_value_map_, key, std::move(value));
    break;
  }
  case OP_VAR_DELETE: {
    util::write_lock_guard lg(var_map_lock_);
    OnDelete_(var_value_map_, key);
    break;
  }
  case OP_FIX_STORE: {
    util::write_lock_guard lg(fix_map_lock_);
    OnStore_(fix_value_map_, key, std::move(value));
    break;
  }
  case OP_FIX_DELETE: {
    util::write_lock_guard lg(fix_map_lock_);
    OnDelete_(fix_value_map_, key);
    break;
  }
  case OP_RESV_STORE: {
    util::write_lock_guard lg(resv_map_lock_);
    OnStore_(resv_value_map_, key, std::move(value));
    break;
  }
  case OP_RESV_DELETE: {
    util::write_lock_guard lg(resv_map_lock_);
    OnDelete_(resv_value_map_, key);
    break;
  }
  default:
    break;
  }
}

bool CraneStateMachine::OnStore_(CraneStateMachine::ValueMapType &map,
                                 const std::string &key,
                                 std::vector<uint8_t> &&data) {
  if (!data.empty()) {
    map[key] = std::move(data);
    return true;
  } else
    return false;
}

bool CraneStateMachine::OnDelete_(CraneStateMachine::ValueMapType &map,
                                  const std::string &key) {
  if (!map.contains(key)) return false;

  if (map.erase(key) == 0) {
    CRANE_ERROR("Failed to delete key {} from db", key);
    return false;
  }
  return true;
}

void CraneStateMachine::CreateSnapshotInternalNoLock_(
    std::shared_ptr<snapshot> ss) {
  m_snapshot_ = std::make_unique<snapshot_ctx>(ss, ss->get_last_log_idx());

  m_snapshot_->var_value_map = var_value_map_;
  m_snapshot_->fix_value_map = fix_value_map_;
  m_snapshot_->resv_value_map = resv_value_map_;
}

void CraneStateMachine::PersistSnapshotNoLock_() {
  if (!m_snapshot_) return;
  txn_id_t var_txn_id, fix_txn_id, resv_txn_id;

  g_embedded_db_client->BeginDbTransaction(m_variable_db_.get(), &var_txn_id);
  m_variable_db_->Clear(var_txn_id);
  for (const auto &[k, v] : m_snapshot_->var_value_map) {
    m_variable_db_->Store(var_txn_id, k, v.data(), v.size());
  }
  // save snapshot object
  auto buf = m_snapshot_->snapshot->serialize();
  m_variable_db_->Store(var_txn_id, s_saved_snapshot_obj_key_str_, buf->data(),
                        buf->size());
  g_embedded_db_client->CommitDbTransaction(m_variable_db_.get(), var_txn_id);

  g_embedded_db_client->BeginDbTransaction(m_fixed_db_.get(), &fix_txn_id);
  m_fixed_db_->Clear(fix_txn_id);
  for (const auto &[k, v] : m_snapshot_->fix_value_map) {
    m_fixed_db_->Store(fix_txn_id, k, v.data(), v.size());
  }
  g_embedded_db_client->CommitDbTransaction(m_fixed_db_.get(), fix_txn_id);

  g_embedded_db_client->BeginDbTransaction(m_resv_db_.get(), &resv_txn_id);
  m_resv_db_->Clear(resv_txn_id);
  for (const auto &[k, v] : m_snapshot_->resv_value_map) {
    m_resv_db_->Store(resv_txn_id, k, v.data(), v.size());
  }
  g_embedded_db_client->CommitDbTransaction(m_resv_db_.get(), resv_txn_id);
}

void CraneStateMachine::CreateSnapshotSync_(
    snapshot &s,
    async_result<bool>::handler_type &when_done) {  // Clone snapshot from `s`.
  std::shared_ptr<buffer> snp_buf = s.serialize();
  std::shared_ptr<snapshot> ss = snapshot::deserialize(*snp_buf);
  {
    util::lock_guard lg(snapshots_lock_);
    {
      util::write_lock_guard lg1(var_map_lock_);
      util::write_lock_guard lg2(fix_map_lock_);
      util::write_lock_guard lg3(resv_map_lock_);
      CreateSnapshotInternalNoLock_(ss);
    }
    PersistSnapshotNoLock_();
  }

  std::shared_ptr<std::exception> except(nullptr);
  bool ret = true;
  when_done(ret, except);

  CRANE_TRACE("snapshot ({}, {}) has been created synchronously",
              ss->get_last_log_term(), ss->get_last_log_idx());
}

void CraneStateMachine::CreateSnapshotAsync_(
    snapshot &s,
    async_result<bool>::handler_type &when_done) {  // Clone snapshot from `s`.
  std::shared_ptr<buffer> snp_buf = s.serialize();
  std::shared_ptr<snapshot> ss = snapshot::deserialize(*snp_buf);

  // Note that this is a very naive and inefficient example
  // that creates a new thread for each snapshot creation.
  g_thread_pool->detach_task([this, ss, when_done]() {
    {
      util::lock_guard lg(snapshots_lock_);
      {
        util::write_lock_guard lg1(var_map_lock_);
        util::write_lock_guard lg2(fix_map_lock_);
        util::write_lock_guard lg3(resv_map_lock_);
        CreateSnapshotInternalNoLock_(ss);
      }
      PersistSnapshotNoLock_();
    }

    std::shared_ptr<std::exception> except(nullptr);
    bool ret = true;
    when_done(ret, except);

    CRANE_TRACE("snapshot ({}, {}) has been created asynchronously",
                ss->get_last_log_term(), ss->get_last_log_idx());
  });
}

}  // namespace Ctld

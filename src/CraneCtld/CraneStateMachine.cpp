#include "CraneStateMachine.h"

namespace Ctld {

std::shared_ptr<buffer> CraneStateMachine::enc_log(CraneCtldOpType type,
                                                   const std::string &key,
                                                   const void *data,
                                                   size_t len) {
  // Encode from op_payload to Raft log.
  size_t total_size =
      sizeof(CraneCtldOpType) + key.capacity() + sizeof(uint64_t) + len;
  std::shared_ptr<buffer> ret = buffer::alloc(total_size);
  buffer_serializer bs(ret);

  // WARNING: We don't consider endian-safety in this example.
  bs.put_u8(type);
  bs.put_str(key);
  bs.put_u64(len);
  if (data) bs.put_raw(data, len);

  //  if (key == "NI")
  //    std::cout << "enc log： " << key << *(static_cast<const uint32_t
  //    *>(data))
  //              << std::endl;
  //  else if (key == "NDI")
  //    std::cout << "enc log： " << key << *(static_cast<const int64_t
  //    *>(data))
  //              << std::endl;
  //  else if (payload.len > 0) {
  //    std::string result(static_cast<const char *>(payload.data.get()),
  //                       payload.len);
  //    std::cout << result << std::endl;
  //  }

  //  std::cout << "enc log： " << (uint8_t)type << " " << key << " " << len
  //            << std::endl;

  return ret;
}

bool CraneStateMachine::dec_log(buffer &log, CraneCtldOpType *type,
                                std::string *key, std::vector<uint8_t> *value) {
  // Decode from Raft log
  buffer_serializer bs(log);
  *type = static_cast<CraneCtldOpType>(bs.get_u8());
  *key = bs.get_str();
  uint64_t len = bs.get_u64();
  if (len > 0) {
    value->resize(len);
    memcpy(value->data(), bs.get_raw(len), len);
  }

  //  uint32_t value1;
  //  int64_t value2;
  //  if (*key == "NI") {
  //    std::memcpy(&value1, value->data(), sizeof(uint32_t));
  //    std::cout << "dec log： " << *key << value1 << std::endl;
  //  } else if (*key == "NDI") {
  //    std::memcpy(&value2, value->data(), sizeof(int64_t));
  //    std::cout << "dec log： " << *key << value2 << std::endl;
  //  }
  //  else if (payload.len > 0) {
  //    std::string result(static_cast<const char *>(payload.data.get()),
  //                       payload.len);
  //    std::cout << result << std::endl;
  //  }

  //  std::cout << "dec log： " << type << " " << key << " " << len <<
  //  std::endl;

  return true;
}

bool CraneStateMachine::init(const std::string &db_path) {
  util::lock_guard lg(snapshots_lock_);
  m_snapshot_ = std::make_unique<snapshot_ctx>();

  if (g_config.CraneEmbeddedDbBackend == "Unqlite") {
#ifdef CRANE_HAVE_UNQLITE
    variable_db = std::make_unique<UnqliteDb>();
    fixed_db = std::make_unique<UnqliteDb>();
#else
    CRANE_ERROR(
        "Select unqlite as the embedded db but it's not been compiled.");
    return false;
#endif

  } else if (g_config.CraneEmbeddedDbBackend == "BerkeleyDB") {
#ifdef CRANE_HAVE_BERKELEY_DB
    variable_db = std::make_unique<BerkeleyDb>();
    fixed_db = std::make_unique<BerkeleyDb>();
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

  auto result = variable_db->Init(db_path + "var_raft");
  if (!result) return false;
  result = fixed_db->Init(db_path + "fix_raft");
  if (!result) return false;

  return true;
}

std::shared_ptr<buffer> CraneStateMachine::commit(const uint64_t log_idx,
                                                  buffer &data) {
  CraneCtldOpType type;
  std::string key;
  std::vector<uint8_t> value;
  dec_log(data, &type, &key, &value);

  switch (type) {
  case OP_VAR_STORE:
    OnStore(var_value_map_, key, std::move(value));
    break;
  case OP_VAR_DELETE:
    OnDelete(var_value_map_, key);
    break;
  case OP_FIX_STORE:
    OnStore(fix_value_map_, key, std::move(value));
    break;
  case OP_FIX_DELETE:
    OnDelete(fix_value_map_, key);
    break;
  default:
    break;
  }

  last_committed_idx_ = log_idx;

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
    it = new ValueMapType::iterator(var_value_map_.begin());
    user_snp_ctx = it;

    data_out = buffer::alloc(sizeof(CraneCtldOpType));
    buffer_serializer bs(data_out);
    bs.put_u8(OP_UNKNOWN);
    is_last_obj = false;
  } else if (obj_id == 1) {
    // Object ID = 1: start read var_value_map_
    if (*it == var_value_map_.end()) {
      *it = fix_value_map_.begin();  // switch value map

      data_out = buffer::alloc(sizeof(CraneCtldOpType));
      buffer_serializer bs(data_out);
      bs.put_u8(OP_FIX_STORE);
      is_last_obj = false;
      return 0;
    }

    data_out = enc_log(OP_VAR_STORE, (*it)->first, (*it)->second.data(),
                       (*it)->second.size());
    is_last_obj = false;
    user_snp_ctx = ++it;
  } else if (obj_id == 2) {
    // Object ID = 2: start read fix_value_map_
    if (*it == fix_value_map_.end()) {
      data_out = nullptr;
      is_last_obj = true;
      return 0;
    }

    data_out = enc_log(OP_FIX_STORE, (*it)->first, (*it)->second.data(),
                       (*it)->second.size());
    is_last_obj = false;
    user_snp_ctx = ++it;
  } else {
    return -1;
  }
  return 0;
}

void CraneStateMachine::save_logical_snp_obj(snapshot &s, uint64_t &obj_id,
                                             buffer &data, bool is_first_obj,
                                             bool is_last_obj) {
  if (obj_id == 0) {
    util::lock_guard lg(snapshots_lock_);
    // Object ID == 0: it contains dummy value, create snapshot context.
    std::shared_ptr<buffer> snp_buf = s.serialize();
    auto ss_ptr = snapshot::deserialize(*snp_buf);
    m_snapshot_ = std::make_unique<snapshot_ctx>(ss_ptr, s.get_last_log_idx());
    obj_id = 1;
  } else if (obj_id == 1) {
    // Object ID = 1: start write var_value_map_
    CraneCtldOpType type;
    std::string key;
    std::vector<uint8_t> value;

    dec_log(data, &type, &key, &value);
    if (type == OP_FIX_STORE) {
      obj_id = 2;
      return;
    }
    var_value_map_[key] = std::move(value);
  } else if (obj_id == 2) {
    // Object ID = 2: start write fix_value_map_
    if (is_last_obj) {
      std::shared_ptr<buffer> snp_buf = s.serialize();
      auto ss_ptr = snapshot::deserialize(*snp_buf);
      create_snapshot_internal(ss_ptr);

      std::cout << "snapshot (" << ss_ptr->get_last_log_term() << ", "
                << ss_ptr->get_last_log_idx() << ") has been created logically"
                << std::endl;

      return;
    }

    CraneCtldOpType type;
    std::string key;
    std::vector<uint8_t> value;

    dec_log(data, &type, &key, &value);
    fix_value_map_[key] = std::move(value);
  }
}

bool CraneStateMachine::apply_snapshot(snapshot &s) {
  util::lock_guard lg(snapshots_lock_);

  if (!m_snapshot_ || s.get_last_log_idx() != m_snapshot_->log_index)
    return false;

  std::expected<void, DbErrorCode> result;

  result = variable_db->IterateAllKv(
      [&](std::string &&key, std::vector<uint8_t> &&value) {
        var_value_map_[key] = std::move(value);
        return true;
      });

  if (!result) {
    CRANE_ERROR("Failed to apply snapshots from variable data!");
    return false;
  }

  result = fixed_db->IterateAllKv(
      [&](std::string &&key, std::vector<uint8_t> &&value) {
        var_value_map_[key] = std::move(value);
        return true;
      });

  if (!result) {
    CRANE_ERROR("Failed to apply snapshots from fixed data!");
    return false;
  }

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
    create_snapshot_sync(s, when_done);
  } else {
    // Create a snapshot in an asynchronous way (in a different thread).
    create_snapshot_async(s, when_done);
  }
}

CraneStateMachine::ValueMapType *CraneStateMachine::GetValueMapInstance(
    uint8_t db_index) {
  if (db_index == 0)
    return &var_value_map_;
  else if (db_index == 1)
    return &fix_value_map_;
  else
    return nullptr;
}

bool CraneStateMachine::OnStore(CraneStateMachine::ValueMapType &map,
                                const std::string &key,
                                std::vector<uint8_t> &&data) {
  if (!data.empty()) {
    map[key] = std::move(data);
    return true;
  } else
    return false;
}

bool CraneStateMachine::OnDelete(CraneStateMachine::ValueMapType &map,
                                 const std::string &key) {
  if (!map.contains(key)) return false;

  if (map.erase(key) == 0) {
    CRANE_ERROR("Failed to delete key {} from db", key);
    return false;
  }
  return true;
}

void CraneStateMachine::create_snapshot_internal(std::shared_ptr<snapshot> ss) {
  util::lock_guard lg(snapshots_lock_);
  m_snapshot_ = std::make_unique<snapshot_ctx>(ss, ss->get_last_log_idx());

  txn_id_t var_txn_id, fix_txn_id;

  g_embedded_db_client->BeginDbTransaction(variable_db.get(), &var_txn_id);
  variable_db->Clear(var_txn_id);
  for (const auto &[k, v] : var_value_map_) {
    variable_db->Store(var_txn_id, k, v.data(), v.size());
  }
  g_embedded_db_client->CommitDbTransaction(variable_db.get(), var_txn_id);

  g_embedded_db_client->BeginDbTransaction(fixed_db.get(), &fix_txn_id);
  fixed_db->Clear(fix_txn_id);
  for (const auto &[k, v] : fix_value_map_) {
    fixed_db->Store(fix_txn_id, k, v.data(), v.size());
  }
  g_embedded_db_client->CommitDbTransaction(fixed_db.get(), fix_txn_id);
}

void CraneStateMachine::create_snapshot_sync(
    snapshot &s,
    async_result<bool>::handler_type &when_done) {  // Clone snapshot from `s`.
  std::shared_ptr<buffer> snp_buf = s.serialize();
  std::shared_ptr<snapshot> ss = snapshot::deserialize(*snp_buf);
  create_snapshot_internal(ss);

  std::shared_ptr<std::exception> except(nullptr);
  bool ret = true;
  when_done(ret, except);

  std::cout << "snapshot (" << ss->get_last_log_term() << ", "
            << ss->get_last_log_idx() << ") has been created synchronously"
            << std::endl;
}

void CraneStateMachine::create_snapshot_async(
    snapshot &s,
    async_result<bool>::handler_type &when_done) {  // Clone snapshot from `s`.
  std::shared_ptr<buffer> snp_buf = s.serialize();
  std::shared_ptr<snapshot> ss = snapshot::deserialize(*snp_buf);

  // Note that this is a very naive and inefficient example
  // that creates a new thread for each snapshot creation.
  g_thread_pool->detach_task([this, ss, when_done]() {
    create_snapshot_internal(ss);

    std::shared_ptr<std::exception> except(nullptr);
    bool ret = true;
    when_done(ret, except);

    std::cout << "snapshot (" << ss->get_last_log_term() << ", "
              << ss->get_last_log_idx() << ") has been created asynchronously"
              << std::endl;
  });
}

}  // namespace Ctld

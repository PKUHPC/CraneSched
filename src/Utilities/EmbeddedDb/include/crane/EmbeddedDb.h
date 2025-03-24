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

#include <expected>
#include <filesystem>
#include <functional>

#include "crane/Logger.h"

#ifdef CRANE_HAVE_BERKELEY_DB
#  include <db_cxx.h>
#endif

#ifdef CRANE_HAVE_UNQLITE
#  include <unqlite.h>
#endif

namespace crane {
namespace Internal {

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

  virtual std::expected<void, DbErrorCode> Clear(txn_id_t txn_id) = 0;

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

  std::expected<void, DbErrorCode> Clear(txn_id_t txn_id) override;

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

  std::expected<void, DbErrorCode> Clear(txn_id_t txn_id) override;

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

}  // namespace Internal
}  // namespace crane
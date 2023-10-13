/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <absl/container/flat_hash_map.h>

#include "Lock.h"
#include "Pointer.h"

namespace util {

template <template <typename...> class MapType, typename key_t,
          typename value_t>
class AtomicHashMap {
 public:
  class MutexValue {
   private:
    value_t value_;
    mutable mutex mutex_;

   public:
    using ReadPtr = util::ScopeSharedPtr<value_t, mutex>;
    using WritePtr = util::ScopeExclusivePtr<value_t, mutex>;

    //    explicit MutexValue(const value_t& value) : value_(value) {}

    explicit MutexValue(value_t&& value) : value_(std::move(value)) {}

    MutexValue(MutexValue&& other) noexcept {
      lock_guard lock(other.mutex_);
      value_ = std::move(other.value_);
    }

    MutexValue& operator=(MutexValue&& other) noexcept {
      if (this != &other) {
        lock_guard lock(other.mutex_);
        value_ = std::move(other.value_);
      }
      return *this;
    }

    MutexValue(const MutexValue& other) = delete;
    MutexValue& operator=(const MutexValue& other) = delete;

    void lock() { mutex_.Lock(); }

    void unlock() { mutex_.Unlock(); }

    value_t* get_value_ptr_nolock() { return &value_; }

    mutex* get_lock_ptr() { return &mutex_; }

    void set_value(const value_t& new_value) {
      lock_guard lock(mutex_);
      value_ = new_value;
    }

    ReadPtr get_read_ptr() const {
      mutex_.Lock();
      return ReadPtr{&value_, &mutex_, [](mutex* mtx) { mtx->Unlock(); }};
    }

    WritePtr get_write_ptr() {
      mutex_.Lock();
      return WritePtr{&value_, &mutex_, [](mutex* mtx) { mtx->Unlock(); }};
    }
  };

  class CombinedLock {
   private:
    rw_mutex* read_associated_write_mutex_;
    mutex* atomic_mutex_;

   public:
    CombinedLock() = default;

    CombinedLock(rw_mutex* a, mutex* b)
        : read_associated_write_mutex_(a), atomic_mutex_(b) {}

    CombinedLock(const CombinedLock&) = delete;
    CombinedLock& operator=(const CombinedLock&) = delete;

    CombinedLock(CombinedLock&& val) noexcept {
      read_associated_write_mutex_ = val.read_associated_write_mutex_;
      atomic_mutex_ = val.atomic_mutex_;

      val.read_associated_write_mutex_ = nullptr;
      val.atomic_mutex_ = nullptr;
    }

    void lock() {
      read_associated_write_mutex_->lock_shared();
      atomic_mutex_->Lock();
    }

    void unlock() {
      atomic_mutex_->Unlock();
      read_associated_write_mutex_->unlock_shared();
    }

    void unlock_shared() { unlock(); }
  };

  using ValueReadMutexSharedPtr = util::ScopeSharedPtr<value_t, CombinedLock>;
  using ValueWriteMutexExclusivePtr =
      util::ScopeExclusivePtr<value_t, CombinedLock>;

  using MapMutexSharedPtr =
      util::ScopeSharedPtr<MapType<key_t, MutexValue>, rw_mutex>;
  using MapMutexExclusivePtr =
      util::ScopeExclusivePtr<MapType<key_t, MutexValue>, rw_mutex>;

  AtomicHashMap() = default;

  void InitFromMap(MapType<key_t, value_t>&& other_map) {
    m_value_map_.reserve(other_map.size());
    for (auto& [k, v] : other_map) {
      // Using the emplace method and std::move for movement construction in
      // site
      m_value_map_.emplace(k, std::move(v));
    }
  }

  bool contains(const key_t& key) {
    read_lock_guard lock_guard(m_global_rw_mutex_);
    return m_value_map_.contains(key);
  }

  ValueReadMutexSharedPtr get_read_ptr(const key_t& key) {
    m_global_rw_mutex_.lock_shared();
    auto iter = m_value_map_.find(key);

    if (iter == m_value_map_.end()) {
      m_global_rw_mutex_.unlock_shared();
      return ValueReadMutexSharedPtr{nullptr};
    } else {
      iter->second.lock();
      CombinedLock combined_lock(&m_global_rw_mutex_,
                                 iter->second.get_lock_ptr());
      return ValueReadMutexSharedPtr{iter->second.get_value_ptr_nolock(),
                                     std::move(combined_lock)};
    }
  }

  bool get_copy(const key_t& key, value_t* value) {
    m_global_rw_mutex_.lock_shared();
    auto iter = m_value_map_.find(key);

    if (iter == m_value_map_.end()) {
      m_global_rw_mutex_.unlock_shared();
      value = nullptr;
      return false;
    } else {
      iter->second.lock();
      *value = *(iter->second.get_value_ptr_nolock());  // copy value
      m_global_rw_mutex_.unlock_shared();
      return true;
    }
  }

  ValueWriteMutexExclusivePtr operator[](const key_t& key) {
    m_global_rw_mutex_.lock_shared();
    auto iter = m_value_map_.find(key);

    if (iter == m_value_map_.end()) {
      m_global_rw_mutex_.unlock_shared();
      return ValueWriteMutexExclusivePtr{nullptr};
    } else {
      iter->second.lock();
      CombinedLock combined_lock(&m_global_rw_mutex_,
                                 iter->second.get_lock_ptr());
      return ValueWriteMutexExclusivePtr{iter->second.get_value_ptr_nolock(),
                                         std::move(combined_lock)};
    }
  }

  MapMutexSharedPtr get_map_read() {
    m_global_rw_mutex_.lock_shared();
    return MapMutexSharedPtr{&m_value_map_, &m_global_rw_mutex_};
  }

  MapMutexExclusivePtr get_map_write() {
    m_global_rw_mutex_.lock();
    return MapMutexExclusivePtr{&m_value_map_, &m_global_rw_mutex_};
  }

 private:
  MapType<key_t, MutexValue> m_value_map_;

  rw_mutex m_global_rw_mutex_;
};

}  // namespace util

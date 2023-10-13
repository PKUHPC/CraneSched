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

namespace util {

/**
 *
 * @tparam T is the type of the stored pointer.
 * @tparam Lockable must have lock() and unlock()
 */
template <typename T, typename Lockable>
class ScopeExclusivePtr {
 public:
  explicit ScopeExclusivePtr(
      T* data, Lockable* lock = nullptr,
      std::function<void(Lockable*)> unlock_func =
          [](Lockable* mutex) { mutex->unlock(); }) noexcept
      : data_(data), lock_(lock), unlock_func_(unlock_func) {}

  explicit ScopeExclusivePtr(
      T* data, Lockable&& lock,
      std::function<void(Lockable*)> unlock_func =
          [](Lockable* mutex) { mutex->unlock(); }) noexcept
      : data_(data),
        own_lock_(std::move(lock)),
        lock_(&own_lock_),
        unlock_func_(unlock_func) {}

  ~ScopeExclusivePtr() noexcept {
    if (lock_) {
      unlock_func_(lock_);
    }
  }

  T* get() { return data_; }
  T& operator*() { return *data_; }
  T* operator->() { return data_; }

  explicit operator bool() { return data_ != nullptr; }

  ScopeExclusivePtr(ScopeExclusivePtr const&) = delete;
  ScopeExclusivePtr& operator=(ScopeExclusivePtr const&) = delete;

  ScopeExclusivePtr(ScopeExclusivePtr&& val) noexcept {
    data_ = val.data_;
    lock_ = val.lock_;
    own_lock_ = std::move(val.own_lock_);
    unlock_func_ = val.unlock_func_;
    val.lock_ = nullptr;
  }

 private:
  T* data_;
  Lockable* lock_;
  Lockable own_lock_;
  std::function<void(Lockable*)> unlock_func_;
};

/**
 * @note remember to limit the lifetime of this variable to prevent the lock
 * from not being released in time
 * @tparam T is the type of the stored pointer.
 * @tparam Lockable must have lock_shared() and unlock_shared()
 */
template <typename T, typename Lockable>
class ScopeSharedPtr {
 public:
  explicit ScopeSharedPtr(
      const T* data, Lockable* lock = nullptr,
      std::function<void(Lockable*)> unlock_func =
          [](Lockable* mutex) { mutex->unlock_shared(); }) noexcept
      : data_(data), lock_(lock), unlock_func_(unlock_func) {}

  explicit ScopeSharedPtr(
      const T* data, Lockable&& lock,
      std::function<void(Lockable*)> unlock_func =
          [](Lockable* mutex) { mutex->unlock_shared(); }) noexcept
      : data_(data),
        own_lock_(std::move(lock)),
        lock_(&own_lock_),
        unlock_func_(unlock_func) {}

  ~ScopeSharedPtr() noexcept {
    if (lock_) {
      unlock_func_(lock_);
    }
  }

  const T* get() { return data_; }
  const T& operator*() { return *data_; }
  const T* operator->() { return data_; }

  explicit operator bool() { return data_ != nullptr; }

  ScopeSharedPtr(ScopeSharedPtr const&) = delete;
  ScopeSharedPtr& operator=(ScopeSharedPtr const&) = delete;

  ScopeSharedPtr(ScopeSharedPtr&& val) noexcept {
    data_ = val.data_;
    lock_ = val.lock_;
    own_lock_ = std::move(val.own_lock_);
    unlock_func_ = val.unlock_func_;
    val.lock_ = nullptr;
  }

 private:
  const T* data_;
  Lockable* lock_;
  Lockable own_lock_;
  std::function<void(Lockable*)> unlock_func_;
};

}  // namespace util
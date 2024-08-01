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

template <typename T>
concept StdUnlockable = requires(T t) {
  { t.unlock() } -> std::same_as<void>;
};

template <typename T>
concept StdSharedUnlockable = StdUnlockable<T> and requires(T t) {
  { t.unlock_shared() } -> std::same_as<void>;
};

template <typename T>
concept AbslUnlockable = requires(T t) {
  { t.Unlock() } -> std::same_as<void>;
};

template <typename T, typename Unlockable>
  requires StdUnlockable<Unlockable> || AbslUnlockable<Unlockable>
class ScopeExclusivePtr {
 public:
  explicit ScopeExclusivePtr(T* data, Unlockable* lock = nullptr) noexcept
      : data_(data), lock_(lock) {}

  ~ScopeExclusivePtr() noexcept {
    if (lock_) {
      if constexpr (StdUnlockable<Unlockable>)
        lock_->unlock();
      else  // AbslUnlockable
        lock_->Unlock();
    }
  }

  T* get() const { return data_; }
  T& operator*() const { return *data_; }
  T* operator->() const { return data_; }

  explicit operator bool() { return data_ != nullptr; }

  ScopeExclusivePtr(ScopeExclusivePtr const&) = delete;
  ScopeExclusivePtr& operator=(ScopeExclusivePtr const&) = delete;

  ScopeExclusivePtr(ScopeExclusivePtr&& val) noexcept {
    data_ = val.data_;
    lock_ = val.lock_;
    val.data_ = nullptr;
    val.lock_ = nullptr;
  }

  ScopeExclusivePtr& operator=(ScopeExclusivePtr&& val) noexcept {
    if (this != &val) {
      data_ = val.data_;
      lock_ = val.lock_;
      val.data_ = nullptr;
      val.lock_ = nullptr;
    }
    return *this;
  };

 private:
  T* data_;
  Unlockable* lock_;
};

template <typename T, typename Unlockable>
  requires StdSharedUnlockable<Unlockable>
class ScopeSharedPtr {
 public:
  explicit ScopeSharedPtr(T* data, Unlockable* lock = nullptr) noexcept
      : data_(data), lock_(lock) {}

  ~ScopeSharedPtr() noexcept {
    if (lock_) {
      lock_->unlock_shared();
    }
  }

  T* get() { return data_; }
  T& operator*() { return *data_; }
  T* operator->() { return data_; }

  explicit operator bool() { return data_ != nullptr; }

  ScopeSharedPtr(ScopeSharedPtr&) = delete;
  ScopeSharedPtr& operator=(ScopeSharedPtr&) = delete;

  ScopeSharedPtr(ScopeSharedPtr&& val) noexcept {
    data_ = val.data_;
    lock_ = val.lock_;
    val.data_ = nullptr;
    val.lock_ = nullptr;
  }

 private:
  T* data_;
  Unlockable* lock_;
};

template <typename T, typename Unlockable>
  requires StdSharedUnlockable<Unlockable>
class ScopeConstSharedPtr {
 public:
  explicit ScopeConstSharedPtr(const T* data,
                               Unlockable* lock = nullptr) noexcept
      : data_(data), lock_(lock) {}

  ~ScopeConstSharedPtr() noexcept {
    if (lock_) {
      lock_->unlock_shared();
    }
  }

  const T* get() const { return data_; }
  const T& operator*() const { return *data_; }
  const T* operator->() const { return data_; }

  explicit operator bool() const { return data_ != nullptr; }

  ScopeConstSharedPtr(ScopeConstSharedPtr const&) = delete;
  ScopeConstSharedPtr& operator=(ScopeConstSharedPtr const&) = delete;

  ScopeConstSharedPtr(ScopeConstSharedPtr&& val) noexcept {
    data_ = val.data_;
    lock_ = val.lock_;
    val.data_ = nullptr;
    val.lock_ = nullptr;
  }

 private:
  const T* data_;
  Unlockable* lock_;
};

template <typename T, typename Unlockable>
  requires StdUnlockable<Unlockable> || AbslUnlockable<Unlockable>
class ManagedScopeExclusivePtr {
 public:
  explicit ManagedScopeExclusivePtr() noexcept : data_(nullptr) {}

  ManagedScopeExclusivePtr(ManagedScopeExclusivePtr const&) = delete;
  ManagedScopeExclusivePtr& operator=(ManagedScopeExclusivePtr const&) = delete;

  ManagedScopeExclusivePtr(T* data, Unlockable&& lock) noexcept
      : data_(data), lock_(std::move(lock)) {}
  ManagedScopeExclusivePtr& operator=(ManagedScopeExclusivePtr&& val) noexcept {
    if (this != &val) {
      data_ = val.data_;
      val.data_ = nullptr;
      lock_ = std::move(val.lock_);
    }
    return *this;
  };

  ~ManagedScopeExclusivePtr() noexcept {
    if (data_ == nullptr) return;

    if constexpr (StdUnlockable<Unlockable>)
      lock_.unlock();
    else  // AbslUnlockable
      lock_.Unlock();
  }

  T* get() const { return data_; }
  T& operator*() const { return *data_; }
  T* operator->() const { return data_; }

  explicit operator bool() { return data_ != nullptr; }

  ManagedScopeExclusivePtr(ManagedScopeExclusivePtr&& val) noexcept {
    data_ = val.data_;
    val.data_ = nullptr;
    lock_ = std::move(val.lock_);
  }

 private:
  T* data_;
  Unlockable lock_;
};

}  // namespace util
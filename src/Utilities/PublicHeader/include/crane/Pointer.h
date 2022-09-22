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
  explicit ScopeExclusivePtr(T* data, Lockable* lock = nullptr) noexcept
      : data_(data), lock_(lock) {}

  ~ScopeExclusivePtr() noexcept {
    if (lock_) {
      lock_->unlock();
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
    val.lock_ = nullptr;
  }

 private:
  T* data_;
  Lockable* lock_;
};

}  // namespace util
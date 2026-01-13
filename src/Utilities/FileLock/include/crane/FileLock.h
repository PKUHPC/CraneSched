/**
 * Copyright (c) 2025 Peking University and Peking University
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

#include <fcntl.h>
#include <spdlog/spdlog.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <expected>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <system_error>
#include <unordered_map>

namespace util {

enum class FileLockType : std::uint8_t {
  READ,   // Shared lock (F_RDLCK)
  WRITE,  // Exclusive lock (F_WRLCK)
};

namespace detail {

// Internal structure to hold file lock information
struct FileLockInfo {
  int fd{-1};
  std::filesystem::path path;
  FileLockType lock_type{FileLockType::READ};
  size_t ref_count{0};
  bool ready{false};
  std::string init_error;
  std::mutex mtx;  // Protects ref_count/state and lock operations
  std::condition_variable cv;

  FileLockInfo() = default;
  FileLockInfo(int fd_, std::filesystem::path path_, FileLockType type_)
      : fd(fd_),
        path(std::move(path_)),
        lock_type(type_),
        ref_count(1),
        ready(true) {}

  ~FileLockInfo() {
    if (fd >= 0) {
      close(fd);
    }
  }

  FileLockInfo(const FileLockInfo&) = delete;
  FileLockInfo& operator=(const FileLockInfo&) = delete;
};

// Global registry to manage file locks with reference counting
class FileLockRegistry {
 public:
  static FileLockRegistry& Instance() {
    static FileLockRegistry instance;
    return instance;
  }

  std::expected<std::shared_ptr<FileLockInfo>, std::string> AcquireLock(
      const std::filesystem::path& path, FileLockType lock_type,
      bool blocking) {
    std::error_code ec;
    auto normalized_path =
        std::filesystem::absolute(path, ec).lexically_normal();
    if (ec) {
      return std::unexpected(
          std::format("Failed to resolve absolute path for '{}': {}",
                      path.string(), ec.message()));
    }
    std::string path_str = normalized_path.string();

    while (true) {
      std::shared_ptr<FileLockInfo> lock_info;

      {
        std::unique_lock<std::mutex> guard(m_registry_mtx_);

        // Check if lock already exists
        auto it = m_locks_.find(path_str);
        if (it != m_locks_.end()) {
          lock_info = it->second;

          // Check if lock type is compatible
          if (lock_info->lock_type != lock_type) {
            return std::unexpected(std::format(
                "Lock type mismatch for file '{}': existing={}, requested={}",
                path_str,
                lock_info->lock_type == FileLockType::READ ? "Read" : "Write",
                lock_type == FileLockType::READ ? "Read" : "Write"));
          }

          std::unique_lock<std::mutex> lock_guard(lock_info->mtx);
          if (lock_info->ready) {
            if (!lock_info->init_error.empty()) {
              return std::unexpected(lock_info->init_error);
            }
            lock_info->ref_count++;
            SPDLOG_TRACE("Incremented lock reference count for '{}' to {}",
                         path_str, lock_info->ref_count);
            return lock_info;
          }

          // Wait for in-progress initialization without holding registry lock
          guard.unlock();
          while (!lock_info->ready) {
            lock_info->cv.wait(lock_guard);
          }
          if (!lock_info->init_error.empty()) {
            return std::unexpected(lock_info->init_error);
          }
          // Retry to safely increment ref_count under registry lock
          continue;
        }

        // Create placeholder entry to avoid blocking with registry mutex held
        lock_info = std::make_shared<FileLockInfo>();
        lock_info->path = normalized_path;
        lock_info->lock_type = lock_type;
        lock_info->ref_count = 0;
        lock_info->ready = false;
        lock_info->init_error.clear();
        m_locks_[path_str] = lock_info;
      }

      auto init_failed = [&](const std::string& message)
          -> std::expected<std::shared_ptr<FileLockInfo>, std::string> {
        {
          std::lock_guard<std::mutex> lock_guard(lock_info->mtx);
          lock_info->init_error = message;
          lock_info->ready = true;
        }
        lock_info->cv.notify_all();

        std::lock_guard<std::mutex> guard(m_registry_mtx_);
        auto it = m_locks_.find(path_str);
        if (it != m_locks_.end() && it->second == lock_info) {
          m_locks_.erase(it);
        }
        return std::unexpected(message);
      };

      // Create new lock with security flags
      // O_NOFOLLOW: Don't follow symbolic links (prevents TOCTOU attacks)
      // O_CLOEXEC: Close on exec (prevents fd leaks to child processes)
      int open_flags = (lock_type == FileLockType::READ) ? O_RDONLY : O_RDWR;
      open_flags |= O_CREAT | O_NOFOLLOW | O_CLOEXEC;
      int fd = open(normalized_path.c_str(), open_flags, 0666);
      if (fd < 0) {
        return init_failed(std::format("Failed to open file '{}': {}", path_str,
                                       strerror(errno)));
      }

      // Acquire fcntl lock
      struct flock fl{};
      fl.l_type = (lock_type == FileLockType::READ) ? F_RDLCK : F_WRLCK;
      fl.l_whence = SEEK_SET;
      fl.l_start = 0;
      fl.l_len = 0;  // Lock entire file

      int cmd = blocking ? F_SETLKW : F_SETLK;
      if (fcntl(fd, cmd, &fl) == -1) {
        int err = errno;
        close(fd);
        return init_failed(
            std::format("Failed to acquire {} lock on '{}': {}",
                        lock_type == FileLockType::READ ? "read" : "write",
                        path_str, strerror(err)));
      }

      {
        std::lock_guard<std::mutex> lock_guard(lock_info->mtx);
        lock_info->fd = fd;
        lock_info->ref_count = 1;
        lock_info->ready = true;
      }
      lock_info->cv.notify_all();

      SPDLOG_TRACE("Acquired {} lock on '{}' (fd={})",
                   lock_type == FileLockType::READ ? "read" : "write", path_str,
                   fd);

      return lock_info;
    }
  }

  void ReleaseLock(const std::shared_ptr<FileLockInfo>& lock_info) {
    if (!lock_info) return;

    std::lock_guard<std::mutex> guard(m_registry_mtx_);

    std::string path_str = lock_info->path.string();
    auto it = m_locks_.find(path_str);
    if (it == m_locks_.end()) {
      SPDLOG_WARN("Attempted to release non-existent lock for '{}'", path_str);
      return;
    }

    std::lock_guard<std::mutex> lock_guard(lock_info->mtx);

    if (lock_info->ref_count == 0) {
      SPDLOG_WARN("Lock reference count already 0 for '{}'", path_str);
      return;
    }

    lock_info->ref_count--;
    SPDLOG_TRACE("Decremented lock reference count for '{}' to {}", path_str,
                 lock_info->ref_count);

    if (lock_info->ref_count == 0) {
      // Release fcntl lock
      struct flock fl{};
      fl.l_type = F_UNLCK;
      fl.l_whence = SEEK_SET;
      fl.l_start = 0;
      fl.l_len = 0;

      if (fcntl(lock_info->fd, F_SETLK, &fl) == -1) {
        SPDLOG_ERROR("Failed to release fcntl lock on '{}': {}", path_str,
                     strerror(errno));
      }

      m_locks_.erase(it);
      SPDLOG_TRACE("Released and removed lock for '{}'", path_str);
    }
  }

  FileLockRegistry(const FileLockRegistry&) = delete;
  FileLockRegistry& operator=(const FileLockRegistry&) = delete;
  FileLockRegistry(FileLockRegistry&&) = delete;
  FileLockRegistry& operator=(FileLockRegistry&&) = delete;

 private:
  FileLockRegistry() = default;
  ~FileLockRegistry() = default;

  std::mutex m_registry_mtx_;
  std::unordered_map<std::string, std::shared_ptr<FileLockInfo>> m_locks_;
};

}  // namespace detail

// RAII wrapper for file locks
class FileLockGuard {
 public:
  // Acquire a file lock (blocking by default)
  [[nodiscard]] static std::expected<FileLockGuard, std::string> Acquire(
      const std::filesystem::path& path, FileLockType lock_type,
      bool blocking = true) {
    auto result = detail::FileLockRegistry::Instance().AcquireLock(
        path, lock_type, blocking);
    if (!result) {
      return std::unexpected(result.error());
    }
    return FileLockGuard(std::move(result.value()));
  }

  ~FileLockGuard() { Release(); }

  // Move constructor and assignment
  FileLockGuard(FileLockGuard&& other) noexcept
      : m_lock_info_(std::move(other.m_lock_info_)) {
    other.m_lock_info_ = nullptr;
  }

  FileLockGuard& operator=(FileLockGuard&& other) noexcept {
    if (this != &other) {
      Release();
      m_lock_info_ = std::move(other.m_lock_info_);
      other.m_lock_info_ = nullptr;
    }
    return *this;
  }

  // Delete copy constructor and assignment
  FileLockGuard(const FileLockGuard&) = delete;
  FileLockGuard& operator=(const FileLockGuard&) = delete;

  // Manually release the lock before destruction
  void Release() {
    if (m_lock_info_) {
      detail::FileLockRegistry::Instance().ReleaseLock(m_lock_info_);
      m_lock_info_ = nullptr;
    }
  }

  // Check if the guard holds a valid lock
  explicit operator bool() const { return m_lock_info_ != nullptr; }

  // Get the locked file path
  std::filesystem::path GetPath() const {
    return m_lock_info_ ? m_lock_info_->path : std::filesystem::path();
  }

  // Get the lock type
  FileLockType GetLockType() const {
    return m_lock_info_ ? m_lock_info_->lock_type : FileLockType::READ;
  }

  // Write metadata to the lock file (e.g., PID, timestamp, hostname)
  // The file will be truncated before writing
  std::expected<void, std::string> WriteMetadata(std::string_view data) {
    if (!m_lock_info_) {
      return std::unexpected("No active lock");
    }

    if (m_lock_info_->lock_type != FileLockType::WRITE) {
      return std::unexpected("WriteMetadata requires a write lock");
    }

    std::lock_guard<std::mutex> lock_guard(m_lock_info_->mtx);

    // Truncate file to 0 and seek to beginning
    if (ftruncate(m_lock_info_->fd, 0) == -1) {
      return std::unexpected(
          std::format("Failed to truncate file: {}", strerror(errno)));
    }

    if (lseek(m_lock_info_->fd, 0, SEEK_SET) == -1) {
      return std::unexpected(
          std::format("Failed to seek to beginning: {}", strerror(errno)));
    }

    // Write data
    ssize_t written = write(m_lock_info_->fd, data.data(), data.size());
    if (written == -1) {
      return std::unexpected(
          std::format("Failed to write metadata: {}", strerror(errno)));
    }

    if (static_cast<size_t>(written) != data.size()) {
      return std::unexpected(
          std::format("Partial write: {} of {} bytes", written, data.size()));
    }

    // Ensure data is flushed to disk
    if (fsync(m_lock_info_->fd) == -1) {
      SPDLOG_WARN("Failed to sync lock file: {}", strerror(errno));
    }

    return {};
  }

  // Read metadata from the lock file
  std::expected<std::string, std::string> ReadMetadata() const {
    if (!m_lock_info_) {
      return std::unexpected("No active lock");
    }

    std::lock_guard<std::mutex> lock_guard(m_lock_info_->mtx);

    // Get file size
    struct stat st{};
    if (fstat(m_lock_info_->fd, &st) == -1) {
      return std::unexpected(
          std::format("Failed to stat file: {}", strerror(errno)));
    }

    if (st.st_size == 0) {
      return std::string();  // Empty file
    }

    // Seek to beginning
    if (lseek(m_lock_info_->fd, 0, SEEK_SET) == -1) {
      return std::unexpected(
          std::format("Failed to seek to beginning: {}", strerror(errno)));
    }

    // Read data
    std::string data(st.st_size, '\0');
    ssize_t bytes_read = read(m_lock_info_->fd, data.data(), st.st_size);
    if (bytes_read == -1) {
      return std::unexpected(
          std::format("Failed to read metadata: {}", strerror(errno)));
    }

    data.resize(bytes_read);
    return data;
  }

  // Get the file descriptor (use with caution!)
  // Note: Do NOT close this fd manually, it's managed by the lock
  int GetFileDescriptor() const { return m_lock_info_ ? m_lock_info_->fd : -1; }

 private:
  explicit FileLockGuard(std::shared_ptr<detail::FileLockInfo> lock_info)
      : m_lock_info_(std::move(lock_info)) {}

  std::shared_ptr<detail::FileLockInfo> m_lock_info_;
};

// Convenience functions for acquiring locks
inline std::expected<FileLockGuard, std::string> AcquireReadLock(
    const std::filesystem::path& path, bool blocking = true) {
  return FileLockGuard::Acquire(path, FileLockType::READ, blocking);
}

inline std::expected<FileLockGuard, std::string> AcquireWriteLock(
    const std::filesystem::path& path, bool blocking = true) {
  return FileLockGuard::Acquire(path, FileLockType::WRITE, blocking);
}

}  // namespace util

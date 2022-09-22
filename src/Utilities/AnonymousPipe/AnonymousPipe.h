#pragma once

#include <sys/socket.h>
#include <unistd.h>

#include "crane/PublicHeader.h"

class AnonymousPipe {
 public:
  AnonymousPipe() noexcept
      : m_fd_(), m_child_end_invalid_(false), m_parent_end_invalid_(false) {
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, m_fd_) != 0) {
      m_child_end_invalid_ = m_parent_end_invalid_ = true;
      CRANE_ERROR("Failed to create AnonymousPipe: {}", strerror(errno));
    }
  }

  ~AnonymousPipe() noexcept = default;

  template <typename Integer>
  [[nodiscard]] bool WriteIntegerToParent(Integer val) {
    static_assert(std::is_integral<Integer>(),
                  "WriteInteger only accepts integral types.");

    return !m_child_end_invalid_ &&
           write(m_fd_[1], &val, sizeof(Integer)) == sizeof(Integer);
  }

  template <typename Integer>
  [[nodiscard]] bool ReadIntegerFromParent(Integer* val) {
    static_assert(std::is_integral<Integer>(),
                  "ReadInteger only accepts integral types.");

    return !m_child_end_invalid_ &&
           read(m_fd_[1], val, sizeof(Integer)) == sizeof(Integer);
  }

  [[nodiscard]] bool ReadBytesFromParent(void* buf, size_t n) {
    return !m_child_end_invalid_ && read(m_fd_[1], buf, n) == n;
  }

  [[nodiscard]] bool WriteBytesToParent(const void* buf, size_t n) {
    return !m_child_end_invalid_ && write(m_fd_[1], buf, n) == n;
  }

  template <typename Integer>
  [[nodiscard]] bool WriteIntegerToChild(Integer val) {
    static_assert(std::is_integral<Integer>(),
                  "WriteInteger only accepts integral types.");

    return !m_parent_end_invalid_ &&
           write(m_fd_[0], &val, sizeof(Integer)) == sizeof(Integer);
  }

  template <typename Integer>
  [[nodiscard]] bool ReadIntegerFromChild(Integer* val) {
    static_assert(std::is_integral<Integer>(),
                  "ReadInteger only accepts integral types.");

    return !m_parent_end_invalid_ &&
           read(m_fd_[0], val, sizeof(Integer)) == sizeof(Integer);
  }

  [[nodiscard]] bool WriteBytesToChild(const void* buf, size_t n) {
    return !m_parent_end_invalid_ && write(m_fd_[0], buf, n) == n;
  }

  [[nodiscard]] bool ReadBytesFromChild(void* buf, size_t n) {
    return !m_parent_end_invalid_ && read(m_fd_[0], buf, n) == n;
  }

  bool CloseParentEnd() {
    if (!m_parent_end_invalid_) {
      m_parent_end_invalid_ = true;
      if (close(m_fd_[0]) != 0) {
        CRANE_ERROR("Failed to close the parent end of AnonymousPipe: {}",
                    strerror(errno));
        return false;
      }
      return true;
    } else
      return true;
  }

  bool CloseChildEnd() {
    if (!m_child_end_invalid_) {
      m_child_end_invalid_ = true;
      if (close(m_fd_[1]) != 0) {
        CRANE_ERROR("Failed to close the child end of AnonymousPipe: {}",
                    strerror(errno));
        return false;
      } else
        return true;
    } else
      return true;
  }

  int GetParentEndFd() { return m_fd_[0]; }
  int GetChildEndFd() { return m_fd_[1]; }

  [[nodiscard]] bool IsChildEndInvalid() const { return m_child_end_invalid_; }
  [[nodiscard]] bool IsParentEndInvalid() const {
    return m_parent_end_invalid_;
  }

 private:
  int m_fd_[2];
  bool m_child_end_invalid_;
  bool m_parent_end_invalid_;
};

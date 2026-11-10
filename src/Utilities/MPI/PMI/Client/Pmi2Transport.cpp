/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 */

#include "Pmi2Transport.h"

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cstring>

#include "Pmi2Codec.h"

namespace crane::pmi2 {

Transport::Transport(int fd, std::chrono::seconds timeout)
    : m_fd_(fd),
      m_timeout_(
          std::chrono::duration_cast<std::chrono::milliseconds>(timeout)) {}

Transport::~Transport() { Close(); }

bool Transport::WriteAll(const void* data, size_t size) {
  const auto* bytes = static_cast<const char*>(data);
  while (size > 0) {
    pollfd descriptor{m_fd_, POLLOUT, 0};
    int wait = poll(&descriptor, 1, static_cast<int>(m_timeout_.count()));
    if (wait <= 0 || (descriptor.revents & (POLLERR | POLLHUP | POLLNVAL)))
      return false;
    ssize_t written = send(m_fd_, bytes, size, MSG_NOSIGNAL);
    if (written > 0) {
      bytes += written;
      size -= static_cast<size_t>(written);
    } else if (written < 0 && errno != EINTR && errno != EAGAIN &&
               errno != EWOULDBLOCK) {
      return false;
    }
  }
  return true;
}

bool Transport::ReadAll(void* data, size_t size) {
  auto* bytes = static_cast<char*>(data);
  while (size > 0) {
    pollfd descriptor{m_fd_, POLLIN, 0};
    int wait = poll(&descriptor, 1, static_cast<int>(m_timeout_.count()));
    if (wait <= 0 || (descriptor.revents & (POLLERR | POLLNVAL))) return false;
    ssize_t received = recv(m_fd_, bytes, size, 0);
    if (received > 0) {
      bytes += received;
      size -= static_cast<size_t>(received);
    } else if (received == 0) {
      return false;
    } else if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
      return false;
    }
  }
  return true;
}

bool Transport::SendInit(std::string_view line) {
  return WriteAll(line.data(), line.size());
}

std::optional<std::string> Transport::ReceiveInit() {
  std::string result;
  result.reserve(256);
  while (result.size() <= 1024) {
    char ch;
    if (!ReadAll(&ch, 1)) return std::nullopt;
    result.push_back(ch);
    if (ch == '\n') return result;
  }
  return std::nullopt;
}

bool Transport::SendFrame(std::string_view body) {
  if (body.empty() || body.size() > Codec::kMaxFrame) return false;
  std::string header = std::to_string(body.size());
  if (header.size() > 6) return false;
  header.insert(header.begin(), 6 - header.size(), ' ');
  return WriteAll(header.data(), header.size()) &&
         WriteAll(body.data(), body.size());
}

std::optional<std::string> Transport::ReceiveFrame() {
  char header[6];
  if (!ReadAll(header, sizeof(header))) return std::nullopt;
  std::string_view length_text(header, sizeof(header));
  while (!length_text.empty() && length_text.front() == ' ')
    length_text.remove_prefix(1);
  while (!length_text.empty() && length_text.back() == ' ')
    length_text.remove_suffix(1);
  size_t length = 0;
  auto [end, error] = std::from_chars(
      length_text.data(), length_text.data() + length_text.size(), length);
  if (error != std::errc{} || end != length_text.data() + length_text.size() ||
      length == 0 || length > Codec::kMaxFrame)
    return std::nullopt;
  std::string body(length, '\0');
  if (!ReadAll(body.data(), body.size())) return std::nullopt;
  return body;
}

void Transport::Close() {
  if (m_fd_ >= 0) {
    shutdown(m_fd_, SHUT_RDWR);
    close(m_fd_);
    m_fd_ = -1;
  }
}

}  // namespace crane::pmi2

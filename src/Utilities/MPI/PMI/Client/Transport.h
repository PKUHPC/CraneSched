/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 */

#pragma once

#include <chrono>
#include <cstddef>
#include <optional>
#include <string>
#include <string_view>

namespace crane::pmi {

class Transport final {
 public:
  explicit Transport(int fd, std::chrono::seconds timeout);
  ~Transport();
  Transport(const Transport&) = delete;
  Transport& operator=(const Transport&) = delete;

  bool SendLine(std::string_view line);
  std::optional<std::string> ReceiveLine(size_t limit = 1024);
  bool SendFrame(std::string_view body);
  std::optional<std::string> ReceiveFrame();
  void Close();

 private:
  bool WriteAll(const void* data, size_t size);
  bool ReadAll(void* data, size_t size);

  std::string m_input_;
  int m_fd_;
  std::chrono::milliseconds m_timeout_;
};

}  // namespace crane::pmi

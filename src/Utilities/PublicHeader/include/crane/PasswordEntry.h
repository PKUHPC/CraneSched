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

#include <pwd.h>

#include <stdexcept>
#include <string>
#include <cstdlib>

#include "crane/Logger.h"
#include "shadow/subid.h"

// For older libsubid versions, we need to include additional headers for file parsing
#ifdef SUBID_ABI_MAJOR
  #if SUBID_ABI_MAJOR < 4
    #include <fstream>
    #include <sstream>
    #include <vector>
    #include <cstdlib>
  #endif
#else
  // If SUBID_ABI_MAJOR is not defined, assume it's a very old version
  #include <fstream>
  #include <sstream>
  #include <vector>
  #include <cstdlib>
#endif

// Compatibility layer for older libsubid versions
namespace {

#if !defined(SUBID_ABI_MAJOR) || SUBID_ABI_MAJOR < 4

// Helper function to parse subid files for older libsubid versions
inline int parse_subid_file(const char* filename, const char* username, struct subid_range** ranges) {
  if (!filename || !username || !ranges) {
    return -1;
  }
  
  std::ifstream file(filename);
  if (!file.is_open()) {
    return 0; // File doesn't exist or can't be opened, return 0 ranges
  }
  
  std::vector<subid_range> temp_ranges;
  std::string line;
  
  while (std::getline(file, line)) {
    if (line.empty() || line[0] == '#') {
      continue; // Skip empty lines and comments
    }
    
    // Parse colon-separated format: user:start:count
    size_t first_colon = line.find(':');
    if (first_colon == std::string::npos) continue;
    
    size_t second_colon = line.find(':', first_colon + 1);
    if (second_colon == std::string::npos) continue;
    
    std::string user = line.substr(0, first_colon);
    std::string start_str = line.substr(first_colon + 1, second_colon - first_colon - 1);
    std::string count_str = line.substr(second_colon + 1);
    
    if (user == username) {
      unsigned long start = std::stoul(start_str);
      unsigned long count = std::stoul(count_str);
      temp_ranges.push_back({start, count});
    }
  }
  
  if (temp_ranges.empty()) {
    *ranges = nullptr;
    return 0;
  }
  
  // Allocate memory for the ranges (compatible with what subid_free would expect)
  *ranges = static_cast<struct subid_range*>(malloc(temp_ranges.size() * sizeof(struct subid_range)));
  if (!*ranges) {
    return -1;
  }
  
  for (size_t i = 0; i < temp_ranges.size(); ++i) {
    (*ranges)[i] = temp_ranges[i];
  }
  
  return static_cast<int>(temp_ranges.size());
}

// Compatibility functions for older libsubid versions
inline int compat_subid_get_uid_ranges(const char* owner, struct subid_range** ranges) {
  return parse_subid_file("/etc/subuid", owner, ranges);
}

inline int compat_subid_get_gid_ranges(const char* owner, struct subid_range** ranges) {
  return parse_subid_file("/etc/subgid", owner, ranges);
}

inline void compat_subid_free(struct subid_range* ranges) {
  if (ranges) {
    free(ranges);
  }
}

#endif // !defined(SUBID_ABI_MAJOR) || SUBID_ABI_MAJOR < 4

} // anonymous namespace

class SubIdRanges {
 public:
  SubIdRanges(struct subid_range* ranges, int count)
      : m_ranges_(ranges), m_count_(count) {}

  ~SubIdRanges() {
    if (m_ranges_ != nullptr) {
      // libsubid uses regular free() for cleanup, not subid_free()
      free(m_ranges_);
    }
  }

  SubIdRanges(const SubIdRanges&) = delete;
  SubIdRanges& operator=(const SubIdRanges&) = delete;

  SubIdRanges(SubIdRanges&& other) noexcept
      : m_ranges_(other.m_ranges_), m_count_(other.m_count_) {
    other.m_ranges_ = nullptr;
    other.m_count_ = 0;
  }

  SubIdRanges& operator=(SubIdRanges&& other) noexcept {
    if (this != &other) {
      if (m_ranges_ != nullptr) {
        // libsubid uses regular free() for cleanup, not subid_free()
        free(m_ranges_);
      }
      m_ranges_ = other.m_ranges_;
      m_count_ = other.m_count_;
      other.m_ranges_ = nullptr;
      other.m_count_ = 0;
    }
    return *this;
  }

  bool Valid() const { return m_count_ > 0; }
  int Count() const { return m_count_; }
  const struct subid_range* Data() const { return m_ranges_; }

  const struct subid_range& operator[](int index) const {
    return m_ranges_[index];
  }

  const struct subid_range& At(int index) const {
    if (index < 0 || index >= m_count_ || m_ranges_ == nullptr) {
      throw std::out_of_range("SubIdRanges index out of range");
    }
    return m_ranges_[index];
  }

 private:
  struct subid_range* m_ranges_;
  int m_count_;
};

class PasswordEntry {
 public:
  static void InitializeEntrySize() {
    s_passwd_size_ = sysconf(_SC_GETPW_R_SIZE_MAX);
    if (s_passwd_size_ == -1) s_passwd_size_ = 16384;
  }

  explicit PasswordEntry(uid_t uid) { Init(uid); }
  PasswordEntry() = default;
  void Init(uid_t uid) {
    m_uid_ = uid;
    struct passwd pwd{};
    struct passwd* result;
    char* buf;
    buf = new char[s_passwd_size_];

    if (getpwuid_r(uid, &pwd, buf, s_passwd_size_, &result) != 0) {
      CRANE_ERROR("Error when getpwuid_r");
    } else if (result == nullptr) {
      CRANE_ERROR("User uid #{} not found.", uid);
    } else {
      m_valid_ = true;
      m_pw_name_.assign(pwd.pw_name);
      m_pw_passwd_.assign(pwd.pw_passwd);
      m_pw_uid_ = pwd.pw_uid;
      m_pw_gid_ = pwd.pw_gid;
      m_pw_gecos_.assign(pwd.pw_gecos);
      m_pw_dir_.assign(pwd.pw_dir);
      m_pw_shell_.assign(pwd.pw_shell);
    }

    delete[] buf;
  }

  bool Valid() const { return m_valid_; };

  const std::string& Username() const { return m_pw_name_; }
  const std::string& HomeDir() const { return m_pw_dir_; }
  const std::string& Shell() const { return m_pw_shell_; }

  gid_t Gid() const { return m_pw_gid_; }
  uid_t Uid() const { return m_pw_uid_; }

  SubIdRanges SubUidRanges() const {
    if (!m_valid_) return {nullptr, 0};
    struct subid_range* ranges = nullptr;
#if defined(SUBID_ABI_MAJOR) && SUBID_ABI_MAJOR >= 4
    int count = subid_get_uid_ranges(m_pw_name_.c_str(), &ranges);
#else
    int count = compat_subid_get_uid_ranges(m_pw_name_.c_str(), &ranges);
#endif
    return {ranges, count > 0 ? count : 0};
  }

  SubIdRanges SubGidRanges() const {
    if (!m_valid_) return {nullptr, 0};
    struct subid_range* ranges = nullptr;
#if defined(SUBID_ABI_MAJOR) && SUBID_ABI_MAJOR >= 4
    int count = subid_get_gid_ranges(m_pw_name_.c_str(), &ranges);
#else
    int count = compat_subid_get_gid_ranges(m_pw_name_.c_str(), &ranges);
#endif
    return {ranges, count > 0 ? count : 0};
  }

 private:
  bool m_valid_{false};
  uid_t m_uid_{};

  std::string m_pw_name_;   /* username */
  std::string m_pw_passwd_; /* user password */
  uid_t m_pw_uid_{};        /* user ID */
  gid_t m_pw_gid_{};        /* group ID */
  std::string m_pw_gecos_;  /* user information */
  std::string m_pw_dir_;    /* home directory */
  std::string m_pw_shell_;  /* shell program */

  static inline size_t s_passwd_size_{16384};  // Default size
};

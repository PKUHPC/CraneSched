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

#include <pwd.h>

#include <string>

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
    struct passwd pwd;
    struct passwd* result;
    char* buf;
    buf = new char[s_passwd_size_];

    if (getpwuid_r(uid, &pwd, buf, s_passwd_size_, &result) != 0) {
      CRANE_ERROR("Error when getpwuid_r");
    } else if (result == NULL) {
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

  static inline size_t s_passwd_size_;
};

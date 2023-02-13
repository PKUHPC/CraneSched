#pragma once

#include <pwd.h>

#include <string>

class PasswordEntry {
 public:
  PasswordEntry() = default;

  explicit PasswordEntry(uid_t uid) { Init(uid); }

  void Init(uid_t uid) {
    m_uid_ = uid;

    passwd* pwd_tmp = getpwuid(uid);
    if (pwd_tmp == nullptr) return;

    m_valid_ = true;
    m_pw_name_.assign(pwd_tmp->pw_name);
    m_pw_passwd_.assign(pwd_tmp->pw_passwd);
    m_pw_uid_ = pwd_tmp->pw_uid;
    m_pw_gid_ = pwd_tmp->pw_gid;
    m_pw_gecos_.assign(pwd_tmp->pw_gecos);
    m_pw_dir_.assign(pwd_tmp->pw_dir);
    m_pw_shell_.assign(pwd_tmp->pw_shell);
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
};

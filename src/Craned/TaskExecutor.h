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

#include "CranedPublicDefs.h"
// Precompiled header comes first.

#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/util.h>
#include <evrpc.h>
#include <grp.h>
#include <sys/eventfd.h>
#include <sys/wait.h>
#include <uv.h>

#include <uvw.hpp>

#include "crane/PasswordEntry.h"
#include "crane/PublicHeader.h"

namespace Craned {

struct BatchMetaInTaskExecutor {
  std::string parsed_output_file_pattern;
};

class TaskExecutor {
 public:
  using EnvVector = std::vector<std::pair<std::string, std::string>>;
  TaskExecutor() : m_ev_buf_event_(nullptr) {}
  virtual ~TaskExecutor() {
    if (m_ev_buf_event_) {
      bufferevent_free(m_ev_buf_event_);
    }
  };

  /* --- Abstract Interfaces --- */

  [[nodiscard]] virtual const std::string& GetExecPath() const = 0;

  virtual void SetPid(pid_t pid) = 0;
  [[nodiscard]] virtual pid_t GetPid() const = 0;

  virtual void SetBatchMeta(BatchMetaInTaskExecutor batch_meta) = 0;
  [[nodiscard]] virtual const BatchMetaInTaskExecutor& GetBatchMeta() const = 0;

  virtual void Output(std::string&& buf) = 0;

  virtual void Finish(bool is_killed, int val) = 0;

  /**
   * Spawn the executable (process / container) in the task.
   * EvActivateTaskStatusChange_ must NOT be called in this method and should be
   *  called in the caller method after checking the return value of this
   *  method.
   * @param task_id
   * @param pwd_entry
   * @param cgroup
   * @param task_envs
   * @return kSystemErr if the socket pair between the parent process and child
   * process cannot be created, and the caller should call strerror() to check
   * the unix error code. kLibEventError* if bufferevent_socket_new() fails.
   * kCgroupError if CgroupManager cannot move the process to the cgroup bound
   * to the TaskInstance. kProtobufError if the communication between the parent
   * and the child process fails.
   */
  [[nodiscard]] virtual CraneErr Spawn(task_id_t task_id,
                                       const PasswordEntry& pwd_entry,
                                       util::Cgroup* cgroup,
                                       EnvVector task_envs) = 0;

  virtual CraneErr Kill(int signum) = 0;

  /* --- Implemented in TaskExecutor --- */

  virtual void SetEvBufEvent(struct bufferevent* ev_buf_event) {
    m_ev_buf_event_ = ev_buf_event;
  }

  virtual void SetOutputCb(std::function<void(std::string&&, void*)> cb) {
    m_output_cb_ = std::move(cb);
  }

  virtual void SetFinishCb(std::function<void(bool, int, void*)> cb) {
    m_finish_cb_ = std::move(cb);
  }

  virtual bool WriteShScript(const std::string_view script) {
    auto& exec_path = GetExecPath();

    FILE* fptr = fopen(exec_path.c_str(), "w");
    if (fptr == nullptr) return false;
    fputs(script.data(), fptr);
    fclose(fptr);

    chmod(exec_path.c_str(), strtol("0755", nullptr, 8));
    return true;
  }

 protected:
  // The underlying event that handles the output of the task.
  struct bufferevent* m_ev_buf_event_{};

  /***
   * The callback function called when a task writes to stdout or stderr.
   * @param[in] buf a slice of output buffer.
   */
  std::function<void(std::string&& buf, void*)> m_output_cb_;

  /***
   * The callback function called when a task is finished.
   * @param[in] bool true if the task is terminated by a signal, false
   * otherwise.
   * @param[in] int the number of signal if bool is true, the return value
   * otherwise.
   */
  std::function<void(bool, int, void*)> m_finish_cb_;
};

class ProcessInstance final : public TaskExecutor {
 public:
  ProcessInstance(std::string cwd, std::string exec_path,
                  std::list<std::string> arg_list)
      : m_cwd_(std::move(cwd)),
        m_executive_path_(std::move(exec_path)),
        m_arguments_(std::move(arg_list)),
        m_pid_(0),
        m_user_data_(nullptr) {}

  ~ProcessInstance() override {
    if (m_user_data_) {
      if (m_clean_cb_) {
        CRANE_TRACE("Clean Callback for pid {} is called.", m_pid_);
        m_clean_cb_(m_user_data_);
      } else
        CRANE_ERROR(
            "user_data in ProcessInstance is set, but clean_cb is not set!");
    }
  }

  void SetBatchMeta(BatchMetaInTaskExecutor batch_meta) override {
    this->m_batch_meta_ = std::move(batch_meta);
  }
  [[nodiscard]] const BatchMetaInTaskExecutor& GetBatchMeta() const override {
    return m_batch_meta_;
  }

  [[nodiscard]] const std::string& GetExecPath() const override {
    return m_executive_path_;
  }

  [[nodiscard]] const std::list<std::string>& GetArgList() const {
    return m_arguments_;
  }

  void SetPid(pid_t pid) override { m_pid_ = pid; }
  [[nodiscard]] pid_t GetPid() const override { return m_pid_; }

  void Output(std::string&& buf) override {
    if (m_output_cb_) m_output_cb_(std::move(buf), m_user_data_);
  }

  void Finish(bool is_killed, int val) override {
    if (m_finish_cb_) m_finish_cb_(is_killed, val, m_user_data_);
  }

  [[nodiscard]] CraneErr Spawn(task_id_t task_id,
                               const PasswordEntry& pwd_entry,
                               util::Cgroup* cgroup,
                               EnvVector task_envs) override;
  CraneErr Kill(int signum) override;

  void SetUserDataAndCleanCb(void* data, std::function<void(void*)> cb) {
    m_user_data_ = data;
    m_clean_cb_ = std::move(cb);
  }

 private:
  std::string m_cwd_;

  BatchMetaInTaskExecutor m_batch_meta_;

  /* ------------- Fields set by SpawnProcessInInstance_  ---------------- */
  pid_t m_pid_;

  /* ------- Fields set by the caller of SpawnProcessInInstance_  -------- */
  std::string m_executive_path_;
  std::list<std::string> m_arguments_;

  void* m_user_data_;
  std::function<void(void*)> m_clean_cb_;
};

class ContainerInstance : public TaskExecutor {
 public:
  ContainerInstance(std::string bundle_path, std::string exec_path)
      : m_executive_path_(std::move(exec_path)),
        m_bundle_path_(std::move(bundle_path)),
        m_pid_(0) {}

  ~ContainerInstance() override = default;

  void SetPid(pid_t pid) override { m_pid_ = pid; }
  [[nodiscard]] pid_t GetPid() const override { return m_pid_; }

  void SetBatchMeta(BatchMetaInTaskExecutor batch_meta) override {
    this->m_batch_meta_ = std::move(batch_meta);
  }
  [[nodiscard]] const BatchMetaInTaskExecutor& GetBatchMeta() const override {
    return m_batch_meta_;
  }

  [[nodiscard]] const std::string& GetExecPath() const override {
    return m_executive_path_;
  }

  void Output(std::string&& buf) override {
    // TODO: Not implemented.
    if (m_output_cb_) m_output_cb_(std::move(buf), nullptr);
  }

  void Finish(bool is_killed, int val) override {
    // TODO: Not implemented.
    if (m_finish_cb_) m_finish_cb_(is_killed, val, nullptr);
  }

  [[nodiscard]] CraneErr Spawn(task_id_t task_id,
                               const PasswordEntry& pwd_entry,
                               util::Cgroup* cgroup,
                               EnvVector task_envs) override;
  CraneErr Kill(int signum) override;

  [[nodiscard]] const std::string& GetBundlePath() const {
    return m_bundle_path_;
  }

  /***
   * Parse the command in config to get the real command for OCI runtime.
   * @param task_id the task id (%j)
   * @param uid the user id (%u, %U)
   * @param bundle the path to the OCI bundle (%b)
   * @param cmd_to_parse the command to parse
   * @return the parsed command.
   */
  static std::string ParseContainerCmd(task_id_t task_id,
                                       const PasswordEntry& pwd_entry,
                                       std::string_view bundle,
                                       std::string cmd_to_parse) {
    absl::StrReplaceAll({{"%b", bundle},
                         {"%j", std::to_string(task_id)},
                         {"%u", pwd_entry.Username()},
                         {"%U", std::to_string(pwd_entry.Uid())}},
                        &cmd_to_parse);
    return cmd_to_parse;
  }

 private:
  std::string m_bundle_path_;
  std::string m_executive_path_;

  pid_t m_pid_;  // TODO: Change to ContainerId

  BatchMetaInTaskExecutor m_batch_meta_;
};

}  // namespace Craned

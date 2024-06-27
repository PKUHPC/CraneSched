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

#include "CtldPublicDefs.h"
// Precompiled header comes first!

namespace Ctld {
class MailSender {
 public:
  MailSender();

  ~MailSender();

  void Init();

  std::atomic_bool m_mail_sender_stop_{};

  std::thread m_check_mail_thread_;
  void CheckMailThread_(const std::shared_ptr<uvw::loop>& uvw_loop);

  struct MailItem {
    uint32_t task_id;
    crane::grpc::TaskStatus status;
    std::string task_name;
    std::string start_time;
    std::string end_time;
    std::string work_dir;
    std::string duration;
    uint32_t exit_code;
    std::string stdout_path;
    std::string stderr_path;
    uint32_t node_num;
    std::list<std::string> node_list;
    std::string mail_user;
    std::string mail_type;
  };

  ConcurrentQueue<MailItem> m_mail_queue_;

  std::shared_ptr<uvw::timer_handle> m_check_mail_timer_handle_;
  void CheckMailTimerCb_();

  std::shared_ptr<uvw::async_handle> m_check_mail_async_handle_;
  void CheckMailAsyncCb_();

  std::shared_ptr<uvw::async_handle> m_clean_mail_queue_handle_;
  void CleanMailQueueCb_();

  static std::string FormatTaskStatus(crane::grpc::TaskStatus status);

  static std::string FormatDurationToHHMMSS(absl::Time start_time,
                                            absl::Time end_time);

  static std::string FormatTime(absl::Time time);

  static std::string FormatMailSubject(const MailItem& mail_item);

  static std::string FormatMailBody(const MailItem& mail_item);

  static bool Send(const std::string& subject, const std::string& body,
                   const std::string& to, const std::string& cc,
                   const std::string& bcc);

  void AddToMailQueueAsync(const Ctld::TaskInCtld* task, std::string mail_type);
};
}  // namespace Ctld

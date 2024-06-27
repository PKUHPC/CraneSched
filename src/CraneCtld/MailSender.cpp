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

#include "MailSender.h"

#include <absl/strings/str_replace.h>

#include "CtldPublicDefs.h"
#include "crane/Logger.h"
#include "crane/PasswordEntry.h"

namespace Ctld {

MailSender::MailSender() {}

MailSender::~MailSender() {
  m_mail_sender_stop_ = true;
  if (m_check_mail_thread_.joinable()) m_check_mail_thread_.join();
}

void MailSender::Init() {
  std::shared_ptr<uvw::loop> uvw_check_mail_loop = uvw::loop::create();
  m_check_mail_timer_handle_ =
      uvw_check_mail_loop->resource<uvw::timer_handle>();
  m_check_mail_timer_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle&) {
        CheckMailTimerCb_();
      });
  m_check_mail_timer_handle_->start(
      std::chrono::milliseconds(kCheckMailTimeoutMS * 3),
      std::chrono::milliseconds(kCheckMailTimeoutMS));

  m_check_mail_async_handle_ =
      uvw_check_mail_loop->resource<uvw::async_handle>();
  m_check_mail_async_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        CheckMailAsyncCb_();
      });

  m_clean_mail_queue_handle_ =
      uvw_check_mail_loop->resource<uvw::async_handle>();
  m_clean_mail_queue_handle_->on<uvw::async_event>(
      [this](const uvw::async_event&, uvw::async_handle&) {
        CleanMailQueueCb_();
      });

  m_check_mail_thread_ =
      std::thread([this, loop = std::move(uvw_check_mail_loop)]() {
        CheckMailThread_(loop);
      });
}

void MailSender::CheckMailThread_(const std::shared_ptr<uvw::loop>& uvw_loop) {
  util::SetCurrentThreadName("CheckMailsThr");

  std::shared_ptr<uvw::idle_handle> idle_handle =
      uvw_loop->resource<uvw::idle_handle>();
  idle_handle->on<uvw::idle_event>(
      [this](const uvw::idle_event&, uvw::idle_handle& h) {
        if (m_mail_sender_stop_) {
          h.parent().walk([](auto&& h) { h.close(); });
          h.parent().stop();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      });

  if (idle_handle->start() != 0) {
    CRANE_ERROR("Failed to start the idle event in check mail loop.");
  }

  uvw_loop->run();
}

void MailSender::CheckMailTimerCb_() { m_clean_mail_queue_handle_->send(); }

void MailSender::CheckMailAsyncCb_() {
  if (m_mail_queue_.size_approx() >= kCheckMailBatchNum) {
    m_clean_mail_queue_handle_->send();
  }
}

void MailSender::CleanMailQueueCb_() {
  auto send_mail = [this](const MailItem& mail_item) -> int {
    // Construct and send mail
    if (!Send(FormatMailSubject(mail_item), FormatMailBody(mail_item),
              mail_item.mail_user, "", "")) {
      CRANE_ERROR("Failed to send mail for task_id #{}", mail_item.task_id);
      return -1;
    }

    return 0;
  };

  size_t approximate_size = m_mail_queue_.size_approx();
  std::vector<MailItem> mail_args;
  mail_args.resize(approximate_size);

  size_t actual_size =
      m_mail_queue_.try_dequeue_bulk(mail_args.begin(), approximate_size);

  // Traverse MailItem and send email.
  for (size_t i = 0; i < actual_size; ++i) {
    const MailItem& mail_arg = mail_args[i];
    if (send_mail(mail_arg) != 0) {
      CRANE_ERROR("Failed to send mail for task_id {}", mail_arg.task_id);
    }
  }
}

std::string MailSender::FormatDurationToHHMMSS(absl::Time start_time,
                                               absl::Time end_time) {
  absl::Duration duration = end_time - start_time;
  int64_t total_seconds = absl::ToUnixSeconds(absl::FromTimeT(0) + duration);
  int hours = total_seconds / 3600;
  int minutes = (total_seconds % 3600) / 60;
  int seconds = total_seconds % 60;
  return absl::StrFormat("%02d:%02d:%02d", hours, minutes, seconds);
}

std::string MailSender::FormatTime(absl::Time time) {
  // Use default local timezone
  absl::TimeZone tz;
  absl::LoadTimeZone(absl::LocalTimeZone().name(), &tz);

  std::string formatted_time =
      absl::FormatTime("%Y-%m-%dT%H:%M:%S%Ez", time, tz);
  return formatted_time;
}

std::string MailSender::FormatTaskStatus(crane::grpc::TaskStatus status) {
  switch (status) {
    case crane::grpc::Pending:
      return "Pending";
    case crane::grpc::Running:
      return "Running";
    case crane::grpc::Completed:
      return "Completed";
    case crane::grpc::Failed:
      return "Failed";
    case crane::grpc::ExceedTimeLimit:
      return "ExceedTimeLimit";
    case crane::grpc::Cancelled:
      return "Cancelled";
    case crane::grpc::Invalid:
      return "Invalid";
    default:
      return "Unknown";
  }
}

std::string MailSender::FormatMailSubject(const MailItem& mail_item) {
  std::string subject =
      fmt::format("[CraneSched] Job_ID={}, Name={}, Type={}, Status={}",
                  mail_item.task_id, mail_item.task_name, mail_item.mail_type,
                  FormatTaskStatus(mail_item.status));

  if (mail_item.status != crane::grpc::Running)
    subject += fmt::format(", Run_Time={}, Exit_Code={}", mail_item.duration,
                           mail_item.exit_code);

  return subject;
}

std::string MailSender::FormatMailBody(const MailItem& mail_item) {
  std::string body = fmt::format(
      "Job ID: {}\nJob Name: {}\nStatus: {}\nWorking Dir: {}\nStart Time: {}\n",
      mail_item.task_id, mail_item.task_name,
      FormatTaskStatus(mail_item.status), mail_item.work_dir,
      mail_item.start_time);

  if (mail_item.status != crane::grpc::Running) {
    body += fmt::format("End Time: {}\nDuration: {}\nExit Code: {}\n",
                        mail_item.end_time, mail_item.duration,
                        mail_item.exit_code);
  }

  body += fmt::format("Stdout File: {}\nStderr File: {}\n",
                      mail_item.stdout_path, mail_item.stderr_path);

  body += fmt::format("Node Number: {}\nNodes List: {}\n", mail_item.node_num,
                      fmt::join(mail_item.node_list, ", "));

  body +=
      "\nThis mail is automatically sent by CraneSched. Please do not reply.\n";

  return body;
}

bool MailSender::Send(const std::string& subject, const std::string& body,
                      const std::string& to, const std::string& cc,
                      const std::string& bcc) {
  // Construct the mail command
  std::string command = fmt::format("mail -r {} -s \"{}\"",
                                    g_config.MailConfig.SenderAddr, subject);
  if (!cc.empty()) {
    command += " -c " + cc;
  }
  if (!bcc.empty()) {
    command += " -b " + bcc;
  }
  command += " " + to;

  CRANE_TRACE("Generated mail command: {}", command);

  // Open a pipe to mail
  FILE* mailpipe = popen(command.c_str(), "w");
  if (mailpipe == nullptr) {
    CRANE_ERROR("Failed to open pipe to mail command.");
    return false;
  }

  // Write the email body to the pipe
  fprintf(mailpipe, "%s", body.c_str());
  int result = pclose(mailpipe);

  return result == 0;
}

void MailSender::AddToMailQueueAsync(const Ctld::TaskInCtld* task,
                                     std::string mail_type) {
  std::string stdout_file_path = "";
  std::string stderr_file_path = "";

  if (task->meta.index() == 1) {
    stdout_file_path =
        std::get<Ctld::BatchMetaInTask>(task->meta).output_file_pattern;
    stderr_file_path =
        std::get<Ctld::BatchMetaInTask>(task->meta).error_file_pattern;
    if (stderr_file_path == "") {
      stderr_file_path = stdout_file_path;
    }

    PasswordEntry entry(task->uid);
    stdout_file_path =
        util::ParseFilePathPattern(stdout_file_path, task->cwd, task->TaskId());
    absl::StrReplaceAll({{"%j", std::to_string(task->TaskId())},
                         {"%u", entry.Username()},
                         {"%x", task->name}},
                        &stdout_file_path);
    stderr_file_path =
        util::ParseFilePathPattern(stderr_file_path, task->cwd, task->TaskId());
    absl::StrReplaceAll({{"%j", std::to_string(task->TaskId())},
                         {"%u", entry.Username()},
                         {"%x", task->name}},
                        &stderr_file_path);
  }

  MailItem mail_item = {
      task->TaskId(),
      task->Status(),
      task->name,
      FormatTime(task->StartTime()),
      FormatTime(task->EndTime()),
      task->cwd,
      FormatDurationToHHMMSS(task->StartTime(), task->EndTime()),
      task->ExitCode(),
      stdout_file_path,
      stderr_file_path,
      task->node_num,
      task->CranedIds(),
      task->mail_user,
      mail_type};
  m_mail_queue_.enqueue(std::move(mail_item));
  m_check_mail_async_handle_->send();
}

}  // namespace Ctld
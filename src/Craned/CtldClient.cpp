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

#include "CtldClient.h"

namespace Craned {

CtldClient::~CtldClient() {
  m_thread_stop_ = true;

  CRANE_TRACE("CtldClient is ending. Waiting for the thread to finish.");
  m_async_send_thread_.join();
}

void CtldClient::InitChannelAndStub(const std::string& server_address) {
  grpc::ChannelArguments channel_args;

  if (g_config.CompressedRpc)
    channel_args.SetCompressionAlgorithm(GRPC_COMPRESS_GZIP);

  if (g_config.ListenConf.UseTls) {
    std::string ctld_address = fmt::format("{}.{}:{}", server_address,
                                           g_config.ListenConf.DomainSuffix,
                                           g_config.CraneCtldListenPort);

    grpc::SslCredentialsOptions ssl_opts;
    // pem_root_certs is actually the certificate of server side rather than
    // CA certificate. CA certificate is not needed.
    // Since we use the same cert/key pair for both cranectld/craned,
    // pem_root_certs is set to the same certificate.
    ssl_opts.pem_root_certs = g_config.ListenConf.ServerCertContent;
    ssl_opts.pem_cert_chain = g_config.ListenConf.ServerCertContent;
    ssl_opts.pem_private_key = g_config.ListenConf.ServerKeyContent;

    m_ctld_channel_ = grpc::CreateCustomChannel(
        ctld_address, grpc::SslCredentials(ssl_opts), channel_args);
  } else {
    std::string ctld_address =
        fmt::format("{}:{}", server_address, g_config.CraneCtldListenPort);
    m_ctld_channel_ = grpc::CreateCustomChannel(
        ctld_address, grpc::InsecureChannelCredentials(), channel_args);
  }

  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = CraneCtld::NewStub(m_ctld_channel_);

  m_async_send_thread_ = std::thread([this] { AsyncSendThread_(); });
}

void CtldClient::OnCraneCtldConnected() {
  crane::grpc::CranedRegisterRequest request;
  grpc::Status status;

  CRANE_INFO("Send a register RPC to cranectld");
  request.set_craned_id(m_craned_id_);

  int retry_time = 10;

  do {
    crane::grpc::CranedRegisterReply reply;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(1));

    status = m_stub_->CranedRegister(&context, request, &reply);
    if (!status.ok()) {
      CRANE_ERROR(
          "NodeActiveConnect RPC returned with status not ok: {}, Resend it.",
          status.error_message());
    } else {
      if (reply.ok()) {
        if (reply.already_registered()) {
          CRANE_INFO("This craned has already registered.");
          return;
        } else {
          CRANE_INFO(
              "This craned has not registered. "
              "Sending Register request...");
          std::this_thread::sleep_for(std::chrono::seconds(3));
        }
      } else {
        CRANE_ERROR("This Craned is not allow to register.");
        return;
      }
    }
  } while (retry_time--);

  CRANE_ERROR("Failed to register actively.");
}

void CtldClient::TaskStatusChangeAsync(TaskStatusChange&& task_status_change) {
  absl::MutexLock lock(&m_task_status_change_mtx_);
  m_task_status_change_list_.emplace_back(std::move(task_status_change));
}

bool CtldClient::CancelTaskStatusChangeByTaskId(
    task_id_t task_id, crane::grpc::TaskStatus* new_status) {
  absl::MutexLock lock(&m_task_status_change_mtx_);

  size_t num_removed{0};

  for (auto it = m_task_status_change_list_.begin();
       it != m_task_status_change_list_.end();)
    if (it->task_id == task_id) {
      num_removed++;
      *new_status = it->new_status;
      it = m_task_status_change_list_.erase(it);
    } else
      ++it;

  CRANE_ASSERT_MSG(num_removed <= 1,
                   "TaskStatusChange should happen at most once "
                   "for a single running task!");

  return num_removed >= 1;
}

void CtldClient::AsyncSendThread_() {
  m_start_connecting_notification_.WaitForNotification();

  std::this_thread::sleep_for(std::chrono::seconds(1));

  absl::Condition cond(
      +[](decltype(m_task_status_change_list_)* queue) {
        return !queue->empty();
      },
      &m_task_status_change_list_);

  bool prev_conn_state = false;
  while (true) {
    if (m_thread_stop_) break;

    bool connected = m_ctld_channel_->WaitForConnected(
        std::chrono::system_clock::now() + std::chrono::seconds(3));

    if (!prev_conn_state && connected) {
      g_ctld_client->OnCraneCtldConnected();
    }
    prev_conn_state = connected;

    if (!connected) {
      CRANE_INFO("Channel to CraneCtlD is not connected. Reconnecting...");
      std::this_thread::sleep_for(std::chrono::seconds(10));
      continue;
    }

    bool has_msg = m_task_status_change_mtx_.LockWhenWithTimeout(
        cond, absl::Milliseconds(50));
    if (!has_msg) {
      m_task_status_change_mtx_.Unlock();
      continue;
    }

    std::list<TaskStatusChange> changes;
    changes.splice(changes.begin(), std::move(m_task_status_change_list_));
    m_task_status_change_mtx_.Unlock();

    while (!changes.empty()) {
      grpc::ClientContext context;
      crane::grpc::TaskStatusChangeRequest request;
      crane::grpc::TaskStatusChangeReply reply;
      grpc::Status status;

      auto status_change = changes.front();

      CRANE_TRACE("Sending TaskStatusChange for task #{}",
                  status_change.task_id);

      request.set_craned_id(m_craned_id_);
      request.set_task_id(status_change.task_id);
      request.set_new_status(status_change.new_status);
      request.set_exit_code(status_change.exit_code);
      if (status_change.reason.has_value())
        request.set_reason(status_change.reason.value());

      status = m_stub_->TaskStatusChange(&context, request, &reply);
      if (!status.ok()) {
        CRANE_ERROR(
            "Failed to send TaskStatusChange: "
            "{{TaskId: {}, NewStatus: {}}}, reason: {} | {}, code: {}",
            status_change.task_id, status_change.new_status,
            status.error_message(), context.debug_error_string(),
            status.error_code());

        if (status.error_code() == grpc::UNAVAILABLE) {
          // If some messages are not sent due to channel failure,
          // put them back into m_task_status_change_list_
          if (!changes.empty()) {
            m_task_status_change_mtx_.Lock();
            m_task_status_change_list_.splice(
                m_task_status_change_list_.begin(), std::move(changes));
            m_task_status_change_mtx_.Unlock();
          }
          break;
        } else
          changes.pop_front();
      } else {
        CRANE_TRACE("TaskStatusChange for task #{} sent. reply.ok={}",
                    status_change.task_id, reply.ok());
        changes.pop_front();
      }
    }
  }
}

}  // namespace Craned

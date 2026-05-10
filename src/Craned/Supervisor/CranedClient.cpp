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

#include "CranedClient.h"

#include "crane/GrpcHelper.h"

namespace Craned::Supervisor {

CranedClient::~CranedClient() {
  if (m_async_send_thread_.joinable()) {
    m_async_send_thread_.join();
  }
}

void CranedClient::Shutdown() {
  m_thread_stop_ = true;
  m_cv_.Signal();
}

void CranedClient::InitChannelAndStub(const std::string& endpoint) {
  m_channel_ = CreateUnixInsecureChannel(endpoint);
  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = crane::grpc::Craned::NewStub(m_channel_);
  m_async_send_thread_ = std::thread([this] { AsyncSendThread_(); });
}

void CranedClient::StepStatusChangeAsync(
    crane::grpc::JobStatus new_status, uint32_t exit_code,
    std::optional<std::string> reason,
    std::optional<crane::grpc::JobStatus> final_status) {
  StepStatusChangeQueueElem elem{
      .new_status = new_status,
      .exit_code = exit_code,
      .reason = std::move(reason),
      .final_status = final_status,
      .timestamp = google::protobuf::util::TimeUtil::GetCurrentTime()};
  absl::MutexLock lock(&m_mutex_);
  m_task_status_change_queue_.push_back(std::move(elem));
  m_cv_.Signal();
}

void CranedClient::AsyncSendThread_() {
  util::SetCurrentThreadName("CrndClient");
  while (true) {
    {
      absl::MutexLock lock(&m_mutex_);
      while (m_task_status_change_queue_.empty() && !m_thread_stop_) {
        m_cv_.Wait(&m_mutex_);
      }
      if (m_task_status_change_queue_.empty() && m_thread_stop_) break;
    }

    bool connected = m_channel_->WaitForConnected(
        std::chrono::system_clock::now() +
        std::chrono::seconds(
            g_config.StatusChange.ChannelConnectTimeoutSec));
    if (!connected) {
      CRANE_INFO(
          "Channel to CraneD is not connected. "
          "Reconnecting...");
      m_channel_->GetState(true);

      absl::MutexLock lock(&m_mutex_);
      m_cv_.WaitWithTimeout(
          &m_mutex_,
          absl::Seconds(g_config.StatusChange.ReconnectBackoffSec));
      if (m_thread_stop_) break;
      continue;
    }

    std::list<StepStatusChangeQueueElem> elems;
    {
      absl::MutexLock lock(&m_mutex_);
      elems.splice(elems.end(), std::move(m_task_status_change_queue_));
    }

    if (elems.empty()) continue;

    crane::grpc::BatchStepStatusChangeRequest batch_request;
    for (auto& elem : elems) {
      auto* req = batch_request.add_changes();
      req->set_job_id(g_config.JobId);
      req->set_step_id(g_config.StepId);
      req->set_new_status(elem.new_status);
      req->set_exit_code(elem.exit_code);
      *req->mutable_timestamp() = elem.timestamp;
      if (elem.reason.has_value()) req->set_reason(elem.reason.value());
      if (elem.final_status.has_value())
        req->set_final_status(elem.final_status.value());
    }

    CRANE_TRACE("Sending batch StepStatusChange with {} items.",
                batch_request.changes_size());

    grpc::ClientContext context;
    crane::grpc::BatchStepStatusChangeReply reply;
    auto status =
        m_stub_->BatchStepStatusChange(&context, batch_request, &reply);
    if (!status.ok()) {
      CRANE_ERROR(
          "Failed to send batch StepStatusChange ({} items), "
          "reason: {} | {}, code: {}",
          batch_request.changes_size(), status.error_message(),
          context.debug_error_string(), int(status.error_code()));
      absl::MutexLock lock(&m_mutex_);
      m_task_status_change_queue_.splice(
          m_task_status_change_queue_.begin(), std::move(elems));
    } else {
      CRANE_TRACE("Batch StepStatusChange sent ({} items), reply.ok={}",
                  batch_request.changes_size(), reply.ok());
    }
  }
}

}  // namespace Craned::Supervisor

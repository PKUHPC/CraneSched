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

#include "SupervisorServer.h"
#include "TaskManager.h"
#include "crane/GrpcHelper.h"
#include "crane/String.h"

namespace Craned::Supervisor {

using grpc::ClientContext;
using grpc::Status;

CranedClient::~CranedClient() {
  if (m_async_send_thread_.joinable()) {
    m_async_send_thread_.join();
  }
}

void CranedClient::Shutdown() { m_thread_stop_ = true; }

void CranedClient::InitChannelAndStub(const std::string& endpoint) {
  m_channel_ = CreateUnixInsecureChannel(endpoint);
  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = crane::grpc::Craned::NewStub(m_channel_);
  m_async_send_thread_ = std::thread([this] { AsyncSendThread_(); });
}

void CranedClient::StepStatusChangeAsync(crane::grpc::TaskStatus new_status,
                                         uint32_t exit_code,
                                         std::optional<std::string> reason) {
  StepStatusChangeQueueElem elem{
      .new_status = new_status,
      .exit_code = exit_code,
      .reason = std::move(reason),
      .timestamp = google::protobuf::util::TimeUtil::GetCurrentTime()};
  absl::MutexLock lock(&m_mutex_);
  m_task_status_change_queue_.push_back(std::move(elem));
}

void CranedClient::AsyncSendThread_() {
  while (true) {
    {
      absl::MutexLock lock(&m_mutex_);
      if (m_task_status_change_queue_.empty() && m_thread_stop_) break;
    }

    bool connected = m_channel_->WaitForConnected(
        std::chrono::system_clock::now() + std::chrono::seconds(3));
    if (!connected) {
      CRANE_INFO("Channel to CraneD is not connected. Reconnecting...");
      m_channel_->GetState(true);
      std::this_thread::sleep_for(std::chrono::seconds(10));
      continue;
    }

    {
      std::list<StepStatusChangeQueueElem> elems;
      {
        absl::MutexLock lock(&m_mutex_);
        if (!m_task_status_change_queue_.empty()) {
          elems.splice(elems.end(), std::move(m_task_status_change_queue_));
        }
      }

      while (!elems.empty()) {
        auto& elem = elems.front();
        grpc::ClientContext context;
        crane::grpc::StepStatusChangeRequest request;
        crane::grpc::StepStatusChangeReply reply;
        grpc::Status status;

        CRANE_TRACE("Sending StepStatusChange for step status: {}",
                    elem.new_status);

        request.set_job_id(g_config.JobId);
        request.set_step_id(g_config.StepId);
        request.set_new_status(elem.new_status);
        request.set_exit_code(elem.exit_code);
        *request.mutable_timestamp() = elem.timestamp;
        if (elem.reason.has_value()) request.set_reason(elem.reason.value());

        status = m_stub_->StepStatusChange(&context, request, &reply);
        if (!status.ok()) {
          CRANE_ERROR(
              "Failed to send StepStatusChange: "
              "NewStatus: {}, reason: {} | {}, code: {}",
              elem.new_status, status.error_message(),
              context.debug_error_string(), int(status.error_code()));
          break;
        }
        elems.pop_front();
        CRANE_TRACE("StepStatusChange sent, status {}. reply.ok={}",
                    elem.new_status, reply.ok());
      }
      m_mutex_.Lock();
      if (!elems.empty()) {
        m_task_status_change_queue_.splice(m_task_status_change_queue_.begin(),
                                           std::move(elems));
      }
      m_mutex_.Unlock();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
}

}  // namespace Craned::Supervisor
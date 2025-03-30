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

namespace Supervisor {

using grpc::ClientContext;
using grpc::Status;

CranedClient::~CranedClient() {
  if (m_async_send_thread_.joinable()) {
    m_async_send_thread_.join();
  }
}
void CranedClient::InitChannelAndStub(const std::string& endpoint) {
  m_channel_ = CreateUnixInsecureChannel(endpoint);
  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = crane::grpc::Craned::NewStub(m_channel_);
  m_async_send_thread_ = std::thread([this] { AsyncSendThread_(); });
}

void CranedClient::TaskStatusChangeAsync(crane::grpc::TaskStatus new_status,
                                         uint32_t exit_code,
                                         std::optional<std::string> reason) {
  TaskStatusChangeQueueElem elem{.task_id = g_config.JobId,
                                 .new_status = new_status,
                                 .exit_code = exit_code,
                                 .reason = std::move(reason)};
  m_task_status_change_queue_.enqueue(std::move(elem));
}

void CranedClient::AsyncSendThread_() {
  int finished_count = 0;

  std::this_thread::sleep_for(std::chrono::seconds(1));

  while (true) {
    if (m_thread_stop_) break;

    bool connected = m_channel_->WaitForConnected(
        std::chrono::system_clock::now() + std::chrono::seconds(3));

    if (!connected) {
      CRANE_INFO("Channel to CraneD is not connected. Reconnecting...");
      std::this_thread::sleep_for(std::chrono::seconds(10));
      continue;
    }

    TaskStatusChangeQueueElem elem;
    while (m_task_status_change_queue_.try_dequeue(elem)) {
      grpc::ClientContext context;
      crane::grpc::TaskStatusChangeRequest request;
      crane::grpc::TaskStatusChangeReply reply;
      grpc::Status status;

      CRANE_TRACE("Sending TaskStatusChange for task #{}", elem.task_id);

      request.set_task_id(elem.task_id);
      request.set_new_status(elem.new_status);
      request.set_exit_code(elem.exit_code);
      if (elem.reason.has_value()) request.set_reason(elem.reason.value());

      status = m_stub_->TaskStatusChange(&context, request, &reply);
      if (!status.ok()) {
        CRANE_ERROR(
            "Failed to send TaskStatusChange: "
            "{{TaskId: {}, NewStatus: {}}}, reason: {} | {}, code: {}",
            elem.task_id, int(elem.new_status), status.error_message(),
            context.debug_error_string(), int(status.error_code()));

        if (status.error_code() == grpc::UNAVAILABLE) {
          // If some messages are not sent due to channel failure,
          // put them back into m_task_status_change_list_
          m_task_status_change_queue_.enqueue(elem);
          std::this_thread::sleep_for(std::chrono::seconds(1));
          break;
        }
      } else {
        finished_count++;
        CRANE_TRACE("TaskStatusChange for task #{} sent. reply.ok={}",
                    elem.task_id, reply.ok());
        if (finished_count == g_config.TaskCount) {
          CRANE_TRACE("All tasks finished,exiting...");
          m_thread_stop_ = true;
          g_server->Shutdown();
          g_task_mgr->Shutdown();
        }
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
}

}  // namespace Supervisor

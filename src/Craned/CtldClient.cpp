#include "CtldClient.h"

#include <boost/uuid/uuid_io.hpp>

namespace Craned {

CtldClient::CtldClient() {
  m_async_send_thread_ = std::thread([this] { AsyncSendThread_(); });
}

CtldClient::~CtldClient() {
  m_thread_stop_ = true;

  CRANE_TRACE("CtldClient is ending. Waiting for the thread to finish.");
  m_async_send_thread_.join();
}

void CtldClient::InitChannelAndStub(const std::string& server_address) {
  m_ctld_channel_ =
      grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());

  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = CraneCtld::NewStub(m_ctld_channel_);
}

void CtldClient::TaskStatusChangeAsync(TaskStatusChange&& task_status_change) {
  m_task_status_change_mtx_.Lock();
  m_task_status_change_list_.emplace_back(std::move(task_status_change));
  m_task_status_change_mtx_.Unlock();
}

void CtldClient::AsyncSendThread_() {
  absl::Condition cond(
      +[](decltype(m_task_status_change_list_)* queue) {
        return !queue->empty();
      },
      &m_task_status_change_list_);

  while (true) {
    bool has_msg = m_task_status_change_mtx_.LockWhenWithTimeout(
        cond, absl::Milliseconds(300));
    if (!has_msg) {
      m_task_status_change_mtx_.Unlock();
      continue;
    }

    bool connected = m_ctld_channel_->WaitForConnected(
        std::chrono::system_clock::now() + std::chrono::seconds(3));
    if (connected) {
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

        request.set_craned_index(m_craned_id_.craned_index);
        request.set_task_id(status_change.task_id);
        request.set_new_status(status_change.new_status);
        if (status_change.reason.has_value())
          request.set_reason(status_change.reason.value());

        status = m_stub_->TaskStatusChange(&context, request, &reply);
        if (!status.ok()) {
          CRANE_ERROR(
              "Failed to send TaskStatusChange: "
              "{{TaskId: {}, NewStatus: {}}}, reason: {}, code: {}",
              status_change.task_id, status_change.new_status,
              status.error_message(), status.error_code());

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
          }
        } else {
          CRANE_TRACE("TaskStatusChange for task #{} sent. reply.ok={}",
                      status_change.task_id, reply.ok());
          changes.pop_front();
        }
      }
    } else {
      CRANE_TRACE(
          "New TaskStatusChange in the queue, "
          "but channel is not connected.");
      m_task_status_change_mtx_.Unlock();
    }

    if (m_thread_stop_) break;
  }
}

}  // namespace Craned

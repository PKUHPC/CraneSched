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

namespace {

int64_t NowUnixMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

int64_t ElapsedMs(std::chrono::steady_clock::time_point begin,
                  std::chrono::steady_clock::time_point end) {
  if (begin == std::chrono::steady_clock::time_point{}) return 0;
  return std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
      .count();
}

}  // namespace

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
  const auto enqueue_steady_time = std::chrono::steady_clock::now();
  const auto enqueue_ts_ms = NowUnixMs();
  StepStatusChangeQueueElem elem{
      .new_status = new_status,
      .exit_code = exit_code,
      .reason = std::move(reason),
      .final_status = final_status,
      .timestamp = google::protobuf::util::TimeUtil::GetCurrentTime(),
      .enqueue_steady_time = enqueue_steady_time,
      .enqueue_ts_ms = enqueue_ts_ms};
  int64_t queue_len = 0;
  {
    absl::MutexLock lock(&m_mutex_);
    m_task_status_change_queue_.push_back(std::move(elem));
    queue_len = static_cast<int64_t>(m_task_status_change_queue_.size());
    m_task_status_change_queue_.back().queue_len_at_enqueue = queue_len;
    m_cv_.Signal();
  }

  CRANE_TRACE_SCOPE_NAMED(enqueue_span, "status_change/supervisor_enqueue");
  enqueue_span.SetAttribute("job_id", g_config.JobId);
  enqueue_span.SetAttribute("step_id", g_config.StepId);
  enqueue_span.SetAttribute("new_status", static_cast<int64_t>(new_status));
  enqueue_span.SetAttribute("exit_code", static_cast<int64_t>(exit_code));
  enqueue_span.SetAttribute("has_final_status", final_status.has_value());
  if (final_status.has_value()) {
    enqueue_span.SetAttribute("final_status",
                              static_cast<int64_t>(final_status.value()));
  }
  enqueue_span.SetAttribute("queue_len", queue_len);
  enqueue_span.SetAttribute("enqueue_ts_ms", enqueue_ts_ms);
}

void CranedClient::AsyncSendThread_() {
  util::SetCurrentThreadName("CrndClient");
  while (true) {
    {
      absl::MutexLock lock(&m_mutex_);
      if (m_task_status_change_queue_.empty() && m_thread_stop_) break;
    }

    bool connected = m_channel_->WaitForConnected(
        std::chrono::system_clock::now() + std::chrono::seconds(3));
    if (!connected) {
      CRANE_INFO(
          "Channel to CraneD is not connected. "
          "Reconnecting...");
      m_channel_->GetState(true);

      // Interruptible sleep: wake up immediately on Shutdown()
      absl::MutexLock lock(&m_mutex_);
      m_cv_.WaitWithTimeout(&m_mutex_, absl::Seconds(10));
      if (m_thread_stop_) break;
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
        const auto send_start = std::chrono::steady_clock::now();
        const int64_t queue_wait_ms =
            ElapsedMs(elem.enqueue_steady_time, send_start);

        CRANE_TRACE("Sending StepStatusChange for step status: {}",
                    elem.new_status);

        CRANE_TRACE_SCOPE_NAMED(send_span, "status_change/supervisor_send");
        send_span.SetAttribute("job_id", g_config.JobId);
        send_span.SetAttribute("step_id", g_config.StepId);
        send_span.SetAttribute("new_status",
                               static_cast<int64_t>(elem.new_status));
        send_span.SetAttribute("exit_code",
                               static_cast<int64_t>(elem.exit_code));
        send_span.SetAttribute("has_final_status",
                               elem.final_status.has_value());
        if (elem.final_status.has_value()) {
          send_span.SetAttribute(
              "final_status", static_cast<int64_t>(elem.final_status.value()));
        }
        send_span.SetAttribute("queue_len", elem.queue_len_at_enqueue);
        send_span.SetAttribute("queue_wait_ms", queue_wait_ms);
        send_span.SetAttribute("enqueue_ts_ms", elem.enqueue_ts_ms);

        request.set_job_id(g_config.JobId);
        request.set_step_id(g_config.StepId);
        request.set_new_status(elem.new_status);
        request.set_exit_code(elem.exit_code);
        *request.mutable_timestamp() = elem.timestamp;
        if (elem.reason.has_value()) request.set_reason(elem.reason.value());
        if (elem.final_status.has_value())
          request.set_final_status(elem.final_status.value());

        const auto rpc_start = std::chrono::steady_clock::now();
        status = m_stub_->StepStatusChange(&context, request, &reply);
        const auto rpc_elapsed_ms =
            ElapsedMs(rpc_start, std::chrono::steady_clock::now());
        send_span.SetAttribute("rpc_elapsed_ms", rpc_elapsed_ms);
        send_span.SetAttribute("grpc_status_code",
                               static_cast<int64_t>(status.error_code()));
        send_span.SetAttribute("grpc_status_ok", status.ok());
        send_span.SetAttribute("reply_ok", status.ok() && reply.ok());
        if (!status.ok()) {
          CRANE_ERROR(
              "Failed to send StepStatusChange: "
              "new_status: {}, reason: {} | {}, code: {}",
              elem.new_status, status.error_message(),
              context.debug_error_string(), int(status.error_code()));
          break;
        }
        CRANE_TRACE("StepStatusChange sent, status={}, reply.ok={}",
                    elem.new_status, reply.ok());
        elems.pop_front();
      }
      m_mutex_.Lock();
      if (!elems.empty()) {
        m_task_status_change_queue_.splice(m_task_status_change_queue_.begin(),
                                           std::move(elems));
      }
      m_mutex_.Unlock();
    }

    // Interruptible wait for new queue items
    absl::MutexLock lock(&m_mutex_);
    m_cv_.WaitWithTimeout(&m_mutex_, absl::Milliseconds(50));
  }
}

}  // namespace Craned::Supervisor

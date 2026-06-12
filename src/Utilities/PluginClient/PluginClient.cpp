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

#include "crane/PluginClient.h"

#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>
#include <google/protobuf/message.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/channel_arguments.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <iterator>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "crane/GrpcHelper.h"
#include "crane/Logger.h"
#include "crane/PublicHeader.h"
#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"
#include "protos/Plugin.pb.h"
#include "protos/PublicDefs.pb.h"

namespace plugin {
namespace {

constexpr size_t kPluginHookMaxRequestBytes = 3 * 1024 * 1024;
constexpr char kTraceHookMaxRequestBytesEnv[] =
    "CRANE_TRACE_HOOK_MAX_REQUEST_BYTES";

std::unique_ptr<crane::grpc::plugin::TraceHookRequest> MakeTraceHookRequest() {
  return std::make_unique<crane::grpc::plugin::TraceHookRequest>();
}

}  // namespace

PluginClient::~PluginClient() {
  m_thread_stop_.store(true);
  CRANE_TRACE("PluginClient is ending. Waiting for the thread to finish.");
  if (m_async_send_thread_.joinable()) m_async_send_thread_.join();
}

// Note that we do not support TLS in plugin yet.
void PluginClient::InitChannelAndStub(const std::string& endpoint,
                                      size_t trace_hook_max_request_bytes) {
  if (const char* env = std::getenv(kTraceHookMaxRequestBytesEnv);
      env != nullptr && env[0] != '\0') {
    char* end = nullptr;
    unsigned long long value = std::strtoull(env, &end, 10);
    if (end != env && value > 0)
      trace_hook_max_request_bytes = static_cast<size_t>(value);
  }
  m_trace_hook_max_request_bytes_ =
      std::max<size_t>(1024, trace_hook_max_request_bytes);
  setenv(kTraceHookMaxRequestBytesEnv,
         std::to_string(m_trace_hook_max_request_bytes_).c_str(), 1);

  m_channel_ = CreateUnixInsecureChannel(endpoint);
  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = CranePluginD::NewStub(m_channel_);
  m_async_send_thread_ = std::thread([this] { AsyncSendThread_(); });
  CRANE_INFO("[Plugin] TraceHook max request bytes: {}",
             m_trace_hook_max_request_bytes_);
}

bool PluginClient::DrainTraceHooks(std::chrono::microseconds timeout) noexcept {
  auto target = m_trace_hooks_enqueued_.load(std::memory_order_acquire);
  if (m_trace_hooks_completed_.load(std::memory_order_acquire) >= target)
    return true;

  std::unique_lock lock(m_trace_drain_mutex_);
  auto deadline = std::chrono::steady_clock::now() + timeout;
  return m_trace_drain_cv_.wait_until(lock, deadline, [&] {
    return m_trace_hooks_completed_.load(std::memory_order_acquire) >= target;
  });
}

void PluginClient::MarkTraceHookCompleted_(const HookEvent& event) {
  if (event.type != HookType::TRACE) return;
  MarkTraceHooksCompleted_(1);
}

void PluginClient::MarkTraceHooksCompleted_(size_t count) {
  if (count == 0) return;
  m_trace_hooks_completed_.fetch_add(count, std::memory_order_release);
  m_trace_drain_cv_.notify_all();
}

size_t PluginClient::CountTraceHookEvents_(const std::list<HookEvent>& events) {
  return std::count_if(
      events.begin(), events.end(),
      [](const HookEvent& event) { return event.type == HookType::TRACE; });
}

void PluginClient::AsyncSendThread_() {
  bool prev_conn_state = false;

  while (true) {
    bool stopping = m_thread_stop_.load();

    if (stopping && m_event_queue_.size_approx() == 0) break;

    // Check channel connection (shorter timeout when stopping)
    auto timeout = stopping ? std::chrono::milliseconds(500)
                            : std::chrono::milliseconds(3000);
    auto connected = m_channel_->WaitForConnected(
        std::chrono::system_clock::now() + timeout);

    if (!prev_conn_state && connected) {
      CRANE_INFO("[Plugin] Plugind is connected.");
    }
    prev_conn_state = connected;

    if (!connected) {
      if (stopping) break;  // Don't wait forever during shutdown
      CRANE_INFO("[Plugin] Plugind is not connected. Reconnecting...");
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }

    auto approx_size = m_event_queue_.size_approx();
    if (approx_size == 0) {
      if (stopping) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }

    // Move events to local list
    std::list<HookEvent> events;
    events.resize(approx_size);

    auto actual_size =
        m_event_queue_.try_dequeue_bulk(events.begin(), approx_size);
    events.resize(actual_size);
    CRANE_DEBUG("[Plugin] Dequeued {} hook events.", actual_size);

    while (!events.empty()) {
      auto& e = events.front();
      grpc::ClientContext context;

      HookDispatchFunc f = s_hook_dispatch_funcs_[size_t(e.type)];
      auto status = (this->*f)(&context, e.msg.get());

      if (!status.ok()) {
        CRANE_ERROR(
            "[Plugin] Failed to send hook event: "
            "hook type: {}; {}; {} (code: {})",
            int(e.type), context.debug_error_string(), status.error_message(),
            int(status.error_code()));

        if (status.error_code() == grpc::UNAVAILABLE) {
          // During shutdown, drop unsent events instead of retrying
          if (stopping) {
            MarkTraceHooksCompleted_(CountTraceHookEvents_(events));
            break;
          }
          if (!events.empty()) {
            m_event_queue_.enqueue_bulk(std::make_move_iterator(events.begin()),
                                        events.size());
          }
          break;
        }
      } else {
        CRANE_TRACE("[Plugin] Hook event sent: hook type: {}", int(e.type));
      }

      MarkTraceHookCompleted_(e);
      events.pop_front();
    }
  }
}

grpc::Status PluginClient::SendStartHook_(grpc::ClientContext* context,
                                          google::protobuf::Message* msg) {
  using crane::grpc::plugin::StartHookReply;
  using crane::grpc::plugin::StartHookRequest;

  auto* request = dynamic_cast<StartHookRequest*>(msg);
  CRANE_ASSERT(request != nullptr);

  StartHookReply reply;

  CRANE_TRACE("[Plugin] Sending StartHook.");
  return m_stub_->StartHook(context, *request, &reply);
}

grpc::Status PluginClient::SendEndHook_(grpc::ClientContext* context,
                                        google::protobuf::Message* msg) {
  using crane::grpc::plugin::EndHookReply;
  using crane::grpc::plugin::EndHookRequest;

  auto* request = dynamic_cast<EndHookRequest*>(msg);
  CRANE_ASSERT(request != nullptr);

  EndHookReply reply;

  CRANE_TRACE("[Plugin] Sending EndHook.");
  return m_stub_->EndHook(context, *request, &reply);
}

grpc::Status PluginClient::SendCreateCgroupHook_(
    grpc::ClientContext* context, google::protobuf::Message* msg) {
  using crane::grpc::plugin::CreateCgroupHookReply;
  using crane::grpc::plugin::CreateCgroupHookRequest;

  auto* request = dynamic_cast<CreateCgroupHookRequest*>(msg);
  CRANE_ASSERT(request != nullptr);

  CreateCgroupHookReply reply;

  CRANE_TRACE("[Plugin] Sending CreateCgroupHook.");
  return m_stub_->CreateCgroupHook(context, *request, &reply);
}

grpc::Status PluginClient::SendDestroyCgroupHook_(
    grpc::ClientContext* context, google::protobuf::Message* msg) {
  using crane::grpc::plugin::DestroyCgroupHookReply;
  using crane::grpc::plugin::DestroyCgroupHookRequest;

  auto* request = dynamic_cast<DestroyCgroupHookRequest*>(msg);
  CRANE_ASSERT(request != nullptr);

  DestroyCgroupHookReply reply;

  CRANE_TRACE("[Plugin] Sending DestroyCgroupHook.");
  return m_stub_->DestroyCgroupHook(context, *request, &reply);
}

grpc::Status PluginClient::NodeEventHook_(grpc::ClientContext* context,
                                          google::protobuf::Message* msg) {
  using crane::grpc::plugin::NodeEventHookReply;
  using crane::grpc::plugin::NodeEventHookRequest;

  auto* request = dynamic_cast<NodeEventHookRequest*>(msg);
  CRANE_ASSERT(request != nullptr);

  NodeEventHookReply reply;

  CRANE_TRACE("[Plugin] Sending NodeEventHook");
  return m_stub_->NodeEventHook(context, *request, &reply);
}

grpc::Status PluginClient::SendUpdatePowerStateHook_(
    grpc::ClientContext* context, google::protobuf::Message* msg) {
  using crane::grpc::plugin::UpdatePowerStateHookReply;
  using crane::grpc::plugin::UpdatePowerStateHookRequest;

  auto* request = dynamic_cast<UpdatePowerStateHookRequest*>(msg);
  CRANE_ASSERT(request != nullptr);

  UpdatePowerStateHookReply reply;

  CRANE_TRACE("[Plugin] Sending UpdatePowerStateHook.");
  return m_stub_->UpdatePowerStateHook(context, *request, &reply);
}

grpc::Status PluginClient::SendRegisterCranedHook_(
    grpc::ClientContext* context, google::protobuf::Message* msg) {
  using crane::grpc::plugin::RegisterCranedHookReply;
  using crane::grpc::plugin::RegisterCranedHookRequest;

  auto* request = dynamic_cast<RegisterCranedHookRequest*>(msg);
  CRANE_ASSERT(request != nullptr);

  RegisterCranedHookReply reply;

  CRANE_TRACE("[Plugin] Sending RegisterCranedHook.");
  return m_stub_->RegisterCranedHook(context, *request, &reply);
}

grpc::Status PluginClient::SendTraceHook_(grpc::ClientContext* context,
                                          google::protobuf::Message* msg) {
  using crane::grpc::plugin::TraceHookReply;
  using crane::grpc::plugin::TraceHookRequest;

  auto* request = dynamic_cast<TraceHookRequest*>(msg);
  CRANE_ASSERT(request != nullptr);

  TraceHookReply reply;

  auto begin = std::chrono::steady_clock::now();
  /* We don't want to trace the TraceHook itself, it will cause infinite loop if
   * not handled carefully. */
  auto status = m_stub_->TraceHook(context, *request, &reply);
  auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - begin)
                        .count();
  if (!status.ok() || elapsed_ms > 1000) {
    CRANE_WARN(
        "[Plugin] TraceHookSendDiag trace_hook_request_bytes={} span_count={} "
        "grpc_status_code={} elapsed_ms={} error_message={}",
        request->ByteSizeLong(), request->spans_size(),
        static_cast<int>(status.error_code()), elapsed_ms,
        status.error_message());
  } else {
    CRANE_DEBUG(
        "[Plugin] TraceHookSendDiag trace_hook_request_bytes={} span_count={} "
        "grpc_status_code={} elapsed_ms={} error_message={}",
        request->ByteSizeLong(), request->spans_size(),
        static_cast<int>(status.error_code()), elapsed_ms,
        status.error_message());
  }
  return status;
}

grpc::Status PluginClient::SendUpdateLicensesHook_(
    grpc::ClientContext* context, google::protobuf::Message* msg) {
  using crane::grpc::plugin::UpdateLicensesHookReply;
  using crane::grpc::plugin::UpdateLicensesHookRequest;

  auto* request = dynamic_cast<UpdateLicensesHookRequest*>(msg);
  if (!request) {
    CRANE_ERROR("UpdateLicensesHookRequest is nullptr");
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "UpdateLicensesHookRequest is nullptr");
  }

  UpdateLicensesHookReply reply;
  return m_stub_->UpdateLicensesHook(context, *request, &reply);
}

void PluginClient::StartHookAsync(std::vector<crane::grpc::JobInfo> jobs) {
  if (jobs.empty()) return;

  std::vector<HookEvent> events;
  auto request = std::make_unique<crane::grpc::plugin::StartHookRequest>();
  for (auto& job : jobs) {
    auto* job_it = request->mutable_job_info_list()->Add();
    job_it->CopyFrom(job);

    if (request->ByteSizeLong() > kPluginHookMaxRequestBytes) {
      if (request->job_info_list_size() == 1) {
        CRANE_WARN(
            "[Plugin] Single StartHook JobInfo is too large: job_id={}, "
            "request_bytes={}, limit_bytes={}",
            job.job_id(), request->ByteSizeLong(), kPluginHookMaxRequestBytes);
        continue;
      }

      request->mutable_job_info_list()->RemoveLast();
      events.push_back(HookEvent{
          HookType::START,
          std::unique_ptr<google::protobuf::Message>(std::move(request))});
      request = std::make_unique<crane::grpc::plugin::StartHookRequest>();
      request->mutable_job_info_list()->Add()->CopyFrom(job);
    }
  }

  if (request->job_info_list_size() > 0) {
    events.push_back(HookEvent{
        HookType::START,
        std::unique_ptr<google::protobuf::Message>(std::move(request))});
  }

  for (auto& event : events) {
    m_event_queue_.enqueue(std::move(event));
  }
}

void PluginClient::EndHookAsync(std::vector<crane::grpc::JobInfo> jobs) {
  if (jobs.empty()) return;

  std::vector<HookEvent> events;
  auto request = std::make_unique<crane::grpc::plugin::EndHookRequest>();
  auto now = absl::ToUnixSeconds(absl::Now());
  for (auto& job : jobs) {
    auto* job_it = request->mutable_job_info_list()->Add();
    job_it->CopyFrom(job);
    job_it->mutable_elapsed_time()->set_seconds(now -
                                                job.start_time().seconds());

    if (request->ByteSizeLong() > kPluginHookMaxRequestBytes) {
      if (request->job_info_list_size() == 1) {
        CRANE_WARN(
            "[Plugin] Single EndHook JobInfo is too large: job_id={}, "
            "request_bytes={}, limit_bytes={}",
            job.job_id(), request->ByteSizeLong(), kPluginHookMaxRequestBytes);
        continue;
      }

      request->mutable_job_info_list()->RemoveLast();
      events.push_back(HookEvent{
          HookType::END,
          std::unique_ptr<google::protobuf::Message>(std::move(request))});
      request = std::make_unique<crane::grpc::plugin::EndHookRequest>();
      auto* next_job_it = request->mutable_job_info_list()->Add();
      next_job_it->CopyFrom(job);
      next_job_it->mutable_elapsed_time()->set_seconds(
          now - job.start_time().seconds());
    }
  }

  if (request->job_info_list_size() > 0) {
    events.push_back(HookEvent{
        HookType::END,
        std::unique_ptr<google::protobuf::Message>(std::move(request))});
  }

  for (auto& event : events) {
    m_event_queue_.enqueue(std::move(event));
  }
}

void PluginClient::CreateCgroupHookAsync(
    job_id_t job_id, const std::string& cgroup,
    const crane::grpc::ResourceInNodeV3& resource) {
  auto request =
      std::make_unique<crane::grpc::plugin::CreateCgroupHookRequest>();
  request->set_job_id(job_id);
  request->set_cgroup(cgroup);
  request->mutable_resource()->CopyFrom(resource);

  HookEvent e{HookType::CREATE_CGROUP,
              std::unique_ptr<google::protobuf::Message>(std::move(request))};
  m_event_queue_.enqueue(std::move(e));
}

void PluginClient::DestroyCgroupHookAsync(job_id_t job_id,
                                          const std::string& cgroup) {
  auto request =
      std::make_unique<crane::grpc::plugin::DestroyCgroupHookRequest>();
  request->set_job_id(job_id);
  request->set_cgroup(cgroup);

  HookEvent e{HookType::DESTROY_CGROUP,
              std::unique_ptr<google::protobuf::Message>(std::move(request))};
  m_event_queue_.enqueue(std::move(e));
}

void PluginClient::NodeEventHookAsync(
    std::vector<crane::grpc::plugin::CranedEventInfo> events) {
  auto request = std::make_unique<crane::grpc::plugin::NodeEventHookRequest>();
  auto* event_list = request->mutable_event_info_list();
  for (auto& event : events) {
    auto* event_it = event_list->Add();
    event_it->CopyFrom(event);
  }

  HookEvent e{HookType::INSERT_EVENT,
              std::unique_ptr<google::protobuf::Message>(std::move(request))};
  m_event_queue_.enqueue(std::move(e));
}

void PluginClient::UpdatePowerStateHookAsync(
    const std::string& craned_id, crane::grpc::CranedControlState state,
    bool enable_auto_power_control) {
  auto request =
      std::make_unique<crane::grpc::plugin::UpdatePowerStateHookRequest>();
  request->set_craned_id(craned_id);
  request->set_state(state);
  request->set_enable_auto_power_control(enable_auto_power_control);

  HookEvent e{HookType::UPDATE_POWER_STATE,
              std::unique_ptr<google::protobuf::Message>(std::move(request))};
  m_event_queue_.enqueue(std::move(e));
}

void PluginClient::RegisterCranedHookAsync(
    const std::string& craned_id,
    const std::vector<crane::NetworkInterface>& interfaces) {
  auto request =
      std::make_unique<crane::grpc::plugin::RegisterCranedHookRequest>();
  request->set_craned_id(craned_id);

  for (const auto& interface : interfaces) {
    request->mutable_network_interfaces()->Add()->CopyFrom(interface);
  }

  HookEvent e{HookType::REGISTER_CRANED,
              std::unique_ptr<google::protobuf::Message>(std::move(request))};
  m_event_queue_.enqueue(std::move(e));
}

void PluginClient::UpdateLicensesHookAsync(
    const std::vector<crane::grpc::LicenseInfo>& licenses) {
  auto request =
      std::make_unique<crane::grpc::plugin::UpdateLicensesHookRequest>();
  auto* mutable_lic = request->mutable_license_info();
  for (auto& lic : licenses) {
    mutable_lic->Add()->CopyFrom(lic);
  }

  HookEvent e{HookType::UPDATE_LICENSES,
              std::unique_ptr<google::protobuf::Message>(std::move(request))};
  m_event_queue_.enqueue(std::move(e));
}

void PluginClient::TraceHookAsync(
    std::vector<crane::grpc::plugin::SpanInfo> spans) {
  if (spans.empty()) return;

  std::vector<HookEvent> events;
  auto request = MakeTraceHookRequest();
  size_t split_count{0};
  size_t max_request_bytes{0};
  size_t oversize_span_count{0};

  auto flush_request = [&] {
    if (request->spans_size() == 0) return;
    max_request_bytes =
        std::max(max_request_bytes,
                 static_cast<size_t>(request->ByteSizeLong()));
    events.push_back(HookEvent{
        HookType::TRACE,
        std::unique_ptr<google::protobuf::Message>(std::move(request))});
    request = MakeTraceHookRequest();
  };

  for (auto& span : spans) {
    request->mutable_spans()->Add()->CopyFrom(span);
    size_t request_bytes = request->ByteSizeLong();
    if (request_bytes <= m_trace_hook_max_request_bytes_) continue;

    if (request->spans_size() == 1) {
      ++oversize_span_count;
      CRANE_WARN(
          "[Plugin] TraceHookOversizeSpanDiag trace_hook_request_bytes={} "
          "span_count=1 span_name={} trace_id={} limit_bytes={}",
          request_bytes, span.name(), span.trace_id(),
          m_trace_hook_max_request_bytes_);
      flush_request();
      ++split_count;
      continue;
    }

    request->mutable_spans()->RemoveLast();
    flush_request();
    ++split_count;
    request->mutable_spans()->Add()->CopyFrom(span);
    request_bytes = request->ByteSizeLong();
    if (request_bytes > m_trace_hook_max_request_bytes_) {
      ++oversize_span_count;
      CRANE_WARN(
          "[Plugin] TraceHookOversizeSpanDiag trace_hook_request_bytes={} "
          "span_count=1 span_name={} trace_id={} limit_bytes={}",
          request_bytes, span.name(), span.trace_id(),
          m_trace_hook_max_request_bytes_);
      flush_request();
      ++split_count;
    }
  }

  flush_request();

  if (split_count > 0 || oversize_span_count > 0) {
    CRANE_WARN(
        "[Plugin] TraceHookSplitDiag span_count={} enqueue_count={} "
        "split_count={} oversize_span_count={} max_request_bytes={} "
        "limit_bytes={}",
        spans.size(), events.size(), split_count, oversize_span_count,
        max_request_bytes, m_trace_hook_max_request_bytes_);
  } else {
    CRANE_DEBUG(
        "[Plugin] TraceHookSplitDiag span_count={} enqueue_count={} "
        "split_count={} oversize_span_count={} max_request_bytes={} "
        "limit_bytes={}",
        spans.size(), events.size(), split_count, oversize_span_count,
        max_request_bytes, m_trace_hook_max_request_bytes_);
  }

  m_trace_hooks_enqueued_.fetch_add(events.size(), std::memory_order_release);
  size_t enqueued = 0;
  for (auto& event : events) {
    if (m_event_queue_.enqueue(std::move(event))) ++enqueued;
  }
  MarkTraceHooksCompleted_(events.size() - enqueued);
}
}  // namespace plugin

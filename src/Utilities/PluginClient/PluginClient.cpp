/**
 * Copyright (c) 2024 Peking University and Peking University
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

#include "crane/PluginClient.h"

#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>
#include <google/protobuf/message.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/channel_arguments.h>
#include <unistd.h>

#include <chrono>
#include <cstddef>
#include <iterator>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "crane/GrpcHelper.h"
#include "crane/Logger.h"
#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"
#include "protos/Plugin.pb.h"
#include "protos/PublicDefs.pb.h"

namespace plugin {

PluginClient::~PluginClient() {
  m_thread_stop_.store(true);
  CRANE_TRACE("PluginClient is ending. Waiting for the thread to finish.");
  if (m_async_send_thread_.joinable()) m_async_send_thread_.join();
}

// Note that we do not support TLS in plugin yet.
void PluginClient::InitChannelAndStub(const std::string& endpoint) {
  m_channel_ = CreateUnixInsecureChannel(endpoint);
  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = CranePluginD::NewStub(m_channel_);
  m_async_send_thread_ = std::thread([this] { AsyncSendThread_(); });
}

void PluginClient::AsyncSendThread_() {
  bool prev_conn_state = false;

  while (true) {
    if (m_thread_stop_.load()) break;

    // Check channel connection
    auto connected = m_channel_->WaitForConnected(
        std::chrono::system_clock::now() + std::chrono::milliseconds(3000));

    if (!prev_conn_state && connected) {
      CRANE_INFO("[Plugin] Plugind is connected.");
    }
    prev_conn_state = connected;

    if (!connected) {
      CRANE_INFO("[Plugin] Plugind is not connected. Reconnecting...");
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }

    auto approx_size = m_event_queue_.size_approx();
    if (approx_size == 0) {
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
          // If some messages are not sent due to channel failure,
          // put them back into m_event_queue_
          if (!events.empty()) {
            m_event_queue_.enqueue_bulk(std::make_move_iterator(events.begin()),
                                        events.size());
          }
          break;
        }
      } else {
        CRANE_TRACE("[Plugin] Hook event sent: hook type: {}", int(e.type));
      }

      events.pop_front();
    }
  }
}

grpc::Status PluginClient::SendStartHook_(grpc::ClientContext* context,
                                          google::protobuf::Message* msg) {
  using crane::grpc::plugin::StartHookReply;
  using crane::grpc::plugin::StartHookRequest;

  auto request = dynamic_cast<StartHookRequest*>(msg);
  StartHookReply reply;

  CRANE_TRACE("[Plugin] Sending StartHook.");
  return m_stub_->StartHook(context, *request, &reply);
}

grpc::Status PluginClient::SendEndHook_(grpc::ClientContext* context,
                                        google::protobuf::Message* msg) {
  using crane::grpc::plugin::EndHookReply;
  using crane::grpc::plugin::EndHookRequest;

  auto request = dynamic_cast<EndHookRequest*>(msg);
  EndHookReply reply;

  CRANE_TRACE("[Plugin] Sending EndHook.");
  return m_stub_->EndHook(context, *request, &reply);
}

grpc::Status PluginClient::SendJobCheckHook_(grpc::ClientContext* context,
                                             google::protobuf::Message* msg) {
  using crane::grpc::plugin::JobCheckHookReply;
  using crane::grpc::plugin::JobCheckHookRequest;

  auto request = dynamic_cast<JobCheckHookRequest*>(msg);
  JobCheckHookReply reply;

  CRANE_TRACE("[Plugin] Sending JobCheckHook.");
  return m_stub_->JobCheckHook(context, *request, &reply);
}

void PluginClient::StartHookAsync(std::vector<crane::grpc::TaskInfo> tasks) {
  auto request = std::make_unique<crane::grpc::plugin::StartHookRequest>();
  auto* task_list = request->mutable_task_info_list();
  for (auto& task : tasks) {
    auto* task_it = task_list->Add();
    task_it->CopyFrom(task);
  }

  HookEvent e{HookType::START,
              std::unique_ptr<google::protobuf::Message>(std::move(request))};
  m_event_queue_.enqueue(std::move(e));
}

void PluginClient::EndHookAsync(std::vector<crane::grpc::TaskInfo> tasks) {
  auto request = std::make_unique<crane::grpc::plugin::EndHookRequest>();
  auto* task_list = request->mutable_task_info_list();

  auto now = absl::ToUnixSeconds(absl::Now());
  for (auto& task : tasks) {
    auto* task_it = task_list->Add();
    task_it->CopyFrom(task);
    task_it->mutable_elapsed_time()->set_seconds(now -
                                                 task.start_time().seconds());
  }

  HookEvent e{HookType::END,
              std::unique_ptr<google::protobuf::Message>(std::move(request))};
  m_event_queue_.enqueue(std::move(e));
}

void PluginClient::JobCheckHookAsync(
    std::vector<crane::grpc::JobCheckInfo> jobcheckinfo) {
  auto request = std::make_unique<crane::grpc::plugin::JobCheckHookRequest>();
  auto* jobcheck_info_list = request->mutable_jobcheck_info_list();

  for (const auto& info : jobcheckinfo) {
    auto* info_it = jobcheck_info_list->Add();
    info_it->set_taskid(info.taskid());
    info_it->set_cgroup(info.cgroup());
  }

  HookEvent e{HookType::JOBCHECK,
              std::unique_ptr<google::protobuf::Message>(std::move(request))};

  m_event_queue_.enqueue(std::move(e));
}

}  // namespace plugin

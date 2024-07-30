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

#include "PluginClient.h"

#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>
#include <google/protobuf/message.h>
#include <unistd.h>

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <thread>

#include "crane/Logger.h"
#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"

namespace Ctld {

PluginClient::~PluginClient() {
  m_thread_stop_.store(true);
  CRANE_TRACE("PluginClient is ending. Waiting for the thread to finish.");
  if (m_async_send_thread_.joinable()) m_async_send_thread_.join();
}

void PluginClient::InitChannelAndStub(const std::string& endpoint) {
  grpc::ChannelArguments channel_args;
  if (g_config.CompressedRpc)
    channel_args.SetCompressionAlgorithm(GRPC_COMPRESS_GZIP);

  if (g_config.ListenConf.UseTls) {
    // TODO: Check if domain suffix is needed.
    grpc::SslCredentialsOptions ssl_opts;
    // pem_root_certs is actually the certificate of server side rather than
    // CA certificate. CA certificate is not needed.
    // Since we use the same cert/key pair for both cranectld/craned,
    // pem_root_certs is set to the same certificate.
    ssl_opts.pem_root_certs = g_config.ListenConf.ServerCertContent;
    ssl_opts.pem_cert_chain = g_config.ListenConf.ServerCertContent;
    ssl_opts.pem_private_key = g_config.ListenConf.ServerKeyContent;

    m_channel_ = grpc::CreateCustomChannel(
        endpoint, grpc::SslCredentials(ssl_opts), channel_args);
  } else {
    m_channel_ = grpc::CreateCustomChannel(
        endpoint, grpc::InsecureChannelCredentials(), channel_args);
  }

  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = CranePluginD::NewStub(m_channel_);

  m_async_send_thread_ = std::thread([this] { AsyncSendThread_(); });
}

void PluginClient::AsyncSendThread_() {
  bool prev_conn_state = false;
  absl::Condition cond(
      +[](decltype(m_event_queue_)* queue) { return !queue->empty(); },
      &m_event_queue_);

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

    auto has_msg =
        m_event_queue_mtx_.LockWhenWithTimeout(cond, absl::Milliseconds(50));
    if (!has_msg) {
      m_event_queue_mtx_.Unlock();
      continue;
    }

    // Move events to local list and release the lock
    std::list<HookEvent> events;
    events.splice(events.begin(), std::move(m_event_queue_));
    m_event_queue_mtx_.Unlock();

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
            m_event_queue_mtx_.Lock();
            m_event_queue_.splice(m_event_queue_.begin(), std::move(events));
            m_event_queue_mtx_.Unlock();
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

grpc::Status PluginClient::SendPreRunHook_(grpc::ClientContext* context,
                                           google::protobuf::Message* msg) {
  using crane::grpc::PreRunHookReply;
  using crane::grpc::PreRunHookRequest;

  auto request = dynamic_cast<PreRunHookRequest*>(msg);
  PreRunHookReply reply;

  CRANE_TRACE("[Plugin] Sending PreRunHook.");
  return m_stub_->PreRunHook(context, *request, &reply);
}

grpc::Status PluginClient::SendPostRunHook_(grpc::ClientContext* context,
                                            google::protobuf::Message* msg) {
  using crane::grpc::PostRunHookReply;
  using crane::grpc::PostRunHookRequest;

  auto request = dynamic_cast<PostRunHookRequest*>(msg);
  PostRunHookReply reply;

  CRANE_TRACE("[Plugin] Sending PostRunHook.");
  return m_stub_->PostRunHook(context, *request, &reply);
}

grpc::Status PluginClient::SendPreCompletionHook_(
    grpc::ClientContext* context, google::protobuf::Message* msg) {
  using crane::grpc::PreCompletionHookReply;
  using crane::grpc::PreCompletionHookRequest;

  auto request = dynamic_cast<PreCompletionHookRequest*>(msg);
  PreCompletionHookReply reply;

  CRANE_TRACE("[Plugin] Sending PreCompletionHook.");
  return m_stub_->PreCompletionHook(context, *request, &reply);
}

grpc::Status PluginClient::SendPostCompletionHook_(
    grpc::ClientContext* context, google::protobuf::Message* msg) {
  using crane::grpc::PostCompletionHookReply;
  using crane::grpc::PostCompletionHookRequest;

  auto request = dynamic_cast<PostCompletionHookRequest*>(msg);
  PostCompletionHookReply reply;

  CRANE_TRACE("[Plugin] Sending PostCompletionHook.");
  return m_stub_->PostCompletionHook(context, *request, &reply);
}

void PluginClient::PreRunHookAsync() {
  auto request = new crane::grpc::PreRunHookRequest();
  // TODO: Add data to request.

  HookEvent e{HookType::PRE_RUN,
              std::unique_ptr<google::protobuf::Message>(request)};
  absl::MutexLock lock(&m_event_queue_mtx_);
  m_event_queue_.emplace_back(std::move(e));
}

void PluginClient::PostRunHookAsync() {
  auto request = new crane::grpc::PostRunHookRequest();

  HookEvent e{HookType::POST_RUN,
              std::unique_ptr<google::protobuf::Message>(request)};
  absl::MutexLock lock(&m_event_queue_mtx_);
  m_event_queue_.emplace_back(std::move(e));
}

void PluginClient::PreCompletionHookAsync() {
  auto request = new crane::grpc::PreCompletionHookRequest();

  HookEvent e{HookType::PRE_COMPLETION,
              std::unique_ptr<google::protobuf::Message>(request)};
  absl::MutexLock lock(&m_event_queue_mtx_);
  m_event_queue_.emplace_back(std::move(e));
}

void PluginClient::PostCompletionHookAsync() {
  auto request = new crane::grpc::PostCompletionHookRequest();

  HookEvent e{HookType::POST_COMPLETION,
              std::unique_ptr<google::protobuf::Message>(request)};
  absl::MutexLock lock(&m_event_queue_mtx_);
  m_event_queue_.emplace_back(std::move(e));
}

}  // namespace Ctld

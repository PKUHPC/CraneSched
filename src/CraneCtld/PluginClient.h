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

#pragma once

#include "CtldPublicDefs.h"
// Precompiled header comes first!

#include <absl/base/internal/thread_annotations.h>
#include <google/protobuf/message.h>
#include <grpcpp/channel.h>
#include <grpcpp/support/status.h>

#include <array>
#include <list>
#include <memory>
#include <thread>

#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"

namespace Ctld {

using crane::grpc::CranePluginD;
using grpc::Channel;

class PluginClient {
 public:
  PluginClient() = default;
  ~PluginClient();

  enum class HookType {
    PRE_RUN,
    POST_RUN,
    PRE_COMPLETION,
    POST_COMPLETION,

    HookTypeCount,
  };

  struct HookEvent {
    HookType type;
    std::unique_ptr<google::protobuf::Message> msg;
  };

  void InitChannelAndStub(const std::string& plugind_host);

  // These functions are used to add HookEvent into the event queue.
  void PreRunHookAsync();

  void PostRunHookAsync();

  void PreCompletionHookAsync();

  void PostCompletionHookAsync();

 private:
  // HookDispatchFunc is a function pointer type that handles different event.
  using HookDispatchFunc = grpc::Status (PluginClient::*)(
      grpc::ClientContext*, google::protobuf::Message* msg);
  grpc::Status SendPreRunHook_(grpc::ClientContext* context,
                               google::protobuf::Message* msg);
  grpc::Status SendPostRunHook_(grpc::ClientContext* context,
                                google::protobuf::Message* msg);
  grpc::Status SendPreCompletionHook_(grpc::ClientContext* context,
                                      google::protobuf::Message* msg);
  grpc::Status SendPostCompletionHook_(grpc::ClientContext* context,
                                       google::protobuf::Message* msg);

  void AsyncSendThread_();

  std::shared_ptr<Channel> m_channel_;
  std::unique_ptr<CranePluginD::Stub> m_stub_;

  std::thread m_async_send_thread_;
  std::atomic<bool> m_thread_stop_{false};

  absl::Mutex m_event_queue_mtx_;
  std::list<HookEvent> m_event_queue_ GUARDED_BY(m_event_queue_mtx_);

  // Use this array to dispatch the hook event to the corresponding function in
  // O(1) time.
  static constexpr std::array<HookDispatchFunc, size_t(HookType::HookTypeCount)>
      s_hook_dispatch_funcs_{{&PluginClient::SendPreRunHook_,
                              &PluginClient::SendPostRunHook_,
                              &PluginClient::SendPreCompletionHook_,
                              &PluginClient::SendPostCompletionHook_}};
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::PluginClient> g_plugin_client;

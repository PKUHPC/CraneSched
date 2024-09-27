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

#include <absl/container/flat_hash_map.h>
#include <concurrentqueue/concurrentqueue.h>
#include <google/protobuf/message.h>
#include <grpcpp/channel.h>
#include <grpcpp/support/status.h>

#include <array>
#include <memory>
#include <thread>
#include <vector>

#include "crane/PublicHeader.h"
#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"
#include "protos/Plugin.grpc.pb.h"
#include "protos/Plugin.pb.h"
#include "protos/PublicDefs.pb.h"

namespace plugin {

using crane::grpc::plugin::CranePluginD;
using grpc::Channel;
using moodycamel::ConcurrentQueue;

class PluginClient {
 public:
  PluginClient() = default;
  ~PluginClient();

  enum class HookType {
    START,
    END,
    JOB_MONITOR,
    HookTypeCount,
  };

  struct HookEvent {
    HookType type;
    std::unique_ptr<google::protobuf::Message> msg;
  };

  void InitChannelAndStub(const std::string& endpoint);

  // These functions are used to add HookEvent into the event queue.
  void StartHookAsync(std::vector<crane::grpc::TaskInfo> tasks);
  void EndHookAsync(std::vector<crane::grpc::TaskInfo> tasks);
  void JobMonitorHookAsync(task_id_t task_id, std::string cgroup_path);

 private:
  // HookDispatchFunc is a function pointer type that handles different
  // event.
  using HookDispatchFunc = grpc::Status (PluginClient::*)(
      grpc::ClientContext*, google::protobuf::Message* msg);
  grpc::Status SendStartHook_(grpc::ClientContext* context,
                              google::protobuf::Message* msg);
  grpc::Status SendEndHook_(grpc::ClientContext* context,
                            google::protobuf::Message* msg);
  grpc::Status SendJobMonitorHook_(grpc::ClientContext* context,
                                   google::protobuf::Message* msg);

  void AsyncSendThread_();

  std::shared_ptr<Channel> m_channel_;
  std::unique_ptr<CranePluginD::Stub> m_stub_;

  std::thread m_async_send_thread_;
  std::atomic<bool> m_thread_stop_{false};

  ConcurrentQueue<HookEvent> m_event_queue_;

  // Use this array to dispatch the hook event to the corresponding function in
  // O(1) time.
  static constexpr std::array<HookDispatchFunc, size_t(HookType::HookTypeCount)>
      s_hook_dispatch_funcs_{{&PluginClient::SendStartHook_,
                              &PluginClient::SendEndHook_,
                              &PluginClient::SendJobMonitorHook_}};
};

}  // namespace plugin

inline std::unique_ptr<plugin::PluginClient> g_plugin_client;

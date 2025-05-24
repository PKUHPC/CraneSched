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

#include "crane/Network.h"
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
    CREATE_CGROUP,
    DESTROY_CGROUP,
    INSERT_EVENT,
    UPDATE_POWER_STATE,
    REGISTER_CRANED,
    HookTypeCount,
  };

  struct HookEvent {
    HookType type;
    std::unique_ptr<google::protobuf::Message> msg;
  };

  void InitChannelAndStub(const std::string& endpoint);

  // These functions are used to add HookEvent into the event queue.
  // Launched by Ctld
  void StartHookAsync(std::vector<crane::grpc::TaskInfo> tasks);
  void EndHookAsync(std::vector<crane::grpc::TaskInfo> tasks);
  void NodeEventHookAsync(
      std::vector<crane::grpc::plugin::CranedEventInfo> events);

  // Launched by Craned
  void CreateCgroupHookAsync(task_id_t task_id, const std::string& cgroup,
                             const crane::grpc::ResourceInNode& resource);
  void DestroyCgroupHookAsync(task_id_t task_id, const std::string& cgroup);

  void UpdatePowerStateHookAsync(const std::string& craned_id,
                                 crane::grpc::CranedControlState state);

  void RegisterCranedHookAsync(
      const std::string& craned_id,
      const std::vector<crane::NetworkInterface>& interfaces);

 private:
  // HookDispatchFunc is a function pointer type that handles different
  // event.
  using HookDispatchFunc = grpc::Status (PluginClient::*)(
      grpc::ClientContext*, google::protobuf::Message* msg);
  grpc::Status SendStartHook_(grpc::ClientContext* context,
                              google::protobuf::Message* msg);
  grpc::Status SendEndHook_(grpc::ClientContext* context,
                            google::protobuf::Message* msg);
  grpc::Status SendCreateCgroupHook_(grpc::ClientContext* context,
                                     google::protobuf::Message* msg);
  grpc::Status SendDestroyCgroupHook_(grpc::ClientContext* context,
                                      google::protobuf::Message* msg);
  grpc::Status NodeEventHook_(grpc::ClientContext* context,
                              google::protobuf::Message* msg);
  grpc::Status SendUpdatePowerStateHook_(grpc::ClientContext* context,
                                         google::protobuf::Message* msg);
  grpc::Status SendRegisterCranedHook_(grpc::ClientContext* context,
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
      s_hook_dispatch_funcs_{
          {&PluginClient::SendStartHook_, &PluginClient::SendEndHook_,
           &PluginClient::SendCreateCgroupHook_,
           &PluginClient::SendDestroyCgroupHook_, &PluginClient::NodeEventHook_,
           &PluginClient::SendUpdatePowerStateHook_,
           &PluginClient::SendRegisterCranedHook_}};
};

}  // namespace plugin

inline std::unique_ptr<plugin::PluginClient> g_plugin_client;

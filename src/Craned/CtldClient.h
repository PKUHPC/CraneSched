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

#include "CranedPublicDefs.h"
// Precompiled header comes first.

#include "protos/Crane.grpc.pb.h"

namespace Craned {

using crane::grpc::CraneCtld;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

class CtldClientStateMachine {
 public:
  void SetActionRequestConfigCb(std::function<void(RegToken const&)>&& cb);
  void SetActionConfigureCb(std::function<void(RegToken const&)>&& cb);
  void SetActionRegisterCb(
      std::function<void(RegToken const&, std::vector<task_id_t> const&)>&& cb);
  void SetActionReadyCb(std::function<void()>&& cb);
  void SetActionDisconnectedCb(std::function<void()>&& cb);

  // Grpc Application-level Events:
  bool EvRecvConfigFromCtld(const crane::grpc::ConfigureCranedRequest& request);
  void EvConfigurationDone(std::optional<std::vector<task_id_t>> lost_task_ids);
  bool EvGetRegisterReply(const crane::grpc::CranedRegisterReply& reply);

  // Grpc Channel events
  void EvGrpcConnected();
  void EvGrpcConnectionFailed();
  void EvGrpcTimeout();

  bool IsReadyNow();

 private:
  enum class State : uint8_t {
    DISCONNECTED = 0,
    REQUESTING_CONFIG,
    CONFIGURING,
    REGISTERING,
    READY,
  };

  static constexpr std::string_view StateToString(State state) {
    switch (state) {
    case State::DISCONNECTED:
      return "DISCONNECTED";
    case State::REQUESTING_CONFIG:
      return "REQUESTING_CONFIG";
    case State::CONFIGURING:
      return "CONFIGURING";
    case State::REGISTERING:
      return "REGISTERING";
    case State::READY:
      return "READY";
    }
    return "UNKNOWN";
  }

  // State Actions:
  void ActionRequestConfig_();
  void ActionConfigure_();
  void ActionRegister_(std::vector<task_id_t>&& non_existent_tasks);
  void ActionReady_();
  void ActionDisconnected_();

  std::function<void(RegToken const&)> m_action_request_config_cb_;
  std::function<void(RegToken const&)> m_action_configure_cb_;
  std::function<void(RegToken const&, std::vector<task_id_t> const&)>
      m_action_register_cb_;
  std::function<void()> m_action_ready_cb_;
  std::function<void()> m_action_disconnected_cb_;

  State m_state_ ABSL_GUARDED_BY(m_mtx_) = State::DISCONNECTED;

  std::optional<RegToken> m_reg_token_ ABSL_GUARDED_BY(m_mtx_);
  std::optional<std::chrono::time_point<std::chrono::steady_clock>>
      m_last_op_time_ ABSL_GUARDED_BY(m_mtx_){std::nullopt};
  std::vector<task_id_t> m_nonexistent_jobs_ ABSL_GUARDED_BY(m_mtx_);

  absl::Mutex m_mtx_;
};

class CtldClient {
 public:
  CtldClient() = default;
  ~CtldClient();

  CtldClient(CtldClient const&) = delete;
  CtldClient(CtldClient&&) = delete;

  CtldClient& operator=(CtldClient const&) = delete;
  CtldClient& operator=(CtldClient&&) = delete;

  void SetCranedId(CranedId const& craned_id) { m_craned_id_ = craned_id; }

  void Init();

  /***
   * InitChannelAndStub the CtldClient to CraneCtld.
   * @param server_address The "[Address]:[Port]" of CraneCtld.
   * @return
   * If CraneCtld is successfully connected, kOk is returned. <br>
   * If CraneCtld cannot be connected within 3s, kConnectionTimeout is
   * returned.
   */
  void InitGrpcChannel(const std::string& server_address);

  void AddGrpcCtldConnectedCb(std::function<void()> cb);

  void AddGrpcCtldDisconnectedCb(std::function<void()> cb);

  void StartGrpcCtldConnection() { m_connection_start_notification_.Notify(); }

  void TaskStatusChangeAsync(TaskStatusChangeQueueElem&& task_status_change);

  bool CancelTaskStatusChangeByTaskId(task_id_t task_id,
                                      crane::grpc::TaskStatus* new_status);

  [[nodiscard]] CranedId GetCranedId() const { return m_craned_id_; };

 private:
  bool RequestConfigFromCtld_(RegToken const& token);

  bool CranedRegister_(RegToken const& token,
                       std::vector<task_id_t> const& nonexistent_jobs);

  void AsyncSendThread_();

  absl::Mutex m_task_status_change_mtx_;

  std::list<TaskStatusChangeQueueElem> m_task_status_change_list_
      ABSL_GUARDED_BY(m_task_status_change_mtx_);

  std::thread m_async_send_thread_;
  std::atomic_bool m_thread_stop_{false};

  std::shared_ptr<Channel> m_ctld_channel_;

  std::unique_ptr<CraneCtld::Stub> m_stub_;

  CranedId m_craned_id_;

  std::atomic<bool> m_grpc_has_initialized_;
  std::vector<std::function<void()>> m_on_ctld_connected_cb_chain_;
  std::vector<std::function<void()>> m_on_ctld_disconnected_cb_chain_;

  absl::Notification m_connection_start_notification_;
};

}  // namespace Craned

inline std::unique_ptr<Craned::CtldClientStateMachine> g_ctld_client_sm;
inline std::unique_ptr<Craned::CtldClient> g_ctld_client;
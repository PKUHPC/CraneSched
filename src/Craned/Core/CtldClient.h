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

using crane::grpc::CraneCtldForInternal;
using crane::grpc::CranedRegisterReply;
using crane::grpc::CranedRegisterRequest;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

class CtldClientStateMachine {
 public:
  CtldClientStateMachine();
  ~CtldClientStateMachine();
  CtldClientStateMachine(CtldClientStateMachine const&) = delete;
  CtldClientStateMachine& operator=(CtldClientStateMachine const&) = delete;
  CtldClientStateMachine(CtldClientStateMachine&&) = delete;
  CtldClientStateMachine& operator=(CtldClientStateMachine&&) = delete;
  void SetActionRequestConfigCb(std::function<void(RegToken const&)>&& cb);

  struct ConfigureArg {
    crane::grpc::ConfigureCranedRequest req;
  };

  template <typename ConfigureCb>
    requires std::invocable<ConfigureCb, ConfigureArg const&>
  void AddActionConfigureCb(ConfigureCb&& cb, CallbackInvokeMode invoke_mode,
                            bool consume) {
    using FuncType = std::function<void(ConfigureArg const&)>;

    m_action_configure_cb_list_.emplace_back(
        CallbackWrapper<FuncType, ConfigureArg const&>{
            .cb = FuncType(std::forward<ConfigureCb>(cb)),
            .mode = invoke_mode,
            .consume = consume});
  }

  struct RegisterArg {
    RegToken token;
    std::set<job_id_t> lost_jobs;
    std::set<step_id_t> lost_steps;
  };

  void SetActionRegisterCb(std::function<void(RegisterArg const&)>&& cb);
  void SetActionReadyCb(std::function<void()>&& cb);
  void SetActionDisconnectedCb(std::function<void()>&& cb);
  void SetActionTimeoutCb(std::function<void()>&& cb);

  // Grpc Application-level Events:
  bool EvRecvConfigFromCtld(const crane::grpc::ConfigureCranedRequest& request);
  void EvConfigurationDone(std::optional<std::set<job_id_t>> lost_jobs,
                           std::optional<std::set<step_id_t>> lost_steps);
  bool EvGetRegisterReply(const crane::grpc::CranedRegisterReply& reply,
                          const RegToken& token);
  void EvPingFailed();
  void EvPingSuccess();

  // Grpc Channel events
  void EvGrpcConnected();
  void EvGrpcConnectionFailed();

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
  void ActionConfigure_(const crane::grpc::ConfigureCranedRequest& config_req);
  void ActionRegister_(std::set<task_id_t>&& lost_jobs,
                       std::set<task_id_t>&& lost_tasks);
  void ActionReady_();
  void ActionTimeout_();
  void ActionDisconnected_();

  // Internal application-level events
  void EvTimeout_();

  std::function<void(RegToken const&)> m_action_request_config_cb_;

  std::list<CallbackWrapper<std::function<void(ConfigureArg const&)>,
                            ConfigureArg const&>>
      m_action_configure_cb_list_ ABSL_GUARDED_BY(m_cb_mutex_);

  std::function<void(RegisterArg const&)> m_action_register_cb_;
  std::function<void()> m_action_ready_cb_;
  std::function<void()> m_action_timeout_cb_;
  std::function<void()> m_action_disconnected_cb_;

  absl::Mutex m_cb_mutex_;

  State m_state_ ABSL_GUARDED_BY(m_mtx_) = State::DISCONNECTED;

  std::optional<RegToken> m_reg_token_ ABSL_GUARDED_BY(m_mtx_);
  std::optional<std::chrono::time_point<std::chrono::steady_clock>>
      m_last_op_time_ ABSL_GUARDED_BY(m_mtx_){std::nullopt};

  absl::Mutex m_mtx_;
  std::atomic_bool m_stopping_{false};

  std::thread m_uvw_thread_;

  std::shared_ptr<uvw::loop> m_uvw_loop_;
  std::shared_ptr<uvw::timer_handle> m_timeout_handle_;
  std::atomic_bool m_check_reg_timeout_{false};
};

class CtldClient {
 public:
  CtldClient();
  void Shutdown();
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

  void SetPingSuccessCb(std::function<void()>&& cb);
  void SetPingFailedCb(std::function<void()>&& cb);

  void StartGrpcCtldConnection() { m_connection_start_notification_.Notify(); }

  void StartPingCtld() { m_ping_ctld_ = true; }
  void StopPingCtld() { m_ping_ctld_ = false; }
  void UpdateLastActiveTime() {
    m_last_active_time_ = std::chrono::steady_clock::now();
  }

  void StepStatusChangeAsync(TaskStatusChangeQueueElem&& task_status_change);

  [[nodiscard]] std::set<step_id_t> GetAllStepStatusChangeId();

  [[nodiscard]] CranedId GetCranedId() const { return m_craned_id_; };

 private:
  bool RequestConfigFromCtld_(RegToken const& token);

  bool CranedRegister_(RegToken const& token,
                       std::set<job_id_t> const& lost_jobs,
                       std::set<step_id_t> const& lost_steps);

  void AsyncSendThread_();
  bool Ping_();

  void SendStatusChanges_();

  absl::Mutex m_step_status_change_mtx_;

  std::list<TaskStatusChangeQueueElem> m_step_status_change_list_
      ABSL_GUARDED_BY(m_step_status_change_mtx_);

  std::thread m_async_send_thread_;
  std::atomic_bool m_stopping_{false};

  std::shared_ptr<Channel> m_ctld_channel_;

  std::unique_ptr<CraneCtldForInternal::Stub> m_stub_;

  CranedId m_craned_id_;

  std::atomic<bool> m_grpc_has_initialized_;
  std::vector<std::function<void()>> m_on_ctld_connected_cb_chain_;
  std::vector<std::function<void()>> m_on_ctld_disconnected_cb_chain_;
  std::function<void()> m_ping_success_cb_;
  std::function<void()> m_ping_failed_cb_;

  absl::Notification m_connection_start_notification_;

  std::thread m_uvw_thread_;

  std::shared_ptr<uvw::loop> m_uvw_loop_;
  std::shared_ptr<uvw::timer_handle> m_ping_handle_;
  std::atomic_bool m_ping_ctld_{false};
  std::atomic<std::chrono::steady_clock::time_point> m_last_active_time_;
};

}  // namespace Craned

inline std::unique_ptr<Craned::CtldClientStateMachine> g_ctld_client_sm;
inline std::unique_ptr<Craned::CtldClient> g_ctld_client;
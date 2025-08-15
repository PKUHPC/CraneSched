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

#include "CtldClient.h"

#include "CranedServer.h"
#include "JobManager.h"
#include "SupervisorKeeper.h"
#include "crane/GrpcHelper.h"

namespace Craned {

void CtldClientStateMachine::SetActionRequestConfigCb(
    std::function<void(RegToken const&)>&& cb) {
  m_action_request_config_cb_ = std::move(cb);
}

void CtldClientStateMachine::SetActionReadyCb(std::function<void()>&& cb) {
  m_action_ready_cb_ = std::move(cb);
}

void CtldClientStateMachine::SetActionDisconnectedCb(
    std::function<void()>&& cb) {
  m_action_disconnected_cb_ = std::move(cb);
}

void CtldClientStateMachine::SetActionRegisterCb(
    std::function<void(RegisterArg const&)>&& cb) {
  m_action_register_cb_ = std::move(cb);
}

bool CtldClientStateMachine::EvRecvConfigFromCtld(
    const crane::grpc::ConfigureCranedRequest& request) {
  absl::MutexLock lk(&m_mtx_);
  m_last_op_time_ = std::chrono::steady_clock::now();

  if (m_state_ != State::REQUESTING_CONFIG) {
    CRANE_WARN(
        "EvRecvConfigFromCtld triggered at incorrect state {}. Ignoring.",
        static_cast<int>(m_state_));
    return false;
  }

  if (request.ok() && request.has_token() &&
      request.token() == m_reg_token_.value()) {
    m_state_ = State::CONFIGURING;
    ActionConfigure_(request);
    return true;
  } else {
    if (!request.ok())
      CRANE_WARN("ConfigureCranedRequest failed from Ctld.");
    else if (!request.has_token()) {
      CRANE_WARN("No token in ConfigureCranedRequest from Ctld.");
    } else
      CRANE_DEBUG(
          "ConfigureCraned failed. Expected to recv token: {} but got {}",
          ProtoTimestampToString(m_reg_token_.value()),
          ProtoTimestampToString(request.token()));

    m_state_ = State::REQUESTING_CONFIG;
    ActionRequestConfig_();

    return false;
  }
}

void CtldClientStateMachine::EvConfigurationDone(
    std::optional<std::set<job_id_t>> lost_jobs,
    std::optional<std::set<step_id_t>> lost_steps) {
  absl::MutexLock lk(&m_mtx_);
  m_last_op_time_ = std::chrono::steady_clock::now();

  if (m_state_ != State::CONFIGURING) {
    CRANE_WARN("EvGetRegisterReply triggered at incorrect state {}. Ignoring.",
               static_cast<int>(m_state_));
    return;
  }

  if (lost_jobs.has_value() && lost_steps.has_value()) {
    m_state_ = State::REGISTERING;
    ActionRegister_(std::move(lost_jobs.value()),
                    std::move(lost_steps.value()));
  } else {
    m_state_ = State::REQUESTING_CONFIG;
    ActionRequestConfig_();
  }
}

bool CtldClientStateMachine::EvGetRegisterReply(
    const crane::grpc::CranedRegisterReply& reply, const RegToken& token) {
  absl::MutexLock lk(&m_mtx_);
  m_last_op_time_ = std::chrono::steady_clock::now();

  if (m_state_ != State::REGISTERING) {
    CRANE_WARN("EvGetRegisterReply triggered at incorrect state {}. Ignoring.",
               static_cast<int>(m_state_));
    return false;
  }

  if (m_reg_token_ != token) {
    CRANE_TRACE("Register token mismatch when recv register reply.");
    return false;
  }

  if (reply.ok()) {
    m_state_ = State::READY;
    ActionReady_();
  } else {
    m_state_ = State::REQUESTING_CONFIG;
    ActionRequestConfig_();
  }

  return reply.ok();
}

void CtldClientStateMachine::EvGrpcConnected() {
  absl::MutexLock lk(&m_mtx_);
  m_last_op_time_ = std::chrono::steady_clock::now();

  if (m_state_ != State::DISCONNECTED) {
    CRANE_WARN("EvGrpcTimeout triggered at incorrect state {}. Ignoring.",
               static_cast<int>(m_state_));
    return;
  }

  m_state_ = State::REQUESTING_CONFIG;
  ActionRequestConfig_();
}

void CtldClientStateMachine::EvGrpcConnectionFailed() {
  absl::MutexLock lk(&m_mtx_);
  m_last_op_time_ = std::chrono::steady_clock::now();

  m_state_ = State::DISCONNECTED;
  ActionDisconnected_();
}

void CtldClientStateMachine::EvGrpcTimeout() {
  absl::MutexLock lk(&m_mtx_);
  m_last_op_time_ = std::chrono::steady_clock::now();

  if (m_state_ != State::REQUESTING_CONFIG && m_state_ != State::CONFIGURING) {
    CRANE_WARN("EvGrpcTimeout triggered at incorrect state {}. Ignoring.",
               static_cast<int>(m_state_));
    return;
  }

  m_state_ = State::REQUESTING_CONFIG;
  ActionRequestConfig_();
}

bool CtldClientStateMachine::IsReadyNow() {
  absl::MutexLock lk(&m_mtx_);
  return m_state_ == State::READY;
}

void CtldClientStateMachine::ActionRequestConfig_() {
  CRANE_DEBUG("Ctld client state machine has entered state {}",
              StateToString(m_state_));

  if (m_reg_token_.has_value()) CRANE_DEBUG("Reset register token.");
  m_reg_token_ = ToProtoTimestamp(std::chrono::steady_clock::now());

  g_thread_pool->detach_task(
      [tok = m_reg_token_.value(), this] { m_action_request_config_cb_(tok); });
}

void CtldClientStateMachine::ActionConfigure_(
    const crane::grpc::ConfigureCranedRequest& config_req) {
  CRANE_DEBUG("Ctld client state machine has entered state {}",
              StateToString(m_state_));

  if (config_req.job_map_size() != 0)
    CRANE_TRACE(
        "Recv ctld job: [{}],task: [{}]",
        absl::StrJoin(config_req.job_map() | std::ranges::views::keys, ","),
        absl::StrJoin(config_req.job_tasks_map() | std::ranges::views::keys,
                      ","));

  absl::MutexLock lk(&m_cb_mutex_);
  auto it = m_action_configure_cb_list_.begin();
  while (it != m_action_configure_cb_list_.end()) {
    auto& cb_wrapper = *it;
    if (cb_wrapper.mode == CallbackInvokeMode::SYNC) {
      cb_wrapper.cb({config_req});
    } else {
      g_thread_pool->detach_task(
          [cb = cb_wrapper.cb, config_req] { cb({config_req}); });
    }

    if (cb_wrapper.consume)
      it = m_action_configure_cb_list_.erase(it);
    else
      ++it;
  }
}

void CtldClientStateMachine::ActionRegister_(std::set<task_id_t>&& lost_jobs,
                                             std::set<task_id_t>&& lost_tasks) {
  CRANE_DEBUG("Ctld client state machine has entered state {}",
              StateToString(m_state_));

  g_thread_pool->detach_task(
      [tok = m_reg_token_.value(), lost_jobs, lost_tasks, this] mutable {
        m_action_register_cb_(
            {.token = tok, .lost_jobs = lost_jobs, .lost_steps = lost_tasks});
      });
}

void CtldClientStateMachine::ActionReady_() {
  CRANE_DEBUG("Ctld client state machine has entered state {}",
              StateToString(m_state_));
  g_server->SetGrpcSrvReady(true);
  g_thread_pool->detach_task([this] {
    if (m_action_ready_cb_) m_action_ready_cb_();
  });
}

void CtldClientStateMachine::ActionDisconnected_() {
  CRANE_DEBUG("Ctld client state machine has entered state {}",
              StateToString(m_state_));
  g_server->SetGrpcSrvReady(false);
  g_thread_pool->detach_task([this] {
    if (m_action_disconnected_cb_) m_action_disconnected_cb_();
  });
}

void CtldClient::Shutdown() {
  m_stopping_ = true;
  m_step_status_change_mtx_.Lock();
  CRANE_INFO("Cleaning up status changes in CtldClient");
  SendStatusChanges_();
}

CtldClient::~CtldClient() {
  CRANE_TRACE("Waiting for CtldClient thread to finish.");
  if (m_async_send_thread_.joinable()) m_async_send_thread_.join();
}

void CtldClient::Init() {
  g_ctld_client_sm->SetActionRequestConfigCb(
      [this](RegToken const& token) { RequestConfigFromCtld_(token); });

  g_ctld_client_sm->AddActionConfigureCb(
      [](CtldClientStateMachine::ConfigureArg const& arg) {
        auto token = arg.req.token();
        auto job_ids = arg.req.job_map() | std::ranges::views::keys |
                       std::ranges::to<std::set<task_id_t>>();
        auto step_ids = arg.req.job_tasks_map() | std::ranges::views::keys |
                        std::ranges::to<std::set<task_id_t>>();
        CRANE_DEBUG("Configuring action for token {}.",
                    ProtoTimestampToString(token));

        std::set exact_job_ids = g_job_mgr->GetAllocatedJobs();
        std::set<task_id_t> lost_jobs{};
        std::set<task_id_t> invalid_jobs{};
        std::ranges::set_difference(job_ids, exact_job_ids,
                                    std::inserter(lost_jobs, lost_jobs.end()));
        std::ranges::set_difference(
            exact_job_ids, job_ids,
            std::inserter(invalid_jobs, invalid_jobs.end()));

        std::set<task_id_t> lost_tasks{};
        std::set<task_id_t> invalid_tasks{};

        std::set exact_step_ids = g_supervisor_keeper->GetRunningSteps();
        std::ranges::set_difference(
            step_ids, exact_step_ids,
            std::inserter(lost_tasks, lost_tasks.end()));
        std::ranges::set_difference(
            exact_step_ids, step_ids,
            std::inserter(invalid_tasks, invalid_tasks.end()));

        g_ctld_client_sm->EvConfigurationDone(lost_jobs, lost_tasks);
        if (!invalid_tasks.empty()) {
          CRANE_DEBUG("Terminating orphaned tasks: [{}].",
                      absl::StrJoin(invalid_tasks, ","));
          for (auto task_id : invalid_tasks) {
            g_job_mgr->MarkStepAsOrphanedAndTerminateAsync(task_id);
          }
        }
        if (!invalid_jobs.empty()) {
          CRANE_DEBUG("Freeing invalid jobs: [{}].",
                      absl::StrJoin(invalid_jobs, ","));
          g_job_mgr->FreeJobs(std::move(invalid_jobs));
        }
      },
      CallbackInvokeMode::ASYNC, false);

  g_ctld_client_sm->SetActionRegisterCb(
      [this](CtldClientStateMachine::RegisterArg const& arg) {
        CranedRegister_(arg.token, arg.lost_jobs, arg.lost_steps);
      });

  AddGrpcCtldConnectedCb([] { g_ctld_client_sm->EvGrpcConnected(); });

  AddGrpcCtldDisconnectedCb([] { g_ctld_client_sm->EvGrpcConnectionFailed(); });
}

void CtldClient::InitGrpcChannel(const std::string& server_address) {
  m_grpc_has_initialized_.store(true, std::memory_order::release);

  grpc::ChannelArguments channel_args;
  SetGrpcClientKeepAliveChannelArgs(&channel_args);

  if (g_config.CompressedRpc)
    channel_args.SetCompressionAlgorithm(GRPC_COMPRESS_GZIP);

  if (g_config.ListenConf.TlsConfig.Enabled)
    m_ctld_channel_ = CreateTcpTlsCustomChannelByHostname(
        server_address, g_config.CraneCtldForInternalListenPort,
        g_config.ListenConf.TlsConfig.TlsCerts,
        g_config.ListenConf.TlsConfig.DomainSuffix, channel_args);
  else
    m_ctld_channel_ = CreateTcpInsecureCustomChannel(
        server_address, g_config.CraneCtldForInternalListenPort, channel_args);

  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = CraneCtldForInternal::NewStub(m_ctld_channel_);

  m_async_send_thread_ = std::thread([this] { AsyncSendThread_(); });
}

void CtldClient::AddGrpcCtldConnectedCb(std::function<void()> cb) {
  if (m_grpc_has_initialized_.load(std::memory_order::acquire)) {
    CRANE_ERROR("CtldClient has been initialized, cannot add callback.");
    return;
  }

  m_on_ctld_connected_cb_chain_.push_back(std::move(cb));
}

void CtldClient::AddGrpcCtldDisconnectedCb(std::function<void()> cb) {
  if (m_grpc_has_initialized_.load(std::memory_order::acquire)) {
    CRANE_ERROR("CtldClient has been initialized, cannot add callback.");
    return;
  }

  m_on_ctld_disconnected_cb_chain_.push_back(std::move(cb));
}

void CtldClient::StepStatusChangeAsync(
    TaskStatusChangeQueueElem&& task_status_change) {
  absl::MutexLock lock(&m_step_status_change_mtx_);
  m_step_status_change_list_.emplace_back(std::move(task_status_change));
}

std::set<task_id_t> CtldClient::GetAllStepStatusChangeId() {
  absl::MutexLock lock(&m_step_status_change_mtx_);
  return m_step_status_change_list_ |
         std::ranges::views::transform(
             [](const TaskStatusChangeQueueElem& elem) {
               return elem.step_id;
             }) |
         std::ranges::to<std::set<task_id_t>>();
}

bool CtldClient::RequestConfigFromCtld_(RegToken const& token) {
  CRANE_DEBUG("Requesting config from CraneCtld...");

  crane::grpc::CranedTriggerReverseConnRequest req;
  req.set_craned_id(g_config.CranedIdOfThisNode);
  *req.mutable_token() = token;

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(1));

  google::protobuf::Empty reply;

  grpc::Status status =
      m_stub_->CranedTriggerReverseConn(&context, req, &reply);
  if (!status.ok()) {
    CRANE_ERROR("Notify CranedConnected failed: {}, {}",
                static_cast<int>(status.error_code()), status.error_message());
    return false;
  }
  return true;
}

bool CtldClient::CranedRegister_(RegToken const& token,
                                 std::set<job_id_t> const& lost_jobs,
                                 std::set<step_id_t> const& lost_steps) {
  CRANE_DEBUG("Sending CranedRegister.");

  crane::grpc::CranedRegisterRequest ready_request;
  ready_request.set_craned_id(g_config.CranedIdOfThisNode);
  *ready_request.mutable_token() = token;

  auto* grpc_meta = ready_request.mutable_remote_meta();
  auto& dres = g_config.CranedRes[g_config.CranedIdOfThisNode]->dedicated_res;

  grpc_meta->mutable_dres_in_node()->CopyFrom(
      static_cast<crane::grpc::DedicatedResourceInNode>(dres));
  grpc_meta->set_craned_version(CRANE_VERSION_STRING);
  grpc_meta->set_config_crc(g_config.ConfigCrcVal);

  const SystemRelInfo& sys_info = g_config.CranedMeta.SysInfo;
  auto* grpc_sys_rel_info = grpc_meta->mutable_sys_rel_info();
  grpc_sys_rel_info->set_name(sys_info.name);
  grpc_sys_rel_info->set_release(sys_info.release);
  grpc_sys_rel_info->set_version(sys_info.version);

  grpc_meta->mutable_craned_start_time()->set_seconds(
      ToUnixSeconds(g_config.CranedMeta.CranedStartTime));
  grpc_meta->mutable_system_boot_time()->set_seconds(
      ToUnixSeconds(g_config.CranedMeta.SystemBootTime));
  grpc_meta->mutable_lost_jobs()->Assign(lost_jobs.begin(), lost_jobs.end());
  grpc_meta->mutable_lost_tasks()->Assign(lost_steps.begin(), lost_steps.end());

  for (const auto& interface : g_config.CranedMeta.NetworkInterfaces) {
    *grpc_meta->add_network_interfaces() = interface;
  }

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(1));

  crane::grpc::CranedRegisterReply ready_reply;
  auto status = m_stub_->CranedRegister(&context, ready_request, &ready_reply);
  if (!status.ok()) {
    CRANE_DEBUG("CranedRegister failed: {}", status.error_message());
    return false;
  }

  return g_ctld_client_sm->EvGetRegisterReply(ready_reply, token);
}

void CtldClient::AsyncSendThread_() {
  // Wait Craned grpc server initialization.
  m_connection_start_notification_.WaitForNotification();

  // Variables for grpc channel maintaining.
  grpc_connectivity_state prev_grpc_state{GRPC_CHANNEL_IDLE};
  grpc_connectivity_state grpc_state;
  bool prev_connected = false, connected = false;

  // Variable for TaskStatusChange sending part.
  absl::Condition cond(
      +[](decltype(m_step_status_change_list_)* queue) {
        return !queue->empty();
      },
      &m_step_status_change_list_);

  while (true) {
    if (m_stopping_) break;

    grpc_state = m_ctld_channel_->GetState(true);
    connected = prev_grpc_state == GRPC_CHANNEL_READY;

    if (!connected) {
      if (prev_connected) {  // Edge triggered: grpc connected -> disconnected.
        CRANE_TRACE("Channel to CraneCtlD is disconnected.");
        for (const auto& cb : m_on_ctld_disconnected_cb_chain_) cb();
      }

      std::chrono::time_point ddl =
          std::chrono::system_clock::now() + std::chrono::seconds(3);
      bool timeout = m_ctld_channel_->WaitForStateChange(prev_grpc_state, ddl);
      if (!timeout) continue;  // No state change. No need to update prev state.

      prev_grpc_state = grpc_state;
      prev_connected = connected;
      continue;
    }

    // Connected case:
    if (!prev_connected) {  // Edge triggered: grpc disconnected -> connected.
      CRANE_TRACE("Channel to CraneCtlD is connected.");
      for (const auto& cb : m_on_ctld_connected_cb_chain_) cb();
    }

    prev_connected = connected;
    prev_grpc_state = grpc_state;

    if (g_ctld_client_sm->IsReadyNow() == false) continue;

    // TaskStatusChange sending is done in this grpc channel maintaining thread
    // if the channel is connected.
    // This is equivalent to sharing some time slice with grpc sending,
    // i.e. this thread is maintaining grpc channel and sending rpc at the same
    // time.

    bool has_msg = m_step_status_change_mtx_.LockWhenWithTimeout(
        cond, absl::Milliseconds(50));
    if (!has_msg) {
      m_step_status_change_mtx_.Unlock();
    } else {
      SendStatusChanges_();
    }
  }
}

void CtldClient::SendStatusChanges_() {
  std::list<TaskStatusChangeQueueElem> changes;
  changes.splice(changes.begin(), std::move(m_step_status_change_list_));
  m_step_status_change_mtx_.Unlock();

  while (!changes.empty()) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(5));
    crane::grpc::StepStatusChangeRequest request;
    crane::grpc::StepStatusChangeReply reply;
    grpc::Status status;

    auto status_change = changes.front();

    CRANE_TRACE("Sending TaskStatusChange for task #{}", status_change.step_id);

    request.set_craned_id(m_craned_id_);
    request.set_task_id(status_change.step_id);
    request.set_new_status(status_change.new_status);
    request.set_exit_code(status_change.exit_code);
    if (status_change.reason.has_value())
      request.set_reason(status_change.reason.value());

    status = m_stub_->StepStatusChange(&context, request, &reply);
    if (!status.ok()) {
      CRANE_ERROR(
          "Failed to send TaskStatusChange: "
          "{{TaskId: {}, NewStatus: {}}}, reason: {} | {}, code: {}",
          status_change.step_id, static_cast<int>(status_change.new_status),
          status.error_message(), context.debug_error_string(),
          static_cast<int>(status.error_code()));

      if (status.error_code() == grpc::UNAVAILABLE) {
        if (m_stopping_) return;
        // If some messages are not sent due to channel failure,
        // put them back into m_task_status_change_list_
        if (!changes.empty()) {
          m_step_status_change_mtx_.Lock();
          m_step_status_change_list_.splice(m_step_status_change_list_.begin(),
                                            std::move(changes));
          m_step_status_change_mtx_.Unlock();
        }
        // Sleep for a while to avoid too many retries.
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        break;
      } else
        changes.pop_front();
    } else {
      CRANE_TRACE("StepStatusChange for step #{} sent. reply.ok={}",
                  status_change.step_id, reply.ok());
      changes.pop_front();
    }
  }
}

}  // namespace Craned

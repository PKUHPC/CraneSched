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

#include "../../cmake-build-debug/_deps/gtest-src/googlemock/include/gmock/gmock-actions.h"
#include "CranedServer.h"
#include "crane/GrpcHelper.h"

namespace Craned {

void CtldClientStateMachine::EvRecvConfigFromCtld(
    const crane::grpc::ConfigureCranedRequest& request) {
  absl::MutexLock lk(&m_mtx_);
  m_last_op_time_ = std::chrono::steady_clock::now();

  if (m_state_ != State::REQUESTING_CONFIG) {
    CRANE_WARN(
        "EvRecvConfigFromCtld triggered at incorrect state {}. Ignoring.",
        static_cast<int>(m_state_));
    return;
  }

  if (request.ok() && request.has_token() && request.token() == m_reg_token_) {
    m_state_ = State::CONFIGURING;
    ActionConfigure_();
  } else {
    CRANE_WARN("ConfigureCranedRequest failed from Ctld.");
    m_state_ = State::REQUESTING_CONFIG;
    ActionRequestConfig_();
  }
}

void CtldClientStateMachine::EvGetRegisterReply(
    const crane::grpc::CranedRegisterReply& reply) {
  absl::MutexLock lk(&m_mtx_);
  m_last_op_time_ = std::chrono::steady_clock::now();

  if (m_state_ != State::CONFIGURING) {
    CRANE_WARN("EvGetRegisterReply triggered at incorrect state {}. Ignoring.",
               static_cast<int>(m_state_));
    return;
  }

  if (reply.ok()) {
    m_state_ = State::READY;
    ActionReady_();
  } else {
    m_state_ = State::REQUESTING_CONFIG;
    ActionRequestConfig_();
  }
}

void CtldClientStateMachine::EvGrpcConnected() {
  absl::MutexLock lk(&m_mtx_);
  m_last_op_time_ = std::chrono::steady_clock::now();

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

CtldClient::~CtldClient() {
  m_thread_stop_ = true;

  CRANE_TRACE("CtldClient is ending. Waiting for the thread to finish.");
  if (m_async_send_thread_.joinable()) m_async_send_thread_.join();
}

void CtldClient::InitChannelAndStub(const std::string& server_address) {
  m_has_initialized_.store(true, std::memory_order::release);

  grpc::ChannelArguments channel_args;
  SetGrpcClientKeepAliveChannelArgs(&channel_args);

  if (g_config.CompressedRpc)
    channel_args.SetCompressionAlgorithm(GRPC_COMPRESS_GZIP);

  if (g_config.ListenConf.UseTls)
    m_ctld_channel_ = CreateTcpTlsCustomChannelByHostname(
        server_address, g_config.CraneCtldListenPort,
        g_config.ListenConf.TlsCerts, channel_args);
  else
    m_ctld_channel_ = CreateTcpInsecureCustomChannel(
        server_address, g_config.CraneCtldListenPort, channel_args);

  // std::unique_ptr will automatically release the dangling stub.
  m_stub_ = CraneCtld::NewStub(m_ctld_channel_);

  m_async_send_thread_ = std::thread([this] { AsyncSendThread_(); });
}

void CtldClient::AddCtldConnectedCb(std::function<void()> cb) {
  if (m_has_initialized_.load(std::memory_order::acquire)) {
    CRANE_ERROR("CtldClient has been initialized, cannot add callback.");
    return;
  }

  m_on_ctld_connected_cb_chain_.push_back(std::move(cb));
}

void CtldClient::AddCtldDisconnectedCb(std::function<void()> cb) {
  if (m_has_initialized_.load(std::memory_order::acquire)) {
    CRANE_ERROR("CtldClient has been initialized, cannot add callback.");
    return;
  }

  m_on_ctld_disconnected_cb_chain_.push_back(std::move(cb));
}

void CtldClient::SetState(CtldClientState new_state) {
  absl::MutexLock lk(&m_register_mutex_);
  m_last_operation_time_ = std::chrono::steady_clock::now();
  m_state_ = new_state;
}

void CtldClient::StartRegister(const std::vector<task_id_t>& nonexistent_jobs,
                               const RegToken& token) {
  absl::MutexLock lk(&m_register_mutex_);
  m_state_ = REGISTER_SENDING;
  m_last_operation_time_.reset();
  m_nonexistent_jobs_ = nonexistent_jobs;
  m_token_ = token;
}

void CtldClient::TaskStatusChangeAsync(
    TaskStatusChangeQueueElem&& task_status_change) {
  absl::MutexLock lock(&m_task_status_change_mtx_);
  m_task_status_change_list_.emplace_back(std::move(task_status_change));
}

bool CtldClient::CancelTaskStatusChangeByTaskId(
    task_id_t task_id, crane::grpc::TaskStatus* new_status) {
  absl::MutexLock lock(&m_task_status_change_mtx_);

  size_t num_removed{0};

  for (auto it = m_task_status_change_list_.begin();
       it != m_task_status_change_list_.end();)
    if (it->task_id == task_id) {
      num_removed++;
      *new_status = it->new_status;
      it = m_task_status_change_list_.erase(it);
    } else
      ++it;

  CRANE_ASSERT_MSG(num_removed <= 1,
                   "TaskStatusChange should happen at most once "
                   "for a single running task!");

  return num_removed >= 1;
}

bool CtldClient::RequestConfigFromCtld_() {
  CRANE_DEBUG("Requesting config from CraneCtld...");

  crane::grpc::CranedConnectedCtldNotify req;
  req.set_craned_id(g_config.CranedIdOfThisNode);
  *req.mutable_token() = g_server->GetNextRegisterToken();

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(1));

  google::protobuf::Empty reply;

  grpc::Status status = m_stub_->CranedConnectedCtld(&context, req, &reply);
  if (!status.ok()) {
    CRANE_ERROR("Notify CranedConnected failed: {}", status.error_message());
    return false;
  }
  return true;
}

bool CtldClient::RegisterOnCtld_() {
  CRANE_DEBUG("Sending CranedRegister.");

  crane::grpc::CranedRegisterRequest ready_request;
  ready_request.set_craned_id(g_config.CranedIdOfThisNode);
  *ready_request.mutable_token() = m_token_.value();
  m_token_.reset();

  auto* grpc_meta = ready_request.mutable_remote_meta();
  auto& dres = g_config.CranedRes[g_config.CranedIdOfThisNode]->dedicated_res;

  grpc_meta->mutable_dres_in_node()->CopyFrom(
      static_cast<crane::grpc::DedicatedResourceInNode>(dres));
  grpc_meta->set_craned_version(CRANE_VERSION_STRING);

  const SystemRelInfo& sys_info = g_config.CranedMeta.SysInfo;
  auto* grpc_sys_rel_info = grpc_meta->mutable_sys_rel_info();
  grpc_sys_rel_info->set_name(sys_info.name);
  grpc_sys_rel_info->set_release(sys_info.release);
  grpc_sys_rel_info->set_version(sys_info.version);

  grpc_meta->mutable_craned_start_time()->set_seconds(
      ToUnixSeconds(g_config.CranedMeta.CranedStartTime));
  grpc_meta->mutable_system_boot_time()->set_seconds(
      ToUnixSeconds(g_config.CranedMeta.SystemBootTime));

  grpc_meta->mutable_nonexistent_jobs()->Assign(m_nonexistent_jobs_.begin(),
                                                m_nonexistent_jobs_.end());

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(1));

  crane::grpc::CranedRegisterReply ready_reply;
  auto status = m_stub_->CranedRegister(&context, ready_request, &ready_reply);
  if (!status.ok()) {
    CRANE_DEBUG("CranedRegister failed: {}", status.error_message());
    return false;
  }

  if (ready_reply.ok()) {
    g_server->SetGrpcSrvReady(true);
    CRANE_INFO("Craned successfully Up.");
    return true;
  } else {
    CRANE_WARN("Craned register get reply of false.");
    return false;
  }
}

void CtldClient::AsyncSendThread_() {
  m_start_connecting_notification_.WaitForNotification();

  std::this_thread::sleep_for(std::chrono::seconds(1));

  absl::Condition cond(
      +[](decltype(m_task_status_change_list_)* queue) {
        return !queue->empty();
      },
      &m_task_status_change_list_);
  const auto reset_notify_sending = [this] {
    m_last_operation_time_.reset();
    m_state_ = NOTIFY_SENDING;
  };

  bool prev_connected = false;
  uint retry_attempt = 1;
  while (true) {
    if (m_thread_stop_) break;

    grpc_connectivity_state pre_grpc_state = m_ctld_channel_->GetState(true);

    auto cur_grpc_state = pre_grpc_state;
    if (pre_grpc_state == GRPC_CHANNEL_CONNECTING ||
        pre_grpc_state == GRPC_CHANNEL_IDLE ||
        pre_grpc_state == GRPC_CHANNEL_TRANSIENT_FAILURE) {
      CRANE_TRACE("Current channel status {},waiting for StateChange.",
                  GrpcConnectivityStateName(pre_grpc_state));
      m_ctld_channel_->WaitForStateChange(
          pre_grpc_state,
          std::chrono::system_clock::now() + std::chrono::seconds(3));

      cur_grpc_state = m_ctld_channel_->GetState(false);
      CRANE_TRACE("New ctld client channel status {}.",
                  GrpcConnectivityStateName(cur_grpc_state));
    }

    if (pre_grpc_state != cur_grpc_state)
      CRANE_TRACE("CtldClient grpc state change: {} -> {}.",
                  GrpcConnectivityStateName(pre_grpc_state),
                  GrpcConnectivityStateName(cur_grpc_state));

    bool connected = (cur_grpc_state == GRPC_CHANNEL_READY);
    if (!prev_connected && connected) {
      CRANE_TRACE("Channel to CraneCtlD is connected.");
      for (const auto& cb : m_on_ctld_connected_cb_chain_) cb();

      retry_attempt = 1;
      {
        absl::MutexLock lk(&m_register_mutex_);
        m_state_ = NOTIFY_SENDING;
      }
    } else if (!connected && prev_connected) {
      CRANE_TRACE("Channel to CraneCtlD is disconnected.");

      for (const auto& cb : m_on_ctld_disconnected_cb_chain_) cb();

      {
        absl::MutexLock lk(&m_register_mutex_);
        m_state_ = DISCONNECTED;
      }
    }

    prev_connected = connected;

    if (!connected) {
      uint64_t delay_time =
          retry_attempt >= 10 ? 10'000 : retry_attempt * 1'000;

      CRANE_INFO(
          "Channel to CraneCtlD is not connected. Reconnecting after {} "
          "seconds.",
          delay_time / 1000);
      retry_attempt++;
      std::this_thread::sleep_for(std::chrono::milliseconds(delay_time));
      continue;
    }

    // Connected
    {
      absl::MutexLock lk(&m_register_mutex_);
      auto now = std::chrono::steady_clock::now();

      if (m_last_operation_time_.has_value() &&
          now - m_last_operation_time_.value() <=
              std::chrono::milliseconds(kRegisterOperationTimeoutMs)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100 /*MS*/));
        continue;
      }

      if (m_state_ != ESTABLISHED) {
        // If any step timeout we start from notify.
        if (m_last_operation_time_.has_value()) {
          // Register never timeout.
          CRANE_DEBUG("Last operation of notify timeout. Start form notify.");
          reset_notify_sending();
        }

        if (m_state_ == NOTIFY_SENDING) {
          m_last_operation_time_ = now;
          if (RequestConfigFromCtld_())
            m_state_ = NOTIFY_SENT;
          else
            m_last_operation_time_.reset();
        } else if (m_state_ == REGISTER_SENDING) {
          m_last_operation_time_ = now;
          if (RegisterOnCtld_()) {
            m_state_ = ESTABLISHED;
          } else {
            reset_notify_sending();
          }
        }
      }
    }

    bool has_msg = m_task_status_change_mtx_.LockWhenWithTimeout(
        cond, absl::Milliseconds(50));
    if (!has_msg) {
      m_task_status_change_mtx_.Unlock();
      continue;
    }

    std::list<TaskStatusChangeQueueElem> changes;
    changes.splice(changes.begin(), std::move(m_task_status_change_list_));
    m_task_status_change_mtx_.Unlock();

    while (!changes.empty()) {
      grpc::ClientContext context;
      crane::grpc::TaskStatusChangeRequest request;
      crane::grpc::TaskStatusChangeReply reply;
      grpc::Status status;

      auto status_change = changes.front();

      CRANE_TRACE("Sending TaskStatusChange for task #{}",
                  status_change.task_id);

      request.set_craned_id(m_craned_id_);
      request.set_task_id(status_change.task_id);
      request.set_new_status(status_change.new_status);
      request.set_exit_code(status_change.exit_code);
      if (status_change.reason.has_value())
        request.set_reason(status_change.reason.value());

      status = m_stub_->TaskStatusChange(&context, request, &reply);
      if (!status.ok()) {
        CRANE_ERROR(
            "Failed to send TaskStatusChange: "
            "{{TaskId: {}, NewStatus: {}}}, reason: {} | {}, code: {}",
            status_change.task_id, int(status_change.new_status),
            status.error_message(), context.debug_error_string(),
            int(status.error_code()));

        if (status.error_code() == grpc::UNAVAILABLE) {
          // If some messages are not sent due to channel failure,
          // put them back into m_task_status_change_list_
          if (!changes.empty()) {
            m_task_status_change_mtx_.Lock();
            m_task_status_change_list_.splice(
                m_task_status_change_list_.begin(), std::move(changes));
            m_task_status_change_mtx_.Unlock();
          }
          break;
        } else
          changes.pop_front();
      } else {
        CRANE_TRACE("TaskStatusChange for task #{} sent. reply.ok={}",
                    status_change.task_id, reply.ok());
        changes.pop_front();
      }
    }
  }
}

}  // namespace Craned

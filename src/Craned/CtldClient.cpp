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
#include "TaskManager.h"
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

void CtldClientStateMachine::SetActionConfigureCb(
    std::function<void(ConfigureArg const&)>&& cb) {
  m_action_configure_cb_ = std::move(cb);
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
    std::optional<std::set<task_id_t>> lost_jobs,
    std::optional<std::set<task_id_t>> lost_tasks) {
  absl::MutexLock lk(&m_mtx_);
  m_last_op_time_ = std::chrono::steady_clock::now();

  if (m_state_ != State::CONFIGURING) {
    CRANE_WARN("EvGetRegisterReply triggered at incorrect state {}. Ignoring.",
               static_cast<int>(m_state_));
    return;
  }

  if (lost_jobs.has_value() && lost_tasks.has_value()) {
    m_state_ = State::REGISTERING;
    ActionRegister_(std::move(lost_jobs.value()),
                    std::move(lost_tasks.value()));
  } else {
    m_state_ = State::REQUESTING_CONFIG;
    ActionRequestConfig_();
  }
}

bool CtldClientStateMachine::EvGetRegisterReply(
    const crane::grpc::CranedRegisterReply& reply) {
  absl::MutexLock lk(&m_mtx_);
  m_last_op_time_ = std::chrono::steady_clock::now();

  if (m_state_ != State::REGISTERING) {
    CRANE_WARN("EvGetRegisterReply triggered at incorrect state {}. Ignoring.",
               static_cast<int>(m_state_));
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
    const crane::grpc::ConfigureCranedRequest& configure_req) {
  CRANE_DEBUG("Ctld client state machine has entered state {}",
              StateToString(m_state_));
  auto job_ids = configure_req.job_map() | std::ranges::views::keys |
                 std::ranges::to<std::set<task_id_t>>();
  auto task_ids = configure_req.job_tasks_map() | std::ranges::views::keys |
                  std::ranges::to<std::set<task_id_t>>();
  if (!job_ids.empty())
    CRANE_TRACE("Recv ctld job: [{}],task: [{}]", absl::StrJoin(job_ids, ","),
                absl::StrJoin(task_ids, ","));

  g_thread_pool->detach_task([tok = m_reg_token_.value(),
                              job_ids = std::move(job_ids),
                              task_ids = std::move(task_ids), this] mutable {
    m_action_configure_cb_(
        {.token = tok, .job_ids = job_ids, .task_ids = task_ids});
  });
}

void CtldClientStateMachine::ActionRegister_(std::set<task_id_t>&& lost_jobs,
                                             std::set<task_id_t>&& lost_tasks) {
  CRANE_DEBUG("Ctld client state machine has entered state {}",
              StateToString(m_state_));

  g_thread_pool->detach_task(
      [tok = m_reg_token_.value(), lost_jobs, lost_tasks, this] mutable {
        m_action_register_cb_(
            {.token = tok, .lost_jobs = lost_jobs, .lost_tasks = lost_tasks});
      });
}

void CtldClientStateMachine::ActionReady_() {
  CRANE_DEBUG("Ctld client state machine has entered state {}",
              StateToString(m_state_));

  g_thread_pool->detach_task([this] { m_action_ready_cb_(); });
}

void CtldClientStateMachine::ActionDisconnected_() {
  CRANE_DEBUG("Ctld client state machine has entered state {}",
              StateToString(m_state_));

  g_thread_pool->detach_task([this] { m_action_disconnected_cb_(); });
}

CtldClient::~CtldClient() {
  m_thread_stop_ = true;

  CRANE_TRACE("CtldClient is ending. Waiting for the thread to finish.");
  if (m_async_send_thread_.joinable()) m_async_send_thread_.join();
}

void CtldClient::Init() {
  g_ctld_client_sm->SetActionDisconnectedCb([] {
    // The implementation now is auto-reconnecting.
    // Just do nothing here.
  });

  g_ctld_client_sm->SetActionRequestConfigCb(
      [this](RegToken const& token) { RequestConfigFromCtld_(token); });

  g_ctld_client_sm->SetActionConfigureCb(
      [](CtldClientStateMachine::ConfigureArg const& arg) {
        CRANE_DEBUG("Configuring action for token {}.",
                    ProtoTimestampToString(arg.token));

        std::set exact_job_ids = g_job_mgr->GetAllocatedJobs();
        std::set<task_id_t> lost_jobs{};
        std::set<task_id_t> invalid_jobs{};
        std::ranges::set_difference(arg.job_ids, exact_job_ids,
                                    std::inserter(lost_jobs, lost_jobs.end()));
        std::ranges::set_difference(
            exact_job_ids, arg.job_ids,
            std::inserter(invalid_jobs, invalid_jobs.end()));

        std::set exact_task_ids = g_task_mgr->QueryRunningTasksAsync();
        std::set<task_id_t> lost_tasks{};
        std::set<task_id_t> invalid_tasks{};
        std::ranges::set_difference(
            arg.task_ids, exact_task_ids,
            std::inserter(lost_tasks, lost_tasks.end()));
        std::ranges::set_difference(
            exact_task_ids, arg.job_ids,
            std::inserter(invalid_tasks, invalid_tasks.end()));

        g_ctld_client_sm->EvConfigurationDone(lost_jobs, lost_tasks);
        if (!invalid_tasks.empty()) {
          CRANE_DEBUG("Terminating orphaned tasks: [{}].",
                      absl::StrJoin(invalid_tasks, ","));
          std::latch latch(invalid_tasks.size());
          for (auto task_id : invalid_tasks) {
            g_thread_pool->detach_task([task_id, &latch] {
              g_task_mgr->MarkTaskAsOrphanedAndTerminateAsync(task_id).wait();
              latch.count_down();
            });
          }
          latch.wait();
        }
        if (!invalid_jobs.empty()) {
          CRANE_DEBUG("Freeing invalid jobs: [{}].",
                      absl::StrJoin(invalid_jobs, ","));
          g_job_mgr->FreeJobs(invalid_jobs);
        }
      });

  g_ctld_client_sm->SetActionRegisterCb(
      [this](CtldClientStateMachine::RegisterArg const& arg) {
        CranedRegister_(arg.token, arg.lost_jobs, arg.lost_tasks);
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

  std::shared_ptr<Channel> channel;

  if (g_config.ListenConf.UseTls)
    channel = CreateTcpTlsCustomChannelByHostname(server_address, listen_port,
                                                  g_config.ListenConf.TlsCerts,
                                                  channel_args);
  else
    channel = CreateTcpInsecureCustomChannel(server_address, listen_port,
                                             channel_args);

  m_ctld_channels_.emplace_back(channel);

  // std::unique_ptr will automatically release the dangling stub.
  m_stubs_.emplace_back(CraneCtld::NewStub(channel));
}

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

void CtldClient::TaskStatusChangeAsync(
    TaskStatusChangeQueueElem&& task_status_change) {
  absl::MutexLock lock(&m_task_status_change_mtx_);
  m_task_status_change_list_.emplace_back(std::move(task_status_change));
}

std::set<task_id_t> CtldClient::GetAllTaskStatusChangeId() {
  absl::MutexLock lock(&m_task_status_change_mtx_);
  return m_task_status_change_list_ |
         std::ranges::views::transform(
             [](const TaskStatusChangeQueueElem& elem) {
               return elem.task_id;
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
    CRANE_ERROR("Notify CranedConnected failed: {}", status.error_message());
    return false;
  }
  return true;
}

bool CtldClient::CranedRegister_(RegToken const& token,
                                 std::set<task_id_t> const& lost_jobs,
                                 std::set<task_id_t> const& lost_tasks) {
  CRANE_DEBUG("Sending CranedRegister.");

  crane::grpc::CranedRegisterRequest ready_request;
  ready_request.set_craned_id(g_config.CranedIdOfThisNode);
  *ready_request.mutable_token() = token;

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
  grpc_meta->mutable_lost_jobs()->Assign(lost_jobs.begin(), lost_jobs.end());
  grpc_meta->mutable_lost_tasks()->Assign(lost_tasks.begin(), lost_tasks.end());

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

  return g_ctld_client_sm->EvGetRegisterReply(ready_reply);
}

void CtldClient::AsyncSendThread_() {
  // Wait Craned grpc server initialization.
  m_connection_start_notification_.WaitForNotification();

  // Variables for grpc channel maintaining.
  grpc_connectivity_state prev_grpc_state{GRPC_CHANNEL_IDLE};
  grpc_connectivity_state grpc_state;
  bool prev_connected = false, connected;

  // Variable for TaskStatusChange sending part.
  absl::Condition cond(
      +[](decltype(m_task_status_change_list_)* queue) {
        return !queue->empty();
      },
      &m_task_status_change_list_);

  while (true) {
    if (m_thread_stop_) break;

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

      status = m_stubs_[m_cur_leader_id_]->TaskStatusChange(&context, request,
                                                            &reply);
      if (!status.ok()) {
        CRANE_ERROR(
            "Failed to send TaskStatusChange: "
            "{{TaskId: {}, NewStatus: {}}}, reason: {} | {}, code: {}",
            status_change.task_id, int(status_change.new_status),
            status.error_message(), context.debug_error_string(),
            int(status.error_code()));

        if (status.error_code() == grpc::UNAVAILABLE ||
            status.error_code() == grpc::CANCELLED) {
          // If some messages are not sent due to channel failure,
          // put them back into m_task_status_change_list_
          CRANE_INFO(
              "Pause TaskStatusChange and reconnect with the server to retry.");
          if (!changes.empty()) {
            m_task_status_change_mtx_.Lock();
            m_task_status_change_list_.splice(
                m_task_status_change_list_.begin(), std::move(changes));
            m_task_status_change_mtx_.Unlock();
          }
          // Sleep for a while to avoid too many retries.
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          break;
        } else {
          CRANE_INFO("Unexpected grpc error code, discard this request.");
          changes.pop_front();
        }
      } else {
        CRANE_TRACE(
            "TaskStatusChange for task #{} sent to server #{}. reply.ok={}",
            status_change.task_id, m_cur_leader_id_.load(), reply.ok());
        changes.pop_front();
      }
    }
  }
}

int CtldClient::ConnectToServersAndFindLeader_(int prev_leader_id) {
  int len = m_ctld_channels_.size();
  static std::vector<bool> prev_conn_states(len, false);

  bool connected = m_ctld_channels_[prev_leader_id]->WaitForConnected(
      std::chrono::system_clock::now() + std::chrono::seconds(3));

  if (connected) {
    if (!prev_conn_states[prev_leader_id]) {
      prev_conn_states[prev_leader_id] = true;
      int check_id = g_ctld_client->OnCraneCtldConnected(prev_leader_id);

      if (check_id == prev_leader_id || check_id == -2)
        return check_id;
      else if (check_id == -1) {
        CRANE_ERROR("Raft Server Error! leader id -1");
        m_cur_leader_id_ = 0;
        return -1;
      } else {
        m_cur_leader_id_ = check_id;
        return -2;
      }
    } else {
      prev_conn_states[prev_leader_id] = true;
      return prev_leader_id;
    }
  } else {  // start query
    prev_conn_states[prev_leader_id] = false;

    for (int i = 1; i < len; i++) {
      int id = (prev_leader_id + i) % len;

      connected = m_ctld_channels_[id]->WaitForConnected(
          std::chrono::system_clock::now() + std::chrono::seconds(3));

      if (connected) {
        if (!prev_conn_states[id]) {
          prev_conn_states[id] = true;

          int leader_id = g_ctld_client->OnCraneCtldConnected(id);
          if (leader_id == id || leader_id == -2)
            return leader_id;
          else if (leader_id == -1) {
            CRANE_ERROR("Raft Server Error! leader id -1");
            m_cur_leader_id_ = 0;
            return -1;
          } else {
            m_cur_leader_id_ = leader_id;
            return -2;
          }
        }
      } else {
        prev_conn_states[id] = false;
      }
    }
    return -2;
  }
}

}  // namespace Craned

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
#include "crane/String.h"

namespace Craned {

CtldClientStateMachine::CtldClientStateMachine() {
  m_logger_ = g_runtime_status.conn_logger;
  m_uvw_loop_ = uvw::loop::create();

  m_timeout_handle_ = m_uvw_loop_->resource<uvw::timer_handle>();
  m_timeout_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle&) {
        if (m_check_reg_timeout_) EvTimeout_();
        return false;
      });

  m_timeout_handle_->start(
      std::chrono::seconds(g_config.CranedConf.CtldTimeoutSec),
      std::chrono::seconds(g_config.CranedConf.CtldTimeoutSec));
  CRANE_LOGGER_TRACE(m_logger_, "Start check ctld timeout of {} sec.",
                     g_config.CranedConf.CtldTimeoutSec);

  m_uvw_thread_ = std::thread([this] {
    util::SetCurrentThreadName("CtldCSMTimeThr");
    auto idle_handle = m_uvw_loop_->resource<uvw::idle_handle>();
    idle_handle->on<uvw::idle_event>(
        [this](const uvw::idle_event&, uvw::idle_handle& h) {
          if (m_stopping_) {
            h.parent().walk([](auto&& h) { h.close(); });
            h.parent().stop();
            return;
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
        });
    if (idle_handle->start() != 0) {
      CRANE_ERROR(
          "Failed to start the idle event in CtldClientStateMachine loop.");
    }
    m_uvw_loop_->run();
  });
}

CtldClientStateMachine::~CtldClientStateMachine() {
  m_stopping_ = true;
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}

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
    CRANE_LOGGER_WARN(
        m_logger_,
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
    if (!request.ok()) {
      CRANE_LOGGER_WARN(m_logger_, "ConfigureCranedRequest failed from Ctld.");
      m_state_ = State::REQUESTING_CONFIG;
      ActionRequestConfig_();
    } else if (!request.has_token()) {
      CRANE_LOGGER_DEBUG(m_logger_,
                         "No token in ConfigureCranedRequest from Ctld.");
      m_state_ = State::REQUESTING_CONFIG;
      ActionRequestConfig_();

    } else {
      CRANE_LOGGER_DEBUG(m_logger_,
                         "ConfigureCraned failed. Expected to recv token: {} "
                         "but got {}, waiting for correct reply.",
                         ProtoTimestampToString(m_reg_token_.value()),
                         ProtoTimestampToString(request.token()));
    }

    return false;
  }
}

void CtldClientStateMachine::EvConfigurationDone(
    std::optional<std::set<job_id_t>> lost_jobs,
    std::optional<std::unordered_map<job_id_t, std::set<step_id_t>>>
        lost_steps) {
  absl::MutexLock lk(&m_mtx_);
  m_last_op_time_ = std::chrono::steady_clock::now();

  if (m_state_ != State::CONFIGURING) {
    CRANE_LOGGER_WARN(
        m_logger_,
        "EvGetRegisterReply triggered at incorrect state {}. Ignoring.",
        StateToString(m_state_));
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
    CRANE_LOGGER_WARN(
        m_logger_,
        "EvGetRegisterReply triggered at incorrect state {}. Ignoring.",
        StateToString(m_state_));
    return false;
  }

  if (m_reg_token_ != token) {
    CRANE_LOGGER_TRACE(m_logger_,
                       "Register token mismatch when recv register reply.");
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
    CRANE_LOGGER_WARN(
        m_logger_, "EvGrpcConnected triggered at incorrect state {}. Ignoring.",
        StateToString(m_state_));
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

void CtldClientStateMachine::EvTimeout_() {
  absl::MutexLock lk(&m_mtx_);
  if (!m_last_op_time_.has_value() || m_state_ == State::DISCONNECTED) return;
  if (m_last_op_time_.value() +
          std::chrono::seconds(g_config.CranedConf.CtldTimeoutSec) >
      std::chrono::steady_clock::now())
    return;
  CRANE_LOGGER_DEBUG(
      m_logger_,
      "CtldClient register timeout, current state {}, starting handshake.",
      StateToString(m_state_));
  m_last_op_time_ = std::chrono::steady_clock::now();
  m_state_ = State::REQUESTING_CONFIG;
  g_server->SetGrpcSrvReady(false);
  ActionRequestConfig_();
}

void CtldClientStateMachine::EvPingFailed() {
  absl::MutexLock lk(&m_mtx_);
  CRANE_LOGGER_DEBUG(m_logger_,
                     "Ping failed, current state {}, starting new handshake.",
                     StateToString(m_state_));
  m_last_op_time_ = std::chrono::steady_clock::now();

  g_server->SetGrpcSrvReady(false);
  m_state_ = State::REQUESTING_CONFIG;
  ActionRequestConfig_();
}

void CtldClientStateMachine::EvPingSuccess() {
  CRANE_LOGGER_DEBUG(m_logger_, "CtldClient ping success.");
  absl::MutexLock lk(&m_mtx_);
  m_last_op_time_ = std::chrono::steady_clock::now();
}

bool CtldClientStateMachine::IsReadyNow() {
  absl::MutexLock lk(&m_mtx_);
  return m_state_ == State::READY;
}

void CtldClientStateMachine::ActionRequestConfig_() {
  CRANE_LOGGER_DEBUG(m_logger_,
                     "Ctld client state machine has entered state {}, start "
                     "check register operation timeout.",
                     StateToString(m_state_));
  m_check_reg_timeout_ = true;
  if (m_reg_token_.has_value()) CRANE_DEBUG("Reset register token.");
  m_reg_token_ = ToProtoTimestamp(std::chrono::steady_clock::now());
  if (m_action_request_config_cb_)
    g_thread_pool->detach_task([tok = m_reg_token_.value(), this] {
      m_action_request_config_cb_(tok);
    });
}

void CtldClientStateMachine::ActionConfigure_(
    const crane::grpc::ConfigureCranedRequest& config_req) {
  CRANE_LOGGER_DEBUG(m_logger_,
                     "Ctld client state machine has entered state {}",
                     StateToString(m_state_));

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

void CtldClientStateMachine::ActionRegister_(
    std::set<job_id_t>&& lost_jobs,
    std::unordered_map<job_id_t, std::set<step_id_t>>&& lost_steps) {
  CRANE_LOGGER_DEBUG(m_logger_,
                     "Ctld client state machine has entered state {}",
                     StateToString(m_state_));
  if (m_action_register_cb_)
    g_thread_pool->detach_task(
        [tok = m_reg_token_.value(), lost_jobs, lost_steps, this] mutable {
          m_action_register_cb_(
              {.token = tok, .lost_jobs = lost_jobs, .lost_steps = lost_steps});
        });
}

void CtldClientStateMachine::ActionReady_() {
  CRANE_LOGGER_DEBUG(m_logger_,
                     "Ctld client state machine has entered state {}, stop "
                     "check register operation timeout",
                     StateToString(m_state_));
  m_check_reg_timeout_ = false;
  g_server->SetGrpcSrvReady(true);
  g_ctld_client->StartPingCtld();
  if (m_action_ready_cb_)
    g_thread_pool->detach_task([this] { m_action_ready_cb_(); });
}

void CtldClientStateMachine::ActionDisconnected_() {
  CRANE_LOGGER_DEBUG(m_logger_,
                     "Ctld client state machine has entered state {}, stop "
                     "check register operation timeout",
                     StateToString(m_state_));
  m_check_reg_timeout_ = false;
  g_server->SetGrpcSrvReady(false);
  if (m_action_disconnected_cb_)
    g_thread_pool->detach_task([this] { m_action_disconnected_cb_(); });
}

void CtldClient::Shutdown() {
  m_stopping_ = true;
  m_step_status_change_mtx_.Lock();
  CRANE_INFO("Cleaning up status changes in CtldClient");
  SendStatusChanges_();
}

CtldClient::CtldClient() {
  m_uvw_loop_ = uvw::loop::create();

  m_ping_handle_ = m_uvw_loop_->resource<uvw::timer_handle>();
  m_ping_handle_->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle& h) {
        if (!m_ping_ctld_) return false;
        if (m_last_active_time_.load(std::memory_order_acquire) +
                std::chrono::seconds(g_config.CranedConf.PingIntervalSec) >
            std::chrono::steady_clock::now())
          return true;

        const bool success = Ping_();
        if (!success)
          m_ping_ctld_ = false;
        else
          UpdateLastActiveTime();

        return success;
      });

  m_uvw_thread_ = std::thread([this] {
    util::SetCurrentThreadName("PingCtldThr");
    auto idle_handle = m_uvw_loop_->resource<uvw::idle_handle>();
    idle_handle->on<uvw::idle_event>(
        [this](const uvw::idle_event&, uvw::idle_handle& h) {
          if (m_stopping_) {
            h.parent().walk([](auto&& h) { h.close(); });
            h.parent().stop();
            return;
          }
          if (m_ping_ctld_ && !m_ping_handle_->active()) {
            m_ping_handle_->start(
                std::chrono::seconds(g_config.CranedConf.PingIntervalSec),
                std::chrono::seconds(g_config.CranedConf.PingIntervalSec));
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
        });
    if (idle_handle->start() != 0) {
      CRANE_ERROR("Failed to start the idle event in CtldClient loop.");
    }
    m_uvw_loop_->run();
  });
  m_last_active_time_ = std::chrono::steady_clock::time_point{};
}

CtldClient::~CtldClient() {
  CRANE_TRACE("Waiting for CtldClient thread to finish.");
  if (m_async_send_thread_.joinable()) m_async_send_thread_.join();
  if (m_uvw_thread_.joinable()) m_uvw_thread_.join();
}

void CtldClient::Init() {
  g_ctld_client_sm->SetActionRequestConfigCb(
      [this](RegToken const& token) { RequestConfigFromCtld_(token); });

  g_ctld_client_sm->AddActionConfigureCb(
      [](CtldClientStateMachine::ConfigureArg const& arg) {
        // FIXME: step recovery
        auto token = arg.req.token();

        std::map<job_id_t, std::set<step_id_t>> job_steps_id_map;
        for (const auto& [job_id, job_steps] : arg.req.job_steps()) {
          job_steps_id_map[job_id] = job_steps.steps() | std::views::keys |
                                     std::ranges::to<std::set>();
        }
        auto job_ids = job_steps_id_map | std::views::keys;

        CRANE_LOGGER_DEBUG(g_runtime_status.conn_logger,
                           "Configuring action for token {}. Steps [{}]",
                           ProtoTimestampToString(token),
                           util::JobStepsToString(job_steps_id_map));

        // std::map keys are ordered, so we can use set difference.
        std::map exact_job_steps = g_job_mgr->GetAllocatedJobSteps();

        for (auto status_change_steps =
                 g_ctld_client->GetAllFinishStepStatusChangeId();
             auto& [k, v] : status_change_steps) {
          exact_job_steps[k].insert(v.begin(), v.end());
        }
        auto exact_job_ids = exact_job_steps | std::views::keys;
        std::set<job_id_t> lost_jobs{};
        std::set<job_id_t> invalid_jobs{};
        std::set<job_id_t> valid_jobs{};

        std::ranges::set_difference(job_ids, exact_job_ids,
                                    std::inserter(lost_jobs, lost_jobs.end()));
        std::ranges::set_difference(
            exact_job_ids, job_ids,
            std::inserter(invalid_jobs, invalid_jobs.end()));
        std::ranges::set_intersection(
            job_ids, exact_job_ids,
            std::inserter(valid_jobs, valid_jobs.end()));

        std::unordered_map<job_id_t, std::set<step_id_t>> lost_steps{};
        std::unordered_map<job_id_t, std::set<step_id_t>> invalid_steps{};
        std::unordered_map<job_id_t, std::set<step_id_t>> valid_steps{};
        for (auto job_id : valid_jobs) {
          const auto& ctld_steps = job_steps_id_map.at(job_id);
          const auto& craned_steps = exact_job_steps.at(job_id);
          std::ranges::set_difference(
              ctld_steps, craned_steps,
              std::inserter(lost_steps[job_id], lost_steps[job_id].end()));
          std::ranges::set_difference(
              craned_steps, ctld_steps,
              std::inserter(invalid_steps[job_id],
                            invalid_steps[job_id].end()));
          std::ranges::set_intersection(
              ctld_steps, craned_steps,
              std::inserter(valid_steps[job_id], valid_steps[job_id].end()));
        }
        std::set<job_id_t> completing_jobs{};
        std::unordered_map<job_id_t, std::unordered_set<step_id_t>>
            completing_steps{};
        for (const auto& [job_id, steps] : valid_steps) {
          for (const auto& step_id : steps) {
            auto status =
                arg.req.job_steps().at(job_id).step_status().at(step_id);
            CRANE_TRACE("[Step #{}.{}] is {}", job_id, step_id, status);
            if (status == StepStatus::Completing) {
              if (step_id == kDaemonStepId) {
                CRANE_TRACE("[Job #{}] is completing", job_id);
                completing_jobs.insert(job_id);
              } else {
                CRANE_TRACE("[Step #{}.{}] is completing", job_id, step_id);
                completing_steps[job_id].insert(step_id);
              }
            }
          }
        }

        g_ctld_client_sm->EvConfigurationDone(lost_jobs, lost_steps);
        if (!invalid_steps.empty()) {
          CRANE_INFO("Terminating orphaned : [{}].",
                     util::JobStepsToString(invalid_steps));
          for (auto [job_id, steps] : invalid_steps) {
            for (auto step_id : steps)
              g_job_mgr->MarkStepAsOrphanedAndTerminateAsync(job_id, step_id);
          }
        }
        if (!invalid_jobs.empty()) {
          CRANE_INFO("Freeing invalid jobs: [{}].",
                     absl::StrJoin(invalid_jobs, ","));
          g_job_mgr->FreeJobs(std::move(invalid_jobs));
        }
        if (!completing_jobs.empty()) {
          CRANE_INFO("Terminating completing jobs: [{}].",
                     absl::StrJoin(completing_jobs, ","));
          g_job_mgr->FreeJobs(std::move(completing_jobs));
        }
        if (!completing_steps.empty()) {
          CRANE_INFO("Terminating completing steps: [{}].",
                     util::JobStepsToString(completing_steps));
          g_job_mgr->FreeSteps(std::move(completing_steps));
        }
      },
      CallbackInvokeMode::ASYNC, false);

  g_ctld_client_sm->SetActionRegisterCb(
      [this](CtldClientStateMachine::RegisterArg const& arg) {
        CranedRegister_(arg.token, arg.lost_jobs, arg.lost_steps);
      });
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

void CtldClient::SetPingSuccessCb(std::function<void()>&& cb) {
  m_ping_success_cb_ = std::move(cb);
}
void CtldClient::SetPingFailedCb(std::function<void()>&& cb) {
  m_ping_failed_cb_ = std::move(cb);
}

void CtldClient::StepStatusChangeAsync(
    StepStatusChangeQueueElem&& task_status_change) {
  absl::MutexLock lock(&m_step_status_change_mtx_);

  CRANE_TRACE(
      "[Step #{}.{}] Step status change added to queue, status {},code {}.",
      task_status_change.job_id, task_status_change.step_id,
      task_status_change.new_status, task_status_change.exit_code);
  m_step_status_change_list_.emplace_back(std::move(task_status_change));
}

std::map<job_id_t, std::set<step_id_t>>
CtldClient::GetAllFinishStepStatusChangeId() {
  absl::MutexLock lock(&m_step_status_change_mtx_);
  std::map<job_id_t, std::set<step_id_t>> finished_steps;
  for (auto& elem : m_step_status_change_list_) {
    switch (elem.new_status) {
    case crane::grpc::TaskStatus::Cancelled:
    case crane::grpc::TaskStatus::Failed:
    case crane::grpc::TaskStatus::Completed:
    case crane::grpc::TaskStatus::ExceedTimeLimit:
      finished_steps[elem.job_id].emplace(elem.step_id);
      break;
    default:
      break;
    }
  }
  return finished_steps;
}

bool CtldClient::RequestConfigFromCtld_(RegToken const& token) {
  CRANE_LOGGER_DEBUG(g_runtime_status.conn_logger,
                     "Requesting config from CraneCtld...");

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
    CRANE_LOGGER_ERROR(
        g_runtime_status.conn_logger, "Notify CranedConnected failed: {}, {}",
        static_cast<int>(status.error_code()), status.error_message());
    return false;
  }
  return true;
}

bool CtldClient::CranedRegister_(
    RegToken const& token, std::set<job_id_t> const& lost_jobs,
    std::unordered_map<job_id_t, std::set<step_id_t>> const& lost_steps) {
  CRANE_LOGGER_DEBUG(g_runtime_status.conn_logger, "Sending CranedRegister.");

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
  auto& grpc_lost_steps = *grpc_meta->mutable_lost_steps();
  for (const auto& [job_id, step_ids] : lost_steps) {
    grpc_lost_steps[job_id].mutable_steps()->Assign(step_ids.begin(),
                                                    step_ids.end());
  }

  for (const auto& interface : g_config.CranedMeta.NetworkInterfaces) {
    *grpc_meta->add_network_interfaces() = interface;
  }

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(1));

  crane::grpc::CranedRegisterReply ready_reply;
  auto status = m_stub_->CranedRegister(&context, ready_request, &ready_reply);
  if (!status.ok()) {
    CRANE_LOGGER_DEBUG(g_runtime_status.conn_logger,
                       "CranedRegister failed: {}", status.error_message());
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
    {
      absl::MutexLock lock(&m_step_status_change_mtx_);
      if (m_step_status_change_list_.empty() && m_stopping_) break;
    }

    grpc_state = m_ctld_channel_->GetState(true);
    connected = prev_grpc_state == GRPC_CHANNEL_READY;

    if (!connected) {
      if (prev_connected) {  // Edge triggered: grpc connected -> disconnected.
        CRANE_LOGGER_INFO(g_runtime_status.conn_logger,
                          "Channel to CraneCtlD is disconnected.");
        g_ctld_client->StopPingCtld();
        g_ctld_client_sm->EvGrpcConnectionFailed();
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
      CRANE_LOGGER_INFO(g_runtime_status.conn_logger,
                        "Channel to CraneCtlD is connected.");
      g_ctld_client_sm->EvGrpcConnected();
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
  std::list<StepStatusChangeQueueElem> changes;
  changes.splice(changes.begin(), std::move(m_step_status_change_list_));
  m_step_status_change_mtx_.Unlock();

  while (!changes.empty()) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(kCranedRpcTimeoutSeconds));

    crane::grpc::StepStatusChangeRequest request;
    crane::grpc::StepStatusChangeReply reply;
    grpc::Status status;

    auto status_change = changes.front();

    CRANE_TRACE("[Step #{}.{}] Sending TaskStatusChange.", status_change.job_id,
                status_change.step_id);

    request.set_craned_id(m_craned_id_);
    request.set_job_id(status_change.job_id);
    request.set_step_id(status_change.step_id);
    request.set_new_status(status_change.new_status);
    request.set_exit_code(status_change.exit_code);
    if (status_change.reason.has_value())
      request.set_reason(status_change.reason.value());

    status = m_stub_->StepStatusChange(&context, request, &reply);
    if (!status.ok()) {
      CRANE_ERROR(
          "Failed to send TaskStatusChange: "
          "{{Step: #{}.{}, NewStatus: {}}}, reason: {} | {}, code: {}",
          status_change.job_id, status_change.step_id, status_change.new_status,
          status.error_message(), context.debug_error_string(),
          static_cast<int>(status.error_code()));

      if (m_stopping_) {
        CRANE_INFO(
            "Failed to send StepStatusChange but stopping, drop all status "
            "change.");
        return;
      }
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

    } else {
      CRANE_TRACE("[Step #{}.{}] StepStatusChange sent. reply.ok={}",
                  status_change.job_id, status_change.step_id, reply.ok());
      changes.pop_front();
    }
  }
}

bool CtldClient::Ping_() {
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(kCranedRpcTimeoutSeconds));

  crane::grpc::CranedPingRequest req;
  req.set_craned_id(m_craned_id_);
  crane::grpc::CranedPingReply reply;
  CRANE_LOGGER_DEBUG(g_runtime_status.conn_logger,
                     "Sending CranedPing request to CraneCtlD.");
  auto status = m_stub_->CranedPing(&context, req, &reply);
  if (!status.ok()) {
    CRANE_LOGGER_ERROR(g_runtime_status.conn_logger, "Craned Ping failed: {}",
                       status.error_message());
    g_ctld_client_sm->EvPingFailed();
    return false;
  }
  CRANE_LOGGER_TRACE(g_runtime_status.conn_logger, "Craned Ping {}.",
                     reply.ok());
  if (reply.ok()) {
    g_ctld_client_sm->EvPingSuccess();
  } else
    g_ctld_client_sm->EvPingFailed();
  return reply.ok();
}

}  // namespace Craned

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
using namespace std::chrono_literals;

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

  std::list<StepStatusChangeQueueElem> changes;
  changes.splice(changes.begin(), std::move(m_step_status_change_list_));
  m_step_status_change_mtx_.Unlock();
  SendStatusChanges_(std::move(changes));
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
  if (m_health_check_thread_.joinable()) m_health_check_thread_.join();
}

void CtldClient::Init() {
  g_ctld_client_sm->SetActionRequestConfigCb(
      [this](RegToken const& token) { RequestConfigFromCtld_(token); });

  g_ctld_client_sm->AddActionConfigureCb(
      [](CtldClientStateMachine::ConfigureArg const& arg) {
        // FIXME: step recovery
        auto token = arg.req.token();
        CRANE_LOGGER_DEBUG(g_runtime_status.conn_logger,
                           "Configuring action for token {}.",
                           ProtoTimestampToString(token));
        std::map<job_id_t, std::map<step_id_t, StepStatus>>
            ctld_job_steps_id_map;
        for (const auto& [job_id, job_steps] : arg.req.job_steps()) {
          for (const auto& [step_id, status] : job_steps.step_status()) {
            ctld_job_steps_id_map[job_id][step_id] = status;
            CRANE_LOGGER_TRACE(g_runtime_status.conn_logger,
                               "[Step #{}.{}] status from Ctld: {}", job_id,
                               step_id, status);
          }
        }
        auto ctld_job_ids = ctld_job_steps_id_map | std::views::keys;

        // std::map keys are ordered, so we can use set difference.
        std::map exact_job_steps = g_job_mgr->GetAllocatedJobSteps();

        for (auto status_change_steps = g_ctld_client->GetAllStepStatusChange();
             auto& [job_id, step_status_map] : status_change_steps) {
          for (auto& [step_id, status] : step_status_map) {
            exact_job_steps[job_id][step_id] = status;
          }
        }
        auto craned_job_ids = exact_job_steps | std::views::keys;
        std::set<job_id_t> lost_jobs{};
        std::set<job_id_t> invalid_jobs{};
        std::set<job_id_t> valid_jobs{};

        std::ranges::set_difference(ctld_job_ids, craned_job_ids,
                                    std::inserter(lost_jobs, lost_jobs.end()));
        std::ranges::set_difference(
            craned_job_ids, ctld_job_ids,
            std::inserter(invalid_jobs, invalid_jobs.end()));
        std::ranges::set_intersection(
            ctld_job_ids, craned_job_ids,
            std::inserter(valid_jobs, valid_jobs.end()));

        std::unordered_map<job_id_t, std::set<step_id_t>> lost_steps{};
        std::unordered_map<job_id_t, std::set<step_id_t>> invalid_steps{};
        std::unordered_map<job_id_t, std::set<step_id_t>> valid_job_steps{};

        // For lost jobs, all steps are lost
        for (auto job_id : lost_jobs) {
          for (auto step_id :
               ctld_job_steps_id_map.at(job_id) | std::views::keys)
            lost_steps[job_id].insert(step_id);
        }

        // For valid jobs, do step-level comparison
        for (auto job_id : valid_jobs) {
          const auto& ctld_steps =
              ctld_job_steps_id_map.at(job_id) | std::views::keys;
          const auto& craned_steps =
              exact_job_steps.at(job_id) | std::views::keys;
          std::ranges::set_difference(
              ctld_steps, craned_steps,
              std::inserter(lost_steps[job_id], lost_steps[job_id].end()));
          std::ranges::set_difference(
              craned_steps, ctld_steps,
              std::inserter(invalid_steps[job_id],
                            invalid_steps[job_id].end()));
          std::ranges::set_intersection(
              ctld_steps, craned_steps,
              std::inserter(valid_job_steps[job_id],
                            valid_job_steps[job_id].end()));
        }
        std::set<job_id_t> completing_jobs{};
        std::unordered_map<job_id_t, std::unordered_set<step_id_t>>
            completing_steps{};
        std::unordered_map<job_id_t, std::unordered_map<step_id_t, StepStatus>>
            steps_to_sync{};

        // Define status priority for intelligent merging
        // Ctld valid status:
        //   - Completing
        //   - Running
        //   - Configured
        //   - Configuring
        // Craned valid status:
        //   - Terminal states (Completed, Failed, ExceedTimeLimit, Cancelled,
        //     OutOfMemory)
        //   - Completing (All task finished on CommonStep / Asked to exit on
        //     DaemonStep, during Epilog)
        //   - Running
        //   - Configured (Only for CommonStep)
        //   - Configuring
        auto GetStatusPriority = [](StepStatus status) -> int {
          switch (status) {
          case StepStatus::Completed:
          case StepStatus::Failed:
          case StepStatus::ExceedTimeLimit:
          case StepStatus::Cancelled:
          case StepStatus::OutOfMemory:
            return 100;  // Terminal states - highest priority

          case StepStatus::Completing:
            return 90;  // Almost terminal

          case StepStatus::Running:
            return 50;  // Active execution

          case StepStatus::Configured:
            return 30;  // Configuration completed

          case StepStatus::Configuring:
            return 20;  // Being configured

          case StepStatus::Pending:
            return 10;  // Lowest priority

          default:
            return 0;
          }
        };

        // Intelligent status synchronization for valid steps
        for (const auto& [job_id, steps] : valid_job_steps) {
          for (const auto& step_id : steps) {
            auto ctld_status =
                arg.req.job_steps().at(job_id).step_status().at(step_id);
            auto craned_status = exact_job_steps.at(job_id).at(step_id);
            CRANE_TRACE("[Step #{}.{}] Ctld status: {}, Craned status: {}.",
                        job_id, step_id, ctld_status, craned_status);

            if (craned_status == ctld_status) {
              // Status match, no action needed
              continue;
            }

            // Special case: Handle timeout-during-offline scenario
            // This occurs when Craned goes offline and comes back online after
            // task timeout. Only DaemonStep can be Running here because:
            // - DaemonStep waits for Epilog to complete before terminating
            // - CommonSteps should already be killed by Timer and in terminal
            //   state
            // Ctld detected timeout and marked as Completing. Let Ctld handle
            // the termination.
            if (craned_status == StepStatus::Running &&
                ctld_status == StepStatus::Completing) {
              CRANE_INFO(
                  "[Step #{}.{}] Ctld has Completing status (likely due to "
                  "timeout during offline), Craned reports Running (should be "
                  "DaemonStep). Keeping Craned's state, Ctld will handle "
                  "termination.",
                  job_id, step_id);
              continue;
            }

            // Handle Completing status from Ctld
            if (ctld_status == StepStatus::Completing) {
              CRANE_TRACE("[Step #{}.{}] is completing", job_id, step_id);
              completing_steps[job_id].insert(step_id);
              continue;
            }

            int ctld_priority = GetStatusPriority(ctld_status);
            int craned_priority = GetStatusPriority(craned_status);

            if (craned_priority > ctld_priority) {
              // Craned has more advanced state
              CRANE_INFO(
                  "[Step #{}.{}] Craned has more advanced status {} (Ctld has "
                  "{}), sent a statuschange to kick ctld step status machine",
                  job_id, step_id, craned_status, ctld_status);
              steps_to_sync[job_id][step_id] = craned_status;
            } else if (craned_status == StepStatus::Configured) {
              // For configured but not running step, terminate it
              CRANE_TRACE(
                  "[Step #{}.{}] is configured but not running, mark as "
                  "invalid",
                  job_id, step_id);
              invalid_steps[job_id].insert(step_id);
            } else {
              // Ctld has higher/equal priority - terminate to maintain
              // consistency
              // This branch handles cases where craned_priority <=
              // ctld_priority and not covered by special cases above,
              // including:
              // - Craned: Running, Ctld: Completing (non-DaemonStep case,
              //   though this should rarely happen as CommonSteps should be
              //   killed by timer)
              // - Other cases where Ctld has equal or higher priority
              // Craned: Configuring with Ctld: Running/Configured should
              // not occur in the normal state machine flow, as Configuring
              // means Ctld hasn't received the Configured status yet. The only
              // valid case for Ctld to be ahead is when Ctld marks it as
              // Completing (handled above).
              CRANE_WARN(
                  "[Step #{}.{}] Status mismatch: Ctld has {}, Craned has {}. "
                  "Terminating step to maintain consistency.",
                  job_id, step_id, ctld_status, craned_status);
              invalid_steps[job_id].insert(step_id);
              lost_steps[job_id].insert(step_id);
            }
          }
        }

        g_ctld_client_sm->EvConfigurationDone(lost_jobs, lost_steps);

        // Report status changes to Ctld for steps that need synchronization
        for (const auto& [job_id, step_status_map] : steps_to_sync) {
          for (const auto& [step_id, status] : step_status_map) {
            uint32_t exit_code = g_job_mgr->GetStepExitCode(job_id, step_id);
            google::protobuf::Timestamp end_time =
                g_job_mgr->GetStepEndTime(job_id, step_id);
            CRANE_INFO(
                "[Step #{}.{}] Reporting status {} to Ctld during recovery "
                "sync",
                job_id, step_id, status);
            StepStatusChangeQueueElem elem{
                .job_id = job_id,
                .step_id = step_id,
                .new_status = status,
                .exit_code = exit_code,
                .reason = "Status synced from Craned during recovery",
                .timestamp = end_time};
            g_ctld_client->StepStatusChangeAsync(std::move(elem));
          }
        }

        // Only terminate truly invalid steps (those not in Ctld's view at all)
        // Don't kill steps that exist in both but may have status differences
        // - Ctld will handle status synchronization via intelligent merging
        if (!invalid_steps.empty()) {
          CRANE_INFO("Terminating invalid steps (not tracked by Ctld): [{}].",
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

  if (g_config.HealthCheck.Interval > 0L) {
    HealthCheck_();
    m_health_check_thread_ = std::thread([this] {
      std::mt19937 rng{std::random_device{}()};
      do {
        uint64_t interval = g_config.HealthCheck.Interval;
        int delay = interval;
        if (g_config.HealthCheck.Cycle) {
          std::uniform_int_distribution<int> dist(1, interval);
          delay = dist(rng);
        }
        std::this_thread::sleep_for(std::chrono::seconds(delay));
        if (m_stopping_ || !m_stub_) return;
        if (CheckNodeState_()) HealthCheck_();
      } while (true);
    });
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

void CtldClient::StepStatusChangeAsync(job_id_t job_id, step_id_t step_id,
                                       crane::grpc::TaskStatus new_status,
                                       uint32_t exit_code,
                                       std::optional<std::string> reason) {
  StepStatusChangeQueueElem elem{
      .job_id = job_id,
      .step_id = step_id,
      .new_status = new_status,
      .exit_code = exit_code,
      .reason = std::move(reason),
      .timestamp = google::protobuf::util::TimeUtil::GetCurrentTime()};
  StepStatusChangeAsync(std::move(elem));
}

void CtldClient::UpdateNodeDrainState(bool is_drain,
                                      const std::string& reason) {
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(kCranedRpcTimeoutSeconds));
  crane::grpc::UpdateNodeDrainStateRequest request;
  crane::grpc::UpdateNodeDrainStateReply reply;
  request.set_craned_id(g_config.CranedIdOfThisNode);
  request.set_drain(is_drain);
  request.set_reason(reason);

  auto status = m_stub_->UpdateNodeDrainState(&context, request, &reply);
  if (!status.ok() || !reply.ok()) CRANE_DEBUG("UpdateNodeDrainState failed");
}

std::map<job_id_t, std::map<step_id_t, StepStatus>>
CtldClient::GetAllStepStatusChange() {
  absl::MutexLock lock(&m_step_status_change_mtx_);
  std::map<job_id_t, std::map<step_id_t, StepStatus>> step_status_map;
  for (auto& elem : m_step_status_change_list_) {
    step_status_map[elem.job_id][elem.step_id] = elem.new_status;
  }
  return step_status_map;
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
  util::SetCurrentThreadName("SendCtldThr");
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
    if (m_stopping_) {
      absl::MutexLock lock(&m_step_status_change_mtx_);
      if (m_step_status_change_list_.empty()) break;
    }

    // Try to connect when not connected.
    grpc_state = m_ctld_channel_->GetState(!prev_connected);
    connected = prev_grpc_state == GRPC_CHANNEL_READY;

    if (!connected) {
      if (prev_connected) {  // Edge triggered: grpc connected -> disconnected.
        CRANE_LOGGER_INFO(g_runtime_status.conn_logger,
                          "Channel to CraneCtlD is disconnected.");
        // Update prev disconnected grpc state
        prev_grpc_state = grpc_state;
        prev_connected = false;
        g_ctld_client->StopPingCtld();
        g_ctld_client_sm->EvGrpcConnectionFailed();
        for (const auto& cb : m_on_ctld_disconnected_cb_chain_) cb();
      }
      // Disconnected state: waiting for conntected state change
      std::chrono::time_point ddl = std::chrono::system_clock::now() + 1s;
      bool status_changed =
          m_ctld_channel_->WaitForStateChange(prev_grpc_state, ddl);
      if (!status_changed)
        continue;  // No state change. No need to update prev state.

      prev_grpc_state = grpc_state;
      continue;
    }

    // Connected case:
    if (!prev_connected) {  // Edge triggered: grpc disconnected -> connected.
      CRANE_LOGGER_INFO(g_runtime_status.conn_logger,
                        "Channel to CraneCtlD is connected.");
      prev_connected = true;
      g_ctld_client_sm->EvGrpcConnected();
      for (const auto& cb : m_on_ctld_connected_cb_chain_) cb();
    }

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
      // No msg, sleep for a while to avoid busy loop.
      std::this_thread::sleep_for(50ms);
    } else {
      std::list<StepStatusChangeQueueElem> changes;
      changes.splice(changes.begin(), std::move(m_step_status_change_list_));
      m_step_status_change_mtx_.Unlock();
      bool success = SendStatusChanges_(std::move(changes));
      if (!success) std::this_thread::sleep_for(100ms);
    }
  }
}

bool CtldClient::SendStatusChanges_(
    std::list<StepStatusChangeQueueElem>&& changes) {
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
    *request.mutable_timestamp() = status_change.timestamp;
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
            "change to send.");
        return false;
      }
      // If some messages are not sent due to channel failure,
      // put them back into m_task_status_change_list_
      if (!changes.empty()) {
        m_step_status_change_mtx_.Lock();
        m_step_status_change_list_.splice(m_step_status_change_list_.begin(),
                                          std::move(changes));
        m_step_status_change_mtx_.Unlock();
      }
      return false;
    } else {
      CRANE_TRACE("[Step #{}.{}] StepStatusChange sent. reply.ok={}",
                  status_change.job_id, status_change.step_id, reply.ok());
      changes.pop_front();
    }
  }
  return true;
}

void CtldClient::SendHealthCheckResult_(bool is_health) const {
  if (m_stopping_ || !m_stub_) return;

  grpc::ClientContext context;
  crane::grpc::SendHealthCheckResultRequest request;
  google::protobuf::Empty reply;

  request.set_craned_id(g_config.CranedIdOfThisNode);
  request.set_healthy(is_health);

  auto result = m_stub_->SendHealthCheckResult(&context, request, &reply);
  if (!result.ok()) {
    CRANE_ERROR("SendHealthCheckResult failed: is_health={}", is_health);
  }
}

void CtldClient::HealthCheck_() {
  if (!g_server->ReadyFor(RequestSource::CTLD)) return;

  CRANE_DEBUG("Health checking.....");

  subprocess_s subprocess{};
  std::vector<const char*> argv = {g_config.HealthCheck.Program.c_str(),
                                   nullptr};

  if (subprocess_create(argv.data(), 0, &subprocess) != 0) {
    CRANE_ERROR(
        "[Craned Subprocess] HealthCheck subprocess creation failed: {}.",
        strerror(errno));
    SendHealthCheckResult_(false);
    return;
  }

  pid_t pid = subprocess.child;
  int result = 0;

  auto fut = std::async(std::launch::async,
                        [pid, &result]() { return waitpid(pid, &result, 0); });

  bool child_exited = false;
  if (fut.wait_for(std::chrono::milliseconds(MaxHealthCheckWaitTime)) ==
      std::future_status::ready) {
    if (fut.get() == pid) {
      child_exited = true;
    }
  }

  auto read_stream = [](std::FILE* f) {
    std::string out;
    char buf[4096];
    while (std::fgets(buf, sizeof(buf), f)) out.append(buf);
    return out;
  };

  if (!child_exited) {
    kill(pid, SIGKILL);
    waitpid(pid, &result, 0);
    std::string stdout_str = read_stream(subprocess_stdout(&subprocess));
    std::string stderr_str = read_stream(subprocess_stderr(&subprocess));
    CRANE_WARN("HealthCheck: Timeout. stdout: {}, stderr: {}", stdout_str,
               stderr_str);
    SendHealthCheckResult_(false);
    subprocess_destroy(&subprocess);
    return;
  }

  if (subprocess_destroy(&subprocess) != 0)
    CRANE_ERROR("[Craned Subprocess] HealthCheck destroy failed.");

  if (result != 0) {
    std::string stdout_str = read_stream(subprocess_stdout(&subprocess));
    std::string stderr_str = read_stream(subprocess_stderr(&subprocess));
    CRANE_WARN("HealthCheck: Failed (exit code:{}). stdout: {}, stderr: {}",
               result, stdout_str, stderr_str);
    SendHealthCheckResult_(false);
    return;
  }

  CRANE_DEBUG("Health check success.");
  SendHealthCheckResult_(true);
}

bool CtldClient::CheckNodeState_() {
  if (g_config.HealthCheck.NodeState == Config::HealthCheckConfig::ANY)
    return true;

  grpc::ClientContext context;
  crane::grpc::QueryNodeStateRequest req;
  crane::grpc::QueryNodeStateReply reply;
  req.set_craned_id(g_config.CranedIdOfThisNode);
  auto result = m_stub_->QueryNodeState(&context, req, &reply);
  if (!result.ok() || !reply.ok()) {
    CRANE_ERROR("QueryNodeState failed");
    return false;
  }

  switch (g_config.HealthCheck.NodeState) {
  case Config::HealthCheckConfig::NONDRAINED_IDLE:
    return !reply.drain() &&
           reply.state() == crane::grpc::CranedResourceState::CRANE_IDLE;
  case Config::HealthCheckConfig::IDLE:
    return reply.state() == crane::grpc::CranedResourceState::CRANE_IDLE;
  case Config::HealthCheckConfig::MIXED:
    return reply.state() == crane::grpc::CranedResourceState::CRANE_MIX;
  case Config::HealthCheckConfig::ALLOC:
    return reply.state() == crane::grpc::CranedResourceState::CRANE_ALLOC;
  case Config::HealthCheckConfig::ANY:
    break;
  }

  return false;
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

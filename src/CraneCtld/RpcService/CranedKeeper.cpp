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

#include "CranedKeeper.h"

#include "CranedMetaContainer.h"
#include "TaskScheduler.h"

namespace Ctld {

using grpc::ClientContext;
using grpc::Status;
CranedStub::CranedStub(CranedKeeper* craned_keeper)
    : m_craned_keeper_(craned_keeper),
      m_failure_retry_times_(0),
      m_disconnected_(true),
      m_registered_(false) {
  // The most part of jobs are done in CranedKeeper::RegisterCraneds().
}

void CranedStub::Fini() {
  if (m_clean_up_cb_) m_clean_up_cb_(this);
}

void CranedStub::ConfigureCraned(const CranedId& craned_id,
                                 const RegToken& token) {
  CRANE_LOGGER_TRACE(g_runtime_status.conn_logger,
                     "Configuring craned {} with token {}", craned_id,
                     ProtoTimestampToString(token));

  crane::grpc::ConfigureCranedRequest request;
  request.set_ok(true);
  *request.mutable_token() = token;

  g_task_scheduler->QueryRnJobOnCtldForNodeConfig(craned_id, &request);

  ClientContext context;
  google::protobuf::Empty reply;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(kCtldRpcTimeoutSeconds));

  auto status = m_stub_->Configure(&context, request, &reply);
  if (!status.ok()) {
    CRANE_LOGGER_ERROR(
        g_runtime_status.conn_logger,
        "ConfigureCraned RPC for Node {} returned with status not ok: {}. "
        "Resetting token.",
        craned_id, status.error_message());
    absl::MutexLock lock(&m_lock_);
    m_token_.reset();
  }
}

CraneErrCode CranedStub::TerminateSteps(
    const std::unordered_map<job_id_t, std::set<step_id_t>>& steps) {
  using crane::grpc::TerminateStepsReply;
  using crane::grpc::TerminateStepsRequest;

  ClientContext context;
  Status status;
  TerminateStepsRequest request;
  TerminateStepsReply reply;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(kCtldRpcTimeoutSeconds));

  auto& job_step_map = *request.mutable_job_step_ids_map();

  for (const auto& [job_id, step_ids] : steps) {
    job_step_map[job_id].mutable_steps()->Assign(step_ids.begin(),
                                                 step_ids.end());
  }
  status = m_stub_->TerminateSteps(&context, request, &reply);
  if (!status.ok()) {
    CRANE_DEBUG(
        "TerminateRunningTask RPC for Node {} returned with status not ok: {}",
        m_craned_id_, status.error_message());
    HandleGrpcErrorCode_(status.error_code());
    return CraneErrCode::ERR_RPC_FAILURE;
  }
  UpdateLastActiveTime();

  return CraneErrCode::SUCCESS;
}

CraneErrCode CranedStub::TerminateOrphanedSteps(
    const std::unordered_map<job_id_t, std::set<step_id_t>>& steps) {
  using crane::grpc::TerminateOrphanedStepReply;
  using crane::grpc::TerminateOrphanedStepRequest;

  ClientContext context;
  Status status;
  TerminateOrphanedStepRequest request;
  TerminateOrphanedStepReply reply;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(kCtldRpcTimeoutSeconds));

  auto& job_step_map = *request.mutable_job_step_ids_map();

  for (const auto& [job_id, step_ids] : steps) {
    job_step_map[job_id].mutable_steps()->Assign(step_ids.begin(),
                                                 step_ids.end());
  }
  status = m_stub_->TerminateOrphanedStep(&context, request, &reply);
  if (!status.ok()) {
    CRANE_DEBUG(
        "TerminateOrphanedTasks RPC for Node {} returned status not ok: {}",
        m_craned_id_, status.error_message());
    HandleGrpcErrorCode_(status.error_code());
    return CraneErrCode::ERR_RPC_FAILURE;
  }
  UpdateLastActiveTime();

  if (reply.ok())
    return CraneErrCode::SUCCESS;
  else
    return CraneErrCode::ERR_GENERIC_FAILURE;
}

CraneErrCode CranedStub::AllocJobs(
    const std::vector<crane::grpc::JobToD>& jobs) {
  using crane::grpc::AllocJobsReply;
  using crane::grpc::AllocJobsRequest;

  Status status;
  AllocJobsRequest request;
  AllocJobsReply reply;

  ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(kCtldRpcTimeoutSeconds));

  for (auto&& job : jobs) {
    *request.add_jobs() = job;
  }

  status = m_stub_->AllocJobs(&context, request, &reply);
  if (!status.ok()) {
    CRANE_ERROR("AllocJobs RPC for Node {} returned with status not ok: {}",
                m_craned_id_, status.error_message());
    HandleGrpcErrorCode_(status.error_code());
    return CraneErrCode::ERR_RPC_FAILURE;
  }
  UpdateLastActiveTime();

  return CraneErrCode::SUCCESS;
}

CraneErrCode CranedStub::FreeJobs(const std::vector<task_id_t>& task) {
  using crane::grpc::FreeJobsReply;
  using crane::grpc::FreeJobsRequest;

  Status status;
  FreeJobsRequest request;
  FreeJobsReply reply;

  ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(kCtldRpcTimeoutSeconds));

  request.mutable_job_id_list()->Assign(task.begin(), task.end());

  status = m_stub_->FreeJobs(&context, request, &reply);
  if (!status.ok()) {
    CRANE_DEBUG("FreeJobs gRPC for Node {} returned with status not ok: {}",
                m_craned_id_, status.error_message());
    HandleGrpcErrorCode_(status.error_code());
    return CraneErrCode::ERR_RPC_FAILURE;
  }
  UpdateLastActiveTime();

  return CraneErrCode::SUCCESS;
}

CraneErrCode CranedStub::AllocSteps(
    const std::vector<crane::grpc::StepToD>& steps) {
  using crane::grpc::AllocStepsReply;
  using crane::grpc::AllocStepsRequest;
  AllocStepsRequest request;
  AllocStepsReply reply;
  ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(kCtldRpcTimeoutSeconds));
  for (auto& step : steps) *request.add_steps() = step;
  Status status = m_stub_->AllocSteps(&context, request, &reply);
  if (!status.ok()) {
    CRANE_DEBUG("AllocSteps RPC for Node {} returned with status not ok: {}",
                m_craned_id_, status.error_message());
    HandleGrpcErrorCode_(status.error_code());
    return CraneErrCode::ERR_RPC_FAILURE;
  }
  return CraneErrCode::SUCCESS;
}
CraneExpected<std::unordered_map<job_id_t, std::set<step_id_t>>>
CranedStub::ExecuteSteps(
    const std::unordered_map<job_id_t, std::set<step_id_t>>& steps) {
  using crane::grpc::ExecuteStepsReply;
  using crane::grpc::ExecuteStepsRequest;
  ExecuteStepsRequest request;
  ExecuteStepsReply reply;
  ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(kCtldRpcTimeoutSeconds));

  auto& job_step_map = *request.mutable_job_step_ids_map();

  for (const auto& [job_id, step_ids] : steps) {
    job_step_map[job_id].mutable_steps()->Assign(step_ids.begin(),
                                                 step_ids.end());
  }

  auto status = m_stub_->ExecuteSteps(&context, request, &reply);
  if (!status.ok()) {
    CRANE_DEBUG("ExecuteSteps RPC for Node {} returned with status not ok: {}",
                m_craned_id_, status.error_message());
    HandleGrpcErrorCode_(status.error_code());
    return std::unexpected{CraneErrCode::ERR_RPC_FAILURE};
  }
  if (reply.failed_job_step_ids_map().empty()) {
    return {};
  }
  std::unordered_map<job_id_t, std::set<step_id_t>> failed_job_step_ids_map;
  for (const auto& [job_id, step_ids] : reply.failed_job_step_ids_map()) {
    failed_job_step_ids_map[job_id].insert(step_ids.steps().begin(),
                                           step_ids.steps().end());
  }
  return failed_job_step_ids_map;
}

CraneErrCode CranedStub::FreeSteps(
    const std::unordered_map<job_id_t, std::set<step_id_t>>& steps) {
  using crane::grpc::FreeStepsReply;
  using crane::grpc::FreeStepsRequest;
  FreeStepsRequest request;
  FreeStepsReply reply;
  ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(kCtldRpcTimeoutSeconds));
  auto& job_step_map = *request.mutable_job_step_ids_map();

  for (const auto& [job_id, step_ids] : steps) {
    job_step_map[job_id].mutable_steps()->Assign(step_ids.begin(),
                                                 step_ids.end());
  }

  Status status = m_stub_->FreeSteps(&context, request, &reply);
  if (!status.ok()) {
    CRANE_DEBUG("FreeSteps RPC for Node {} returned with status not ok: {}",
                m_craned_id_, status.error_message());
    HandleGrpcErrorCode_(status.error_code());
    return CraneErrCode::ERR_RPC_FAILURE;
  }
  return CraneErrCode::SUCCESS;
}

CraneErrCode CranedStub::SuspendJobs(const std::vector<job_id_t>& job_ids) {
  using crane::grpc::SuspendJobsReply;
  using crane::grpc::SuspendJobsRequest;

  ClientContext context;
  Status status;
  SuspendJobsRequest request;
  SuspendJobsReply reply;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(kCtldRpcTimeoutSeconds));

  request.mutable_job_id_list()->Assign(job_ids.begin(), job_ids.end());

  status = m_stub_->SuspendJobs(&context, request, &reply);
  if (!status.ok()) {
    CRANE_DEBUG("SuspendJobs RPC for Node {} failed: {}", m_craned_id_,
                status.error_message());
    HandleGrpcErrorCode_(status.error_code());
    return CraneErrCode::ERR_RPC_FAILURE;
  }
  UpdateLastActiveTime();

  if (reply.ok()) return CraneErrCode::SUCCESS;

  CRANE_WARN("SuspendJobs RPC for Node {} declined: {}", m_craned_id_,
             reply.reason());
  return CraneErrCode::ERR_GENERIC_FAILURE;
}

CraneErrCode CranedStub::ResumeJobs(const std::vector<job_id_t>& job_ids) {
  using crane::grpc::ResumeJobsReply;
  using crane::grpc::ResumeJobsRequest;

  ClientContext context;
  Status status;
  ResumeJobsRequest request;
  ResumeJobsReply reply;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(kCtldRpcTimeoutSeconds));

  request.mutable_job_id_list()->Assign(job_ids.begin(), job_ids.end());

  status = m_stub_->ResumeJobs(&context, request, &reply);
  if (!status.ok()) {
    CRANE_DEBUG("ResumeJobs RPC for Node {} failed: {}", m_craned_id_,
                status.error_message());
    HandleGrpcErrorCode_(status.error_code());
    return CraneErrCode::ERR_RPC_FAILURE;
  }
  UpdateLastActiveTime();

  if (reply.ok()) return CraneErrCode::SUCCESS;

  CRANE_WARN("ResumeJobs RPC for Node {} declined: {}", m_craned_id_,
             reply.reason());
  return CraneErrCode::ERR_GENERIC_FAILURE;
}

CraneErrCode CranedStub::ChangeJobTimeLimit(uint32_t task_id,
                                            uint64_t seconds) {
  using crane::grpc::ChangeJobTimeLimitReply;
  using crane::grpc::ChangeJobTimeLimitRequest;

  ClientContext context;
  Status status;
  ChangeJobTimeLimitRequest request;
  ChangeJobTimeLimitReply reply;

  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(kCtldRpcTimeoutSeconds));
  request.set_task_id(task_id);
  request.set_time_limit_seconds(seconds);
  status = m_stub_->ChangeJobTimeLimit(&context, request, &reply);

  if (!status.ok()) {
    CRANE_ERROR("ChangeTaskTimeLimitAsync to Craned {} failed: {} ",
                m_craned_id_, status.error_message());
    HandleGrpcErrorCode_(status.error_code());
    return CraneErrCode::ERR_RPC_FAILURE;
  }
  UpdateLastActiveTime();
  if (reply.ok())
    return CraneErrCode::SUCCESS;
  else
    return CraneErrCode::ERR_GENERIC_FAILURE;
}

void CranedStub::HandleGrpcErrorCode_(grpc::StatusCode code) {
  if (code == grpc::UNAVAILABLE) {
    CRANE_INFO("Craned {} reports service unavailable. Considering it down.",
               m_craned_id_);
    g_meta_container->CranedDown(m_craned_id_);
  }
}

CranedKeeper::CranedKeeper(uint32_t node_num) : m_cq_closed_(false) {
  m_pmr_pool_res_ = std::make_unique<std::pmr::synchronized_pool_resource>();
  m_tag_sync_allocator_ =
      std::make_unique<std::pmr::polymorphic_allocator<CqTag>>(
          m_pmr_pool_res_.get());

  uint32_t thread_num = std::bit_ceil(node_num / kCompletionQueueCapacity);

  m_cq_mtx_vec_ = std::vector<Mutex>(thread_num);
  m_cq_vec_ = std::vector<grpc::CompletionQueue>(thread_num);

  for (int i = 0; i < thread_num; i++) {
    m_cq_thread_vec_.emplace_back(&CranedKeeper::StateMonitorThreadFunc_, this,
                                  i);
  }

  m_period_connect_thread_ =
      std::thread(&CranedKeeper::PeriodConnectCranedThreadFunc_, this);

  g_runtime_status.conn_logger = AddLogger(
      "conn", StrToLogLevel(g_config.CraneCtldDebugLevel).value(), true);
}

CranedKeeper::~CranedKeeper() {
  Shutdown();

  for (auto& cq_thread : m_cq_thread_vec_) cq_thread.join();
  if (m_period_connect_thread_.joinable()) m_period_connect_thread_.join();

  CRANE_TRACE("CranedKeeper has been closed.");
}

void CranedKeeper::Shutdown() {
  if (m_cq_closed_) return;

  m_cq_closed_ = true;

  {
    util::lock_guard l(m_connect_craned_mtx_);
    for (auto&& [craned_id, stub] : m_connected_craned_id_stub_map_) {
      stub->m_channel_.reset();
    }
    m_connected_craned_id_stub_map_.clear();
  }

  for (int i = 0; i < m_cq_vec_.size(); i++) {
    util::lock_guard lock(m_cq_mtx_vec_[i]);
    m_cq_vec_[i].Shutdown();
  }

  // Dependency order: rpc_cq -> channel_state_cq -> tag pool.
  // Tag pool's destructor will free all trailing tags in cq.
}

void CranedKeeper::StateMonitorThreadFunc_(int thread_id) {
  util::SetCurrentThreadName(fmt::format("KeeStatMon{:0>3}", thread_id));

  bool ok;
  CqTag* tag;
  grpc::CompletionQueue::NextStatus next_status;

  while (true) {
    auto ddl = std::chrono::system_clock::now() + std::chrono::seconds(10);
    next_status = m_cq_vec_[thread_id].AsyncNext((void**)&tag, &ok, ddl);

    // If Shutdown() is called, return immediately.
    if (next_status == grpc::CompletionQueue::SHUTDOWN) break;

    if (m_cq_closed_) {
      if (tag->type == CqTag::kInitializingCraned) delete tag->craned;
      continue;
    }

    if (next_status == grpc::CompletionQueue::TIMEOUT) continue;
    if (next_status == grpc::CompletionQueue::GOT_EVENT) {
      // If ok is false, the tag timed out.
      // However, we can also check timeout
      // by comparing prev_state and current state,
      // and it is handled in state machines.
      // It's fine to ignore the value of ok.

      auto* craned = tag->craned;

      CqTag* next_tag = nullptr;
      grpc_connectivity_state new_state = craned->m_channel_->GetState(true);

      switch (tag->type) {
      case CqTag::kInitializingCraned:
        next_tag = InitCranedStateMachine_(craned, new_state);
        break;
      case CqTag::kEstablishedCraned:
        next_tag = EstablishedCranedStateMachine_(craned, new_state);
        break;
      }

      if (next_tag) {
        // When cq is closed, do not register any more callbacks on it.
        if (!m_cq_closed_) {
          // CRANE_TRACE("Registering next tag {} for {}", tag->type,
          //            craned->m_craned_id_);
          if (CheckNodeTimeoutAndClean(tag)) continue;

          auto deadline = std::chrono::system_clock::now();
          if (tag->type == CqTag::kInitializingCraned)
            deadline +=
                std::chrono::seconds(kCompletionQueueConnectingTimeoutSeconds);
          else {
            deadline +=
                std::chrono::seconds(kCompletionQueueEstablishedTimeoutSeconds);
          }

          craned->m_prev_channel_state_ = new_state;

          // CRANE_TRACE("Registering next tag {} for {}", next_tag->type,
          //            craned->m_craned_id_);

          util::lock_guard lock(m_cq_mtx_vec_[thread_id]);
          craned->m_channel_->NotifyOnStateChange(
              craned->m_prev_channel_state_, deadline, &m_cq_vec_[thread_id],
              next_tag);
        }
      } else {
        // END state of both state machine. Free the Craned client.
        if (tag->type == CqTag::kInitializingCraned) {
          CRANE_LOGGER_TRACE(g_runtime_status.conn_logger,
                             "Failed connect to {}, stub destroyed.",
                             craned->m_craned_id_);

          // Callback in destructor will erase from connecting queue.

          util::lock_guard guard(m_connect_craned_mtx_);
          craned->Fini();
          delete craned;
        } else if (tag->type == CqTag::kEstablishedCraned) {
          CRANE_LOGGER_TRACE(g_runtime_status.conn_logger,
                             "Craned {} disconnected.", craned->m_craned_id_);
          if (m_craned_disconnected_cb_) {
            g_thread_pool->detach_task(
                [this, craned_id = craned->m_craned_id_] {
                  m_craned_disconnected_cb_(craned_id);
                });
          }

          WriterLock lock(&m_connect_craned_mtx_);
          craned->Fini();
          m_connected_craned_id_stub_map_.erase(craned->m_craned_id_);
        } else {
          CRANE_LOGGER_TRACE(g_runtime_status.conn_logger,
                             "Unknown tag type: {}", (int)tag->type);
        }
      }

      m_tag_sync_allocator_->delete_object(tag);
    } else {
      // m_cq_.Shutdown() has been called. Exit the thread.
      break;
    }
  }
}

CranedKeeper::CqTag* CranedKeeper::InitCranedStateMachine_(
    CranedStub* craned, grpc_connectivity_state new_state) {
  // CRANE_TRACE("Enter InitCranedStateMachine_");

  std::optional<CqTag::Type> next_tag_type;

  switch (new_state) {
  case GRPC_CHANNEL_READY: {
    RegToken token;
    {
      CRANE_LOGGER_TRACE(g_runtime_status.conn_logger,
                         "{} -> READY. New craned {} with token {} connected.",
                         static_cast<int>(craned->m_prev_channel_state_),
                         craned->m_craned_id_,
                         ProtoTimestampToString(craned->m_token_.value()));

      WriterLock lock(&m_connect_craned_mtx_);
      util::lock_guard guard(m_unavail_craned_set_mtx_);
      CRANE_ASSERT(
          !m_connected_craned_id_stub_map_.contains(craned->m_craned_id_));
      m_connected_craned_id_stub_map_.emplace(craned->m_craned_id_, craned);
      craned->m_disconnected_ = false;
      CRANE_ASSERT(m_unavail_craned_set_.contains(craned->m_craned_id_));
      CRANE_ASSERT(m_connecting_craned_set_.contains(craned->m_craned_id_));
      token = m_unavail_craned_set_.at(craned->m_craned_id_);
      m_unavail_craned_set_.erase(craned->m_craned_id_);
      m_connecting_craned_set_.erase(craned->m_craned_id_);
    }

    if (m_craned_connected_cb_)
      g_thread_pool->detach_task(
          [this, craned_id = craned->m_craned_id_, token]() {
            m_craned_connected_cb_(craned_id, token);
          });

    // Switch to EstablishedCraned state machine
    next_tag_type = CqTag::kEstablishedCraned;
    break;
  }

  case GRPC_CHANNEL_TRANSIENT_FAILURE: {
    // current              next
    // TRANSIENT_FAILURE -> CONNECTING/END

    if (++craned->m_failure_retry_times_ <= craned->s_maximum_retry_times_)
      next_tag_type = CqTag::kInitializingCraned;
    else
      next_tag_type = std::nullopt;

    CRANE_LOGGER_TRACE(
        g_runtime_status.conn_logger,
        "[Node #{}] {} -> TRANSIENT_FAILURE({}/{}) -> CONNECTING/END",
        craned->m_craned_id_, (int)craned->m_prev_channel_state_,
        craned->m_failure_retry_times_, craned->s_maximum_retry_times_);
    break;
  }

  case GRPC_CHANNEL_SHUTDOWN: {
    CRANE_LOGGER_WARN(g_runtime_status.conn_logger,
                      "Unexpected InitializingCraned SHUTDOWN state!");
    next_tag_type = std::nullopt;
    break;
  }

  case GRPC_CHANNEL_CONNECTING: {
    if (craned->m_prev_channel_state_ == GRPC_CHANNEL_IDLE) {
      // prev    now
      // IDLE -> CONNECTING
      // CRANE_TRACE("IDLE -> CONNECTING");
      next_tag_type = CqTag::kInitializingCraned;
    } else {
      // prev    current       next
      // Any  -> CONNECTING -> CONNECTING
      CRANE_LOGGER_TRACE(g_runtime_status.conn_logger,
                         "[Node #{}] {} -> CONNECTING -> CONNECTING",
                         craned->m_craned_id_,
                         (int)craned->m_prev_channel_state_);
      next_tag_type = CqTag::kInitializingCraned;
    }
    break;
  }

  case GRPC_CHANNEL_IDLE:
    // InitializingCraned: BEGIN -> IDLE state switching is handled in
    // CranedKeeper::RegisterNewCraneds. Execution should never reach here.
    CRANE_LOGGER_ERROR(g_runtime_status.conn_logger,
                       "Unexpected InitializingCraned IDLE state!");
    break;
  }

  // CRANE_TRACE("Exit InitCranedStateMachine_");
  if (next_tag_type.has_value()) {
    return m_tag_sync_allocator_->new_object<CqTag>(
        CqTag{next_tag_type.value(), craned});
  }

  return nullptr;
}

CranedKeeper::CqTag* CranedKeeper::EstablishedCranedStateMachine_(
    CranedStub* craned, grpc_connectivity_state new_state) {
  // CRANE_TRACE("Enter EstablishedCranedStateMachine_");

  std::optional<CqTag::Type> next_tag_type;

  switch (new_state) {
  case GRPC_CHANNEL_CONNECTING: {
    if (craned->m_prev_channel_state_ == GRPC_CHANNEL_CONNECTING) {
      // prev          current       next
      // CONNECTING -> CONNECTING -> END
      CRANE_LOGGER_TRACE(g_runtime_status.conn_logger,
                         "[Node #{}] CONNECTING -> CONNECTING -> END",
                         craned->m_craned_id_);
      next_tag_type = std::nullopt;
    } else {
      // prev    now
      // IDLE -> CONNECTING
      CRANE_LOGGER_TRACE(g_runtime_status.conn_logger,
                         "[Node #{}] IDLE -> CONNECTING", craned->m_craned_id_);
      next_tag_type = CqTag::kEstablishedCraned;
    }
    break;
  }

  case GRPC_CHANNEL_IDLE: {
    // prev     current
    // READY -> IDLE (the only edge)

    CRANE_TRACE("[Node #{}] READY -> IDLE", craned->m_craned_id_);
    craned->m_disconnected_ = true;
    craned->m_registered_ = false;

    next_tag_type = std::nullopt;
    break;
  }

  case GRPC_CHANNEL_READY: {
    // Any -> READY
    next_tag_type = CqTag::kEstablishedCraned;
    break;
  }

  case GRPC_CHANNEL_TRANSIENT_FAILURE: {
    // current              next
    // TRANSIENT_FAILURE -> END
    next_tag_type = std::nullopt;
    break;
  }

  case GRPC_CHANNEL_SHUTDOWN: {
    craned->m_disconnected_ = true;
    craned->m_registered_ = false;

    next_tag_type = std::nullopt;
    break;
  }
  }

  if (next_tag_type.has_value()) {
    // CRANE_TRACE("Exit EstablishedCranedStateMachine_");
    return m_tag_sync_allocator_->new_object<CqTag>(
        CqTag{next_tag_type.value(), craned});
  }

  // CRANE_TRACE("Exit EstablishedCranedStateMachine_");
  return nullptr;
}

bool CranedKeeper::CheckNodeTimeoutAndClean(CqTag* tag) {
  if (tag->type != CqTag::kEstablishedCraned) return false;
  auto* craned = tag->craned;
  if (craned->m_last_active_time_.load(std::memory_order_acquire) +
          std::chrono::seconds(g_config.CtldConf.CranedTimeout) >=
      std::chrono::steady_clock::now()) {
    return false;
  }
  CRANE_LOGGER_TRACE(g_runtime_status.conn_logger,
                     "Craned {} going to down because timeout",
                     craned->m_craned_id_);
  if (m_craned_disconnected_cb_) {
    g_thread_pool->detach_task([this, craned_id = craned->m_craned_id_]() {
      m_craned_disconnected_cb_(craned_id);
    });
  }
  WriterLock lock(&m_connect_craned_mtx_);

  CRANE_LOGGER_TRACE(g_runtime_status.conn_logger,
                     "Craned {} stub destroyed: timeout.",
                     craned->m_craned_id_);
  craned->Fini();
  m_connected_craned_id_stub_map_.erase(craned->m_craned_id_);
  m_tag_sync_allocator_->delete_object(tag);
  return true;
}

uint32_t CranedKeeper::AvailableCranedCount() {
  absl::ReaderMutexLock r_lock(&m_connect_craned_mtx_);
  return m_connected_craned_id_stub_map_.size();
}

bool CranedKeeper::IsCranedConnected(const CranedId& craned_id) {
  ReaderLock lock(&m_connect_craned_mtx_);
  return m_connected_craned_id_stub_map_.contains(craned_id);
}

std::shared_ptr<CranedStub> CranedKeeper::GetCranedStub(
    const CranedId& craned_id) {
  ReaderLock lock(&m_connect_craned_mtx_);
  auto iter = m_connected_craned_id_stub_map_.find(craned_id);
  if (iter != m_connected_craned_id_stub_map_.end()) return iter->second;

  return nullptr;
}

void CranedKeeper::SetCranedConnectedCb(
    std::function<void(CranedId, const RegToken&)> cb) {
  m_craned_connected_cb_ = std::move(cb);
}

void CranedKeeper::SetCranedDisconnectedCb(std::function<void(CranedId)> cb) {
  m_craned_disconnected_cb_ = std::move(cb);
}

void CranedKeeper::PutNodeIntoUnavailSet(const std::string& crane_id,
                                         const RegToken& token) {
  if (m_cq_closed_) return;

  CRANE_TRACE("Put craned {} into unavail. Token: {}.", crane_id,
              ProtoTimestampToString(token));
  util::lock_guard guard(m_unavail_craned_set_mtx_);
  m_unavail_craned_set_.emplace(crane_id, token);
}

void CranedKeeper::ConnectCranedNode_(CranedId const& craned_id,
                                      const RegToken& token) {
  static Mutex s_craned_id_to_ip_cache_map_mtx;
  static std::unordered_map<CranedId, std::variant<ipv4_t, ipv6_t>>
      s_craned_id_to_ip_cache_map;

  std::string ip_addr;

  {
    util::lock_guard guard(s_craned_id_to_ip_cache_map_mtx);

    auto it = s_craned_id_to_ip_cache_map.find(craned_id);
    if (it != s_craned_id_to_ip_cache_map.end()) {
      if (std::holds_alternative<ipv4_t>(it->second)) {  // Ipv4
        ip_addr = crane::Ipv4ToStr(std::get<ipv4_t>(it->second));
      } else {
        CRANE_ASSERT(std::holds_alternative<ipv6_t>(it->second));
        ip_addr = crane::Ipv6ToStr(std::get<ipv6_t>(it->second));
      }
    } else {
      ipv4_t ipv4_addr;
      ipv6_t ipv6_addr;
      if (crane::ResolveIpv4FromHostname(craned_id, &ipv4_addr)) {
        ip_addr = crane::Ipv4ToStr(ipv4_addr);
        s_craned_id_to_ip_cache_map.emplace(craned_id, ipv4_addr);
      } else if (crane::ResolveIpv6FromHostname(craned_id, &ipv6_addr)) {
        ip_addr = crane::Ipv6ToStr(ipv6_addr);
        s_craned_id_to_ip_cache_map.emplace(craned_id, ipv6_addr);
      } else {
        // Just hostname. It should never happen,
        // but we add error handling here for robustness.
        CRANE_ERROR("Unresolved hostname: {}", craned_id);
        ip_addr = craned_id;
      }
    }
  }

  auto* craned = new CranedStub(this);
  craned->SetRegToken(token);

  // InitializingCraned: BEGIN -> IDLE

  /* TODO: Adjust the value here.
   * In default case, TRANSIENT_FAILURE -> TRANSIENT_FAILURE will use the
   * connection-backoff algorithm. We might need to adjust these values.
   * https://grpc.github.io/grpc/cpp/md_doc_connection-backoff.html
   */
  grpc::ChannelArguments channel_args;
  SetGrpcClientKeepAliveChannelArgs(&channel_args);

  if (g_config.CompressedRpc)
    channel_args.SetCompressionAlgorithm(GRPC_COMPRESS_GZIP);

  CRANE_TRACE("Creating a channel to {} {}:{}. Token: {}. Channel count: {}",
              craned_id, ip_addr, g_config.CranedListenConf.CranedListenPort,
              ProtoTimestampToString(token), m_channel_count_.fetch_add(1) + 1);

  if (g_config.ListenConf.TlsConfig.Enabled) {
    SetTlsHostnameOverride(&channel_args, craned_id,
                           g_config.ListenConf.TlsConfig.DomainSuffix);
    craned->m_channel_ = CreateTcpTlsCustomChannelByIp(
        ip_addr, g_config.CranedListenConf.CranedListenPort,
        g_config.ListenConf.TlsConfig.InternalCerts, channel_args);
  } else
    craned->m_channel_ = CreateTcpInsecureCustomChannel(
        ip_addr, g_config.CranedListenConf.CranedListenPort, channel_args);

  craned->m_prev_channel_state_ = craned->m_channel_->GetState(true);
  craned->m_stub_ = crane::grpc::Craned::NewStub(craned->m_channel_);

  craned->m_craned_id_ = craned_id;
  craned->m_clean_up_cb_ = &CranedKeeper::CranedChannelConnFailNoLock_;

  CqTag* tag = m_tag_sync_allocator_->new_object<CqTag>(
      CqTag{CqTag::kInitializingCraned, craned});

  // Round-robin distribution here.
  // Note: this function might be called from multiple thread.
  //       Use atomic variable here.
  static std::atomic<uint32_t> cur_cq_id = 0;
  uint32_t thread_id = cur_cq_id++ % m_cq_vec_.size();

  util::lock_guard lock(m_cq_mtx_vec_[thread_id]);
  craned->m_channel_->NotifyOnStateChange(
      craned->m_prev_channel_state_,
      std::chrono::system_clock::now() +
          std::chrono::seconds(kCompletionQueueConnectingTimeoutSeconds),
      &m_cq_vec_[thread_id], tag);
}

void CranedKeeper::CranedChannelConnFailNoLock_(CranedStub* stub) {
  CranedKeeper* craned_keeper = stub->m_craned_keeper_;
  craned_keeper->m_connect_craned_mtx_.AssertHeld();
  craned_keeper->m_channel_count_.fetch_sub(1);
  craned_keeper->m_connecting_craned_set_.erase(stub->m_craned_id_);
  CRANE_TRACE("Craned {} connection failed.", stub->m_craned_id_);
}

void CranedKeeper::PeriodConnectCranedThreadFunc_() {
  util::SetCurrentThreadName("PeriConnCraned");

  while (true) {
    if (m_cq_closed_) break;

    // Use a window to limit the maximum number of connecting craned nodes.
    {
      absl::ReaderMutexLock connected_reader_lock(&m_connect_craned_mtx_);
      util::lock_guard guard(m_unavail_craned_set_mtx_);

      uint32_t fetch_num =
          kConcurrentStreamQuota - m_connecting_craned_set_.size();

      auto it = m_unavail_craned_set_.begin();
      while (it != m_unavail_craned_set_.end() && fetch_num > 0) {
        if (!m_connecting_craned_set_.contains(it->first) &&
            !m_connected_craned_id_stub_map_.contains(it->first)) {
          m_connecting_craned_set_.emplace(it->first);
          g_thread_pool->detach_task(
              [this, craned_id = it->first, token = it->second] {
                ConnectCranedNode_(craned_id, token);
              });
          fetch_num--;
        } else {
          CRANE_LOGGER_TRACE(g_runtime_status.conn_logger,
                             "Craned {} is already connecting or connected, "
                             "ignore new connection request. Token {}.",
                             it->first, ProtoTimestampToString(it->second));
        }
        ++it;
      }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }
}

}  // namespace Ctld
/**
 * Copyright (c) 2023 Peking University and Peking University
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

#include "CranedKeeper.h"

#include <google/protobuf/util/time_util.h>

namespace Ctld {

using grpc::ClientContext;
using grpc::Status;

CranedStub::CranedStub(CranedKeeper *craned_keeper)
    : m_craned_keeper_(craned_keeper),
      m_failure_retry_times_(0),
      m_invalid_(true) {
  // The most part of jobs are done in CranedKeeper::RegisterCraneds().
}

CranedStub::~CranedStub() {
  if (m_clean_up_cb_) m_clean_up_cb_(this);
}

CraneErr CranedStub::ExecuteTasks(
    std::vector<TaskInCtld const *> const &tasks) {
  using crane::grpc::ExecuteTasksReply;
  using crane::grpc::ExecuteTasksRequest;

  ExecuteTasksRequest request;
  ExecuteTasksReply reply;
  ClientContext context;
  Status status;

  for (TaskInCtld const *task : tasks) {
    auto *mutable_task = request.mutable_tasks()->Add();

    // Set time_limit
    mutable_task->mutable_time_limit()->CopyFrom(
        google::protobuf::util::TimeUtil::MillisecondsToDuration(
            ToInt64Milliseconds(task->time_limit)));

    // Set resources
    auto *mutable_allocatable_resource =
        mutable_task->mutable_resources()->mutable_allocatable_resource();
    mutable_allocatable_resource->set_cpu_core_limit(
        task->resources.allocatable_resource.cpu_count);
    mutable_allocatable_resource->set_memory_limit_bytes(
        task->resources.allocatable_resource.memory_bytes);
    mutable_allocatable_resource->set_memory_sw_limit_bytes(
        task->resources.allocatable_resource.memory_sw_bytes);

    // Set type
    mutable_task->set_type(task->type);
    mutable_task->set_task_id(task->TaskId());
    mutable_task->set_name(task->name);
    mutable_task->set_account(task->account);
    mutable_task->set_qos(task->qos);
    mutable_task->set_partition(task->TaskToCtld().partition_name());

    for (auto &&node : task->included_nodes) {
      mutable_task->mutable_nodelist()->Add()->assign(node);
    }

    for (auto &&node : task->excluded_nodes) {
      mutable_task->mutable_excludes()->Add()->assign(node);
    }

    mutable_task->set_node_num(task->node_num);
    mutable_task->set_ntasks_per_node(task->ntasks_per_node);
    mutable_task->set_cpus_per_task(task->cpus_per_task);

    mutable_task->set_uid(task->uid);
    mutable_task->set_env(task->env);
    mutable_task->set_cwd(task->cwd);

    for (const auto &hostname : task->CranedIds())
      mutable_task->mutable_allocated_nodes()->Add()->assign(hostname);

    mutable_task->mutable_start_time()->set_seconds(
        task->StartTimeInUnixSecond());
    mutable_task->mutable_time_limit()->set_seconds(
        ToInt64Seconds(task->time_limit));

    if (task->type == crane::grpc::Batch) {
      auto &meta_in_ctld = std::get<BatchMetaInTask>(task->meta);
      auto *mutable_meta = mutable_task->mutable_batch_meta();
      mutable_meta->set_output_file_pattern(meta_in_ctld.output_file_pattern);
      mutable_meta->set_sh_script(meta_in_ctld.sh_script);
    }
  }

  status = m_stub_->ExecuteTask(&context, request, &reply);
  if (!status.ok()) {
    CRANE_DEBUG("Execute RPC for Node {} returned with status not ok: {}",
                m_craned_id_, status.error_message());
    return CraneErr::kRpcFailure;
  }

  return CraneErr::kOk;
}

CraneErr CranedStub::TerminateTasks(const std::vector<task_id_t> &task_ids) {
  using crane::grpc::TerminateTasksReply;
  using crane::grpc::TerminateTasksRequest;

  ClientContext context;
  Status status;
  TerminateTasksRequest request;
  TerminateTasksReply reply;

  for (const auto &id : task_ids) request.add_task_id_list(id);

  status = m_stub_->TerminateTasks(&context, request, &reply);
  if (!status.ok()) {
    CRANE_DEBUG(
        "TerminateRunningTask RPC for Node {} returned with status not ok: {}",
        m_craned_id_, status.error_message());
    return CraneErr::kRpcFailure;
  }

  return CraneErr::kOk;
}

CraneErr CranedStub::TerminateOrphanedTask(task_id_t task_id) {
  using crane::grpc::TerminateOrphanedTaskReply;
  using crane::grpc::TerminateOrphanedTaskRequest;

  ClientContext context;
  Status status;
  TerminateOrphanedTaskRequest request;
  TerminateOrphanedTaskReply reply;

  request.set_task_id(task_id);

  status = m_stub_->TerminateOrphanedTask(&context, request, &reply);
  if (!status.ok()) {
    CRANE_DEBUG(
        "TerminateOrphanedTask RPC for Node {} returned with status not ok: {}",
        m_craned_id_, status.error_message());
    return CraneErr::kRpcFailure;
  }

  if (reply.ok())
    return CraneErr::kOk;
  else
    return CraneErr::kGenericFailure;
}

CraneErr CranedStub::CreateCgroupForTasks(
    std::vector<std::pair<task_id_t, uid_t>> const &task_uid_pairs) {
  using crane::grpc::CreateCgroupForTasksReply;
  using crane::grpc::CreateCgroupForTasksRequest;

  ClientContext context;
  Status status;
  CreateCgroupForTasksRequest request;
  CreateCgroupForTasksReply reply;

  for (auto &&[task_id, uid] : task_uid_pairs) {
    request.mutable_task_id_list()->Add(task_id);
    request.mutable_uid_list()->Add(uid);
  }

  status = m_stub_->CreateCgroupForTasks(&context, request, &reply);
  if (!status.ok()) {
    CRANE_ERROR(
        "CreateCgroupForTasks RPC for Node {} returned with status not ok: {}",
        m_craned_id_, status.error_message());
    return CraneErr::kRpcFailure;
  }

  return CraneErr::kOk;
}

CraneErr CranedStub::ReleaseCgroupForTask(uint32_t task_id, uid_t uid) {
  using crane::grpc::ReleaseCgroupForTaskReply;
  using crane::grpc::ReleaseCgroupForTaskRequest;

  ClientContext context;
  Status status;
  ReleaseCgroupForTaskRequest request;
  ReleaseCgroupForTaskReply reply;

  request.set_task_id(task_id);
  request.set_uid(uid);
  status = m_stub_->ReleaseCgroupForTask(&context, request, &reply);
  if (!status.ok()) {
    CRANE_DEBUG(
        "ReleaseCgroupForTask gRPC for Node {} returned with status not ok: {}",
        m_craned_id_, status.error_message());
    return CraneErr::kRpcFailure;
  }

  return CraneErr::kOk;
}

CraneErr CranedStub::CheckTaskStatus(task_id_t task_id,
                                     crane::grpc::TaskStatus *status) {
  using crane::grpc::CheckTaskStatusReply;
  using crane::grpc::CheckTaskStatusRequest;

  ClientContext context;
  Status grpc_status;
  CheckTaskStatusRequest request;
  CheckTaskStatusReply reply;

  request.set_task_id(task_id);
  grpc_status = m_stub_->CheckTaskStatus(&context, request, &reply);

  if (!grpc_status.ok()) {
    CRANE_DEBUG(
        "CheckTaskStatus gRPC for Node {} returned with status not ok: {}",
        m_craned_id_, grpc_status.error_message());
    return CraneErr::kRpcFailure;
  }

  if (reply.ok()) {
    *status = reply.status();
    return CraneErr::kOk;
  } else
    return CraneErr::kNonExistent;
}

CraneErr CranedStub::ChangeTaskTimeLimit(uint32_t task_id, uint64_t seconds) {
  using crane::grpc::ChangeTaskTimeLimitReply;
  using crane::grpc::ChangeTaskTimeLimitRequest;

  ClientContext context;
  Status grpc_status;
  ChangeTaskTimeLimitRequest request;
  ChangeTaskTimeLimitReply reply;

  request.set_task_id(task_id);
  request.set_time_limit_seconds(seconds);
  grpc_status = m_stub_->ChangeTaskTimeLimit(&context, request, &reply);

  if (!grpc_status.ok()) {
    CRANE_ERROR("ChangeTaskTimeLimitAsync to Craned {} failed: {} ",
                m_craned_id_, grpc_status.error_message());
    return CraneErr::kRpcFailure;
  }

  if (reply.ok())
    return CraneErr::kOk;
  else
    return CraneErr::kGenericFailure;
}

CranedKeeper::CranedKeeper() : m_cq_closed_(false), m_tag_pool_(32, 0) {
  m_cq_thread_ = std::thread(&CranedKeeper::StateMonitorThreadFunc_, this);
  m_period_connect_thread_ =
      std::thread(&CranedKeeper::PeriodConnectCranedThreadFunc_, this);
}

CranedKeeper::~CranedKeeper() {
  m_cq_mtx_.Lock();

  m_cq_.Shutdown();
  m_cq_closed_ = true;

  m_cq_mtx_.Unlock();

  m_cq_thread_.join();
  m_period_connect_thread_.join();

  // Dependency order: rpc_cq -> channel_state_cq -> tag pool.
  // Tag pool's destructor will free all trailing tags in cq.
}

void CranedKeeper::InitAndRegisterCraneds(std::list<CranedId> craned_id_list) {
  WriterLock guard(&m_unavail_craned_list_mtx_);

  m_unavail_craned_list_.insert(craned_id_list.begin(), craned_id_list.end());
  CRANE_TRACE("Trying register all craneds...");
}

void CranedKeeper::StateMonitorThreadFunc_() {
  bool ok;
  CqTag *tag;

  while (true) {
    if (m_cq_.Next((void **)&tag, &ok)) {
      CranedStub *craned;
      switch (tag->type) {
        case CqTag::kInitializingCraned:
          craned = reinterpret_cast<InitializingCranedTagData *>(tag->data)
                       ->craned.get();
          break;
        case CqTag::kEstablishedCraned:
          craned = reinterpret_cast<CranedStub *>(tag->data);
          break;
      }
      // CRANE_TRACE("CQ: ok: {}, tag: {}, craned: {}, prev state: {}", ok,
      // tag->type, (void *)craned, craned->m_prev_channel_state_);

      if (ok) {
        CqTag *next_tag = nullptr;
        grpc_connectivity_state new_state = craned->m_channel_->GetState(true);

        switch (tag->type) {
          case CqTag::kInitializingCraned:
            next_tag = InitCranedStateMachine_(
                (InitializingCranedTagData *)tag->data, new_state);
            break;
          case CqTag::kEstablishedCraned:
            next_tag = EstablishedCranedStateMachine_(craned, new_state);
            break;
        }
        if (next_tag) {
          util::lock_guard lock(m_cq_mtx_);
          if (!m_cq_closed_) {
            // CRANE_TRACE("Registering next tag: {}", next_tag->type);

            craned->m_prev_channel_state_ = new_state;
            // When cq is closed, do not register any more callbacks on it.
            craned->m_channel_->NotifyOnStateChange(
                craned->m_prev_channel_state_,
                std::chrono::system_clock::now() +
                    std::chrono::seconds(kCompletionQueueDelaySeconds),
                &m_cq_, next_tag);
          }
        } else {
          // END state of both state machine. Free the Craned client.
          if (tag->type == CqTag::kInitializingCraned) {
            // free tag_data
            auto *tag_data =
                reinterpret_cast<InitializingCranedTagData *>(tag->data);
            CRANE_TRACE(
                "Failed connect to {}. Waiting for its active connection",
                tag_data->craned->m_craned_id_);

            // When deleting tag_data, the destructor will call
            // PutBackNodeIntoUnavailList_ in m_clean_up_cb_ and put this craned
            // into the re-connecting queue again.
            delete tag_data;
          } else if (tag->type == CqTag::kEstablishedCraned) {
            if (m_craned_is_down_cb_)
              g_thread_pool->push_task(m_craned_is_down_cb_,
                                       craned->m_craned_id_);

            WriterLock lock(&m_connected_craned_mtx_);
            m_connected_craned_id_stub_map_.erase(craned->m_craned_id_);
          } else {
            CRANE_ERROR("Unknown tag type: {}", tag->type);
          }
        }

        util::lock_guard lock(m_tag_pool_mtx_);
        m_tag_pool_.free(tag);
      } else {
        /* ok = false implies that NotifyOnStateChange() timed out.
         * See GRPC code: src/core/ext/filters/client_channel/
         *  channel_connectivity.cc:grpc_channel_watch_connectivity_state()
         *
         * Register the same tag again. Do not free it because we have no newly
         * allocated tag. */
        util::lock_guard lock(m_cq_mtx_);
        if (!m_cq_closed_) {
          // CRANE_TRACE("Registering next tag: {}", tag->type);

          // When cq is closed, do not register any more callbacks on it.
          craned->m_channel_->NotifyOnStateChange(
              craned->m_prev_channel_state_,
              std::chrono::system_clock::now() +
                  std::chrono::seconds(kCompletionQueueDelaySeconds),
              &m_cq_, tag);
        }
      }
    } else {
      // m_cq_.Shutdown() has been called. Exit the thread.
      break;
    }
  }
}

CranedKeeper::CqTag *CranedKeeper::InitCranedStateMachine_(
    InitializingCranedTagData *tag_data, grpc_connectivity_state new_state) {
  // CRANE_TRACE("Enter InitCranedStateMachine_");

  std::optional<CqTag::Type> next_tag_type;
  CranedStub *raw_craned = tag_data->craned.get();

  switch (new_state) {
    case GRPC_CHANNEL_READY: {
      {
        CRANE_TRACE("CONNECTING -> READY. New craned {} connected.",
                    raw_craned->m_craned_id_);
        WriterLock lock(&m_connected_craned_mtx_);

        m_connected_craned_id_stub_map_.emplace(raw_craned->m_craned_id_,
                                                std::move(tag_data->craned));

        raw_craned->m_failure_retry_times_ = 0;
        raw_craned->m_invalid_ = false;
      }
      if (m_craned_is_up_cb_)
        g_thread_pool->push_task(m_craned_is_up_cb_, raw_craned->m_craned_id_);

      // free tag_data
      delete tag_data;

      // Switch to EstablishedCraned state machine
      next_tag_type = CqTag::kEstablishedCraned;
      break;
    }

    case GRPC_CHANNEL_TRANSIENT_FAILURE: {
      if (raw_craned->m_failure_retry_times_ <
          raw_craned->m_maximum_retry_times_) {
        raw_craned->m_failure_retry_times_++;
        next_tag_type = CqTag::kInitializingCraned;

        // CRANE_TRACE(
        // "CONNECTING/TRANSIENT_FAILURE -> TRANSIENT_FAILURE -> CONNECTING");
        // prev                            current              next
        // CONNECTING/TRANSIENT_FAILURE -> TRANSIENT_FAILURE -> CONNECTING
      } else {
        next_tag_type = std::nullopt;
        // CRANE_TRACE("TRANSIENT_FAILURE -> TRANSIENT_FAILURE -> END");
        // prev must be TRANSIENT_FAILURE.
        // when prev is CONNECTING, retry_times = 0
        // prev          current              next
        // TRANSIENT_FAILURE -> TRANSIENT_FAILURE -> END
      }
      break;
    }

    case GRPC_CHANNEL_CONNECTING: {
      if (raw_craned->m_prev_channel_state_ == GRPC_CHANNEL_CONNECTING) {
        if (raw_craned->m_failure_retry_times_ <
            raw_craned->m_maximum_retry_times_) {
          // prev          current
          // CONNECTING -> CONNECTING (Timeout)
          CRANE_TRACE("CONNECTING -> CONNECTING");
          raw_craned->m_failure_retry_times_++;
          next_tag_type = CqTag::kInitializingCraned;
        } else {
          // prev          current       next
          // CONNECTING -> CONNECTING -> END
          CRANE_TRACE("CONNECTING -> CONNECTING -> END");
          next_tag_type = std::nullopt;
        }
      } else {
        // prev    now
        // IDLE -> CONNECTING
        // CRANE_TRACE("IDLE -> CONNECTING");
        next_tag_type = CqTag::kInitializingCraned;
      }
      break;
    }

    case GRPC_CHANNEL_IDLE:
      // InitializingCraned: BEGIN -> IDLE state switching is handled in
      // CranedKeeper::RegisterNewCraneds. Execution should never reach here.
      CRANE_ERROR("Unexpected InitializingCraned IDLE state!");
      break;

    case GRPC_CHANNEL_SHUTDOWN:
      CRANE_ERROR("Unexpected InitializingCraned SHUTDOWN state!");
      break;
  }

  // CRANE_TRACE("Exit InitCranedStateMachine_");
  if (next_tag_type.has_value()) {
    if (next_tag_type.value() == CqTag::kInitializingCraned) {
      util::lock_guard lock(m_tag_pool_mtx_);
      return m_tag_pool_.construct(CqTag{next_tag_type.value(), tag_data});
    } else if (next_tag_type.value() == CqTag::kEstablishedCraned) {
      util::lock_guard lock(m_tag_pool_mtx_);
      return m_tag_pool_.construct(CqTag{next_tag_type.value(), raw_craned});
    }
  }
  return nullptr;
}

CranedKeeper::CqTag *CranedKeeper::EstablishedCranedStateMachine_(
    CranedStub *craned, grpc_connectivity_state new_state) {
  CRANE_TRACE("Enter EstablishedCranedStateMachine_");

  std::optional<CqTag::Type> next_tag_type;

  switch (new_state) {
    case GRPC_CHANNEL_CONNECTING: {
      if (craned->m_prev_channel_state_ == GRPC_CHANNEL_CONNECTING) {
        if (craned->m_failure_retry_times_ < craned->m_maximum_retry_times_) {
          // prev          current
          // CONNECTING -> CONNECTING (Timeout)
          CRANE_TRACE("CONNECTING -> CONNECTING");
          craned->m_failure_retry_times_++;
          next_tag_type = CqTag::kEstablishedCraned;
        } else {
          // prev          current       next
          // CONNECTING -> CONNECTING -> END
          CRANE_TRACE("CONNECTING -> CONNECTING -> END");
          next_tag_type = std::nullopt;
        }
      } else {
        // prev    now
        // IDLE -> CONNECTING
        CRANE_TRACE("IDLE -> CONNECTING");
        next_tag_type = CqTag::kEstablishedCraned;
      }
      break;
    }

    case GRPC_CHANNEL_IDLE: {
      // prev     current
      // READY -> IDLE (the only edge)
      CRANE_TRACE("READY -> IDLE");

      craned->m_invalid_ = true;
      if (m_craned_is_temp_down_cb_)
        g_thread_pool->push_task(m_craned_is_temp_down_cb_,
                                 craned->m_craned_id_);

      next_tag_type = CqTag::kEstablishedCraned;
      break;
    }

    case GRPC_CHANNEL_READY: {
      if (craned->m_prev_channel_state_ == GRPC_CHANNEL_READY) {
        // READY -> READY
        CRANE_TRACE("READY -> READY");
        next_tag_type = CqTag::kEstablishedCraned;
      } else {
        // prev          current
        // CONNECTING -> READY
        CRANE_TRACE("CONNECTING -> READY");

        craned->m_failure_retry_times_ = 0;
        craned->m_invalid_ = false;

        if (m_craned_rec_from_temp_failure_cb_)
          g_thread_pool->push_task(m_craned_rec_from_temp_failure_cb_,
                                   craned->m_craned_id_);

        next_tag_type = CqTag::kEstablishedCraned;
      }
      break;
    }

    case GRPC_CHANNEL_TRANSIENT_FAILURE: {
      if (craned->m_prev_channel_state_ == GRPC_CHANNEL_READY) {
        // prev     current              next
        // READY -> TRANSIENT_FAILURE -> CONNECTING
        CRANE_TRACE("READY -> TRANSIENT_FAILURE -> CONNECTING");

        craned->m_invalid_ = true;
        if (m_craned_is_temp_down_cb_)
          g_thread_pool->push_task(m_craned_is_temp_down_cb_,
                                   craned->m_craned_id_);

        next_tag_type = CqTag::kEstablishedCraned;
      } else if (craned->m_prev_channel_state_ == GRPC_CHANNEL_CONNECTING) {
        if (craned->m_failure_retry_times_ < craned->m_maximum_retry_times_) {
          // prev          current              next
          // CONNECTING -> TRANSIENT_FAILURE -> CONNECTING
          CRANE_TRACE("CONNECTING -> TRANSIENT_FAILURE -> CONNECTING ({}/{})",
                      craned->m_failure_retry_times_,
                      craned->m_maximum_retry_times_);

          craned->m_failure_retry_times_++;
          next_tag_type = CqTag::kEstablishedCraned;
        } else {
          // prev          current              next
          // CONNECTING -> TRANSIENT_FAILURE -> END
          CRANE_TRACE("CONNECTING -> TRANSIENT_FAILURE -> END");
          next_tag_type = std::nullopt;
        }
      } else if (craned->m_prev_channel_state_ ==
                 GRPC_CHANNEL_TRANSIENT_FAILURE) {
        if (craned->m_failure_retry_times_ < craned->m_maximum_retry_times_) {
          // prev                 current
          // TRANSIENT_FAILURE -> TRANSIENT_FAILURE (Timeout)
          CRANE_TRACE("TRANSIENT_FAILURE -> TRANSIENT_FAILURE ({}/{})",
                      craned->m_failure_retry_times_,
                      craned->m_maximum_retry_times_);
          craned->m_failure_retry_times_++;
          next_tag_type = CqTag::kEstablishedCraned;
        } else {
          // prev                 current       next
          // TRANSIENT_FAILURE -> TRANSIENT_FAILURE -> END
          CRANE_TRACE("TRANSIENT_FAILURE -> TRANSIENT_FAILURE -> END");
          next_tag_type = std::nullopt;
        }
      } else if (craned->m_prev_channel_state_ == GRPC_CHANNEL_IDLE) {
        CRANE_TRACE("IDLE -> TRANSIENT_FAILURE");
        next_tag_type = CqTag::kEstablishedCraned;
      } else {
        CRANE_ERROR("Unknown State: {} -> TRANSIENT_FAILURE",
                    craned->m_prev_channel_state_);
      }
      break;
    }

    case GRPC_CHANNEL_SHUTDOWN:
      CRANE_ERROR("Unexpected SHUTDOWN channel state on Established Craned {}!",
                  craned->m_craned_id_);
      break;
  }

  if (next_tag_type.has_value()) {
    util::lock_guard lock(m_tag_pool_mtx_);
    CRANE_TRACE("Exit EstablishedCranedStateMachine_");
    return m_tag_pool_.construct(CqTag{next_tag_type.value(), craned});
  }

  CRANE_TRACE("Exit EstablishedCranedStateMachine_");
  return nullptr;
}

uint32_t CranedKeeper::AvailableCranedCount() {
  absl::ReaderMutexLock r_lock(&m_connected_craned_mtx_);
  return m_connected_craned_id_stub_map_.size();
}

CranedStub *CranedKeeper::GetCranedStub(const CranedId &craned_id) {
  ReaderLock lock(&m_connected_craned_mtx_);
  auto iter = m_connected_craned_id_stub_map_.find(craned_id);
  if (iter != m_connected_craned_id_stub_map_.end())
    return iter->second.get();
  else
    return nullptr;
}

void CranedKeeper::SetCranedIsUpCb(std::function<void(CranedId)> cb) {
  m_craned_is_up_cb_ = std::move(cb);
}

void CranedKeeper::SetCranedIsDownCb(std::function<void(CranedId)> cb) {
  m_craned_is_down_cb_ = std::move(cb);
}

void CranedKeeper::SetCranedIsTempDownCb(std::function<void(CranedId)> cb) {
  m_craned_is_temp_down_cb_ = std::move(cb);
}

void CranedKeeper::SetCranedIsTempUpCb(std::function<void(CranedId)> cb) {
  m_craned_rec_from_temp_failure_cb_ = std::move(cb);
}

void CranedKeeper::PutNodeIntoUnavailList(const std::string &crane_id) {
  util::lock_guard guard(g_craned_keeper->m_unavail_craned_list_mtx_);
  g_craned_keeper->m_unavail_craned_list_.emplace(crane_id);
}

void CranedKeeper::ConnectCranedNode_(CranedId const &craned_id) {
  std::string ip_addr;
  if (!crane::ResolveIpv4FromHostname(craned_id, &ip_addr)) {
    ip_addr = craned_id;
  }

  auto *cq_tag_data = new InitializingCranedTagData{};
  cq_tag_data->craned = std::make_unique<CranedStub>(this);

  // InitializingCraned: BEGIN -> IDLE

  /* Todo: Adjust the value here.
   * In default case, TRANSIENT_FAILURE -> TRANSIENT_FAILURE will use the
   * connection-backoff algorithm. We might need to adjust these values.
   * https://grpc.github.io/grpc/cpp/md_doc_connection-backoff.html
   */
  grpc::ChannelArguments channel_args;

  if (g_config.CompressedRpc)
    channel_args.SetCompressionAlgorithm(GRPC_COMPRESS_GZIP);

  channel_args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 1000 /*ms*/);
  channel_args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 2 /*s*/ * 1000
                      /*ms*/);
  channel_args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS,
                      30 /*s*/ * 1000 /*ms*/);
  //    channel_args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 5 /*s*/ * 1000 /*ms*/);
  //    channel_args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 10 /*s*/ * 1000
  //    /*ms*/); channel_args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1
  //    /*true*/);

  CRANE_TRACE("Creating a channel to {}:{}. Channel count: {}", craned_id,
              kCranedDefaultPort, m_channel_count_.fetch_add(1) + 1);

  std::string addr_port = fmt::format("{}:{}", ip_addr, kCranedDefaultPort);
  if (g_config.ListenConf.UseTls) {
    channel_args.SetSslTargetNameOverride(
        fmt::format("{}.{}", craned_id, g_config.ListenConf.DomainSuffix));

    grpc::SslCredentialsOptions ssl_opts;
    // pem_root_certs is actually the certificate of server side rather than
    // CA certificate. CA certificate is not needed.
    // Since we use the same cert/key pair for both cranectld/craned,
    // pem_root_certs is set to the same certificate.
    ssl_opts.pem_root_certs = g_config.ListenConf.ServerCertContent;
    ssl_opts.pem_cert_chain = g_config.ListenConf.ServerCertContent;
    ssl_opts.pem_private_key = g_config.ListenConf.ServerKeyContent;

    cq_tag_data->craned->m_channel_ = grpc::CreateCustomChannel(
        addr_port, grpc::SslCredentials(ssl_opts), channel_args);
  } else {
    cq_tag_data->craned->m_channel_ = grpc::CreateCustomChannel(
        addr_port, grpc::InsecureChannelCredentials(), channel_args);
  }

  cq_tag_data->craned->m_prev_channel_state_ =
      cq_tag_data->craned->m_channel_->GetState(true);
  cq_tag_data->craned->m_stub_ =
      crane::grpc::Craned::NewStub(cq_tag_data->craned->m_channel_);

  cq_tag_data->craned->m_craned_id_ = craned_id;
  cq_tag_data->craned->m_clean_up_cb_ = CranedChannelConnectFail_;

  cq_tag_data->craned->m_maximum_retry_times_ = 2;

  CqTag *tag;
  {
    util::lock_guard lock(m_tag_pool_mtx_);
    tag = m_tag_pool_.construct(CqTag{CqTag::kInitializingCraned, cq_tag_data});
  }

  cq_tag_data->craned->m_channel_->NotifyOnStateChange(
      cq_tag_data->craned->m_prev_channel_state_,
      std::chrono::system_clock::now() +
          std::chrono::seconds(kCompletionQueueDelaySeconds),
      &m_cq_, tag);
}

void CranedKeeper::CranedChannelConnectFail_(CranedStub *stub) {
  CranedKeeper *node_keeper = stub->m_craned_keeper_;
  util::lock_guard guard(node_keeper->m_unavail_craned_list_mtx_);

  node_keeper->m_channel_count_.fetch_sub(1);
  node_keeper->m_connecting_craned_set_.erase(stub->m_craned_id_);
}

void CranedKeeper::PeriodConnectCranedThreadFunc_() {
  while (true) {
    if (m_cq_closed_) break;

    {
      util::lock_guard guard(m_unavail_craned_list_mtx_);
      for (const auto &craned : m_unavail_craned_list_) {
        if (!m_connecting_craned_set_.contains(craned)) {
          m_connecting_craned_set_.emplace(craned);
          g_thread_pool->push_task(
              [this, craned_id = craned]() { ConnectCranedNode_(craned_id); });
        }
      }
      m_unavail_craned_list_.clear();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }
}

}  // namespace Ctld
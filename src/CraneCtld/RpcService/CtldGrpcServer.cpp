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

#include "CtldGrpcServer.h"

#include <grpcpp/support/status.h>

#include "AccountManager.h"
#include "AccountMetaContainer.h"
#include "CranedKeeper.h"
#include "CranedMetaContainer.h"
#include "CtldPublicDefs.h"
#include "Security/VaultClient.h"
#include "TaskScheduler.h"
#include "crane/PluginClient.h"
#include "protos/PublicDefs.pb.h"

namespace Ctld {

grpc::Status CtldForInternalServiceImpl::StepStatusChange(
    grpc::ServerContext *context,
    const crane::grpc::StepStatusChangeRequest *request,
    crane::grpc::StepStatusChangeReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};

  g_task_scheduler->StepStatusChangeAsync(
      request->job_id(), request->step_id(), request->craned_id(),
      request->new_status(), request->exit_code(), request->reason(),
      request->timestamp());
  response->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status CtldForInternalServiceImpl::CranedTriggerReverseConn(
    grpc::ServerContext *context,
    const crane::grpc::CranedTriggerReverseConnRequest *request,
    google::protobuf::Empty *response) {
  const auto &craned_id = request->craned_id();
  CRANE_TRACE("Craned {} requires Ctld to connect.", craned_id);
  if (!g_meta_container->CheckCranedAllowed(request->craned_id())) {
    CRANE_WARN("Reject register request from unknown node {}",
               request->craned_id());
    return grpc::Status::OK;
  }

  if (!g_craned_keeper->IsCranedConnected(craned_id)) {
    g_craned_keeper->PutNodeIntoUnavailSet(craned_id, request->token());
  } else {
    // Before configure, craned should be connected but not online
    if (g_meta_container->CheckCranedOnline(craned_id)) {
      CRANE_TRACE(
          "Already online craned {} notify craned connected, consider it down.",
          craned_id);
      g_meta_container->CranedDown(craned_id);
    }
    auto stub = g_craned_keeper->GetCranedStub(craned_id);
    if (stub != nullptr) {
      stub->SetRegToken(request->token());
      g_thread_pool->detach_task([stub, token = request->token(), craned_id] {
        stub->ConfigureCraned(craned_id, token);
      });
    } else {
      g_craned_keeper->PutNodeIntoUnavailSet(craned_id, request->token());
    }
  }

  return grpc::Status::OK;
}

grpc::Status CtldForInternalServiceImpl::CranedRegister(
    grpc::ServerContext *context,
    const crane::grpc::CranedRegisterRequest *request,
    crane::grpc::CranedRegisterReply *response) {
  CRANE_ASSERT(g_meta_container->CheckCranedAllowed(request->craned_id()));

  if (g_meta_container->CheckCranedOnline(request->craned_id())) {
    CRANE_WARN("Reject register request from already online node {}",
               request->craned_id());
    response->set_ok(false);
    return grpc::Status::OK;
  }

  if (!g_config.IgnoreConfigInconsistency &&
      (request->remote_meta().config_crc() != g_config.ConfigCrcVal)) {
    CRANE_ERROR(
        "CranedNode #{} appears to have a diffrent config.yaml than the "
        "CraneCtld.",
        request->craned_id());
  }

  auto stub = g_craned_keeper->GetCranedStub(request->craned_id());
  if (stub == nullptr) {
    CRANE_WARN("Craned {} to be ready is not connected.", request->craned_id());
    response->set_ok(false);
    return grpc::Status::OK;
  }

  if (!stub->CheckToken(request->token())) {
    CRANE_WARN("Reject register request from node {} with invalid token.",
               request->craned_id());
    response->set_ok(false);
    return grpc::Status::OK;
  }

  std::unordered_map<job_id_t, std::set<step_id_t>> orphaned_steps;

  // Some job or step lost, terminate them with orphaned status
  if (!request->remote_meta().lost_jobs().empty()) {
    CRANE_INFO("Craned {} lost job allocation:[{}].", request->craned_id(),
               absl::StrJoin(request->remote_meta().lost_jobs(), ","));
    for (const auto &job_id : request->remote_meta().lost_jobs()) {
      orphaned_steps[job_id].emplace(kDaemonStepId);
    }
  }

  if (!request->remote_meta().lost_steps().empty()) {
    for (const auto &[job_id, steps] : request->remote_meta().lost_steps()) {
      orphaned_steps[job_id].insert(steps.steps().begin(), steps.steps().end());
    }
  }
  CRANE_INFO("Craned {} lost executing step: [{}]", request->craned_id(),
             util::JobStepsToString(orphaned_steps));

  if (!orphaned_steps.empty()) {
    auto now = google::protobuf::util::TimeUtil::GetCurrentTime();
    g_task_scheduler->TerminateOrphanedSteps(orphaned_steps,
                                             request->craned_id());
    for (const auto &[job_id, steps] : orphaned_steps) {
      // Reverse order: we should process larger step_id first to avoid warnings
      // abort daemon/primary steps
      for (const auto step_id : steps | std::views::reverse)
        g_task_scheduler->StepStatusChangeWithReasonAsync(
            job_id, step_id, request->craned_id(),
            crane::grpc::TaskStatus::Failed, ExitCode::EC_CRANED_DOWN,
            "Craned re-registered but step lost.", now);
    }
  }

  stub->SetReady();
  g_meta_container->CranedUp(request->craned_id(), request->remote_meta());
  response->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status CtldForInternalServiceImpl::CranedPing(
    grpc::ServerContext *context, const crane::grpc::CranedPingRequest *request,
    crane::grpc::CranedPingReply *response) {
  if (!g_meta_container->CheckCranedOnline(request->craned_id())) {
    CRANE_WARN("Reject ping from offline node {}", request->craned_id());
    response->set_ok(false);
    return grpc::Status::OK;
  }

  auto stub = g_craned_keeper->GetCranedStub(request->craned_id());
  if (stub == nullptr) {
    CRANE_WARN("Ping from Craned {}, which is not connected.",
               request->craned_id());
    response->set_ok(false);
    return grpc::Status::OK;
  }
  stub->UpdateLastActiveTime();
  response->set_ok(true);

  return grpc::Status::OK;
}

grpc::Status CtldForInternalServiceImpl::CforedStream(
    grpc::ServerContext *context,
    grpc::ServerReaderWriter<crane::grpc::StreamCtldReply,
                             crane::grpc::StreamCforedRequest> *stream) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};

  using crane::grpc::InteractiveTaskType;
  using crane::grpc::StreamCforedRequest;
  using crane::grpc::StreamCtldReply;
  using grpc::Status;

  enum class StreamState {
    kWaitRegReq = 0,
    kWaitMsg,
    kCleanData,
  };

  bool ok;

  StreamCforedRequest cfored_request;

  auto stream_writer = std::make_shared<CforedStreamWriter>(stream);
  std::weak_ptr<CforedStreamWriter> writer_weak_ptr(stream_writer);
  std::string cfored_name;

  CRANE_TRACE("CforedStream from {} created.", context->peer());

  StreamState state = StreamState::kWaitRegReq;
  while (true) {
    switch (state) {
    case StreamState::kWaitRegReq:
      ok = stream->Read(&cfored_request);
      if (ok) {
        if (cfored_request.type() != StreamCforedRequest::CFORED_REGISTRATION) {
          CRANE_ERROR("Expect type CFORED_REGISTRATION from peer {}.",
                      context->peer());
          return Status::CANCELLED;
        }

        cfored_name = cfored_request.payload_cfored_reg().cfored_name();
        CRANE_INFO("Cfored {} registered.", cfored_name);

        ok = stream_writer->WriteCforedRegistrationAck({});
        if (ok) {
          state = StreamState::kWaitMsg;
        } else {
          CRANE_ERROR(
              "Failed to send msg to cfored {}. Connection is broken. "
              "Exiting...",
              cfored_name);
          state = StreamState::kCleanData;
        }

      } else {
        state = StreamState::kCleanData;
      }

      break;

    case StreamState::kWaitMsg: {
      ok = stream->Read(&cfored_request);
      if (ok) {
        switch (cfored_request.type()) {
        case StreamCforedRequest::TASK_REQUEST: {
          auto const &payload = cfored_request.payload_task_req();
          auto task = std::make_unique<TaskInCtld>();
          task->SetFieldsByTaskToCtld(payload.task());

          auto &meta = std::get<InteractiveMetaInTask>(task->meta);

          meta.cb_task_res_allocated =
              [writer_weak_ptr](task_id_t task_id,
                                std::string const &allocated_craned_regex,
                                std::vector<std::string> const &craned_ids) {
                if (auto writer = writer_weak_ptr.lock(); writer)
                  writer->WriteTaskResAllocReply(
                      task_id,
                      {std::make_pair(allocated_craned_regex, craned_ids)});
              };

          meta.cb_task_cancel = [writer_weak_ptr](task_id_t task_id) {
            CRANE_TRACE("Sending TaskCancelRequest in task_cancel", task_id);
            if (auto writer = writer_weak_ptr.lock(); writer)
              writer->WriteTaskCancelRequest(task_id);
          };

          meta.cb_task_completed = [this, cfored_name, writer_weak_ptr](
                                       task_id_t task_id,
                                       bool send_completion_ack) {
            CRANE_TRACE("The completion callback of task #{} has been called.",
                        task_id);
            if (auto writer = writer_weak_ptr.lock(); writer) {
              if (send_completion_ack)
                writer->WriteTaskCompletionAckReply(task_id);
            } else {
              CRANE_ERROR(
                  "Stream writer of ia task #{} has been destroyed. "
                  "TaskCompletionAckReply will not be sent.",
                  task_id);
            }

            m_ctld_server_->m_mtx_.Lock();

            // If cfored disconnected, the cfored_name should have be
            // removed from the map and the task completion callback is
            // generated from cleaning the remaining tasks by calling
            // g_task_scheduler->TerminateTask(), we should ignore this
            // callback since the task id has already been cleaned.
            auto iter =
                m_ctld_server_->m_cfored_running_tasks_.find(cfored_name);
            if (iter != m_ctld_server_->m_cfored_running_tasks_.end())
              iter->second.erase(task_id);
            m_ctld_server_->m_mtx_.Unlock();
          };

          auto submit_result =
              g_task_scheduler->SubmitTaskToScheduler(std::move(task));
          std::expected<task_id_t, std::string> result;
          if (submit_result.has_value()) {
            result = std::expected<task_id_t, std::string>{
                submit_result.value().get()};
          } else {
            result = std::unexpected(CraneErrStr(submit_result.error()));
          }
          ok = stream_writer->WriteTaskIdReply(payload.pid(), result);

          if (!ok) {
            CRANE_ERROR(
                "Failed to send msg to cfored {}. Connection is broken. "
                "Exiting...",
                cfored_name);
            state = StreamState::kCleanData;
          } else {
            if (result.has_value()) {
              m_ctld_server_->m_mtx_.Lock();
              m_ctld_server_->m_cfored_running_tasks_[cfored_name].emplace(
                  result.value());
              m_ctld_server_->m_mtx_.Unlock();
            }
          }
        } break;

        case StreamCforedRequest::TASK_COMPLETION_REQUEST: {
          auto const &payload = cfored_request.payload_task_complete_req();
          CRANE_TRACE("Recv TaskCompletionReq of Task #{}", payload.job_id());
          if (g_task_scheduler->TerminatePendingOrRunningIaTask(
                  payload.job_id()) != CraneErrCode::SUCCESS)
            stream_writer->WriteTaskCompletionAckReply(payload.job_id());
          else {
            CRANE_TRACE(
                "Termination of task #{} succeeded. "
                "Leave TaskCompletionAck to TaskStatusChange.",
                payload.job_id());
          }
        } break;

        case StreamCforedRequest::CFORED_GRACEFUL_EXIT: {
          stream_writer->WriteCforedGracefulExitAck();
          stream_writer->Invalidate();
          state = StreamState::kCleanData;
        } break;

        default:
          CRANE_ERROR("Not expected cfored request type: {}",
                      StreamCforedRequest_CforedRequestType_Name(
                          cfored_request.type()));
          return Status::CANCELLED;
        }
      } else {
        state = StreamState::kCleanData;
      }
    } break;

    case StreamState::kCleanData: {
      CRANE_INFO("Cfored {} disconnected. Cleaning its data...", cfored_name);
      stream_writer->Invalidate();
      m_ctld_server_->m_mtx_.Lock();

      auto const &running_task_set =
          m_ctld_server_->m_cfored_running_tasks_[cfored_name];
      std::vector<task_id_t> running_tasks(running_task_set.begin(),
                                           running_task_set.end());
      m_ctld_server_->m_cfored_running_tasks_.erase(cfored_name);
      m_ctld_server_->m_mtx_.Unlock();

      for (task_id_t task_id : running_tasks) {
        g_task_scheduler->TerminateRunningTask(task_id);
      }

      return Status::OK;
    }
    }
  }
}

grpc::Status CraneCtldServiceImpl::SubmitBatchTask(
    grpc::ServerContext *context,
    const crane::grpc::SubmitBatchTaskRequest *request,
    crane::grpc::SubmitBatchTaskReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->task().uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  // Check task type
  if (request->task().type() == crane::grpc::TaskType::Container &&
      !g_config.Container.Enabled) {
    response->set_ok(false);
    response->set_code(CraneErrCode::ERR_CRI_DISABLED);
    return grpc::Status::OK;
  }

  auto task = std::make_unique<TaskInCtld>();
  task->SetFieldsByTaskToCtld(request->task());

  auto result = g_task_scheduler->SubmitTaskToScheduler(std::move(task));
  if (result.has_value()) {
    task_id_t id = result.value().get();
    if (id != 0) {
      response->set_ok(true);
      response->set_task_id(id);
    } else {
      response->set_ok(false);
      response->set_code(CraneErrCode::ERR_BEYOND_TASK_ID);
    }
  } else {
    response->set_ok(false);
    response->set_code(result.error());
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::SubmitBatchTasks(
    grpc::ServerContext *context,
    const crane::grpc::SubmitBatchTasksRequest *request,
    crane::grpc::SubmitBatchTasksReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->task().uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  // Check task type
  if (request->task().type() == crane::grpc::TaskType::Container &&
      !g_config.Container.Enabled) {
    response->add_task_id_list(0);
    response->add_code_list(CraneErrCode::ERR_CRI_DISABLED);
    return grpc::Status::OK;
  }

  std::vector<CraneExpected<std::future<task_id_t>>> results;

  uint32_t task_count = request->count();
  const auto &task_to_ctld = request->task();
  results.reserve(task_count);

  for (int i = 0; i < task_count; i++) {
    auto task = std::make_unique<TaskInCtld>();
    task->SetFieldsByTaskToCtld(task_to_ctld);

    auto result = g_task_scheduler->SubmitTaskToScheduler(std::move(task));
    results.emplace_back(std::move(result));
  }

  for (auto &res : results) {
    if (res.has_value())
      response->mutable_task_id_list()->Add(res.value().get());
    else
      response->mutable_code_list()->Add(std::move(res.error()));
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::CancelTask(
    grpc::ServerContext *context, const crane::grpc::CancelTaskRequest *request,
    crane::grpc::CancelTaskReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};

  *response = g_task_scheduler->CancelPendingOrRunningTask(*request);
  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::QueryCranedInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryCranedInfoRequest *request,
    crane::grpc::QueryCranedInfoReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};

  if (request->craned_name().empty()) {
    *response = g_meta_container->QueryAllCranedInfo();
  } else {
    *response = g_meta_container->QueryCranedInfo(request->craned_name());
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::QueryPartitionInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryPartitionInfoRequest *request,
    crane::grpc::QueryPartitionInfoReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};

  if (request->partition_name().empty()) {
    *response = g_meta_container->QueryAllPartitionInfo();
  } else {
    *response = g_meta_container->QueryPartitionInfo(request->partition_name());
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::QueryReservationInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryReservationInfoRequest *request,
    crane::grpc::QueryReservationInfoReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  auto res = g_account_manager->CheckUidIsAdmin(request->uid());
  if (!res) {
    response->set_ok(false);
    response->set_reason(CraneErrStr(res.error()));
    return grpc::Status::OK;
  }

  if (request->reservation_name().empty()) {
    *response = g_meta_container->QueryAllResvInfo();
  } else {
    *response = g_meta_container->QueryResvInfo(request->reservation_name());
  }
  response->set_ok(true);

  return grpc::Status::OK;
}
grpc::Status CraneCtldServiceImpl::QueryLicensesInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryLicensesInfoRequest *request,
    crane::grpc::QueryLicensesInfoReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};

  g_licenses_manager->GetLicensesInfo(request, response);
  response->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::ModifyTask(
    grpc::ServerContext *context, const crane::grpc::ModifyTaskRequest *request,
    crane::grpc::ModifyTaskReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  using ModifyTaskRequest = crane::grpc::ModifyTaskRequest;

  auto res = g_account_manager->CheckUidIsAdmin(request->uid());
  if (!res) {
    for (auto task_id : request->task_ids()) {
      response->add_not_modified_tasks(task_id);
      if (res.error() == CraneErrCode::ERR_INVALID_USER) {
        response->add_not_modified_reasons("User is not a user of Crane");
      } else if (res.error() == CraneErrCode::ERR_USER_NO_PRIVILEGE) {
        response->add_not_modified_reasons("User has insufficient privilege");
      }
    }
    return grpc::Status::OK;
  }

  CraneErrCode err;
  if (request->attribute() == ModifyTaskRequest::TimeLimit) {
    for (auto task_id : request->task_ids()) {
      err = g_task_scheduler->ChangeTaskTimeLimit(
          task_id, request->time_limit_seconds());
      if (err == CraneErrCode::SUCCESS) {
        response->add_modified_tasks(task_id);
      } else if (err == CraneErrCode::ERR_NON_EXISTENT) {
        response->add_not_modified_tasks(task_id);
        response->add_not_modified_reasons(fmt::format(
            "Task #{} was not found in running or pending queue.", task_id));
      } else if (err == CraneErrCode::ERR_INVALID_PARAM) {
        response->add_not_modified_tasks(task_id);
        response->add_not_modified_reasons("Invalid time limit value.");
      } else {
        response->add_not_modified_tasks(task_id);
        response->add_not_modified_reasons(
            fmt::format("Failed to change the time limit of Task#{}: {}.",
                        task_id, CraneErrStr(err)));
      }
    }
  } else if (request->attribute() == ModifyTaskRequest::Priority) {
    for (auto task_id : request->task_ids()) {
      err = g_task_scheduler->ChangeTaskPriority(task_id,
                                                 request->mandated_priority());
      if (err == CraneErrCode::SUCCESS) {
        response->add_modified_tasks(task_id);
      } else if (err == CraneErrCode::ERR_NON_EXISTENT) {
        response->add_not_modified_tasks(task_id);
        response->add_not_modified_reasons(
            fmt::format("Task #{} was not found in pending queue.", task_id));
      } else {
        response->add_not_modified_tasks(task_id);
        response->add_not_modified_reasons(
            fmt::format("Failed to change priority: {}.", CraneErrStr(err)));
      }
    }
  } else if (request->attribute() == ModifyTaskRequest::Hold) {
    int64_t secs = request->hold_seconds();
    std::vector<std::pair<task_id_t, std::future<CraneErrCode>>> results;
    results.reserve(request->task_ids().size());
    for (auto task_id : request->task_ids()) {
      results.emplace_back(
          task_id, g_task_scheduler->HoldReleaseTaskAsync(task_id, secs));
    }
    for (auto &[task_id, res] : results) {
      err = res.get();
      if (err == CraneErrCode::SUCCESS) {
        response->add_modified_tasks(task_id);
      } else if (err == CraneErrCode::ERR_NON_EXISTENT) {
        response->add_not_modified_tasks(task_id);
        response->add_not_modified_reasons(
            fmt::format("Task #{} was not found in pending queue.", task_id));
      } else {
        response->add_not_modified_tasks(false);
        response->add_not_modified_reasons(
            fmt::format("Failed to hold/release job: {}.", CraneErrStr(err)));
      }
    }
  } else {
    for (auto task_id : request->task_ids()) {
      response->add_not_modified_tasks(task_id);
      response->add_not_modified_reasons("Invalid function.");
    }
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::ModifyTasksExtraAttrs(
    grpc::ServerContext *context,
    const crane::grpc::ModifyTasksExtraAttrsRequest *request,
    crane::grpc::ModifyTasksExtraAttrsReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  auto res = g_account_manager->CheckUidIsAdmin(request->uid());
  if (!res) {
    for (const auto [task_id, _] : request->extra_attrs_list()) {
      response->add_not_modified_tasks(task_id);
      if (res.error() == CraneErrCode::ERR_INVALID_USER) {
        response->add_not_modified_reasons("User is not a user of Crane");
      } else if (res.error() == CraneErrCode::ERR_USER_NO_PRIVILEGE) {
        response->add_not_modified_reasons("User has insufficient privilege");
      }
    }
    return grpc::Status::OK;
  }

  CraneErrCode err;
  for (const auto [task_id, extra_attrs] : request->extra_attrs_list()) {
    err = g_task_scheduler->ChangeTaskExtraAttrs(task_id, extra_attrs);
    if (err == CraneErrCode::SUCCESS) {
      response->add_modified_tasks(task_id);
    } else if (err == CraneErrCode::ERR_NON_EXISTENT) {
      response->add_not_modified_tasks(task_id);
      response->add_not_modified_reasons(
          fmt::format("Task #{} was not found in pd/r queue.", task_id));
    } else {
      response->add_not_modified_tasks(task_id);
      response->add_not_modified_reasons(
          fmt::format("Failed to change extra_attrs: {}.", CraneErrStr(err)));
    }
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::ModifyNode(
    grpc::ServerContext *context,
    const crane::grpc::ModifyCranedStateRequest *request,
    crane::grpc::ModifyCranedStateReply *response) {
  CRANE_TRACE("Received update state request: {}",
              crane::grpc::CranedControlState_Name(request->new_state()));
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};

  auto res = g_account_manager->CheckUidIsAdmin(request->uid());
  if (!res) {
    for (auto crane_id : request->craned_ids()) {
      response->add_not_modified_nodes(crane_id);
      if (res.error() == CraneErrCode::ERR_INVALID_USER) {
        response->add_not_modified_reasons("User is not a user of Crane");
      } else if (res.error() == CraneErrCode::ERR_USER_NO_PRIVILEGE) {
        response->add_not_modified_reasons("User has insufficient privilege");
      }
    }
    return grpc::Status::OK;
  }

  if (request->new_state() == crane::grpc::CRANE_POWEROFF ||
      request->new_state() == crane::grpc::CRANE_SLEEP ||
      request->new_state() == crane::grpc::CRANE_WAKE ||
      request->new_state() == crane::grpc::CRANE_POWERON ||
      request->new_state() == crane::grpc::CRANE_UNREGISTER_POWER) {
    if (!g_config.Plugin.Enabled || g_plugin_client == nullptr) {
      for (const auto &crane_id : request->craned_ids()) {
        response->add_not_modified_nodes(crane_id);
        response->add_not_modified_reasons(
            "Plugin system not available for update state");
      }
      return grpc::Status::OK;
    }

    for (const auto &crane_id : request->craned_ids()) {
      if (request->new_state() == crane::grpc::CRANE_UNREGISTER_POWER) {
        g_plugin_client->UpdatePowerStateHookAsync(crane_id,
                                                   request->new_state());
        response->add_modified_nodes(crane_id);
        continue;
      }

      auto craned_meta = g_meta_container->GetCranedMetaPtr(crane_id);
      if (!craned_meta) {
        response->add_not_modified_nodes(crane_id);
        response->add_not_modified_reasons("Node not found");
        continue;
      }

      const auto requested_state = request->new_state();
      const auto current_state = craned_meta->power_state;

      if ((requested_state == crane::grpc::CranedControlState::CRANE_POWEROFF ||
           requested_state == crane::grpc::CranedControlState::CRANE_SLEEP) &&
          current_state == crane::grpc::CranedPowerState::CRANE_POWER_ACTIVE) {
        response->add_not_modified_nodes(crane_id);
        response->add_not_modified_reasons(
            "Node is running, can't sleep or poweroff");
        continue;
      }
      if ((requested_state == crane::grpc::CranedControlState::CRANE_WAKE ||
           requested_state == crane::grpc::CranedControlState::CRANE_POWERON) &&
          (current_state == crane::grpc::CranedPowerState::CRANE_POWER_IDLE ||
           current_state ==
               crane::grpc::CranedPowerState::CRANE_POWER_ACTIVE)) {
        response->add_not_modified_nodes(crane_id);
        response->add_not_modified_reasons(
            "Node is idle or running, don't need to wake up or poweron");
        continue;
      }

      CRANE_INFO("Updating state {} on node {}",
                 crane::grpc::CranedControlState_Name(request->new_state()),
                 crane_id);

      g_plugin_client->UpdatePowerStateHookAsync(crane_id,
                                                 request->new_state());
      response->add_modified_nodes(crane_id);
    }

    return grpc::Status::OK;
  }

  *response = g_meta_container->ChangeNodeState(*request);

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::ModifyPartitionAcl(
    grpc::ServerContext *context,
    const crane::grpc::ModifyPartitionAclRequest *request,
    crane::grpc::ModifyPartitionAclReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  CraneExpected<void> result;

  std::unordered_set<std::string> accounts;

  for (const auto &account_name : request->accounts()) {
    accounts.insert(account_name);
  }

  result = g_account_manager->CheckModifyPartitionAcl(
      request->uid(), request->partition(), accounts);

  if (!result) {
    response->set_ok(false);
    response->set_code(result.error());
    return grpc::Status::OK;
  }

  result = g_meta_container->ModifyPartitionAcl(
      request->partition(), request->is_allowed_list(), std::move(accounts));

  if (!result) {
    response->set_ok(false);
    response->set_code(result.error());
  } else {
    response->set_ok(true);
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::QueryTasksInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryTasksInfoRequest *request,
    crane::grpc::QueryTasksInfoReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  // Query tasks in RAM
  g_task_scheduler->QueryTasksInRam(request, response);

  size_t num_limit = request->num_limit() == 0 ? kDefaultQueryTaskNumLimit
                                               : request->num_limit();

  auto *task_list = response->mutable_task_info_list();

  auto sort_and_truncate = [](auto *task_list, size_t limit) -> void {
    std::sort(
        task_list->begin(), task_list->end(),
        [](const crane::grpc::TaskInfo &a, const crane::grpc::TaskInfo &b) {
          return (a.status() == b.status()) ? (a.priority() > b.priority())
                                            : (a.status() < b.status());
        });

    if (task_list->size() > limit)
      task_list->DeleteSubrange(limit, task_list->size());
  };

  if (task_list->size() >= num_limit ||
      !request->option_include_completed_tasks()) {
    sort_and_truncate(task_list, num_limit);
    response->set_ok(true);
    return grpc::Status::OK;
  }

  // Query completed tasks in Mongodb
  // (only for cacct, which sets `option_include_completed_tasks` to true)
  if (!g_db_client->FetchJobRecords(request, response,
                                    num_limit - task_list->size())) {
    CRANE_ERROR("Failed to call g_db_client->FetchJobRecords");
    return grpc::Status::OK;
  }

  sort_and_truncate(task_list, num_limit);
  response->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::AddAccount(
    grpc::ServerContext *context, const crane::grpc::AddAccountRequest *request,
    crane::grpc::AddAccountReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  Account account;
  const crane::grpc::AccountInfo *account_info = &request->account();

  account.name = account_info->name();
  account.parent_account = account_info->parent_account();
  account.description = account_info->description();
  account.default_qos = account_info->default_qos();
  for (const auto &p : account_info->allowed_partitions()) {
    account.allowed_partition.emplace_back(p);
  }
  for (const auto &qos : account_info->allowed_qos_list()) {
    account.allowed_qos_list.emplace_back(qos);
  }

  auto result = g_account_manager->AddAccount(request->uid(), account);
  if (result) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_code(result.error());
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::AddUser(
    grpc::ServerContext *context, const crane::grpc::AddUserRequest *request,
    crane::grpc::AddUserReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  User user;
  const crane::grpc::UserInfo *user_info = &request->user();

  user.name = user_info->name();
  user.uid = user_info->uid();
  user.default_account = user_info->account();
  user.admin_level = User::AdminLevel(user_info->admin_level());
  for (const auto &acc : user_info->coordinator_accounts()) {
    user.coordinator_accounts.emplace_back(acc);
  }

  // For user adding operation, the front end allows user only to set
  // 'Allowed Partition'. 'Qos Lists' of the 'Allowed Partitions' can't be
  // set by user. It's inherited from the parent account.
  // However, we use UserInfo message defined in gRPC here. The `qos_list` field
  // for any `allowed_partition_qos_list` is empty as just mentioned. Only
  // `partition_name` field is set.
  // Moreover, if `allowed_partition_qos_list` is empty, both allowed partitions
  // and qos_list for allowed partitions are inherited from the parent.
  if (!user.default_account.empty()) {
    user.account_to_attrs_map[user.default_account];
    for (const auto &apq : user_info->allowed_partition_qos_list())
      user.account_to_attrs_map[user.default_account]
          .allowed_partition_qos_map[apq.partition_name()];
  }

  CraneExpected<void> result = g_account_manager->AddUser(request->uid(), user);
  if (result) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_code(result.error());
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::AddQos(
    grpc::ServerContext *context, const crane::grpc::AddQosRequest *request,
    crane::grpc::AddQosReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};
  Qos qos;
  const crane::grpc::QosInfo *qos_info = &request->qos();

  qos.name = qos_info->name();
  qos.description = qos_info->description();
  qos.priority =
      qos_info->priority() == 0 ? kDefaultQosPriority : qos_info->priority();
  qos.max_jobs_per_user = qos_info->max_jobs_per_user();
  qos.max_cpus_per_user = qos_info->max_cpus_per_user();

  int64_t sec = qos_info->max_time_limit_per_task();
  if (!CheckIfTimeLimitSecIsValid(sec)) {
    response->set_ok(false);
    response->set_code(CraneErrCode::ERR_TIME_LIMIT);
    return grpc::Status::OK;
  }
  qos.max_time_limit_per_task = absl::Seconds(sec);

  auto result = g_account_manager->AddQos(request->uid(), qos);
  if (result) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_code(result.error());
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::ModifyAccount(
    grpc::ServerContext *context,
    const crane::grpc::ModifyAccountRequest *request,
    crane::grpc::ModifyAccountReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  if (request->type() == crane::grpc::OperationType::Overwrite &&
      request->modify_field() ==
          crane::grpc::ModifyField::Partition) {  // SetAccountAllowedPartition
    std::unordered_set<std::string> partition_list{
        request->value_list().begin(), request->value_list().end()};

    auto rich_res = g_account_manager->SetAccountAllowedPartition(
        request->uid(), request->name(), std::move(partition_list),
        request->force());
    if (!rich_res) {
      if (rich_res.error().description().empty())
        rich_res.error().set_description(
            absl::StrJoin(request->value_list(), ","));

      response->mutable_rich_error_list()->Add()->CopyFrom(rich_res.error());
    }

  } else if (request->type() == crane::grpc::OperationType::Overwrite &&
             request->modify_field() ==
                 crane::grpc::ModifyField::Qos) {  // SetAccountAllowedQos
    std::unordered_set<std::string> qos_list{request->value_list().begin(),
                                             request->value_list().end()};
    std::string default_qos = "";
    if (!request->value_list().empty()) default_qos = request->value_list()[0];
    auto rich_res = g_account_manager->SetAccountAllowedQos(
        request->uid(), request->name(), default_qos, std::move(qos_list),
        request->force());
    if (!rich_res) {
      if (rich_res.error().description().empty())
        rich_res.error().set_description(
            absl::StrJoin(request->value_list(), ","));

      response->mutable_rich_error_list()->Add()->CopyFrom(rich_res.error());
    }
  } else {  // other operations
    for (const auto &value : request->value_list()) {
      auto modify_res = g_account_manager->ModifyAccount(
          request->type(), request->uid(), request->name(),
          request->modify_field(), value, request->force());
      if (!modify_res) {
        auto *new_err_record = response->mutable_rich_error_list()->Add();
        new_err_record->set_description(value);
        new_err_record->set_code(modify_res.error());
      }
    }
  }

  if (response->rich_error_list().empty()) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::ModifyUser(
    grpc::ServerContext *context, const crane::grpc::ModifyUserRequest *request,
    crane::grpc::ModifyUserReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  CraneExpected<void> modify_res;

  if (request->type() == crane::grpc::OperationType::Delete) {
    switch (request->modify_field()) {
    case crane::grpc::ModifyField::Partition:
      for (const auto &value : request->value_list()) {
        modify_res = g_account_manager->DeleteUserAllowedPartition(
            request->uid(), request->name(), request->account(), value);
        if (!modify_res) {
          auto *new_err_record = response->mutable_rich_error_list()->Add();
          new_err_record->set_description(value);
          new_err_record->set_code(modify_res.error());
        }
      }

      break;
    case crane::grpc::ModifyField::Qos:
      for (const auto &value : request->value_list()) {
        modify_res = g_account_manager->DeleteUserAllowedQos(
            request->uid(), request->name(), request->partition(),
            request->account(), value, request->force());
        if (!modify_res) {
          auto *new_err_record = response->mutable_rich_error_list()->Add();
          new_err_record->set_description(value);
          new_err_record->set_code(modify_res.error());
        }
      }
      break;
    default:
      std::unreachable();
    }
  } else {
    switch (request->modify_field()) {
    case crane::grpc::ModifyField::AdminLevel:
      modify_res = g_account_manager->ModifyAdminLevel(
          request->uid(), request->name(), request->value_list()[0]);
      if (!modify_res) {
        auto *new_err_record = response->mutable_rich_error_list()->Add();
        new_err_record->set_description(request->value_list()[0]);
        new_err_record->set_code(modify_res.error());
      }
      break;
    case crane::grpc::ModifyField::Partition:
      if (request->type() == crane::grpc::OperationType::Add) {
        for (const auto &partition_name : request->value_list()) {
          modify_res = g_account_manager->AddUserAllowedPartition(
              request->uid(), request->name(), request->account(),
              partition_name);
          if (!modify_res) {
            auto *new_err_record = response->mutable_rich_error_list()->Add();
            new_err_record->set_description(partition_name);
            new_err_record->set_code(modify_res.error());
          }
        }
      } else if (request->type() == crane::grpc::OperationType::Overwrite) {
        std::unordered_set<std::string> partition_list{
            request->value_list().begin(), request->value_list().end()};
        auto rich_res = g_account_manager->SetUserAllowedPartition(
            request->uid(), request->name(), request->account(),
            partition_list);
        if (!rich_res) {
          if (rich_res.error().description().empty())
            rich_res.error().set_description(
                absl::StrJoin(request->value_list(), ","));

          response->mutable_rich_error_list()->Add()->CopyFrom(
              rich_res.error());
        }
      }
      break;
    case crane::grpc::ModifyField::Qos:
      if (request->type() == crane::grpc::OperationType::Add) {
        for (const auto &qos_name : request->value_list()) {
          modify_res = g_account_manager->AddUserAllowedQos(
              request->uid(), request->name(), request->partition(),
              request->account(), qos_name);
          if (!modify_res) {
            auto *new_err_record = response->mutable_rich_error_list()->Add();
            new_err_record->set_description(qos_name);
            new_err_record->set_code(modify_res.error());
          }
        }
      } else if (request->type() == crane::grpc::OperationType::Overwrite) {
        std::unordered_set<std::string> qos_list{request->value_list().begin(),
                                                 request->value_list().end()};
        std::string default_qos = "";
        if (!request->value_list().empty())
          default_qos = request->value_list()[0];
        auto rich_res = g_account_manager->SetUserAllowedQos(
            request->uid(), request->name(), request->partition(),
            request->account(), default_qos, std::move(qos_list),
            request->force());
        if (!rich_res) {
          if (rich_res.error().description().empty())
            rich_res.error().set_description(
                absl::StrJoin(request->value_list(), ","));
          response->mutable_rich_error_list()->Add()->CopyFrom(
              rich_res.error());
        }
      }
      break;
    case crane::grpc::ModifyField::DefaultQos:
      modify_res = g_account_manager->ModifyUserDefaultQos(
          request->uid(), request->name(), request->partition(),
          request->account(), request->value_list()[0]);
      if (!modify_res) {
        auto *new_err_record = response->mutable_rich_error_list()->Add();
        new_err_record->set_description(request->value_list()[0]);
        new_err_record->set_code(modify_res.error());
      }
      break;
    case crane::grpc::ModifyField::DefaultAccount:
      modify_res = g_account_manager->ModifyUserDefaultAccount(
          request->uid(), request->name(), request->value_list()[0]);
      if (!modify_res) {
        auto *new_err_record = response->mutable_rich_error_list()->Add();
        new_err_record->set_description(request->value_list()[0]);
        new_err_record->set_code(modify_res.error());
      }
      break;
    default:
      std::unreachable();
    }
  }

  if (response->rich_error_list().empty()) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
  }
  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::ModifyQos(
    grpc::ServerContext *context, const crane::grpc::ModifyQosRequest *request,
    crane::grpc::ModifyQosReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};
  auto modify_res =
      g_account_manager->ModifyQos(request->uid(), request->name(),
                                   request->modify_field(), request->value());

  if (modify_res) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_code(modify_res.error());
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::QueryAccountInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryAccountInfoRequest *request,
    crane::grpc::QueryAccountInfoReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};
  std::vector<Account> res_account_list;
  if (request->account_list().empty()) {
    auto res = g_account_manager->QueryAllAccountInfo(request->uid());
    if (!res) {
      auto *new_err_record = response->mutable_rich_error_list()->Add();
      new_err_record->set_code(res.error());
      new_err_record->set_description("");
    } else {
      res_account_list = std::move(res.value());
    }
  } else {
    std::unordered_set<std::string> account_list{
        request->account_list().begin(), request->account_list().end()};
    for (const auto &account : account_list) {
      auto res = g_account_manager->QueryAccountInfo(request->uid(), account);
      if (!res) {
        auto *new_err_record = response->mutable_rich_error_list()->Add();
        new_err_record->set_description(account);
        new_err_record->set_code(res.error());
      } else {
        res_account_list.emplace_back(std::move(res.value()));
      }
    }
  }

  if (response->rich_error_list().empty()) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
  }

  for (const auto &account : res_account_list) {
    // put the account info into grpc element
    auto *account_info = response->mutable_account_list()->Add();
    account_info->set_name(account.name);
    account_info->set_description(account.description);

    auto *user_list = account_info->mutable_users();
    for (auto &&user : account.users) {
      user_list->Add()->assign(user);
    }

    auto *child_list = account_info->mutable_child_accounts();
    for (auto &&child : account.child_accounts) {
      child_list->Add()->assign(child);
    }
    account_info->set_parent_account(account.parent_account);

    auto *partition_list = account_info->mutable_allowed_partitions();
    for (auto &&partition : account.allowed_partition) {
      partition_list->Add()->assign(partition);
    }
    account_info->set_default_qos(account.default_qos);
    account_info->set_blocked(account.blocked);

    auto *allowed_qos_list = account_info->mutable_allowed_qos_list();
    for (const auto &qos : account.allowed_qos_list) {
      allowed_qos_list->Add()->assign(qos);
    }

    auto *coordinators = account_info->mutable_coordinators();
    for (auto &&coord : account.coordinators) {
      coordinators->Add()->assign(coord);
    }
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::QueryUserInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryUserInfoRequest *request,
    crane::grpc::QueryUserInfoReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  std::unordered_set<std::string> user_list{request->user_list().begin(),
                                            request->user_list().end()};

  std::vector<User> res_user_list;
  if (user_list.empty()) {
    auto res = g_account_manager->QueryAllUserInfo(request->uid());
    if (!res) {
      auto *new_err_record = response->mutable_rich_error_list()->Add();
      new_err_record->set_code(res.error());
      new_err_record->set_description("");
    } else {
      res_user_list = std::move(res.value());
    }
  } else {
    for (const auto &username : user_list) {
      auto res = g_account_manager->QueryUserInfo(request->uid(), username);
      if (!res) {
        auto *new_err_record = response->mutable_rich_error_list()->Add();
        new_err_record->set_description(username);
        new_err_record->set_code(res.error());
      } else {
        res_user_list.emplace_back(std::move(res.value()));
      }
    }
  }

  if (response->rich_error_list().empty()) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
  }

  for (const auto &user : res_user_list) {
    for (const auto &[account, item] : user.account_to_attrs_map) {
      if (!request->account().empty() && account != request->account()) {
        continue;
      }
      auto *user_info = response->mutable_user_list()->Add();
      user_info->set_name(user.name);
      user_info->set_uid(user.uid);
      if (account == user.default_account) {
        user_info->set_account(account + '*');
      } else {
        user_info->set_account(account);
      }
      user_info->set_admin_level(
          static_cast<crane::grpc::UserInfo_AdminLevel>(user.admin_level));
      user_info->set_blocked(item.blocked);

      auto *partition_qos_list =
          user_info->mutable_allowed_partition_qos_list();
      for (const auto &[par_name, pair] : item.allowed_partition_qos_map) {
        auto *partition_qos = partition_qos_list->Add();
        partition_qos->set_partition_name(par_name);
        partition_qos->set_default_qos(pair.first);

        auto *qos_list = partition_qos->mutable_qos_list();
        for (const auto &qos : pair.second) {
          qos_list->Add()->assign(qos);
        }
      }

      auto *coordinated_accounts = user_info->mutable_coordinator_accounts();
      for (auto &&coord : user.coordinator_accounts) {
        coordinated_accounts->Add()->assign(coord);
      }
    }
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::QueryQosInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryQosInfoRequest *request,
    crane::grpc::QueryQosInfoReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  std::vector<Qos> res_qos_list;

  if (request->qos_list().empty()) {
    auto res = g_account_manager->QueryAllQosInfo(request->uid());
    if (!res) {
      auto *new_err_record = response->mutable_rich_error_list()->Add();
      new_err_record->set_code(res.error());
      new_err_record->set_description("");
    } else {
      res_qos_list = std::move(res.value());
    }
  } else {
    std::unordered_set<std::string> qos_list{request->qos_list().begin(),
                                             request->qos_list().end()};
    for (const auto &qos : qos_list) {
      auto res = g_account_manager->QueryQosInfo(request->uid(), qos);
      if (!res) {
        auto *new_err_record = response->mutable_rich_error_list()->Add();
        new_err_record->set_description(qos);
        new_err_record->set_code(res.error());
      } else {
        res_qos_list.emplace_back(std::move(res.value()));
      }
    }
  }

  if (response->rich_error_list().empty()) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
  }

  auto *list = response->mutable_qos_list();
  for (const auto &qos : res_qos_list) {
    auto *qos_info = list->Add();
    qos_info->set_name(qos.name);
    qos_info->set_description(qos.description);
    qos_info->set_priority(qos.priority);
    qos_info->set_max_jobs_per_user(qos.max_jobs_per_user);
    qos_info->set_max_cpus_per_user(qos.max_cpus_per_user);
    qos_info->set_max_time_limit_per_task(
        absl::ToInt64Seconds(qos.max_time_limit_per_task));
  }

  return grpc::Status::OK;
}
grpc::Status CraneCtldServiceImpl::DeleteAccount(
    grpc::ServerContext *context,
    const crane::grpc::DeleteAccountRequest *request,
    crane::grpc::DeleteAccountReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  for (const auto &account_name : request->account_list()) {
    auto res = g_account_manager->DeleteAccount(request->uid(), account_name);
    if (!res) {
      auto *new_err_record = response->mutable_rich_error_list()->Add();
      new_err_record->set_description(account_name);
      new_err_record->set_code(res.error());
    }
  }

  if (response->rich_error_list().empty()) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
  }
  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::DeleteUser(
    grpc::ServerContext *context, const crane::grpc::DeleteUserRequest *request,
    crane::grpc::DeleteUserReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  std::unordered_set<std::string> user_list;
  bool contains_all = std::ranges::find(request->user_list(), "ALL") !=
                      request->user_list().end();
  if (contains_all && request->force()) {
    auto account_ptr =
        g_account_manager->GetExistedAccountInfo(request->account());
    if (!account_ptr) {
      response->set_ok(false);
      auto *new_err_record = response->mutable_rich_error_list()->Add();
      new_err_record->set_description(request->account());
      new_err_record->set_code(CraneErrCode::ERR_INVALID_ACCOUNT);
      return grpc::Status::OK;
    }

    user_list.insert(account_ptr->users.begin(), account_ptr->users.end());
  } else if (contains_all && !request->force()) {
    response->set_ok(false);
    auto *new_err_record = response->mutable_rich_error_list()->Add();
    new_err_record->set_description("all");
    new_err_record->set_code(CraneErrCode::ERR_NOT_FORCE);
    return grpc::Status::OK;
  } else if (!contains_all) {
    user_list.insert(request->user_list().begin(), request->user_list().end());
  }

  for (const auto &user_name : user_list) {
    auto res = g_account_manager->DeleteUser(request->uid(), user_name,
                                             request->account());
    if (!res) {
      auto *new_err_record = response->mutable_rich_error_list()->Add();
      new_err_record->set_description(user_name);
      new_err_record->set_code(res.error());
    }
  }

  if (response->rich_error_list().empty()) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::DeleteQos(
    grpc::ServerContext *context, const crane::grpc::DeleteQosRequest *request,
    crane::grpc::DeleteQosReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  for (const auto &qos_name : request->qos_list()) {
    auto res = g_account_manager->DeleteQos(request->uid(), qos_name);
    if (!res) {
      auto *new_err_record = response->mutable_rich_error_list()->Add();
      new_err_record->set_description(qos_name);
      new_err_record->set_code(res.error());
    }
  }

  if (response->rich_error_list().empty()) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::BlockAccountOrUser(
    grpc::ServerContext *context,
    const crane::grpc::BlockAccountOrUserRequest *request,
    crane::grpc::BlockAccountOrUserReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready");
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};
  CraneExpected<void> res;
  std::unordered_set<std::string> entity_list{request->entity_list().begin(),
                                              request->entity_list().end()};

  switch (request->entity_type()) {
  case crane::grpc::Account:
    if (request->entity_list().empty()) {
      const auto account_map_ptr = g_account_manager->GetAllAccountInfo();
      for (const auto &account_name : *account_map_ptr | std::views::keys) {
        if (account_name == "ROOT") continue;
        entity_list.insert(account_name);
      }
    }

    for (const auto &account_name : entity_list) {
      res = g_account_manager->BlockAccount(request->uid(), account_name,
                                            request->block());
      if (!res) {
        auto *new_err_record = response->mutable_rich_error_list()->Add();
        if (request->entity_list().empty()) {
          if (res.error() == CraneErrCode::ERR_INVALID_OP_USER ||
              res.error() == CraneErrCode::ERR_INVALID_UID) {
            new_err_record->set_description("");
            new_err_record->set_code(res.error());
            break;
          }
        }
        new_err_record->set_description(account_name);
        new_err_record->set_code(res.error());
      }
    }
    break;
  case crane::grpc::User:
    if (request->entity_list().empty()) {
      auto account_ptr =
          g_account_manager->GetExistedAccountInfo(request->account());
      if (!account_ptr) {
        response->set_ok(false);
        auto *new_err_record = response->mutable_rich_error_list()->Add();
        new_err_record->set_description(request->account());
        new_err_record->set_code(CraneErrCode::ERR_INVALID_ACCOUNT);
        return grpc::Status::OK;
      }

      entity_list.insert(account_ptr->users.begin(), account_ptr->users.end());
    }

    for (const auto &user_name : entity_list) {
      res = g_account_manager->BlockUser(request->uid(), user_name,
                                         request->account(), request->block());
      if (!res) {
        auto *new_err_record = response->mutable_rich_error_list()->Add();
        if (request->entity_list().empty()) {
          if (res.error() == CraneErrCode::ERR_INVALID_OP_USER ||
              res.error() == CraneErrCode::ERR_INVALID_UID) {
            new_err_record->set_description("");
            new_err_record->set_code(res.error());
            break;
          }
        }
        new_err_record->set_description(user_name);
        new_err_record->set_code(res.error());
      }
    }
    break;
  default:
    std::unreachable();
  }

  if (response->rich_error_list().empty()) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::ResetUserCredential(
    grpc::ServerContext *context,
    const crane::grpc::ResetUserCredentialRequest *request,
    crane::grpc::ResetUserCredentialReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  std::unordered_set<std::string> user_list{request->user_list().begin(),
                                            request->user_list().end()};

  if (request->user_list().empty()) {
    const auto user_map_ptr = g_account_manager->GetAllUserInfo();
    for (const auto &username : *user_map_ptr | std::views::keys) {
      user_list.insert(username);
    }
  }

  for (const auto &username : user_list) {
    auto result =
        g_account_manager->ResetUserCertificate(request->uid(), username);
    if (!result) {
      auto *new_err_record = response->mutable_rich_error_list()->Add();
      new_err_record->set_description(username);
      new_err_record->set_code(result.error());
    }
  }

  if (!response->rich_error_list().empty()) {
    response->set_ok(false);
  } else {
    response->set_ok(true);
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::QueryTxnLog(
    grpc::ServerContext *context,
    const crane::grpc::QueryTxnLogRequest *request,
    crane::grpc::QueryTxnLogReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  std::unordered_map<std::string, std::string> conditions;
  if (!request->actor().empty()) conditions.emplace("actor", request->actor());
  if (!request->target().empty())
    conditions.emplace("target", request->target());
  if (!request->action().empty())
    conditions.emplace("action", request->action());
  if (!request->info().empty()) conditions.emplace("info", request->info());

  auto result = g_account_manager->QueryTxnList(
      request->uid(), conditions,
      request->time_interval().lower_bound().seconds(),
      request->time_interval().upper_bound().seconds());
  if (!result) {
    response->set_ok(false);
    response->set_code(result.error());
  } else {
    response->set_ok(true);
    for (auto &txn : result.value()) {
      auto *new_txn = response->add_txn_log_list();
      new_txn->set_actor(txn.actor);
      new_txn->set_target(txn.target);
      new_txn->set_action(txn.action);
      new_txn->set_creation_time(txn.creation_time);
      new_txn->set_info(txn.info);
    }
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::QueryClusterInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryClusterInfoRequest *request,
    crane::grpc::QueryClusterInfoReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  g_db_client->ClusterRollupUsage();
  *response = g_meta_container->QueryClusterInfo(*request);
  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::CreateReservation(
    grpc::ServerContext *context,
    const crane::grpc::CreateReservationRequest *request,
    crane::grpc::CreateReservationReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  auto res = g_account_manager->CheckUidIsAdmin(request->uid());
  if (!res) {
    response->set_ok(false);
    response->set_reason(CraneErrStr(res.error()));
    return grpc::Status::OK;
  }

  *response = g_task_scheduler->CreateResv(*request);
  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::DeleteReservation(
    grpc::ServerContext *context,
    const crane::grpc::DeleteReservationRequest *request,
    crane::grpc::DeleteReservationReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  auto res = g_account_manager->CheckUidIsAdmin(request->uid());
  if (!res) {
    response->set_ok(false);
    response->set_reason(CraneErrStr(res.error()));
    return grpc::Status::OK;
  }

  *response = g_task_scheduler->DeleteResv(*request);
  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::PowerStateChange(
    grpc::ServerContext *context,
    const crane::grpc::PowerStateChangeRequest *request,
    crane::grpc::PowerStateChangeReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};

  CRANE_INFO("Received power state change request for node {}: {}",
             request->craned_id(),
             crane::grpc::CranedPowerState_Name(request->state()));

  auto craned_meta = g_meta_container->GetCranedMetaPtr(request->craned_id());
  if (!craned_meta) {
    response->set_ok(false);
    return grpc::Status::OK;
  }
  craned_meta->power_state = request->state();

  if (g_config.Plugin.Enabled && g_plugin_client != nullptr) {
    std::vector<crane::grpc::plugin::CranedEventInfo> event_list;
    crane::grpc::plugin::CranedEventInfo event;

    absl::Time now = absl::Now();
    int64_t seconds = absl::ToUnixSeconds(now);
    int32_t nanos = static_cast<int32_t>(absl::ToUnixNanos(now) % 1000000000);

    auto timestamp = std::make_unique<::google::protobuf::Timestamp>();
    timestamp->set_seconds(seconds);
    timestamp->set_nanos(nanos);

    event.set_cluster_name(g_config.CraneClusterName);
    event.set_node_name(request->craned_id());
    event.set_reason(request->reason());
    event.set_allocated_start_time(timestamp.release());

    event.set_power_state(request->state());

    event_list.emplace_back(event);

    g_plugin_client->NodeEventHookAsync(std::move(event_list));
  }

  response->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::EnableAutoPowerControl(
    grpc::ServerContext *context,
    const crane::grpc::EnableAutoPowerControlRequest *request,
    crane::grpc::EnableAutoPowerControlReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  CRANE_INFO(
      "Received enable auto power control request for {} nodes, enable: {}",
      request->craned_ids_size(), request->enable());

  auto res = g_account_manager->CheckUidIsAdmin(request->uid());
  if (!res) {
    for (const auto &craned_id : request->craned_ids()) {
      response->add_not_modified_nodes(craned_id);
      if (res.error() == CraneErrCode::ERR_INVALID_USER) {
        response->add_not_modified_reasons("User is not a user of Crane");
      } else if (res.error() == CraneErrCode::ERR_USER_NO_PRIVILEGE) {
        response->add_not_modified_reasons("User has insufficient privilege");
      }
    }
    return grpc::Status::OK;
  }

  if (!g_config.Plugin.Enabled || g_plugin_client == nullptr) {
    for (const auto &craned_id : request->craned_ids()) {
      response->add_not_modified_nodes(craned_id);
      response->add_not_modified_reasons("Plugin is not enabled");
    }
    return grpc::Status::OK;
  }

  auto craned_meta_map = g_meta_container->GetCranedMetaMapConstPtr();

  std::vector<std::string> valid_nodes;
  valid_nodes.reserve(request->craned_ids_size());

  for (const auto &craned_id : request->craned_ids()) {
    auto it = craned_meta_map->find(craned_id);
    if (it == craned_meta_map->end()) {
      response->add_not_modified_nodes(craned_id);
      response->add_not_modified_reasons("Node not found");
    } else {
      valid_nodes.emplace_back(craned_id);
    }
  }

  for (const auto &craned_id : valid_nodes) {
    CRANE_INFO("Modifying auto power control status for node {}: enable={}",
               craned_id, request->enable());

    // Use CRANE_NONE as a placeholder to leave the control state unchanged
    // while toggling auto-power control
    g_plugin_client->UpdatePowerStateHookAsync(
        craned_id, crane::grpc::CranedControlState::CRANE_NONE,
        request->enable());

    response->add_modified_nodes(craned_id);
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::SignUserCertificate(
    grpc::ServerContext *context,
    const crane::grpc::SignUserCertificateRequest *request,
    crane::grpc::SignUserCertificateResponse *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};
  if (!g_config.ListenConf.TlsConfig.AllowedNodes.empty()) {
    std::string client_address = context->peer();
    std::vector<std::string> str_list = absl::StrSplit(client_address, ":");
    if (str_list.size() < 2) {
      CRANE_ERROR("Invalid client address format: {}", client_address);
      response->set_ok(false);
      response->set_reason(crane::grpc::ErrCode::ERR_INVALID_PARAM);
      return grpc::Status::OK;
    }

    std::string hostname;
    bool resolve_result = false;
    if (str_list[0] == "ipv4") {
      ipv4_t addr;
      if (!crane::StrToIpv4(str_list[1], &addr)) {
        CRANE_ERROR("Failed to parse ipv4 address: {}", str_list[1]);
        response->set_ok(false);
        response->set_reason(crane::grpc::ErrCode::ERR_INVALID_PARAM);
        return grpc::Status::OK;
      }
      resolve_result = crane::ResolveHostnameFromIpv4(addr, &hostname);
    } else {
      ipv6_t addr;
      if (!crane::StrToIpv6(str_list[1], &addr)) {
        CRANE_ERROR("Failed to parse ipv6 address: {}", str_list[1]);
        response->set_ok(false);
        response->set_reason(crane::grpc::ErrCode::ERR_INVALID_PARAM);
        return grpc::Status::OK;
      }
      resolve_result = crane::ResolveHostnameFromIpv6(addr, &hostname);
    }

    if (!resolve_result) {
      CRANE_ERROR("Failed to resolve hostname for address: {}", client_address);
      response->set_ok(false);
      response->set_reason(crane::grpc::ErrCode::ERR_INVALID_PARAM);
      return grpc::Status::OK;
    }

    if (!g_config.ListenConf.TlsConfig.AllowedNodes.contains(hostname)) {
      CRANE_DEBUG(
          "User {} tried to access from host {}, the host is not allowed.",
          request->uid(), hostname);
      response->set_ok(false);
      response->set_reason(crane::grpc::ErrCode::ERR_PERMISSION_USER);
      return grpc::Status::OK;
    }
  }

  auto result = g_account_manager->SignUserCertificate(
      request->uid(), request->csr_content(), request->alt_names());
  if (!result) {
    response->set_ok(false);
    response->set_reason(result.error());
  } else {
    response->set_ok(true);
    response->set_certificate(result.value());
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::AttachInContainerTask(
    grpc::ServerContext *context,
    const crane::grpc::AttachInContainerTaskRequest *request,
    crane::grpc::AttachInContainerTaskReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};

  // Validate request
  if (request->task_id() <= 0) {
    auto *err = response->mutable_status();
    err->set_code(CraneErrCode::ERR_INVALID_PARAM);
    err->set_description("Invalid task ID");
    response->set_ok(false);
    return grpc::Status::OK;
  }

  if (request->tty() && request->stderr()) {
    auto *err = response->mutable_status();
    err->set_code(CraneErrCode::ERR_INVALID_PARAM);
    err->set_description("Cannot attach both tty and stderr");
    response->set_ok(false);
    return grpc::Status::OK;
  }

  if (!(request->stdout() || request->stderr() || request->stdin())) {
    auto *err = response->mutable_status();
    err->set_code(CraneErrCode::ERR_INVALID_PARAM);
    err->set_description(
        "At least one of stdout, stderr, or stdin must be attached");
    response->set_ok(false);
    return grpc::Status::OK;
  }

  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  *response = g_task_scheduler->AttachInContainerTask(*request);

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::ExecInContainerTask(
    grpc::ServerContext *context,
    const crane::grpc::ExecInContainerTaskRequest *request,
    crane::grpc::ExecInContainerTaskReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};

  // Validate request
  if (request->task_id() <= 0) {
    auto *err = response->mutable_status();
    err->set_code(CraneErrCode::ERR_INVALID_PARAM);
    err->set_description("Invalid task ID");
    response->set_ok(false);
    return grpc::Status::OK;
  }

  if (request->command_size() == 0) {
    auto *err = response->mutable_status();
    err->set_code(CraneErrCode::ERR_INVALID_PARAM);
    err->set_description("Command cannot be empty");
    response->set_ok(false);
    return grpc::Status::OK;
  }

  if (request->tty() && request->stderr()) {
    auto *err = response->mutable_status();
    err->set_code(CraneErrCode::ERR_INVALID_PARAM);
    err->set_description("Cannot exec with both tty and stderr");
    response->set_ok(false);
    return grpc::Status::OK;
  }

  if (!(request->stdout() || request->stderr() || request->stdin())) {
    auto *err = response->mutable_status();
    err->set_code(CraneErrCode::ERR_INVALID_PARAM);
    err->set_description(
        "At least one of stdout, stderr, or stdin must be enabled");
    response->set_ok(false);
    return grpc::Status::OK;
  }

  if (auto msg = CheckCertAndUIDAllowed_(context, request->uid()); msg)
    return {grpc::StatusCode::UNAUTHENTICATED, msg.value()};

  *response = g_task_scheduler->ExecInContainerTask(*request);

  return grpc::Status::OK;
}

std::optional<std::string> CraneCtldServiceImpl::CheckCertAndUIDAllowed_(
    const grpc::ServerContext *context, uint32_t uid) {
  if (!g_config.ListenConf.TlsConfig.Enabled) return std::nullopt;

  auto cert = context->auth_context()->FindPropertyValues("x509_pem_cert");
  if (cert.empty()) return "Certificate is empty";

  std::string certificate = std::string(cert[0].data(), cert[0].size());

  auto result = util::ParseCertificate(certificate);
  if (!result) return result.error();

  if (!g_vault_client->IsCertAllowed(result.value().second))
    return "Certificate has expired";

  std::vector<std::string> cn_parts = absl::StrSplit(result.value().first, '.');
  if (cn_parts.empty() || cn_parts[0].empty()) return "Certificate is invalid";

  try {
    auto parsed_uid = static_cast<uint32_t>(std::stoul(cn_parts[0]));
    if (parsed_uid != uid) return "Uid mismatch";
  } catch (const std::invalid_argument &) {
    return "Certificate contains an invalid UID";
  } catch (const std::out_of_range &) {
    return "Certificate UID is out of range";
  } catch (const std::exception &) {
    return "Parse uid unknown error";
  }

  return std::nullopt;
}

grpc::Status CraneCtldServiceImpl::QueryAccountUserSummaryItem(
    grpc::ServerContext *context,
    const crane::grpc::QueryAccountUserSummaryItemRequest *request,
    crane::grpc::QueryAccountUserSummaryItemReply *response) {
  std::string user_name = request->username();
  std::string account = request->account();
  auto start_time = request->start_time().seconds();
  auto end_time = request->end_time().seconds();
  response->set_cluster(g_config.CraneClusterName);
  g_db_client->QueryAccountUserSummary(account, user_name, start_time, end_time,
                                       response);
  return grpc::Status::OK;
}

CtldServer::CtldServer(const Config::CraneCtldListenConf &listen_conf) {
  std::string cranectld_listen_addr = listen_conf.CraneCtldListenAddr;

  // internal
  m_internal_service_impl_ = std::make_unique<CtldForInternalServiceImpl>(this);
  grpc::ServerBuilder internal_builder;
  ServerBuilderSetKeepAliveArgs(&internal_builder);

  if (g_config.CompressedRpc) ServerBuilderSetCompression(&internal_builder);

  if (listen_conf.TlsConfig.Enabled)
    ServerBuilderAddTcpTlsListeningPort(
        &internal_builder, cranectld_listen_addr,
        listen_conf.CraneCtldForInternalListenPort,
        listen_conf.TlsConfig.InternalCerts, listen_conf.TlsConfig.CaContent);
  else
    ServerBuilderAddTcpInsecureListeningPort(
        &internal_builder, cranectld_listen_addr,
        listen_conf.CraneCtldForInternalListenPort);

  internal_builder.RegisterService(m_internal_service_impl_.get());

  m_internal_server_ = internal_builder.BuildAndStart();
  if (!m_internal_server_) {
    CRANE_ERROR("Cannot start internal gRPC server!");
    std::exit(1);
  }

  // external
  m_service_impl_ = std::make_unique<CraneCtldServiceImpl>(this);
  grpc::ServerBuilder builder;
  ServerBuilderSetKeepAliveArgs(&builder);

  if (g_config.CompressedRpc) ServerBuilderSetCompression(&builder);

  if (listen_conf.TlsConfig.Enabled) {
    ServerBuilderAddTcpTlsListeningPort(
        &builder, cranectld_listen_addr, listen_conf.CraneCtldListenPort,
        listen_conf.TlsConfig.ExternalCerts, listen_conf.TlsConfig.CaContent);
  } else {
    ServerBuilderAddTcpInsecureListeningPort(&builder, cranectld_listen_addr,
                                             listen_conf.CraneCtldListenPort);
  }

  builder.RegisterService(m_service_impl_.get());

  m_server_ = builder.BuildAndStart();
  if (!m_server_) {
    CRANE_ERROR("Cannot start gRPC server!");
    std::exit(1);
  }

  CRANE_INFO("CraneCtld is listening on {}:{} and {}:{} and Tls is {}",
             cranectld_listen_addr, listen_conf.CraneCtldListenPort,
             cranectld_listen_addr, listen_conf.CraneCtldForInternalListenPort,
             listen_conf.TlsConfig.Enabled);

  // Avoid the potential deadlock error in underlying absl::mutex
  std::thread signal_waiting_thread(
      [p_server = m_server_.get(),
       p_internal_server = m_internal_server_.get()] {
        util::SetCurrentThreadName("SIG_Waiter");

        std::unique_lock<std::mutex> lk(s_signal_cv_mtx_);
        s_signal_cv_.wait(lk);

        CRANE_TRACE(
            "SIGINT or SIGTERM captured. Calling Shutdown() on grpc server...");

        // craned_keeper MUST be shutdown before GrpcServer.
        // Otherwise, once GrpcServer is shut down, the main thread stops and
        // g_craned_keeper.reset() is called. The Shutdown here and reset() in
        // the main thread will access g_craned_keeper simultaneously and a race
        // condition will occur.
        g_craned_keeper->Shutdown();

        auto ddl = std::chrono::seconds(1);
        p_internal_server->Shutdown(std::chrono::system_clock::now() + ddl);
        p_server->Shutdown(std::chrono::system_clock::now() + ddl);
      });
  signal_waiting_thread.detach();

  signal(SIGINT, &CtldServer::signal_handler_func);
  signal(SIGTERM, &CtldServer::signal_handler_func);
}

}  // namespace Ctld
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

#include "AccountManager.h"
#include "CranedKeeper.h"
#include "CranedMetaContainer.h"
#include "TaskScheduler.h"

namespace Ctld {

grpc::Status CraneCtldServiceImpl::SubmitBatchTask(
    grpc::ServerContext *context,
    const crane::grpc::SubmitBatchTaskRequest *request,
    crane::grpc::SubmitBatchTaskReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};

  auto task = std::make_unique<TaskInCtld>();
  task->SetFieldsByTaskToCtld(request->task());

  auto result = m_ctld_server_->SubmitTaskToScheduler(std::move(task));
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

  std::vector<CraneExpected<std::future<task_id_t>>> results;

  uint32_t task_count = request->count();
  const auto &task_to_ctld = request->task();
  results.reserve(task_count);

  for (int i = 0; i < task_count; i++) {
    auto task = std::make_unique<TaskInCtld>();
    task->SetFieldsByTaskToCtld(task_to_ctld);

    auto result = m_ctld_server_->SubmitTaskToScheduler(std::move(task));
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

grpc::Status CraneCtldServiceImpl::TaskStatusChange(
    grpc::ServerContext *context,
    const crane::grpc::TaskStatusChangeRequest *request,
    crane::grpc::TaskStatusChangeReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};

  std::optional<std::string> reason;
  if (!request->reason().empty()) reason = request->reason();

  g_task_scheduler->TaskStatusChangeWithReasonAsync(
      request->task_id(), request->craned_id(), request->new_status(),
      request->exit_code(), std::move(reason));
  response->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::CranedConnectedCtld(
    grpc::ServerContext *context,
    const crane::grpc::CranedConnectedCtldNotify *request,
    google::protobuf::Empty *response) {
  const auto &craned_id = request->craned_id();
  CRANE_TRACE("Craned {} requires Ctld to connect.", craned_id);
  if (!g_meta_container->CheckCranedAllowed(request->craned_id())) {
    CRANE_WARN("Reject register request from unknown node {}",
               request->craned_id());
    return grpc::Status::OK;
  }

  if (!g_craned_keeper->IsCranedConnected(craned_id)) {
    CRANE_TRACE("Put craned {} into unavail.", craned_id);
    g_craned_keeper->PutNodeIntoUnavailSet(craned_id, request->token());
  } else {
    // Before configure, craned should be connected but not online
    if (!g_meta_container->CheckCranedOnline(craned_id)) {
      auto stub = g_craned_keeper->GetCranedStub(craned_id);
      if (stub != nullptr)
        g_thread_pool->detach_task([stub, token = request->token(), craned_id] {
          stub->SetRegToken(token);
          stub->ConfigureCraned(craned_id, token);
        });
    } else {
      CRANE_TRACE("Already online craned {} notify craned connected.",
                  craned_id);
    }
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::CranedRegister(
    grpc::ServerContext *context,
    const crane::grpc::CranedRegisterRequest *request,
    crane::grpc::CranedRegisterReply *response) {
  CRANE_TRACE("Craned {} trying to register.", request->craned_id());
  if (!g_meta_container->CheckCranedAllowed(request->craned_id())) {
    CRANE_WARN("Reject register request from unknown node {}",
               request->craned_id());
    response->set_ok(false);
    return grpc::Status::OK;
  }

  if (!g_craned_keeper->IsCranedConnected(request->craned_id())) {
    // Be careful! Ctld to craned channel disconnected during craned
    // configuration. Now we don't care about this.
    g_craned_keeper->PutNodeIntoUnavailSet(request->craned_id(),
                                           request->token());
    CRANE_DEBUG("Craned {} to be ready is not connected.",
                request->craned_id());
    response->set_ok(false);
    return grpc::Status::OK;
  }

  if (g_meta_container->CheckCranedOnline(request->craned_id())) {
    CRANE_WARN("Reject register request from already online node {}",
               request->craned_id());
    response->set_ok(false);
    return grpc::Status::OK;
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
  stub->SetReady();

  g_meta_container->CranedUp(request->craned_id(), request->remote_meta());
  response->set_ok(true);
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

grpc::Status CraneCtldServiceImpl::ModifyTask(
    grpc::ServerContext *context, const crane::grpc::ModifyTaskRequest *request,
    crane::grpc::ModifyTaskReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};

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

grpc::Status CraneCtldServiceImpl::ModifyNode(
    grpc::ServerContext *context,
    const crane::grpc::ModifyCranedStateRequest *request,
    crane::grpc::ModifyCranedStateReply *response) {
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
  if (!request->filter_task_ids().empty())
    num_limit = std::min((size_t)request->filter_task_ids_size(), num_limit);

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
          (crane::grpc::UserInfo_AdminLevel)user.admin_level);
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
  for (const auto &user_name : request->user_list()) {
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

grpc::Status CraneCtldServiceImpl::QueryClusterInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryClusterInfoRequest *request,
    crane::grpc::QueryClusterInfoReply *response) {
  if (!g_runtime_status.srv_ready.load(std::memory_order_acquire))
    return grpc::Status{grpc::StatusCode::UNAVAILABLE,
                        "CraneCtld Server is not ready"};

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

  auto res = g_account_manager->CheckUidIsAdmin(request->uid());
  if (!res) {
    response->set_ok(false);
    response->set_reason(CraneErrStr(res.error()));
    return grpc::Status::OK;
  }

  *response = g_task_scheduler->DeleteResv(*request);
  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::CforedStream(
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
                                std::list<std::string> const &craned_ids) {
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
            if (auto writer = writer_weak_ptr.lock();
                writer && send_completion_ack) {
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
              m_ctld_server_->SubmitTaskToScheduler(std::move(task));
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
          CRANE_TRACE("Recv TaskCompletionReq of Task #{}", payload.task_id());
          if (g_task_scheduler->TerminatePendingOrRunningIaTask(
                  payload.task_id()) != CraneErrCode::SUCCESS)
            stream_writer->WriteTaskCompletionAckReply(payload.task_id());
          else {
            CRANE_TRACE(
                "Termination of task #{} succeeded. "
                "Leave TaskCompletionAck to TaskStatusChange.",
                payload.task_id());
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

CtldServer::CtldServer(const Config::CraneCtldListenConf &listen_conf) {
  m_service_impl_ = std::make_unique<CraneCtldServiceImpl>(this);

  grpc::ServerBuilder builder;
  ServerBuilderSetKeepAliveArgs(&builder);

  if (g_config.CompressedRpc) ServerBuilderSetCompression(&builder);

  std::string cranectld_listen_addr = listen_conf.CraneCtldListenAddr;
  if (listen_conf.UseTls) {
    ServerBuilderAddTcpTlsListeningPort(&builder, cranectld_listen_addr,
                                        listen_conf.CraneCtldListenPort,
                                        listen_conf.Certs);
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

  CRANE_INFO("CraneCtld is listening on {}:{} and Tls is {}",
             cranectld_listen_addr, listen_conf.CraneCtldListenPort,
             listen_conf.UseTls);

  // Avoid the potential deadlock error in underlying absl::mutex
  std::thread signal_waiting_thread([p_server = m_server_.get()] {
    util::SetCurrentThreadName("SIG_Waiter");

    std::unique_lock<std::mutex> lk(s_exit_mtx);
    s_sigint_cv.wait(lk);

    CRANE_TRACE(
        "SIGINT or SIGTERM captured. Calling Shutdown() on grpc server...");

    // craned_keeper MUST be shutdown before GrpcServer.
    // Otherwise, once GrpcServer is shut down, the main thread stops and
    // g_craned_keeper.reset() is called. The Shutdown here and reset() in the
    // main thread will access g_craned_keeper simultaneously and a race
    // condition will occur.
    g_craned_keeper->Shutdown();

    p_server->Shutdown(std::chrono::system_clock::now() +
                       std::chrono::seconds(1));
  });
  signal_waiting_thread.detach();

  signal(SIGINT, &CtldServer::signal_handler_func);
  signal(SIGTERM, &CtldServer::signal_handler_func);
}

CraneExpected<std::future<task_id_t>> CtldServer::SubmitTaskToScheduler(
    std::unique_ptr<TaskInCtld> task) {
  if (!task->password_entry->Valid()) {
    CRANE_DEBUG("Uid {} not found on the controller node", task->uid);
    return std::unexpected(CraneErrCode::ERR_INVALID_UID);
  }
  task->SetUsername(task->password_entry->Username());

  {  // Limit the lifecycle of user_scoped_ptr
    auto user_scoped_ptr =
        g_account_manager->GetExistedUserInfo(task->Username());
    if (!user_scoped_ptr) {
      CRANE_DEBUG("User '{}' not found in the account database",
                  task->Username());
      return std::unexpected(CraneErrCode::ERR_INVALID_USER);
    }

    if (task->account.empty()) {
      task->account = user_scoped_ptr->default_account;
      task->MutableTaskToCtld()->set_account(user_scoped_ptr->default_account);
    } else {
      if (!user_scoped_ptr->account_to_attrs_map.contains(task->account)) {
        CRANE_DEBUG(
            "Account '{}' is not in the user account list when submitting the "
            "task",
            task->account);
        return std::unexpected(CraneErrCode::ERR_USER_ACCOUNT_MISMATCH);
      }
    }
  }

  if (!g_account_manager->CheckUserPermissionToPartition(
          task->Username(), task->account, task->partition_id)) {
    CRANE_DEBUG(
        "User '{}' doesn't have permission to use partition '{}' when using "
        "account '{}'",
        task->Username(), task->partition_id, task->account);
    return std::unexpected(CraneErrCode::ERR_PARTITION_MISSING);
  }

  auto enable_res = g_account_manager->CheckIfUserOfAccountIsEnabled(
      task->Username(), task->account);
  if (!enable_res) {
    return std::unexpected(enable_res.error());
  }

  auto result = g_meta_container->CheckIfAccountIsAllowedInPartition(
      task->partition_id, task->account);
  if (!result) return std::unexpected(result.error());

  task->SetSubmitTime(absl::Now());

  result = TaskScheduler::HandleUnsetOptionalInTaskToCtld(task.get());
  if (result) result = TaskScheduler::AcquireTaskAttributes(task.get());
  if (result) result = TaskScheduler::CheckTaskValidity(task.get());
  if (result) {
    std::future<task_id_t> future =
        g_task_scheduler->SubmitTaskAsync(std::move(task));
    return {std::move(future)};
  }

  return std::unexpected(result.error());
}

}  // namespace Ctld

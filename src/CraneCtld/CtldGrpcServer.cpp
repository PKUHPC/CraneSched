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
#include "EmbeddedDbClient.h"
#include "TaskScheduler.h"

namespace Ctld {

grpc::Status CraneCtldServiceImpl::SubmitBatchTask(
    grpc::ServerContext *context,
    const crane::grpc::SubmitBatchTaskRequest *request,
    crane::grpc::SubmitBatchTaskReply *response) {
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
      response->set_reason(
          "System error occurred or "
          "the number of pending tasks exceeded maximum value.");
    }
  } else {
    response->set_ok(false);
    response->set_reason(result.error());
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::SubmitBatchTasks(
    grpc::ServerContext *context,
    const crane::grpc::SubmitBatchTasksRequest *request,
    crane::grpc::SubmitBatchTasksReply *response) {
  std::vector<std::expected<std::future<task_id_t>, std::string>> results;

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
      response->mutable_reason_list()->Add(std::move(res.error()));
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::TaskStatusChange(
    grpc::ServerContext *context,
    const crane::grpc::TaskStatusChangeRequest *request,
    crane::grpc::TaskStatusChangeReply *response) {
  std::optional<std::string> reason;
  if (!request->reason().empty()) reason = request->reason();

  g_task_scheduler->TaskStatusChangeWithReasonAsync(
      request->task_id(), request->craned_id(), request->new_status(),
      request->exit_code(), std::move(reason));
  response->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::CranedRegister(
    grpc::ServerContext *context,
    const crane::grpc::CranedRegisterRequest *request,
    crane::grpc::CranedRegisterReply *response) {
  if (!g_meta_container->CheckCranedAllowed(request->craned_id())) {
    response->set_ok(false);
    return grpc::Status::OK;
  }

  bool alive = g_meta_container->CheckCranedOnline(request->craned_id());
  if (!alive) {
    g_craned_keeper->PutNodeIntoUnavailList(request->craned_id());
  }

  response->set_ok(true);
  response->set_already_registered(alive);

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::CancelTask(
    grpc::ServerContext *context, const crane::grpc::CancelTaskRequest *request,
    crane::grpc::CancelTaskReply *response) {
  *response = g_task_scheduler->CancelPendingOrRunningTask(*request);
  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::QueryCranedInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryCranedInfoRequest *request,
    crane::grpc::QueryCranedInfoReply *response) {
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
  if (request->partition_name().empty()) {
    *response = g_meta_container->QueryAllPartitionInfo();
  } else {
    *response = g_meta_container->QueryPartitionInfo(request->partition_name());
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::ModifyTask(
    grpc::ServerContext *context, const crane::grpc::ModifyTaskRequest *request,
    crane::grpc::ModifyTaskReply *response) {
  using ModifyTaskRequest = crane::grpc::ModifyTaskRequest;

  auto res = g_account_manager->CheckUidIsAdmin(request->uid());
  if (!res) {
    for (auto task_id : request->task_ids()) {
      response->add_not_modified_tasks(task_id);
      response->add_not_modified_reasons(res.error());
    }
    return grpc::Status::OK;
  }

  CraneErr err;
  if (request->attribute() == ModifyTaskRequest::TimeLimit) {
    for (auto task_id : request->task_ids()) {
      err = g_task_scheduler->ChangeTaskTimeLimit(
          task_id, request->time_limit_seconds());
      if (err == CraneErr::kOk) {
        response->add_modified_tasks(task_id);
      } else if (err == CraneErr::kNonExistent) {
        response->add_not_modified_tasks(task_id);
        response->add_not_modified_reasons(fmt::format(
            "Task #{} was not found in running or pending queue.", task_id));
      } else if (err == CraneErr::kInvalidParam) {
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
      if (err == CraneErr::kOk) {
        response->add_modified_tasks(task_id);
      } else if (err == CraneErr::kNonExistent) {
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
    std::vector<std::pair<task_id_t, std::future<CraneErr>>> results;
    results.reserve(request->task_ids().size());
    for (auto task_id : request->task_ids()) {
      results.emplace_back(
          task_id, g_task_scheduler->HoldReleaseTaskAsync(task_id, secs));
    }
    for (auto &[task_id, res] : results) {
      err = res.get();
      if (err == CraneErr::kOk) {
        response->add_modified_tasks(task_id);
      } else if (err == CraneErr::kNonExistent) {
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
  auto res = g_account_manager->CheckUidIsAdmin(request->uid());
  if (!res) {
    for (auto crane_id : request->craned_ids()) {
      response->add_not_modified_nodes(crane_id);
      response->add_not_modified_reasons(res.error());
    }
    return grpc::Status::OK;
  }
  *response = g_meta_container->ChangeNodeState(*request);

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::ModifyPartitionAllowAccounts(
    grpc::ServerContext *context,
    const crane::grpc::ModifyPartitionAllowAccountsRequest *request,
    crane::grpc::ModifyPartitionAllowAccountsReply *response) {
  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::QueryTasksInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryTasksInfoRequest *request,
    crane::grpc::QueryTasksInfoReply *response) {
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
    response->set_reason(result.error());
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::AddUser(
    grpc::ServerContext *context, const crane::grpc::AddUserRequest *request,
    crane::grpc::AddUserReply *response) {
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

  AccountManager::CraneExpected<void> result =
      g_account_manager->AddUser(request->uid(), user);
  if (result) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(result.error());
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::AddQos(
    grpc::ServerContext *context, const crane::grpc::AddQosRequest *request,
    crane::grpc::AddQosReply *response) {
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
    response->set_reason(AccountManager::CraneErrCode::ERR_TIME_LIMIT);
    return grpc::Status::OK;
  }
  qos.max_time_limit_per_task = absl::Seconds(sec);

  auto result = g_account_manager->AddQos(request->uid(), qos);
  if (result) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(result.error());
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::ModifyAccount(
    grpc::ServerContext *context,
    const crane::grpc::ModifyAccountRequest *request,
    crane::grpc::ModifyAccountReply *response) {
  auto modify_res = g_account_manager->ModifyAccount(
      request->type(), request->uid(), request->name(), request->modify_field(),
      request->value(), request->force());

  if (modify_res) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(modify_res.error());
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::ModifyUser(
    grpc::ServerContext *context, const crane::grpc::ModifyUserRequest *request,
    crane::grpc::ModifyUserReply *response) {
  AccountManager::CraneExpected<void> modify_res;

  if (request->type() == crane::grpc::OperationType::Delete) {
    switch (request->modify_field()) {
    case crane::grpc::ModifyField::Partition:
      modify_res = g_account_manager->DeleteUserAllowedPartition(
          request->uid(), request->name(), request->account(),
          request->value());
      break;
    case crane::grpc::ModifyField::Qos:
      modify_res = g_account_manager->DeleteUserAllowedQos(
          request->uid(), request->name(), request->partition(),
          request->account(), request->value(), request->force());
      break;
    default:
      std::unreachable();
    }
  } else {
    switch (request->modify_field()) {
    case crane::grpc::ModifyField::AdminLevel:
      modify_res = g_account_manager->ModifyAdminLevel(
          request->uid(), request->name(), request->value());
      break;
    case crane::grpc::ModifyField::Partition:
      modify_res = g_account_manager->ModifyUserAllowedPartition(
          request->type(), request->uid(), request->name(), request->account(),
          request->value());
      break;
    case crane::grpc::ModifyField::Qos:
      modify_res = g_account_manager->ModifyUserAllowedQos(
          request->type(), request->uid(), request->name(),
          request->partition(), request->account(), request->value(),
          request->force());
      break;
    case crane::grpc::ModifyField::DefaultQos:
      modify_res = g_account_manager->ModifyUserDefaultQos(
          request->uid(), request->name(), request->partition(),
          request->account(), request->value());
      break;
    default:
      std::unreachable();
    }
  }

  if (modify_res) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(modify_res.error());
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::ModifyQos(
    grpc::ServerContext *context, const crane::grpc::ModifyQosRequest *request,
    crane::grpc::ModifyQosReply *response) {
  auto modify_res =
      g_account_manager->ModifyQos(request->uid(), request->name(),
                                   request->modify_field(), request->value());

  if (modify_res) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(modify_res.error());
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::QueryAccountInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryAccountInfoRequest *request,
    crane::grpc::QueryAccountInfoReply *response) {
  std::unordered_map<std::string, Account> res_account_map;
  auto modify_res = g_account_manager->QueryAccountInfo(
      request->uid(), request->name(), &res_account_map);
  if (modify_res) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(modify_res.error());
  }

  for (const auto &it : res_account_map) {
    const auto &account = it.second;
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
  std::unordered_map<uid_t, User> res_user_map;
  auto modify_res = g_account_manager->QueryUserInfo(
      request->uid(), request->name(), &res_user_map);
  if (modify_res) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(modify_res.error());
  }

  for (const auto &it : res_user_map) {
    const auto &user = it.second;
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
  std::unordered_map<std::string, Qos> res_qos_map;

  auto modify_res = g_account_manager->QueryQosInfo(
      request->uid(), request->name(), &res_qos_map);
  if (modify_res) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(modify_res.error());
  }

  auto *list = response->mutable_qos_list();
  for (const auto &[name, qos] : res_qos_map) {
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
  auto res = g_account_manager->DeleteAccount(request->uid(), request->name());
  if (res) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(res.error());
  }
  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::DeleteUser(
    grpc::ServerContext *context, const crane::grpc::DeleteUserRequest *request,
    crane::grpc::DeleteUserReply *response) {
  auto res = g_account_manager->DeleteUser(request->uid(), request->name(),
                                           request->account());
  if (res) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(res.error());
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::DeleteQos(
    grpc::ServerContext *context, const crane::grpc::DeleteQosRequest *request,
    crane::grpc::DeleteQosReply *response) {
  auto res = g_account_manager->DeleteQos(request->uid(), request->name());
  if (res) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(res.error());
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::BlockAccountOrUser(
    grpc::ServerContext *context,
    const crane::grpc::BlockAccountOrUserRequest *request,
    crane::grpc::BlockAccountOrUserReply *response) {
  AccountManager::CraneExpected<void> res;

  switch (request->entity_type()) {
  case crane::grpc::Account:
    res = g_account_manager->BlockAccount(request->uid(), request->name(),
                                          request->block());
    break;
  case crane::grpc::User:
    res = g_account_manager->BlockUser(request->uid(), request->name(),
                                       request->account(), request->block());
    break;
  default:
    std::unreachable();
  }

  if (res) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(res.error());
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::QueryClusterInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryClusterInfoRequest *request,
    crane::grpc::QueryClusterInfoReply *response) {
  *response = g_meta_container->QueryClusterInfo(*request);
  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::CforedStream(
    grpc::ServerContext *context,
    grpc::ServerReaderWriter<crane::grpc::StreamCtldReply,
                             crane::grpc::StreamCforedRequest> *stream) {
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
          auto i_type = meta.interactive_type;

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

          meta.cb_task_completed = [this, i_type, cfored_name, writer_weak_ptr](
                                       task_id_t task_id,
                                       bool send_completion_ack) {
            if (auto writer = writer_weak_ptr.lock();
                writer && send_completion_ack)
              writer->WriteTaskCompletionAckReply(task_id);
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
            result = std::unexpected(submit_result.error());
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
                  payload.task_id()) != CraneErr::kOk)
            stream_writer->WriteTaskCompletionAckReply(payload.task_id());
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
  std::thread sigint_waiting_thread([p_server = m_server_.get()] {
    util::SetCurrentThreadName("SIGINT_Waiter");

    std::unique_lock<std::mutex> lk(s_sigint_mtx);
    s_sigint_cv.wait(lk);

    CRANE_TRACE("SIGINT captured. Calling Shutdown() on grpc server...");

    // craned_keeper MUST be shutdown before GrpcServer.
    // Otherwise, once GrpcServer is shut down, the main thread stops and
    // g_craned_keeper.reset() is called. The Shutdown here and reset() in the
    // main thread will access g_craned_keeper simultaneously and a race
    // condition will occur.
    g_craned_keeper->Shutdown();

    p_server->Shutdown(std::chrono::system_clock::now() +
                       std::chrono::seconds(1));
  });
  sigint_waiting_thread.detach();

  signal(SIGINT, &CtldServer::signal_handler_func);
}

std::expected<std::future<task_id_t>, std::string>
CtldServer::SubmitTaskToScheduler(std::unique_ptr<TaskInCtld> task) {
  CraneErr err;

  if (!task->password_entry->Valid()) {
    return std::unexpected(
        fmt::format("Uid {} not found on the controller node", task->uid));
  }
  task->SetUsername(task->password_entry->Username());

  {  // Limit the lifecycle of user_scoped_ptr
    auto user_scoped_ptr =
        g_account_manager->GetExistedUserInfo(task->Username());
    if (!user_scoped_ptr) {
      return std::unexpected(fmt::format(
          "User '{}' not found in the account database", task->Username()));
    }

    if (task->account.empty()) {
      task->account = user_scoped_ptr->default_account;
      task->MutableTaskToCtld()->set_account(user_scoped_ptr->default_account);
    } else {
      if (!user_scoped_ptr->account_to_attrs_map.contains(task->account)) {
        return std::unexpected(fmt::format(
            "Account '{}' is not in your account list", task->account));
      }
    }
  }

  if (!g_account_manager->CheckUserPermissionToPartition(
          task->Username(), task->account, task->partition_id)) {
    return std::unexpected(
        fmt::format("User '{}' doesn't have permission to use partition '{}' "
                    "when using account '{}'",
                    task->Username(), task->partition_id, task->account));
  }

  auto enable_res = g_account_manager->CheckIfUserOfAccountIsEnabled(
      task->Username(), task->account);
  if (!enable_res) {
    return std::unexpected(enable_res.error());
  }

  err = g_task_scheduler->AcquireTaskAttributes(task.get());

  if (err == CraneErr::kOk)
    err = g_task_scheduler->CheckTaskValidity(task.get());

  if (err == CraneErr::kOk) {
    task->SetSubmitTime(absl::Now());
    std::future<task_id_t> future =
        g_task_scheduler->SubmitTaskAsync(std::move(task));
    return {std::move(future)};
  }

  if (err == CraneErr::kNonExistent) {
    CRANE_DEBUG("Task submission failed. Reason: Partition doesn't exist!");
    return std::unexpected("Partition doesn't exist!");
  } else if (err == CraneErr::kInvalidNodeNum) {
    CRANE_DEBUG(
        "Task submission failed. Reason: --node is either invalid or greater "
        "than the number of nodes in its partition.");
    return std::unexpected(
        "--node is either invalid or greater than the number of nodes in its "
        "partition.");
  } else if (err == CraneErr::kNoResource) {
    CRANE_DEBUG(
        "Task submission failed. "
        "Reason: The resources of the partition are insufficient.");
    return std::unexpected("The resources of the partition are insufficient");
  } else if (err == CraneErr::kNoAvailNode) {
    CRANE_DEBUG(
        "Task submission failed. "
        "Reason: Nodes satisfying the requirements of task are insufficient");
    return std::unexpected(
        "Nodes satisfying the requirements of task are insufficient.");
  } else if (err == CraneErr::kInvalidParam) {
    CRANE_DEBUG(
        "Task submission failed. "
        "Reason: The param of task is invalid.");
    return std::unexpected("The param of task is invalid.");
  }
  return std::unexpected<std::string>(CraneErrStr(err));
}

}  // namespace Ctld

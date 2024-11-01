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

  auto result = g_task_scheduler->SubmitTaskToScheduler(std::move(task));
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
  std::vector<result::result<std::future<task_id_t>, std::string>> results;

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
      response->mutable_reason_list()->Add(res.error());
  }

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
  if (res.has_error()) {
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
    for (auto task_id : request->task_ids()) {
      err = g_task_scheduler->HoldReleaseTaskAsync(task_id, secs).get();
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
  *response = g_meta_container->ChangeNodeState(*request);

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
  user.password = user_info->password();
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

CtldServer::CtldServer(const Config::CraneCtldListenConf &listen_conf) {
  m_service_impl_ = std::make_unique<CraneCtldServiceImpl>(this);

  grpc::ServerBuilder builder;

  if (g_config.CompressedRpc) ServerBuilderSetCompression(&builder);

  std::string cranectld_listen_addr = listen_conf.CraneCtldListenAddr;
  if (listen_conf.UseTls) {
    ServerBuilderAddTcpTlsListeningPort(&builder, cranectld_listen_addr,
                                        listen_conf.CraneCtldListenPort,
                                        listen_conf.TlsCerts.ExternalCerts);
  } else {
    ServerBuilderAddTcpInsecureListeningPort(&builder, cranectld_listen_addr,
                                             listen_conf.CraneCtldListenPort);
  }

  // std::vector<
  //     std::unique_ptr<grpc::experimental::ServerInterceptorFactoryInterface>>
  //     creators;
  // creators.push_back(
  //     std::unique_ptr<grpc::experimental::ServerInterceptorFactoryInterface>(
  //         new AuthInterceptorFactory()));

  // builder.experimental().SetInterceptorCreators(std::move(creators));
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

}  // namespace Ctld

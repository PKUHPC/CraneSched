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

#include "CtldGrpcServer.h"

#include <google/protobuf/util/time_util.h>

#include "AccountManager.h"
#include "CranedKeeper.h"
#include "CranedMetaContainer.h"
#include "EmbeddedDbClient.h"
#include "TaskScheduler.h"
#include "crane/String.h"

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
  std::vector<result::result<std::future<task_id_t>, std::string>> results;

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
      response->mutable_reason_list()->Add(res.error());
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
  if (res.has_error()) {
    response->set_ok(false);
    response->set_reason(res.error());
    return grpc::Status::OK;
  }

  CraneErr err;
  if (request->attribute() == ModifyTaskRequest::TimeLimit) {
    err = g_task_scheduler->ChangeTaskTimeLimit(request->task_id(),
                                                request->time_limit_seconds());
    if (err == CraneErr::kOk) {
      response->set_ok(true);
    } else if (err == CraneErr::kNonExistent) {
      response->set_ok(false);
      response->set_reason(
          fmt::format("Task #{} was not found in running or pending queue.",
                      request->task_id()));
    } else if (err == CraneErr::kInvalidParam) {
      response->set_ok(false);
      response->set_reason("Invalid time limit value.");
    } else {
      response->set_ok(false);
      response->set_reason(
          fmt::format("Failed to change the time limit of Task#{}: {}.",
                      request->task_id(), CraneErrStr(err)));
    }
  } else if (request->attribute() == ModifyTaskRequest::Priority) {
    err = g_task_scheduler->ChangeTaskPriority(request->task_id(),
                                               request->mandated_priority());
    if (err == CraneErr::kOk) {
      response->set_ok(true);
    } else if (err == CraneErr::kNonExistent) {
      response->set_ok(false);
      response->set_reason(fmt::format(
          "Task #{} was not found in pending queue.", request->task_id()));
    } else {
      response->set_ok(false);
      response->set_reason(
          fmt::format("Failed to change priority: {}.", CraneErrStr(err)));
    }
  } else if (request->attribute() == ModifyTaskRequest::Hold) {
    task_id_t task_id = request->task_id();
    int64_t secs = request->hold_seconds();

    err = g_task_scheduler->HoldReleaseTaskAsync(task_id, secs).get();
    if (err == CraneErr::kOk) {
      response->set_ok(true);
    } else if (err == CraneErr::kNonExistent) {
      response->set_ok(false);
      response->set_reason(fmt::format(
          "Task #{} was not found in pending queue.", request->task_id()));
    } else {
      response->set_ok(false);
      response->set_reason(
          fmt::format("Failed to hold/release job: {}.", CraneErrStr(err)));
    }
  } else {
    response->set_ok(false);
    response->set_reason("Invalid function.");
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
  AccountManager::Result judge_res = g_account_manager->HasPermissionToAccount(
      request->uid(), request->account().parent_account(), false);

  if (!judge_res.ok) {
    response->set_ok(false);
    response->set_reason(judge_res.reason);
    return grpc::Status::OK;
  }

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

  AccountManager::Result result =
      g_account_manager->AddAccount(std::move(account));
  if (result.ok) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(result.reason);
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::AddUser(
    grpc::ServerContext *context, const crane::grpc::AddUserRequest *request,
    crane::grpc::AddUserReply *response) {
  User::AdminLevel user_level;
  AccountManager::Result judge_res = g_account_manager->HasPermissionToAccount(
      request->uid(), request->user().account(), false, &user_level);

  if (!judge_res.ok) {
    response->set_ok(false);
    response->set_reason(judge_res.reason);
    return grpc::Status::OK;
  }

  if (static_cast<int>(request->user().admin_level()) >=
      static_cast<int>(user_level)) {
    response->set_ok(false);
    response->set_reason(
        "Permission error : You cannot add a user with the same or greater "
        "permissions as yourself");
    return grpc::Status::OK;
  }

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

  AccountManager::Result result = g_account_manager->AddUser(std::move(user));
  if (result.ok) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(result.reason);
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::AddQos(
    grpc::ServerContext *context, const crane::grpc::AddQosRequest *request,
    crane::grpc::AddQosReply *response) {
  auto judge_res = g_account_manager->CheckUidIsAdmin(request->uid());

  if (judge_res.has_error()) {
    response->set_ok(false);
    response->set_reason(judge_res.error());
    return grpc::Status::OK;
  }

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
    response->set_reason(fmt::format("Time limit should be in [{}, {}] seconds",
                                     kTaskMinTimeLimitSec,
                                     kTaskMaxTimeLimitSec));
    return grpc::Status::OK;
  }
  qos.max_time_limit_per_task = absl::Seconds(sec);

  AccountManager::Result result = g_account_manager->AddQos(qos);
  if (result.ok) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(result.reason);
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::ModifyEntity(
    grpc::ServerContext *context,
    const crane::grpc::ModifyEntityRequest *request,
    crane::grpc::ModifyEntityReply *response) {
  AccountManager::Result judge_res;
  AccountManager::Result modify_res;

  switch (request->entity_type()) {
  case crane::grpc::Account:
    judge_res = g_account_manager->HasPermissionToAccount(
        request->uid(), request->name(), false);

    if (!judge_res.ok) {
      response->set_ok(false);
      response->set_reason(judge_res.reason);
      return grpc::Status::OK;
    }
    modify_res = g_account_manager->ModifyAccount(
        request->type(), request->name(), request->item(), request->value(),
        request->force());
    break;
  case crane::grpc::User: {
    AccountManager::UserMutexSharedPtr modifier_shared_ptr =
        g_account_manager->GetExistedUserInfo(request->name());
    User::AdminLevel user_level;
    judge_res = g_account_manager->HasPermissionToUser(
        request->uid(), request->name(), false, &user_level);

    if (!judge_res.ok) {
      response->set_ok(false);
      response->set_reason(judge_res.reason);
      return grpc::Status::OK;
    }

    if (modifier_shared_ptr->admin_level >= user_level) {
      response->set_ok(false);
      response->set_reason(
          "Permission error : You cannot modify a user with the same or "
          "greater permissions as yourself");
      return grpc::Status::OK;
    }
    if (request->item() == "admin_level") {
      User::AdminLevel new_level;
      if (request->value() == "none") {
        new_level = User::None;
      } else if (request->value() == "operator") {
        new_level = User::Operator;
      } else if (request->value() == "admin") {
        new_level = User::Admin;
      } else {
        response->set_ok(false);
        response->set_reason(
            fmt::format("Unknown admin level '{}'", request->value()));
        return grpc::Status::OK;
      }
      if (new_level > user_level) {
        response->set_ok(false);
        response->set_reason(
            "Permission error : You cannot modify a user's permissions to "
            "which greater than your own permissions");
        return grpc::Status::OK;
      }
    }
  }

    modify_res = g_account_manager->ModifyUser(
        request->type(), request->name(), request->partition(),
        request->account(), request->item(), request->value(),
        request->force());
    break;

  case crane::grpc::Qos: {
    auto res = g_account_manager->CheckUidIsAdmin(request->uid());

    if (res.has_error()) {
      response->set_ok(false);
      response->set_reason(res.error());
      return grpc::Status::OK;
    }

    modify_res = g_account_manager->ModifyQos(request->name(), request->item(),
                                              request->value());
  } break;

  default:
    break;
  }
  if (modify_res.ok) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(modify_res.reason);
  }
  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::QueryEntityInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryEntityInfoRequest *request,
    crane::grpc::QueryEntityInfoReply *response) {
  User::AdminLevel user_level;
  std::list<std::string> user_accounts;
  std::unordered_map<std::string, Account> res_account_map;
  std::unordered_map<uid_t, User> res_user_map;

  AccountManager::Result find_res =
      g_account_manager->FindUserLevelAccountsOfUid(request->uid(), &user_level,
                                                    &user_accounts);
  if (!find_res.ok) {
    response->set_ok(false);
    response->set_reason(find_res.reason);
    return grpc::Status::OK;
  }

  switch (request->entity_type()) {
  case crane::grpc::Account:
    if (request->name().empty()) {
      AccountManager::AccountMapMutexSharedPtr account_map_shared_ptr =
          g_account_manager->GetAllAccountInfo();
      if (account_map_shared_ptr) {
        if (user_level != User::None) {
          // If an administrator user queries account information, all
          // accounts are returned, variable user_account not used
          for (const auto &[name, account] : *account_map_shared_ptr) {
            if (account->deleted) {
              continue;
            }
            res_account_map.try_emplace(account->name, *account);
          }
        } else {
          // Otherwise, only all sub-accounts under your own accounts will be
          // returned
          std::queue<std::string> queue;
          for (const auto &acct : user_accounts) {
            // Z->A->B--->C->E
            //    |->D    |->F
            // If we query account C, [Z,A,B,C,E,F] is included.
            std::string p_name =
                account_map_shared_ptr->at(acct)->parent_account;
            while (!p_name.empty()) {
              res_account_map.try_emplace(
                  p_name, *(account_map_shared_ptr->at(p_name)));
              p_name = account_map_shared_ptr->at(p_name)->parent_account;
            }

            queue.push(acct);
            while (!queue.empty()) {
              std::string father = queue.front();
              res_account_map.try_emplace(
                  account_map_shared_ptr->at(father)->name,
                  *(account_map_shared_ptr->at(father)));
              queue.pop();
              for (const auto &child :
                   account_map_shared_ptr->at(father)->child_accounts) {
                queue.push(child);
              }
            }
          }
        }
        response->set_ok(true);
      } else {
        response->set_ok(false);
        response->set_reason("Can't find any account!");
        return grpc::Status::OK;
      }
    } else {
      // Query an account
      Account temp;
      {
        AccountManager::AccountMutexSharedPtr account_shared_ptr =
            g_account_manager->GetExistedAccountInfo(request->name());
        if (!account_shared_ptr) {
          response->set_ok(false);
          response->set_reason(
              fmt::format("Can't find account {}!", request->name()));
          return grpc::Status::OK;
        }
        temp = *account_shared_ptr;
      }

      AccountManager::Result judge_res =
          g_account_manager->HasPermissionToAccount(request->uid(),
                                                    request->name(), true);

      if (!judge_res.ok) {
        response->set_ok(false);
        response->set_reason(judge_res.reason);
        return grpc::Status::OK;
      }

      res_account_map.emplace(temp.name, std::move(temp));
      response->set_ok(true);
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
    }
    break;
  case crane::grpc::User:
    if (request->name().empty()) {
      AccountManager::UserMapMutexSharedPtr user_map_shared_ptr =
          g_account_manager->GetAllUserInfo();

      if (user_map_shared_ptr) {
        if (user_level != User::None) {
          // The rules for querying user information are the same as those for
          // querying accounts
          for (const auto &[user_name, user] : *user_map_shared_ptr) {
            if (user->deleted) {
              continue;
            }
            res_user_map.try_emplace(user->uid, *user);
          }
        } else {
          AccountManager::AccountMapMutexSharedPtr account_map_shared_ptr =
              g_account_manager->GetAllAccountInfo();

          std::queue<std::string> queue;
          for (const auto &acct : user_accounts) {
            queue.push(acct);
            while (!queue.empty()) {
              std::string father = queue.front();
              for (const auto &user :
                   account_map_shared_ptr->at(father)->users) {
                res_user_map.try_emplace(user_map_shared_ptr->at(user)->uid,
                                         *(user_map_shared_ptr->at(user)));
              }
              queue.pop();
              for (const auto &child :
                   account_map_shared_ptr->at(father)->child_accounts) {
                queue.push(child);
              }
            }
          }
        }
        response->set_ok(true);
      } else {
        response->set_ok(false);
        response->set_reason("Can't find any user!");
        return grpc::Status::OK;
      }

    } else {
      AccountManager::UserMutexSharedPtr user_shared_ptr =
          g_account_manager->GetExistedUserInfo(request->name());
      if (user_shared_ptr) {
        AccountManager::Result judge_res =
            g_account_manager->HasPermissionToUser(request->uid(),
                                                   request->name(), true);

        if (!judge_res.ok) {
          response->set_ok(false);
          response->set_reason(judge_res.reason);
          return grpc::Status::OK;
        }

        res_user_map.try_emplace(user_shared_ptr->uid, *user_shared_ptr);
        response->set_ok(true);
      } else {
        response->set_ok(false);
        response->set_reason(
            fmt::format("Can't find user {}", request->name()));
        return grpc::Status::OK;
      }
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
      }
    }
    break;
  case crane::grpc::Qos:
    if (request->name().empty()) {
      AccountManager::QosMapMutexSharedPtr qos_map_shared_ptr =
          g_account_manager->GetAllQosInfo();

      if (qos_map_shared_ptr) {
        auto *list = response->mutable_qos_list();
        for (const auto &[name, qos] : *qos_map_shared_ptr) {
          if (qos->deleted) {
            continue;
          }

          auto *qos_info = list->Add();
          qos_info->set_name(qos->name);
          qos_info->set_description(qos->description);
          qos_info->set_priority(qos->priority);
          qos_info->set_max_jobs_per_user(qos->max_jobs_per_user);
          qos_info->set_max_cpus_per_user(qos->max_cpus_per_user);
          qos_info->set_max_time_limit_per_task(
              absl::ToInt64Seconds(qos->max_time_limit_per_task));
        }
      }
      response->set_ok(true);
    } else {
      AccountManager::QosMutexSharedPtr qos_shared_ptr =
          g_account_manager->GetExistedQosInfo(request->name());
      if (qos_shared_ptr) {
        auto *qos_info = response->mutable_qos_list()->Add();
        qos_info->set_name(qos_shared_ptr->name);
        qos_info->set_description(qos_shared_ptr->description);
        qos_info->set_priority(qos_shared_ptr->priority);
        qos_info->set_max_jobs_per_user(qos_shared_ptr->max_jobs_per_user);
        qos_info->set_max_cpus_per_user(qos_shared_ptr->max_cpus_per_user);
        qos_info->set_max_time_limit_per_task(
            absl::ToInt64Seconds(qos_shared_ptr->max_time_limit_per_task));
        response->set_ok(true);
      } else {
        response->set_ok(false);
      }
    }
  default:
    break;
  }
  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::DeleteEntity(
    grpc::ServerContext *context,
    const crane::grpc::DeleteEntityRequest *request,
    crane::grpc::DeleteEntityReply *response) {
  User::AdminLevel user_level;
  AccountManager::Result res;

  switch (request->entity_type()) {
  case crane::grpc::User: {
    AccountManager::UserMutexSharedPtr deleter_shared_ptr =
        g_account_manager->GetExistedUserInfo(request->name());
    if (!deleter_shared_ptr) {
      response->set_ok(false);
      response->set_reason(
          fmt::format("User '{}' is not a crane user", request->name()));
      return grpc::Status::OK;
    }

    if (request->account().empty()) {
      // Remove user from all of it's accounts
      AccountManager::Result judge_res =
          g_account_manager->HasPermissionToAccount(
              request->uid(), deleter_shared_ptr->default_account, false,
              &user_level);
      if (user_level == User::None) {
        if (deleter_shared_ptr->account_to_attrs_map.size() != 1) {
          response->set_ok(false);
          response->set_reason(
              "Permission error : You can't remove user form more than one "
              "account at a time");
          return grpc::Status::OK;
        } else {
          if (!judge_res.ok) {
            response->set_ok(false);
            response->set_reason(judge_res.reason);
            return grpc::Status::OK;
          }
        }
      }

    } else {
      // Remove user from specific account
      AccountManager::Result judge_res =
          g_account_manager->HasPermissionToAccount(
              request->uid(), request->account(), false, &user_level);

      if (!judge_res.ok) {
        response->set_ok(false);
        response->set_reason(judge_res.reason);
        return grpc::Status::OK;
      }
    }

    if (user_level <= deleter_shared_ptr->admin_level) {
      response->set_ok(false);
      response->set_reason(
          "Permission error : You cannot delete a user with the same or "
          "greater permissions as yourself");
      return grpc::Status::OK;
    }
  }
    res = g_account_manager->DeleteUser(request->name(), request->account());
    break;
  case crane::grpc::Account: {
    AccountManager::Result judge_res =
        g_account_manager->HasPermissionToAccount(request->uid(),
                                                  request->name(), false);

    if (!judge_res.ok) {
      response->set_ok(false);
      response->set_reason(judge_res.reason);
      return grpc::Status::OK;
    }
  }
    res = g_account_manager->DeleteAccount(request->name());
    break;
  case crane::grpc::Qos: {
    auto judge_res = g_account_manager->CheckUidIsAdmin(request->uid());

    if (judge_res.has_error()) {
      response->set_ok(false);
      response->set_reason(judge_res.error());
      return grpc::Status::OK;
    }
  }
    res = g_account_manager->DeleteQos(request->name());
    break;
  default:
    break;
  }

  if (res.ok) {
    response->set_ok(true);
  } else {
    response->set_ok(false);
    response->set_reason(res.reason);
  }
  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::BlockAccountOrUser(
    grpc::ServerContext *context,
    const crane::grpc::BlockAccountOrUserRequest *request,
    crane::grpc::BlockAccountOrUserReply *response) {
  AccountManager::Result res;

  switch (request->entity_type()) {
  case crane::grpc::Account:
    res = g_account_manager->HasPermissionToAccount(request->uid(),
                                                    request->name(), false);

    if (!res.ok) {
      response->set_ok(false);
      response->set_reason(res.reason);
      return grpc::Status::OK;
    }
    res = g_account_manager->BlockAccount(request->name(), request->block());
    response->set_ok(res.ok);
    response->set_reason(res.reason);
    break;
  case crane::grpc::User:
    res = g_account_manager->HasPermissionToUser(request->uid(),
                                                 request->name(), false);

    if (!res.ok) {
      response->set_ok(false);
      response->set_reason(res.reason);
      return grpc::Status::OK;
    }
    res = g_account_manager->BlockUser(request->name(), request->account(),
                                       request->block());
    response->set_ok(res.ok);
    response->set_reason(res.reason);
    break;
  default:
    break;
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
        } else {
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
          if (payload.has_task_id()) {
            auto result = g_task_scheduler->SubmitProc(
                std::move(task), payload.task_id(), payload.pid());
            const auto &[proc_id, craned_ids] = result.get();
            ok = stream_writer.WriteTaskIdReply(
                payload.pid(),
                result::result<task_id_t, std::string>{payload.task_id()},
                proc_id, craned_ids);
            if (!ok) {
              CRANE_ERROR(
                  "Failed to send msg to cfored {}. Connection is broken. "
                  "Exiting...",
                  cfored_name);
              state = StreamState::kCleanData;
            }
            break;
          }

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
            if (auto writer = writer_weak_ptr.lock(); writer)
              writer->WriteTaskCancelRequest(task_id);
          };

          meta.cb_task_completed = [&, cfored_name](task_id_t task_id) {
            // calloc will not send TaskCompletionAckReply when task
            // Complete.
            // crun task will send TaskStatusChange from Craned,
//            if (meta.interactive_type == InteractiveTaskType::Crun) {
//              stream_writer.WriteTaskCompletionAckReply(task_id);
//            }
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
          result::result<task_id_t, std::string> result;
          if (submit_result.has_value()) {
            result = result::result<task_id_t, std::string>{
                submit_result.value().get()};
          } else {
            result = result::fail(submit_result.error());
          }
          ok = stream_writer->WriteTaskIdReply(payload.pid(), result, 0, {});

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

          if (g_task_scheduler->TerminatePendingOrRunningTask(
                  payload.task_id()) != CraneErr::kOk)
            CRANE_WARN("TaskCompletionReq error: Not found!");
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

  std::string listen_addr_port =
      fmt::format("{}:{}", listen_conf.CraneCtldListenAddr,
                  listen_conf.CraneCtldListenPort);

  grpc::ServerBuilder builder;

  if (g_config.CompressedRpc)
    builder.SetDefaultCompressionAlgorithm(GRPC_COMPRESS_GZIP);

  if (listen_conf.UseTls) {
    grpc::SslServerCredentialsOptions::PemKeyCertPair pem_key_cert_pair;
    pem_key_cert_pair.cert_chain = listen_conf.ServerCertContent;
    pem_key_cert_pair.private_key = listen_conf.ServerKeyContent;

    grpc::SslServerCredentialsOptions ssl_opts;
    // pem_root_certs is actually the certificate of server side rather than
    // CA certificate. CA certificate is not needed.
    // Since we use the same cert/key pair for both cranectld/craned,
    // pem_root_certs is set to the same certificate.
    ssl_opts.pem_root_certs = listen_conf.ServerCertContent;
    ssl_opts.pem_key_cert_pairs.emplace_back(std::move(pem_key_cert_pair));
    ssl_opts.client_certificate_request =
        GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;

    builder.AddListeningPort(listen_addr_port,
                             grpc::SslServerCredentials(ssl_opts));
  } else {
    builder.AddListeningPort(listen_addr_port,
                             grpc::InsecureServerCredentials());
  }

  builder.RegisterService(m_service_impl_.get());

  m_server_ = builder.BuildAndStart();
  if (!m_server_) {
    CRANE_ERROR("Cannot start gRPC server!");
    std::exit(1);
  }

  CRANE_INFO("CraneCtld is listening on {} and Tls is {}", listen_addr_port,
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

result::result<std::future<task_id_t>, std::string>
CtldServer::SubmitTaskToScheduler(std::unique_ptr<TaskInCtld> task) {
  CraneErr err;

  if (!task->password_entry->Valid()) {
    return result::fail(
        fmt::format("Uid {} not found on the controller node", task->uid));
  }
  task->SetUsername(task->password_entry->Username());

  {  // Limit the lifecycle of user_scoped_ptr
    auto user_scoped_ptr =
        g_account_manager->GetExistedUserInfo(task->Username());
    if (!user_scoped_ptr) {
      return result::fail(fmt::format(
          "User '{}' not found in the account database", task->Username()));
    }

    if (task->account.empty()) {
      task->account = user_scoped_ptr->default_account;
      task->MutableTaskToCtld()->set_account(user_scoped_ptr->default_account);
    } else {
      if (!user_scoped_ptr->account_to_attrs_map.contains(task->account)) {
        return result::fail(fmt::format(
            "Account '{}' is not in your account list", task->account));
      }
    }
  }

  if (!g_account_manager->CheckUserPermissionToPartition(
          task->Username(), task->account, task->partition_id)) {
    return result::fail(
        fmt::format("User '{}' doesn't have permission to use partition '{}' "
                    "when using account '{}'",
                    task->Username(), task->partition_id, task->account));
  }

  auto enable_res =
      g_account_manager->CheckEnableState(task->account, task->Username());
  if (enable_res.has_error()) {
    return result::fail(enable_res.error());
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
    return result::fail("Partition doesn't exist!");
  } else if (err == CraneErr::kInvalidNodeNum) {
    CRANE_DEBUG(
        "Task submission failed. Reason: --node is either invalid or greater "
        "than the number of nodes in its partition.");
    return result::fail(
        "--node is either invalid or greater than the number of nodes in its "
        "partition.");
  } else if (err == CraneErr::kNoResource) {
    CRANE_DEBUG(
        "Task submission failed. "
        "Reason: The resources of the partition are insufficient.");
    return result::fail("The resources of the partition are insufficient");
  } else if (err == CraneErr::kNoAvailNode) {
    CRANE_DEBUG(
        "Task submission failed. "
        "Reason: Nodes satisfying the requirements of task are insufficient");
    return result::fail(
        "Nodes satisfying the requirements of task are insufficient.");
  } else if (err == CraneErr::kInvalidParam) {
    CRANE_DEBUG(
        "Task submission failed. "
        "Reason: The param of task is invalid.");
    return result::fail("The param of task is invalid.");
  }
  return result::fail(CraneErrStr(err));
}

}  // namespace Ctld

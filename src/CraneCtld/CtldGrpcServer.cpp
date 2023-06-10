#include "CtldGrpcServer.h"

#include <google/protobuf/util/time_util.h>

#include "AccountManager.h"
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
    response->set_ok(true);
    response->set_task_id(result.value());
  } else {
    response->set_ok(false);
    response->set_reason(result.error());
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::TaskStatusChange(
    grpc::ServerContext *context,
    const crane::grpc::TaskStatusChangeRequest *request,
    crane::grpc::TaskStatusChangeReply *response) {
  crane::grpc::TaskStatus status{};
  if (request->new_status() == crane::grpc::Finished)
    status = crane::grpc::Finished;
  else if (request->new_status() == crane::grpc::Failed)
    status = crane::grpc::Failed;
  else if (request->new_status() == crane::grpc::Cancelled)
    status = crane::grpc::Cancelled;
  else
    CRANE_ERROR(
        "Task #{}: When TaskStatusChange RPC is called, the task should either "
        "be Finished, Failed or Cancelled. new_status = {}",
        request->task_id(), request->new_status());

  std::optional<std::string> reason;
  if (!request->reason().empty()) reason = request->reason();

  g_task_scheduler->TaskStatusChange(request->task_id(), request->craned_id(),
                                     status, request->exit_code(), reason);
  response->set_ok(true);
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
  using TargetAttributes = crane::grpc::ModifyTaskRequest::TargetAttributes;

  CraneErr err;
  if (request->attribute() ==
      TargetAttributes::ModifyTaskRequest_TargetAttributes_TimeLimit) {
    err = g_task_scheduler->ChangeTaskTimeLimit(request->task_id(),
                                                request->time_limit_seconds());
    if (err == CraneErr::kOk) {
      response->set_ok(true);
    } else if (err == CraneErr::kNonExistent) {
      response->set_ok(false);
      response->set_reason(
          fmt::format("Task #{} was not found in running or pending queue",
                      request->task_id()));
    } else if (err == CraneErr::kGenericFailure) {
      response->set_ok(false);
      response->set_reason(fmt::format(
          "The compute node failed to change the time limit of task#{}.",
          request->task_id()));
    }
  } else {
    response->set_ok(false);
    response->set_reason(fmt::format("Invalid request parameters."));
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::QueryTasksInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryTasksInfoRequest *request,
    crane::grpc::QueryTasksInfoReply *response) {
  // Query tasks in RAM
  g_task_scheduler->QueryTasksInRam(request, response);

  int num_limit = request->num_limit() <= 0 ? kDefaultQueryTaskNumLimit
                                            : request->num_limit();
  auto *task_list = response->mutable_task_info_list();
  if (task_list->size() >= num_limit) {
    std::sort(
        task_list->begin(), task_list->end(),
        [](const crane::grpc::TaskInfo &a, const crane::grpc::TaskInfo &b) {
          return a.end_time() > b.end_time();
        });
    response->set_ok(true);
    return grpc::Status::OK;
  }

  // Query finished tasks in Embedded database
  bool ok;

  std::list<crane::grpc::TaskInEmbeddedDb> ended_list;
  ok = g_embedded_db_client->GetEndedQueueCopy(&ended_list);
  if (!ok) {
    CRANE_ERROR(
        "Failed to call "
        "g_embedded_db_client->GetEndedQueueCopy(&ended_list)");
    return grpc::Status::OK;
  }

  auto ended_append_fn = [&](crane::grpc::TaskInEmbeddedDb &task) {
    auto *task_it = task_list->Add();

    task_it->set_type(task.task_to_ctld().type());
    task_it->set_task_id(task.persisted_part().task_id());
    task_it->set_name(task.task_to_ctld().name());
    task_it->set_partition(task.task_to_ctld().partition_name());
    task_it->set_uid(task.task_to_ctld().uid());

    task_it->set_gid(task.persisted_part().gid());
    task_it->mutable_time_limit()->CopyFrom(task.task_to_ctld().time_limit());
    task_it->mutable_start_time()->CopyFrom(task.persisted_part().start_time());
    task_it->mutable_end_time()->CopyFrom(task.persisted_part().end_time());
    task_it->set_account(task.task_to_ctld().account());

    task_it->set_node_num(task.task_to_ctld().node_num());
    task_it->set_cmd_line(task.task_to_ctld().cmd_line());
    task_it->set_cwd(task.task_to_ctld().cwd());
    task_it->set_username(task.persisted_part().username());
    task_it->set_qos(task.task_to_ctld().qos());

    task_it->set_alloc_cpus(task.task_to_ctld().cpus_per_task());
    task_it->set_exit_code(task.persisted_part().exit_code());

    task_it->set_status(task.persisted_part().status());
    task_it->set_craned_list(
        util::HostNameListToStr(task.persisted_part().craned_ids()));
  };
  bool no_start_time_constraint = !request->has_filter_start_time();
  bool no_end_time_constraint = !request->has_filter_end_time();
  auto task_rng_filter_time = [&](crane::grpc::TaskInEmbeddedDb &task) {
    bool filter_start_time =
        no_start_time_constraint ||
        task.persisted_part().start_time() >= request->filter_start_time();
    bool filter_end_time =
        no_end_time_constraint ||
        task.persisted_part().end_time() <= request->filter_end_time();
    return filter_start_time && filter_end_time;
  };

  bool no_accounts_constraint = request->filter_accounts().empty();
  std::unordered_set<std::string> req_accounts(
      request->filter_accounts().begin(), request->filter_accounts().end());
  auto task_rng_filter_account = [&](crane::grpc::TaskInEmbeddedDb &task) {
    return no_accounts_constraint ||
           req_accounts.contains(task.task_to_ctld().account());
  };

  bool no_username_constraint = request->filter_users().empty();
  std::unordered_set<std::string> req_users(request->filter_users().begin(),
                                            request->filter_users().end());
  auto task_rng_filter_username = [&](crane::grpc::TaskInEmbeddedDb &task) {
    return no_username_constraint ||
           req_users.contains(task.persisted_part().username());
  };

  bool no_qos_constraint = request->filter_qos().empty();
  std::unordered_set<std::string> req_qos(request->filter_qos().begin(),
                                          request->filter_qos().end());
  auto task_rng_filter_qos = [&](crane::grpc::TaskInEmbeddedDb &task) {
    return no_qos_constraint || req_users.contains(task.task_to_ctld().qos());
  };

  bool no_task_names_constraint = request->filter_task_names().empty();
  std::unordered_set<std::string> req_task_names(
      request->filter_task_names().begin(), request->filter_task_names().end());
  auto task_rng_filter_task_name = [&](crane::grpc::TaskInEmbeddedDb &task) {
    return no_task_names_constraint ||
           req_task_names.contains(task.task_to_ctld().name());
  };

  bool no_partitions_constraint = request->filter_partitions().empty();
  std::unordered_set<std::string> req_partitions(
      request->filter_partitions().begin(), request->filter_partitions().end());
  auto task_rng_filter_partition = [&](crane::grpc::TaskInEmbeddedDb &task) {
    return no_partitions_constraint ||
           req_partitions.contains(task.task_to_ctld().partition_name());
  };

  bool no_task_ids_constraint = request->filter_task_ids().empty();
  std::unordered_set<uint32_t> req_task_ids(request->filter_task_ids().begin(),
                                            request->filter_task_ids().end());
  auto task_rng_filter_id = [&](crane::grpc::TaskInEmbeddedDb &task) {
    return no_task_ids_constraint ||
           req_task_ids.contains(task.persisted_part().task_id());
  };

  bool no_task_states_constraint = request->filter_task_states().empty();
  std::unordered_set<int> req_task_states(request->filter_task_states().begin(),
                                          request->filter_task_states().end());
  auto task_rng_filter_state = [&](crane::grpc::TaskInEmbeddedDb &task) {
    return no_task_states_constraint ||
           req_task_states.contains(task.persisted_part().status());
  };

  auto ended_rng =
      ended_list |
      ranges::views::filter([&](crane::grpc::TaskInEmbeddedDb &task) -> bool {
        return task.persisted_part().status() == crane::grpc::Cancelled;
      });
  auto filtered_ended_rng = ended_rng |
                            ranges::views::filter(task_rng_filter_account) |
                            ranges::views::filter(task_rng_filter_task_name) |
                            ranges::views::filter(task_rng_filter_username) |
                            ranges::views::filter(task_rng_filter_partition) |
                            ranges::views::filter(task_rng_filter_id) |
                            ranges::views::filter(task_rng_filter_state) |
                            ranges::views::filter(task_rng_filter_time) |
                            ranges::views::filter(task_rng_filter_qos) |
                            ranges::views::take(num_limit - task_list->size());
  ranges::for_each(filtered_ended_rng, ended_append_fn);

  if (task_list->size() >= num_limit ||
      !request->option_include_completed_tasks()) {
    std::sort(
        task_list->begin(), task_list->end(),
        [](const crane::grpc::TaskInfo &a, const crane::grpc::TaskInfo &b) {
          return a.end_time() > b.end_time();
        });
    response->set_ok(true);
    return grpc::Status::OK;
  }

  // Query completed tasks in Mongodb
  // (only for cacct, which sets `option_include_completed_tasks` to true)
  std::list<TaskInCtld> db_ended_list;
  ok = g_db_client->FetchJobRecords(&db_ended_list,
                                    num_limit - task_list->size(), true);
  if (!ok) {
    CRANE_ERROR("Failed to call g_db_client->FetchJobRecords");
    return grpc::Status::OK;
  }

  auto db_ended_append_fn = [&](TaskInCtld &task) {
    auto *task_it = task_list->Add();

    task_it->set_type(task.type);
    task_it->set_task_id(task.TaskId());
    task_it->set_name(task.name);
    task_it->set_partition(task.partition_id);
    task_it->set_uid(task.uid);

    task_it->set_gid(task.Gid());
    task_it->mutable_time_limit()->set_seconds(ToInt64Seconds(task.time_limit));
    task_it->mutable_start_time()->CopyFrom(task.PersistedPart().start_time());
    task_it->mutable_end_time()->CopyFrom(task.PersistedPart().end_time());
    task_it->set_account(task.account);

    task_it->set_node_num(task.node_num);
    task_it->set_cmd_line(task.cmd_line);
    task_it->set_cwd(task.cwd);
    task_it->set_username(task.PersistedPart().username());
    task_it->set_qos(task.qos);

    task_it->set_alloc_cpus(task.resources.allocatable_resource.cpu_count);
    task_it->set_exit_code(task.ExitCode());

    task_it->set_status(task.Status());
    task_it->set_craned_list(task.allocated_craneds_regex);
  };

  auto db_task_rng_filter_time = [&](TaskInCtld &task) {
    bool filter_start_time =
        no_start_time_constraint ||
        task.PersistedPart().start_time() >= request->filter_start_time();
    bool filter_end_time =
        no_end_time_constraint ||
        task.PersistedPart().end_time() <= request->filter_end_time();
    return filter_start_time && filter_end_time;
  };
  auto db_task_rng_filter_account = [&](TaskInCtld &task) {
    return no_accounts_constraint || req_accounts.contains(task.account);
  };

  auto db_task_rng_filter_user = [&](TaskInCtld &task) {
    return no_username_constraint || req_users.contains(task.Username());
  };

  auto db_task_rng_filter_name = [&](TaskInCtld &task) {
    return no_task_names_constraint ||
           req_task_names.contains(task.TaskToCtld().name());
  };

  auto db_task_rng_filter_qos = [&](TaskInCtld &task) {
    return no_qos_constraint || req_qos.contains(task.qos);
  };

  auto db_task_rng_filter_partition = [&](TaskInCtld &task) {
    return no_partitions_constraint ||
           req_partitions.contains(task.TaskToCtld().partition_name());
  };

  auto db_task_rng_filter_id = [&](TaskInCtld &task) {
    return no_task_ids_constraint || req_task_ids.contains(task.TaskId());
  };

  auto db_task_rng_filter_state = [&](TaskInCtld &task) {
    return no_task_states_constraint ||
           req_task_states.contains(task.PersistedPart().status());
  };

  auto db_ended_rng = db_ended_list |
                      ranges::views::filter(db_task_rng_filter_account) |
                      ranges::views::filter(db_task_rng_filter_name) |
                      ranges::views::filter(db_task_rng_filter_partition) |
                      ranges::views::filter(db_task_rng_filter_id) |
                      ranges::views::filter(db_task_rng_filter_state) |
                      ranges::views::filter(db_task_rng_filter_user) |
                      ranges::views::filter(db_task_rng_filter_time) |
                      ranges::views::filter(db_task_rng_filter_qos) |
                      ranges::views::take(num_limit - task_list->size());
  ranges::for_each(db_ended_rng, db_ended_append_fn);
  std::sort(task_list->begin(), task_list->end(),
            [](const crane::grpc::TaskInfo &a, const crane::grpc::TaskInfo &b) {
              return a.end_time() > b.end_time();
            });
  response->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::AddAccount(
    grpc::ServerContext *context, const crane::grpc::AddAccountRequest *request,
    crane::grpc::AddAccountReply *response) {
  AccountManager::Result judge_res = g_account_manager->HasPermissionToAccount(
      request->uid(), request->account().parent_account(), nullptr);

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
      request->uid(), request->user().account(), &user_level);

  if (!judge_res.ok) {
    response->set_ok(false);
    response->set_reason(judge_res.reason);
    return grpc::Status::OK;
  }

  if (static_cast<int>(request->user().admin_level()) >
      static_cast<int>(user_level)) {
    response->set_ok(false);
    response->set_reason(
        "Permission error : You cannot add user who has a larger permission "
        "than yours");
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
  AccountManager::Result judge_res =
      g_account_manager->HasPermissionToAccount(request->uid(), "", nullptr);

  if (!judge_res.ok) {
    response->set_ok(false);
    response->set_reason(judge_res.reason);
    return grpc::Status::OK;
  }

  Qos qos;
  const crane::grpc::QosInfo *qos_info = &request->qos();

  qos.name = qos_info->name();
  qos.description = qos_info->description();
  qos.priority = qos_info->priority();
  qos.max_jobs_per_user = qos_info->max_jobs_per_user();
  qos.max_cpus_per_user = qos_info->max_cpus_per_user();
  qos.max_time_limit_per_task =
      absl::Seconds(qos_info->max_time_limit_per_task());

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
          request->uid(), request->name(), nullptr);

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
          request->uid(), request->name(), &user_level);

      if (!judge_res.ok) {
        response->set_ok(false);
        response->set_reason(judge_res.reason);
        return grpc::Status::OK;
      }

      if (modifier_shared_ptr->admin_level > user_level) {
        response->set_ok(false);
        response->set_reason(
            "Permission error : You cannot modify a user who has a larger "
            "permission than yours");
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
    case crane::grpc::Qos:
      judge_res = g_account_manager->HasPermissionToAccount(request->uid(), "",
                                                            nullptr);

      if (!judge_res.ok) {
        response->set_ok(false);
        response->set_reason(judge_res.reason);
        return grpc::Status::OK;
      }
      modify_res = g_account_manager->ModifyQos(
          request->name(), request->item(), request->value());
      break;
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
  std::string user_account;
  std::list<Account> res_account_list;
  std::list<User> res_user_list;

  AccountManager::Result find_res =
      g_account_manager->FindUserLevelAccountOfUid(request->uid(), &user_level,
                                                   &user_account);
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
              res_account_list.emplace_back(*account);
            }
          } else {
            // Otherwise, only all sub accounts under your own account will be
            // returned
            std::queue<std::string> queue;
            queue.push(user_account);
            while (!queue.empty()) {
              std::string father = queue.front();
              res_account_list.emplace_back(
                  *(account_map_shared_ptr->at(father)));
              queue.pop();
              for (const auto &child :
                   account_map_shared_ptr->at(father)->child_accounts) {
                queue.push(child);
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
                                                      request->name(), nullptr);

        if (!judge_res.ok) {
          response->set_ok(false);
          response->set_reason(judge_res.reason);
          return grpc::Status::OK;
        }

        res_account_list.emplace_back(std::move(temp));
        response->set_ok(true);
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
              res_user_list.emplace_back(*user);
            }
          } else {
            AccountManager::AccountMapMutexSharedPtr account_map_shared_ptr =
                g_account_manager->GetAllAccountInfo();

            std::queue<std::string> queue;
            queue.push(user_account);
            while (!queue.empty()) {
              std::string father = queue.front();
              for (const auto &user :
                   account_map_shared_ptr->at(father)->users) {
                res_user_list.emplace_back(*(user_map_shared_ptr->at(user)));
              }
              queue.pop();
              for (const auto &child :
                   account_map_shared_ptr->at(father)->child_accounts) {
                queue.push(child);
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
                                                     request->name(), nullptr);

          if (!judge_res.ok) {
            response->set_ok(false);
            response->set_reason(judge_res.reason);
            return grpc::Status::OK;
          }

          res_user_list.emplace_back(*user_shared_ptr);
          response->set_ok(true);
        } else {
          response->set_ok(false);
          response->set_reason(
              fmt::format("Can't find user {}", request->name()));
          return grpc::Status::OK;
        }
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
                request->uid(), deleter_shared_ptr->default_account,
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
                request->uid(), request->account(), &user_level);

        if (!judge_res.ok) {
          response->set_ok(false);
          response->set_reason(judge_res.reason);
          return grpc::Status::OK;
        }
      }

      if (user_level <= deleter_shared_ptr->admin_level) {
        response->set_ok(false);
        response->set_reason(
            "Permission error : You can't delete a user who has a larger or "
            "same permission than yours");
        return grpc::Status::OK;
      }
    }
      res = g_account_manager->DeleteUser(request->name(), request->account());
      break;
    case crane::grpc::Account: {
      AccountManager::Result judge_res =
          g_account_manager->HasPermissionToAccount(request->uid(),
                                                    request->name(), nullptr);

      if (!judge_res.ok) {
        response->set_ok(false);
        response->set_reason(judge_res.reason);
        return grpc::Status::OK;
      }
    }
      res = g_account_manager->DeleteAccount(request->name());
      break;
    case crane::grpc::Qos: {
      AccountManager::Result judge_res =
          g_account_manager->HasPermissionToAccount(request->uid(), "",
                                                    nullptr);

      if (!judge_res.ok) {
        response->set_ok(false);
        response->set_reason(judge_res.reason);
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
                                                      request->name(), nullptr);

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
                                                   request->name(), nullptr);

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
  using crane::grpc::StreamCforedRequest;
  using crane::grpc::StreamCtldReply;
  using grpc::Status;

  enum class StreamState {
    kWaitRegReq = 0,
    kWaitMsg,
    kCleanData,
  };

  CraneErr err;
  bool ok;

  StreamCforedRequest cfored_request;
  StreamCtldReply ctld_reply;

  CforedStreamWriter stream_writer(stream);
  std::string cfored_name;

  CRANE_TRACE("CforedStream from {} created.", context->peer());

  StreamState state = StreamState::kWaitRegReq;
  while (true) {
    switch (state) {
      case StreamState::kWaitRegReq:
        ok = stream->Read(&cfored_request);
        if (ok) {
          if (cfored_request.type() !=
              StreamCforedRequest::CFORED_REGISTRATION) {
            CRANE_ERROR("Expect type CFORED_REGISTRATION from peer {}.",
                        context->peer());
            return Status::CANCELLED;
          } else {
            cfored_name = cfored_request.payload_cfored_reg().cfored_name();
            CRANE_INFO("Cfored {} registered.", cfored_name);

            ctld_reply.set_type(StreamCtldReply::CFORED_REGISTRATION_ACK);
            ctld_reply.mutable_payload_cfored_reg_ack()->set_ok(true);

            ok = stream->Write(ctld_reply);
            ctld_reply.Clear();
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

              auto &meta = std::get<InteractiveMetaInTask>(task->meta);
              meta.cb_task_res_allocated =
                  [writer = &stream_writer](task_id_t task_id) {
                    writer->WriteTaskResAllocReply(task_id, {});
                  };
              meta.cb_task_completed = [&, cfored_name](task_id_t task_id) {
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

              auto result =
                  m_ctld_server_->SubmitTaskToScheduler(std::move(task));

              ok = stream_writer.WriteTaskIdReply(payload.pid(), result);
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

              g_task_scheduler->TerminateRunningTask(payload.task_id());

              ok = stream_writer.WriteTaskCompletionAckReply(payload.task_id());
              if (!ok) {
                state = StreamState::kCleanData;
              }
            } break;

            case StreamCforedRequest::CFORED_GRACEFUL_EXIT: {
              stream_writer.WriteCforedGracefulExitAck();
              stream_writer.Invalidate();
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

  return grpc::Status::OK;
}

CtldServer::CtldServer(const Config::CraneCtldListenConf &listen_conf) {
  m_service_impl_ = std::make_unique<CraneCtldServiceImpl>(this);

  std::string listen_addr_port =
      fmt::format("{}:{}", listen_conf.CraneCtldListenAddr,
                  listen_conf.CraneCtldListenPort);

  grpc::ServerBuilder builder;
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
    std::unique_lock<std::mutex> lk(s_sigint_mtx);
    s_sigint_cv.wait(lk);

    CRANE_TRACE("SIGINT captured. Calling Shutdown() on grpc server...");
    p_server->Shutdown();
  });
  sigint_waiting_thread.detach();

  signal(SIGINT, &CtldServer::signal_handler_func);
}

result::result<task_id_t, std::string> CtldServer::SubmitTaskToScheduler(
    std::unique_ptr<TaskInCtld> task) {
  CraneErr err;

  if (!task->password_entry->Valid()) {
    return result::failure(
        fmt::format("Uid {} not found on the controller node", task->uid));
  }
  task->SetUsername(task->password_entry->Username());

  {  // Limit the lifecycle of user_scoped_ptr
    auto user_scoped_ptr =
        g_account_manager->GetExistedUserInfo(task->Username());
    if (!user_scoped_ptr) {
      return result::failure(fmt::format(
          "User '{}' not found in the account database", task->Username()));
    }

    if (task->account.empty()) {
      task->account = user_scoped_ptr->default_account;
      task->MutableTaskToCtld()->set_account(user_scoped_ptr->default_account);
    } else {
      if (!user_scoped_ptr->account_to_attrs_map.contains(task->account)) {
        return result::failure(fmt::format(
            "Account '{}' is not in your account list", task->account));
      }
    }
  }

  if (!g_account_manager->CheckUserPermissionToPartition(
          task->Username(), task->account, task->partition_id)) {
    return result::failure(fmt::format(
        "The user '{}' don't have access to submit task in partition '{}'",
        task->uid, task->partition_id));
  }

  if (!g_account_manager->CheckEnableState(task->account, task->Username())) {
    return result::failure(fmt::format(
        "The user '{}' or the Ancestor account is disabled", task->Username()));
  }

  auto check_qos_result = g_account_manager->CheckAndApplyQosLimitOnTask(
      task->Username(), task->account, task.get());
  if (check_qos_result.has_error()) {
    return result::failure(check_qos_result.error());
  }

  uint32_t task_id;
  err = g_task_scheduler->SubmitTask(std::move(task), &task_id);
  if (err == CraneErr::kOk) {
    return {task_id};
    CRANE_DEBUG("Received an task request. Task id allocated: {}", task_id);
  } else if (err == CraneErr::kNonExistent) {
    CRANE_DEBUG("Task submission failed. Reason: Partition doesn't exist!");
    return result::failure("Partition doesn't exist!");
  } else if (err == CraneErr::kInvalidNodeNum) {
    CRANE_DEBUG(
        "Task submission failed. Reason: --node is either invalid or "
        "greater than the number of alive nodes in its partition.");
    return result::failure(
        "--node is either invalid or greater than "
        "the number of alive nodes in its partition.");
  } else if (err == CraneErr::kNoResource) {
    CRANE_DEBUG(
        "Task submission failed. "
        "Reason: The resources of the partition are insufficient.");
    return result::failure("The resources of the partition are insufficient");
  }

  return {task_id};
}

}  // namespace Ctld
#include "CtldGrpcServer.h"

#include <absl/strings/str_split.h>
#include <google/protobuf/util/time_util.h>
#include <pwd.h>

#include <csignal>
#include <utility>

#include "AccountManager.h"
#include "CranedKeeper.h"
#include "CranedMetaContainer.h"
#include "TaskScheduler.h"
#include "crane/Network.h"
#include "crane/String.h"

namespace Ctld {

grpc::Status CraneCtldServiceImpl::AllocateInteractiveTask(
    grpc::ServerContext *context,
    const crane::grpc::InteractiveTaskAllocRequest *request,
    crane::grpc::InteractiveTaskAllocReply *response) {
  CraneErr err;
  auto task = std::make_unique<TaskInCtld>();

  task->partition_name = request->partition_name();
  task->resources.allocatable_resource =
      request->required_resources().allocatable_resource();
  task->time_limit = absl::Seconds(request->time_limit_sec());
  task->type = crane::grpc::Interactive;
  task->meta = InteractiveMetaInTask{};

  // Todo: Eliminate useless allocation here when err!=kOk.
  uint32_t task_id;
  err = g_task_scheduler->SubmitTask(std::move(task), false, &task_id);

  if (err == CraneErr::kOk) {
    response->set_ok(true);
    response->set_task_id(task_id);
  } else {
    response->set_ok(false);
    response->set_reason(err == CraneErr::kNonExistent
                             ? "Partition doesn't exist!"
                             : "Resource not enough!");
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::SubmitBatchTask(
    grpc::ServerContext *context,
    const crane::grpc::SubmitBatchTaskRequest *request,
    crane::grpc::SubmitBatchTaskReply *response) {
  CraneErr err;

  auto task = std::make_unique<TaskInCtld>();
  if (request->task().partition_name().empty()) {
    task->partition_name = g_config.DefaultPartition;
  } else {
    task->partition_name = request->task().partition_name();
  }
  task->resources.allocatable_resource =
      request->task().resources().allocatable_resource();
  task->time_limit = absl::Seconds(request->task().time_limit().seconds());

  task->meta = BatchMetaInTask{};
  auto &batch_meta = std::get<BatchMetaInTask>(task->meta);
  batch_meta.sh_script = request->task().batch_meta().sh_script();
  batch_meta.output_file_pattern =
      request->task().batch_meta().output_file_pattern();

  task->type = crane::grpc::Batch;

  task->node_num = request->task().node_num();
  task->ntasks_per_node = request->task().ntasks_per_node();
  task->cpus_per_task = request->task().cpus_per_task();

  task->uid = request->task().uid();
  task->name = request->task().name();
  task->cmd_line = request->task().cmd_line();
  task->env = request->task().env();
  task->cwd = request->task().cwd();

  task->task_to_ctld = request->task();

  if (task->uid) {
    if (!g_account_manager->CheckUserPermissionToPartition(
            getpwuid(task->uid)->pw_name, task->partition_name)) {
      response->set_ok(false);
      response->set_reason(fmt::format(
          "The user:{} don't have access to submit task in partition:{}",
          task->uid, task->partition_name));
      return grpc::Status::OK;
    }
  }

  uint32_t task_id;
  err = g_task_scheduler->SubmitTask(std::move(task), false, &task_id);
  if (err == CraneErr::kOk) {
    response->set_ok(true);
    response->set_task_id(task_id);
    CRANE_DEBUG("Received an batch task request. Task id allocated: {}",
                task_id);
  } else if (err == CraneErr::kNonExistent) {
    response->set_ok(false);
    response->set_reason("Partition doesn't exist!");
    CRANE_DEBUG(
        "Received an batch task request "
        "but the allocation failed. Reason: Resource "
        "not enough!");
  } else if (err == CraneErr::kInvalidNodeNum) {
    response->set_ok(false);
    response->set_reason(
        "--node is either invalid or greater than "
        "the number of alive nodes in its partition.");
    CRANE_DEBUG(
        "Received an batch task request "
        "but the allocation failed. Reason: --node is either invalid or "
        "greater than the number of alive nodes in its partition.");
  }

  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::QueryInteractiveTaskAllocDetail(
    grpc::ServerContext *context,
    const crane::grpc::QueryInteractiveTaskAllocDetailRequest *request,
    crane::grpc::QueryInteractiveTaskAllocDetailReply *response) {
  auto *detail = g_ctld_server->QueryAllocDetailOfIaTask(request->task_id());
  if (detail) {
    response->set_ok(true);
    response->mutable_detail()->set_ipv4_addr(detail->ipv4_addr);
    response->mutable_detail()->set_port(detail->port);
    response->mutable_detail()->set_craned_index(detail->craned_index);
    response->mutable_detail()->set_resource_uuid(detail->resource_uuid.data,
                                                  detail->resource_uuid.size());
  } else {
    response->set_ok(false);
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

  g_task_scheduler->TaskStatusChange(request->task_id(),
                                     request->craned_index(), status, reason);
  response->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::CancelTask(
    grpc::ServerContext *context, const crane::grpc::CancelTaskRequest *request,
    crane::grpc::CancelTaskReply *response) {
  uint32_t task_id = request->task_id();
  uint32_t operator_uid = request->operator_uid();

  CraneErr err =
      g_task_scheduler->CancelPendingOrRunningTask(operator_uid, task_id);
  // Todo: make the reason be set here!
  if (err == CraneErr::kOk)
    response->set_ok(true);
  else {
    response->set_ok(false);
    if (err == CraneErr::kNonExistent)
      response->set_reason("Task id doesn't exist!");
    else
      response->set_reason(CraneErrStr(err).data());
  }
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

grpc::Status CraneCtldServiceImpl::QueryJobsInPartition(
    grpc::ServerContext *context,
    const crane::grpc::QueryJobsInPartitionRequest *request,
    crane::grpc::QueryJobsInPartitionReply *response) {
  std::list<TaskInCtld> task_list;

  g_db_client->FetchJobRecordsWithStates(
      &task_list, {crane::grpc::Running, crane::grpc::Pending,
                   crane::grpc::Cancelled, crane::grpc::Completing});

  auto *meta_list = response->mutable_task_metas();
  auto *state_list = response->mutable_task_status();
  auto *allocated_craned_list = response->mutable_allocated_craneds();
  auto *id_list = response->mutable_task_ids();
  auto *partition_list = response->mutable_task_partitions();
  auto *name_list = response->mutable_task_names();

  std::unordered_set<uint32_t> req_task_id;
  std::unordered_set<std::string> req_task_names;
  std::unordered_set<int> req_task_status;
  std::unordered_set<std::string> req_partitions;

  if (!request->task_ids().empty()) {
    std::copy(request->task_ids().begin(), request->task_ids().end(),
              std::inserter(req_task_id, req_task_id.begin()));
  }
  if (!request->task_names().empty()) {
    std::copy(request->task_names().begin(), request->task_names().end(),
              std::inserter(req_task_names, req_task_names.begin()));
  }
  if (!request->task_status().empty()) {
    std::copy(request->task_status().begin(), request->task_status().end(),
              std::inserter(req_task_status, req_task_status.begin()));
  }
  if (!request->partitions().empty()) {
    std::copy(request->partitions().begin(), request->partitions().end(),
              std::inserter(req_partitions, req_partitions.begin()));
  }

  for (auto &task : task_list) {
    if (!request->task_ids().empty() &&
        req_task_id.find(task.task_id) == req_task_id.end())
      continue;
    if (!request->task_names().empty() &&
        req_task_names.find(task.name) == req_task_names.end())
      continue;
    if (!request->task_status().empty() &&
        req_task_status.find(task.status) == req_task_status.end())
      continue;
    if (!request->partitions().empty() &&
        req_partitions.find(task.partition_name) == req_partitions.end())
      continue;

    auto *meta_it = meta_list->Add();
    meta_it->CopyFrom(task.task_to_ctld);

    auto *state_it = state_list->Add();
    *state_it = task.status;

    auto *partition_it = partition_list->Add();
    *partition_it = task.partition_name;

    auto *node_list_it = allocated_craned_list->Add();
    *node_list_it = task.allocated_craneds_regex;

    auto *id_it = id_list->Add();
    *id_it = task.task_id;

    auto *name_it = name_list->Add();
    *name_it = task.name;
  }
  return grpc::Status::OK;
}

grpc::Status CraneCtldServiceImpl::QueryJobsInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryJobsInfoRequest *request,
    crane::grpc::QueryJobsInfoReply *response) {
  std::list<TaskInCtld> task_list;
  g_db_client->FetchJobRecordsWithStates(
      &task_list,
      {crane::grpc::Pending, crane::grpc::Running, crane::grpc::Finished});

  if (request->find_all()) {
    auto *task_info_list = response->mutable_task_info_list();

    for (auto &&task : task_list) {
      if (task.status == crane::grpc::Finished &&
          absl::ToInt64Seconds(absl::Now() - task.end_time) > 300)
        continue;
      auto *task_it = task_info_list->Add();

      task_it->mutable_submit_info()->CopyFrom(task.task_to_ctld);
      task_it->set_task_id(task.task_id);
      task_it->set_gid(task.gid);
      task_it->set_account(task.account);
      task_it->set_status(task.status);
      task_it->set_craned_list(task.allocated_craneds_regex);

      task_it->mutable_start_time()->CopyFrom(
          google::protobuf::util::TimeUtil::SecondsToTimestamp(
              ToUnixSeconds(task.start_time)));
      task_it->mutable_end_time()->CopyFrom(
          google::protobuf::util::TimeUtil::SecondsToTimestamp(
              ToUnixSeconds(task.end_time)));
    }
  } else {
    auto *task_info_list = response->mutable_task_info_list();

    for (auto &&task : task_list) {
      if (task.task_id == request->job_id()) {
        auto *task_it = task_info_list->Add();
        task_it->mutable_submit_info()->CopyFrom(task.task_to_ctld);
        task_it->set_task_id(task.task_id);
        task_it->set_gid(task.gid);
        task_it->set_account(task.account);
        task_it->set_status(task.status);
        task_it->set_craned_list(task.allocated_craneds_regex);

        task_it->mutable_start_time()->CopyFrom(
            google::protobuf::util::TimeUtil::SecondsToTimestamp(
                ToUnixSeconds(task.start_time)));
        task_it->mutable_end_time()->CopyFrom(
            google::protobuf::util::TimeUtil::SecondsToTimestamp(
                ToUnixSeconds(task.end_time)));
      }
    }
  }
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
  User user;
  const crane::grpc::UserInfo *user_info = &request->user();

  user.name = user_info->name();
  user.uid = user_info->uid();
  user.account = user_info->account();
  user.admin_level = User::AdminLevel(user_info->admin_level());

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
  Qos qos;
  const crane::grpc::QosInfo *qos_info = &request->qos();

  qos.name = qos_info->name();
  qos.description = qos_info->description();
  qos.priority = qos_info->priority();
  qos.max_jobs_per_user = qos_info->max_jobs_per_user();

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
  AccountManager::Result res;

  switch (request->entity_type()) {
    case crane::grpc::Account:
      res = g_account_manager->ModifyAccount(request->type(), request->name(),
                                             request->lhs(), request->rhs());

      break;
    case crane::grpc::User:
      res = g_account_manager->ModifyUser(request->type(), request->name(),
                                          request->partition(), request->lhs(),
                                          request->rhs());
      break;
    case crane::grpc::Qos:
      res = g_account_manager->ModifyQos(request->name(), request->lhs(),
                                         request->rhs());
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

grpc::Status CraneCtldServiceImpl::QueryEntityInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryEntityInfoRequest *request,
    crane::grpc::QueryEntityInfoReply *response) {
  switch (request->entity_type()) {
    case crane::grpc::Account:
      if (request->name().empty()) {
        AccountManager::AccountMapMutexSharedPtr account_map_shared_ptr =
            g_account_manager->GetAllAccountInfo();

        auto *list = response->mutable_account_list();

        if (account_map_shared_ptr) {
          for (const auto &[name, account] : *account_map_shared_ptr) {
            if (account->deleted) {
              continue;
            }

            auto *account_info = list->Add();
            account_info->set_name(account->name);
            account_info->set_description(account->description);

            auto *user_list = account_info->mutable_users();
            for (auto &&user : account->users) {
              user_list->Add()->assign(user);
            }

            auto *child_list = account_info->mutable_child_accounts();
            for (auto &&child : account->child_accounts) {
              child_list->Add()->assign(child);
            }
            account_info->set_parent_account(account->parent_account);

            auto *partition_list = account_info->mutable_allowed_partitions();
            for (auto &&partition : account->allowed_partition) {
              partition_list->Add()->assign(partition);
            }
            account_info->set_default_qos(account->default_qos);

            auto *allowed_qos_list = account_info->mutable_allowed_qos_list();
            for (const auto &qos : account->allowed_qos_list) {
              allowed_qos_list->Add()->assign(qos);
            }
          }
        }
        response->set_ok(true);
      } else {
        // Query an account
        AccountManager::AccountMutexSharedPtr account_shared_ptr =
            g_account_manager->GetExistedAccountInfo(request->name());
        if (account_shared_ptr) {
          auto *account_info = response->mutable_account_list()->Add();
          account_info->set_name(account_shared_ptr->name);
          account_info->set_description(account_shared_ptr->description);

          auto *user_list = account_info->mutable_users();
          for (auto &&user : account_shared_ptr->users) {
            user_list->Add()->assign(user);
          }

          auto *child_list = account_info->mutable_child_accounts();
          for (auto &&child : account_shared_ptr->child_accounts) {
            child_list->Add()->assign(child);
          }
          account_info->set_parent_account(account_shared_ptr->parent_account);

          auto *partition_list = account_info->mutable_allowed_partitions();
          for (auto &&partition : account_shared_ptr->allowed_partition) {
            partition_list->Add()->assign(partition);
          }
          account_info->set_default_qos(account_shared_ptr->default_qos);

          auto *allowed_qos_list = account_info->mutable_allowed_qos_list();
          for (const auto &qos : account_shared_ptr->allowed_qos_list) {
            allowed_qos_list->Add()->assign(qos);
          }
          response->set_ok(true);
        } else {
          response->set_ok(false);
        }
      }
      break;
    case crane::grpc::User:
      if (request->name().empty()) {
        AccountManager::UserMapMutexSharedPtr user_map_shared_ptr =
            g_account_manager->GetAllUserInfo();

        if (user_map_shared_ptr) {
          auto *list = response->mutable_user_list();
          for (const auto &[user_name, user] : *user_map_shared_ptr) {
            if (user->deleted) {
              continue;
            }

            auto *user_info = list->Add();
            user_info->set_name(user->name);
            user_info->set_uid(user->uid);
            user_info->set_account(user->account);
            user_info->set_admin_level(
                (crane::grpc::UserInfo_AdminLevel)user->admin_level);

            auto *partition_qos_list =
                user_info->mutable_allowed_partition_qos_list();
            for (const auto &[par_name, pair] :
                 user->allowed_partition_qos_map) {
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
        response->set_ok(true);
      } else {
        AccountManager::UserMutexSharedPtr user_shared_ptr =
            g_account_manager->GetExistedUserInfo(request->name());
        if (user_shared_ptr) {
          auto *user_info = response->mutable_user_list()->Add();
          user_info->set_name(user_shared_ptr->name);
          user_info->set_uid(user_shared_ptr->uid);
          user_info->set_account(user_shared_ptr->account);
          user_info->set_admin_level(
              (crane::grpc::UserInfo_AdminLevel)user_shared_ptr->admin_level);

          auto *partition_qos_list =
              user_info->mutable_allowed_partition_qos_list();
          for (const auto &[name, pair] :
               user_shared_ptr->allowed_partition_qos_map) {
            auto *partition_qos = partition_qos_list->Add();
            partition_qos->set_partition_name(name);
            partition_qos->set_default_qos(pair.first);

            auto *qos_list = partition_qos->mutable_qos_list();
            for (const auto &qos : pair.second) {
              qos_list->Add()->assign(qos);
            }
          }
          response->set_ok(true);
        } else {
          response->set_ok(false);
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
  AccountManager::Result res;

  switch (request->entity_type()) {
    case crane::grpc::User:
      res = g_account_manager->DeleteUser(request->name());
      break;
    case crane::grpc::Account:
      res = g_account_manager->DeleteAccount(request->name());
      break;
    case crane::grpc::Qos:
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

grpc::Status CraneCtldServiceImpl::QueryClusterInfo(
    grpc::ServerContext *context,
    const crane::grpc::QueryClusterInfoRequest *request,
    crane::grpc::QueryClusterInfoReply *response) {
  *response = g_meta_container->QueryClusterInfo(*request);
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
    pem_key_cert_pair.cert_chain = listen_conf.CertContent;
    pem_key_cert_pair.private_key = listen_conf.KeyContent;

    grpc::SslServerCredentialsOptions ssl_opts;
    ssl_opts.pem_root_certs = listen_conf.CertContent;
    ssl_opts.pem_key_cert_pairs.emplace_back(std::move(pem_key_cert_pair));
    ssl_opts.force_client_auth = true;
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

  g_craned_keeper->SetCranedIsUpCb(
      std::bind(&CtldServer::CranedIsUpCb_, this, std::placeholders::_1));

  g_craned_keeper->SetCranedIsDownCb(
      std::bind(&CtldServer::CranedIsDownCb_, this, std::placeholders::_1));
}

void CtldServer::CranedIsUpCb_(CranedId craned_id) {
  CRANE_TRACE(
      "A new node #{} is up now. Add its resource to the global resource pool.",
      craned_id);

  CranedStub *craned_stub = g_craned_keeper->GetCranedStub(craned_id);
  CRANE_ASSERT_MSG(craned_stub != nullptr,
                   "Got nullptr of CranedStub in NodeIsUp() callback!");

  g_meta_container->CranedUp(craned_id);

  CRANE_INFO("Node {} is up.", craned_id);
}

void CtldServer::CranedIsDownCb_(CranedId craned_id) {
  CRANE_TRACE(
      "CranedNode #{} is down now. Remove its resource from the global "
      "resource pool.",
      craned_id);

  g_meta_container->CranedDown(craned_id);
}

void CtldServer::AddAllocDetailToIaTask(
    uint32_t task_id, InteractiveTaskAllocationDetail detail) {
  LockGuard guard(m_mtx_);
  m_task_alloc_detail_map_.emplace(task_id, std::move(detail));
}

const InteractiveTaskAllocationDetail *CtldServer::QueryAllocDetailOfIaTask(
    uint32_t task_id) {
  LockGuard guard(m_mtx_);
  auto iter = m_task_alloc_detail_map_.find(task_id);
  if (iter == m_task_alloc_detail_map_.end()) return nullptr;

  return &iter->second;
}

void CtldServer::RemoveAllocDetailOfIaTask(uint32_t task_id) {
  LockGuard guard(m_mtx_);
  m_task_alloc_detail_map_.erase(task_id);
}

}  // namespace Ctld
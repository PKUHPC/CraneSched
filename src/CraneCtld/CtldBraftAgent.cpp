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

#include "CtldBraftAgent.h"

#include "TaskScheduler.h"

namespace Ctld {

void CraneCtldClosure::Run() {
  // Auto delete this after Run()
  std::unique_ptr<CraneCtldClosure> self_guard(
      this);  // TODO 尚不清楚此代码的目的，可能导致悬挂指针
  RefactorGuard done_guard(done_);
  if (status().ok()) {
    return;
  }
  // Try redirect if this request failed.
  cranectld_->redirect(response_);
}

int CraneStateMachine::start(const std::string &ip_addr, int port) {
  butil::EndPoint addr(butil::my_ip(), port);
  braft::NodeOptions node_options;
  if (node_options.initial_conf.parse_from("") != 0) {  // TODO
    //    LOG(ERROR) << "Fail to parse configuration";
    return -1;
  }
  node_options.election_timeout_ms = 5000;
  node_options.fsm = this;
  node_options.node_owns_fsm = false;
  node_options.snapshot_interval_s = 30;
  std::string prefix = "local://./data";
  node_options.log_uri = prefix + "/log";
  node_options.raft_meta_uri = prefix + "/raft_meta";
  node_options.snapshot_uri = prefix + "/snapshot";
  node_options.disable_cli = false;
  braft::Node *node = new braft::Node("CraneCtld", braft::PeerId(addr));
  if (node->init(node_options) != 0) {
    //    LOG(ERROR) << "Fail to init raft node";
    delete node;
    return -1;
  }
  node_ = node;
  return 0;
}

void CraneStateMachine::apply(CraneStateMachine::CraneCtldOpType type,
                              const google::protobuf::Message *request,
                      google::protobuf::Message *response,
                      grpc::ServerUnaryReactor *done) {
  RefactorGuard done_guard(done);

  // Serialize request to the replicated write-ahead-log so that all the
  // peers in the group receive this request as well.
  // Notice that _value can't be modified in this routine otherwise it
  // will be inconsistent with others in this group.

  // Serialize request to IOBuf
  const int64_t term = leader_term_.load(butil::memory_order_relaxed);
  if (term < 0) {
    return redirect(response);
  }
  butil::IOBuf log;
  log.push_back((uint8_t)type);
  butil::IOBufAsZeroCopyOutputStream wrapper(&log);
  if (!request->SerializeToZeroCopyStream(&wrapper)) {
    //    LOG(ERROR) << "Fail to serialize request";
    // TODO
    //    response->set_success(false);
    return;
  }
  // Apply this log as a braft::Task
  braft::Task task;
  task.data = &log;
  // This callback would be iovoked when the task actually excuted or
  // fail
  task.done = new CraneCtldClosure(this, request, response, nullptr,
                                   done_guard.release());
  // ABA problem can be avoided if expected_term is set
  task.expected_term = term;

  // Now the task is applied to the group, waiting for the result.
  return node_->apply(task);
}

void CraneStateMachine::on_apply(braft::Iterator &iter) {
  // A batch of tasks are committed, which must be processed through
  // |iter|
  for (; iter.valid(); iter.next()) {
    // This guard helps invoke iter.done()->Run() asynchronously to
    // avoid that callback blocks the StateMachine.
    braft::AsyncClosureGuard done_guard(iter.done());

    // Parse data
    butil::IOBuf data = iter.data();
    // Fetch the type of operation from the leading byte.
    uint8_t type = OP_UNKNOWN;
    data.cutn(&type, sizeof(uint8_t));

    CraneCtldClosure *c = nullptr;
    if (iter.done()) {
      c = dynamic_cast<CraneCtldClosure *>(iter.done());
    }

    const google::protobuf::Message *request = c ? c->request() : nullptr;
    google::protobuf::Message *response = c ? c->response() : nullptr;
    const char *op = nullptr;
    // Execute the operation according to type
    switch (type) {
    case OP_SUBMIT_BATCH_TASK:
      op = "submit_batch_task";
      SubmitBatchTask(data, request, response);
      break;
    case OP_TASK_STATUS_CHANGE:
      op = "task_status_change";
      TaskStatusChange(data, request, response);
      break;
    case OP_CRANED_REGISTER:
      op = "craned_register";
      CranedRegister(data, request, response);
      break;
    case OP_CANCEL_TASK:
      op = "cancel_task";
      CancelTask(data, request, response);
      break;
      //    case OP_QUERY_TASKS_INFO:
      //      op = "query_tasks_info";
      //      QueryTasksInfo(data,request, response);
      //      break;
      //    case OP_QUERY_CRANED_INFO:
      //      op = "query_craned_info";
      //      QueryCranedInfo(data,request, response);
      //      break;
      //    case OP_QUERY_PARTITION_INFO:
      //      op = "query_partition_info";
      //      QueryPartitionInfo(data,request, response);
      //      break;
    case OP_MODIFY_TASK:
      op = "modify_task";
      ModifyTask(data, request, response);
      break;
    case OP_MODIFY_NODE:
      op = "modify_node";
      ModifyNode(data, request, response);
      break;
    case OP_ADD_ACCOUNT:
      op = "add_account";
      AddAccount(data, request, response);
      break;
    case OP_ADD_USER:
      op = "add_user";
      AddUser(data, request, response);
      break;
    case OP_ADD_QOS:
      op = "add_qos";
      AddQos(data, request, response);
      break;
    case OP_MODIFY_ENTITY:
      op = "modify_entity";
      ModifyEntity(data, request, response);
      break;
      //    case OP_QUERY_ENTITY_INFO:
      //      op = "query_entity_info";
      //      QueryEntityInfo(data,request, response);
      //      break;
    case OP_DELETE_ENTITY:
      op = "delete_entity";
      DeleteEntity(data, request, response);
      break;
    case OP_BLOCK_ACCOUNT_OR_USER:
      op = "block_account_or_user";
      BlockAccountOrUser(data, request, response);
      break;
      //    case OP_QUERY_CLUSTER_INFO:
      //      op = "query_cluster_info";
      //      QueryClusterInfo(data,request, response);
      //      break;
    default:
      CHECK(false) << "Unknown type=" << static_cast<uint8_t>(type);
      break;
    }
  }
}

void CraneStateMachine::SubmitBatchTask(
    const butil::IOBuf &data, const google::protobuf::Message *request,
                                google::protobuf::Message *response) {
  auto task = std::make_unique<TaskInCtld>();
  auto *resp = dynamic_cast<crane::grpc::SubmitBatchTaskReply *>(response);

  if (request) {
    // This task is applied by this node, get value from this
    // closure to avoid additional parsing.
    auto *req =
        dynamic_cast<const crane::grpc::SubmitBatchTaskRequest *>(request);
    task->SetFieldsByTaskToCtld(req->task());
  } else {
    butil::IOBufAsZeroCopyInputStream wrapper(data);
    crane::grpc::SubmitBatchTaskRequest req;
    CHECK(req.ParseFromZeroCopyStream(&wrapper));
    task->SetFieldsByTaskToCtld(req.task());
  }

  auto result = m_ctld_server_->SubmitTaskToScheduler(std::move(task));
  if (result.has_value()) {
    task_id_t id = result.value().get();
    if (id != 0) {
      resp->set_ok(true);
      resp->set_task_id(id);
    } else {
      resp->set_ok(false);
      resp->set_reason(
          "System error occurred or "
          "the number of pending tasks exceeded maximum value.");
    }
  } else {
    resp->set_ok(false);
    resp->set_reason(result.error());
  }
}

void CraneStateMachine::TaskStatusChange(
    const butil::IOBuf &data, const google::protobuf::Message *request,
                                 google::protobuf::Message *response) {
  std::optional<std::string> reason;
  auto *resp = dynamic_cast<crane::grpc::TaskStatusChangeReply *>(response);

  if (request) {
    auto *req =
        dynamic_cast<const crane::grpc::TaskStatusChangeRequest *>(request);
    if (!req->reason().empty()) reason = req->reason();

    g_task_scheduler->TaskStatusChangeWithReasonAsync(
        req->task_id(), req->craned_id(), req->new_status(), req->exit_code(),
        std::move(reason));
    resp->set_ok(true);
  } else {
    butil::IOBufAsZeroCopyInputStream wrapper(data);
    crane::grpc::TaskStatusChangeRequest req;
    CHECK(req.ParseFromZeroCopyStream(&wrapper));
    if (!req.reason().empty()) reason = req.reason();

    g_task_scheduler->TaskStatusChangeWithReasonAsync(
        req.task_id(), req.craned_id(), req.new_status(), req.exit_code(),
        std::move(reason));
    resp->set_ok(true);
  }
}

void CraneStateMachine::CranedRegister(const butil::IOBuf &data,
                                       const google::protobuf::Message *request,
                               google::protobuf::Message *response) {}

void CraneStateMachine::CancelTask(const butil::IOBuf &data,
                                   const google::protobuf::Message *request,
                           google::protobuf::Message *response) {}

void CraneStateMachine::QueryTasksInfo(const butil::IOBuf &data,
                                       const google::protobuf::Message *request,
                               google::protobuf::Message *response) {}

void CraneStateMachine::QueryCranedInfo(
    const butil::IOBuf &data, const google::protobuf::Message *request,
                                google::protobuf::Message *response) {}

void CraneStateMachine::QueryPartitionInfo(
    const butil::IOBuf &data, const google::protobuf::Message *request,
                                   google::protobuf::Message *response) {}

void CraneStateMachine::ModifyTask(const butil::IOBuf &data,
                                   const google::protobuf::Message *request,
                           google::protobuf::Message *response) {}

void CraneStateMachine::ModifyNode(const butil::IOBuf &data,
                                   const google::protobuf::Message *request,
                           google::protobuf::Message *response) {}
void CraneStateMachine::AddAccount(const butil::IOBuf &data,
                                   const google::protobuf::Message *request,
                           google::protobuf::Message *response) {}
void CraneStateMachine::AddUser(const butil::IOBuf &data,
                                const google::protobuf::Message *request,
                        google::protobuf::Message *response) {}
void CraneStateMachine::AddQos(const butil::IOBuf &data,
                               const google::protobuf::Message *request,
                       google::protobuf::Message *response) {}
void CraneStateMachine::ModifyEntity(const butil::IOBuf &data,
                                     const google::protobuf::Message *request,
                             google::protobuf::Message *response) {}
void CraneStateMachine::QueryEntityInfo(
    const butil::IOBuf &data, const google::protobuf::Message *request,
                                google::protobuf::Message *response) {}
void CraneStateMachine::DeleteEntity(const butil::IOBuf &data,
                                     const google::protobuf::Message *request,
                             google::protobuf::Message *response) {}
void CraneStateMachine::BlockAccountOrUser(
    const butil::IOBuf &data, const google::protobuf::Message *request,
                                   google::protobuf::Message *response) {}
void CraneStateMachine::QueryClusterInfo(
    const butil::IOBuf &data, const google::protobuf::Message *request,
                                 google::protobuf::Message *response) {}

}  // namespace Ctld
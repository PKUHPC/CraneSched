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

#pragma once

#include <braft/raft.h>           // braft::Node braft::StateMachine
#include <braft/storage.h>        // braft::SnapshotWriter
#include <braft/util.h>           // braft::AsyncClosureGuard
#include <brpc/server.h>          // brpc::Server
#include <butil/sys_byteorder.h>  // butil::NetToHost32
#include <fcntl.h>                // open
#include <grpcpp/support/server_callback.h>

#include "CtldGrpcServer.h"
#include "CtldPublicDefs.h"
#include "crane/Logger.h"
#include "protos/Crane.pb.h"

namespace Ctld {

class RefactorGuard {
  // RAII: Call Run() of the closure on destruction.
 public:
  RefactorGuard() : done_(nullptr) {}

  // Constructed with a closure which will be Run() inside dtor.
  explicit RefactorGuard(grpc::ServerUnaryReactor *done) : done_(done) {}

  // Run internal closure if it's not NULL.
  ~RefactorGuard() {
    if (done_) {
      done_->Finish(grpc::Status::OK);
    }
  }

  // Run internal refactor if it's not NULL and set it to `done'.
  void reset(grpc::ServerUnaryReactor *done) {
    if (done_) {
      done_->Finish(grpc::Status::OK);
    }
    done_ = done;
  }

  // Return and set internal closure to NULL.
  grpc::ServerUnaryReactor *release() {
    grpc::ServerUnaryReactor *const prev_done = done_;
    done_ = nullptr;
    return prev_done;
  }

  // True if no closure inside.
  bool empty() const { return done_ == nullptr; }

  // Exchange closure with another guard.
  void swap(RefactorGuard &other) { std::swap(done_, other.done_); }

 private:
  // Copying this object makes no sense.
  DISALLOW_COPY_AND_ASSIGN(RefactorGuard);

  grpc::ServerUnaryReactor *done_;
};

class CraneCtld;

// Implements Closure which encloses RPC stuff
class CraneCtldClosure : public braft::Closure {
 public:
  CraneCtldClosure(CraneCtld *cranectld,
                   const google::protobuf::Message *request,
                   google::protobuf::Message *response, butil::IOBuf *data,
                   grpc::ServerUnaryReactor *done)
      : cranectld_(cranectld),
        request_(request),
        response_(response),
        data_(data),
        done_(done) {}
  ~CraneCtldClosure() {}

  const google::protobuf::Message *request() const { return request_; }
  google::protobuf::Message *response() const { return response_; }
  // 自动调用该函数，作用是通知通信层本次RPC已经完成
  void Run() override;
  butil::IOBuf *data() const { return data_; }

 private:
  // Disable explicitly delete
  CraneCtld *cranectld_;
  const google::protobuf::Message *request_;
  google::protobuf::Message *response_;
  butil::IOBuf *data_;
  grpc::ServerUnaryReactor *done_;
};

class CtldServer;

// Implementation of CraneCtld as a braft::StateMachine.
class CraneCtld : public braft::StateMachine {
 public:
  // Define types for different operation
  enum CraneCtldOpType : uint8_t {
    OP_UNKNOWN = 0,
    OP_SUBMIT_BATCH_TASK = 1,
    OP_TASK_STATUS_CHANGE = 2,
    OP_CRANED_REGISTER = 3,
    OP_CANCEL_TASK = 4,
    //    OP_QUERY_TASKS_INFO = 5,
    //    OP_QUERY_CRANED_INFO = 6,
    //    OP_QUERY_PARTITION_INFO = 7,
    OP_MODIFY_TASK = 8,
    OP_MODIFY_NODE = 9,
    OP_ADD_ACCOUNT = 10,
    OP_ADD_USER = 11,
    OP_ADD_QOS = 12,
    OP_MODIFY_ENTITY = 13,
    //    OP_QUERY_ENTITY_INFO = 14,
    OP_DELETE_ENTITY = 15,
    OP_BLOCK_ACCOUNT_OR_USER = 16,
    //    OP_QUERY_CLUSTER_INFO = 17,
  };

  explicit CraneCtld(CtldServer *server)
      : m_ctld_server_(server), node_(nullptr), leader_term_(-1) {}

  ~CraneCtld() override { delete node_; }

  // Starts this node
  int start(const std::string &ip_addr, int port);

  // Impelements service methods
  void SubmitBatchTask(const crane::grpc::SubmitBatchTaskRequest *request,
                       crane::grpc::SubmitBatchTaskReply *response,
                       grpc::ServerUnaryReactor *done) {
    return apply(OP_SUBMIT_BATCH_TASK, request, response, done);
  }

  void TaskStatusChange(const crane::grpc::TaskStatusChangeRequest *request,
                        crane::grpc::TaskStatusChangeReply *response,
                        grpc::ServerUnaryReactor *done) {
    return apply(OP_TASK_STATUS_CHANGE, request, response, done);
  }

  void CranedRegister(const crane::grpc::CranedRegisterRequest *request,
                      crane::grpc::CranedRegisterReply *response,
                      grpc::ServerUnaryReactor *done) {
    return apply(OP_CRANED_REGISTER, request, response, done);
  }

  void CancelTask(const crane::grpc::CancelTaskRequest *request,
                  crane::grpc::CancelTaskReply *response,
                  grpc::ServerUnaryReactor *done) {
    return apply(OP_CANCEL_TASK, request, response, done);
  }

  void QueryTasksInfo(const crane::grpc::QueryTasksInfoRequest *request,
                      crane::grpc::QueryTasksInfoReply *response) {
    //
  }

  void QueryCranedInfo(const crane::grpc::QueryCranedInfoRequest *request,
                       crane::grpc::QueryCranedInfoReply *response) {}

  void QueryPartitionInfo(const crane::grpc::QueryPartitionInfoRequest *request,
                          crane::grpc::QueryPartitionInfoReply *response) {}

  void ModifyTask(const crane::grpc::ModifyTaskRequest *request,
                  crane::grpc::ModifyTaskReply *response,
                  grpc::ServerUnaryReactor *done) {
    return apply(OP_MODIFY_TASK, request, response, done);
  }

  void ModifyNode(const crane::grpc::ModifyCranedStateRequest *request,
                  crane::grpc::ModifyCranedStateReply *response,
                  grpc::ServerUnaryReactor *done) {
    return apply(OP_MODIFY_NODE, request, response, done);
  }

  void AddAccount(const crane::grpc::AddAccountRequest *request,
                  crane::grpc::AddAccountReply *response,
                  grpc::ServerUnaryReactor *done) {
    return apply(OP_ADD_ACCOUNT, request, response, done);
  }

  void AddUser(const crane::grpc::AddUserRequest *request,
               crane::grpc::AddUserReply *response,
               grpc::ServerUnaryReactor *done) {
    return apply(OP_ADD_USER, request, response, done);
  }

  void AddQos(const crane::grpc::AddQosRequest *request,
              crane::grpc::AddQosReply *response,
              grpc::ServerUnaryReactor *done) {
    return apply(OP_ADD_QOS, request, response, done);
  }

  void ModifyEntity(const crane::grpc::ModifyEntityRequest *request,
                    crane::grpc::ModifyEntityReply *response,
                    grpc::ServerUnaryReactor *done) {
    return apply(OP_MODIFY_ENTITY, request, response, done);
  }

  void QueryEntityInfo(const crane::grpc::QueryEntityInfoRequest *request,
                       crane::grpc::QueryEntityInfoReply *response) {}

  void DeleteEntity(const crane::grpc::DeleteEntityRequest *request,
                    crane::grpc::DeleteEntityReply *response,
                    grpc::ServerUnaryReactor *done) {
    return apply(OP_DELETE_ENTITY, request, response, done);
  }

  void BlockAccountOrUser(const crane::grpc::BlockAccountOrUserRequest *request,
                          crane::grpc::BlockAccountOrUserReply *response,
                          grpc::ServerUnaryReactor *done) {
    return apply(OP_BLOCK_ACCOUNT_OR_USER, request, response, done);
  }

  void QueryClusterInfo(const crane::grpc::QueryClusterInfoRequest *request,
                        crane::grpc::QueryClusterInfoReply *response) {}

  // Shut this node down.
  void shutdown() {
    if (node_) {
      node_->shutdown(nullptr);
    }
  }

  // Blocking this thread until the node is eventually down.
  void join() {
    if (node_) {
      node_->join();
    }
  }

 private:
  friend class CraneCtldClosure;

  void apply(CraneCtldOpType type, const google::protobuf::Message *request,
             google::protobuf::Message *response,
             grpc::ServerUnaryReactor *done);

  void redirect(google::protobuf::Message *response) {
    //  response->set_success(false);
    if (node_) {
      braft::PeerId leader = node_->leader_id();
      if (!leader.is_empty()) {
        //      response->set_redirect(leader.to_string());
      }
    }
  }

  // @braft::StateMachine
  void on_apply(braft::Iterator &iter) override;

  // empty method
  void on_snapshot_save(braft::SnapshotWriter *writer,
                        braft::Closure *done) override {}

  // empty method
  int on_snapshot_load(braft::SnapshotReader *reader) override { return 0; }

  void on_leader_start(int64_t term) override {
    leader_term_.store(term, butil::memory_order_release);
    CRANE_INFO("Node becomes leader.");
  }

  void on_leader_stop(const butil::Status &status) override {
    leader_term_.store(-1, butil::memory_order_release);
    CRANE_INFO("Node stepped down.");
  }

  void on_shutdown() override { CRANE_INFO("This node is down."); }

  void on_error(const ::braft::Error &e) override {
    LOG(ERROR) << "Met raft error " << e;
  }

  void on_configuration_committed(const ::braft::Configuration &conf) override {
    LOG(INFO) << "Configuration of this group is " << conf;
  }

  void on_stop_following(const ::braft::LeaderChangeContext &ctx) override {
    LOG(INFO) << "Node stops following " << ctx;
  }

  void on_start_following(const ::braft::LeaderChangeContext &ctx) override {
    LOG(INFO) << "Node start following " << ctx;
  }
  // end of @braft::StateMachine

  // 实际rpc执行的地方
  void SubmitBatchTask(const butil::IOBuf &data,
                       const google::protobuf::Message *request,
                       google::protobuf::Message *response);

  void TaskStatusChange(const butil::IOBuf &data,
                        const google::protobuf::Message *request,
                        google::protobuf::Message *response);

  void CranedRegister(const butil::IOBuf &data,
                      const google::protobuf::Message *request,
                      google::protobuf::Message *response);

  void CancelTask(const butil::IOBuf &data,
                  const google::protobuf::Message *request,
                  google::protobuf::Message *response);

  void QueryTasksInfo(const butil::IOBuf &data,
                      const google::protobuf::Message *request,
                      google::protobuf::Message *response);

  void QueryCranedInfo(const butil::IOBuf &data,
                       const google::protobuf::Message *request,
                       google::protobuf::Message *response);

  void QueryPartitionInfo(const butil::IOBuf &data,
                          const google::protobuf::Message *request,
                          google::protobuf::Message *response);

  void ModifyTask(const butil::IOBuf &data,
                  const google::protobuf::Message *request,
                  google::protobuf::Message *response);

  void ModifyNode(const butil::IOBuf &data,
                  const google::protobuf::Message *request,
                  google::protobuf::Message *response);

  void AddAccount(const butil::IOBuf &data,
                  const google::protobuf::Message *request,
                  google::protobuf::Message *response);

  void AddUser(const butil::IOBuf &data,
               const google::protobuf::Message *request,
               google::protobuf::Message *response);

  void AddQos(const butil::IOBuf &data,
              const google::protobuf::Message *request,
              google::protobuf::Message *response);

  void ModifyEntity(const butil::IOBuf &data,
                    const google::protobuf::Message *request,
                    google::protobuf::Message *response);

  void QueryEntityInfo(const butil::IOBuf &data,
                       const google::protobuf::Message *request,
                       google::protobuf::Message *response);

  void DeleteEntity(const butil::IOBuf &data,
                    const google::protobuf::Message *request,
                    google::protobuf::Message *response);

  void BlockAccountOrUser(const butil::IOBuf &data,
                          const google::protobuf::Message *request,
                          google::protobuf::Message *response);

  void QueryClusterInfo(const butil::IOBuf &data,
                        const google::protobuf::Message *request,
                        google::protobuf::Message *response);

  braft::Node *volatile node_;
  butil::atomic<int64_t> leader_term_;
  CtldServer *m_ctld_server_;
};

}  // namespace Ctld
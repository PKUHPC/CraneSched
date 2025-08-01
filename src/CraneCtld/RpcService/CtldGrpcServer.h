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

#include "CtldPublicDefs.h"
// Precompiled header comes first!

#include "crane/Lock.h"
#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"

namespace Ctld {

using crane::grpc::Craned;
using grpc::Channel;
using grpc::Server;

class CtldServer;

class CraneCtldServiceImpl final : public crane::grpc::CraneCtld::Service {
 public:
  explicit CraneCtldServiceImpl(CtldServer *server) : m_ctld_server_(server) {}

  grpc::Status SubmitBatchTask(
      grpc::ServerContext *context,
      const crane::grpc::SubmitBatchTaskRequest *request,
      crane::grpc::SubmitBatchTaskReply *response) override;

  // This gRPC is for testing purposes only
  grpc::Status SubmitBatchTasks(
      grpc::ServerContext *context,
      const crane::grpc::SubmitBatchTasksRequest *request,
      crane::grpc::SubmitBatchTasksReply *response) override;

  grpc::Status CancelTask(grpc::ServerContext *context,
                          const crane::grpc::CancelTaskRequest *request,
                          crane::grpc::CancelTaskReply *response) override;

  grpc::Status QueryTasksInfo(
      grpc::ServerContext *context,
      const crane::grpc::QueryTasksInfoRequest *request,
      crane::grpc::QueryTasksInfoReply *response) override;

  grpc::Status QueryCranedInfo(
      grpc::ServerContext *context,
      const crane::grpc::QueryCranedInfoRequest *request,
      crane::grpc::QueryCranedInfoReply *response) override;

  grpc::Status QueryPartitionInfo(
      grpc::ServerContext *context,
      const crane::grpc::QueryPartitionInfoRequest *request,
      crane::grpc::QueryPartitionInfoReply *response) override;

  grpc::Status QueryReservationInfo(
      grpc::ServerContext *context,
      const crane::grpc::QueryReservationInfoRequest *request,
      crane::grpc::QueryReservationInfoReply *response) override;

  grpc::Status ModifyTask(grpc::ServerContext *context,
                          const crane::grpc::ModifyTaskRequest *request,
                          crane::grpc::ModifyTaskReply *response) override;

  grpc::Status ModifyTasksExtraAttrs(
      grpc::ServerContext *context,
      const crane::grpc::ModifyTasksExtraAttrsRequest *request,
      crane::grpc::ModifyTasksExtraAttrsReply *response) override;

  grpc::Status ModifyNode(
      grpc::ServerContext *context,
      const crane::grpc::ModifyCranedStateRequest *request,
      crane::grpc::ModifyCranedStateReply *response) override;

  grpc::Status ModifyPartitionAcl(
      grpc::ServerContext *context,
      const crane::grpc::ModifyPartitionAclRequest *request,
      crane::grpc::ModifyPartitionAclReply *response) override;

  grpc::Status AddAccount(grpc::ServerContext *context,
                          const crane::grpc::AddAccountRequest *request,
                          crane::grpc::AddAccountReply *response) override;

  grpc::Status AddUser(grpc::ServerContext *context,
                       const crane::grpc::AddUserRequest *request,
                       crane::grpc::AddUserReply *response) override;

  grpc::Status AddQos(grpc::ServerContext *context,
                      const crane::grpc::AddQosRequest *request,
                      crane::grpc::AddQosReply *response) override;

  grpc::Status ModifyAccount(
      grpc::ServerContext *context,
      const crane::grpc::ModifyAccountRequest *request,
      crane::grpc::ModifyAccountReply *response) override;

  grpc::Status ModifyUser(grpc::ServerContext *context,
                          const crane::grpc::ModifyUserRequest *request,
                          crane::grpc::ModifyUserReply *response) override;

  grpc::Status ModifyQos(grpc::ServerContext *context,
                         const crane::grpc::ModifyQosRequest *request,
                         crane::grpc::ModifyQosReply *response) override;

  grpc::Status QueryAccountInfo(
      grpc::ServerContext *context,
      const crane::grpc::QueryAccountInfoRequest *request,
      crane::grpc::QueryAccountInfoReply *response) override;

  grpc::Status QueryUserInfo(
      grpc::ServerContext *context,
      const crane::grpc::QueryUserInfoRequest *request,
      crane::grpc::QueryUserInfoReply *response) override;

  grpc::Status QueryQosInfo(grpc::ServerContext *context,
                            const crane::grpc::QueryQosInfoRequest *request,
                            crane::grpc::QueryQosInfoReply *response) override;

  grpc::Status DeleteAccount(
      grpc::ServerContext *context,
      const crane::grpc::DeleteAccountRequest *request,
      crane::grpc::DeleteAccountReply *response) override;

  grpc::Status DeleteUser(grpc::ServerContext *context,
                          const crane::grpc::DeleteUserRequest *request,
                          crane::grpc::DeleteUserReply *response) override;

  grpc::Status DeleteQos(grpc::ServerContext *context,
                         const crane::grpc::DeleteQosRequest *request,
                         crane::grpc::DeleteQosReply *response) override;

  grpc::Status BlockAccountOrUser(
      grpc::ServerContext *context,
      const crane::grpc::BlockAccountOrUserRequest *request,
      crane::grpc::BlockAccountOrUserReply *response) override;

  grpc::Status QueryClusterInfo(
      grpc::ServerContext *context,
      const crane::grpc::QueryClusterInfoRequest *request,
      crane::grpc::QueryClusterInfoReply *response) override;

  grpc::Status CreateReservation(
      grpc::ServerContext *context,
      const crane::grpc::CreateReservationRequest *request,
      crane::grpc::CreateReservationReply *response) override;

  grpc::Status DeleteReservation(
      grpc::ServerContext *context,
      const crane::grpc::DeleteReservationRequest *request,
      crane::grpc::DeleteReservationReply *response) override;

  grpc::Status PowerStateChange(
      grpc::ServerContext *context,
      const crane::grpc::PowerStateChangeRequest *request,
      crane::grpc::PowerStateChangeReply *response) override;

  grpc::Status EnableAutoPowerControl(
      grpc::ServerContext *context,
      const crane::grpc::EnableAutoPowerControlRequest *request,
      crane::grpc::EnableAutoPowerControlReply *response) override;

 private:
  CtldServer *m_ctld_server_;
};

/***
 * Note: There should be only ONE instance of CtldServer!!!!
 */
class CtldServer {
 public:
  /***
   * User must make sure that this constructor is called only once!
   * @param listen_address The "[Address]:[Port]" of CraneCtld.
   */
  explicit CtldServer(const Config::CraneCtldListenConf &listen_conf);

  void Wait() { m_server_->Wait(); }

 private:
  std::unique_ptr<CraneCtldServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  inline static std::mutex s_signal_cv_mtx_;
  inline static std::condition_variable s_signal_cv_;
  static void signal_handler_func(int) { s_signal_cv_.notify_one(); };

  friend class CraneCtldServiceImpl;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::CtldServer> g_ctld_server;
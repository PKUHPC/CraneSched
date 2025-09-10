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

#include <grpcpp/server_context.h>

#include "CtldPublicDefs.h"
// Precompiled header comes first!

#include "crane/Lock.h"
#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"

namespace Ctld {

using crane::grpc::Craned;
using grpc::Channel;
using grpc::Server;

class CforedStreamWriter {
 private:
  using Mutex = absl::Mutex;
  using LockGuard = absl::MutexLock;

  using StreamCtldReply = crane::grpc::StreamCtldReply;

 public:
  explicit CforedStreamWriter(
      grpc::ServerReaderWriter<crane::grpc::StreamCtldReply,
                               crane::grpc::StreamCforedRequest> *stream)
      : m_stream_(stream), m_valid_(true) {}

  bool WriteTaskIdReply(
      pid_t calloc_pid,
      std::expected<std::pair<job_id_t, step_id_t>, std::string> res) {
    LockGuard guard(&m_stream_mtx_);
    if (!m_valid_) return false;

    StreamCtldReply reply;
    reply.set_type(StreamCtldReply::TASK_ID_REPLY);
    auto *task_id_reply = reply.mutable_payload_task_id_reply();
    if (res.has_value()) {
      task_id_reply->set_ok(true);
      task_id_reply->set_pid(calloc_pid);
      auto [job_id, step_id] = res.value();
      task_id_reply->set_job_id(job_id);
      task_id_reply->set_step_id(step_id);
    } else {
      task_id_reply->set_ok(false);
      task_id_reply->set_pid(calloc_pid);
      task_id_reply->set_failure_reason(std::move(res.error()));
    }

    return m_stream_->Write(reply);
  }

  bool WriteTaskResAllocReply(
      const StepInteractiveMeta::StepResAllocArgs &args) {
    LockGuard guard(&m_stream_mtx_);
    if (!m_valid_) return false;

    StreamCtldReply reply;
    reply.set_type(StreamCtldReply::TASK_RES_ALLOC_REPLY);
    auto &[job_id, step_id, allocated_craned_expt] = args;
    auto *task_res_alloc_reply = reply.mutable_payload_task_res_alloc_reply();
    task_res_alloc_reply->set_job_id(job_id);
    task_res_alloc_reply->set_step_id(step_id);

    if (allocated_craned_expt.has_value()) {
      task_res_alloc_reply->set_ok(true);
      task_res_alloc_reply->set_allocated_craned_regex(
          allocated_craned_expt.value().first);
      auto &craned_list = allocated_craned_expt.value().second;
      task_res_alloc_reply->mutable_craned_ids()->Assign(craned_list.begin(),
                                                         craned_list.end());
    } else {
      task_res_alloc_reply->set_ok(false);
      task_res_alloc_reply->set_failure_reason(
          std::move(allocated_craned_expt.error()));
    }

    return m_stream_->Write(reply);
  }

  bool WriteTaskCompletionAckReply(job_id_t job_id, step_id_t step_id) {
    LockGuard guard(&m_stream_mtx_);
    if (!m_valid_) return false;
    CRANE_TRACE("Sending TaskCompletionAckReply to cfored of task id {}",
                job_id);
    StreamCtldReply reply;
    reply.set_type(StreamCtldReply::TASK_COMPLETION_ACK_REPLY);

    auto *task_completion_ack = reply.mutable_payload_task_completion_ack();
    task_completion_ack->set_job_id(job_id);
    task_completion_ack->set_step_id(step_id);

    return m_stream_->Write(reply);
  }

  bool WriteTaskCancelRequest(job_id_t job_id, step_id_t step_id) {
    LockGuard guard(&m_stream_mtx_);
    if (!m_valid_) return false;

    StreamCtldReply reply;
    reply.set_type(StreamCtldReply::TASK_CANCEL_REQUEST);

    auto *task_cancel_req = reply.mutable_payload_task_cancel_request();
    task_cancel_req->set_job_id(job_id);
    task_cancel_req->set_step_id(step_id);

    return m_stream_->Write(reply);
  }

  bool WriteCforedRegistrationAck(const std::expected<void, std::string> &res) {
    LockGuard guard(&m_stream_mtx_);
    if (!m_valid_) return false;

    StreamCtldReply reply;
    reply.set_type(StreamCtldReply::CFORED_REGISTRATION_ACK);

    auto *cfored_reg_ack = reply.mutable_payload_cfored_reg_ack();
    if (res.has_value()) {
      cfored_reg_ack->set_ok(true);
    } else {
      cfored_reg_ack->set_ok(false);
      cfored_reg_ack->set_failure_reason(res.error());
    }

    return m_stream_->Write(reply);
  }

  bool WriteCforedGracefulExitAck() {
    LockGuard guard(&m_stream_mtx_);
    if (!m_valid_) return false;

    StreamCtldReply reply;
    reply.set_type(StreamCtldReply::CFORED_GRACEFUL_EXIT_ACK);

    auto *cfored_graceful_exit_ack = reply.mutable_payload_graceful_exit_ack();
    cfored_graceful_exit_ack->set_ok(true);

    return m_stream_->Write(reply);
  }

  bool WriteTaskMetaReply(bool ok, const std::string& failure_reason, const crane::grpc::TaskToCtld& task, int32_t pid) {
    LockGuard guard(&m_stream_mtx_);
    if (!m_valid_) return false;

    StreamCtldReply reply;
    reply.set_type(StreamCtldReply::TASK_META_REPLY);
    auto* task_meta_reply = reply.mutable_payload_task_meta_reply();
    task_meta_reply->set_ok(ok);
    task_meta_reply->set_failure_reason(failure_reason);
    task_meta_reply->set_cattach_pid(pid);
    task_meta_reply->mutable_task()->CopyFrom(task);

    return m_stream_->Write(reply);
  }

  void Invalidate() {
    LockGuard guard(&m_stream_mtx_);
    m_valid_ = false;
  }

 private:
  Mutex m_stream_mtx_;

  bool m_valid_;

  grpc::ServerReaderWriter<crane::grpc::StreamCtldReply,
                           crane::grpc::StreamCforedRequest> *m_stream_
      ABSL_GUARDED_BY(m_stream_mtx_);
};

class StreamWriterProxy {
public:
  void SetWriter(std::shared_ptr<CforedStreamWriter> writer) {
    std::lock_guard<std::mutex> lock(mtx_);
    writer_ = std::move(writer);
  }

  std::shared_ptr<CforedStreamWriter> GetWriter() {
    std::lock_guard<std::mutex> lock(mtx_);
    return writer_;
  }

  template<typename Func>
  void WithWriter(Func&& func) {
    std::shared_ptr<CforedStreamWriter> writer = GetWriter();
    if (writer) func(*writer);
  }

private:
  std::mutex mtx_;
  std::shared_ptr<CforedStreamWriter> writer_;
};

class CtldServer;

class CtldForInternalServiceImpl final
    : public crane::grpc::CraneCtldForInternal::Service {
 public:
  explicit CtldForInternalServiceImpl(CtldServer *server)
      : m_ctld_server_(server) {}

  grpc::Status StepStatusChange(
      grpc::ServerContext *context,
      const crane::grpc::StepStatusChangeRequest *request,
      crane::grpc::StepStatusChangeReply *response) override;

  grpc::Status CranedTriggerReverseConn(
      grpc::ServerContext *context,
      const crane::grpc::CranedTriggerReverseConnRequest *request,
      google::protobuf::Empty *response) override;

  grpc::Status CranedRegister(
      grpc::ServerContext *context,
      const crane::grpc::CranedRegisterRequest *request,
      crane::grpc::CranedRegisterReply *response) override;

  grpc::Status CranedPing(grpc::ServerContext *context,
                          const crane::grpc::CranedPingRequest *request,
                          crane::grpc::CranedPingReply *response) override;

  grpc::Status CforedStream(
      grpc::ServerContext *context,
      grpc::ServerReaderWriter<crane::grpc::StreamCtldReply,
                               crane::grpc::StreamCforedRequest> *stream)
      override;

 private:
  CtldServer *m_ctld_server_;
};

class CraneCtldServiceImpl final : public crane::grpc::CraneCtld::Service {
 public:
  explicit CraneCtldServiceImpl(CtldServer *server) : m_ctld_server_(server) {}

  grpc::Status SubmitBatchTask(
      grpc::ServerContext *context,
      const crane::grpc::SubmitBatchTaskRequest *request,
      crane::grpc::SubmitBatchTaskReply *response) override;

  grpc::Status SubmitContainerStep(
      grpc::ServerContext *context,
      const crane::grpc::SubmitContainerStepRequest *request,
      crane::grpc::SubmitContainerStepReply *response) override;

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

  grpc::Status QueryLicensesInfo(
      grpc::ServerContext *context,
      const crane::grpc::QueryLicensesInfoRequest *request,
      crane::grpc::QueryLicensesInfoReply *response) override;

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

  grpc::Status AddWckey(grpc::ServerContext *context,
                        const crane::grpc::AddWckeyRequest *request,
                        crane::grpc::AddWckeyReply *response) override;

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

  grpc::Status ModifyDefaultWckey(
      grpc::ServerContext *context,
      const crane::grpc::ModifyDefaultWckeyRequest *request,
      crane::grpc::ModifyDefaultWckeyReply *response) override;

  grpc::Status QueryAccountInfo(
      grpc::ServerContext *context,
      const crane::grpc::QueryAccountInfoRequest *request,
      crane::grpc::QueryAccountInfoReply *response) override;

  grpc::Status QueryUserInfo(
      grpc::ServerContext *context,
      const crane::grpc::QueryUserInfoRequest *request,
      crane::grpc::QueryUserInfoReply *response) override;

  grpc::Status QueryWckeyInfo(
      grpc::ServerContext *context,
      const crane::grpc::QueryWckeyInfoRequest *request,
      crane::grpc::QueryWckeyInfoReply *response) override;

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

  grpc::Status DeleteWckey(grpc::ServerContext *context,
                           const crane::grpc::DeleteWckeyRequest *request,
                           crane::grpc::DeleteWckeyReply *response) override;

  grpc::Status BlockAccountOrUser(
      grpc::ServerContext *context,
      const crane::grpc::BlockAccountOrUserRequest *request,
      crane::grpc::BlockAccountOrUserReply *response) override;

  grpc::Status ResetUserCredential(
      grpc::ServerContext *context,
      const crane::grpc::ResetUserCredentialRequest *request,
      crane::grpc::ResetUserCredentialReply *response) override;

  grpc::Status QueryTxnLog(grpc::ServerContext *context,
                           const crane::grpc::QueryTxnLogRequest *request,
                           crane::grpc::QueryTxnLogReply *response) override;

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

  grpc::Status SignUserCertificate(
      grpc::ServerContext *context,
      const crane::grpc::SignUserCertificateRequest *request,
      crane::grpc::SignUserCertificateResponse *response) override;

  grpc::Status AttachContainerStep(
      grpc::ServerContext *context,
      const crane::grpc::AttachContainerStepRequest *request,
      crane::grpc::AttachContainerStepReply *response) override;

  grpc::Status ExecInContainerStep(
      grpc::ServerContext *context,
      const crane::grpc::ExecInContainerStepRequest *request,
      crane::grpc::ExecInContainerStepReply *response) override;

 private:
  static std::optional<std::string> CheckCertAndUIDAllowed_(
      const grpc::ServerContext *context, uint32_t uid);

  CtldServer *m_ctld_server_;
};

/***
 * Note: There should be only ONE instance of CtldServer!!!!
 */
class CtldServer {
 public:
  /***
   * User must make sure that this constructor is called only once!
   */
  explicit CtldServer(const Config::CraneCtldListenConf &listen_conf);

  void Wait() { m_server_->Wait(); }

 private:
  template <typename K, typename V,
            typename Hash = absl::container_internal::hash_default_hash<K>>
  using HashMap = absl::flat_hash_map<K, V, Hash>;

  template <typename K,
            typename Hash = absl::container_internal::hash_default_hash<K>>
  using HashSet = absl::flat_hash_set<K, Hash>;

  using Mutex = util::mutex;

  // internal
  Mutex m_mtx_;
  HashMap<std::string /* cfored_name */,
          HashMap<job_id_t, std::unordered_set<step_id_t>>>
      m_cfored_running_tasks_ ABSL_GUARDED_BY(m_mtx_);

  Mutex m_stream_proxy_mtx_;
  HashMap<std::string /* cfored_name */, std::shared_ptr<StreamWriterProxy>>
    m_cfored_stream_proxy_map_ ABSL_GUARDED_BY(m_stream_proxy_mtx_);

  std::unique_ptr<CtldForInternalServiceImpl> m_internal_service_impl_;
  std::unique_ptr<Server> m_internal_server_;

  friend class CtldForInternalServiceImpl;

  // external
  std::unique_ptr<CraneCtldServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  inline static std::mutex s_signal_cv_mtx_;
  inline static std::condition_variable s_signal_cv_;
  static void signal_handler_func(int) { s_signal_cv_.notify_one(); };

  friend class CraneCtldServiceImpl;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::CtldServer> g_ctld_server;

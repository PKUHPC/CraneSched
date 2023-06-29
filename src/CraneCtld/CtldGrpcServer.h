#pragma once

#include "CtldPublicDefs.h"
// Precompiled header comes first!

#include "crane/Lock.h"
#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"

namespace Ctld {

using boost::uuids::uuid;
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

  bool WriteTaskIdReply(pid_t calloc_pid,
                        result::result<task_id_t, std::string> res) {
    LockGuard guard(&m_stream_mtx_);
    if (!m_valid_) return false;

    StreamCtldReply reply;
    reply.set_type(StreamCtldReply::TASK_ID_REPLY);
    auto *task_id_reply = reply.mutable_payload_task_id_reply();
    if (res.has_value()) {
      task_id_reply->set_ok(true);
      task_id_reply->set_pid(calloc_pid);
      task_id_reply->set_task_id(res.value());
    } else {
      task_id_reply->set_ok(false);
      task_id_reply->set_pid(calloc_pid);
      task_id_reply->set_failure_reason(std::move(res.error()));
    }

    return m_stream_->Write(reply);
  }

  bool WriteTaskResAllocReply(task_id_t task_id,
                              result::result<std::string, std::string> res) {
    LockGuard guard(&m_stream_mtx_);
    if (!m_valid_) return false;

    StreamCtldReply reply;
    reply.set_type(StreamCtldReply::TASK_RES_ALLOC_REPLY);
    auto *task_res_alloc_reply = reply.mutable_payload_task_res_alloc_reply();
    task_res_alloc_reply->set_task_id(task_id);

    if (res.has_value()) {
      task_res_alloc_reply->set_ok(true);
      task_res_alloc_reply->set_allocated_craned_regex(std::move(res.value()));
    } else {
      task_res_alloc_reply->set_ok(false);
      task_res_alloc_reply->set_failure_reason(std::move(res.error()));
    }

    return m_stream_->Write(reply);
  }

  bool WriteTaskCompletionAckReply(task_id_t task_id) {
    LockGuard guard(&m_stream_mtx_);
    if (!m_valid_) return false;

    StreamCtldReply reply;
    reply.set_type(StreamCtldReply::TASK_COMPLETION_ACK_REPLY);

    auto *task_completion_ack = reply.mutable_payload_task_completion_ack();
    task_completion_ack->set_task_id(task_id);

    return m_stream_->Write(reply);
  }

  bool WriteTaskCancelRequest(task_id_t task_id) {
    LockGuard guard(&m_stream_mtx_);
    if (!m_valid_) return false;

    StreamCtldReply reply;
    reply.set_type(StreamCtldReply::TASK_CANCEL_REQUEST);

    auto *task_cancel_req = reply.mutable_payload_task_cancel_request();
    task_cancel_req->set_task_id(task_id);

    return m_stream_->Write(reply);
  }

  bool WriteCforedRegistrationAck(result::result<void, std::string> res) {
    LockGuard guard(&m_stream_mtx_);
    if (!m_valid_) return false;

    StreamCtldReply reply;
    reply.set_type(StreamCtldReply::CFORED_REGISTRATION_ACK);

    auto *cfored_reg_ack = reply.mutable_payload_cfored_reg_ack();
    if (res.has_value()) {
      cfored_reg_ack->set_ok(true);
    } else {
      cfored_reg_ack->set_ok(false);
      cfored_reg_ack->set_failure_reason(std::move(res.error()));
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

  void Invalidate() {
    LockGuard guard(&m_stream_mtx_);
    m_valid_ = false;
  }

 private:
  Mutex m_stream_mtx_;

  bool m_valid_;

  grpc::ServerReaderWriter<crane::grpc::StreamCtldReply,
                           crane::grpc::StreamCforedRequest> *m_stream_
      GUARDED_BY(m_stream_mtx_);
};

class CtldServer;

class CraneCtldServiceImpl final : public crane::grpc::CraneCtld::Service {
 public:
  explicit CraneCtldServiceImpl(CtldServer *server) : m_ctld_server_(server) {}

  grpc::Status CforedStream(
      grpc::ServerContext *context,
      grpc::ServerReaderWriter<crane::grpc::StreamCtldReply,
                               crane::grpc::StreamCforedRequest> *stream)
      override;

  grpc::Status SubmitBatchTask(
      grpc::ServerContext *context,
      const crane::grpc::SubmitBatchTaskRequest *request,
      crane::grpc::SubmitBatchTaskReply *response) override;

  grpc::Status SubmitBatchTasks(
      grpc::ServerContext *context,
      const crane::grpc::SubmitBatchTasksRequest *request,
      crane::grpc::SubmitBatchTasksReply *response) override;

  grpc::Status TaskStatusChange(
      grpc::ServerContext *context,
      const crane::grpc::TaskStatusChangeRequest *request,
      crane::grpc::TaskStatusChangeReply *response) override;

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

  grpc::Status ModifyTask(grpc::ServerContext *context,
                          const crane::grpc::ModifyTaskRequest *request,
                          crane::grpc::ModifyTaskReply *response) override;

  grpc::Status AddAccount(grpc::ServerContext *context,
                          const crane::grpc::AddAccountRequest *request,
                          crane::grpc::AddAccountReply *response) override;

  grpc::Status AddUser(grpc::ServerContext *context,
                       const crane::grpc::AddUserRequest *request,
                       crane::grpc::AddUserReply *response) override;

  grpc::Status AddQos(grpc::ServerContext *context,
                      const crane::grpc::AddQosRequest *request,
                      crane::grpc::AddQosReply *response) override;

  grpc::Status ModifyEntity(grpc::ServerContext *context,
                            const crane::grpc::ModifyEntityRequest *request,
                            crane::grpc::ModifyEntityReply *response) override;

  grpc::Status QueryEntityInfo(
      grpc::ServerContext *context,
      const crane::grpc::QueryEntityInfoRequest *request,
      crane::grpc::QueryEntityInfoReply *response) override;

  grpc::Status DeleteEntity(grpc::ServerContext *context,
                            const crane::grpc::DeleteEntityRequest *request,
                            crane::grpc::DeleteEntityReply *response) override;
  grpc::Status BlockAccountOrUser(
      grpc::ServerContext *context,
      const crane::grpc::BlockAccountOrUserRequest *request,
      crane::grpc::BlockAccountOrUserReply *response) override;

  grpc::Status QueryClusterInfo(
      grpc::ServerContext *context,
      const crane::grpc::QueryClusterInfoRequest *request,
      crane::grpc::QueryClusterInfoReply *response) override;

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

  inline void Wait() { m_server_->Wait(); }

  result::result<task_id_t, std::string> SubmitTaskToScheduler(
      std::unique_ptr<TaskInCtld> task);

 private:
  template <typename K, typename V,
            typename Hash = absl::container_internal::hash_default_hash<K>>
  using HashMap = absl::flat_hash_map<K, V, Hash>;

  template <typename K,
            typename Hash = absl::container_internal::hash_default_hash<K>>
  using HashSet = absl::flat_hash_set<K, Hash>;

  using Mutex = util::mutex;
  using LockGuard = util::AbslMutexLockGuard;

  std::unique_ptr<CraneCtldServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  Mutex m_mtx_;
  HashMap<std::string /* cfored_name */, HashSet<task_id_t>>
      m_cfored_running_tasks_ GUARDED_BY(m_mtx_);

  inline static std::mutex s_sigint_mtx;
  inline static std::condition_variable s_sigint_cv;
  static void signal_handler_func(int) { s_sigint_cv.notify_one(); };

  friend class CraneCtldServiceImpl;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::CtldServer> g_ctld_server;
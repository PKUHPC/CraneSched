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
                        std::expected<task_id_t, std::string> res) {
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

  bool WriteTaskResAllocReply(
      task_id_t task_id,
      std::expected<std::pair<std::string, std::list<CranedId>>, std::string>
          res) {
    LockGuard guard(&m_stream_mtx_);
    if (!m_valid_) return false;

    StreamCtldReply reply;
    reply.set_type(StreamCtldReply::TASK_RES_ALLOC_REPLY);
    auto *task_res_alloc_reply = reply.mutable_payload_task_res_alloc_reply();
    task_res_alloc_reply->set_task_id(task_id);

    if (res.has_value()) {
      task_res_alloc_reply->set_ok(true);
      task_res_alloc_reply->set_allocated_craned_regex(
          std::move(res.value().first));
      std::ranges::for_each(res.value().second,
                            [&task_res_alloc_reply](const auto &craned_id) {
                              task_res_alloc_reply->add_craned_ids(craned_id);
                            });
    } else {
      task_res_alloc_reply->set_ok(false);
      task_res_alloc_reply->set_failure_reason(std::move(res.error()));
    }

    return m_stream_->Write(reply);
  }

  bool WriteTaskCompletionAckReply(task_id_t task_id) {
    LockGuard guard(&m_stream_mtx_);
    if (!m_valid_) return false;
    CRANE_TRACE("Sending TaskCompletionAckReply to cfored of task id {}",
                task_id);
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
      ABSL_GUARDED_BY(m_stream_mtx_);
};

class CtldForInternalServer;

class CtldForInternalServiceImpl final
    : public crane::grpc::CraneCtldForInternal::Service {
 public:
  explicit CtldForInternalServiceImpl(CtldForInternalServer *server)
      : m_ctld_for_internal_server_(server) {}

  grpc::Status TaskStatusChange(
      grpc::ServerContext *context,
      const crane::grpc::TaskStatusChangeRequest *request,
      crane::grpc::TaskStatusChangeReply *response) override;

  grpc::Status CranedTriggerReverseConn(
      grpc::ServerContext *context,
      const crane::grpc::CranedTriggerReverseConnRequest *request,
      google::protobuf::Empty *response) override;

  grpc::Status CranedRegister(
      grpc::ServerContext *context,
      const crane::grpc::CranedRegisterRequest *request,
      crane::grpc::CranedRegisterReply *response) override;

  grpc::Status CforedStream(
      grpc::ServerContext *context,
      grpc::ServerReaderWriter<crane::grpc::StreamCtldReply,
                               crane::grpc::StreamCforedRequest> *stream)
      override;

 private:
  CtldForInternalServer *m_ctld_for_internal_server_;
};

class CtldForInternalServer {
 public:
  explicit CtldForInternalServer(
      const Config::CraneCtldListenConf &listen_conf);

  inline void Wait() { m_server_->Wait(); }

  void Shutdown();

 private:
  template <typename K, typename V,
            typename Hash = absl::container_internal::hash_default_hash<K>>
  using HashMap = absl::flat_hash_map<K, V, Hash>;

  template <typename K,
            typename Hash = absl::container_internal::hash_default_hash<K>>
  using HashSet = absl::flat_hash_set<K, Hash>;

  using Mutex = util::mutex;

  std::unique_ptr<CtldForInternalServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  Mutex m_mtx_;
  HashMap<std::string /* cfored_name */, HashSet<task_id_t>>
      m_cfored_running_tasks_ ABSL_GUARDED_BY(m_mtx_);

  friend class CtldForInternalServiceImpl;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::CtldForInternalServer> g_ctld_for_internal_server;
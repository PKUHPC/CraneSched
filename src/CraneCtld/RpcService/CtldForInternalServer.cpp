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

#include "CtldForInternalServer.h"

#include "CranedKeeper.h"
#include "CranedMetaContainer.h"
#include "TaskScheduler.h"

namespace Ctld {
grpc::Status CtldForInternalServiceImpl::TaskStatusChange(
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

grpc::Status CtldForInternalServiceImpl::CranedRegister(
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

grpc::Status CtldForInternalServiceImpl::CforedStream(
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
        }

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
            CRANE_TRACE("Sending TaskCancelRequest in task_cancel", task_id);
            if (auto writer = writer_weak_ptr.lock(); writer)
              writer->WriteTaskCancelRequest(task_id);
          };

          meta.cb_task_completed = [this, cfored_name, writer_weak_ptr](
                                       task_id_t task_id,
                                       bool send_completion_ack) {
            CRANE_TRACE("The completion callback of task #{} has been called.",
                        task_id);
            if (auto writer = writer_weak_ptr.lock();
                writer && send_completion_ack) {
              writer->WriteTaskCompletionAckReply(task_id);
            } else {
              CRANE_ERROR(
                  "Stream writer of ia task #{} has been destroyed. "
                  "TaskCompletionAckReply will not be sent.",
                  task_id);
            }

            m_ctld_for_internal_server_->m_mtx_.Lock();

            // If cfored disconnected, the cfored_name should have be
            // removed from the map and the task completion callback is
            // generated from cleaning the remaining tasks by calling
            // g_task_scheduler->TerminateTask(), we should ignore this
            // callback since the task id has already been cleaned.
            auto iter =
                m_ctld_for_internal_server_->m_cfored_running_tasks_.find(cfored_name);
            if (iter != m_ctld_for_internal_server_->m_cfored_running_tasks_.end())
              iter->second.erase(task_id);
            m_ctld_for_internal_server_->m_mtx_.Unlock();
          };

          auto submit_result =
              g_task_scheduler->SubmitTaskToScheduler(std::move(task));
          std::expected<task_id_t, std::string> result;
          if (submit_result.has_value()) {
            result = std::expected<task_id_t, std::string>{
                submit_result.value().get()};
          } else {
            result = std::unexpected(CraneErrStr(submit_result.error()));
          }
          ok = stream_writer->WriteTaskIdReply(payload.pid(), result);

          if (!ok) {
            CRANE_ERROR(
                "Failed to send msg to cfored {}. Connection is broken. "
                "Exiting...",
                cfored_name);
            state = StreamState::kCleanData;
          } else {
            if (result.has_value()) {
              m_ctld_for_internal_server_->m_mtx_.Lock();
              m_ctld_for_internal_server_->m_cfored_running_tasks_[cfored_name].emplace(
                  result.value());
              m_ctld_for_internal_server_->m_mtx_.Unlock();
            }
          }
        } break;

        case StreamCforedRequest::TASK_COMPLETION_REQUEST: {
          auto const &payload = cfored_request.payload_task_complete_req();
          CRANE_TRACE("Recv TaskCompletionReq of Task #{}", payload.task_id());
          if (g_task_scheduler->TerminatePendingOrRunningIaTask(
                  payload.task_id()) != CraneErrCode::SUCCESS)
            stream_writer->WriteTaskCompletionAckReply(payload.task_id());
          else {
            CRANE_TRACE(
                "Termination of task #{} succeeded. "
                "Leave TaskCompletionAck to TaskStatusChange.",
                payload.task_id());
          }
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
      m_ctld_for_internal_server_->m_mtx_.Lock();

      auto const &running_task_set =
          m_ctld_for_internal_server_->m_cfored_running_tasks_[cfored_name];
      std::vector<task_id_t> running_tasks(running_task_set.begin(),
                                           running_task_set.end());
      m_ctld_for_internal_server_->m_cfored_running_tasks_.erase(cfored_name);
      m_ctld_for_internal_server_->m_mtx_.Unlock();

      for (task_id_t task_id : running_tasks) {
        g_task_scheduler->TerminateRunningTask(task_id);
      }

      return Status::OK;
    }
    }
  }
}

void CtldForInternalServer::Shutdown() {
  m_server_->Shutdown(std::chrono::system_clock::now() +
                   std::chrono::seconds(1));
}

CtldForInternalServer::CtldForInternalServer(const Config::CraneCtldListenConf &listen_conf) {

  m_service_impl_ = std::make_unique<CtldForInternalServiceImpl>(this);

  grpc::ServerBuilder builder;

  if (g_config.CompressedRpc) ServerBuilderSetCompression(&builder);

  std::string cranectld_listen_addr = listen_conf.CraneCtldListenAddr;
  if (listen_conf.UseTls) {
    ServerBuilderAddTcpTlsListeningPort(&builder, cranectld_listen_addr,
                                        listen_conf.CraneCtldForInternalListenPort,
                                        listen_conf.Certs);
  } else {
    ServerBuilderAddTcpInsecureListeningPort(&builder, cranectld_listen_addr,
                                             listen_conf.CraneCtldForInternalListenPort);
  }

  builder.RegisterService(m_service_impl_.get());

  m_server_ = builder.BuildAndStart();
  if (!m_server_) {
    CRANE_ERROR("Cannot start CraneCtldForInternal server!");
    std::exit(1);
  }

  CRANE_INFO("CraneCtldForInternal is listening on {}:{} and Tls is {}",
             cranectld_listen_addr, listen_conf.CraneCtldForInternalListenPort,
             listen_conf.UseTls);

}

} // namespace ctld

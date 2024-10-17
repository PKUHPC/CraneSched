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

#include "CtldForCforedServer.h"

#include <google/protobuf/util/time_util.h>
#include <grpcpp/server_builder.h>
#include <memory>

#include "AccountManager.h"
#include "CranedKeeper.h"
#include "CranedMetaContainer.h"
#include "CtldGrpcServer.h"
#include "EmbeddedDbClient.h"
#include "TaskScheduler.h"
#include "crane/String.h"

namespace Ctld {

grpc::Status CtldForCforedServiceImpl::CforedStream(
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

          meta.cb_task_completed = [this, i_type, cfored_name,
                                    writer_weak_ptr](task_id_t task_id) {
            CRANE_TRACE("Sending TaskCompletionAckReply in task_completed",
                        task_id);
            if (auto writer = writer_weak_ptr.lock(); writer)
              writer->WriteTaskCompletionAckReply(task_id);
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
          ok = stream_writer->WriteTaskIdReply(payload.pid(), result);

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
            stream_writer->WriteTaskCompletionAckReply(payload.task_id());
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

CtldForCforedServer::CtldForCforedServer(const Config::CraneCtldListenConf &listen_conf) {
  m_service_impl_ = std::make_unique<CtldForCforedServiceImpl>(this);

  grpc::ServerBuilder builder;

  if (g_config.CompressedRpc) ServerBuilderSetCompression(&builder);

  if (listen_conf.UseTls) 
    ServerBuilderAddmTcpTlsListeningPort(
        &builder, listen_conf.CraneCtldListenAddr,
        listen_conf.CraneCtldForCforedListenPort, listen_conf.TlsCerts.InternalCerts, 
        listen_conf.TlsCerts.InternalCaContent);
  else 
    ServerBuilderAddTcpInsecureListeningPort(&builder,
                                             listen_conf.CraneCtldListenAddr,
                                             listen_conf.CraneCtldForCforedListenPort);

  builder.RegisterService(m_service_impl_.get());
  m_server_ = builder.BuildAndStart();
  if (!m_server_) {
    CRANE_ERROR("Cannot start gRPC server!");
    std::exit(1);
  }
  CRANE_INFO("CraneCtld For Cfored Server is listening on {}:{} and Tls is {}",
             listen_conf.CraneCtldListenAddr, listen_conf.CraneCtldForCforedListenPort,
             listen_conf.UseTls);
}

result::result<std::future<task_id_t>, std::string>
CtldForCforedServer::SubmitTaskToScheduler(std::unique_ptr<TaskInCtld> task) {
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

} // namespace Ctld
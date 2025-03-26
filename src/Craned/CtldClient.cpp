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

#include "CtldClient.h"

namespace Craned {

CtldClient::~CtldClient() {
  m_thread_stop_ = true;

  CRANE_TRACE("CtldClient is ending. Waiting for the thread to finish.");
  if (m_async_send_thread_.joinable()) m_async_send_thread_.join();
}

void CtldClient::AddCtldChannelAndStub(const std::string& server_address,
                                       const std::string& listen_port) {
  grpc::ChannelArguments channel_args;

  if (g_config.CompressedRpc)
    channel_args.SetCompressionAlgorithm(GRPC_COMPRESS_GZIP);

  std::shared_ptr<Channel> channel;

  if (g_config.ListenConf.UseTls)
    channel = CreateTcpTlsCustomChannelByHostname(server_address, listen_port,
                                                  g_config.ListenConf.TlsCerts,
                                                  channel_args);
  else
    channel = CreateTcpInsecureCustomChannel(server_address, listen_port,
                                             channel_args);

  m_ctld_channels_.emplace_back(channel);

  // std::unique_ptr will automatically release the dangling stub.
  m_stubs_.emplace_back(CraneCtld::NewStub(channel));
}

void CtldClient::InitSendThread() {
  m_async_send_thread_ = std::thread([this] { AsyncSendThread_(); });
}

int CtldClient::OnCraneCtldConnected(int server_id) {
  crane::grpc::CranedRegisterRequest request;
  grpc::Status status;

  assert(server_id >= 0 && server_id < m_stubs_.size());

  CRANE_INFO("Send a register RPC to cranectld, server id: {}", server_id);
  request.set_craned_id(m_craned_id_);

  int retry_time = 10;

  do {
    crane::grpc::CranedRegisterReply reply;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(1));

    status = m_stubs_[server_id]->CranedRegister(&context, request, &reply);
    if (!status.ok()) {
      CRANE_ERROR(
          "NodeActiveConnect RPC returned with status not ok: {}, Resend it, "
          "server id: {}",
          status.error_message(), server_id);
    } else {
      if (reply.ok()) {
        if (reply.already_registered()) {
          CRANE_INFO("This craned has already registered, server id: {}",
                     server_id);
          return reply.cur_leader_id();
        } else {
          CRANE_INFO(
              "This craned has not registered to server #{}, "
              "Sending Register request...",
              server_id);
          std::this_thread::sleep_for(std::chrono::seconds(3));
        }
      } else {
        CRANE_ERROR("This Craned is not allow to register, server id: {}",
                    server_id);
        return reply.cur_leader_id();
      }
    }
  } while (!m_thread_stop_ && retry_time--);

  CRANE_ERROR("Failed to register to server #{} actively.", server_id);
  return -2;
}

void CtldClient::TaskStatusChangeAsync(
    TaskStatusChangeQueueElem&& task_status_change) {
  absl::MutexLock lock(&m_task_status_change_mtx_);
  m_task_status_change_list_.emplace_back(std::move(task_status_change));
}

bool CtldClient::CancelTaskStatusChangeByTaskId(
    task_id_t task_id, crane::grpc::TaskStatus* new_status) {
  absl::MutexLock lock(&m_task_status_change_mtx_);

  size_t num_removed{0};

  for (auto it = m_task_status_change_list_.begin();
       it != m_task_status_change_list_.end();)
    if (it->task_id == task_id) {
      num_removed++;
      *new_status = it->new_status;
      it = m_task_status_change_list_.erase(it);
    } else
      ++it;

  CRANE_ASSERT_MSG(num_removed <= 1,
                   "TaskStatusChange should happen at most once "
                   "for a single running task!");

  return num_removed >= 1;
}

void CtldClient::AsyncSendThread_() {
  m_start_connecting_notification_.WaitForNotification();

  std::this_thread::sleep_for(std::chrono::seconds(1));

  absl::Condition cond(
      +[](decltype(m_task_status_change_list_)* queue) {
        return !queue->empty();
      },
      &m_task_status_change_list_);

  while (true) {
    if (m_thread_stop_) break;

    int cur_leader_id = ConnectToServersAndFindLeader_(m_cur_leader_id_);

    if (cur_leader_id < 0) {
      CRANE_INFO(
          "All channels to CraneCtlD are not connected. Reconnecting...");
      std::this_thread::sleep_for(std::chrono::seconds(10));
      continue;
    } else {
      m_cur_leader_id_ = cur_leader_id;
    }

    bool has_msg = m_task_status_change_mtx_.LockWhenWithTimeout(
        cond, absl::Milliseconds(50));
    if (!has_msg) {
      m_task_status_change_mtx_.Unlock();
      continue;
    }

    std::list<TaskStatusChangeQueueElem> changes;
    changes.splice(changes.begin(), std::move(m_task_status_change_list_));
    m_task_status_change_mtx_.Unlock();

    while (!changes.empty()) {
      grpc::ClientContext context;
      crane::grpc::TaskStatusChangeRequest request;
      crane::grpc::TaskStatusChangeReply reply;
      grpc::Status status;

      auto status_change = changes.front();

      CRANE_TRACE("Sending TaskStatusChange for task #{}",
                  status_change.task_id);

      request.set_craned_id(m_craned_id_);
      request.set_task_id(status_change.task_id);
      request.set_new_status(status_change.new_status);
      request.set_exit_code(status_change.exit_code);
      if (status_change.reason.has_value())
        request.set_reason(status_change.reason.value());

      status = m_stubs_[m_cur_leader_id_]->TaskStatusChange(&context, request,
                                                            &reply);
      if (!status.ok()) {
        CRANE_ERROR(
            "Failed to send TaskStatusChange: "
            "{{TaskId: {}, NewStatus: {}}}, reason: {} | {}, code: {}",
            status_change.task_id, int(status_change.new_status),
            status.error_message(), context.debug_error_string(),
            int(status.error_code()));

        if (status.error_code() == grpc::UNAVAILABLE) {
          // If some messages are not sent due to channel failure,
          // put them back into m_task_status_change_list_
          if (!changes.empty()) {
            m_task_status_change_mtx_.Lock();
            m_task_status_change_list_.splice(
                m_task_status_change_list_.begin(), std::move(changes));
            m_task_status_change_mtx_.Unlock();
          }
          break;
        } else
          changes.pop_front();
      } else {
        CRANE_TRACE(
            "TaskStatusChange for task #{} sent to server #{}. reply.ok={}",
            status_change.task_id, m_cur_leader_id_, reply.ok());
        if (!reply.ok()) {
          // try again
          if (reply.cur_leader_id() != -2) {
            m_cur_leader_id_ = reply.cur_leader_id();
            CRANE_TRACE(
                "Leader id was changed to %d, try again in a little while.",
                reply.cur_leader_id());
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(500));
        } else {
          changes.pop_front();
        }
      }
    }
  }
}

int CtldClient::ConnectToServersAndFindLeader_(int prev_leader_id) {
  int len = m_ctld_channels_.size();
  static std::vector<bool> prev_conn_states(len, false);

  bool connected = m_ctld_channels_[prev_leader_id]->WaitForConnected(
      std::chrono::system_clock::now() + std::chrono::seconds(3));

  if (connected) {
    if (!prev_conn_states[prev_leader_id]) {
      prev_conn_states[prev_leader_id] = true;
      int check_id = g_ctld_client->OnCraneCtldConnected(prev_leader_id);

      if (check_id == prev_leader_id || check_id == -2)
        return check_id;
      else if (check_id == -1) {
        CRANE_ERROR("Raft Server Error! leader id -1");
        m_cur_leader_id_ = 0;
        return -1;
      } else {
        m_cur_leader_id_ = check_id;
        return -2;
      }
    } else {
      prev_conn_states[prev_leader_id] = true;
      return prev_leader_id;
    }
  } else {  // start query
    prev_conn_states[prev_leader_id] = false;

    for (int i = 1; i < len; i++) {
      int id = (prev_leader_id + i) % len;

      connected = m_ctld_channels_[id]->WaitForConnected(
          std::chrono::system_clock::now() + std::chrono::seconds(3));

      if (connected) {
        if (!prev_conn_states[id]) {
          prev_conn_states[id] = true;

          int leader_id = g_ctld_client->OnCraneCtldConnected(id);
          if (leader_id == id || leader_id == -2)
            return leader_id;
          else if (leader_id == -1) {
            CRANE_ERROR("Raft Server Error! leader id -1");
            m_cur_leader_id_ = 0;
            return -1;
          } else {
            m_cur_leader_id_ = leader_id;
            return -2;
          }
        }
      } else {
        prev_conn_states[id] = false;
      }
    }
    return -2;
  }
}

}  // namespace Craned

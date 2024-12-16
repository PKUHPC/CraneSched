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

#include "CranedPublicDefs.h"
// Precompiled header comes first.

#include "protos/Crane.grpc.pb.h"

namespace Craned {

using crane::grpc::CraneCtld;
using crane::grpc::CranedRegisterReply;
using crane::grpc::CranedRegisterRequest;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

class CtldClient {
 public:
  CtldClient() = default;

  ~CtldClient();

  void SetCranedId(CranedId const& craned_id) { m_craned_id_ = craned_id; }

  /***
   * InitChannelAndStub the CtldClient to CraneCtld.
   * @param server_address The "[Address]:[Port]" of CraneCtld.
   * @return
   * If CraneCtld is successfully connected, kOk is returned. <br>
   * If CraneCtld cannot be connected within 3s, kConnectionTimeout is
   * returned.
   */
  void InitChannelAndStub(const std::string& server_address);

  void OnCraneCtldConnected();

  void TaskStatusChangeAsync(TaskStatusChangeQueueElem&& task_status_change);

  bool CancelTaskStatusChangeByTaskId(task_id_t task_id,
                                      crane::grpc::TaskStatus* new_status);

  [[nodiscard]] CranedId GetCranedId() const { return m_craned_id_; };

  inline void StartConnectingCtld() {
    m_start_connecting_notification_.Notify();
  }

 private:
  void AsyncSendThread_();

  absl::Mutex m_task_status_change_mtx_;

  std::list<TaskStatusChangeQueueElem> m_task_status_change_list_
      ABSL_GUARDED_BY(m_task_status_change_mtx_);

  std::thread m_async_send_thread_;
  std::atomic_bool m_thread_stop_{false};

  std::shared_ptr<Channel> m_ctld_channel_;

  std::unique_ptr<CraneCtld::Stub> m_stub_;

  CranedId m_craned_id_;

  absl::Notification m_start_connecting_notification_;
};

}  // namespace Craned

inline std::unique_ptr<Craned::CtldClient> g_ctld_client;
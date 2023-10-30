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

#pragma once

#include "CranedPublicDefs.h"
// Precompiled header comes first.

#include "crane/PublicHeader.h"
#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"

namespace Craned {

using crane::grpc::CraneCtld;
using crane::grpc::CranedRegisterRequest;
using crane::grpc::CranedRegisterResult;
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

  void CraneCtldConnected();

  void TaskStatusChangeAsync(TaskStatusChange&& task_status_change);

  bool CancelTaskStatusChangeByTaskId(task_id_t task_id,
                                      crane::grpc::TaskStatus* new_status);

  [[nodiscard]] CranedId GetCranedId() const { return m_craned_id_; };

  inline void notify_handle() { s_sigint_cv.notify_one(); }

 private:
  void AsyncSendThread_();

  absl::Mutex m_task_status_change_mtx_;

  std::list<TaskStatusChange> m_task_status_change_list_
      GUARDED_BY(m_task_status_change_mtx_);

  std::thread m_async_send_thread_;
  std::atomic_bool m_thread_stop_{false};

  std::shared_ptr<Channel> m_ctld_channel_;

  std::unique_ptr<CraneCtld::Stub> m_stub_;

  CranedId m_craned_id_;

  std::mutex s_sigint_mtx;
  std::condition_variable s_sigint_cv;
};

}  // namespace Craned

inline std::unique_ptr<Craned::CtldClient> g_ctld_client;
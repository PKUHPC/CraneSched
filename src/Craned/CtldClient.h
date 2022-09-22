#pragma once

#include <absl/base/thread_annotations.h>
#include <absl/synchronization/mutex.h>
#include <grpc++/grpc++.h>

#include <atomic>
#include <boost/uuid/uuid.hpp>
#include <chrono>
#include <memory>
#include <queue>
#include <thread>

#include "CranedPublicDefs.h"
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
  CtldClient();

  ~CtldClient();

  void SetNodeId(CranedId node_id) { m_craned_id_ = node_id; }

  /***
   * InitChannelAndStub the CtldClient to CraneCtld.
   * @param server_address The "[Address]:[Port]" of CraneCtld.
   * @return
   * If CraneCtld is successfully connected, kOk is returned. <br>
   * If CraneCtld cannot be connected within 3s, kConnectionTimeout is
   * returned.
   */
  void InitChannelAndStub(const std::string& server_address);

  void TaskStatusChangeAsync(TaskStatusChange&& task_status_change);

  [[nodiscard]] CranedId GetNodeId() const { return m_craned_id_; };

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
};

}  // namespace Craned

inline std::unique_ptr<Craned::CtldClient> g_ctld_client;
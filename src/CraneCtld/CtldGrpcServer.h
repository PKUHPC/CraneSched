#pragma once

#include <absl/container/node_hash_map.h>
#include <grpc++/grpc++.h>

#include <boost/algorithm/string.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#if Boost_MINOR_VERSION >= 71
#include <boost/uuid/uuid_hash.hpp>
#endif

#include "CtldPublicDefs.h"
#include "crane/Lock.h"
#include "crane/PublicHeader.h"
#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"

namespace Ctld {

using boost::uuids::uuid;
using crane::grpc::Craned;
using grpc::Channel;
using grpc::Server;

class CtldServer;

class CraneCtldServiceImpl final : public crane::grpc::CraneCtld::Service {
 public:
  explicit CraneCtldServiceImpl(CtldServer *server) : m_ctld_server_(server) {}

  grpc::Status AllocateInteractiveTask(
      grpc::ServerContext *context,
      const crane::grpc::InteractiveTaskAllocRequest *request,
      crane::grpc::InteractiveTaskAllocReply *response) override;

  grpc::Status QueryInteractiveTaskAllocDetail(
      grpc::ServerContext *context,
      const crane::grpc::QueryInteractiveTaskAllocDetailRequest *request,
      crane::grpc::QueryInteractiveTaskAllocDetailReply *response) override;

  grpc::Status SubmitBatchTask(
      grpc::ServerContext *context,
      const crane::grpc::SubmitBatchTaskRequest *request,
      crane::grpc::SubmitBatchTaskReply *response) override;

  grpc::Status TaskStatusChange(
      grpc::ServerContext *context,
      const crane::grpc::TaskStatusChangeRequest *request,
      crane::grpc::TaskStatusChangeReply *response) override;

  grpc::Status QueryCranedListFromTaskId(
      grpc::ServerContext *context,
      const crane::grpc::QueryCranedListFromTaskIdRequest *request,
      crane::grpc::QueryCranedListFromTaskIdReply *response) override;

  grpc::Status CancelTask(grpc::ServerContext *context,
                          const crane::grpc::CancelTaskRequest *request,
                          crane::grpc::CancelTaskReply *response) override;

  grpc::Status QueryJobsInPartition(
      grpc::ServerContext *context,
      const crane::grpc::QueryJobsInPartitionRequest *request,
      crane::grpc::QueryJobsInPartitionReply *response) override;

  grpc::Status QueryJobsInfo(
      grpc::ServerContext *context,
      const crane::grpc::QueryJobsInfoRequest *request,
      crane::grpc::QueryJobsInfoReply *response) override;

  grpc::Status QueryCranedInfo(
      grpc::ServerContext *context,
      const crane::grpc::QueryCranedInfoRequest *request,
      crane::grpc::QueryCranedInfoReply *response) override;

  grpc::Status QueryPartitionInfo(
      grpc::ServerContext *context,
      const crane::grpc::QueryPartitionInfoRequest *request,
      crane::grpc::QueryPartitionInfoReply *response) override;

  grpc::Status AddAccount(grpc::ServerContext *context,
                          const crane::grpc::AddAccountRequest *request,
                          crane::grpc::AddAccountReply *response) override;

  grpc::Status AddUser(grpc::ServerContext *context,
                       const crane::grpc::AddUserRequest *request,
                       crane::grpc::AddUserReply *response) override;

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

  void AddAllocDetailToIaTask(uint32_t task_id,
                              InteractiveTaskAllocationDetail detail)
      LOCKS_EXCLUDED(m_mtx_);

  const InteractiveTaskAllocationDetail *QueryAllocDetailOfIaTask(
      uint32_t task_id) LOCKS_EXCLUDED(m_mtx_);

  void RemoveAllocDetailOfIaTask(uint32_t task_id) LOCKS_EXCLUDED(m_mtx_);

 private:
  using Mutex = util::mutex;
  using LockGuard = util::AbslMutexLockGuard;

  void CranedIsUpCb_(CranedId craned_id);
  void CranedIsDownCb_(CranedId craned_id);

  std::unique_ptr<CraneCtldServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  Mutex m_mtx_;
  // Use absl::hash_node_map because QueryAllocDetailOfIaTask returns a
  // pointer. Pointer stability is needed here. The return type is a const
  // pointer, and it guarantees that the thread safety is not broken.
  absl::node_hash_map<uint32_t /*task id*/, InteractiveTaskAllocationDetail>
      m_task_alloc_detail_map_ GUARDED_BY(m_mtx_);

  inline static std::mutex s_sigint_mtx;
  inline static std::condition_variable s_sigint_cv;
  static void signal_handler_func(int) { s_sigint_cv.notify_one(); };

  friend class CraneCtldServiceImpl;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::CtldServer> g_ctld_server;
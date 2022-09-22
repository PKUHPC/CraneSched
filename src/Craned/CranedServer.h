#pragma once

#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include <atomic>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>

#include "crane/Lock.h"

#if Boost_MINOR_VERSION >= 71
#include <boost/uuid/uuid_hash.hpp>
#endif

#include "TaskManager.h"
#include "crane/PublicHeader.h"
#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"

namespace Craned {

using boost::uuids::uuid;

using grpc::Channel;
using grpc::Server;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;

using crane::grpc::Craned;
using crane::grpc::SrunXStreamReply;
using crane::grpc::SrunXStreamRequest;

class CranedServiceImpl : public Craned::Service {
 public:
  CranedServiceImpl() = default;

  Status SrunXStream(ServerContext *context,
                     ServerReaderWriter<SrunXStreamReply, SrunXStreamRequest>
                         *stream) override;

  grpc::Status ExecuteTask(grpc::ServerContext *context,
                           const crane::grpc::ExecuteTaskRequest *request,
                           crane::grpc::ExecuteTaskReply *response) override;

  grpc::Status TerminateTask(
      grpc::ServerContext *context,
      const crane::grpc::TerminateTaskRequest *request,
      crane::grpc::TerminateTaskReply *response) override;

  grpc::Status QueryTaskIdFromPort(
      grpc::ServerContext *context,
      const crane::grpc::QueryTaskIdFromPortRequest *request,
      crane::grpc::QueryTaskIdFromPortReply *response) override;

  grpc::Status QueryTaskIdFromPortForward(
      grpc::ServerContext *context,
      const crane::grpc::QueryTaskIdFromPortForwardRequest *request,
      crane::grpc::QueryTaskIdFromPortForwardReply *response) override;

  grpc::Status MigrateSshProcToCgroup(
      grpc::ServerContext *context,
      const crane::grpc::MigrateSshProcToCgroupRequest *request,
      crane::grpc::MigrateSshProcToCgroupReply *response) override;

  grpc::Status CreateCgroupForTask(
      grpc::ServerContext *context,
      const crane::grpc::CreateCgroupForTaskRequest *request,
      crane::grpc::CreateCgroupForTaskReply *response) override;

  grpc::Status ReleaseCgroupForTask(
      grpc::ServerContext *context,
      const crane::grpc::ReleaseCgroupForTaskRequest *request,
      crane::grpc::ReleaseCgroupForTaskReply *response) override;
};

class CranedServer {
 public:
  explicit CranedServer(const Config::CranedListenConf &listen_conf);

  inline void Shutdown() { m_server_->Shutdown(); }

  inline void Wait() { m_server_->Wait(); }

  void GrantResourceToken(const uuid &resource_uuid, uint32_t task_id)
      LOCKS_EXCLUDED(m_mtx_);

  CraneErr RevokeResourceToken(const uuid &resource_uuid)
      LOCKS_EXCLUDED(m_mtx_);

  CraneErr CheckValidityOfResourceUuid(const uuid &resource_uuid,
                                       uint32_t task_id) LOCKS_EXCLUDED(m_mtx_);

 private:
  using Mutex = util::mutex;
  using LockGuard = util::AbslMutexLockGuard;

  // Craned no longer takes the responsibility for resource management.
  // Resource management is handled in CraneCtld. Craned only records
  // who have the permission to execute interactive tasks in Craned.
  // If someone holds a valid resource uuid on a task id, we assume that he
  // has allocated required resource from CraneCtld.
  std::unordered_map<uuid, uint32_t /*task id*/, boost::hash<uuid>>
      m_resource_uuid_map_ GUARDED_BY(m_mtx_);

  Mutex m_mtx_;

  std::unique_ptr<CranedServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  friend class CranedServiceImpl;
};
}  // namespace Craned

// The initialization of CranedServer requires some parameters.
// We can't use the Singleton pattern here. So we use one global variable.
inline std::unique_ptr<Craned::CranedServer> g_server;

#pragma once

#include "CranedPublicDefs.h"
// Precompiled header comes first.

#include "TaskManager.h"
#include <uvw.hpp>

#include "crane/PublicHeader.h"
#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"
namespace Craned {
using crane::grpc::CraneForeD;
using grpc::Channel;

class CforedClient {
 public:
  CforedClient();
  ~CforedClient();
  void InitChannelAndStub(const std::string& cfored_name);
  void AsyncSendRecvThread_();
  void SetTaskInputForwardCb(
      task_id_t task_id, std::function<void(const std::string&)> task_input_cb);
  void UnsetTaskInputForwardCb(
      task_id_t task_id);
  void TaskOutPutForward(task_id_t task_id, const std::string& msg);

 private:
  moodycamel::ConcurrentQueue<std::pair<task_id_t ,std::string /*msg*/>> m_msg_queue_to_cfored_;
  std::atomic<bool> m_stopped;
  std::string m_cfored_name_;
  std::thread m_async_read_write_thread_;
  absl::Mutex m_mtx_;
  std::shared_ptr<Channel> m_cfored_channel_;
  std::unique_ptr<CraneForeD::Stub> m_stub_;
  std::unordered_map<task_id_t, std::function<void(const std::string&)>>
      m_task_input_cb_map_;
};

class CforedManager {
 public:
  CforedManager();
  ~CforedManager();
  void RegisterIOForward(TaskInstance* instance);
  void UnregisterIOForward(TaskInstance* instance);


 private:

  std::atomic<bool> m_stopped_;
  std::atomic<bool> m_stopped_temp_;
  std::shared_ptr<uvw::loop> m_loop;
  std::thread m_ev_loop_thread_;
//  std::shared_ptr<uvw::async_handle> m_
  moodycamel::ConcurrentQueue<std::pair<task_id_t ,std::string /*msg*/>> m_msg_queue_to_task_;
  absl::Mutex m_mtx;
  std::unordered_map<std::string /*cfored name*/, std::shared_ptr<CforedClient>>
      m_cfored_client_map_ GUARDED_BY(m_mtx);
  std::unordered_map<std::string /*cfored name*/, uint32_t /*cfored refcount*/>
      m_cfored_client_ref_count_map_ GUARDED_BY(m_mtx);
  std::unordered_map<task_id_t, std::shared_ptr<uvw::poll_handle>> m_task_id_handle_map_
      GUARDED_BY(m_mtx);
  std::unordered_map<int /*fd*/, TaskInstance*> m_fd_task_instance_map_ GUARDED_BY(m_mtx);
};
}  // namespace Craned
inline std::unique_ptr<Craned::CforedManager> g_cfored_manager;
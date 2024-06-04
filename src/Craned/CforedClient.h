#pragma once

#include "CranedPublicDefs.h"
// Precompiled header comes first.

#include <uvw.hpp>

#include "TaskManager.h"

namespace Craned {

// Todo: Move using into classes or functions!
using grpc::Channel;
using grpc::ClientAsyncReaderWriter;
using grpc::CompletionQueue;

using crane::grpc::CraneForeD;
using crane::grpc::StreamCforedTaskIOReply;
using crane::grpc::StreamCforedTaskIORequest;

class CforedClient {
 public:
  CforedClient();
  ~CforedClient();

  void InitChannelAndStub(const std::string& cfored_name);

  void AsyncSendRecvThread_();

  void SetTaskInputForwardCb(
      task_id_t task_id, std::function<void(const std::string&)> task_input_cb);

  void UnsetTaskInputForwardCb(task_id_t task_id);

  void TaskOutPutForward(task_id_t task_id, const std::string& msg);

 private:
  void CleanOutputQueueAndWriteToStreamSt_(
      ClientAsyncReaderWriter<StreamCforedTaskIORequest,
                              StreamCforedTaskIOReply>* stream);

  moodycamel::ConcurrentQueue<std::pair<task_id_t, std::string /*msg*/>>
      m_output_queue_;
  std::atomic<bool> m_stopped_{false};
  std::string m_cfored_name_;
  std::thread m_async_read_write_thread_;

  std::shared_ptr<Channel> m_cfored_channel_;
  std::unique_ptr<CraneForeD::Stub> m_stub_;

  enum class Tag : int { Prepare = 0, Read = 1, Write = 2 };
  grpc::CompletionQueue m_cq_;

  absl::Mutex m_mtx_;
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
  moodycamel::ConcurrentQueue<std::pair<task_id_t, std::string /*msg*/>>
      m_msg_queue_to_task_;

  absl::Mutex m_mtx;

  std::unordered_map<std::string /*cfored name*/, std::shared_ptr<CforedClient>>
      m_cfored_client_map_ GUARDED_BY(m_mtx);

  std::unordered_map<std::string /*cfored name*/, uint32_t /*cfored refcount*/>
      m_cfored_client_ref_count_map_ GUARDED_BY(m_mtx);

  std::unordered_map<task_id_t, std::shared_ptr<uvw::poll_handle>>
      m_task_id_handle_map_ GUARDED_BY(m_mtx);

  std::unordered_map<int /*fd*/, TaskInstance*> m_fd_task_instance_map_
      GUARDED_BY(m_mtx);
};
}  // namespace Craned
inline std::unique_ptr<Craned::CforedManager> g_cfored_manager;
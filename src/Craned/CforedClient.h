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
  template <class T>
  using ConcurrentQueue = moodycamel::ConcurrentQueue<T>;

 public:
  CforedClient();
  ~CforedClient();

  void InitChannelAndStub(const std::string& cfored_name);

  void AsyncSendRecvThread_();

  void InitTaskFwdAndSetInputCb(
      task_id_t task_id, std::function<void(const std::string&)> task_input_cb);

  void TaskOutPutForward(task_id_t task_id, const std::string& msg);

  bool TaskOutputFinish(task_id_t task_id);

  bool TaskProcessStop(task_id_t task_id);

 private:
  struct TaskFwdMeta {
    std::function<void(const std::string&)> input_cb;
    bool output_stopped{false};
    bool proc_stopped{false};
  };

  void CleanOutputQueueAndWriteToStreamThread_(
      ClientAsyncReaderWriter<StreamCforedTaskIORequest,
                              StreamCforedTaskIOReply>* stream,
      std::atomic<bool>* write_pending);

  ConcurrentQueue<std::pair<task_id_t, std::string /*msg*/>> m_output_queue_;
  std::thread m_fwd_thread_;
  std::atomic<bool> m_stopped_{false};

  std::string m_cfored_name_;
  std::shared_ptr<Channel> m_cfored_channel_;
  std::unique_ptr<CraneForeD::Stub> m_stub_;

  // Tag MUST have the same size of void* !!!!
  enum class Tag : intptr_t { Prepare = 0, Read = 1, Write = 2 };
  grpc::CompletionQueue m_cq_;

  absl::Mutex m_mtx_;
  std::unordered_map<task_id_t, TaskFwdMeta> m_task_fwd_meta_map_;
};

class CforedManager {
  template <class T>
  using ConcurrentQueue = moodycamel::ConcurrentQueue<T>;

 public:
  CforedManager() = default;
  ~CforedManager();

  bool Init();

  void RegisterIOForward(std::string const& cfored, task_id_t task_id, int fd);
  void TaskProcOnCforedStopped(std::string const& cfored, task_id_t task_id);

 private:
  struct RegisterElem {
    std::string cfored;
    task_id_t task_id;
    int fd;
  };

  struct TaskStopElem {
    std::string cfored;
    task_id_t task_id;
  };

  struct UnregisterElem {
    std::string cfored;
    task_id_t task_id;
  };

  void UnregisterIOForward_(std::string const& cfored, task_id_t task_id);

  void EvLoopThread_(const std::shared_ptr<uvw::loop>& uvw_loop);

  std::atomic<bool> m_stopped_{false};
  std::shared_ptr<uvw::loop> m_loop_;
  std::thread m_ev_loop_thread_;

  std::shared_ptr<uvw::async_handle> m_register_handle_;
  ConcurrentQueue<std::pair<RegisterElem, std::promise<bool>>>
      m_register_queue_;
  void RegisterCb_();

  std::shared_ptr<uvw::async_handle> m_task_stop_handle_;
  ConcurrentQueue<TaskStopElem> m_task_stop_queue_;
  void TaskStopCb_();

  std::shared_ptr<uvw::async_handle> m_unregister_handle_;
  ConcurrentQueue<UnregisterElem> m_unregister_queue_;
  void UnregisterCb_();

  std::unordered_map<std::string /*cfored name*/, std::shared_ptr<CforedClient>>
      m_cfored_client_map_;

  std::unordered_map<std::string /*cfored name*/, uint32_t /*cfored refcount*/>
      m_cfored_client_ref_count_map_;
};
}  // namespace Craned

inline std::unique_ptr<Craned::CforedManager> g_cfored_manager;
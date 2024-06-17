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

  void InitProcFwdAndSetInputCb(
      task_id_t task_id, proc_id_t proc_id,
      std::function<bool(const std::string&)> task_input_cb);

  void ProcOutPutForward(unsigned int task_id, unsigned int proc_id,
                         const std::string& msg, bool end);

  bool ProcOutputFinish(task_id_t task_id, proc_id_t proc_id);

  bool ProcProcessStop(task_id_t task_id, proc_id_t proc_id);

 private:
  struct TaskFwdMeta {
    std::function<bool(const std::string&)> input_cb;
    bool input_stopped{false};
    bool output_stopped{false};
    bool proc_stopped{false};
  };

  struct TaskOutPutElem {
    task_id_t task_id;
    proc_id_t proc_id;
    std::string msg;
    bool end;
    TaskOutPutElem() = default;
    TaskOutPutElem(task_id_t task_id, proc_id_t proc_id, std::string msg,
                   bool end)
        : task_id(task_id), proc_id(proc_id), msg(std::move(msg)), end{end} {}
  };

  void CleanOutputQueueAndWriteToStreamThread_(
      ClientAsyncReaderWriter<StreamCforedTaskIORequest,
                              StreamCforedTaskIOReply>* stream,
      std::atomic<bool>* write_pending);

  ConcurrentQueue<TaskOutPutElem> m_output_queue_;
  std::thread m_fwd_thread_;
  std::atomic<bool> m_stopped_{false};

  std::string m_cfored_name_;
  std::shared_ptr<Channel> m_cfored_channel_;
  std::unique_ptr<CraneForeD::Stub> m_stub_;

  // Tag MUST have the same size of void* !!!!
  enum class Tag : intptr_t { Prepare = 0, Read = 1, Write = 2 };
  grpc::CompletionQueue m_cq_;

  absl::Mutex m_mtx_;
  std::unordered_map<task_id_t, std::unordered_map<proc_id_t, TaskFwdMeta>>
      m_task_proc_fwd_meta_map_;
};

class CforedManager {
  template <class T>
  using ConcurrentQueue = moodycamel::ConcurrentQueue<T>;

 public:
  CforedManager() = default;
  ~CforedManager();

  bool Init();

  void RegisterIOForward(std::string const& cfored, task_id_t task_id,
                         proc_id_t proc_id, int in_fd, int out_fd);

  /*!
   *
   * @param cfored
   * @param task_id
   * @param proc_id 0 indicate main proc exit,trigger task end.
   */
  void TaskProcOnCforedStopped(std::string const& cfored, task_id_t task_id,
                               proc_id_t proc_id, int proc_in_fd,
                               int proc_out_fd);

 private:
  struct RegisterElem {
    std::string cfored;
    task_id_t task_id;
    proc_id_t proc_id;
    int in_fd;
    int out_fd;
  };

  struct TaskStopElem {
    std::string cfored;
    task_id_t task_id;
    proc_id_t proc_id;
    int in_fd;
    int out_fd;
  };

  struct UnregisterElem {
    std::string cfored;
    task_id_t task_id;
    proc_id_t proc_id;
  };

  void UnregisterIOForward_(std::string const& cfored, task_id_t task_id,
                            proc_id_t proc_id);

  void EvLoopThread_(const std::shared_ptr<uvw::loop>& uvw_loop);

  std::atomic<bool> m_stopped_{false};
  std::shared_ptr<uvw::loop> m_loop_;
  std::thread m_ev_loop_thread_;

  std::shared_ptr<uvw::async_handle> m_register_handle_;
  ConcurrentQueue<std::pair<RegisterElem, std::promise<bool>>>
      m_register_queue_;
  void RegisterCb_();

  std::shared_ptr<uvw::async_handle> m_proc_stop_handle_;
  ConcurrentQueue<TaskStopElem> m_proc_stop_queue_;
  void ProcStopCb_();

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
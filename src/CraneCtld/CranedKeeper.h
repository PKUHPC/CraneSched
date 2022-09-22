#pragma once

#include <grpc++/alarm.h>
#include <grpc++/completion_queue.h>
#include <grpc++/grpc++.h>

#include <boost/dynamic_bitset.hpp>
#include <boost/pool/object_pool.hpp>
#include <boost/uuid/uuid.hpp>
#include <functional>
#include <future>
#include <memory>
#include <thread>

#include "CtldPublicDefs.h"
#include "crane/Lock.h"
#include "crane/PublicHeader.h"
#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"

namespace Ctld {

class CranedKeeper;

struct CranedAddrAndId {
  std::string node_addr;
  CranedId node_id;
};

/**
 * A class that encapsulate the detail of the underlying gRPC stub.
 */
class CranedStub {
 public:
  explicit CranedStub(CranedKeeper *craned_keeper);

  ~CranedStub();

  CraneErr ExecuteTask(const TaskInCtld *task);

  CraneErr CreateCgroupForTask(uint32_t task_id, uid_t uid);

  CraneErr ReleaseCgroupForTask(uint32_t task_id, uid_t uid);

  CraneErr TerminateTask(uint32_t task_id);

 private:
  CranedKeeper *m_craned_keeper_;

  uint32_t m_slot_offset_;

  grpc_connectivity_state m_prev_channel_state_;
  std::shared_ptr<grpc::Channel> m_channel_;

  std::unique_ptr<crane::grpc::Craned::Stub> m_stub_;

  // Set if underlying gRPC is down.
  bool m_invalid_;

  uint32_t m_maximum_retry_times_;
  uint32_t m_failure_retry_times_;

  CranedAddrAndId m_addr_and_id_;

  // void* parameter is m_data_. Used to free m_data_ when CranedStub is being
  // destructed.
  std::function<void(CranedStub *)> m_clean_up_cb_;

  friend class CranedKeeper;
};

class CranedKeeper {
 public:
  CranedKeeper();

  ~CranedKeeper();

  void RegisterCraneds(std::list<CranedAddrAndId> node_addr_id_list);

  uint32_t AvailableCranedCount();

  bool CranedValid(uint32_t index);

  /**
   * Get the pointer to CranedStub.
   * @param craned_id the index of CranedStub
   * @return nullptr if index points to an invalid slot, the pointer to
   * CranedStub otherwise.
   * @attention It's ok to return the pointer of CranedStub directly. The
   * CranedStub will not be freed before the CranedIsDown() callback returns.
   * The callback registerer should do necessary synchronization to clean up all
   * the usage of the CranedStub pointer before CranedIsDown() returns.
   */
  CranedStub *GetCranedStub(const CranedId &craned_id);

  bool CheckCranedIdExists(const CranedId &craned_id);

  void SetCranedIsUpCb(std::function<void(CranedId)> cb);

  void SetCranedIsDownCb(std::function<void(CranedId)> cb);

  void SetCranedTempDownCb(std::function<void(CranedId)> cb);

  void SetCranedRecFromTempFailureCb(std::function<void(CranedId)> cb);

 private:
  struct InitializingCranedTagData {
    std::unique_ptr<CranedStub> craned;
  };

  struct CqTag {
    enum Type { kInitializingCraned, kEstablishedCraned };
    Type type;
    void *data;
  };

  static void PutBackNodeIntoUnavailList_(CranedStub *stub);

  void ConnectCranedNode_(CranedAddrAndId addr_info);

  CqTag *InitCranedStateMachine_(InitializingCranedTagData *tag_data,
                                 grpc_connectivity_state new_state);
  CqTag *EstablishedCranedStateMachine_(CranedStub *craned,
                                        grpc_connectivity_state new_state);

  void StateMonitorThreadFunc_();

  void PeriodConnectCranedThreadFunc_();

  std::function<void(CranedId)> m_craned_is_up_cb_;
  std::function<void(CranedId)> m_craned_is_temp_down_cb_;
  std::function<void(CranedId)> m_craned_rec_from_temp_failure_cb_;

  // Guarantee that the Craned will not be freed before this callback is
  // called.
  std::function<void(CranedId)> m_craned_is_down_cb_;

  util::mutex m_tag_pool_mtx_;

  // Must be declared previous to any grpc::CompletionQueue, so it can be
  // constructed before any CompletionQueue and be destructed after any
  // CompletionQueue.
  boost::object_pool<CqTag> m_tag_pool_;

  // Protect m_node_vec_, m_node_id_slot_offset_map_ and m_empty_slot_bitset_.
  util::mutex m_craned_mtx_;

  // Todo: Change to std::shared_ptr. GRPC has sophisticated error handling
  //  mechanism. So it's ok to access the stub when the Craned is down. What
  //  should be avoided is null pointer accessing.
  // Contains connection-established nodes only.
  std::vector<std::unique_ptr<CranedStub>> m_craned_vec_;

  // Used to track the empty slots in m_craned_vec_. We can use find_first() to
  // locate the first empty slot.
  boost::dynamic_bitset<> m_empty_slot_bitset_;

  std::unordered_map<CranedId, uint32_t, CranedId::Hash>
      m_craned_id_slot_offset_map_;

  util::mutex m_unavail_craned_list_mtx_;
  std::list<CranedAddrAndId> m_unavail_craned_list_;

  // Protect m_alive_craned_bitset_
  util::rw_mutex m_alive_craned_rw_mtx_;

  // If bit n is set, the craned client n is available to send grpc. (underlying
  // grpc channel state is GRPC_CHANNEL_READY).
  boost::dynamic_bitset<> m_alive_craned_bitset_;

  grpc::CompletionQueue m_cq_;
  util::mutex m_cq_mtx_;
  bool m_cq_closed_;

  std::thread m_cq_thread_;

  std::thread m_period_connect_thread_;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::CranedKeeper> g_craned_keeper;
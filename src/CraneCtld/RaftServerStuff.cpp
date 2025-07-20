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

#include "RaftServerStuff.h"

#include "AccountManager.h"
#include "RpcService/CranedKeeper.h"

namespace Ctld {

void RaftServerStuff::Init() {  // State manager.
  m_state_mgr_ = std::make_shared<crane::Internal::NuRaftStateManager>(
      m_server_id_, m_endpoint_);
  // State machine.
  m_state_machine_ = std::make_shared<CraneStateMachine>(true);

  m_logger_ = std::make_shared<crane::Internal::NuRaftLoggerWrapper>();
  static_cast<crane::Internal::NuRaftLoggerWrapper *>(m_logger_.get())
      ->SetLevel(int(StrToLogLevel(g_config.Raft.DebugLevel).value()));

  // ASIO options.
  nuraft::asio_service::options asio_opt;
  asio_opt.thread_pool_size_ = 4;

  // Raft parameters.
  raft_params params;
  // heartbeat: A ms, election timeout: B - C ms.
  params.heart_beat_interval_ = kHeartbeatIntervalMs;
  params.election_timeout_lower_bound_ = kElectionTimeoutLowerMs;
  params.election_timeout_upper_bound_ = kElectionTimeoutUpperMs;

  // Up to X logs will be preserved ahead the last snapshot.
  params.reserved_log_items_ = kReservedLogItems;
  // Snapshot will be created for every X log appends.
  params.snapshot_distance_ = kSnapshotDistance;
  params.client_req_timeout_ = kClientRequestTimeoutMs;
  // According to this method, `append_log` function
  // should be handled differently.
  params.return_method_ = nuraft::raft_params::async_handler;  // or blocking

  params.auto_adjust_quorum_for_small_cluster_ = true;
  params.auto_forwarding_ = true;

  raft_server::init_options init_options{};
  init_options.raft_callback_ = StatusChangeCallback;

  GetStateMachine()->Init(g_config.CraneCtldDbPath,
                          static_cast<crane::Internal::NuRaftLogStore *>(
                              m_state_mgr_->load_log_store().get())
                              ->all_log_entries());

  // Initialize Raft server.
  m_raft_instance_ = m_launcher_.init(m_state_machine_, m_state_mgr_, m_logger_,
                                      m_port_, asio_opt, params, init_options);
  if (!m_raft_instance_) {
    CRANE_CRITICAL(
        "Failed to initialize launcher (see the message "
        "in the log file).");
  }

  static_cast<crane::Internal::NuRaftStateManager *>(m_state_mgr_.get())
      ->SetRaftServerBwdPointer(m_raft_instance_.get());

  // Wait until the Raft server is ready (up to 5 seconds).
  constexpr size_t max_try = 20;
  CRANE_TRACE("init Raft instance");
  for (size_t i = 0; i < max_try; ++i) {
    if (m_raft_instance_->is_initialized()) {
      CRANE_TRACE("Raft server init done!");
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
  CRANE_ERROR(
      "Init raft server failed! During the offline period of this node, there "
      "may have been a raft configuration update. Please have the "
      "administrator fix it.");
}

void RaftServerStuff::Shutdown() {
  if (m_exit_) return;

  m_exit_ = true;
  bool res = m_launcher_.shutdown();
  if (res)
    CRANE_INFO("Raft server shutdown success!");
  else
    CRANE_ERROR("Raft server shutdown failed!");
}

bool RaftServerStuff::CheckServerNodeExist(int server_id) const {
  std::shared_ptr<srv_config> conf =
      m_raft_instance_->get_srv_config(server_id);
  return conf != nullptr;
}

bool RaftServerStuff::AddServerAsync(int server_id,
                                     const std::string &endpoint) {
  if (server_id < 0 || server_id == m_server_id_) {
    CRANE_ERROR("Add Server failed: Wrong server id: {}.", server_id);
    return false;
  }

  srv_config srv_conf(server_id, endpoint);
  std::shared_ptr<raft_result> ret = m_raft_instance_->add_srv(srv_conf);
  if (!ret->get_accepted()) {
    CRANE_ERROR("Add Server failed: ret: {}, reason: {}.",
                static_cast<int32_t>(ret->get_result_code()),
                ret->get_result_str());
    return false;
  } else {
    CRANE_TRACE("Server #{} {} joining request submit success.", server_id,
                endpoint);
  }
  return true;
}

bool RaftServerStuff::AddServer(int server_id, const std::string &endpoint) {
  bool res = AddServerAsync(server_id, endpoint);
  if (!res) return false;

  // Wait until add server done;
  constexpr size_t max_try = 40;
  for (size_t i = 0; i < max_try; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    std::shared_ptr<srv_config> conf =
        m_raft_instance_->get_srv_config(server_id);
    if (conf) {
      CRANE_INFO("Server #{} {} joining request done.", server_id, endpoint);
      return true;
    }
  }

  CRANE_ERROR("Server #{} {} joining request timeout.", server_id, endpoint);
  return false;
}

bool RaftServerStuff::RemoveServer(int server_id) {
  if (!CheckServerNodeExist(server_id)) return false;
  m_raft_instance_->remove_srv(server_id);
  return true;
}

bool RaftServerStuff::AppendLog(std::shared_ptr<nuraft::buffer> new_log) {
  // To measure the elapsed time.
  //  std::shared_ptr<TestSuite::Timer> timer =
  //  std::make_shared<TestSuite::Timer>();

  // Do append.
  auto ret = m_raft_instance_->append_entries({new_log});

  if (!ret->get_accepted()) {
    // Log append rejected, usually because this node is not a leader.
    CRANE_ERROR("Failed to replicate log: result_code={}, result_str={}",
                static_cast<int>(ret->get_result_code()),
                ret->get_result_str());
    return false;
  }
  // Log append accepted, but that doesn't mean the log is committed.
  // Commit result can be obtained below.

  if (m_raft_instance_->get_current_params().return_method_ ==
      raft_params::blocking) {
    // Blocking mode:
    //   `append_entries` returns after getting a consensus,
    //   so that `ret` already has the result from state machine.
    std::shared_ptr<std::exception> err(nullptr);
    handle_result(*ret, err);
  } else if (m_raft_instance_->get_current_params().return_method_ ==
             raft_params::async_handler) {
    // Async mode:
    //   `append_entries` returns immediately.
    //   `handle_result` will be invoked asynchronously,
    //   after getting a consensus.
    ret->when_ready(std::bind(&RaftServerStuff::handle_result, this,
                              std::placeholders::_1, std::placeholders::_2));

  } else {
    return false;
  }
  return true;
}

CraneStateMachine *RaftServerStuff::GetStateMachine() {
  return static_cast<CraneStateMachine *>(m_state_machine_.get());
}

void RaftServerStuff::GetNodeStatus(
    crane::grpc::QueryLeaderInfoReply *response) {
  std::vector<std::shared_ptr<srv_config>> configs;
  m_raft_instance_->get_srv_config_all(configs);
  std::ranges::sort(configs, [](const std::shared_ptr<srv_config> &a,
                                const std::shared_ptr<srv_config> &b) {
    return a->get_id() < b->get_id();
  });

  std::vector<raft_server::peer_info> peers =
      m_raft_instance_->get_peer_info_all();
  std::ranges::sort(
      peers, [](const raft_server::peer_info &a,
                const raft_server::peer_info &b) { return a.id_ < b.id_; });

  int leader_id = m_raft_instance_->get_leader();

  auto peer = peers.begin();
  for (const auto &srv : configs) {
    auto server = response->add_server_list();

    server->set_id(srv->get_id());
    server->set_end_point(srv->get_endpoint());
    if (srv->get_id() == leader_id) {
      server->set_role(crane::grpc::QueryLeaderInfoReply::Leader);
    } else {
      if (peer->last_succ_resp_us_ > 3000 * 1000 /* 3s*/)
        server->set_role(crane::grpc::QueryLeaderInfoReply::Offline);
      else
        server->set_role(crane::grpc::QueryLeaderInfoReply::Follower);

      peer++;
    }
  }

  std::shared_ptr<nuraft::log_store> ls = m_state_mgr_->load_log_store();

  response->set_server_id(m_server_id_);
  response->set_leader_id(m_raft_instance_->get_leader());
  response->set_start_index(ls->start_index());
  response->set_next_slot(ls->next_slot());
  response->set_committed_log_idx(m_raft_instance_->get_committed_log_idx());
  response->set_cur_term(m_raft_instance_->get_term());
  response->set_last_snapshot_log_idx(
      m_state_machine_->last_snapshot()
          ? m_state_machine_->last_snapshot()->get_last_log_idx()
          : 0);
  response->set_last_snapshot_log_term(
      m_state_machine_->last_snapshot()
          ? m_state_machine_->last_snapshot()->get_last_log_term()
          : 0);
}

void RaftServerStuff::handle_result(raft_result &result,
                                    std::shared_ptr<std::exception> &err) {
  if (result.get_result_code() != nuraft::cmd_result_code::OK) {
    // Something went wrong.
    // This means committing this log failed,
    // but the log itself is still in the log store.
    CRANE_ERROR("Append log failed: {},{}",
                static_cast<int>(result.get_result_code()),
                result.get_result_str());
    return;
  }
  std::shared_ptr<buffer> buf = result.get();
  uint64_t ret_value = buf->get_ulong();
  CRANE_TRACE("Append log succeeded, return value: {}", ret_value);
}

cb_func::ReturnCode RaftServerStuff::StatusChangeCallback(cb_func::Type type,
                                                          cb_func::Param *p) {
  static crane::grpc::QueryLeaderInfoReply_RaftRole role =
      crane::grpc::QueryLeaderInfoReply_RaftRole_Follower;

  if (type == cb_func::Type::BecomeLeader) {
    // before become leader
    uint64_t term_id = *static_cast<uint64_t *>(p->ctx);
    CRANE_TRACE("I am leader, term id : {}", term_id);
    role = crane::grpc::QueryLeaderInfoReply_RaftRole_Leader;

    if (term_id > 1 &&
        g_task_scheduler) {  // Check if the pointer is not null, so that the
                             // following functions will not run during the
                             // initialization phase.

      g_embedded_db_client->RestoreTaskID();
      // The reason for using asynchrony is that the raft state change can only
      // be completed after this function returns
      g_thread_pool->detach_task([]() {
        // Wait for raft status change completion
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
        if (g_account_manager) g_account_manager->InitDataMap();
        if (g_task_scheduler)
          g_task_scheduler
              ->RestoreFromEmbeddedDb();  // A flag is set here, it must be
                                          // placed at the end.
      });
    }
  } else if (type == cb_func::Type::NewSessionFromLeader) {
    cb_func::ConnectionArgs ctx =
        *static_cast<cb_func::ConnectionArgs *>(p->ctx);
    if (!ctx.isLeader) return cb_func::ReturnCode::Ok;
    if (role == crane::grpc::QueryLeaderInfoReply_RaftRole_Leader &&
        g_task_scheduler) {  // before become follower
      // async
      g_thread_pool->detach_task([id = ctx.srvId]() {
        // Although the following operations are time-consuming, this node is
        // already a follower and has no other tasks.
        std::this_thread::sleep_for(std::chrono::milliseconds(
            1000));  // Wait for task data in leader ready
        if (g_craned_keeper) g_craned_keeper->BroadcastLeaderId(id);
        if (g_meta_container) g_meta_container->MarkAllCranedDown();
      });

      g_task_scheduler->ResetTaskData();
    }

    role = crane::grpc::QueryLeaderInfoReply_RaftRole_Follower;
  }

  return cb_func::ReturnCode::Ok;
}

}  // namespace Ctld

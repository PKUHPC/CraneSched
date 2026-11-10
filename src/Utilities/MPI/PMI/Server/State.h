/**
 * Copyright (c) 2026 Peking University and Peking University
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

#pragma once
#include <condition_variable>
#include <map>
#include <mutex>
#include <set>
#include <thread>

#include "Protocol/Pmi1Codec.h"
#include "Protocol/Pmi2Codec.h"
#include "Server/Server.h"
#include "protos/mpi/Pmi.grpc.pb.h"

namespace pmi {
using Fields = crane::pmi2::Fields;
inline constexpr auto kMaxFrame = crane::pmi2::Codec::kMaxFrame;
inline constexpr auto kMaxKey = crane::pmi2::Codec::kMaxKey;
inline constexpr auto kMaxValue = crane::pmi2::Codec::kMaxValue;
struct Server::Impl : crane::grpc::mpi::pmi::Pmi::Service {
  enum class Protocol : uint32_t { kPmi11 = 1, kPmi20 = 2 };
  struct Client {
    int fd{-1};
    uint32_t rank{};
    std::optional<Protocol> protocol;
    bool initialized{false};
    bool finalized{false};
    bool closing{false};
    std::string input;
    std::string output;
    std::optional<Fields> waiting;
    std::chrono::steady_clock::time_point deadline;
    ~Client() {
      if (fd >= 0) close(fd);
    }
  };
  mpi::Config m_config_;
  crane::grpc::StepToD m_step_;
  std::string m_jobid_;
  uint32_t m_node_{};
  size_t m_local_size_{};
  std::vector<uint32_t> m_local_ranks_;
  std::map<uint32_t, std::unique_ptr<Client>> m_clients_;
  Fields m_pending_kvs_;
  Fields m_visible_kvs_;
  Fields m_node_attrs_;
  Fields m_job_attrs_;
  std::optional<Protocol> m_protocol_;
  std::mutex m_mutex_;
  std::condition_variable m_cv_;
  std::thread m_io_thread_;
  std::thread m_fence_thread_;
  std::unique_ptr<grpc::Server> m_rpc_server_;
  std::string m_port_;
  std::map<CranedId, std::shared_ptr<crane::grpc::mpi::pmi::Pmi::Stub>>
      m_peers_;
  std::map<CranedId, std::string> m_peer_ports_;
  std::optional<CranedId> m_parent_;
  std::set<CranedId> m_children_;
  std::map<CranedId, Fields> m_contributions_;
  uint64_t m_epoch_{0};
  std::optional<uint64_t> m_completed_epoch_;
  std::optional<std::chrono::steady_clock::time_point> m_fence_deadline_;
  std::unique_ptr<grpc::ClientContext> m_outgoing_context_;
  int m_wake_fd_{-1};
  bool m_stopping_{false};
  bool m_failed_{false};
  std::function<void()> m_on_failure_;
  std::chrono::seconds m_timeout_;

  ~Impl();
  void Wake_() const;
  void Fail_(std::string_view reason);
  uint32_t ProtocolVersion_() const;
  void Reply_(Client& c, const Fields& request, Fields fields, int rc = 0);
  void Attribute_(Client& c, const Fields& request, const Fields& attributes);
  size_t LocalArrivals_() const;
  void StartFenceDeadline_();
  grpc::Status Fence(grpc::ServerContext* context,
                     const crane::grpc::mpi::pmi::FenceRequest* request,
                     crane::grpc::mpi::pmi::FenceReply* reply) override;
  grpc::Status Abort(grpc::ServerContext*,
                     const crane::grpc::mpi::pmi::AbortRequest* request,
                     crane::grpc::mpi::pmi::AbortReply* reply) override;
  void FenceLoop_();
  void Pmi2Initialize_(Client& client, const Fields& request);
  void Pmi2Abort_(Client& client, const Fields& request);
  void Pmi2JobId_(Client& client, const Fields& request);
  void Pmi2Put_(Client& client, const Fields& request);
  void Pmi2Get_(Client& client, const Fields& request);
  void Pmi2Attribute_(Client& client, const Fields& request);
  void Request_(Client& c, const Fields& request);
  void Pmi1InvalidRequest_(Client& c, const pmi1::Fields& request);
  void Pmi1GetMaxes_(Client& c, const pmi1::Fields& request);
  void Pmi1GetUniverseSize_(Client& c, const pmi1::Fields& request);
  void Pmi1GetAppnum_(Client& c, const pmi1::Fields& request);
  void Pmi1GetKvsName_(Client& c, const pmi1::Fields& request);
  void Pmi1Put_(Client& c, const pmi1::Fields& request);
  void Pmi1Get_(Client& c, const pmi1::Fields& request);
  void WaitForFence_(Client& c, const pmi1::Fields& request);
  void Finalize_(Client& c, const pmi1::Fields& request);
  void Pmi1Abort_(Client&, const pmi1::Fields&);
  void RequestPmi11_(Client& c, const pmi1::Fields& request);
  void Decode_(Client& c);
  void IoLoop_();
};
}  // namespace pmi

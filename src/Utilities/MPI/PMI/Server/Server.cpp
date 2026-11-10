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

#include <fcntl.h>
#include <sys/eventfd.h>
#include <sys/socket.h>

#include "Server/State.h"
#include "Server/Utilities.h"
#include "crane/Logger.h"
#include "fmt/format.h"

namespace pmi {

Server::Server() : impl_(std::make_unique<Impl>()) {}
Server::~Server() = default;

bool Server::Init(const mpi::Config& config, const crane::grpc::StepToD& step,
                  std::function<void()> on_failure,
                  std::chrono::seconds timeout) {
  auto& s = *impl_;
  if (s.m_wake_fd_ >= 0 || timeout <= std::chrono::seconds::zero() ||
      step.task_node_list_size() == 0)
    return false;
  std::set<CranedId> unique_nodes(step.nodelist().begin(),
                                  step.nodelist().end());
  if (unique_nodes.size() != static_cast<size_t>(step.nodelist_size()))
    return false;
  auto it = std::ranges::find(step.nodelist(), config.CranedIdOfThisNode);
  if (it == step.nodelist().end()) return false;
  s.m_node_ = it - step.nodelist().begin();
  s.m_local_size_ = 0;
  s.m_local_ranks_.resize(step.task_node_list_size());
  std::vector<bool> active(step.nodelist_size());
  uint32_t task_rank = 0;
  for (auto node : step.task_node_list()) {
    if (node >= step.nodelist_size()) return false;
    active[node] = true;
    if (node == s.m_node_) {
      s.m_local_ranks_[task_rank] = s.m_local_size_;
      ++s.m_local_size_;
    }
    ++task_rank;
  }

  s.m_config_ = config;
  s.m_step_ = step;
  s.m_timeout_ = timeout;
  s.m_on_failure_ = std::move(on_failure);
  // Legacy Open MPI PMI-2 components parse this as a numeric job.step pair.
  s.m_jobid_ = fmt::format("{}.{}", step.job_id(), step.step_id());
  s.m_mapping_ = BuildProcessMapping(step);
  if (s.m_mapping_.size() >= kMaxValue) return false;
  s.m_job_attrs_ = {
      {"PMI_process_mapping", s.m_mapping_},
      {"universeSize", std::to_string(step.task_node_list_size())}};
  s.m_wake_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  if (s.m_wake_fd_ < 0) return false;
  std::vector<CranedId> members;
  for (int index = 0; index < step.nodelist_size(); ++index)
    if (active[index]) members.push_back(step.nodelist(index));
  auto member = std::ranges::find(members, config.CranedIdOfThisNode);
  if (member != members.end()) {
    auto index = static_cast<size_t>(member - members.begin());
    if (index > 0) s.m_parent_ = members[(index - 1) / 2];
    for (auto child : {index * 2 + 1, index * 2 + 2})
      if (child < members.size()) s.m_children_.insert(members[child]);
  }
  grpc::ServerBuilder builder;
  ServerBuilderSetKeepAliveArgs(&builder);
  if (config.CompressedRpc) ServerBuilderSetCompression(&builder);
  int port = 0;
  if (config.UseTls)
    ServerBuilderAddTcpTlsListeningRandomPort(&builder, "0.0.0.0",
                                              config.TlsCerts, &port);
  else
    ServerBuilderAddTcpInsecureListeningRandomPort(&builder, "0.0.0.0", &port);
  builder.RegisterService(&s);
  s.m_rpc_server_ = builder.BuildAndStart();
  if (!s.m_rpc_server_ || port == 0) return false;
  s.m_port_ = std::to_string(port);
  s.m_io_thread_ = std::thread([&s] { s.IoLoop_(); });
  if (s.m_local_size_ > 0)
    s.m_fence_thread_ = std::thread([&s] { s.FenceLoop_(); });
  return true;
}

std::optional<mpi::LaunchInfo> Server::PrepareTask(uint32_t rank) {
  auto& s = *impl_;
  std::lock_guard lock(s.m_mutex_);
  if (s.m_wake_fd_ < 0 || s.m_failed_ || s.m_stopping_ ||
      rank >= s.m_step_.task_node_list_size() ||
      s.m_step_.task_node_list(rank) != s.m_node_ ||
      s.m_clients_.contains(rank))
    return std::nullopt;
  int fds[2];
  if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) != 0)
    return std::nullopt;
  if (fcntl(fds[0], F_SETFL, O_NONBLOCK) == -1) {
    close(fds[0]);
    close(fds[1]);
    return std::nullopt;
  }
  auto client = std::make_unique<Impl::Client>();
  client->fd = fds[0];
  client->rank = rank;
  client->deadline = std::chrono::steady_clock::now() + s.m_timeout_;
  s.m_clients_.emplace(rank, std::move(client));
  mpi::LaunchInfo launch;
  launch.fd = fds[1];
  const uint32_t local_rank = s.m_local_ranks_[rank];
  launch.env = {{"PMI_FD", std::to_string(launch.fd)},
                {"PMI_RANK", std::to_string(rank)},
                {"PMI_SIZE", std::to_string(s.m_step_.task_node_list_size())},
                {"PMI_JOBID", s.m_jobid_},
                {"PMI_LOCAL_RANK", std::to_string(local_rank)},
                {"PMI_LOCAL_SIZE", std::to_string(s.m_local_size_)}};
  s.Wake_();
  return launch;
}

bool Server::ReceivePeers(
    const std::vector<std::pair<CranedId, std::string>>& peers) {
  auto& s = *impl_;
  std::map<CranedId, std::shared_ptr<crane::grpc::mpi::pmi::Pmi::Stub>> stubs;
  std::map<CranedId, std::string> ports;
  for (const auto& [id, port] : peers) {
    auto number = ParseNumber(port);
    if (id == s.m_config_.CranedIdOfThisNode || !number || *number == 0 ||
        *number > 65535 ||
        std::ranges::find(s.m_step_.nodelist(), id) ==
            s.m_step_.nodelist().end() ||
        !s.m_config_.CranedIdToNodeHostname.contains(id) ||
        !s.m_config_.CranedIdToNodeAddr.contains(id) || stubs.contains(id))
      return false;
    grpc::ChannelArguments args;
    std::shared_ptr<grpc::Channel> channel;
    if (s.m_config_.UseTls) {
      SetTlsTargetNameOverride(&args,
                               s.m_config_.CranedIdToNodeHostname.at(id));
      channel =
          CreateTcpTlsCustomChannelByIp(s.m_config_.CranedIdToNodeAddr.at(id),
                                        port, s.m_config_.TlsCerts, args);
    } else {
      channel = CreateTcpInsecureCustomChannel(
          s.m_config_.CranedIdToNodeAddr.at(id), port, args);
    }
    stubs.emplace(id, crane::grpc::mpi::pmi::Pmi::NewStub(channel));
    ports.emplace(id, port);
  }
  if (stubs.size() + 1 != static_cast<size_t>(s.m_step_.nodelist_size()))
    return false;
  std::lock_guard lock(s.m_mutex_);
  if (s.m_failed_ || s.m_stopping_) return false;
  // Endpoint delivery can be retried; never replace a stub used by an in-flight
  // RPC.
  if (!s.m_peer_ports_.empty()) return s.m_peer_ports_ == ports;
  s.m_peer_ports_ = std::move(ports);
  s.m_peers_ = std::move(stubs);
  s.m_cv_.notify_all();
  return true;
}
void Server::TaskExited(uint32_t rank) {
  auto& s = *impl_;
  std::lock_guard lock(s.m_mutex_);
  auto it = s.m_clients_.find(rank);
  if (it != s.m_clients_.end() && !it->second->finalized)
    s.Fail_("rank exited before Finalize");
}
std::string Server::Port() const { return impl_->m_port_; }
}  // namespace pmi

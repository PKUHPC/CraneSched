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

#include <poll.h>
#include <sys/socket.h>

#include "Server/State.h"
#include "Server/Utilities.h"
#include "crane/Logger.h"
#include "fmt/format.h"

namespace pmi {
Server::Impl::~Impl() {
  {
    std::lock_guard lock(m_mutex_);
    m_stopping_ = true;
    if (m_outgoing_context_) m_outgoing_context_->TryCancel();
    Wake_();
    m_cv_.notify_all();
  }
  // Do not hold the state lock while gRPC joins handlers waiting on that
  // lock.
  if (m_rpc_server_) m_rpc_server_->Shutdown(std::chrono::system_clock::now());
  if (m_fence_thread_.joinable()) m_fence_thread_.join();
  if (m_io_thread_.joinable()) m_io_thread_.join();
  if (m_wake_fd_ >= 0) close(m_wake_fd_);
}

void Server::Impl::Wake_() const {
  uint64_t one = 1;
  if (m_wake_fd_ >= 0) (void)write(m_wake_fd_, &one, sizeof(one));
}

void Server::Impl::Fail_(std::string_view reason) {
  if (m_failed_ || m_stopping_) return;
  CRANE_ERROR("[Step #{}.{}] PMI-2 failure: {}", m_step_.job_id(),
              m_step_.step_id(), reason);
  m_failed_ = true;
  m_cv_.notify_all();
  Wake_();
}

uint32_t Server::Impl::ProtocolVersion_() const {
  return m_protocol_ ? static_cast<uint32_t>(*m_protocol_) : 0;
}

void Server::Impl::IoLoop_() {
  std::vector<pollfd> fds;
  std::vector<Client*> active;
  for (;;) {
    fds.clear();
    active.clear();
    fds.push_back({m_wake_fd_, POLLIN, 0});
    {
      std::lock_guard lock(m_mutex_);
      if (m_stopping_ || m_failed_) break;
      for (auto& [rank, c] : m_clients_) {
        if (c->fd < 0) continue;
        short events = POLLIN;
        if (!c->output.empty()) events |= POLLOUT;
        fds.push_back({c->fd, events, 0});
        active.push_back(c.get());
      }
    }
    int rc = poll(fds.data(), fds.size(), 100);
    if (rc < 0 && errno == EINTR) continue;
    std::lock_guard lock(m_mutex_);
    if (rc < 0) {
      Fail_("poll failed");
      continue;
    }
    uint64_t count;
    if (fds[0].revents & POLLIN) (void)read(m_wake_fd_, &count, sizeof(count));
    for (size_t i = 0; i < active.size(); ++i) {
      auto& c = *active[i];
      if (c.fd < 0) continue;
      auto events = fds[i + 1].revents;
      if (events & (POLLIN | POLLHUP)) {
        char buffer[8192];
        auto bytes = recv(c.fd, buffer, sizeof(buffer), 0);
        if (bytes > 0) {
          if (c.input.empty())
            c.deadline = std::chrono::steady_clock::now() + m_timeout_;
          c.input.append(buffer, bytes);
          Decode_(c);
        } else if (bytes == 0 || (errno != EINTR && errno != EAGAIN &&
                                  errno != EWOULDBLOCK)) {
          close(std::exchange(c.fd, -1));
          if (!c.finalized) Fail_("client disconnected before Finalize");
        }
      }
      if (c.fd >= 0 && !c.output.empty() && (events & POLLOUT)) {
        auto bytes = send(c.fd, c.output.data(), c.output.size(), MSG_NOSIGNAL);
        if (bytes > 0)
          c.output.erase(0, bytes);
        else if (bytes < 0 && errno != EINTR && errno != EAGAIN &&
                 errno != EWOULDBLOCK)
          Fail_("client write failed");
      }
      if (c.fd >= 0 && c.closing && c.output.empty())
        close(std::exchange(c.fd, -1));
      if ((!c.initialized || !c.input.empty() || c.waiting) && !c.closing &&
          std::chrono::steady_clock::now() >= c.deadline)
        Fail_("client request timed out");
      if (events & (POLLERR | POLLNVAL)) Fail_("client channel failed");
    }
  }
  // Invoke scheduler code outside the protocol lock. It may synchronously
  // request task cleanup, which calls back into TaskExited().
  bool report;
  {
    std::lock_guard lock(m_mutex_);
    for (auto& [rank, c] : m_clients_)
      if (c->fd >= 0) close(std::exchange(c->fd, -1));
    report = m_failed_;
  }
  if (report) {
    // Propagate only along tree edges. Each server reports failure once, so
    // cancellation reaches all ranks without an all-to-all RPC storm.
    std::vector<std::shared_ptr<crane::grpc::mpi::pmi::Pmi::Stub>> neighbors;
    {
      std::lock_guard lock(m_mutex_);
      for (const auto& [id, stub] : m_peers_)
        if (m_children_.contains(id) || m_parent_ == id)
          neighbors.push_back(stub);
    }
    for (const auto& stub : neighbors) {
      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() +
                           std::chrono::seconds(2));
      crane::grpc::mpi::pmi::AbortRequest request;
      crane::grpc::mpi::pmi::AbortReply reply;
      request.set_job_id(m_step_.job_id());
      request.set_step_id(m_step_.step_id());
      request.set_craned_id(m_config_.CranedIdOfThisNode);
      (void)stub->Abort(&context, request, &reply);
    }
    if (m_on_failure_) m_on_failure_();
  }
}
}  // namespace pmi

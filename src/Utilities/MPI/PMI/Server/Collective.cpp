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

#include "Server/State.h"
#include "Server/Utilities.h"
#include "crane/Logger.h"
#include "fmt/format.h"

namespace pmi {
size_t Server::Impl::LocalArrivals_() const {
  size_t arrived = 0;
  for (const auto& [rank, c] : m_clients_)
    if (c->waiting && (c->waiting->at("cmd") == "kvs-fence" ||
                       c->waiting->at("cmd") == "barrier_in"))
      ++arrived;
  return arrived;
}

void Server::Impl::StartFenceDeadline_() {
  if (!m_fence_deadline_)
    m_fence_deadline_ = std::chrono::steady_clock::now() + m_timeout_;
  m_cv_.notify_all();
}

grpc::Status Server::Impl::Fence(
    grpc::ServerContext* context,
    const crane::grpc::mpi::pmi::FenceRequest* request,
    crane::grpc::mpi::pmi::FenceReply* reply) {
  std::unique_lock lock(m_mutex_);
  if (request->job_id() != m_step_.job_id() ||
      request->step_id() != m_step_.step_id() ||
      !m_children_.contains(request->craned_id()))
    return {grpc::StatusCode::INVALID_ARGUMENT,
            "unexpected step or tree member"};
  if (request->pmi_version() != 0 && ProtocolVersion_() != 0 &&
      request->pmi_version() != ProtocolVersion_())
    return {grpc::StatusCode::FAILED_PRECONDITION,
            "PMI protocol differs across step nodes"};
  if (m_completed_epoch_ == request->epoch()) {
    reply->set_ok(true);
    reply->mutable_kvs()->insert(m_completed_kvs_.begin(),
                                 m_completed_kvs_.end());
    return grpc::Status::OK;
  }
  if (request->epoch() != m_epoch_)
    return {grpc::StatusCode::FAILED_PRECONDITION, "unexpected fence epoch"};
  Fields incoming(request->kvs().begin(), request->kvs().end());
  for (const auto& [key, value] : incoming)
    if (key.empty() || key.size() >= kMaxKey || value.size() >= kMaxValue)
      return {grpc::StatusCode::INVALID_ARGUMENT, "invalid KVS entry"};
  auto [it, inserted] =
      m_contributions_.emplace(request->craned_id(), incoming);
  if (!inserted && it->second != incoming)
    return {grpc::StatusCode::INVALID_ARGUMENT, "conflicting fence retry"};
  StartFenceDeadline_();
  auto deadline = std::chrono::steady_clock::now() + m_timeout_;
  if (!m_cv_.wait_until(lock, deadline, [&] {
        return m_failed_ || m_stopping_ ||
               m_completed_epoch_ == request->epoch();
      }))
    Fail_("child fence timed out");
  if (m_failed_ || m_stopping_ || context->IsCancelled())
    return {grpc::StatusCode::CANCELLED, "step fence did not complete"};
  reply->set_ok(true);
  reply->mutable_kvs()->insert(m_completed_kvs_.begin(),
                               m_completed_kvs_.end());
  return grpc::Status::OK;
}

grpc::Status Server::Impl::Abort(
    grpc::ServerContext*, const crane::grpc::mpi::pmi::AbortRequest* request,
    crane::grpc::mpi::pmi::AbortReply* reply) {
  std::lock_guard lock(m_mutex_);
  if (request->job_id() != m_step_.job_id() ||
      request->step_id() != m_step_.step_id() ||
      !m_peers_.contains(request->craned_id()))
    return {grpc::StatusCode::INVALID_ARGUMENT, "unexpected step or peer"};
  Fail_("peer reported a step failure");
  reply->set_ok(true);
  return grpc::Status::OK;
}

void Server::Impl::FenceLoop_() {
  std::unique_lock lock(m_mutex_);
  while (!m_stopping_ && !m_failed_) {
    m_cv_.wait_for(lock, std::chrono::milliseconds(100), [&] {
      return m_stopping_ || m_failed_ ||
             (LocalArrivals_() == m_local_size_ &&
              m_contributions_.size() == m_children_.size() &&
              m_peers_.size() + 1 ==
                  static_cast<size_t>(m_step_.nodelist_size()));
    });
    if (m_stopping_ || m_failed_) break;
    if (m_fence_deadline_ &&
        std::chrono::steady_clock::now() >= *m_fence_deadline_) {
      Fail_("step fence timed out");
      break;
    }
    if (LocalArrivals_() != m_local_size_ ||
        m_contributions_.size() != m_children_.size() ||
        m_peers_.size() + 1 != static_cast<size_t>(m_step_.nodelist_size()))
      continue;
    auto round = m_epoch_;
    auto delta = m_pending_kvs_;
    for (const auto& [id, data] : m_contributions_)
      for (const auto& [key, value] : data) delta[key] = value;
    if (m_parent_) {
      auto stub = m_peers_.at(*m_parent_);
      crane::grpc::mpi::pmi::FenceRequest request;
      crane::grpc::mpi::pmi::FenceReply reply;
      request.set_job_id(m_step_.job_id());
      request.set_step_id(m_step_.step_id());
      request.set_craned_id(m_config_.CranedIdOfThisNode);
      request.set_epoch(round);
      request.set_pmi_version(ProtocolVersion_());
      request.mutable_kvs()->insert(delta.begin(), delta.end());
      m_outgoing_context_ = std::make_unique<grpc::ClientContext>();
      m_outgoing_context_->set_deadline(std::chrono::system_clock::now() +
                                        m_timeout_);
      auto* context = m_outgoing_context_.get();
      lock.unlock();
      auto status = stub->Fence(context, request, &reply);
      lock.lock();
      m_outgoing_context_.reset();
      if (m_stopping_ || m_failed_) break;
      if (!status.ok() || !reply.ok()) {
        Fail_("parent fence failed");
        break;
      }
      m_visible_kvs_ = Fields(reply.kvs().begin(), reply.kvs().end());
    } else {
      for (const auto& [key, value] : delta) m_visible_kvs_[key] = value;
    }
    // Publish the complete snapshot before releasing either local clients or
    // child RPCs. KVS_Put data is intentionally invisible until this point.
    m_completed_kvs_ = m_visible_kvs_;
    m_completed_epoch_ = round;
    ++m_epoch_;
    m_pending_kvs_.clear();
    m_contributions_.clear();
    m_fence_deadline_.reset();
    for (auto& [rank, c] : m_clients_) {
      Reply_(*c, *c->waiting, {});
      c->waiting.reset();
    }
    m_cv_.notify_all();
  }
}
}  // namespace pmi

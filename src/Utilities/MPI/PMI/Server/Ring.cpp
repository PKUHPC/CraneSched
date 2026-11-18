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

namespace pmi {
void Server::Impl::WaitForRing_(Client& client, const Fields& request) {
  auto count = request.find("ring-count");
  auto left = request.find("ring-left");
  auto right = request.find("ring-right");
  if (count == request.end() || ParseNumber(count->second) != 1 ||
      left == request.end() || right == request.end() ||
      left->second != right->second || left->second.size() >= kMaxValue) {
    Reply_(client, request, {{"errmsg", "invalid ring contribution"}}, 3);
    return;
  }
  client.waiting = request;
  client.deadline = std::chrono::steady_clock::now() + m_timeout_;
  if (!m_ring_.deadline) m_ring_.deadline = client.deadline;
  m_cv_.notify_all();
}

grpc::Status Server::Impl::Ring(
    grpc::ServerContext* context,
    const crane::grpc::mpi::pmi::RingRequest* request,
    crane::grpc::mpi::pmi::RingReply* reply) {
  std::unique_lock lock(m_mutex_);
  auto child = m_child_task_counts_.find(request->craned_id());
  if (request->job_id() != m_step_.job_id() ||
      request->step_id() != m_step_.step_id() ||
      child == m_child_task_counts_.end() ||
      request->count() != child->second ||
      request->left().size() >= kMaxValue ||
      request->right().size() >= kMaxValue)
    return {grpc::StatusCode::INVALID_ARGUMENT, "invalid ring subtree"};
  if (request->pmi_version() != 2 ||
      (ProtocolVersion_() != 0 && ProtocolVersion_() != 2))
    return {grpc::StatusCode::FAILED_PRECONDITION, "ring requires PMI-2"};
  if (m_failed_ || m_stopping_)
    return {grpc::StatusCode::CANCELLED, "step ring did not complete"};
  RingValue incoming{request->count(), request->left(), request->right()};
  if (m_ring_.completed == request->epoch()) {
    if (m_ring_.completed_contributions.at(request->craned_id()) != incoming)
      return {grpc::StatusCode::INVALID_ARGUMENT, "conflicting ring retry"};
  } else {
    if (request->epoch() != m_ring_.epoch)
      return {grpc::StatusCode::FAILED_PRECONDITION, "unexpected ring epoch"};
    auto [it, inserted] =
        m_ring_.contributions.emplace(request->craned_id(), incoming);
    if (!inserted && it->second != incoming)
      return {grpc::StatusCode::INVALID_ARGUMENT, "conflicting ring retry"};
    const auto deadline = std::chrono::steady_clock::now() + m_timeout_;
    if (!m_ring_.deadline) m_ring_.deadline = deadline;
    m_cv_.notify_all();
    if (!m_cv_.wait_until(lock, deadline, [&] {
          return m_failed_ || m_stopping_ ||
                 m_ring_.completed == request->epoch();
        }))
      Fail_("child ring timed out");
  }
  if (m_failed_ || m_stopping_ || context->IsCancelled())
    return {grpc::StatusCode::CANCELLED, "step ring did not complete"};
  const auto& result = m_ring_.results.at(request->craned_id());
  reply->set_ok(true);
  reply->set_rank(result.rank);
  reply->set_left(result.left);
  reply->set_right(result.right);
  return grpc::Status::OK;
}

void Server::Impl::CompleteRing_(std::unique_lock<std::mutex>& lock) {
  if (ProtocolVersion_() != 2) {
    Fail_("ring requires PMI-2 on every step node");
    return;
  }
  struct Segment {
    RingValue value;
    Client* client;
    CranedId child;
  };
  // Local tasks precede child subtrees, as in Slurm's scan. Ring ranks are
  // deliberately independent of the application's PMI ranks.
  std::vector<Segment> segments;
  segments.reserve(m_local_size_ + m_child_order_.size());
  uint32_t total = 0;
  for (auto& [rank, client] : m_clients_) {
    const auto& value = client->waiting->at("ring-left");
    segments.push_back({{1, value, value}, client.get(), {}});
    ++total;
  }
  for (const auto& child : m_child_order_) {
    const auto& value = m_ring_.contributions.at(child);
    segments.push_back({value, nullptr, child});
    total += value.count;
  }
  RingResult outer{0, segments.back().value.right, segments.front().value.left};
  if (m_parent_) {
    auto stub = m_peers_.at(*m_parent_);
    crane::grpc::mpi::pmi::RingRequest request;
    crane::grpc::mpi::pmi::RingReply reply;
    request.set_job_id(m_step_.job_id());
    request.set_step_id(m_step_.step_id());
    request.set_craned_id(m_config_.CranedIdOfThisNode);
    request.set_epoch(m_ring_.epoch);
    request.set_pmi_version(2);
    request.set_count(total);
    request.set_left(segments.front().value.left);
    request.set_right(segments.back().value.right);
    m_outgoing_context_ = std::make_unique<grpc::ClientContext>();
    m_outgoing_context_->set_deadline(std::chrono::system_clock::now() +
                                      m_timeout_);
    auto* context = m_outgoing_context_.get();
    lock.unlock();
    auto status = stub->Ring(context, request, &reply);
    lock.lock();
    m_outgoing_context_.reset();
    if (m_stopping_ || m_failed_) return;
    const auto size = static_cast<uint32_t>(m_step_.task_node_list_size());
    if (!status.ok() || !reply.ok() || reply.rank() > size ||
        total > size - reply.rank() || reply.left().size() >= kMaxValue ||
        reply.right().size() >= kMaxValue) {
      Fail_("parent ring failed");
      return;
    }
    outer = {reply.rank(), reply.left(), reply.right()};
  }
  m_ring_.results.clear();
  uint32_t rank = outer.rank;
  for (size_t i = 0; i < segments.size(); ++i) {
    const auto& segment = segments[i];
    RingResult result{
        rank, i == 0 ? outer.left : segments[i - 1].value.right,
        i + 1 == segments.size() ? outer.right : segments[i + 1].value.left};
    if (segment.client) {
      Reply_(*segment.client, *segment.client->waiting,
             {{"ring-count", std::to_string(result.rank)},
              {"ring-left", result.left},
              {"ring-right", result.right}});
      segment.client->waiting.reset();
    } else {
      m_ring_.results.emplace(segment.child, std::move(result));
    }
    rank += segment.value.count;
  }
  m_ring_.completed = m_ring_.epoch++;
  m_ring_.completed_contributions = std::move(m_ring_.contributions);
  m_ring_.contributions.clear();
  m_ring_.deadline.reset();
  m_cv_.notify_all();
}
}  // namespace pmi

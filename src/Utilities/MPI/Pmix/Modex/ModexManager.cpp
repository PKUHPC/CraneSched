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

#include "Modex/ModexManager.h"

#ifdef HAVE_PMIX
#  include <pmix_server.h>
#endif
#include <mutex>
#include <optional>
#include <unordered_map>

#include "Transport/PmixClient.h"

namespace pmix {
#ifdef HAVE_PMIX
namespace {
struct PendingRequest {
  CranedId peer;
  std::chrono::steady_clock::time_point created;
  pmix_modex_cbfunc_t callback;
  void* data;
};

void Complete(const PendingRequest& request, pmix_status_t status) {
  PmixLibModexInvoke(request.callback, status, nullptr, 0, request.data,
                     nullptr, nullptr);
}

struct LocalRequest {
  uint32_t sequence;
  CranedId peer;
  CranedId sender;
  std::shared_ptr<PmixStub> stub;
};

void LocalRequestCompleted(pmix_status_t status, char* data, size_t size,
                           void* context) {
  std::unique_ptr<LocalRequest> request(static_cast<LocalRequest*>(context));
  crane::grpc::mpi::pmix::PmixDModexResponseReq reply;
  reply.set_seq_num(request->sequence);
  reply.set_craned_id(request->sender);
  reply.set_status(status);
  if (data && size) reply.set_data(data, size);
  request->stub->PmixDModexResponseNoBlock(
      reply, [peer = request->peer](bool ok) {
        if (!ok)
          CRANE_ERROR("Failed to send direct modex response to {}", peer);
      });
}
}  // namespace

struct ModexManager::PendingRequests {
  std::mutex mutex;
  uint32_t sequence{0};
  bool stopping{false};
  std::unordered_map<uint32_t, PendingRequest> requests;

  std::optional<PendingRequest> Take(uint32_t id,
                                     std::string_view sender = {}) {
    std::lock_guard lock(mutex);
    auto it = requests.find(id);
    if (it == requests.end() || (!sender.empty() && it->second.peer != sender))
      return std::nullopt;
    auto request = std::move(it->second);
    requests.erase(it);
    return request;
  }
};

ModexManager::ModexManager(const StepInfo& step_info, PmixClient* client,
                           std::chrono::seconds timeout)
    : m_step_info_(step_info),
      m_client_(client),
      m_timeout_(timeout),
      m_requests_(std::make_shared<PendingRequests>()) {}

ModexManager::~ModexManager() { DrainAllRequests(); }

bool ModexManager::Get(const std::string& nspace, int rank,
                       pmix_modex_cbfunc_t callback, void* data) {
  if (!callback || !m_client_ || nspace != m_step_info_.nspace || rank < 0 ||
      static_cast<size_t>(rank) >= m_step_info_.task_map.size())
    return false;
  auto node = m_step_info_.task_map[rank];
  if (node >= m_step_info_.node_list.size()) return false;
  const auto& peer = m_step_info_.node_list[node];
  auto stub = m_client_->GetPmixStub(peer);
  if (!stub) return false;
  uint32_t sequence;
  {
    std::lock_guard lock(m_requests_->mutex);
    if (m_requests_->stopping) return false;
    sequence = m_requests_->sequence++;
    if (!m_requests_->requests
             .try_emplace(sequence,
                          PendingRequest{peer, std::chrono::steady_clock::now(),
                                         callback, data})
             .second)
      return false;
  }
  crane::grpc::mpi::pmix::PmixDModexRequestReq request;
  request.set_seq_num(sequence);
  request.set_craned_id(m_step_info_.craned_id);
  request.set_local_namespace(nspace);
  request.mutable_pmix_proc()->set_nspace(nspace);
  request.mutable_pmix_proc()->set_rank(rank);
  // A late transport callback owns only shared pending state, never this
  // manager.
  stub->PmixDModexRequestNoBlock(request,
                                 [pending = m_requests_, sequence](bool ok) {
                                   if (!ok) {
                                     if (auto request = pending->Take(sequence))
                                       Complete(*request, PMIX_ERROR);
                                   }
                                 });
  return true;
}

void ModexManager::HandleRequest(uint32_t sequence, const CranedId& sender,
                                 const pmix_proc_t& process,
                                 const std::string& nspace) {
  if (nspace != m_step_info_.nspace || process.nspace != m_step_info_.nspace) {
    ResponseWithError_(sequence, sender, PMIX_ERR_INVALID_NAMESPACE);
    return;
  }
  if (process.rank >= m_step_info_.task_map.size() ||
      m_step_info_.task_map[process.rank] != m_step_info_.node_id) {
    ResponseWithError_(sequence, sender, PMIX_ERR_BAD_PARAM);
    return;
  }
  auto stub = m_client_->GetPmixStub(sender);
  if (!stub) return;
  auto* context = new LocalRequest{sequence, sender, m_step_info_.craned_id,
                                   std::move(stub)};
  auto status =
      PMIx_server_dmodex_request(&process, LocalRequestCompleted, context);
  if (status != PMIX_SUCCESS) {
    delete context;
    ResponseWithError_(sequence, sender, status);
  }
}

void ModexManager::HandleResponse(uint32_t sequence, const CranedId& sender,
                                  const std::string& data,
                                  pmix_status_t status) {
  auto request = m_requests_->Take(sequence, sender);
  if (!request) return;
  if (status != PMIX_SUCCESS || data.empty()) {
    Complete(*request, status);
    return;
  }
  // PMIx may retain data until its release callback; it cannot borrow an RPC
  // buffer.
  auto payload = std::make_unique<std::string>(data);
  const char* bytes = payload->data();
  const size_t size = payload->size();
  PmixLibModexInvoke(
      request->callback, status, bytes, size, request->data,
      [](void* context) { delete static_cast<std::string*>(context); },
      payload.release());
}

void ModexManager::ResponseWithError_(uint32_t sequence, const CranedId& peer,
                                      pmix_status_t status) {
  auto stub = m_client_->GetPmixStub(peer);
  if (!stub) return;
  crane::grpc::mpi::pmix::PmixDModexResponseReq reply;
  reply.set_seq_num(sequence);
  reply.set_craned_id(m_step_info_.craned_id);
  reply.set_status(status);
  stub->PmixDModexResponseNoBlock(reply, [peer](bool ok) {
    if (!ok) CRANE_ERROR("Failed to send direct modex error to {}", peer);
  });
}

void ModexManager::CleanupTimeoutRequests() {
  std::vector<PendingRequest> expired;
  const auto now = std::chrono::steady_clock::now();
  {
    std::lock_guard lock(m_requests_->mutex);
    std::erase_if(m_requests_->requests, [&](const auto& item) {
      if (now - item.second.created < m_timeout_) return false;
      expired.push_back(item.second);
      return true;
    });
  }
  for (const auto& request : expired) Complete(request, PMIX_ERR_TIMEOUT);
}

void ModexManager::DrainAllRequests() {
  std::unordered_map<uint32_t, PendingRequest> pending;
  {
    std::lock_guard lock(m_requests_->mutex);
    m_requests_->stopping = true;
    pending.swap(m_requests_->requests);
  }
  for (const auto& [sequence, request] : pending)
    Complete(request, PMIX_ERR_TIMEOUT);
}
#endif
}  // namespace pmix

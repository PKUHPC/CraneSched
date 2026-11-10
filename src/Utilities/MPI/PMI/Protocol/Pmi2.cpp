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

namespace pmi {
void Server::Impl::Request_(Client& client, const Fields& request) {
  const auto& command = request.at("cmd");
  if (command == "fullinit" && !client.initialized) {
    Pmi2Initialize_(client, request);
    return;
  }
  if (!client.initialized || client.finalized || client.waiting) {
    Fail_("request out of order");
    return;
  }
  using Handler = void (Impl::*)(Client&, const Fields&);
  static const std::map<std::string, Handler> handlers{
      {"finalize", &Impl::Finalize_},
      {"abort", &Impl::Pmi2Abort_},
      {"job-getid", &Impl::Pmi2JobId_},
      {"kvs-put", &Impl::Pmi2Put_},
      {"info-putnodeattr", &Impl::Pmi2Put_},
      {"kvs-get", &Impl::Pmi2Get_},
      {"kvs-fence", &Impl::WaitForFence_},
      {"info-getjobattr", &Impl::Pmi2Attribute_},
      {"info-getnodeattr", &Impl::Pmi2Attribute_}};
  auto handler = handlers.find(command);
  if (handler == handlers.end()) {
    Reply_(client, request, {{"errmsg", "operation not supported"}}, 14);
    return;
  }
  (this->*handler->second)(client, request);
}

void Server::Impl::Pmi2Initialize_(Client& client, const Fields& request) {
  auto rank = request.find("pmirank");
  if (rank == request.end() || ParseNumber(rank->second) != client.rank) {
    Reply_(client, request,
           {{"errmsg", "rank does not match the assigned channel"}}, 3);
    return;
  }
  client.initialized = true;
  Reply_(client, request,
         {{"pmi-version", "2"},
          {"pmi-subversion", "0"},
          {"rank", std::to_string(client.rank)},
          {"size", std::to_string(m_step_.task_node_list_size())},
          {"appnum", "0"},
          {"debugged", "FALSE"},
          {"pmiverbose", "FALSE"}});
}

void Server::Impl::Pmi2Abort_(Client&, const Fields&) {
  Fail_("client requested Abort");
}
void Server::Impl::Pmi2JobId_(Client& client, const Fields& request) {
  Reply_(client, request, {{"jobid", m_jobid_}});
}
void Server::Impl::Pmi2Put_(Client& client, const Fields& request) {
  if (!request.contains("key") || !request.contains("value") ||
      request.at("key").empty() || request.at("key").size() >= kMaxKey ||
      request.at("value").size() >= kMaxValue) {
    Reply_(client, request, {{"errmsg", "invalid key or value length"}}, 3);
    return;
  }
  const bool node_attribute = request.at("cmd") == "info-putnodeattr";
  auto& destination = node_attribute ? m_node_attrs_ : m_pending_kvs_;
  destination[request.at("key")] = request.at("value");
  Reply_(client, request, {});
  if (!node_attribute) return;
  for (auto& [rank, waiting] : m_clients_) {
    if (waiting->waiting && waiting->waiting->at("cmd") == "info-getnodeattr" &&
        m_node_attrs_.contains(waiting->waiting->at("key"))) {
      Attribute_(*waiting, *waiting->waiting, m_node_attrs_);
      waiting->waiting.reset();
    }
  }
}
void Server::Impl::Pmi2Get_(Client& client, const Fields& request) {
  if (!request.contains("key") || !request.contains("jobid") ||
      request.at("jobid") != m_jobid_) {
    Reply_(client, request, {{"errmsg", "invalid job or key"}}, 3);
    return;
  }
  auto it = m_visible_kvs_.find(request.at("key"));
  if (it == m_visible_kvs_.end())
    Reply_(client, request, {{"errmsg", "key not found"}}, 4);
  else
    Reply_(client, request, {{"value", it->second}, {"found", "TRUE"}});
}
void Server::Impl::Pmi2Attribute_(Client& client, const Fields& request) {
  if (!request.contains("key")) {
    Reply_(client, request, {}, 3);
    return;
  }
  if (request.at("cmd") == "info-getjobattr") {
    Attribute_(client, request, m_job_attrs_);
    return;
  }
  if (!m_node_attrs_.contains(request.at("key")) && request.contains("wait") &&
      request.at("wait") == "TRUE") {
    client.waiting = request;
    client.deadline = std::chrono::steady_clock::now() + m_timeout_;
    return;
  }
  Attribute_(client, request, m_node_attrs_);
}

}  // namespace pmi

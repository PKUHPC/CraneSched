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
void Server::Impl::Pmi1InvalidRequest_(Client& c, const pmi1::Fields& request) {
  Reply_(c, request, {{"errmsg", "invalid_argument"}}, 3);
}

void Server::Impl::Pmi1GetMaxes_(Client& c, const pmi1::Fields& request) {
  Reply_(
      c, request,
      {{"kvsname_max", "256"}, {"keylen_max", "64"}, {"vallen_max", "1024"}});
}

void Server::Impl::Pmi1GetUniverseSize_(Client& c,
                                        const pmi1::Fields& request) {
  Reply_(c, request, {{"size", std::to_string(m_step_.task_node_list_size())}});
}

void Server::Impl::Pmi1GetAppnum_(Client& c, const pmi1::Fields& request) {
  Reply_(c, request, {{"appnum", "-1"}});
}

void Server::Impl::Pmi1GetKvsName_(Client& c, const pmi1::Fields& request) {
  Reply_(c, request, {{"kvsname", m_jobid_}});
}

void Server::Impl::Pmi1Put_(Client& c, const pmi1::Fields& request) {
  if (!request.contains("key") || !request.contains("value") ||
      request.at("key").empty() || request.at("key").size() >= kMaxKey ||
      request.at("value").size() >= kMaxValue) {
    Pmi1InvalidRequest_(c, request);
    return;
  }
  m_pending_kvs_[request.at("key")] = request.at("value");
  Reply_(c, request, {});
}

void Server::Impl::Pmi1Get_(Client& c, const pmi1::Fields& request) {
  if (!request.contains("key") || request.at("key").empty() ||
      request.at("key").size() >= kMaxKey) {
    Pmi1InvalidRequest_(c, request);
    return;
  }
  auto it = m_visible_kvs_.find(request.at("key"));
  if (it == m_visible_kvs_.end())
    Reply_(c, request, {}, 1);
  else
    Reply_(c, request, {{"value", it->second}});
}

void Server::Impl::Pmi1Abort_(Client&, const pmi1::Fields&) {
  Fail_("PMI-1 client requested Abort");
}

void Server::Impl::RequestPmi11_(Client& c, const pmi1::Fields& request) {
  if (!c.initialized || c.finalized || c.waiting) {
    Fail_("PMI-1 request out of order");
    return;
  }
  using Handler = void (Impl::*)(Client&, const pmi1::Fields&);
  static const std::map<std::string, Handler> handlers{
      {"get_maxes", &Impl::Pmi1GetMaxes_},
      {"get_universe_size", &Impl::Pmi1GetUniverseSize_},
      {"get_appnum", &Impl::Pmi1GetAppnum_},
      {"get_my_kvsname", &Impl::Pmi1GetKvsName_},
      {"put", &Impl::Pmi1Put_},
      {"get", &Impl::Pmi1Get_},
      {"barrier_in", &Impl::WaitForFence_},
      {"finalize", &Impl::Finalize_},
      {"abort", &Impl::Pmi1Abort_}};
  auto handler = handlers.find(request.at("cmd"));
  if (handler == handlers.end()) {
    Reply_(c, request, {{"errmsg", "unsupported_command"}}, 14);
    return;
  }
  (this->*handler->second)(c, request);
}
}  // namespace pmi

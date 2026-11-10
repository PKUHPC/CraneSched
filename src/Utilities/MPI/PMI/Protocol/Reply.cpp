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
void Server::Impl::Reply_(Client& c, const Fields& request, Fields fields,
                          int rc) {
  const auto& command = request.at("cmd");
  if (c.protocol == Protocol::kPmi11) {
    static const std::map<std::string, std::string> responses{
        {"get_maxes", "maxes"},        {"get_universe_size", "universe_size"},
        {"get_appnum", "appnum"},      {"get_my_kvsname", "my_kvsname"},
        {"put", "put_result"},         {"get", "get_result"},
        {"barrier_in", "barrier_out"}, {"finalize", "finalize_ack"}};
    auto response = responses.find(command);
    std::string_view response_command = "error";
    if (response == responses.end()) {
      fields["errmsg"] = "unsupported_command";
      if (rc == 0) rc = 14;
    } else {
      response_command = response->second;
    }
    fields["rc"] = std::to_string(rc);
    auto line = pmi1::Codec::EncodeLine(response_command, fields);
    if (!line || c.output.size() + line->size() > 2 * kMaxFrame) {
      Fail_("PMI-1 response exceeds the protocol limit");
      return;
    }
    c.output += *line;
    Wake_();
    return;
  }

  fields["cmd"] = command + "-response";
  fields["rc"] = std::to_string(rc);
  if (auto it = request.find("thrid"); it != request.end())
    fields["thrid"] = it->second;
  auto body = crane::pmi2::Codec::EncodeFrame(fields);
  if (!body || c.output.size() + 6 + body->size() > 2 * kMaxFrame) {
    Fail_("PMI-2 response exceeds the protocol limit");
    return;
  }
  c.output += fmt::format("{:6}", body->size());
  c.output += *body;
  Wake_();
}

void Server::Impl::Attribute_(Client& c, const Fields& request,
                              const Fields& attributes) {
  auto it = attributes.find(request.at("key"));
  Fields response{{"found", it == attributes.end() ? "FALSE" : "TRUE"}};
  if (it != attributes.end()) response["value"] = it->second;
  Reply_(c, request, std::move(response));
}
void Server::Impl::WaitForFence_(Client& c, const pmi1::Fields& request) {
  c.waiting = Fields(request.begin(), request.end());
  c.deadline = std::chrono::steady_clock::now() + m_timeout_;
  StartFenceDeadline_();
}

void Server::Impl::Finalize_(Client& c, const pmi1::Fields& request) {
  c.finalized = true;
  c.closing = true;
  Reply_(c, request, {});
}

}  // namespace pmi

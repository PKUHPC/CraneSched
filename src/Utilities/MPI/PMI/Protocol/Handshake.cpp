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
void Server::Impl::Decode_(Client& c) {
  while (!c.input.empty() && !m_failed_ && !c.closing) {
    if (!c.negotiated) {
      auto end = c.input.find('\n');
      if (end == std::string::npos) {
        if (c.input.size() > 1024) Fail_("oversized PMI initialization");
        return;
      }
      auto request =
          pmi1::Codec::ParseLine(std::string_view(c.input).substr(0, end + 1));
      if (!request || request->at("cmd") != "init") {
        Fail_("invalid PMI initialization");
        return;
      }
      auto version = request->contains("pmi_version")
                         ? ParseNumber(request->at("pmi_version"))
                         : std::nullopt;
      auto subversion = request->contains("pmi_subversion")
                            ? ParseNumber(request->at("pmi_subversion"))
                            : std::nullopt;
      Protocol requested_protocol = Protocol::kPmi20;
      bool supported = false;
      if (version && subversion && *version == 1 && *subversion == 1) {
        requested_protocol = Protocol::kPmi11;
        supported = true;
      } else if (version && subversion && *version == 2 && *subversion == 0) {
        supported = true;
      }
      const bool version_supported = supported;
      const bool consistent = !version_supported || !m_protocol_ ||
                              *m_protocol_ == requested_protocol;
      supported = version_supported && consistent;
      if (supported) {
        m_protocol_ = requested_protocol;
        c.protocol = requested_protocol;
        c.negotiated = true;
        c.initialized = requested_protocol == Protocol::kPmi11;
      }
      if (!consistent) Fail_("inconsistent PMI protocol versions in step");
      uint32_t response_version = version.value_or(2);
      uint32_t response_subversion = subversion.value_or(0);
      if (!consistent && m_protocol_) {
        response_version = static_cast<uint32_t>(*m_protocol_);
        response_subversion = *m_protocol_ == Protocol::kPmi11 ? 1 : 0;
      }
      auto response = pmi1::Codec::EncodeLine(
          "response_to_init",
          {{"rc", supported ? "0" : "1"},
           {"pmi_version", std::to_string(response_version)},
           {"pmi_subversion", std::to_string(response_subversion)}});
      if (!response || c.output.size() + response->size() > 2 * kMaxFrame) {
        Fail_("invalid PMI initialization response");
        return;
      }
      c.output += *response;
      c.input.erase(0, end + 1);
      c.closing = !supported;
    } else {
      if (c.protocol == Protocol::kPmi11) {
        auto end = c.input.find('\n');
        if (end == std::string::npos) {
          if (c.input.size() > pmi1::Codec::kMaxLine)
            Fail_("oversized PMI-1 request");
          return;
        }
        auto request = pmi1::Codec::ParseLine(
            std::string_view(c.input).substr(0, end + 1));
        if (!request) {
          Fail_("invalid PMI-1 fields");
          return;
        }
        c.input.erase(0, end + 1);
        RequestPmi11_(c, *request);
        continue;
      }
      if (c.input.size() < 6) return;
      auto header = std::string_view(c.input).substr(0, 6);
      while (!header.empty() && header.front() == ' ') header.remove_prefix(1);
      while (!header.empty() && header.back() == ' ') header.remove_suffix(1);
      auto length = ParseNumber(header);
      if (!length || *length == 0 || *length > kMaxFrame) {
        Fail_("invalid PMI-2 frame length");
        return;
      }
      if (c.input.size() < 6 + *length) return;
      auto request = crane::pmi2::Codec::ParseFrame(
          std::string_view(c.input).substr(6, *length));
      if (!request) {
        Fail_("invalid PMI-2 fields");
        return;
      }
      c.input.erase(0, 6 + *length);
      Request_(c, *request);
    }
  }
}
}  // namespace pmi

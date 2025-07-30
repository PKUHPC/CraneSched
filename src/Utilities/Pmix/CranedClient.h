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

#pragma once

#include "crane/Lock.h"
#include "crane/Network.h"
#include "crane/PublicHeader.h"
#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"

namespace pmix {

class CranedClient {
public:
  CranedClient() = default;

  ~CranedClient() = default;

  void InitChannelAndStub(const std::string& endpoint);

  bool TerminateTasks();

  bool BroadcastPmixPort(uint32_t pmix_port);

private:
  std::shared_ptr<grpc::Channel> m_channel_;

  std::unique_ptr<crane::grpc::Craned::Stub> m_stub_;
};

} // namespace pmix
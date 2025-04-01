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



#include <vector>
#include <fmt/format.h>

#include "PmixComment.h"

#include "crane/Logger.h"
#include "protos/Crane.grpc.pb.h"

namespace pmix {

class PmixServer {
 public:
  PmixServer() = default;

  ~PmixServer();

  bool Init(const crane::grpc::TaskToD& task, const std::unordered_map<std::string, std::string>& env_map);

  std::optional<std::unordered_map<std::string, std::string>> SetupFork(uint32_t rank);

private:
  uint32_t m_uid_;
  uint32_t m_gid_;
  std::string m_nspace_; // crane.pmix.jobid
  std::string hostname;
  std::string m_server_tmpdir_;
  uint32_t m_nprocs_;
  std::string m_hostname_;
  uint32_t m_node_id_;
  std::string m_node_list_;

  void InfoSet_(const crane::grpc::TaskToD& task, const std::unordered_map<std::string, std::string>& env_map);
};



}
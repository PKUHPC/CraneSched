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

#include <parallel_hashmap/phmap.h>

#include <condition_variable>

#include "PmixColl.h"
#include "absl/container/node_hash_map.h"
#include "crane/Network.h"
#include "crane/PublicHeader.h"

namespace pmix {

using AsyncCallback = std::function<void(bool)>;

class PmixStub {
 public:
  virtual ~PmixStub() = default;

  virtual void SendPmixRingMsgNoBlock(
      const crane::grpc::pmix::SendPmixRingMsgReq &request, AsyncCallback callback) = 0;

  virtual void PmixTreeUpwardForwardNoBlock(
      const crane::grpc::pmix::PmixTreeUpwardForwardReq &request, AsyncCallback callback) = 0;

  virtual void PmixTreeDownwardForwardNoBlock(
      const crane::grpc::pmix::PmixTreeDownwardForwardReq &request, AsyncCallback callback) = 0;

  virtual void PmixDModexRequestNoBlock(
      const crane::grpc::pmix::PmixDModexRequestReq &request, AsyncCallback callback) = 0;

  virtual void PmixDModexResponseNoBlock(
      const crane::grpc::pmix::PmixDModexResponseReq &request, AsyncCallback callback) = 0;
};

class PmixClient {
 public:
  explicit PmixClient(int node_num) : m_node_num_(node_num) {}

  virtual ~PmixClient() = default;

  virtual void EmplacePmixStub(const CranedId &craned_id,
                               const std::string &port) = 0;

  virtual std::shared_ptr<PmixStub> GetPmixStub(const CranedId &craned_id) = 0;

  uint64_t GetChannelCount() const { return m_channel_count_.load(); }

  virtual void WaitAllStubReady() = 0;

 private:
  std::mutex m_mutex_;
  std::condition_variable m_cv_;

  int m_node_num_;

  std::atomic_uint64_t m_channel_count_{0};
};

}  // namespace pmix
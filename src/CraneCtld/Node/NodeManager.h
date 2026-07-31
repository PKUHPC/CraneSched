/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

#pragma once

#include "CtldPublicDefs.h"
// Precompiled header comes first!

#include "protos/Crane.pb.h"

namespace Ctld {

class NodeManager final {
 public:
  using DynamicNodeRecord = crane::grpc::DynamicNodeRecord;

  bool Init();
  void RestoreDynamicNodes();

  crane::grpc::CreateNodesReply CreateNodes(
      const crane::grpc::CreateNodesRequest& request);
  crane::grpc::DeleteNodesReply DeleteNodes(
      const crane::grpc::DeleteNodesRequest& request);
  crane::grpc::QueryDynamicNodeConfigReply QueryDynamicNodeConfig(
      const crane::grpc::QueryDynamicNodeConfigRequest& request);

  std::expected<bool, std::string> BeginRegistration(const CranedId& node_id,
                                                     uint64_t generation,
                                                     const RegToken& token);
  std::expected<void, std::string> ValidateRegistration(
      const CranedId& node_id, uint64_t generation,
      const crane::grpc::CranedRemoteMeta& remote_meta);
  std::expected<void, std::string> MarkRegistered(const CranedId& node_id,
                                                  uint64_t generation);

 private:
  std::expected<void, std::string> ValidateRegistrationNoLock_(
      const CranedId& node_id, uint64_t generation) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  std::expected<void, std::string> ValidatePresentRecord_(
      const DynamicNodeRecord& record) const;

  absl::Mutex mutex_;
  std::unordered_map<CranedId, DynamicNodeRecord> records_
      ABSL_GUARDED_BY(mutex_);
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::NodeManager> g_node_manager;

/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

#include "Node/NodeManager.h"

#include "Database/EmbeddedDbClient.h"
#include "JobScheduler.h"
#include "Node/CranedMetaContainer.h"
#include "RpcService/CranedKeeper.h"
#include "crane/String.h"

namespace Ctld {

namespace {

using crane::grpc::DYNAMIC_NODE_RECORD_STATE_DELETING;
using crane::grpc::DYNAMIC_NODE_RECORD_STATE_PRESENT;
using crane::grpc::DYNAMIC_NODE_RECORD_STATE_TOMBSTONE;

std::expected<void, std::string> ValidateNodeName(const CranedId& node_id) {
  std::list<std::string> expanded;
  if (!util::ParseHostList(node_id, &expanded) || expanded.size() != 1 ||
      expanded.front() != node_id) {
    return std::unexpected(fmt::format("Invalid node name {}", node_id));
  }
  return {};
}

template <typename Range>
bool HasDuplicates(const Range& values) {
  std::unordered_set<std::string> unique;
  for (const auto& value : values) {
    if (!unique.emplace(value).second) return true;
  }
  return false;
}

}  // namespace

bool NodeManager::Init() {
  std::unordered_map<CranedId, DynamicNodeRecord> records;
  if (!g_embedded_db_client->RetrieveDynamicNodeRecords(&records)) return false;

  std::vector<DynamicNodeRecord> recovered_deletions;
  size_t present_node_count = 0;
  for (auto& [node_id, record] : records) {
    if (node_id != record.node_name()) {
      CRANE_ERROR("Dynamic node record key {} does not match node name {}.",
                  node_id, record.node_name());
      return false;
    }
    if (record.state() == DYNAMIC_NODE_RECORD_STATE_DELETING) {
      record.set_state(DYNAMIC_NODE_RECORD_STATE_TOMBSTONE);
      recovered_deletions.emplace_back(record);
      continue;
    }
    if (record.state() == DYNAMIC_NODE_RECORD_STATE_TOMBSTONE) continue;
    if (record.state() != DYNAMIC_NODE_RECORD_STATE_PRESENT) {
      CRANE_ERROR("Dynamic node {} has an invalid persistent state.", node_id);
      return false;
    }
    if (g_config.Nodes.contains(node_id)) {
      CRANE_ERROR("Dynamic node {} conflicts with a static node.", node_id);
      return false;
    }

    auto result = ValidatePresentRecord_(record);
    if (!result) {
      CRANE_ERROR("Invalid dynamic node {}: {}", node_id, result.error());
      return false;
    }
    ++present_node_count;
  }

  if (g_config.Nodes.size() + present_node_count >
      g_config.CtldConf.MaxNodeCount) {
    CRANE_ERROR(
        "Static and dynamic node count exceeds configured MaxNodeCount {}.",
        g_config.CtldConf.MaxNodeCount);
    return false;
  }
  if (!recovered_deletions.empty() &&
      !g_embedded_db_client->StoreDynamicNodeRecords(recovered_deletions)) {
    CRANE_ERROR("Failed to finish recovered dynamic node deletions.");
    return false;
  }

  absl::MutexLock lock(&mutex_);
  records_ = std::move(records);
  return true;
}

void NodeManager::RestoreDynamicNodes() {
  std::vector<DynamicNodeRecord> records;
  {
    absl::MutexLock lock(&mutex_);
    for (const auto& [node_id, record] : records_) {
      if (record.state() == DYNAMIC_NODE_RECORD_STATE_PRESENT)
        records.emplace_back(record);
    }
  }
  g_meta_container->AddDynamicNodes(records);
}

crane::grpc::CreateNodesReply NodeManager::CreateNodes(
    const crane::grpc::CreateNodesRequest& request) {
  crane::grpc::CreateNodesReply reply;
  absl::MutexLock lock(&mutex_);

  if (request.node_names().empty()) {
    reply.set_reason("No node name specified");
    return reply;
  }
  if (HasDuplicates(request.node_names())) {
    reply.set_reason("Duplicate node names are not allowed");
    return reply;
  }
  if (!request.has_spec() || request.spec().cpu_count() == 0 ||
      request.spec().memory_bytes() == 0 || request.spec().sockets() == 0 ||
      request.spec().sockets() > request.spec().cpu_count()) {
    reply.set_reason("CPU, memory and sockets must form a valid node spec");
    return reply;
  }
  if (request.partition_names().empty()) {
    reply.set_reason("At least one partition must be specified");
    return reply;
  }
  if (HasDuplicates(request.partition_names())) {
    reply.set_reason("Duplicate partitions are not allowed");
    return reply;
  }
  for (const auto& partition_id : request.partition_names()) {
    if (!g_config.Partitions.contains(partition_id)) {
      reply.set_reason(
          fmt::format("Partition {} does not exist", partition_id));
      return reply;
    }
  }

  size_t dynamic_node_count =
      std::ranges::count_if(records_, [](const auto& entry) {
        return entry.second.state() != DYNAMIC_NODE_RECORD_STATE_TOMBSTONE;
      });
  if (g_config.Nodes.size() + dynamic_node_count + request.node_names_size() >
      g_config.CtldConf.MaxNodeCount) {
    reply.set_reason(fmt::format("MaxNodeCount {} would be exceeded",
                                 g_config.CtldConf.MaxNodeCount));
    return reply;
  }

  std::vector<DynamicNodeRecord> new_records;
  new_records.reserve(request.node_names_size());
  for (const auto& node_id : request.node_names()) {
    auto name_result = ValidateNodeName(node_id);
    if (!name_result) {
      reply.set_reason(name_result.error());
      return reply;
    }
    if (g_config.Nodes.contains(node_id)) {
      reply.set_reason(fmt::format("Node {} is static", node_id));
      return reply;
    }

    uint64_t generation = 1;
    auto it = records_.find(node_id);
    if (it != records_.end()) {
      if (it->second.state() != DYNAMIC_NODE_RECORD_STATE_TOMBSTONE) {
        reply.set_reason(fmt::format("Node {} already exists", node_id));
        return reply;
      }
      generation = it->second.generation() + 1;
    }

    DynamicNodeRecord record;
    record.set_node_name(node_id);
    *record.mutable_spec() = request.spec();
    record.mutable_partition_names()->CopyFrom(request.partition_names());
    record.set_generation(generation);
    record.set_state(DYNAMIC_NODE_RECORD_STATE_PRESENT);
    new_records.emplace_back(std::move(record));
  }

  if (!g_embedded_db_client->StoreDynamicNodeRecords(new_records)) {
    reply.set_reason("Failed to persist dynamic nodes");
    return reply;
  }
  for (const auto& record : new_records) records_[record.node_name()] = record;
  g_meta_container->AddDynamicNodes(new_records);

  reply.set_ok(true);
  return reply;
}

crane::grpc::DeleteNodesReply NodeManager::DeleteNodes(
    const crane::grpc::DeleteNodesRequest& request) {
  crane::grpc::DeleteNodesReply reply;
  absl::MutexLock lock(&mutex_);

  if (request.node_names().empty()) {
    reply.set_reason("No node name specified");
    return reply;
  }
  if (HasDuplicates(request.node_names())) {
    reply.set_reason("Duplicate node names are not allowed");
    return reply;
  }

  std::vector<CranedId> node_ids(request.node_names().begin(),
                                 request.node_names().end());
  std::vector<CranedId> new_deletion_nodes;
  std::vector<DynamicNodeRecord> original_records;
  new_deletion_nodes.reserve(node_ids.size());
  original_records.reserve(node_ids.size());
  for (const auto& node_id : node_ids) {
    if (g_config.Nodes.contains(node_id)) {
      reply.set_reason(fmt::format("Node {} is static", node_id));
      return reply;
    }
    auto it = records_.find(node_id);
    if (it == records_.end()) {
      reply.set_reason(fmt::format("Node {} does not exist", node_id));
      return reply;
    }
    if (it->second.state() == DYNAMIC_NODE_RECORD_STATE_TOMBSTONE) {
      reply.set_reason(fmt::format("Node {} does not exist", node_id));
      return reply;
    }
    if (it->second.state() == DYNAMIC_NODE_RECORD_STATE_PRESENT)
      new_deletion_nodes.emplace_back(node_id);
    if (g_craned_keeper->IsCranedTracked(node_id)) {
      reply.set_reason(fmt::format("Node {} is still connected", node_id));
      return reply;
    }
    original_records.emplace_back(it->second);
  }
  if (g_job_scheduler->HasJobsOnNodes(node_ids)) {
    reply.set_reason("One or more nodes are still referenced by jobs");
    return reply;
  }

  if (!new_deletion_nodes.empty()) {
    auto result = g_meta_container->SetDynamicNodesDeleting(new_deletion_nodes);
    if (!result) {
      reply.set_reason(result.error());
      return reply;
    }
  }
  for (const auto& node_id : node_ids) {
    if (!g_craned_keeper->ForgetCraned(node_id)) {
      g_meta_container->ClearDynamicNodesDeleting(new_deletion_nodes);
      reply.set_reason(fmt::format("Node {} is still connected", node_id));
      return reply;
    }
  }

  std::vector<DynamicNodeRecord> deleting_records;
  deleting_records.reserve(original_records.size());
  for (auto record : original_records) {
    record.set_state(DYNAMIC_NODE_RECORD_STATE_DELETING);
    deleting_records.emplace_back(std::move(record));
  }
  if (!g_embedded_db_client->StoreDynamicNodeRecords(deleting_records)) {
    g_meta_container->ClearDynamicNodesDeleting(new_deletion_nodes);
    reply.set_reason("Failed to persist dynamic node deletion intent");
    return reply;
  }
  for (const auto& record : deleting_records)
    records_[record.node_name()] = record;

  std::vector<CranedId> runtime_nodes;
  for (const auto& node_id : node_ids) {
    if (g_meta_container->CheckCranedAllowed(node_id))
      runtime_nodes.emplace_back(node_id);
  }
  if (!runtime_nodes.empty()) {
    auto result = g_meta_container->RemoveDynamicNodes(runtime_nodes);
    if (!result) {
      if (g_embedded_db_client->StoreDynamicNodeRecords(original_records)) {
        for (const auto& record : original_records)
          records_[record.node_name()] = record;
        g_meta_container->ClearDynamicNodesDeleting(new_deletion_nodes);
        reply.set_reason(result.error());
      } else {
        reply.set_reason(
            fmt::format("{}; failed to restore dynamic node deletion state",
                        result.error()));
      }
      return reply;
    }
  }

  std::vector<DynamicNodeRecord> tombstones = deleting_records;
  for (auto& record : tombstones)
    record.set_state(DYNAMIC_NODE_RECORD_STATE_TOMBSTONE);
  if (!g_embedded_db_client->StoreDynamicNodeRecords(tombstones)) {
    reply.set_reason("Failed to persist dynamic node tombstones");
    return reply;
  }
  for (const auto& record : tombstones) records_[record.node_name()] = record;

  reply.set_ok(true);
  return reply;
}

crane::grpc::QueryDynamicNodeConfigReply NodeManager::QueryDynamicNodeConfig(
    const crane::grpc::QueryDynamicNodeConfigRequest& request) {
  crane::grpc::QueryDynamicNodeConfigReply reply;
  absl::MutexLock lock(&mutex_);
  auto it = records_.find(request.node_name());
  if (it == records_.end() ||
      it->second.state() != DYNAMIC_NODE_RECORD_STATE_PRESENT) {
    reply.set_reason("Dynamic node is not precreated");
    return reply;
  }

  reply.set_ok(true);
  *reply.mutable_spec() = it->second.spec();
  reply.mutable_partition_names()->CopyFrom(it->second.partition_names());
  reply.set_generation(it->second.generation());
  return reply;
}

std::expected<bool, std::string> NodeManager::BeginRegistration(
    const CranedId& node_id, uint64_t generation, const RegToken& token) {
  absl::MutexLock lock(&mutex_);
  auto result = ValidateRegistrationNoLock_(node_id, generation);
  if (!result) return std::unexpected(result.error());

  bool connected = g_craned_keeper->IsCranedConnected(node_id);
  if (!connected) g_craned_keeper->PutNodeIntoUnavailSet(node_id, token);
  return connected;
}

std::expected<void, std::string> NodeManager::ValidateRegistration(
    const CranedId& node_id, uint64_t generation,
    const crane::grpc::CranedRemoteMeta& remote_meta) {
  absl::MutexLock lock(&mutex_);
  auto result = ValidateRegistrationNoLock_(node_id, generation);
  if (!result) return result;

  auto it = records_.find(node_id);
  if (it == records_.end()) return {};
  uint32_t reported_sockets = remote_meta.node_topo_info().sockets();
  if (reported_sockets != it->second.spec().sockets()) {
    return std::unexpected(
        fmt::format("Socket count {} does not match precreated value {}",
                    reported_sockets, it->second.spec().sockets()));
  }
  return {};
}

std::expected<void, std::string> NodeManager::MarkRegistered(
    const CranedId& node_id, uint64_t generation) {
  absl::MutexLock lock(&mutex_);
  auto result = ValidateRegistrationNoLock_(node_id, generation);
  if (!result) return result;

  auto it = records_.find(node_id);
  if (it == records_.end() || it->second.ever_registered()) return {};

  DynamicNodeRecord record = it->second;
  record.set_ever_registered(true);
  if (!g_embedded_db_client->StoreDynamicNodeRecords({record}))
    return std::unexpected("Failed to persist dynamic node registration");

  it->second = std::move(record);
  g_meta_container->SetDynamicNodeRegistered(node_id);
  return {};
}

std::expected<void, std::string> NodeManager::ValidateRegistrationNoLock_(
    const CranedId& node_id, uint64_t generation) const {
  if (g_config.Nodes.contains(node_id)) {
    if (generation != 0)
      return std::unexpected("Static node generation must be 0");
    return {};
  }

  auto it = records_.find(node_id);
  if (it == records_.end() ||
      it->second.state() != DYNAMIC_NODE_RECORD_STATE_PRESENT)
    return std::unexpected("Dynamic node is not precreated");
  if (it->second.generation() != generation)
    return std::unexpected(fmt::format("Stale generation {}, expected {}",
                                       generation, it->second.generation()));
  return {};
}

std::expected<void, std::string> NodeManager::ValidatePresentRecord_(
    const DynamicNodeRecord& record) const {
  auto name_result = ValidateNodeName(record.node_name());
  if (!name_result) return name_result;
  if (!record.has_spec() || record.spec().cpu_count() == 0 ||
      record.spec().memory_bytes() == 0 || record.spec().sockets() == 0 ||
      record.spec().sockets() > record.spec().cpu_count())
    return std::unexpected("Invalid CPU, memory or sockets");
  if (record.generation() == 0)
    return std::unexpected("Dynamic node generation must be positive");
  if (record.partition_names().empty())
    return std::unexpected("No partition specified");
  if (HasDuplicates(record.partition_names()))
    return std::unexpected("Duplicate partitions are not allowed");
  for (const auto& partition_id : record.partition_names()) {
    if (!g_config.Partitions.contains(partition_id))
      return std::unexpected(
          fmt::format("Partition {} does not exist", partition_id));
  }
  return {};
}

}  // namespace Ctld

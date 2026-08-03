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

#include <map>
#include <random>
#include <regex>
#include <set>

#include "Database/EmbeddedDbClient.h"
#include "JobScheduler.h"
#include "Node/CranedMetaContainer.h"
#include "RpcService/CranedKeeper.h"
#include "crane/PluginClient.h"
#include "crane/String.h"

namespace Ctld {

namespace {

using DynamicNodeRecord = crane::grpc::DynamicNodeRecord;
using DynamicNodeSpec = crane::grpc::DynamicNodeSpec;
using crane::grpc::DYNAMIC_NODE_LIFECYCLE_ACTIVE;
using crane::grpc::DYNAMIC_NODE_LIFECYCLE_DELETING;
using crane::grpc::DYNAMIC_NODE_LIFECYCLE_DOWN;
using crane::grpc::DYNAMIC_NODE_LIFECYCLE_FUTURE;
using crane::grpc::DYNAMIC_NODE_LIFECYCLE_REGISTERING;
using crane::grpc::DYNAMIC_NODE_LIFECYCLE_TOMBSTONE;
using crane::grpc::DYNAMIC_NODE_ORIGIN_DYNAMIC_ADMIN;
using crane::grpc::DYNAMIC_NODE_ORIGIN_DYNAMIC_REGISTERED;
using crane::grpc::DYNAMIC_NODE_POWER_STATE_OFF;
using crane::grpc::DYNAMIC_NODE_POWER_STATE_ON;
using crane::grpc::DYNAMIC_NODE_POWER_STATE_UNSPECIFIED;

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

std::expected<void, std::string> ValidateGresDefinition(
    const crane::grpc::DedicatedResourceInNode& gres) {
  std::unordered_set<std::string> slot_ids;
  for (const auto& [name, types] : gres.name_type_map()) {
    if (name.empty() || types.type_slots_map().empty())
      return std::unexpected("Invalid GRES definition");
    for (const auto& slots : types.type_slots_map() | std::views::values) {
      if (slots.slots().empty()) return std::unexpected("Invalid GRES slots");
      for (const auto& slot : slots.slots()) {
        if (slot.empty() || !slot_ids.emplace(slot).second)
          return std::unexpected("Invalid GRES slots");
      }
    }
  }
  return {};
}

bool IsDynamicRecordPresent(const DynamicNodeRecord& record) {
  switch (record.lifecycle()) {
  case DYNAMIC_NODE_LIFECYCLE_FUTURE:
  case DYNAMIC_NODE_LIFECYCLE_REGISTERING:
  case DYNAMIC_NODE_LIFECYCLE_ACTIVE:
  case DYNAMIC_NODE_LIFECYCLE_DOWN:
    return true;
  default:
    return false;
  }
}

void MarkRecordTombstone(DynamicNodeRecord* record) {
  record->set_lifecycle(DYNAMIC_NODE_LIFECYCLE_TOMBSTONE);
  record->clear_registration_token();
  record->clear_registration_nonce();
  record->clear_lease_expire_time();
  record->mutable_tombstone_time()->set_seconds(
      absl::ToUnixSeconds(absl::Now()));
}

void SetRecordPowerState(DynamicNodeRecord* record,
                         crane::grpc::DynamicNodePowerState state) {
  record->set_power_state(state);
  record->mutable_power_state_change_time()->set_seconds(
      absl::ToUnixSeconds(absl::Now()));
}

bool IsTransitionalPowerState(crane::grpc::DynamicNodePowerState state) {
  switch (state) {
  case crane::grpc::DYNAMIC_NODE_POWER_STATE_POWERING_ON:
  case crane::grpc::DYNAMIC_NODE_POWER_STATE_POWERING_OFF:
  case crane::grpc::DYNAMIC_NODE_POWER_STATE_WAKING_UP:
  case crane::grpc::DYNAMIC_NODE_POWER_STATE_TO_SLEEPING:
    return true;
  default:
    return false;
  }
}

bool PowerActionExpired(const DynamicNodeRecord& record) {
  if (!record.has_power_state_change_time()) return true;
  return record.power_state_change_time().seconds() +
             g_config.CtldConf.DynamicNodes.PowerActionTimeoutSeconds <=
         absl::ToUnixSeconds(absl::Now());
}

const DynamicNodeSpec& EffectiveSpec(const DynamicNodeRecord& record) {
  return record.has_effective_spec() ? record.effective_spec() : record.spec();
}

bool HasRequestedPartition(
    const google::protobuf::RepeatedPtrField<std::string>& requested,
    const google::protobuf::RepeatedPtrField<std::string>& available) {
  if (requested.empty()) return true;
  return std::ranges::all_of(requested, [&](const auto& partition) {
    return std::ranges::find(available, partition) != available.end();
  });
}

bool ResetRegistrationState(DynamicNodeRecord* record) {
  bool changed = !record->registration_token().empty() ||
                 !record->registration_nonce().empty() ||
                 record->has_lease_expire_time();
  record->clear_registration_token();
  record->clear_registration_nonce();
  record->clear_lease_expire_time();
  if (record->ever_registered() ||
      record->origin() == DYNAMIC_NODE_ORIGIN_DYNAMIC_REGISTERED) {
    changed |= record->lifecycle() != DYNAMIC_NODE_LIFECYCLE_DOWN;
    record->set_lifecycle(DYNAMIC_NODE_LIFECYCLE_DOWN);
    return changed;
  }

  changed |= record->lifecycle() != DYNAMIC_NODE_LIFECYCLE_FUTURE ||
             record->power_state() != DYNAMIC_NODE_POWER_STATE_OFF ||
             record->has_reported_spec() ||
             !record->physical_hostname().empty();
  record->set_lifecycle(DYNAMIC_NODE_LIFECYCLE_FUTURE);
  record->set_power_state(DYNAMIC_NODE_POWER_STATE_OFF);
  record->clear_reported_spec();
  record->clear_physical_hostname();
  return changed;
}

void PublishNodeDefinition(const DynamicNodeRecord& record,
                           crane::grpc::plugin::NodeDefinitionAction action) {
  if (!g_config.Plugin.Enabled || g_plugin_client == nullptr) return;
  g_plugin_client->NodeDefinitionHookAsync(record, action);
}

}  // namespace

bool NodeManager::RegistrationLeaseExpired_(const DynamicNodeRecord& record) {
  if (record.registration_token().empty() || !record.has_lease_expire_time())
    return true;
  return record.lease_expire_time().seconds() <=
         absl::ToUnixSeconds(absl::Now());
}

std::string NodeManager::GenerateRegistrationToken_() {
  std::random_device random_device;
  auto random_u64 = [&random_device] {
    return static_cast<uint64_t>(random_device()) << 32 | random_device();
  };
  return fmt::format("{:016x}{:016x}{:016x}{:016x}", random_u64(), random_u64(),
                     random_u64(), random_u64());
}

bool NodeManager::GresMatch_(const DynamicNodeSpec& expected,
                             const DynamicNodeSpec& reported) {
  for (const auto& [name, expected_types] : expected.gres().name_type_map()) {
    auto reported_name = reported.gres().name_type_map().find(name);
    if (reported_name == reported.gres().name_type_map().end()) return false;
    size_t expected_total = 0;
    size_t reported_total = 0;
    for (const auto& slots :
         reported_name->second.type_slots_map() | std::views::values)
      reported_total += slots.slots_size();
    for (const auto& [type, expected_slots] : expected_types.type_slots_map()) {
      expected_total += expected_slots.slots_size();
      if (type.empty()) continue;
      auto reported_type = reported_name->second.type_slots_map().find(type);
      if (reported_type == reported_name->second.type_slots_map().end() ||
          reported_type->second.slots_size() < expected_slots.slots_size())
        return false;
    }
    if (reported_total < expected_total) return false;
  }
  return true;
}

bool NodeManager::GresAllocationMatch_(const DynamicNodeSpec& expected,
                                       const DynamicNodeSpec& allocated) {
  if (expected.gres().name_type_map_size() !=
          allocated.gres().name_type_map_size() ||
      !GresMatch_(expected, allocated))
    return false;

  for (const auto& [name, expected_types] : expected.gres().name_type_map()) {
    size_t expected_total = 0;
    for (const auto& slots :
         expected_types.type_slots_map() | std::views::values)
      expected_total += slots.slots_size();

    size_t allocated_total = 0;
    const auto& allocated_types =
        allocated.gres().name_type_map().at(name).type_slots_map();
    for (const auto& slots : allocated_types | std::views::values)
      allocated_total += slots.slots_size();
    if (allocated_total != expected_total) return false;
  }
  return true;
}

bool NodeManager::FeaturesMatch_(const DynamicNodeSpec& expected,
                                 const DynamicNodeSpec& reported) {
  for (const auto& feature : expected.features()) {
    if (std::ranges::find(reported.features(), feature) ==
        reported.features().end())
      return false;
  }
  return true;
}

std::expected<void, std::string> NodeManager::ValidateReportedSpecStructure_(
    const DynamicNodeSpec& reported) const {
  if (reported.cpu_count() == 0 || reported.memory_bytes() == 0 ||
      reported.sockets() == 0 || reported.sockets() > reported.cpu_count())
    return std::unexpected("Invalid reported CPU, memory or sockets");
  auto gres_validation = ValidateGresDefinition(reported.gres());
  if (!gres_validation) return gres_validation;
  if (HasDuplicates(reported.features()) ||
      std::ranges::any_of(reported.features(),
                          [](const auto& feature) { return feature.empty(); }))
    return std::unexpected("Invalid reported features");
  return {};
}

std::expected<void, std::string> NodeManager::ValidateReportedSpec_(
    const DynamicNodeSpec& expected, const DynamicNodeSpec& reported) const {
  auto structure_validation = ValidateReportedSpecStructure_(reported);
  if (!structure_validation) return structure_validation;
  if (reported.cpu_count() < expected.cpu_count())
    return std::unexpected(
        fmt::format("Reported CPU count {} is smaller than configured {}",
                    reported.cpu_count(), expected.cpu_count()));
  if (reported.memory_bytes() < expected.memory_bytes())
    return std::unexpected(
        fmt::format("Reported memory {} is smaller than configured {}",
                    reported.memory_bytes(), expected.memory_bytes()));
  if (reported.sockets() != expected.sockets())
    return std::unexpected(
        fmt::format("Reported socket count {} does not match configured {}",
                    reported.sockets(), expected.sockets()));
  if (!GresMatch_(expected, reported))
    return std::unexpected("Reported GRES does not satisfy configured GRES");
  if (!FeaturesMatch_(expected, reported))
    return std::unexpected(
        "Reported features do not satisfy configured features");
  return {};
}

std::expected<void, std::string> NodeManager::ValidateRegistrationToken_(
    const DynamicNodeRecord& record,
    std::string_view registration_token) const {
  if (record.registration_token().empty())
    return std::unexpected("Dynamic node has no active registration lease");
  if (record.lifecycle() != DYNAMIC_NODE_LIFECYCLE_REGISTERING)
    return std::unexpected("Dynamic node is not registering");
  if (registration_token.empty() ||
      record.registration_token() != registration_token)
    return std::unexpected("Invalid dynamic registration token");
  if (RegistrationLeaseExpired_(record))
    return std::unexpected("Dynamic registration token has expired");
  return {};
}

std::optional<CranedId> NodeManager::FindNodeByPhysicalHostnameNoLock_(
    std::string_view physical_hostname, std::string_view excluded_node) const {
  for (const auto& [node_id, record] : records_) {
    if (node_id != excluded_node && IsDynamicRecordPresent(record) &&
        record.physical_hostname() == physical_hostname)
      return node_id;
  }
  return std::nullopt;
}

void NodeManager::FillPreparationReply_(
    const DynamicNodeRecord& record,
    crane::grpc::PrepareCranedRegistrationReply* reply) {
  reply->set_ok(true);
  reply->set_node_name(record.node_name());
  reply->set_generation(record.generation());
  reply->set_registration_token(record.registration_token());
  *reply->mutable_effective_spec() = EffectiveSpec(record);
  reply->mutable_partition_names()->CopyFrom(record.partition_names());
  if (record.has_lease_expire_time())
    *reply->mutable_expire_time() = record.lease_expire_time();
  reply->set_catalog_revision(record.revision());
}

bool NodeManager::Init() {
  std::unordered_map<CranedId, DynamicNodeRecord> records;
  if (!g_embedded_db_client->RetrieveDynamicNodeRecords(&records)) return false;

  for (const auto& record : records | std::views::values)
    catalog_revision_ = std::max(catalog_revision_, record.revision());

  std::vector<DynamicNodeRecord> changed_records;
  std::unordered_map<std::string, CranedId> physical_hosts;
  size_t present_node_count = 0;
  for (auto& [node_id, record] : records) {
    if (node_id != record.node_name()) {
      CRANE_ERROR("Dynamic node record key {} does not match node name {}.",
                  node_id, record.node_name());
      return false;
    }
    bool changed = false;
    if (record.lifecycle() == DYNAMIC_NODE_LIFECYCLE_DELETING) {
      MarkRecordTombstone(&record);
      changed = true;
    } else if (record.lifecycle() == DYNAMIC_NODE_LIFECYCLE_REGISTERING ||
               record.lifecycle() == DYNAMIC_NODE_LIFECYCLE_ACTIVE) {
      changed = ResetRegistrationState(&record);
    }
    if (record.lifecycle() == DYNAMIC_NODE_LIFECYCLE_TOMBSTONE) {
      if (changed) {
        record.set_revision(++catalog_revision_);
        changed_records.emplace_back(record);
      }
      continue;
    }
    if (g_config.Nodes.contains(node_id)) {
      CRANE_ERROR("Dynamic node {} conflicts with a static node.", node_id);
      return false;
    }
    if (!record.physical_hostname().empty() &&
        g_config.Nodes.contains(record.physical_hostname())) {
      CRANE_ERROR(
          "Physical host {} of dynamic node {} is configured as static.",
          record.physical_hostname(), node_id);
      return false;
    }

    auto result = ValidatePresentRecord_(record);
    if (!result) {
      CRANE_ERROR("Invalid dynamic node {}: {}", node_id, result.error());
      return false;
    }
    if (!record.physical_hostname().empty()) {
      auto [it, inserted] =
          physical_hosts.emplace(record.physical_hostname(), node_id);
      if (!inserted) {
        CRANE_ERROR("Dynamic nodes {} and {} use the same physical host {}.",
                    it->second, node_id, record.physical_hostname());
        return false;
      }
    }
    ++present_node_count;
    if (changed) {
      record.set_revision(++catalog_revision_);
      changed_records.emplace_back(record);
    }
  }

  if (g_config.CtldConf.MaxNodeCount != 0 &&
      g_config.Nodes.size() + present_node_count >
          g_config.CtldConf.MaxNodeCount) {
    CRANE_ERROR(
        "Static and dynamic node count exceeds configured MaxNodeCount {}.",
        g_config.CtldConf.MaxNodeCount);
    return false;
  }
  if (!changed_records.empty() &&
      !g_embedded_db_client->StoreDynamicNodeRecords(changed_records)) {
    CRANE_ERROR("Failed to persist dynamic node record migrations.");
    return false;
  }

  absl::MutexLock lock(&mutex_);
  records_ = std::move(records);
  CleanupExpiredTombstonesNoLock_();
  return true;
}

void NodeManager::RestoreDynamicNodes() {
  std::vector<DynamicNodeRecord> runtime_records;
  {
    absl::MutexLock lock(&mutex_);
    for (const auto& [node_id, record] : records_) {
      if (IsDynamicRecordPresent(record)) runtime_records.emplace_back(record);
    }
  }
  g_meta_container->AddDynamicNodes(runtime_records);
}

void NodeManager::ReconcilePluginState() {
  if (!g_config.Plugin.Enabled || g_plugin_client == nullptr) return;

  {
    absl::MutexLock lock(&mutex_);
    for (const auto& record : records_ | std::views::values) {
      PublishNodeDefinition(
          record, IsDynamicRecordPresent(record)
                      ? crane::grpc::plugin::NODE_DEFINITION_ACTION_UPSERT
                      : crane::grpc::plugin::NODE_DEFINITION_ACTION_REMOVE);
    }
  }
  g_meta_container->ReconcilePluginState();

  absl::MutexLock lock(&mutex_);
  for (const auto& record : records_ | std::views::values) {
    if (record.provider() != kPowerControlProvider ||
        !IsDynamicRecordPresent(record))
      continue;
    if (record.power_state() ==
        crane::grpc::DYNAMIC_NODE_POWER_STATE_POWERING_ON) {
      g_plugin_client->UpdatePowerStateHookAsync(record.node_name(),
                                                 crane::grpc::CRANE_POWERON,
                                                 true, true, record.provider());
    } else if (record.power_state() ==
               crane::grpc::DYNAMIC_NODE_POWER_STATE_WAKING_UP) {
      g_plugin_client->UpdatePowerStateHookAsync(record.node_name(),
                                                 crane::grpc::CRANE_WAKE, true,
                                                 true, record.provider());
    } else if (record.power_state() ==
               crane::grpc::DYNAMIC_NODE_POWER_STATE_POWERING_OFF) {
      g_plugin_client->UpdatePowerStateHookAsync(record.node_name(),
                                                 crane::grpc::CRANE_POWEROFF,
                                                 true, true, record.provider());
    } else if (record.power_state() ==
               crane::grpc::DYNAMIC_NODE_POWER_STATE_TO_SLEEPING) {
      g_plugin_client->UpdatePowerStateHookAsync(record.node_name(),
                                                 crane::grpc::CRANE_SLEEP, true,
                                                 true, record.provider());
    }
  }
}

crane::grpc::CreateNodesReply NodeManager::CreateNodes(
    const crane::grpc::CreateNodesRequest& request) {
  crane::grpc::CreateNodesReply reply;
  absl::MutexLock lock(&mutex_);
  CleanupExpiredTombstonesNoLock_();

  if (!g_config.CtldConf.DynamicNodes.Enabled) {
    reply.set_reason("Dynamic nodes are disabled");
    return reply;
  }
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

  size_t dynamic_node_count = std::ranges::count_if(
      records_,
      [](const auto& entry) { return IsDynamicRecordPresent(entry.second); });
  if (g_config.CtldConf.MaxNodeCount != 0 &&
      g_config.Nodes.size() + dynamic_node_count + request.node_names_size() >
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
      if (it->second.lifecycle() != DYNAMIC_NODE_LIFECYCLE_TOMBSTONE) {
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
    record.set_origin(request.origin() ==
                              crane::grpc::DYNAMIC_NODE_ORIGIN_UNSPECIFIED
                          ? DYNAMIC_NODE_ORIGIN_DYNAMIC_ADMIN
                          : request.origin());
    if (record.origin() != DYNAMIC_NODE_ORIGIN_DYNAMIC_ADMIN) {
      reply.set_reason(
          "Only administrator-created dynamic nodes can be created");
      return reply;
    }
    record.set_lifecycle(request.lifecycle() ==
                                 crane::grpc::DYNAMIC_NODE_LIFECYCLE_UNSPECIFIED
                             ? DYNAMIC_NODE_LIFECYCLE_FUTURE
                             : request.lifecycle());
    if (record.lifecycle() != DYNAMIC_NODE_LIFECYCLE_FUTURE) {
      reply.set_reason("New dynamic nodes must start in FUTURE state");
      return reply;
    }
    if (request.power_state() != DYNAMIC_NODE_POWER_STATE_UNSPECIFIED &&
        request.power_state() != DYNAMIC_NODE_POWER_STATE_OFF) {
      reply.set_reason("New dynamic nodes must start powered off");
      return reply;
    }
    record.set_power_state(DYNAMIC_NODE_POWER_STATE_OFF);
    record.set_pool(request.pool());
    record.set_provider(request.provider());
    record.set_provider_profile(request.provider_profile());
    if (!record.provider_profile().empty() && record.provider().empty()) {
      reply.set_reason("ProviderProfile requires Provider");
      return reply;
    }
    *record.mutable_effective_spec() = record.spec();
    auto validation = ValidatePresentRecord_(record);
    if (!validation) {
      reply.set_reason(validation.error());
      return reply;
    }
    record.set_revision(++catalog_revision_);
    new_records.emplace_back(std::move(record));
  }

  if (!g_embedded_db_client->StoreDynamicNodeRecords(new_records)) {
    reply.set_reason("Failed to persist dynamic nodes");
    return reply;
  }
  for (const auto& record : new_records) records_[record.node_name()] = record;
  g_meta_container->AddDynamicNodes(new_records);
  for (const auto& record : new_records) {
    PublishNodeDefinition(record,
                          crane::grpc::plugin::NODE_DEFINITION_ACTION_UPSERT);
  }

  reply.set_ok(true);
  return reply;
}

crane::grpc::DeleteNodesReply NodeManager::DeleteNodes(
    const crane::grpc::DeleteNodesRequest& request) {
  crane::grpc::DeleteNodesReply reply;
  absl::MutexLock lock(&mutex_);
  CleanupExpiredTombstonesNoLock_();

  auto reject = [&reply](const CranedId& node_id, std::string_view reason) {
    reply.add_not_deleted_nodes(node_id);
    reply.add_not_deleted_reasons(std::string(reason));
  };
  if (HasDuplicates(request.node_names())) {
    for (const auto& node_id : request.node_names())
      reject(node_id, "Duplicate node names are not allowed");
    return reply;
  }

  struct PendingDeletion {
    CranedId node_id;
    DynamicNodeRecord record;
    bool newly_deleting;
  };
  std::vector<PendingDeletion> pending;
  pending.reserve(request.node_names_size());
  for (const auto& node_id : request.node_names()) {
    if (g_config.Nodes.contains(node_id)) {
      reject(node_id, "Node is static");
      continue;
    }
    auto it = records_.find(node_id);
    if (it == records_.end() ||
        it->second.lifecycle() == DYNAMIC_NODE_LIFECYCLE_TOMBSTONE) {
      reject(node_id, "Node does not exist");
      continue;
    }
    const DynamicNodeRecord& record = it->second;
    if (record.lifecycle() == DYNAMIC_NODE_LIFECYCLE_REGISTERING &&
        !RegistrationLeaseExpired_(record)) {
      reject(node_id, "Node is still registering");
      continue;
    }
    if (IsTransitionalPowerState(record.power_state())) {
      if (!PowerActionExpired(record)) {
        reject(node_id, "Node has a power action in progress");
        continue;
      }
    } else if (record.provider() == kPowerControlProvider &&
               record.power_state() != DYNAMIC_NODE_POWER_STATE_OFF) {
      reject(node_id, "Node must be powered off before deletion");
      continue;
    }
    if (g_craned_keeper->IsCranedTracked(node_id)) {
      reject(node_id, "Node is still connected");
      continue;
    }
    pending.emplace_back(node_id, record,
                         record.lifecycle() != DYNAMIC_NODE_LIFECYCLE_DELETING);
  }
  if (pending.empty()) return reply;

  {
    std::vector<CranedId> pending_ids;
    pending_ids.reserve(pending.size());
    for (const auto& node : pending) pending_ids.emplace_back(node.node_id);
    auto busy_nodes = g_job_scheduler->FilterNodesWithJobs(pending_ids);
    std::erase_if(pending, [&](const PendingDeletion& node) {
      if (!busy_nodes.contains(node.node_id)) return false;
      reject(node.node_id, "Node is still referenced by jobs");
      return true;
    });
  }

  {
    std::vector<CranedId> new_deletion_nodes;
    for (const auto& node : pending)
      if (node.newly_deleting) new_deletion_nodes.emplace_back(node.node_id);
    auto failures =
        g_meta_container->SetDynamicNodesDeleting(new_deletion_nodes);
    std::erase_if(pending, [&](const PendingDeletion& node) {
      auto failure = failures.find(node.node_id);
      if (failure == failures.end()) return false;
      reject(node.node_id, failure->second);
      return true;
    });
  }

  std::erase_if(pending, [&](const PendingDeletion& node) {
    if (g_craned_keeper->ForgetCraned(node.node_id)) return false;
    if (node.newly_deleting)
      g_meta_container->ClearDynamicNodesDeleting({node.node_id});
    reject(node.node_id, "Node is still connected");
    return true;
  });
  if (pending.empty()) return reply;

  std::vector<DynamicNodeRecord> deleting_records;
  deleting_records.reserve(pending.size());
  for (const auto& node : pending) {
    DynamicNodeRecord record = node.record;
    record.set_lifecycle(DYNAMIC_NODE_LIFECYCLE_DELETING);
    record.clear_registration_token();
    record.clear_registration_nonce();
    record.clear_lease_expire_time();
    record.set_revision(++catalog_revision_);
    deleting_records.emplace_back(std::move(record));
  }
  if (!g_embedded_db_client->StoreDynamicNodeRecords(deleting_records)) {
    std::vector<CranedId> newly_deleting;
    for (const auto& node : pending)
      if (node.newly_deleting) newly_deleting.emplace_back(node.node_id);
    g_meta_container->ClearDynamicNodesDeleting(newly_deleting);
    for (const auto& node : pending)
      reject(node.node_id, "Failed to persist dynamic node deletion intent");
    return reply;
  }
  for (const auto& record : deleting_records)
    records_[record.node_name()] = record;

  {
    std::vector<CranedId> runtime_nodes;
    for (const auto& node : pending) {
      if (g_meta_container->CheckCranedAllowed(node.node_id))
        runtime_nodes.emplace_back(node.node_id);
    }
    auto failures = g_meta_container->RemoveDynamicNodes(runtime_nodes);
    if (!failures.empty()) {
      std::vector<DynamicNodeRecord> restored_records;
      std::vector<CranedId> restored_deleting;
      for (const auto& node : pending) {
        if (!failures.contains(node.node_id)) continue;
        restored_records.emplace_back(node.record);
        if (node.newly_deleting) restored_deleting.emplace_back(node.node_id);
      }
      const bool restored =
          g_embedded_db_client->StoreDynamicNodeRecords(restored_records);
      if (restored) {
        for (const auto& record : restored_records)
          records_[record.node_name()] = record;
        g_meta_container->ClearDynamicNodesDeleting(restored_deleting);
      }
      std::erase_if(pending, [&](const PendingDeletion& node) {
        auto failure = failures.find(node.node_id);
        if (failure == failures.end()) return false;
        reject(node.node_id,
               restored
                   ? failure->second
                   : fmt::format(
                         "{}; failed to restore dynamic node deletion state",
                         failure->second));
        return true;
      });
      if (pending.empty()) return reply;
    }
  }

  std::vector<DynamicNodeRecord> tombstones;
  tombstones.reserve(pending.size());
  for (const auto& node : pending) {
    DynamicNodeRecord record = records_.at(node.node_id);
    MarkRecordTombstone(&record);
    record.set_revision(++catalog_revision_);
    tombstones.emplace_back(std::move(record));
  }
  if (!g_embedded_db_client->StoreDynamicNodeRecords(tombstones)) {
    for (const auto& node : pending)
      reject(node.node_id, "Failed to persist dynamic node tombstones");
    return reply;
  }
  for (const auto& record : tombstones) records_[record.node_name()] = record;
  for (const auto& record : tombstones) {
    PublishNodeDefinition(record,
                          crane::grpc::plugin::NODE_DEFINITION_ACTION_REMOVE);
  }
  for (const auto& node : pending) reply.add_deleted_nodes(node.node_id);

  return reply;
}

void NodeManager::CleanupExpiredTombstonesNoLock_() {
  const int64_t expire_before =
      absl::ToUnixSeconds(absl::Now()) -
      g_config.CtldConf.DynamicNodes.TombstoneRetentionSeconds;
  std::vector<CranedId> expired;
  for (const auto& [node_id, record] : records_) {
    if (record.lifecycle() != DYNAMIC_NODE_LIFECYCLE_TOMBSTONE) continue;
    if (!record.has_tombstone_time() ||
        record.tombstone_time().seconds() <= expire_before)
      expired.emplace_back(node_id);
  }
  if (expired.empty()) return;
  if (!g_embedded_db_client->DeleteDynamicNodeRecords(expired)) {
    CRANE_WARN("Failed to purge expired dynamic node tombstones.");
    return;
  }
  for (const auto& node_id : expired) records_.erase(node_id);
}

std::expected<void, std::string>
NodeManager::ReleaseExpiredRegistrationLeasesNoLock_() {
  std::vector<DynamicNodeRecord> released_records;
  for (const auto& record : records_ | std::views::values) {
    if (record.lifecycle() != DYNAMIC_NODE_LIFECYCLE_REGISTERING ||
        !RegistrationLeaseExpired_(record))
      continue;

    DynamicNodeRecord released = record;
    ResetRegistrationState(&released);
    released.set_revision(++catalog_revision_);
    released_records.emplace_back(std::move(released));
  }
  if (released_records.empty()) return {};
  if (!g_embedded_db_client->StoreDynamicNodeRecords(released_records))
    return std::unexpected("Failed to release expired registration leases");
  for (const auto& record : released_records)
    records_[record.node_name()] = record;
  for (const auto& record : released_records)
    g_meta_container->UpdateDynamicNodeMetadata(record);
  for (const auto& record : released_records) {
    PublishNodeDefinition(record,
                          crane::grpc::plugin::NODE_DEFINITION_ACTION_UPSERT);
  }
  return {};
}

crane::grpc::PrepareCranedRegistrationReply
NodeManager::PrepareCranedRegistration(
    const crane::grpc::PrepareCranedRegistrationRequest& request) {
  crane::grpc::PrepareCranedRegistrationReply reply;
  if (!g_config.CtldConf.DynamicNodes.Enabled) {
    reply.set_reason("Dynamic node registration is disabled");
    return reply;
  }
  if (!g_config.ListenConf.TlsConfig.Enabled) {
    reply.set_reason("Dynamic node registration requires TLS");
    return reply;
  }
  if (request.mode() ==
          crane::grpc::DYNAMIC_NODE_REGISTRATION_MODE_UNSPECIFIED ||
      request.physical_hostname().empty() || request.client_nonce().empty()) {
    reply.set_reason("Registration mode, hostname and nonce are required");
    return reply;
  }

  const DynamicNodeSpec& reported_spec = request.reported_spec();
  auto reported_validation = ValidateReportedSpecStructure_(reported_spec);
  if (!reported_validation) {
    reply.set_reason(reported_validation.error());
    return reply;
  }
  if (HasDuplicates(request.requested_partitions())) {
    reply.set_reason("Duplicate partitions are not allowed");
    return reply;
  }

  if (g_config.Nodes.contains(request.physical_hostname())) {
    reply.set_reason("Physical host is configured as a static node");
    return reply;
  }

  absl::MutexLock lock(&mutex_);
  auto lease_result = ReleaseExpiredRegistrationLeasesNoLock_();
  if (!lease_result) {
    reply.set_reason(lease_result.error());
    return reply;
  }
  const auto now = absl::Now();
  const auto lease_expire =
      now +
      absl::Seconds(g_config.CtldConf.DynamicNodes.RegistrationLeaseSeconds);
  const auto make_expire_time = [&lease_expire] {
    google::protobuf::Timestamp timestamp;
    timestamp.set_seconds(absl::ToUnixSeconds(lease_expire));
    return timestamp;
  };

  auto prepare_record = [&](DynamicNodeRecord* record) {
    record->set_lifecycle(DYNAMIC_NODE_LIFECYCLE_REGISTERING);
    record->set_physical_hostname(request.physical_hostname());
    record->set_registration_nonce(request.client_nonce());
    record->set_registration_token(GenerateRegistrationToken_());
    *record->mutable_reported_spec() = reported_spec;
    record->set_revision(++catalog_revision_);
    *record->mutable_lease_expire_time() = make_expire_time();
    if (!record->has_effective_spec())
      *record->mutable_effective_spec() = record->spec();
  };

  if (request.mode() ==
      crane::grpc::DYNAMIC_NODE_REGISTRATION_MODE_PRECREATED) {
    if (request.requested_node_name().empty()) {
      reply.set_reason("Precreated registration requires a node name");
      return reply;
    }
    auto it = records_.find(request.requested_node_name());
    if (it == records_.end() || !IsDynamicRecordPresent(it->second)) {
      reply.set_reason("Dynamic node is not precreated");
      return reply;
    }
    if (request.generation() != 0 &&
        request.generation() != it->second.generation()) {
      reply.set_reason("Dynamic node generation has changed");
      return reply;
    }
    if (it->second.origin() != DYNAMIC_NODE_ORIGIN_DYNAMIC_ADMIN) {
      reply.set_reason("Node is not an administrator-created dynamic node");
      return reply;
    }
    if (!request.pool().empty() && request.pool() != it->second.pool()) {
      reply.set_reason("Requested pool does not match the precreated node");
      return reply;
    }
    if (!HasRequestedPartition(request.requested_partitions(),
                               it->second.partition_names())) {
      reply.set_reason("Requested partitions do not match the precreated node");
      return reply;
    }
    if (it->second.ever_registered() &&
        !it->second.physical_hostname().empty() &&
        it->second.physical_hostname() != request.physical_hostname()) {
      reply.set_reason("Dynamic node is owned by another physical host");
      return reply;
    }
    if (auto bound_node = FindNodeByPhysicalHostnameNoLock_(
            request.physical_hostname(), request.requested_node_name())) {
      reply.set_reason(fmt::format("Physical host is already registered as {}",
                                   *bound_node));
      return reply;
    }
    if (it->second.lifecycle() == DYNAMIC_NODE_LIFECYCLE_REGISTERING &&
        it->second.registration_nonce() == request.client_nonce() &&
        it->second.physical_hostname() == request.physical_hostname() &&
        !RegistrationLeaseExpired_(it->second)) {
      FillPreparationReply_(it->second, &reply);
      return reply;
    }
    if (it->second.lifecycle() == DYNAMIC_NODE_LIFECYCLE_REGISTERING &&
        !RegistrationLeaseExpired_(it->second)) {
      reply.set_reason("Dynamic node is already registering");
      return reply;
    }
    auto result =
        ValidateReportedSpec_(EffectiveSpec(it->second), reported_spec);
    if (!result) {
      reply.set_reason(result.error());
      return reply;
    }
    DynamicNodeRecord prepared = it->second;
    prepare_record(&prepared);
    if (!g_embedded_db_client->StoreDynamicNodeRecords({prepared})) {
      reply.set_reason("Failed to persist dynamic registration lease");
      return reply;
    }
    it->second = std::move(prepared);
    g_meta_container->UpdateDynamicNodeMetadata(it->second);
    FillPreparationReply_(it->second, &reply);
    return reply;
  }

  if (request.mode() ==
      crane::grpc::DYNAMIC_NODE_REGISTRATION_MODE_AUTO_CREATE) {
    if (!g_config.CtldConf.DynamicNodes.AutoCreate) {
      reply.set_reason("Dynamic auto-create registration is disabled");
      return reply;
    }
    if (request.pool().empty()) {
      reply.set_reason("Dynamic auto-create registration requires a pool");
      return reply;
    }
    const auto& policies = g_config.CtldConf.DynamicNodes.AutoCreatePools;
    auto policy_it = std::ranges::find_if(policies, [&](const auto& policy) {
      return policy.Name == request.pool();
    });
    if (policy_it == policies.end()) {
      reply.set_reason("Dynamic auto-create pool is not allowed");
      return reply;
    }
    const auto& policy = *policy_it;

    std::string node_name = request.requested_node_name().empty()
                                ? request.physical_hostname()
                                : request.requested_node_name();
    auto existing = records_.end();
    if (request.requested_node_name().empty()) {
      for (auto candidate = records_.begin(); candidate != records_.end();
           ++candidate) {
        if (candidate->second.origin() ==
                DYNAMIC_NODE_ORIGIN_DYNAMIC_REGISTERED &&
            candidate->second.physical_hostname() ==
                request.physical_hostname() &&
            IsDynamicRecordPresent(candidate->second)) {
          if (existing == records_.end() ||
              candidate->first < existing->first) {
            node_name = candidate->first;
            existing = candidate;
          }
        }
      }
      if (existing == records_.end()) existing = records_.find(node_name);
    } else {
      existing = records_.find(node_name);
    }
    if (auto bound_node = FindNodeByPhysicalHostnameNoLock_(
            request.physical_hostname(), node_name)) {
      reply.set_reason(fmt::format("Physical host is already registered as {}",
                                   *bound_node));
      return reply;
    }
    auto name_result = ValidateNodeName(node_name);
    if (!name_result) {
      reply.set_reason(name_result.error());
      return reply;
    }
    if (!std::regex_match(node_name, std::regex(policy.NodeNamePattern))) {
      reply.set_reason("Node name is outside the auto-create pool policy");
      return reply;
    }
    if (g_config.Nodes.contains(node_name)) {
      reply.set_reason("Auto-create node conflicts with a static node");
      return reply;
    }
    if (reported_spec.cpu_count() < policy.MinCpu ||
        reported_spec.cpu_count() > policy.MaxCpu ||
        reported_spec.memory_bytes() < policy.MinMemoryBytes ||
        reported_spec.memory_bytes() > policy.MaxMemoryBytes ||
        reported_spec.sockets() < policy.MinSockets ||
        reported_spec.sockets() > policy.MaxSockets) {
      reply.set_reason(
          "Reported resources are outside the auto-create pool bounds");
      return reply;
    }
    for (const auto& feature : policy.RequiredFeatures) {
      if (std::ranges::find(reported_spec.features(), feature) ==
          reported_spec.features().end()) {
        reply.set_reason(
            "Reported features do not satisfy the auto-create pool");
        return reply;
      }
    }
    for (const auto& feature : reported_spec.features()) {
      if (!policy.AllowedFeatures.contains(feature)) {
        reply.set_reason(
            "Reported feature is not allowed by the auto-create "
            "pool");
        return reply;
      }
    }
    for (const auto& partition : request.requested_partitions()) {
      if (std::ranges::find(policy.Partitions, partition) ==
          policy.Partitions.end()) {
        reply.set_reason(
            "Requested partition is not allowed by the auto-create pool");
        return reply;
      }
    }

    std::map<std::pair<std::string, std::string>, uint64_t> reported_gres;
    for (const auto& [name, types] : reported_spec.gres().name_type_map()) {
      for (const auto& [type, slots] : types.type_slots_map()) {
        auto gres_policy =
            std::ranges::find_if(policy.Gres, [&](const auto& range) {
              return range.Name == name && range.Type == type;
            });
        if (gres_policy == policy.Gres.end()) {
          reply.set_reason(
              "Reported GRES is not allowed by the auto-create pool");
          return reply;
        }
        reported_gres.emplace(std::pair{name, type}, slots.slots_size());
      }
    }
    for (const auto& range : policy.Gres) {
      uint64_t count = 0;
      if (auto it = reported_gres.find({range.Name, range.Type});
          it != reported_gres.end())
        count = it->second;
      if (count < range.Min || count > range.Max) {
        reply.set_reason(
            "Reported GRES is outside the auto-create pool bounds");
        return reply;
      }
    }

    if (existing != records_.end() &&
        IsDynamicRecordPresent(existing->second)) {
      if (existing->second.origin() != DYNAMIC_NODE_ORIGIN_DYNAMIC_REGISTERED) {
        reply.set_reason("Node name is reserved by a precreated dynamic node");
        return reply;
      }
      if (!std::ranges::all_of(
              existing->second.partition_names(), [&](const auto& partition) {
                return std::ranges::find(policy.Partitions, partition) !=
                       policy.Partitions.end();
              })) {
        reply.set_reason(
            "Dynamic node partitions are outside the auto-create pool");
        return reply;
      }
      if (request.generation() != 0 &&
          request.generation() != existing->second.generation()) {
        reply.set_reason("Dynamic node generation has changed");
        return reply;
      }
      if (request.pool() != existing->second.pool()) {
        reply.set_reason("Requested pool does not match the dynamic node");
        return reply;
      }
      if (!HasRequestedPartition(request.requested_partitions(),
                                 existing->second.partition_names())) {
        reply.set_reason("Requested partitions do not match the dynamic node");
        return reply;
      }
      if (existing->second.lifecycle() == DYNAMIC_NODE_LIFECYCLE_REGISTERING &&
          existing->second.registration_nonce() == request.client_nonce() &&
          existing->second.physical_hostname() == request.physical_hostname() &&
          !RegistrationLeaseExpired_(existing->second)) {
        FillPreparationReply_(existing->second, &reply);
        return reply;
      }
      if (existing->second.lifecycle() == DYNAMIC_NODE_LIFECYCLE_REGISTERING &&
          !RegistrationLeaseExpired_(existing->second)) {
        reply.set_reason("Dynamic node is already registering");
        return reply;
      }
      if (existing->second.physical_hostname() != request.physical_hostname()) {
        reply.set_reason("Dynamic node is owned by another physical host");
        return reply;
      }
      auto result =
          ValidateReportedSpec_(EffectiveSpec(existing->second), reported_spec);
      if (!result) {
        reply.set_reason(result.error());
        return reply;
      }
      DynamicNodeRecord prepared = existing->second;
      prepare_record(&prepared);
      if (!g_embedded_db_client->StoreDynamicNodeRecords({prepared})) {
        reply.set_reason("Failed to persist dynamic registration lease");
        return reply;
      }
      existing->second = std::move(prepared);
      g_meta_container->UpdateDynamicNodeMetadata(existing->second);
      FillPreparationReply_(existing->second, &reply);
      return reply;
    }

    if (request.generation() != 0) {
      reply.set_reason("Dynamic node is no longer available");
      return reply;
    }

    if (g_config.CtldConf.DynamicNodes.MaxAutoCreateNodes != 0) {
      size_t auto_count =
          std::ranges::count_if(records_, [](const auto& entry) {
            return entry.second.origin() ==
                       DYNAMIC_NODE_ORIGIN_DYNAMIC_REGISTERED &&
                   IsDynamicRecordPresent(entry.second);
          });
      if (auto_count >= g_config.CtldConf.DynamicNodes.MaxAutoCreateNodes) {
        reply.set_reason("Dynamic auto-create node limit reached");
        return reply;
      }
    }
    size_t pool_node_count =
        std::ranges::count_if(records_, [&](const auto& entry) {
          return entry.second.origin() ==
                     DYNAMIC_NODE_ORIGIN_DYNAMIC_REGISTERED &&
                 entry.second.pool() == policy.Name &&
                 IsDynamicRecordPresent(entry.second);
        });
    if (pool_node_count >= policy.MaxNodes) {
      reply.set_reason("Dynamic auto-create pool node limit reached");
      return reply;
    }
    if (g_config.CtldConf.MaxNodeCount != 0 &&
        g_config.Nodes.size() +
                std::ranges::count_if(records_,
                                      [](const auto& entry) {
                                        return IsDynamicRecordPresent(
                                            entry.second);
                                      }) +
                (existing == records_.end() ||
                         !IsDynamicRecordPresent(existing->second)
                     ? 1
                     : 0) >
            g_config.CtldConf.MaxNodeCount) {
      reply.set_reason("MaxNodeCount would be exceeded");
      return reply;
    }

    DynamicNodeRecord record;
    record.set_node_name(node_name);
    *record.mutable_spec() = reported_spec;
    if (request.requested_partitions().empty())
      record.mutable_partition_names()->Assign(policy.Partitions.begin(),
                                               policy.Partitions.end());
    else
      record.mutable_partition_names()->CopyFrom(
          request.requested_partitions());
    uint64_t generation = 1;
    if (existing != records_.end())
      generation = existing->second.generation() + 1;
    record.set_generation(generation);
    record.set_origin(DYNAMIC_NODE_ORIGIN_DYNAMIC_REGISTERED);
    record.set_pool(policy.Name);
    record.set_lifecycle(DYNAMIC_NODE_LIFECYCLE_REGISTERING);
    record.set_power_state(crane::grpc::DYNAMIC_NODE_POWER_STATE_ON);
    *record.mutable_effective_spec() = record.spec();
    auto validation = ValidatePresentRecord_(record);
    if (!validation) {
      reply.set_reason(validation.error());
      return reply;
    }
    prepare_record(&record);
    if (!g_embedded_db_client->StoreDynamicNodeRecords({record})) {
      reply.set_reason("Failed to persist auto-created dynamic node");
      return reply;
    }
    records_[node_name] = record;
    g_meta_container->AddDynamicNodes({record});
    PublishNodeDefinition(record,
                          crane::grpc::plugin::NODE_DEFINITION_ACTION_UPSERT);
    FillPreparationReply_(record, &reply);
    return reply;
  }

  if (request.mode() !=
      crane::grpc::DYNAMIC_NODE_REGISTRATION_MODE_FUTURE_POOL) {
    reply.set_reason("Unsupported dynamic registration mode");
    return reply;
  }
  if (request.pool().empty()) {
    reply.set_reason("FUTURE registration requires a pool");
    return reply;
  }
  if (!request.requested_node_name().empty()) {
    reply.set_reason("FUTURE registration cannot request a node name");
    return reply;
  }

  std::vector<std::reference_wrapper<DynamicNodeRecord>> candidates;
  for (auto& [node_name, record] : records_) {
    if (record.origin() != DYNAMIC_NODE_ORIGIN_DYNAMIC_ADMIN) continue;
    if (record.lifecycle() == DYNAMIC_NODE_LIFECYCLE_REGISTERING &&
        record.registration_nonce() == request.client_nonce() &&
        record.physical_hostname() == request.physical_hostname() &&
        (request.generation() == 0 ||
         record.generation() == request.generation()) &&
        record.pool() == request.pool() && !RegistrationLeaseExpired_(record)) {
      FillPreparationReply_(record, &reply);
      return reply;
    }
    const bool future_candidate =
        record.lifecycle() == DYNAMIC_NODE_LIFECYCLE_FUTURE &&
        (record.physical_hostname().empty() ||
         record.physical_hostname() == request.physical_hostname()) &&
        (request.generation() == 0 ||
         (record.generation() == request.generation() &&
          record.physical_hostname() == request.physical_hostname()));
    const bool down_candidate =
        record.lifecycle() == DYNAMIC_NODE_LIFECYCLE_DOWN &&
        record.physical_hostname() == request.physical_hostname() &&
        (request.generation() == 0 ||
         record.generation() == request.generation());
    const bool active_candidate =
        record.lifecycle() == DYNAMIC_NODE_LIFECYCLE_ACTIVE &&
        record.physical_hostname() == request.physical_hostname() &&
        (request.generation() == 0 ||
         record.generation() == request.generation());
    if (!IsDynamicRecordPresent(record) ||
        (!future_candidate && !down_candidate && !active_candidate) ||
        record.pool() != request.pool())
      continue;
    if (!record.registration_token().empty() &&
        !RegistrationLeaseExpired_(record))
      continue;
    const auto& expected = EffectiveSpec(record);
    if (!ValidateReportedSpec_(expected, reported_spec) ||
        reported_spec.cpu_count() != expected.cpu_count() ||
        reported_spec.sockets() != expected.sockets() ||
        !GresAllocationMatch_(expected, reported_spec))
      continue;
    if (!HasRequestedPartition(request.requested_partitions(),
                               record.partition_names()))
      continue;
    candidates.emplace_back(record);
  }
  std::ranges::sort(candidates, [&](const auto& lhs, const auto& rhs) {
    const bool lhs_reused =
        lhs.get().physical_hostname() == request.physical_hostname();
    const bool rhs_reused =
        rhs.get().physical_hostname() == request.physical_hostname();
    if (lhs_reused != rhs_reused) return lhs_reused;
    return lhs.get().node_name() < rhs.get().node_name();
  });
  if (candidates.empty()) {
    reply.set_reason("No FUTURE node matches the reported resources");
    return reply;
  }
  auto& record = candidates.front().get();
  if (auto bound_node = FindNodeByPhysicalHostnameNoLock_(
          request.physical_hostname(), record.node_name())) {
    reply.set_reason(
        fmt::format("Physical host is already registered as {}", *bound_node));
    return reply;
  }
  DynamicNodeRecord prepared = record;
  prepare_record(&prepared);
  if (!g_embedded_db_client->StoreDynamicNodeRecords({prepared})) {
    reply.set_reason("Failed to persist FUTURE node lease");
    return reply;
  }
  record = std::move(prepared);
  g_meta_container->UpdateDynamicNodeMetadata(record);
  FillPreparationReply_(record, &reply);
  return reply;
}

std::expected<NodeManager::RegistrationStartResult, std::string>
NodeManager::BeginRegistration(const CranedId& node_id, uint64_t generation,
                               const RegToken& token,
                               std::string_view registration_token) {
  absl::MutexLock lock(&mutex_);
  auto result = ValidateRegistrationNoLock_(node_id, generation);
  if (!result) return std::unexpected(result.error());

  RegistrationStartResult start_result{
      .connected = g_craned_keeper->IsCranedConnected(node_id),
      .connection_hostname = node_id};
  auto it = records_.find(node_id);
  if (it != records_.end()) {
    auto token_result =
        ValidateRegistrationToken_(it->second, registration_token);
    if (!token_result) return std::unexpected(token_result.error());
    start_result.connection_hostname = it->second.physical_hostname();
  }

  if (!start_result.connected) {
    g_craned_keeper->PutNodeIntoUnavailSet(node_id, token,
                                           start_result.connection_hostname);
  }
  return start_result;
}

std::expected<void, std::string> NodeManager::ValidateRegistration(
    const CranedId& node_id, uint64_t generation,
    const crane::grpc::CranedRemoteMeta& remote_meta,
    std::string_view registration_token) {
  absl::MutexLock lock(&mutex_);
  auto result = ValidateRegistrationNoLock_(node_id, generation);
  if (!result) return result;

  auto it = records_.find(node_id);
  if (it == records_.end()) return {};
  auto registered =
      BuildRegisteredRecord_(it->second, remote_meta, registration_token);
  if (!registered) return std::unexpected(registered.error());
  return {};
}

std::expected<DynamicNodeRecord, std::string>
NodeManager::BuildRegisteredRecord_(
    const DynamicNodeRecord& record,
    const crane::grpc::CranedRemoteMeta& remote_meta,
    std::string_view registration_token) const {
  auto token_result = ValidateRegistrationToken_(record, registration_token);
  if (!token_result) return std::unexpected(token_result.error());
  if (remote_meta.physical_hostname().empty() ||
      remote_meta.physical_hostname() != record.physical_hostname())
    return std::unexpected("Physical hostname does not match registration");
  if (!remote_meta.has_reported_spec())
    return std::unexpected("Dynamic node did not report its resource spec");

  DynamicNodeSpec reported = remote_meta.reported_spec();
  const auto& expected = record.spec();
  auto spec_result = ValidateReportedSpec_(expected, reported);
  if (!spec_result) return std::unexpected(spec_result.error());

  DynamicNodeSpec allocated;
  *allocated.mutable_gres() = remote_meta.dres_in_node();
  auto gres_validation = ValidateGresDefinition(allocated.gres());
  if (!gres_validation) return std::unexpected(gres_validation.error());
  if (!GresAllocationMatch_(expected, allocated))
    return std::unexpected(
        "Registered GRES allocation does not match the effective node spec");

  DynamicNodeRecord registered = record;
  *registered.mutable_reported_spec() = reported;
  if (!registered.ever_registered())
    *registered.mutable_effective_spec() = expected;
  *registered.mutable_effective_spec()->mutable_gres() =
      remote_meta.dres_in_node();
  registered.set_physical_hostname(remote_meta.physical_hostname());
  registered.mutable_network_interfaces()->CopyFrom(
      remote_meta.network_interfaces());
  return registered;
}

std::expected<void, std::string> NodeManager::MarkRegistered(
    const CranedId& node_id, uint64_t generation,
    const crane::grpc::CranedRemoteMeta& remote_meta,
    std::string_view registration_token) {
  absl::MutexLock lock(&mutex_);
  auto result = ValidateRegistrationNoLock_(node_id, generation);
  if (!result) return result;

  auto it = records_.find(node_id);
  if (it == records_.end()) return {};

  auto registered =
      BuildRegisteredRecord_(it->second, remote_meta, registration_token);
  if (!registered) return std::unexpected(registered.error());
  DynamicNodeRecord record = std::move(registered.value());
  record.set_ever_registered(true);
  record.set_lifecycle(DYNAMIC_NODE_LIFECYCLE_ACTIVE);
  record.set_power_state(DYNAMIC_NODE_POWER_STATE_ON);
  record.clear_registration_token();
  record.clear_registration_nonce();
  record.clear_lease_expire_time();
  record.set_revision(++catalog_revision_);
  if (!g_embedded_db_client->StoreDynamicNodeRecords({record})) {
    g_meta_container->UpdateDynamicNodeMetadata(it->second);
    return std::unexpected("Failed to persist dynamic node registration");
  }

  it->second = std::move(record);
  g_meta_container->UpdateDynamicNodeMetadata(it->second);
  return {};
}

std::expected<void, std::string> NodeManager::MarkDisconnected(
    const CranedId& node_id, uint64_t generation) {
  absl::MutexLock lock(&mutex_);
  auto it = records_.find(node_id);
  if (it == records_.end()) return {};
  if ((generation != 0 && it->second.generation() != generation) ||
      !IsDynamicRecordPresent(it->second))
    return {};

  DynamicNodeRecord record = it->second;
  if (!ResetRegistrationState(&record)) return {};
  record.set_revision(++catalog_revision_);
  if (!g_embedded_db_client->StoreDynamicNodeRecords({record}))
    return std::unexpected("Failed to persist dynamic node down state");
  it->second = record;
  g_meta_container->UpdateDynamicNodeMetadata(record);
  return {};
}

std::expected<void, std::string> NodeManager::UpdatePowerState(
    const CranedId& node_id, crane::grpc::CranedPowerState power_state) {
  absl::MutexLock lock(&mutex_);
  auto dynamic_power_state = DynamicNodePowerStateFromCraned(power_state);
  if (!dynamic_power_state)
    return std::unexpected("Invalid Craned power state");

  auto it = records_.find(node_id);
  if (it == records_.end()) return {};
  if (!IsDynamicRecordPresent(it->second))
    return std::unexpected("Dynamic node is not available");

  if (it->second.power_state() == *dynamic_power_state) return {};
  DynamicNodeRecord record = it->second;
  SetRecordPowerState(&record, *dynamic_power_state);
  record.set_revision(++catalog_revision_);
  if (!g_embedded_db_client->StoreDynamicNodeRecords({record}))
    return std::unexpected("Failed to persist dynamic node power state");
  it->second = record;
  g_meta_container->UpdateDynamicNodeMetadata(record);
  return {};
}

std::expected<NodeManager::ScaleUpResult, std::string>
NodeManager::RequestScaleUp(
    const PartitionId& partition, const ResourceView& node_resource,
    const ResourceView& task_resource, uint32_t min_tasks_per_node,
    uint32_t max_tasks_per_node, uint32_t node_count, uint32_t task_count,
    const std::vector<uint32_t>& available_node_task_counts,
    const std::unordered_set<std::string>& included_nodes,
    const std::unordered_set<std::string>& excluded_nodes) {
  ScaleUpResult result;
  if (node_count == 0 || task_count == 0) return result;

  absl::MutexLock lock(&mutex_);
  auto lease_result = ReleaseExpiredRegistrationLeasesNoLock_();
  if (!lease_result) return std::unexpected(lease_result.error());

  struct Candidate {
    CranedId node_id;
    uint32_t task_count;
  };
  std::vector<Candidate> power_on_candidates;
  std::vector<Candidate> wake_candidates;
  std::vector<Candidate> in_progress_candidates;

  auto task_capacity = [&](const DynamicNodeSpec& spec) {
    ResourceInNodeV3 available = ResourceInNodeFromDynamicSpec(spec);

    ResourceInNodeV3 feasible;
    const ResourceView minimum =
        node_resource + task_resource * min_tasks_per_node;
    if (!minimum.GetFeasibleResourceInNode(available, &feasible)) return 0U;
    available -= feasible;

    uint32_t tasks = min_tasks_per_node;
    while (tasks < max_tasks_per_node &&
           task_resource.GetFeasibleResourceInNode(available, &feasible)) {
      ++tasks;
      available -= feasible;
    }
    return tasks;
  };

  for (const auto& [node_id, record] : records_) {
    if (!IsDynamicRecordPresent(record) ||
        (record.lifecycle() != DYNAMIC_NODE_LIFECYCLE_FUTURE &&
         record.lifecycle() != DYNAMIC_NODE_LIFECYCLE_REGISTERING &&
         record.lifecycle() != DYNAMIC_NODE_LIFECYCLE_DOWN) ||
        record.provider() != kPowerControlProvider ||
        std::ranges::find(record.partition_names(), partition) ==
            record.partition_names().end() ||
        (!included_nodes.empty() && !included_nodes.contains(node_id)) ||
        excluded_nodes.contains(node_id))
      continue;

    const uint32_t tasks = task_capacity(EffectiveSpec(record));
    if (tasks == 0) continue;

    if (record.lifecycle() == DYNAMIC_NODE_LIFECYCLE_REGISTERING ||
        record.power_state() ==
            crane::grpc::DYNAMIC_NODE_POWER_STATE_POWERING_ON ||
        record.power_state() ==
            crane::grpc::DYNAMIC_NODE_POWER_STATE_WAKING_UP) {
      in_progress_candidates.emplace_back(node_id, tasks);
    } else if (record.power_state() == DYNAMIC_NODE_POWER_STATE_OFF) {
      power_on_candidates.emplace_back(node_id, tasks);
    } else if (record.power_state() ==
               crane::grpc::DYNAMIC_NODE_POWER_STATE_SLEEPING) {
      wake_candidates.emplace_back(node_id, tasks);
    }
  }

  auto by_capacity = [](const Candidate& lhs, const Candidate& rhs) {
    if (lhs.task_count != rhs.task_count)
      return lhs.task_count > rhs.task_count;
    return lhs.node_id < rhs.node_id;
  };
  std::ranges::sort(in_progress_candidates, by_capacity);
  std::ranges::sort(wake_candidates, by_capacity);
  std::ranges::sort(power_on_candidates, by_capacity);

  enum class SelectionKind : uint8_t {
    kAvailable,
    kInProgress,
    kWake,
    kPowerOn,
  };
  struct Selection {
    uint32_t task_count;
    SelectionKind kind;
    CranedId node_id;
  };
  auto better_selection = [](const Selection& lhs, const Selection& rhs) {
    if (lhs.task_count != rhs.task_count)
      return lhs.task_count > rhs.task_count;
    if (lhs.kind != rhs.kind) return lhs.kind < rhs.kind;
    return lhs.node_id < rhs.node_id;
  };
  std::multiset<Selection, decltype(better_selection)> selected(
      better_selection);
  uint64_t selected_tasks = 0;
  auto add_selection = [&](Selection selection) {
    selected_tasks += selection.task_count;
    selected.emplace(std::move(selection));
    if (selected.size() > node_count) {
      auto worst = std::prev(selected.end());
      selected_tasks -= worst->task_count;
      selected.erase(worst);
    }
  };
  for (uint32_t tasks : available_node_task_counts)
    add_selection({tasks, SelectionKind::kAvailable, {}});
  auto requirements_met = [&] {
    return selected.size() == node_count && selected_tasks >= task_count;
  };
  if (requirements_met()) return result;

  for (const auto& candidate : in_progress_candidates) {
    add_selection(
        {candidate.task_count, SelectionKind::kInProgress, candidate.node_id});
    if (requirements_met()) break;
  }
  if (!requirements_met()) {
    for (const auto& candidate : wake_candidates) {
      add_selection(
          {candidate.task_count, SelectionKind::kWake, candidate.node_id});
      if (requirements_met()) break;
    }
  }
  if (!requirements_met()) {
    for (const auto& candidate : power_on_candidates) {
      add_selection(
          {candidate.task_count, SelectionKind::kPowerOn, candidate.node_id});
      if (requirements_met()) break;
    }
  }
  if (!requirements_met()) return result;

  for (const auto& selection : selected) {
    switch (selection.kind) {
    case SelectionKind::kAvailable:
      break;
    case SelectionKind::kInProgress:
      result.reserved_nodes.emplace_back(selection.node_id);
      break;
    case SelectionKind::kWake:
      result.nodes_to_wake.emplace_back(selection.node_id);
      result.reserved_nodes.emplace_back(selection.node_id);
      break;
    case SelectionKind::kPowerOn:
      result.nodes_to_power_on.emplace_back(selection.node_id);
      result.reserved_nodes.emplace_back(selection.node_id);
      break;
    }
  }
  result.in_progress = !result.reserved_nodes.empty();
  if (result.nodes_to_wake.empty() && result.nodes_to_power_on.empty())
    return result;

  std::vector<DynamicNodeRecord> updated_records;
  updated_records.reserve(result.nodes_to_wake.size() +
                          result.nodes_to_power_on.size());
  for (const auto& node_id : result.nodes_to_wake) {
    DynamicNodeRecord record = records_.at(node_id);
    SetRecordPowerState(&record,
                        crane::grpc::DYNAMIC_NODE_POWER_STATE_WAKING_UP);
    record.set_revision(++catalog_revision_);
    updated_records.emplace_back(std::move(record));
  }
  for (const auto& node_id : result.nodes_to_power_on) {
    DynamicNodeRecord record = records_.at(node_id);
    SetRecordPowerState(&record,
                        crane::grpc::DYNAMIC_NODE_POWER_STATE_POWERING_ON);
    record.set_revision(++catalog_revision_);
    updated_records.emplace_back(std::move(record));
  }
  if (!g_embedded_db_client->StoreDynamicNodeRecords(updated_records))
    return std::unexpected("Failed to persist dynamic node scale-up state");

  for (const auto& record : updated_records) {
    records_[record.node_name()] = record;
    g_meta_container->UpdateDynamicNodeMetadata(record);
  }
  return result;
}

std::expected<void, std::string> NodeManager::ValidateRegistrationNoLock_(
    const CranedId& node_id, uint64_t generation) const {
  if (g_config.Nodes.contains(node_id)) {
    if (generation != 0)
      return std::unexpected("Static node generation must be 0");
    return {};
  }
  if (!g_config.CtldConf.DynamicNodes.Enabled)
    return std::unexpected("Dynamic node registration is disabled");

  auto it = records_.find(node_id);
  if (it == records_.end() || !IsDynamicRecordPresent(it->second))
    return std::unexpected("Dynamic node is not available");
  if (it->second.generation() != generation)
    return std::unexpected(fmt::format("Stale generation {}, expected {}",
                                       generation, it->second.generation()));
  return {};
}

std::expected<void, std::string> NodeManager::ValidatePresentRecord_(
    const DynamicNodeRecord& record) const {
  auto name_result = ValidateNodeName(record.node_name());
  if (!name_result) return name_result;
  const auto& spec = EffectiveSpec(record);
  if (!record.has_spec() || spec.cpu_count() == 0 || spec.memory_bytes() == 0 ||
      spec.sockets() == 0 || spec.sockets() > spec.cpu_count())
    return std::unexpected("Invalid CPU, memory or sockets");
  if (record.generation() == 0)
    return std::unexpected("Dynamic node generation must be positive");
  if (record.origin() != DYNAMIC_NODE_ORIGIN_DYNAMIC_ADMIN &&
      record.origin() != DYNAMIC_NODE_ORIGIN_DYNAMIC_REGISTERED)
    return std::unexpected("Invalid dynamic node origin");
  if (record.lifecycle() != DYNAMIC_NODE_LIFECYCLE_FUTURE &&
      record.lifecycle() != DYNAMIC_NODE_LIFECYCLE_REGISTERING &&
      record.lifecycle() != DYNAMIC_NODE_LIFECYCLE_ACTIVE &&
      record.lifecycle() != DYNAMIC_NODE_LIFECYCLE_DOWN)
    return std::unexpected("Invalid dynamic node lifecycle");
  if (record.power_state() != DYNAMIC_NODE_POWER_STATE_ON &&
      record.power_state() != DYNAMIC_NODE_POWER_STATE_OFF &&
      record.power_state() !=
          crane::grpc::DYNAMIC_NODE_POWER_STATE_POWERING_ON &&
      record.power_state() !=
          crane::grpc::DYNAMIC_NODE_POWER_STATE_POWERING_OFF &&
      record.power_state() != crane::grpc::DYNAMIC_NODE_POWER_STATE_SLEEPING &&
      record.power_state() != crane::grpc::DYNAMIC_NODE_POWER_STATE_WAKING_UP &&
      record.power_state() != crane::grpc::DYNAMIC_NODE_POWER_STATE_TO_SLEEPING)
    return std::unexpected("Invalid dynamic node power state");
  if (record.partition_names().empty())
    return std::unexpected("No partition specified");
  if (HasDuplicates(record.partition_names()))
    return std::unexpected("Duplicate partitions are not allowed");
  for (const auto& partition_id : record.partition_names()) {
    if (!g_config.Partitions.contains(partition_id))
      return std::unexpected(
          fmt::format("Partition {} does not exist", partition_id));
  }
  if (HasDuplicates(spec.features()))
    return std::unexpected("Duplicate features are not allowed");
  if (std::ranges::any_of(spec.features(),
                          [](const auto& feature) { return feature.empty(); }))
    return std::unexpected("Empty features are not allowed");
  return ValidateGresDefinition(spec.gres());
}

}  // namespace Ctld

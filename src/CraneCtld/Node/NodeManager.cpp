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

#include "Node/NodeManager.h"

#include <openssl/crypto.h>

#include <limits>
#include <map>
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

// Bump when the semantics of persisted DynamicNodeRecord fields change in a
// way protobuf field evolution cannot express.
constexpr uint32_t kDynamicNodeSchemaVersion = 1;

CraneExpectedRich<void> ValidateNodeName(const CranedId& node_id) {
  std::list<std::string> expanded;
  if (!util::ParseHostList(node_id, &expanded) || expanded.size() != 1 ||
      expanded.front() != node_id) {
    return std::unexpected(FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                                         "Invalid node name {}", node_id));
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

CraneExpectedRich<void> ValidateGresDefinition(
    const crane::grpc::DedicatedResourceInNode& gres) {
  std::unordered_set<std::string> slot_ids;
  for (const auto& [name, types] : gres.name_type_map()) {
    if (name.empty() || types.type_slots_map().empty())
      return std::unexpected(FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                                           "Invalid GRES definition"));
    for (const auto& slots : types.type_slots_map() | std::views::values) {
      if (slots.slots().empty())
        return std::unexpected(FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                                             "Invalid GRES slots"));
      for (const auto& slot : slots.slots()) {
        if (slot.empty() || !slot_ids.emplace(slot).second)
          return std::unexpected(FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                                               "Invalid GRES slots"));
      }
    }
  }
  return {};
}

CraneExpectedRich<void> ValidateGresCounts(const crane::grpc::GresMap& gres) {
  for (const auto& [name, count] : gres.name_gres_map()) {
    if (name.empty() || count.total() == 0)
      return std::unexpected(FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                                           "Invalid GRES definition"));
    uint64_t typed_total = 0;
    for (const auto& [type, type_count] : count.specified()) {
      if (type.empty() || type_count == 0)
        return std::unexpected(FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                                             "Invalid GRES definition"));
      typed_total += type_count;
    }
    if (typed_total > count.total())
      return std::unexpected(
          FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                        "Typed GRES counts exceed the total count"));
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
  record->clear_registration_token_digest();
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
  bool changed = !record->registration_token_digest().empty() ||
                 !record->registration_nonce().empty() ||
                 record->has_lease_expire_time();
  record->clear_registration_token_digest();
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
  if (record.registration_token_digest().empty() ||
      !record.has_lease_expire_time())
    return true;
  return record.lease_expire_time().seconds() <=
         absl::ToUnixSeconds(absl::Now());
}

std::string NodeManager::GenerateRegistrationToken_() {
  constexpr size_t kRegistrationTokenBytes = 32;
  return util::GenerateSecureRandomHex(kRegistrationTokenBytes);
}

bool NodeManager::GresMatch_(const crane::grpc::GresMap& expected,
                             const crane::grpc::GresMap& reported) {
  for (const auto& [name, expected_count] : expected.name_gres_map()) {
    auto reported_name = reported.name_gres_map().find(name);
    if (reported_name == reported.name_gres_map().end()) return false;
    const auto& reported_count = reported_name->second;
    if (reported_count.total() < expected_count.total()) return false;
    for (const auto& [type, count] : expected_count.specified()) {
      auto reported_type = reported_count.specified().find(type);
      if (reported_type == reported_count.specified().end() ||
          reported_type->second < count)
        return false;
    }
  }
  return true;
}

bool NodeManager::GresCountsMatch_(const crane::grpc::GresMap& expected,
                                   const crane::grpc::GresMap& allocated) {
  if (expected.name_gres_map_size() != allocated.name_gres_map_size() ||
      !GresMatch_(expected, allocated))
    return false;

  for (const auto& [name, expected_count] : expected.name_gres_map()) {
    if (allocated.name_gres_map().at(name).total() != expected_count.total())
      return false;
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

CraneExpectedRich<void> NodeManager::ValidateReportedSpecStructure_(
    const DynamicNodeSpec& reported) const {
  if (reported.cpu_count() == 0 || reported.memory_bytes() == 0 ||
      reported.sockets() == 0 || reported.sockets() > reported.cpu_count())
    return std::unexpected(
        FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                      "Invalid reported CPU, memory or sockets"));
  auto gres_validation = ValidateGresCounts(reported.gres());
  if (!gres_validation) return gres_validation;
  if (HasDuplicates(reported.features()) ||
      std::ranges::any_of(reported.features(),
                          [](const auto& feature) { return feature.empty(); }))
    return std::unexpected(FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                                         "Invalid reported features"));
  return {};
}

CraneExpectedRich<void> NodeManager::ValidateReportedSpec_(
    const DynamicNodeSpec& expected, const DynamicNodeSpec& reported) const {
  auto structure_validation = ValidateReportedSpecStructure_(reported);
  if (!structure_validation) return structure_validation;
  if (reported.cpu_count() < expected.cpu_count())
    return std::unexpected(
        FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                      "Reported CPU count {} is smaller than configured {}",
                      reported.cpu_count(), expected.cpu_count()));
  if (reported.memory_bytes() < expected.memory_bytes())
    return std::unexpected(
        FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                      "Reported memory {} is smaller than configured {}",
                      reported.memory_bytes(), expected.memory_bytes()));
  if (reported.sockets() != expected.sockets())
    return std::unexpected(
        FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                      "Reported socket count {} does not match configured {}",
                      reported.sockets(), expected.sockets()));
  if (!GresMatch_(expected.gres(), reported.gres()))
    return std::unexpected(
        FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                      "Reported GRES does not satisfy configured GRES"));
  if (!FeaturesMatch_(expected, reported))
    return std::unexpected(
        FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                      "Reported features do not satisfy configured features"));
  return {};
}

CraneExpectedRich<void> NodeManager::ValidateRegistrationToken_(
    const DynamicNodeRecord& record,
    std::string_view registration_token) const {
  if (record.registration_token_digest().empty())
    return std::unexpected(
        FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                      "Dynamic node has no active registration lease"));
  if (record.lifecycle() != DYNAMIC_NODE_LIFECYCLE_REGISTERING)
    return std::unexpected(FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                                         "Dynamic node is not registering"));
  const std::string digest = util::Sha256Hex(registration_token);
  if (digest.size() != record.registration_token_digest().size() ||
      CRYPTO_memcmp(digest.data(), record.registration_token_digest().data(),
                    digest.size()) != 0)
    return std::unexpected(FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                                         "Invalid dynamic registration token"));
  if (RegistrationLeaseExpired_(record))
    return std::unexpected(
        FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                      "Dynamic registration token has expired"));
  return {};
}

std::optional<CranedId> NodeManager::FindNodeByPhysicalHostnameNoLock_(
    std::string_view physical_hostname, std::string_view excluded_node) const {
  for (const auto& [node_id, record] : records_) {
    if (node_id != excluded_node &&
        (IsDynamicRecordPresent(record) ||
         record.lifecycle() == DYNAMIC_NODE_LIFECYCLE_DELETING) &&
        record.physical_hostname() == physical_hostname)
      return node_id;
  }
  return std::nullopt;
}

// The raw registration token is not part of the record (only its digest is);
// the caller sets it on the reply.
void NodeManager::FillPreparationReply_(
    const DynamicNodeRecord& record,
    crane::grpc::PrepareCranedRegistrationReply* reply) {
  reply->set_ok(true);
  reply->set_node_name(record.node_name());
  reply->set_generation(record.generation());
  *reply->mutable_effective_spec() = EffectiveSpec(record);
  reply->mutable_partition_names()->CopyFrom(record.partition_names());
  if (record.has_lease_expire_time())
    *reply->mutable_expire_time() = record.lease_expire_time();
  reply->set_catalog_revision(record.revision());
}

bool NodeManager::Init() {
  std::unordered_map<CranedId, DynamicNodeRecord> records;
  EmbeddedDbClient::DynamicNodeGenerationMap generation_high_watermarks;
  if (!g_embedded_db_client->RetrieveDynamicNodeRecords(
          &records, &generation_high_watermarks))
    return false;

  absl::MutexLock lock(&mutex_);
  for (const auto& record : records | std::views::values) {
    catalog_revision_ = std::max(catalog_revision_, record.revision());
    auto& high_watermark = generation_high_watermarks[record.node_name()];
    high_watermark = std::max(high_watermark, record.generation());
  }

  std::vector<DynamicNodeRecord> changed_records;
  std::unordered_map<std::string, CranedId> physical_hosts;
  std::unordered_set<CranedId> quarantined;
  size_t present_node_count = 0;
  for (auto& [node_id, record] : records) {
    if (node_id != record.node_name()) {
      CRANE_ERROR("Dynamic node record key {} does not match node name {}.",
                  node_id, record.node_name());
      return false;
    }
    if (record.schema_version() > kDynamicNodeSchemaVersion) {
      CRANE_ERROR(
          "Dynamic node {} has schema version {} newer than supported {}.",
          node_id, record.schema_version(), kDynamicNodeSchemaVersion);
      return false;
    }
    bool changed = false;
    if (record.schema_version() != kDynamicNodeSchemaVersion) {
      record.set_schema_version(kDynamicNodeSchemaVersion);
      changed = true;
    }
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

    auto missing_partition = std::ranges::find_if(
        record.partition_names(), [](const std::string& partition_id) {
          return !g_config.Partitions.contains(partition_id);
        });
    if (missing_partition != record.partition_names().end()) {
      CRANE_ERROR(
          "Dynamic node {} references partition {} which no longer exists. "
          "The node is excluded from the cluster topology and cannot "
          "register; restore the partition or delete the node.",
          node_id, *missing_partition);
      quarantined.emplace(node_id);
    } else {
      auto result = ValidatePresentRecord_(record);
      if (!result) {
        CRANE_ERROR("Invalid dynamic node {}: {}", node_id,
                    result.error().description());
        return false;
      }
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

  if (!g_config.CtldConf.DynamicNodes.Enabled && present_node_count > 0)
    CRANE_WARN(
        "DynamicNodes is disabled but {} dynamic node records exist in the "
        "embedded db. They are restored into the cluster topology; delete "
        "them or re-enable DynamicNodes.",
        present_node_count);

  records_ = std::move(records);
  generation_high_watermarks_ = std::move(generation_high_watermarks);
  quarantined_nodes_ = std::move(quarantined);
  CleanupExpiredTombstonesNoLock_();
  return true;
}

NodeManager::~NodeManager() {
  if (reconcile_thread_.joinable()) {
    reconcile_stop_notification_.Notify();
    reconcile_thread_.join();
  }
}

void NodeManager::StartReconcileThread() {
  reconcile_thread_ = std::thread(&NodeManager::ReconcileThreadFunc_, this);
}

void NodeManager::ReconcileThreadFunc_() {
  util::SetCurrentThreadName("NodeReconcile");
  const absl::Duration period =
      absl::Seconds(g_config.CtldConf.DynamicNodes.RegistrationLeaseSeconds);
  // Plugin hooks are dropped after their retries are exhausted; a sparse
  // full re-publish converges the plugin back to the controller's state.
  const absl::Duration plugin_reconcile_period = absl::Minutes(10);
  absl::Time last_plugin_reconcile = absl::Now();
  while (!reconcile_stop_notification_.WaitForNotificationWithTimeout(period)) {
    {
      absl::MutexLock lock(&mutex_);
      auto lease_result = ReleaseExpiredRegistrationLeasesNoLock_();
      if (!lease_result)
        CRANE_WARN("Failed to release expired dynamic registration leases: {}",
                   lease_result.error().description());
      CleanupExpiredTombstonesNoLock_();
    }
    if (absl::Now() - last_plugin_reconcile >= plugin_reconcile_period) {
      last_plugin_reconcile = absl::Now();
      ReconcilePluginState();
    }
  }
}

void NodeManager::RestoreDynamicNodes() {
  std::vector<DynamicNodeRecord> runtime_records;
  {
    absl::MutexLock lock(&mutex_);
    for (const auto& [node_id, record] : records_) {
      if (IsDynamicRecordPresent(record) &&
          !quarantined_nodes_.contains(node_id))
        runtime_records.emplace_back(record);
    }
  }
  g_meta_container->AddDynamicNodes(runtime_records);
}

void NodeManager::ReconcilePluginState() {
  if (!g_config.Plugin.Enabled || g_plugin_client == nullptr) return;

  {
    absl::MutexLock lock(&mutex_);
    for (const auto& [node_id, record] : records_) {
      const bool available = IsDynamicRecordPresent(record) &&
                             !quarantined_nodes_.contains(node_id);
      PublishNodeDefinition(
          record, available
                      ? crane::grpc::plugin::NODE_DEFINITION_ACTION_UPSERT
                      : crane::grpc::plugin::NODE_DEFINITION_ACTION_REMOVE);
    }
  }
  g_meta_container->ReconcilePluginState();

  absl::MutexLock lock(&mutex_);
  for (const auto& [node_id, record] : records_) {
    if (record.provider() != kPowerControlProvider ||
        !IsDynamicRecordPresent(record) || quarantined_nodes_.contains(node_id))
      continue;
    if (record.power_state() ==
        crane::grpc::DYNAMIC_NODE_POWER_STATE_POWERING_ON) {
      g_plugin_client->UpdatePowerStateHookAsync(
          record.node_name(), crane::grpc::CRANE_POWERON, true, true,
          record.provider(), record.generation());
    } else if (record.power_state() ==
               crane::grpc::DYNAMIC_NODE_POWER_STATE_WAKING_UP) {
      g_plugin_client->UpdatePowerStateHookAsync(
          record.node_name(), crane::grpc::CRANE_WAKE, true, true,
          record.provider(), record.generation());
    } else if (record.power_state() ==
               crane::grpc::DYNAMIC_NODE_POWER_STATE_POWERING_OFF) {
      g_plugin_client->UpdatePowerStateHookAsync(
          record.node_name(), crane::grpc::CRANE_POWEROFF, true, true,
          record.provider(), record.generation());
    } else if (record.power_state() ==
               crane::grpc::DYNAMIC_NODE_POWER_STATE_TO_SLEEPING) {
      g_plugin_client->UpdatePowerStateHookAsync(
          record.node_name(), crane::grpc::CRANE_SLEEP, true, true,
          record.provider(), record.generation());
    }
  }
}

crane::grpc::CreateNodesReply NodeManager::CreateNodes(
    const crane::grpc::CreateNodesRequest& request) {
  crane::grpc::CreateNodesReply reply;
  const auto fail = [&reply](crane::grpc::ErrCode code,
                             std::string_view reason) {
    reply.set_code(code);
    reply.set_reason(std::string(reason));
  };
  absl::MutexLock lock(&mutex_);
  CleanupExpiredTombstonesNoLock_();

  if (!g_config.CtldConf.DynamicNodes.Enabled) {
    fail(crane::grpc::ERR_GENERIC_FAILURE, "Dynamic nodes are disabled");
    return reply;
  }
  if (request.node_names().empty()) {
    fail(crane::grpc::ERR_INVALID_PARAM, "No node name specified");
    return reply;
  }
  if (HasDuplicates(request.node_names())) {
    fail(crane::grpc::ERR_INVALID_PARAM,
         "Duplicate node names are not allowed");
    return reply;
  }
  if (!request.has_spec() || request.spec().cpu_count() == 0 ||
      request.spec().memory_bytes() == 0 || request.spec().sockets() == 0 ||
      request.spec().sockets() > request.spec().cpu_count()) {
    fail(crane::grpc::ERR_INVALID_PARAM,
         "CPU, memory and sockets must form a valid node spec");
    return reply;
  }
  if (request.partition_names().empty()) {
    fail(crane::grpc::ERR_INVALID_PARAM,
         "At least one partition must be specified");
    return reply;
  }
  if (HasDuplicates(request.partition_names())) {
    fail(crane::grpc::ERR_INVALID_PARAM,
         "Duplicate partitions are not allowed");
    return reply;
  }
  for (const auto& partition_id : request.partition_names()) {
    if (!g_config.Partitions.contains(partition_id)) {
      fail(crane::grpc::ERR_INVALID_PARAM,
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
    fail(crane::grpc::ERR_NODE_LIMIT_REACHED,
         fmt::format("MaxNodeCount {} would be exceeded",
                     g_config.CtldConf.MaxNodeCount));
    return reply;
  }

  std::vector<DynamicNodeRecord> new_records;
  new_records.reserve(request.node_names_size());
  for (const auto& node_id : request.node_names()) {
    auto name_result = ValidateNodeName(node_id);
    if (!name_result) {
      fail(name_result.error().code(), name_result.error().description());
      return reply;
    }
    if (g_config.Nodes.contains(node_id)) {
      fail(crane::grpc::ERR_NODE_ALREADY_EXISTS,
           fmt::format("Node {} is static", node_id));
      return reply;
    }

    auto it = records_.find(node_id);
    if (it != records_.end()) {
      if (it->second.lifecycle() != DYNAMIC_NODE_LIFECYCLE_TOMBSTONE) {
        fail(crane::grpc::ERR_NODE_ALREADY_EXISTS,
             fmt::format("Node {} already exists", node_id));
        return reply;
      }
    }
    auto generation = NextGenerationNoLock_(node_id);
    if (!generation) {
      fail(generation.error().code(), generation.error().description());
      return reply;
    }

    DynamicNodeRecord record;
    record.set_node_name(node_id);
    record.set_schema_version(kDynamicNodeSchemaVersion);
    *record.mutable_spec() = request.spec();
    record.mutable_partition_names()->CopyFrom(request.partition_names());
    record.set_generation(*generation);
    record.set_origin(request.origin() ==
                              crane::grpc::DYNAMIC_NODE_ORIGIN_UNSPECIFIED
                          ? DYNAMIC_NODE_ORIGIN_DYNAMIC_ADMIN
                          : request.origin());
    if (record.origin() != DYNAMIC_NODE_ORIGIN_DYNAMIC_ADMIN) {
      fail(crane::grpc::ERR_INVALID_PARAM,
           "Only administrator-created dynamic nodes can be created");
      return reply;
    }
    record.set_lifecycle(request.lifecycle() ==
                                 crane::grpc::DYNAMIC_NODE_LIFECYCLE_UNSPECIFIED
                             ? DYNAMIC_NODE_LIFECYCLE_FUTURE
                             : request.lifecycle());
    if (record.lifecycle() != DYNAMIC_NODE_LIFECYCLE_FUTURE) {
      fail(crane::grpc::ERR_INVALID_PARAM,
           "New dynamic nodes must start in FUTURE state");
      return reply;
    }
    if (request.power_state() != DYNAMIC_NODE_POWER_STATE_UNSPECIFIED &&
        request.power_state() != DYNAMIC_NODE_POWER_STATE_OFF) {
      fail(crane::grpc::ERR_INVALID_PARAM,
           "New dynamic nodes must start powered off");
      return reply;
    }
    record.set_power_state(DYNAMIC_NODE_POWER_STATE_OFF);
    record.set_pool(request.pool());
    record.set_provider(request.provider());
    record.set_provider_profile(request.provider_profile());
    if (!record.provider_profile().empty() && record.provider().empty()) {
      fail(crane::grpc::ERR_INVALID_PARAM, "ProviderProfile requires Provider");
      return reply;
    }
    *record.mutable_effective_spec() = record.spec();
    auto validation = ValidatePresentRecord_(record);
    if (!validation) {
      fail(validation.error().code(), validation.error().description());
      return reply;
    }
    record.set_revision(++catalog_revision_);
    new_records.emplace_back(std::move(record));
  }

  if (!g_embedded_db_client->StoreDynamicNodeRecords(new_records)) {
    fail(crane::grpc::ERR_SYSTEM_ERR, "Failed to persist dynamic nodes");
    return reply;
  }
  for (const auto& record : new_records) records_[record.node_name()] = record;
  for (const auto& record : new_records)
    generation_high_watermarks_[record.node_name()] = record.generation();
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

  auto reject = [&reply](const CranedId& node_id, crane::grpc::ErrCode code,
                         std::string_view reason) {
    auto* result = reply.add_not_deleted_nodes();
    result->set_node_name(node_id);
    result->set_code(code);
    result->set_reason(std::string(reason));
  };
  if (HasDuplicates(request.node_names())) {
    for (const auto& node_id : request.node_names())
      reject(node_id, crane::grpc::ERR_INVALID_PARAM,
             "Duplicate node names are not allowed");
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
      reject(node_id, crane::grpc::ERR_NODE_NOT_DYNAMIC, "Node is static");
      continue;
    }
    auto it = records_.find(node_id);
    if (it == records_.end() ||
        it->second.lifecycle() == DYNAMIC_NODE_LIFECYCLE_TOMBSTONE) {
      reject(node_id, crane::grpc::ERR_INVALID_PARAM, "Node does not exist");
      continue;
    }
    const DynamicNodeRecord& record = it->second;
    const bool quarantined = quarantined_nodes_.contains(node_id);
    if (!quarantined) {
      if (record.lifecycle() == DYNAMIC_NODE_LIFECYCLE_REGISTERING &&
          !RegistrationLeaseExpired_(record)) {
        reject(node_id, crane::grpc::ERR_NODE_BUSY,
               "Node is still registering");
        continue;
      }
      if (IsTransitionalPowerState(record.power_state())) {
        if (!PowerActionExpired(record)) {
          reject(node_id, crane::grpc::ERR_NODE_BUSY,
                 "Node has a power action in progress");
          continue;
        }
      } else if (record.provider() == kPowerControlProvider &&
                 record.power_state() != DYNAMIC_NODE_POWER_STATE_OFF) {
        reject(node_id, crane::grpc::ERR_NODE_BUSY,
               "Node must be powered off before deletion");
        continue;
      }
    }
    // Quarantined nodes were never added to the runtime topology, so the
    // topology-side deleting steps must be skipped for them.
    pending.emplace_back(
        node_id, record,
        record.lifecycle() != DYNAMIC_NODE_LIFECYCLE_DELETING && !quarantined);
  }
  if (pending.empty()) return reply;

  {
    std::vector<CranedId> pending_ids;
    pending_ids.reserve(pending.size());
    for (const auto& node : pending) pending_ids.emplace_back(node.node_id);
    auto busy_nodes = g_job_scheduler->FilterNodesWithJobs(pending_ids);
    std::erase_if(pending, [&](const PendingDeletion& node) {
      if (!busy_nodes.contains(node.node_id)) return false;
      reject(node.node_id, crane::grpc::ERR_NODE_BUSY,
             "Node is still referenced by jobs");
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
      reject(node.node_id, failure->second.code(),
             failure->second.description());
      return true;
    });
  }

  // An idle node may still be connected when it is deleted; no prior drain
  // or disconnect is required. Take it offline now so the topology removal
  // below sees it dead; the craned learns on its next ping renewal that its
  // generation is stale and shuts itself down, and the leftover stub and
  // address cache entry are dropped once the connection actually breaks.
  for (const auto& node : pending) {
    if (!g_craned_keeper->ForgetCraned(node.node_id))
      g_meta_container->CranedDown(node.node_id);
  }

  std::vector<DynamicNodeRecord> deleting_records;
  deleting_records.reserve(pending.size());
  for (const auto& node : pending) {
    DynamicNodeRecord record = node.record;
    // The generation is kept as-is: the tombstone rejects any registration
    // of this incarnation, and a re-created node gets a strictly larger
    // generation from the persisted high watermark (NextGenerationNoLock_).
    record.set_lifecycle(DYNAMIC_NODE_LIFECYCLE_DELETING);
    record.clear_registration_token_digest();
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
      reject(node.node_id, crane::grpc::ERR_SYSTEM_ERR,
             "Failed to persist dynamic node deletion intent");
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
               restored ? failure->second.code() : crane::grpc::ERR_SYSTEM_ERR,
               restored
                   ? failure->second.description()
                   : fmt::format(
                         "{}; failed to restore dynamic node deletion state",
                         failure->second.description()));
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
      reject(node.node_id, crane::grpc::ERR_SYSTEM_ERR,
             "Failed to persist dynamic node tombstones");
    return reply;
  }
  for (const auto& record : tombstones) records_[record.node_name()] = record;
  for (const auto& record : tombstones) {
    PublishNodeDefinition(record,
                          crane::grpc::plugin::NODE_DEFINITION_ACTION_REMOVE);
  }
  for (const auto& node : pending) {
    quarantined_nodes_.erase(node.node_id);
    reply.add_deleted_nodes(node.node_id);
  }

  return reply;
}

void NodeManager::CleanupExpiredTombstonesNoLock_() {
  const int64_t expire_before =
      absl::ToUnixSeconds(absl::Now()) -
      g_config.CtldConf.DynamicNodes.TombstoneRetentionSeconds;
  std::vector<DynamicNodeRecord> expired;
  for (const auto& record : records_ | std::views::values) {
    if (record.lifecycle() != DYNAMIC_NODE_LIFECYCLE_TOMBSTONE) continue;
    if (!record.has_tombstone_time() ||
        record.tombstone_time().seconds() <= expire_before)
      expired.emplace_back(record);
  }
  if (expired.empty()) return;
  if (!g_embedded_db_client->DeleteDynamicNodeRecords(expired)) {
    CRANE_WARN("Failed to purge expired dynamic node tombstones.");
    return;
  }
  for (const auto& record : expired) records_.erase(record.node_name());
}

CraneExpectedRich<uint64_t> NodeManager::NextGenerationNoLock_(
    const CranedId& node_id) const {
  uint64_t high_watermark = 0;
  if (auto it = generation_high_watermarks_.find(node_id);
      it != generation_high_watermarks_.end())
    high_watermark = it->second;
  if (auto it = records_.find(node_id); it != records_.end())
    high_watermark = std::max(high_watermark, it->second.generation());
  if (high_watermark == std::numeric_limits<uint64_t>::max())
    return std::unexpected(FormatRichErr(crane::grpc::ERR_GENERIC_FAILURE,
                                         "Node {} generation is exhausted",
                                         node_id));
  return high_watermark + 1;
}

CraneExpectedRich<void> NodeManager::ReleaseExpiredRegistrationLeasesNoLock_() {
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
    return std::unexpected(
        FormatRichErr(crane::grpc::ERR_SYSTEM_ERR,
                      "Failed to release expired registration leases"));
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
  const auto fail = [&reply](crane::grpc::ErrCode code,
                             std::string_view reason) {
    reply.set_code(code);
    reply.set_reason(std::string(reason));
  };
  if (!g_config.CtldConf.DynamicNodes.Enabled) {
    fail(crane::grpc::ERR_GENERIC_FAILURE,
         "Dynamic node registration is disabled");
    return reply;
  }
  if (!g_config.ListenConf.TlsConfig.Enabled) {
    fail(crane::grpc::ERR_GENERIC_FAILURE,
         "Dynamic node registration requires TLS");
    return reply;
  }
  if (request.mode() ==
          crane::grpc::DYNAMIC_NODE_REGISTRATION_MODE_UNSPECIFIED ||
      request.physical_hostname().empty() || request.client_nonce().empty()) {
    fail(crane::grpc::ERR_INVALID_PARAM,
         "Registration mode, hostname and nonce are required");
    return reply;
  }

  const DynamicNodeSpec& reported_spec = request.reported_spec();
  auto reported_validation = ValidateReportedSpecStructure_(reported_spec);
  if (!reported_validation) {
    fail(reported_validation.error().code(),
         reported_validation.error().description());
    return reply;
  }
  if (HasDuplicates(request.requested_partitions())) {
    fail(crane::grpc::ERR_INVALID_PARAM,
         "Duplicate partitions are not allowed");
    return reply;
  }

  if (g_config.Nodes.contains(request.physical_hostname())) {
    fail(crane::grpc::ERR_NODE_ALREADY_EXISTS,
         "Physical host is configured as a static node");
    return reply;
  }

  absl::MutexLock lock(&mutex_);
  auto lease_result = ReleaseExpiredRegistrationLeasesNoLock_();
  if (!lease_result) {
    fail(lease_result.error().code(), lease_result.error().description());
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

  // Returns the raw registration token; only its digest is stored in the
  // record. A retried preparation therefore always rotates the token.
  auto prepare_record = [&](DynamicNodeRecord* record) {
    std::string token = GenerateRegistrationToken_();
    record->set_lifecycle(DYNAMIC_NODE_LIFECYCLE_REGISTERING);
    record->set_physical_hostname(request.physical_hostname());
    record->set_registration_nonce(request.client_nonce());
    record->set_registration_token_digest(util::Sha256Hex(token));
    *record->mutable_reported_spec() = reported_spec;
    record->set_revision(++catalog_revision_);
    *record->mutable_lease_expire_time() = make_expire_time();
    if (!record->has_effective_spec())
      *record->mutable_effective_spec() = record->spec();
    return token;
  };

  if (request.mode() ==
      crane::grpc::DYNAMIC_NODE_REGISTRATION_MODE_PRECREATED) {
    if (request.requested_node_name().empty()) {
      fail(crane::grpc::ERR_INVALID_PARAM,
           "Precreated registration requires a node name");
      return reply;
    }
    auto it = records_.find(request.requested_node_name());
    if (it == records_.end() || !IsDynamicRecordPresent(it->second)) {
      fail(request.generation() != 0 ? crane::grpc::ERR_NODE_STALE_GENERATION
                                     : crane::grpc::ERR_INVALID_PARAM,
           "Dynamic node is not precreated");
      return reply;
    }
    if (quarantined_nodes_.contains(request.requested_node_name())) {
      fail(crane::grpc::ERR_INVALID_PARAM,
           "Dynamic node references a partition that no longer exists");
      return reply;
    }
    if (request.generation() != 0 &&
        request.generation() != it->second.generation()) {
      fail(crane::grpc::ERR_NODE_STALE_GENERATION,
           "Dynamic node generation has changed");
      return reply;
    }
    if (it->second.origin() != DYNAMIC_NODE_ORIGIN_DYNAMIC_ADMIN) {
      fail(crane::grpc::ERR_INVALID_PARAM,
           "Node is not an administrator-created dynamic node");
      return reply;
    }
    if (!request.pool().empty() && request.pool() != it->second.pool()) {
      fail(crane::grpc::ERR_INVALID_PARAM,
           "Requested pool does not match the precreated node");
      return reply;
    }
    if (!HasRequestedPartition(request.requested_partitions(),
                               it->second.partition_names())) {
      fail(crane::grpc::ERR_INVALID_PARAM,
           "Requested partitions do not match the precreated node");
      return reply;
    }
    if (it->second.ever_registered() &&
        !it->second.physical_hostname().empty() &&
        it->second.physical_hostname() != request.physical_hostname()) {
      fail(crane::grpc::ERR_NODE_BUSY,
           "Dynamic node is owned by another physical host");
      return reply;
    }
    if (auto bound_node = FindNodeByPhysicalHostnameNoLock_(
            request.physical_hostname(), request.requested_node_name())) {
      fail(crane::grpc::ERR_NODE_ALREADY_EXISTS,
           fmt::format("Physical host is already registered as {}",
                       *bound_node));
      return reply;
    }
    // A retry from the same client (same nonce and host) falls through and
    // rotates the lease; only a lease held by a different client blocks.
    if (it->second.lifecycle() == DYNAMIC_NODE_LIFECYCLE_REGISTERING &&
        !RegistrationLeaseExpired_(it->second) &&
        (it->second.registration_nonce() != request.client_nonce() ||
         it->second.physical_hostname() != request.physical_hostname())) {
      fail(crane::grpc::ERR_NODE_BUSY, "Dynamic node is already registering");
      return reply;
    }
    auto result =
        ValidateReportedSpec_(EffectiveSpec(it->second), reported_spec);
    if (!result) {
      fail(result.error().code(), result.error().description());
      return reply;
    }
    DynamicNodeRecord prepared = it->second;
    const std::string token = prepare_record(&prepared);
    if (!g_embedded_db_client->StoreDynamicNodeRecords({prepared})) {
      fail(crane::grpc::ERR_SYSTEM_ERR,
           "Failed to persist dynamic registration lease");
      return reply;
    }
    it->second = std::move(prepared);
    g_meta_container->UpdateDynamicNodeMetadata(it->second);
    FillPreparationReply_(it->second, &reply);
    reply.set_registration_token(token);
    return reply;
  }

  if (request.mode() ==
      crane::grpc::DYNAMIC_NODE_REGISTRATION_MODE_AUTO_CREATE) {
    if (!g_config.CtldConf.DynamicNodes.AutoCreate) {
      fail(crane::grpc::ERR_GENERIC_FAILURE,
           "Dynamic auto-create registration is disabled");
      return reply;
    }
    if (request.pool().empty()) {
      fail(crane::grpc::ERR_INVALID_PARAM,
           "Dynamic auto-create registration requires a pool");
      return reply;
    }
    const auto& policies = g_config.CtldConf.DynamicNodes.AutoCreatePools;
    auto policy_it = std::ranges::find_if(policies, [&](const auto& policy) {
      return policy.Name == request.pool();
    });
    if (policy_it == policies.end()) {
      fail(crane::grpc::ERR_INVALID_PARAM,
           "Dynamic auto-create pool is not allowed");
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
      fail(crane::grpc::ERR_NODE_ALREADY_EXISTS,
           fmt::format("Physical host is already registered as {}",
                       *bound_node));
      return reply;
    }
    auto name_result = ValidateNodeName(node_name);
    if (!name_result) {
      fail(name_result.error().code(), name_result.error().description());
      return reply;
    }
    if (!std::regex_match(node_name, std::regex(policy.NodeNamePattern))) {
      fail(crane::grpc::ERR_INVALID_PARAM,
           "Node name is outside the auto-create pool policy");
      return reply;
    }
    if (g_config.Nodes.contains(node_name)) {
      fail(crane::grpc::ERR_NODE_ALREADY_EXISTS,
           "Auto-create node conflicts with a static node");
      return reply;
    }
    if (reported_spec.cpu_count() < policy.MinCpu ||
        reported_spec.cpu_count() > policy.MaxCpu ||
        reported_spec.memory_bytes() < policy.MinMemoryBytes ||
        reported_spec.memory_bytes() > policy.MaxMemoryBytes ||
        reported_spec.sockets() < policy.MinSockets ||
        reported_spec.sockets() > policy.MaxSockets) {
      fail(crane::grpc::ERR_INVALID_PARAM,
           "Reported resources are outside the auto-create pool bounds");
      return reply;
    }
    for (const auto& feature : policy.RequiredFeatures) {
      if (std::ranges::find(reported_spec.features(), feature) ==
          reported_spec.features().end()) {
        fail(crane::grpc::ERR_INVALID_PARAM,
             "Reported features do not satisfy the auto-create pool");
        return reply;
      }
    }
    for (const auto& feature : reported_spec.features()) {
      if (!policy.AllowedFeatures.contains(feature)) {
        fail(crane::grpc::ERR_INVALID_PARAM,
             "Reported feature is not allowed by the auto-create pool");
        return reply;
      }
    }
    for (const auto& partition : request.requested_partitions()) {
      if (std::ranges::find(policy.Partitions, partition) ==
          policy.Partitions.end()) {
        fail(crane::grpc::ERR_INVALID_PARAM,
             "Requested partition is not allowed by the auto-create pool");
        return reply;
      }
    }

    std::map<std::pair<std::string, std::string>, uint64_t> reported_gres;
    for (const auto& [name, count] : reported_spec.gres().name_gres_map()) {
      uint64_t untyped = count.total();
      for (const auto& [type, type_count] : count.specified()) {
        untyped -= type_count;
        reported_gres.emplace(std::pair{name, type}, type_count);
      }
      if (untyped > 0)
        reported_gres.emplace(std::pair{name, std::string{}}, untyped);
    }
    for (const auto& [name_type, count] : reported_gres) {
      auto gres_policy =
          std::ranges::find_if(policy.Gres, [&](const auto& range) {
            return range.Name == name_type.first &&
                   range.Type == name_type.second;
          });
      if (gres_policy == policy.Gres.end()) {
        fail(crane::grpc::ERR_INVALID_PARAM,
             "Reported GRES is not allowed by the auto-create pool");
        return reply;
      }
    }
    for (const auto& range : policy.Gres) {
      uint64_t count = 0;
      if (auto it = reported_gres.find({range.Name, range.Type});
          it != reported_gres.end())
        count = it->second;
      if (count < range.Min || count > range.Max) {
        fail(crane::grpc::ERR_INVALID_PARAM,
             "Reported GRES is outside the auto-create pool bounds");
        return reply;
      }
    }

    if (existing != records_.end() &&
        IsDynamicRecordPresent(existing->second)) {
      if (quarantined_nodes_.contains(existing->first)) {
        fail(crane::grpc::ERR_INVALID_PARAM,
             "Dynamic node references a partition that no longer exists");
        return reply;
      }
      if (existing->second.origin() != DYNAMIC_NODE_ORIGIN_DYNAMIC_REGISTERED) {
        fail(crane::grpc::ERR_NODE_ALREADY_EXISTS,
             "Node name is reserved by a precreated dynamic node");
        return reply;
      }
      if (!std::ranges::all_of(
              existing->second.partition_names(), [&](const auto& partition) {
                return std::ranges::find(policy.Partitions, partition) !=
                       policy.Partitions.end();
              })) {
        fail(crane::grpc::ERR_INVALID_PARAM,
             "Dynamic node partitions are outside the auto-create pool");
        return reply;
      }
      if (request.generation() != 0 &&
          request.generation() != existing->second.generation()) {
        fail(crane::grpc::ERR_NODE_STALE_GENERATION,
             "Dynamic node generation has changed");
        return reply;
      }
      if (request.pool() != existing->second.pool()) {
        fail(crane::grpc::ERR_INVALID_PARAM,
             "Requested pool does not match the dynamic node");
        return reply;
      }
      if (!HasRequestedPartition(request.requested_partitions(),
                                 existing->second.partition_names())) {
        fail(crane::grpc::ERR_INVALID_PARAM,
             "Requested partitions do not match the dynamic node");
        return reply;
      }
      // A retry from the same client (same nonce and host) falls through and
      // rotates the lease; only a lease held by a different client blocks.
      if (existing->second.lifecycle() == DYNAMIC_NODE_LIFECYCLE_REGISTERING &&
          !RegistrationLeaseExpired_(existing->second) &&
          (existing->second.registration_nonce() != request.client_nonce() ||
           existing->second.physical_hostname() !=
               request.physical_hostname())) {
        fail(crane::grpc::ERR_NODE_BUSY, "Dynamic node is already registering");
        return reply;
      }
      if (existing->second.physical_hostname() != request.physical_hostname()) {
        fail(crane::grpc::ERR_NODE_BUSY,
             "Dynamic node is owned by another physical host");
        return reply;
      }
      auto result =
          ValidateReportedSpec_(EffectiveSpec(existing->second), reported_spec);
      if (!result) {
        fail(result.error().code(), result.error().description());
        return reply;
      }
      DynamicNodeRecord prepared = existing->second;
      const std::string token = prepare_record(&prepared);
      if (!g_embedded_db_client->StoreDynamicNodeRecords({prepared})) {
        fail(crane::grpc::ERR_SYSTEM_ERR,
             "Failed to persist dynamic registration lease");
        return reply;
      }
      existing->second = std::move(prepared);
      g_meta_container->UpdateDynamicNodeMetadata(existing->second);
      FillPreparationReply_(existing->second, &reply);
      reply.set_registration_token(token);
      return reply;
    }

    if (request.generation() != 0) {
      fail(crane::grpc::ERR_NODE_STALE_GENERATION,
           "Dynamic node is no longer available");
      return reply;
    }
    if (existing != records_.end() &&
        existing->second.lifecycle() == DYNAMIC_NODE_LIFECYCLE_DELETING) {
      fail(crane::grpc::ERR_NODE_BUSY,
           "Dynamic node deletion is still in progress");
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
        fail(crane::grpc::ERR_NODE_LIMIT_REACHED,
             "Dynamic auto-create node limit reached");
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
      fail(crane::grpc::ERR_NODE_LIMIT_REACHED,
           "Dynamic auto-create pool node limit reached");
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
      fail(crane::grpc::ERR_NODE_LIMIT_REACHED,
           "MaxNodeCount would be exceeded");
      return reply;
    }

    DynamicNodeRecord record;
    record.set_node_name(node_name);
    record.set_schema_version(kDynamicNodeSchemaVersion);
    *record.mutable_spec() = reported_spec;
    if (request.requested_partitions().empty())
      record.mutable_partition_names()->Assign(policy.Partitions.begin(),
                                               policy.Partitions.end());
    else
      record.mutable_partition_names()->CopyFrom(
          request.requested_partitions());
    auto generation = NextGenerationNoLock_(node_name);
    if (!generation) {
      fail(generation.error().code(), generation.error().description());
      return reply;
    }
    record.set_generation(*generation);
    record.set_origin(DYNAMIC_NODE_ORIGIN_DYNAMIC_REGISTERED);
    record.set_pool(policy.Name);
    record.set_lifecycle(DYNAMIC_NODE_LIFECYCLE_REGISTERING);
    record.set_power_state(crane::grpc::DYNAMIC_NODE_POWER_STATE_ON);
    *record.mutable_effective_spec() = record.spec();
    auto validation = ValidatePresentRecord_(record);
    if (!validation) {
      fail(validation.error().code(), validation.error().description());
      return reply;
    }
    const std::string token = prepare_record(&record);
    if (!g_embedded_db_client->StoreDynamicNodeRecords({record})) {
      fail(crane::grpc::ERR_SYSTEM_ERR,
           "Failed to persist auto-created dynamic node");
      return reply;
    }
    records_[node_name] = record;
    generation_high_watermarks_[node_name] = record.generation();
    g_meta_container->AddDynamicNodes({record});
    PublishNodeDefinition(record,
                          crane::grpc::plugin::NODE_DEFINITION_ACTION_UPSERT);
    CRANE_INFO("Auto-created dynamic node {} in pool {} for host {}.",
               node_name, policy.Name, request.physical_hostname());
    FillPreparationReply_(record, &reply);
    reply.set_registration_token(token);
    return reply;
  }

  if (request.mode() !=
      crane::grpc::DYNAMIC_NODE_REGISTRATION_MODE_FUTURE_POOL) {
    fail(crane::grpc::ERR_INVALID_PARAM,
         "Unsupported dynamic registration mode");
    return reply;
  }
  if (request.pool().empty()) {
    fail(crane::grpc::ERR_INVALID_PARAM, "FUTURE registration requires a pool");
    return reply;
  }
  if (!request.requested_node_name().empty()) {
    fail(crane::grpc::ERR_INVALID_PARAM,
         "FUTURE registration cannot request a node name");
    return reply;
  }

  std::vector<std::reference_wrapper<DynamicNodeRecord>> candidates;
  for (auto& [node_name, record] : records_) {
    if (record.origin() != DYNAMIC_NODE_ORIGIN_DYNAMIC_ADMIN ||
        quarantined_nodes_.contains(node_name))
      continue;
    // A retry from the same client rotates its existing lease instead of
    // leasing another FUTURE slot.
    if (record.lifecycle() == DYNAMIC_NODE_LIFECYCLE_REGISTERING &&
        record.registration_nonce() == request.client_nonce() &&
        record.physical_hostname() == request.physical_hostname() &&
        (request.generation() == 0 ||
         record.generation() == request.generation()) &&
        record.pool() == request.pool() && !RegistrationLeaseExpired_(record)) {
      DynamicNodeRecord prepared = record;
      const std::string token = prepare_record(&prepared);
      if (!g_embedded_db_client->StoreDynamicNodeRecords({prepared})) {
        fail(crane::grpc::ERR_SYSTEM_ERR,
             "Failed to persist FUTURE node lease");
        return reply;
      }
      record = std::move(prepared);
      g_meta_container->UpdateDynamicNodeMetadata(record);
      FillPreparationReply_(record, &reply);
      reply.set_registration_token(token);
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
    if ((!future_candidate && !down_candidate && !active_candidate) ||
        record.pool() != request.pool())
      continue;
    if (!record.registration_token_digest().empty() &&
        !RegistrationLeaseExpired_(record))
      continue;
    const auto& expected = EffectiveSpec(record);
    // CPU count, sockets and GRES must match exactly. Memory may exceed the
    // configured value; the effective spec stays capped at it.
    if (!ValidateReportedSpec_(expected, reported_spec) ||
        reported_spec.cpu_count() != expected.cpu_count() ||
        !GresCountsMatch_(expected.gres(), reported_spec.gres()))
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
    fail(request.generation() != 0 ? crane::grpc::ERR_NODE_STALE_GENERATION
                                   : crane::grpc::ERR_INVALID_PARAM,
         "No FUTURE node matches the reported resources");
    return reply;
  }
  auto& record = candidates.front().get();
  if (auto bound_node = FindNodeByPhysicalHostnameNoLock_(
          request.physical_hostname(), record.node_name())) {
    fail(crane::grpc::ERR_NODE_ALREADY_EXISTS,
         fmt::format("Physical host is already registered as {}", *bound_node));
    return reply;
  }
  DynamicNodeRecord prepared = record;
  const std::string token = prepare_record(&prepared);
  if (!g_embedded_db_client->StoreDynamicNodeRecords({prepared})) {
    fail(crane::grpc::ERR_SYSTEM_ERR, "Failed to persist FUTURE node lease");
    return reply;
  }
  record = std::move(prepared);
  g_meta_container->UpdateDynamicNodeMetadata(record);
  FillPreparationReply_(record, &reply);
  reply.set_registration_token(token);
  return reply;
}

CraneExpectedRich<void> NodeManager::BeginRegistration(
    const CranedId& node_id, uint64_t generation, const RegToken& token,
    std::string_view registration_token) {
  absl::MutexLock lock(&mutex_);
  auto result = ValidateRegistrationNoLock_(node_id, generation);
  if (!result) return std::unexpected(result.error());

  std::string connection_hostname = node_id;
  auto it = records_.find(node_id);
  if (!g_config.Nodes.contains(node_id) && it != records_.end()) {
    auto token_result =
        ValidateRegistrationToken_(it->second, registration_token);
    if (!token_result) return std::unexpected(token_result.error());
    connection_hostname = it->second.physical_hostname();
  }

  if (!g_craned_keeper->PutNodeIntoUnavailSet(node_id, token,
                                              std::move(connection_hostname))) {
    // Registration is serialized by mutex_, so the channel cannot finish the
    // registration before the previous runtime state is marked down.
    g_meta_container->CranedDown(node_id);
  }
  return {};
}

CraneExpectedRich<void> NodeManager::ValidateRegistration(
    const CranedId& node_id, uint64_t generation,
    const crane::grpc::CranedRemoteMeta& remote_meta,
    std::string_view registration_token) {
  absl::MutexLock lock(&mutex_);
  auto result = ValidateRegistrationNoLock_(node_id, generation);
  if (!result) return result;
  if (g_config.Nodes.contains(node_id)) return {};

  // ValidateRegistrationNoLock_ guarantees a present record for non-static
  // nodes.
  auto it = records_.find(node_id);
  CRANE_ASSERT(it != records_.end());
  auto registered =
      BuildRegisteredRecord_(it->second, remote_meta, registration_token);
  if (!registered) return std::unexpected(registered.error());
  return {};
}

CraneExpectedRich<DynamicNodeRecord> NodeManager::BuildRegisteredRecord_(
    const DynamicNodeRecord& record,
    const crane::grpc::CranedRemoteMeta& remote_meta,
    std::string_view registration_token) const {
  auto token_result = ValidateRegistrationToken_(record, registration_token);
  if (!token_result) return std::unexpected(token_result.error());
  if (remote_meta.physical_hostname().empty() ||
      remote_meta.physical_hostname() != record.physical_hostname())
    return std::unexpected(
        FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                      "Physical hostname does not match registration"));
  if (!remote_meta.has_reported_spec())
    return std::unexpected(
        FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                      "Dynamic node did not report its resource spec"));

  DynamicNodeSpec reported = remote_meta.reported_spec();
  const auto& expected = record.spec();
  auto spec_result = ValidateReportedSpec_(expected, reported);
  if (!spec_result) return std::unexpected(spec_result.error());

  auto gres_validation = ValidateGresDefinition(remote_meta.dres_in_node());
  if (!gres_validation) return std::unexpected(gres_validation.error());
  if (!GresCountsMatch_(expected.gres(),
                        GresCountsFromSlots(remote_meta.dres_in_node())))
    return std::unexpected(FormatRichErr(
        crane::grpc::ERR_INVALID_PARAM,
        "Registered GRES allocation does not match the effective node spec"));

  DynamicNodeRecord registered = record;
  *registered.mutable_reported_spec() = reported;
  if (!registered.ever_registered())
    *registered.mutable_effective_spec() = expected;
  *registered.mutable_registered_gres() = remote_meta.dres_in_node();
  registered.set_physical_hostname(remote_meta.physical_hostname());
  registered.mutable_network_interfaces()->CopyFrom(
      remote_meta.network_interfaces());
  return registered;
}

CraneExpectedRich<void> NodeManager::MarkRegistered(
    const CranedId& node_id, uint64_t generation,
    const crane::grpc::CranedRemoteMeta& remote_meta,
    std::string_view registration_token) {
  absl::MutexLock lock(&mutex_);
  auto result = ValidateRegistrationNoLock_(node_id, generation);
  if (!result) return result;
  if (g_config.Nodes.contains(node_id)) return {};

  // ValidateRegistrationNoLock_ guarantees a present record for non-static
  // nodes.
  auto it = records_.find(node_id);
  CRANE_ASSERT(it != records_.end());

  auto registered =
      BuildRegisteredRecord_(it->second, remote_meta, registration_token);
  if (!registered) return std::unexpected(registered.error());
  DynamicNodeRecord record = std::move(registered.value());
  record.set_ever_registered(true);
  record.set_lifecycle(DYNAMIC_NODE_LIFECYCLE_ACTIVE);
  record.set_power_state(DYNAMIC_NODE_POWER_STATE_ON);
  record.clear_registration_token_digest();
  record.clear_registration_nonce();
  record.clear_lease_expire_time();
  record.set_revision(++catalog_revision_);
  if (!g_embedded_db_client->StoreDynamicNodeRecords({record})) {
    // Not dead code: CranedUp has already rebound the node's GRES to the
    // freshly reported devices. Restore the runtime metadata from the
    // unmodified record.
    g_meta_container->UpdateDynamicNodeMetadata(it->second);
    return std::unexpected(
        FormatRichErr(crane::grpc::ERR_SYSTEM_ERR,
                      "Failed to persist dynamic node registration"));
  }

  it->second = std::move(record);
  g_meta_container->UpdateDynamicNodeMetadata(it->second);
  CRANE_INFO("Dynamic node {} registered with generation {}.", node_id,
             it->second.generation());
  return {};
}

CraneExpectedRich<void> NodeManager::MarkRegistrationFailed(
    const CranedId& node_id, uint64_t generation,
    std::string_view registration_token) {
  absl::MutexLock lock(&mutex_);
  auto it = records_.find(node_id);
  if (it == records_.end() || it->second.generation() != generation ||
      !ValidateRegistrationToken_(it->second, registration_token))
    return {};
  return MarkDisconnectedNoLock_(node_id, generation);
}

CraneExpectedRich<void> NodeManager::MarkDisconnectedIfUntracked(
    const CranedId& node_id) {
  absl::MutexLock lock(&mutex_);
  if (g_craned_keeper->IsCranedTracked(node_id)) return {};
  g_meta_container->CranedDown(node_id);
  auto it = records_.find(node_id);
  if (it != records_.end() &&
      it->second.lifecycle() == DYNAMIC_NODE_LIFECYCLE_TOMBSTONE) {
    // The node was deleted while still connected; drop the leftover address
    // cache entry now that the connection is gone.
    g_craned_keeper->ForgetCraned(node_id);
    return {};
  }
  // A delayed callback from the old connection must not revoke a new lease.
  if (it != records_.end() &&
      it->second.lifecycle() == DYNAMIC_NODE_LIFECYCLE_REGISTERING &&
      !RegistrationLeaseExpired_(it->second))
    return {};
  return MarkDisconnectedNoLock_(node_id, 0);
}

CraneExpectedRich<void> NodeManager::MarkDisconnectedNoLock_(
    const CranedId& node_id, uint64_t generation) {
  auto it = records_.find(node_id);
  if (it == records_.end()) return {};
  if ((generation != 0 && it->second.generation() != generation) ||
      !IsDynamicRecordPresent(it->second))
    return {};

  DynamicNodeRecord record = it->second;
  if (!ResetRegistrationState(&record)) return {};
  record.set_revision(++catalog_revision_);
  if (!g_embedded_db_client->StoreDynamicNodeRecords({record}))
    return std::unexpected(
        FormatRichErr(crane::grpc::ERR_SYSTEM_ERR,
                      "Failed to persist dynamic node down state"));
  it->second = record;
  g_meta_container->UpdateDynamicNodeMetadata(record);
  return {};
}

CraneExpectedRich<bool> NodeManager::UpdatePowerState(
    const CranedId& node_id, uint64_t generation,
    crane::grpc::CranedPowerState power_state) {
  absl::MutexLock lock(&mutex_);
  auto dynamic_power_state = DynamicNodePowerStateFromCraned(power_state);
  if (!dynamic_power_state)
    return std::unexpected(FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                                         "Invalid Craned power state"));
  if (g_config.Nodes.contains(node_id)) {
    // Ignore a delayed report from a former dynamic incarnation of this name.
    return generation != 0;
  }

  auto it = records_.find(node_id);
  if (it == records_.end()) return generation != 0;
  if (quarantined_nodes_.contains(node_id) ||
      !IsDynamicRecordPresent(it->second) ||
      it->second.generation() != generation)
    return true;

  if (it->second.power_state() == *dynamic_power_state) return false;
  DynamicNodeRecord record = it->second;
  SetRecordPowerState(&record, *dynamic_power_state);
  record.set_revision(++catalog_revision_);
  if (!g_embedded_db_client->StoreDynamicNodeRecords({record}))
    return std::unexpected(
        FormatRichErr(crane::grpc::ERR_SYSTEM_ERR,
                      "Failed to persist dynamic node power state"));
  it->second = record;
  g_meta_container->UpdateDynamicNodeMetadata(record);
  return false;
}

bool NodeManager::HasScalableNodes(const PartitionId& partition) {
  absl::MutexLock lock(&mutex_);
  return std::ranges::any_of(records_, [&](const auto& entry) {
    const auto& record = entry.second;
    return IsDynamicRecordPresent(record) &&
           !quarantined_nodes_.contains(entry.first) &&
           (record.lifecycle() == DYNAMIC_NODE_LIFECYCLE_FUTURE ||
            record.lifecycle() == DYNAMIC_NODE_LIFECYCLE_REGISTERING ||
            record.lifecycle() == DYNAMIC_NODE_LIFECYCLE_DOWN) &&
           record.provider() == kPowerControlProvider &&
           std::ranges::find(record.partition_names(), partition) !=
               record.partition_names().end();
  });
}

CraneExpectedRich<NodeManager::ScaleUpResult> NodeManager::RequestScaleUp(
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
        quarantined_nodes_.contains(node_id) ||
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
        ((record.power_state() ==
              crane::grpc::DYNAMIC_NODE_POWER_STATE_POWERING_ON ||
          record.power_state() ==
              crane::grpc::DYNAMIC_NODE_POWER_STATE_WAKING_UP) &&
         !PowerActionExpired(record))) {
      in_progress_candidates.emplace_back(node_id, tasks);
    } else if (record.power_state() == DYNAMIC_NODE_POWER_STATE_OFF ||
               record.power_state() ==
                   crane::grpc::DYNAMIC_NODE_POWER_STATE_POWERING_ON) {
      power_on_candidates.emplace_back(node_id, tasks);
    } else if (record.power_state() ==
                   crane::grpc::DYNAMIC_NODE_POWER_STATE_SLEEPING ||
               record.power_state() ==
                   crane::grpc::DYNAMIC_NODE_POWER_STATE_WAKING_UP) {
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
      result.nodes_to_wake.emplace_back(
          selection.node_id, records_.at(selection.node_id).generation());
      result.reserved_nodes.emplace_back(selection.node_id);
      break;
    case SelectionKind::kPowerOn:
      result.nodes_to_power_on.emplace_back(
          selection.node_id, records_.at(selection.node_id).generation());
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
  for (const auto& target : result.nodes_to_wake) {
    DynamicNodeRecord record = records_.at(target.node_id);
    SetRecordPowerState(&record,
                        crane::grpc::DYNAMIC_NODE_POWER_STATE_WAKING_UP);
    record.set_revision(++catalog_revision_);
    updated_records.emplace_back(std::move(record));
  }
  for (const auto& target : result.nodes_to_power_on) {
    DynamicNodeRecord record = records_.at(target.node_id);
    SetRecordPowerState(&record,
                        crane::grpc::DYNAMIC_NODE_POWER_STATE_POWERING_ON);
    record.set_revision(++catalog_revision_);
    updated_records.emplace_back(std::move(record));
  }
  if (!g_embedded_db_client->StoreDynamicNodeRecords(updated_records))
    return std::unexpected(
        FormatRichErr(crane::grpc::ERR_SYSTEM_ERR,
                      "Failed to persist dynamic node scale-up state"));

  for (const auto& record : updated_records) {
    records_[record.node_name()] = record;
    g_meta_container->UpdateDynamicNodeMetadata(record);
  }
  return result;
}

CraneExpectedRich<void> NodeManager::ValidateRegistrationNoLock_(
    const CranedId& node_id, uint64_t generation) const {
  if (g_config.Nodes.contains(node_id)) {
    if (generation != 0)
      return std::unexpected(FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                                           "Static node generation must be 0"));
    return {};
  }
  if (!g_config.CtldConf.DynamicNodes.Enabled)
    return std::unexpected(
        FormatRichErr(crane::grpc::ERR_GENERIC_FAILURE,
                      "Dynamic node registration is disabled"));

  auto it = records_.find(node_id);
  if (it == records_.end() || !IsDynamicRecordPresent(it->second))
    return std::unexpected(FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                                         "Dynamic node is not available"));
  if (quarantined_nodes_.contains(node_id))
    return std::unexpected(FormatRichErr(
        crane::grpc::ERR_INVALID_PARAM,
        "Dynamic node references a partition that no longer exists"));
  if (it->second.generation() != generation)
    return std::unexpected(FormatRichErr(crane::grpc::ERR_NODE_STALE_GENERATION,
                                         "Stale generation {}, expected {}",
                                         generation, it->second.generation()));
  return {};
}

CraneExpectedRich<void> NodeManager::ValidatePresentRecord_(
    const DynamicNodeRecord& record) const {
  auto name_result = ValidateNodeName(record.node_name());
  if (!name_result) return name_result;
  const auto& spec = EffectiveSpec(record);
  if (!record.has_spec() || spec.cpu_count() == 0 || spec.memory_bytes() == 0 ||
      spec.sockets() == 0 || spec.sockets() > spec.cpu_count())
    return std::unexpected(FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                                         "Invalid CPU, memory or sockets"));
  if (record.generation() == 0)
    return std::unexpected(
        FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                      "Dynamic node generation must be positive"));
  if (record.origin() != DYNAMIC_NODE_ORIGIN_DYNAMIC_ADMIN &&
      record.origin() != DYNAMIC_NODE_ORIGIN_DYNAMIC_REGISTERED)
    return std::unexpected(FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                                         "Invalid dynamic node origin"));
  if (record.lifecycle() != DYNAMIC_NODE_LIFECYCLE_FUTURE &&
      record.lifecycle() != DYNAMIC_NODE_LIFECYCLE_REGISTERING &&
      record.lifecycle() != DYNAMIC_NODE_LIFECYCLE_ACTIVE &&
      record.lifecycle() != DYNAMIC_NODE_LIFECYCLE_DOWN)
    return std::unexpected(FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                                         "Invalid dynamic node lifecycle"));
  if (record.power_state() != DYNAMIC_NODE_POWER_STATE_ON &&
      record.power_state() != DYNAMIC_NODE_POWER_STATE_OFF &&
      record.power_state() !=
          crane::grpc::DYNAMIC_NODE_POWER_STATE_POWERING_ON &&
      record.power_state() !=
          crane::grpc::DYNAMIC_NODE_POWER_STATE_POWERING_OFF &&
      record.power_state() != crane::grpc::DYNAMIC_NODE_POWER_STATE_SLEEPING &&
      record.power_state() != crane::grpc::DYNAMIC_NODE_POWER_STATE_WAKING_UP &&
      record.power_state() != crane::grpc::DYNAMIC_NODE_POWER_STATE_TO_SLEEPING)
    return std::unexpected(FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                                         "Invalid dynamic node power state"));
  if (record.partition_names().empty())
    return std::unexpected(FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                                         "No partition specified"));
  if (HasDuplicates(record.partition_names()))
    return std::unexpected(
        FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                      "Duplicate partitions are not allowed"));
  for (const auto& partition_id : record.partition_names()) {
    if (!g_config.Partitions.contains(partition_id))
      return std::unexpected(FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                                           "Partition {} does not exist",
                                           partition_id));
  }
  if (HasDuplicates(spec.features()))
    return std::unexpected(FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                                         "Duplicate features are not allowed"));
  if (std::ranges::any_of(spec.features(),
                          [](const auto& feature) { return feature.empty(); }))
    return std::unexpected(FormatRichErr(crane::grpc::ERR_INVALID_PARAM,
                                         "Empty features are not allowed"));
  return ValidateGresCounts(spec.gres());
}

}  // namespace Ctld

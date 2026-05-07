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

#include "CtldPreCompiledHeader.h"

namespace Ctld {

enum class QosFlags { DenyOnLimit, _Count };

struct Qos {
  bool deleted = false;
  std::string name;
  std::string description;
  uint32_t reference_count = 0;
  uint32_t priority;
  uint32_t max_jobs_per_user;
  uint32_t max_jobs_per_account;
  uint32_t max_running_jobs_per_user;
  absl::Duration max_time_limit_per_job;
  cpu_t max_cpus_per_user;
  uint32_t max_submit_jobs_per_user;
  uint32_t max_submit_jobs_per_account;
  uint32_t max_jobs;
  uint32_t max_submit_jobs;
  absl::Duration max_wall;
  ResourceView max_tres;
  ResourceView max_tres_per_user;
  ResourceView max_tres_per_account;
  FlagSet<QosFlags> flags;

  static constexpr const char* FieldStringOfDeleted() { return "deleted"; }
  static constexpr const char* FieldStringOfName() { return "name"; }
  static constexpr const char* FieldStringOfDescription() {
    return "description";
  }
  static constexpr const char* FieldStringOfReferenceCount() {
    return "reference_count";
  }
  static constexpr const char* FieldStringOfPriority() { return "priority"; }
  static constexpr const char* FieldStringOfMaxJobsPerUser() {
    return "max_jobs_per_user";
  }
  static constexpr const char* FieldStringOfMaxJobsPerAccount() {
    return "max_jobs_per_account";
  }
  static constexpr const char* FieldStringOfMaxTimeLimitPerJob() {
    return "max_time_limit_per_job";
  }
  static constexpr const char* FieldStringOfMaxCpusPerUser() {
    return "max_cpus_per_user";
  }
  static constexpr const char* FieldStringOfMaxSubmitJobsPerUser() {
    return "max_submit_jobs_per_user";
  }
  static constexpr const char* FieldStringOfMaxSubmitJobsPerAccount() {
    return "max_submit_jobs_per_account";
  }
  static constexpr const char* FieldStringOfMaxTresPerUser() {
    return "max_tres_per_user";
  }
  static constexpr const char* FieldStringOfMaxTresPerAccount() {
    return "max_tres_per_account";
  }
  static constexpr const char* FieldStringOfMaxTres() { return "max_tres"; }
  static constexpr const char* FieldStringOfMaxJobs() { return "max_jobs"; }
  static constexpr const char* FieldStringOfMaxSubmitJobs() {
    return "max_submit_jobs";
  }
  static constexpr const char* FieldStringOfMaxWall() { return "max_wall"; }
  static constexpr const char* FieldStringOfFlags() { return "flags"; }

  std::string QosToString() const {
    return fmt::format(
        "name: {}, description: {}, reference_count: {}, priority: {}, "
        "max_jobs_per_user: {}, max_running_jobs_per_user: {}, "
        "max_time_limit_per_job: {}, max_cpus_per_user: {}, "
        "max_jobs_per_account: {}, "
        "max_submit_jobs_per_user: {}, max_submit_jobs_per_account: {}, "
        "max_jobs: {}, max_submit_jobs: {}, max_wall: {}, flags: {}, max_tres: "
        "{}, max_tres_per_user: {}, max_tres_per_account: {}",
        name, description, reference_count, priority, max_jobs_per_user,
        max_running_jobs_per_user, absl::FormatDuration(max_time_limit_per_job),
        max_cpus_per_user, max_jobs_per_account, max_submit_jobs_per_user,
        max_submit_jobs_per_account, max_jobs, max_submit_jobs,
        absl::FormatDuration(max_wall), flags.ToString(),
        util::ReadableResourceView(max_tres),
        util::ReadableResourceView(max_tres_per_user),
        util::ReadableResourceView(max_tres_per_account));
  }

  static const std::string GetModifyFieldStr(
      const crane::grpc::ModifyField& modify_field) {
    switch (modify_field) {
    case crane::grpc::ModifyField::Description:
      return "description";
    case crane::grpc::ModifyField::Priority:
      return "priority";
    case crane::grpc::ModifyField::MaxJobsPerUser:
      return "max_jobs_per_user";
    case crane::grpc::ModifyField::MaxCpusPerUser:
      return "max_cpus_per_user";
    case crane::grpc::ModifyField::MaxTimeLimitPerJob:
      return "max_time_limit_per_job";
    case crane::grpc::ModifyField::MaxJobsPerAccount:
      return "max_jobs_per_account";
    case crane::grpc::ModifyField::MaxSubmitJobsPerUser:
      return "max_submit_jobs_per_user";
    case crane::grpc::ModifyField::MaxSubmitJobsPerAccount:
      return "max_submit_jobs_per_account";
    case crane::grpc::ModifyField::MaxJobs:
      return "max_jobs";
    case crane::grpc::ModifyField::MaxSubmitJobs:
      return "max_submit_jobs";
    case crane::grpc::ModifyField::MaxTres:
      return "max_tres";
    case crane::grpc::ModifyField::MaxTresPerAccount:
      return "max_tres_per_account";
    case crane::grpc::ModifyField::MaxTresPerUser:
      return "max_tres_per_user";
    case crane::grpc::ModifyField::MaxWall:
      return "max_wall";
    case crane::grpc::ModifyField::Flags:
      return "flags";
    default:
      std::unreachable();
    }
  }

  bool operator<(Qos const& other) const { return name < other.name; }
};

struct Account {
  bool deleted = false;
  bool blocked = false;
  std::string name;
  std::string description;
  std::list<std::string> users;
  std::list<std::string> child_accounts;
  std::string parent_account;
  std::list<std::string> allowed_partition;
  std::string default_qos;
  std::list<std::string> allowed_qos_list;
  std::list<std::string> coordinators;

  std::string AccountToString() const {
    return fmt::format(
        "name: {}, description: {}, blocked: {}, parent_account: {}, "
        "default_qos: {}, users: [{}], child_accounts: [{}], "
        "allowed_partition: [{}], allowed_qos_list: [{}], coordinators: [{}]",
        name, description, blocked, parent_account, default_qos,
        fmt::join(users, ", "), fmt::join(child_accounts, ", "),
        fmt::join(allowed_partition, ", "), fmt::join(allowed_qos_list, ", "),
        fmt::join(coordinators, ", "));
  }

  bool operator<(Account const& other) const { return name < other.name; }
};

struct User {
  // Root corresponds to root user in Linux System and have any permission over
  // the whole system.
  // Admin and Operator are created for just compatability of existing systems
  // like Slurm.
  // Root, Admin and Operator actually have no difference when controlling Crane
  // system in the current stage.
  // However, Crane system follows the rule that the users with the same admin
  // level can't control each other, but users with higher level can control
  // users with lower level. Thus, Root level is created for the integrity of
  // Crane system to guarantee that there will always a superuser to control all
  // the administrator of the whole system.
  enum AdminLevel { None, Operator, Admin, Root };

  using PartToAllowedQosMap = std::unordered_map<
      std::string /*partition name*/,
      std::pair<std::string /*default qos*/,
                std::list<std::string> /*allowed qos list*/>>;

  struct AttrsInAccount {
    PartToAllowedQosMap allowed_partition_qos_map;
    bool blocked;
  };

  /* Map<account name, item> */
  using AccountToAttrsMap = std::unordered_map<std::string, AttrsInAccount>;

  bool deleted = false;
  uid_t uid;
  std::string name;
  std::string default_account;
  std::string default_wckey;
  AccountToAttrsMap account_to_attrs_map;
  std::list<std::string> coordinator_accounts;
  AdminLevel admin_level;
  std::string cert_number;

  std::string UserToString() const {
    std::string accounts_info = "{";
    for (auto it = account_to_attrs_map.begin();
         it != account_to_attrs_map.end(); ++it) {
      if (it != account_to_attrs_map.begin()) {
        accounts_info += ", ";
      }

      std::string partition_qos_info = "{";
      for (auto pit = it->second.allowed_partition_qos_map.begin();
           pit != it->second.allowed_partition_qos_map.end(); ++pit) {
        if (pit != it->second.allowed_partition_qos_map.begin()) {
          partition_qos_info += ", ";
        }
        partition_qos_info +=
            fmt::format("{}: default={}, allowed=[{}]", pit->first,
                        pit->second.first, fmt::join(pit->second.second, ", "));
      }
      partition_qos_info += "}";

      accounts_info +=
          fmt::format("{}: {{blocked: {}, partition_qos: {}}}", it->first,
                      it->second.blocked, partition_qos_info);
    }
    accounts_info += "}";

    return fmt::format(
        "uid: {}, name: {}, default_account: {}, admin_level: {}, "
        "coordinator_accounts: [{}], account_attributes: {}",
        uid, name, default_account, static_cast<int>(admin_level),
        fmt::join(coordinator_accounts, ", "), accounts_info);
  }

  static const char* AdminLevelToString(AdminLevel level) {
    switch (level) {
    case None:
      return "None";
    case Operator:
      return "Operator";
    case Admin:
      return "Admin";
    case Root:
      return "Root";
    default:
      return "Unknown";
    }
  }

  bool operator<(User const& other) const { return uid < other.uid; }
};

struct Wckey {
  bool deleted = false;
  std::string name;
  std::string user_name; /* user name */
  bool is_default = false;

  bool operator==(const Wckey& other) const noexcept {
    return name == other.name && user_name == other.user_name &&
           is_default == other.is_default && deleted == other.deleted;
  }
};

struct LicenseResourceInDb {
  std::string name;
  std::string server;
  std::string server_type;
  crane::grpc::LicenseResource::Type type;
  uint32_t allocated{0};     /* count allocated to the cluster_resources */
  uint32_t last_consumed{0}; /* number from the server saying how many it
                              * currently has consumed */
  std::unordered_map<std::string, uint32_t> /* cluster, allowed */
      cluster_resources;
  uint32_t total_resource_count{
      0};            /* count of resources managed on the server */
  uint32_t flags{0}; /* resource attribute flags */
  absl::Time last_update;
  std::string description;
};

struct License {
  LicenseId license_id;   /* license id */
  uint32_t total;         /* The total number of configured license */
  uint32_t used;          /* Number of license in use */
  uint32_t reserved;      /* currently reserved licenses */
  bool remote;            /* non-zero if remote (from database) */
  uint32_t last_consumed; /* consumed count (for remote) */
  uint32_t last_deficit;  /* last calculated deficit */
  absl::Time last_update; /* last updated timestamp (for remote) */
};

// TODO: Overload the += and -= operators?
struct QosResource {
  ResourceView resource;
  uint32_t jobs_count{};
  uint32_t submit_jobs_count{};
};

// Transaction
struct Txn {
  uint64_t creation_time;
  std::string actor;
  std::string target;
  crane::grpc::TxnAction action;
  std::string info;
};

}  // namespace Ctld

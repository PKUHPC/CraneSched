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

#include "CtldPublicDefs.h"
// Precompiled header comes first!

#include <spdlog/fmt/fmt.h>

#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/cursor.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>

#include "protos/Crane.grpc.pb.h"

namespace Ctld {

template <typename T>
struct BsonFieldTrait {
  static T get(const bsoncxx::document::element&) {
    static_assert(sizeof(T) == 0,
                  "BsonFieldTrait not implemented for this type");
  }
  static constexpr bsoncxx::type bson_type = bsoncxx::type::k_null;
};

template <>
struct BsonFieldTrait<bool> {
  static bool get(const bsoncxx::document::element& ele) {
    return ele.get_bool().value;
  }
  static constexpr bsoncxx::type bson_type = bsoncxx::type::k_bool;
};

template <>
struct BsonFieldTrait<double> {
  static double get(const bsoncxx::document::element& ele) {
    return ele.get_double().value;
  }
  static constexpr bsoncxx::type bson_type = bsoncxx::type::k_double;
};

template <>
struct BsonFieldTrait<std::string> {
  static std::string get(const bsoncxx::document::element& ele) {
    return std::string(ele.get_string().value);
  }
  static constexpr bsoncxx::type bson_type = bsoncxx::type::k_string;
};

template <>
struct BsonFieldTrait<bsoncxx::array::view> {
  static bsoncxx::array::view get(const bsoncxx::document::element& ele) {
    return ele.get_array().value;
  }
  static constexpr bsoncxx::type bson_type = bsoncxx::type::k_array;
};

template <>
struct BsonFieldTrait<bsoncxx::document::view> {
  static bsoncxx::document::view get(const bsoncxx::document::element& ele) {
    return ele.get_document().view();
  }
  static constexpr bsoncxx::type bson_type = bsoncxx::type::k_document;
};

class MongodbClient {
 private:
  using array = bsoncxx::builder::basic::array;
  using document = bsoncxx::builder::basic::document;
  using sub_array = bsoncxx::builder::basic::sub_array;
  using sub_document = bsoncxx::builder::basic::sub_document;
  struct JobSummary {
    enum class Type : std::uint8_t { HOUR, DAY, MONTH };
  };

  // Job aggregation information for acc_usage tables
  struct JobAggregationInfo {
    std::string account;
    std::string username;
    std::string qos;
    std::string wckey;
    std::chrono::sys_seconds start_time;
    std::chrono::sys_seconds end_time;
    cpu_t total_cpus;
  };

  struct MongoServerVersion {
    int major{0};
    int minor{0};
    int patch{0};
  };

 public:
  enum class EntityType {
    ACCOUNT = 0,
    USER = 1,
    QOS = 2,
    WCKEY = 3,
    RESOURCE = 4,
  };

  MongodbClient();  // Mongodb-c++ don't need to close the connection
  ~MongodbClient();
  bool Init();
  bool Connect();

  // Job summary (hour/day/month tables) relies on aggregation features.
  // - Queries use $unionWith (MongoDB >= 4.4)
  // - Aggregation uses $merge (MongoDB >= 4.2)
  // We intentionally disable the feature on older servers to avoid crashes.
  [[nodiscard]] bool JobSummaryEnabled() const {
    return m_job_summary_enabled_;
  }

  /* ----- Method of operating the job table ----------- */
  bool InsertRecoveredJob(
      crane::grpc::JobInEmbeddedDb const& job_in_embedded_db);
  bool InsertJob(JobInCtld* job);
  bool InsertJobs(const std::unordered_set<JobInCtld*>& jobs);

  bool FetchJobRecords(
      const crane::grpc::QueryJobsInfoRequest* request,
      std::unordered_map<job_id_t, crane::grpc::JobInfo>* job_info_map,
      size_t limit);

  bool FetchJobStepRecords(
      const crane::grpc::QueryJobsInfoRequest* request,
      std::unordered_map<job_id_t, crane::grpc::JobInfo>* job_info_map);

  bool CheckJobDbIdExisted(int64_t job_db_id);

  // Fetch job status (state, exit_code, time_end, time_start)
  std::unordered_map<
      job_id_t, std::tuple<crane::grpc::JobStatus, uint32_t, int64_t, int64_t>>
  FetchJobStatus(const std::unordered_set<job_id_t>& job_ids);

  /* ----- Method of operating the step table ----------- */
  bool InsertRecoveredStep(
      crane::grpc::StepInEmbeddedDb const& step_in_embedded_db);
  bool InsertSteps(const std::unordered_set<StepInCtld*>& steps);
  bool CheckStepExisted(job_id_t job_id, step_id_t step_id);

  bool QueryJobSizeSummary(
      const crane::grpc::QueryJobSizeSummaryRequest* request,
      grpc::ServerWriter<::crane::grpc::QueryJobSizeSummaryReply>* stream);
  grpc::Status QueryJobSummary(
      const crane::grpc::QueryJobSummaryRequest* request,
      grpc::ServerWriter<::crane::grpc::QueryJobSummaryReply>* stream);

  // Real-time aggregation: append job to acc_usage tables
  void AppendToAccUsageTable(const bsoncxx::document::view& job_doc,
                             mongocxx::client_session* session = nullptr);
  void AppendToAccUsageTable(const JobInCtld* job,
                             mongocxx::client_session* session = nullptr);

  // Mark job as aggregated in job_table
  void MarkJobAsAggregated(job_id_t job_id,
                           mongocxx::client_session* session = nullptr);

  // Recovery functions for startup
  void RecoverMissingAggregations_();

 private:
  [[nodiscard]] bool MongoVersionAtLeast_(int major, int minor) const {
    if (m_mongo_server_version_.major != major)
      return m_mongo_server_version_.major > major;
    return m_mongo_server_version_.minor >= minor;
  }

  bool UpdateJobSummaryLastSuccessTime_(JobSummary::Type summary_type,
                                        std::chrono::sys_seconds last_success);
  static std::string JobSummaryTypeToString_(JobSummary::Type summary_type);
  std::optional<std::chrono::sys_seconds> GetJobSummaryLastSuccessTime_(
      JobSummary::Type summary_type);

  // Initial aggregation completion tracking
  bool GetInitialAggregationCompleted_(JobSummary::Type type);
  void SetInitialAggregationCompleted_(JobSummary::Type type, bool completed);

  std::chrono::sys_seconds GetJobMinStartTime_();

  // Execute aggregation for a single hour (worker function for thread pool)
  // Returns: discovered min_start on success, std::nullopt on failure
  std::optional<int64_t> AggregateJobSummaryForSingleHour_(
      std::chrono::sys_seconds hour_start, std::chrono::sys_seconds hour_end,
      const std::string& job_collection_name, int64_t cached_min_start);

  // (For new cluster initialization only)
  bool AggregateJobSummaryByHour_(std::chrono::sys_seconds start_sec,
                                  std::chrono::sys_seconds end_sec,
                                  const std::string& job_collection_name);

  // (For new cluster initialization only)
  bool AggregateJobSummaryByDayOrMonth_(
      JobSummary::Type src_type, JobSummary::Type dst_type,
      std::chrono::sys_seconds period_start_tp,
      std::chrono::sys_seconds period_end_tp);

  // New acc_usage aggregation helpers - each handles one granularity
  void AppendToHourTable_(const JobAggregationInfo& info,
                          mongocxx::client_session* session = nullptr);
  void AppendToDayTable_(const JobAggregationInfo& info,
                         mongocxx::client_session* session = nullptr);
  void AppendToMonthTable_(const JobAggregationInfo& info,
                           mongocxx::client_session* session = nullptr);

  // Recovery helper functions
  void RecoverNewClusterAggregations_(bool hour_done, bool day_done,
                                      bool month_done);
  void RecoverExistingClusterAggregations_();

  // Database schema migration
  static constexpr int kCurrentDbSchemaVersion = 1;
  static constexpr const char* kV0CollectionName = "task_table";
  bool CheckAndMigrateDbSchema_();
  bool RecoverInterruptedMigration_();
  std::optional<int> GetDbSchemaVersion_();
  bool SetDbSchemaVersion_(int version);
  bool CopyJobTableForMigration_(const std::string& source_collection);
  bool SwapMigratedJobTable_(const std::string& source_collection,
                             int from_version, int to_version);
  void CleanupMigrationTemp_();
  bool MigrateV0ToV1_();

  /* ----- Method of operating the account table ----------- */
 public:
  bool InsertUser(const User& new_user);
  bool InsertWckey(const Wckey& new_wckey);
  bool InsertAccount(const Account& new_account);
  bool InsertQos(const Qos& new_qos);

  bool DeleteEntity(EntityType type, const std::string& name);

  template <typename T>
  bool SelectUser(const std::string& key, const T& value, User* user);
  template <typename T>
  bool SelectAccount(const std::string& key, const T& value, Account* account) {
    using bsoncxx::builder::basic::kvp;
    document filter;
    filter.append(kvp(key, value));
    bsoncxx::stdx::optional<bsoncxx::document::value> result =
        (*GetClient_())[m_db_name_][m_account_collection_name_].find_one(
            filter.view());

    if (result) {
      bsoncxx::document::view account_view = result->view();
      ViewToAccount_(account_view, account);
      return true;
    }
    return false;
  };
  template <typename T>
  bool SelectQos(const std::string& key, const T& value, Qos* qos) {
    using bsoncxx::builder::basic::kvp;
    document filter;
    filter.append(kvp(key, value));
    bsoncxx::stdx::optional<bsoncxx::document::value> result =
        (*GetClient_())[m_db_name_][m_qos_collection_name_].find_one(
            filter.view());

    if (result) {
      bsoncxx::document::view qos_view = result->view();
      ViewToQos_(qos_view, qos);
      return true;
    }
    return false;
  };

  void SelectAllUser(std::list<User>* user_list);
  void SelectAllWckey(std::list<Ctld::Wckey>* wckey_list);
  void SelectAllAccount(std::list<Account>* account_list);
  void SelectAllQos(std::list<Qos>* qos_list);

  template <typename T>
  void SubDocumentAppendItem_(sub_document& doc, const std::string& key,
                              const T& value) {
    using bsoncxx::builder::basic::kvp;
    doc.append(kvp(key, value));
  };

  template <std::ranges::range T>
  void SubDocumentAppendItem_(sub_document& doc, const std::string& key,
                              const T& value)
    requires std::same_as<std::ranges::range_value_t<T>, std::string>
  {
    using bsoncxx::builder::basic::kvp;
    doc.append(kvp(key, [&value](sub_array array) {
      for (const std::string& v : value) array.append(v);
    }));
  }
  void SubDocumentAppendItem_(sub_document& doc, const std::string& key,
                              const User::PartToAllowedQosMap& value);

  template <typename T>
  bool UpdateEntityOne(EntityType type, const std::string& opt,
                       const std::string& name, const std::string& key,
                       const T& value) {
    using bsoncxx::builder::basic::kvp;

    std::string coll_name;
    document filter, updateItem;

    filter.append(kvp("name", name));
    updateItem.append(
        kvp(opt,
            [&](sub_document subDocument) {
              SubDocumentAppendItem_(subDocument, key, value);
            }),
        kvp("$set", [](sub_document subDocument) {
          subDocument.append(kvp("mod_time", ToUnixSeconds(absl::Now())));
        }));

    switch (type) {
    case EntityType::ACCOUNT:
      coll_name = m_account_collection_name_;
      break;
    case EntityType::USER:
      coll_name = m_user_collection_name_;
      break;
    case EntityType::QOS:
      coll_name = m_qos_collection_name_;
      break;
    case EntityType::WCKEY:
      CRANE_ERROR(
          "UpdateEntityOne does not support WCKEY. Use UpdateEntityOneByFields "
          "instead.");
      return false;
    case EntityType::RESOURCE:
      CRANE_ERROR("UpdateEntityOne does not support RESOURCE.");
      return false;
    }

    bsoncxx::stdx::optional<mongocxx::result::update> result =
        (*GetClient_())[m_db_name_][coll_name].update_one(
            *GetSession_(), filter.view(), updateItem.view());

    if (!result || !result->modified_count()) {
      CRANE_ERROR(
          "Update date in database fail(name:{},opt:{},key:{},value:{})", name,
          opt, key, value);
      return false;
    }
    return true;
  }

  using FilterValue = std::variant<std::string, int32_t, int64_t, double, bool>;
  using FilterFields = std::vector<std::pair<std::string, FilterValue>>;
  template <typename T>
  bool UpdateEntityOneByFields(EntityType type, const std::string& opt,
                               const FilterFields& filter_fields,
                               const std::string& key, const T& value) {
    using bsoncxx::builder::basic::kvp;
    std::string coll_name;
    document filter, updateItem;

    for (const auto& [fname, fvalue] : filter_fields) {
      std::visit([&](auto&& arg) { filter.append(kvp(fname, arg)); }, fvalue);
    }

    updateItem.append(
        kvp(opt,
            [&](sub_document subDocument) {
              SubDocumentAppendItem_(subDocument, key, value);
            }),
        kvp("$set", [](sub_document subDocument) {
          subDocument.append(kvp("mod_time", ToUnixSeconds(absl::Now())));
        }));

    switch (type) {
    case EntityType::ACCOUNT:
      coll_name = m_account_collection_name_;
      break;
    case EntityType::USER:
      coll_name = m_user_collection_name_;
      break;
    case EntityType::QOS:
      coll_name = m_qos_collection_name_;
      break;
    case EntityType::WCKEY:
      coll_name = m_wckey_collection_name_;
      break;
    case EntityType::RESOURCE:
      CRANE_ERROR("UpdateEntityOneByFields does not support RESOURCE.");
      return false;
    }

    auto count =
        (*GetClient_())[m_db_name_][coll_name].count_documents(filter.view());
    if (count != 1) return false;  // or switch to update_many if intended

    bsoncxx::stdx::optional<mongocxx::result::update> result =
        (*GetClient_())[m_db_name_][coll_name].update_one(
            *GetSession_(), filter.view(), updateItem.view());

    if (!result || !result->modified_count()) {
      std::string filter_str = "{";
      for (size_t i = 0; i < filter_fields.size(); ++i) {
        filter_str += std::visit(
            [&](auto&& arg) -> std::string {
              if constexpr (std::is_same_v<std::decay_t<decltype(arg)>, bool>) {
                return std::format("{}:{}", filter_fields[i].first,
                                   arg ? "true" : "false");
              } else {
                return std::format("{}:{}", filter_fields[i].first, arg);
              }
            },
            filter_fields[i].second);
        if (i != filter_fields.size() - 1) filter_str += ", ";
      }
      filter_str += "}";

      CRANE_ERROR(
          "Update date in database fail(filter:{}, opt:{}, key:{}, value:{}, "
          "{})",
          filter_str, opt, key, value, result ? result->modified_count() : -1);
      return false;
    }
    return true;
  }

  bool UpdateUser(const User& user);
  bool UpdateWckey(const Wckey& wckey);
  bool UpdateAccount(const Account& account);
  bool UpdateQos(const Qos& qos);

  bool InsertTxn(const Txn& txn);
  void SelectTxns(
      const std::unordered_map<std::string, std::string>& conditions,
      int64_t start_time, int64_t end_time, std::list<Txn>* res_txn);

  bool InsertLicenseResource(const LicenseResourceInDb& resource);
  bool UpdateLicenseResource(const LicenseResourceInDb& resource);
  bool DeleteLicenseResource(const std::string& resource_name,
                             const std::string& server);
  void SelectAllLicenseResource(std::list<LicenseResourceInDb>* resource_list);

  bool CommitTransaction(
      const mongocxx::client_session::with_transaction_cb& callback);

  void CreateCollectionIndex(mongocxx::collection& coll,
                             const std::vector<std::string>& fields,
                             bool unique);
  bool InitTableIndexes();

 private:
  bool CheckDefaultRootAccountUserAndInit_();

  template <typename V>
    requires requires(bsoncxx::builder::core core, std::remove_cvref_t<V> t) {
      core.append(t);
    }
  void DocumentAppendItem_(document& doc, const std::string& key,
                           const V& value) {
    using bsoncxx::builder::basic::kvp;
    doc.append(kvp(key, value));
  };

  template <typename V>
    requires std::ranges::input_range<std::remove_cvref_t<V>> &&
             (!std::convertible_to<std::remove_cvref_t<V>, std::string> &&
              !std::convertible_to<std::remove_cvref_t<V>, std::string_view> &&
              !std::is_convertible_v<std::remove_cvref_t<V>, const char*>)
  void DocumentAppendItem_(document& doc, const std::string& key,
                           const V& value) {
    using bsoncxx::builder::basic::kvp;
    using T = std::remove_cvref_t<V>;
    constexpr bool not_string = !std::convertible_to<T, std::string> &&
                                !std::convertible_to<T, std::string_view> &&
                                !std::is_convertible_v<T, const char*>;
    constexpr bool is_map = requires {
      typename T::key_type;
      typename T::mapped_type;
    };
    if constexpr (is_map) {
      doc.append(kvp(key, [&](sub_document sub_doc) {
        for (const auto& [k, v] : value) {
          SubDocumentAppendItem_(sub_doc, k, v);
        }
      }));
    } else if constexpr (not_string) {
      doc.append(kvp(key, [&](sub_array array) {
        for (const auto& v : value) {
          array.append(v);
        }
      }));

    } else {
      static_assert(
          false,
          "DocumentAppendItem_ not implemented for this range-like type");
    }
  };

  void DocumentAppendItem_(
      document& doc, const std::string& key,
      const std::unordered_map<std::string, User::AttrsInAccount>& value);

  void DocumentAppendItem_(document& doc, const std::string& key,
                           const GresMap& value);
  void DocumentAppendItem_(document& doc, const std::string& key,
                           const std::vector<gid_t>& value);
  void DocumentAppendItem_(document& doc, const std::string& key,
                           const DedicatedResourceInNode& value);
  void DocumentAppendItem_(document& doc, const std::string& key,
                           const ResourceInNodeV3& value);
  void DocumentAppendItem_(document& doc, const std::string& key,
                           const ResourceV3& value);
  void DocumentAppendItem_(document& doc, const std::string& key,
                           const std::optional<ContainerMetaInJob>& value);

  void DocumentAppendItem_(document& doc, const std::string& key,
                           const std::optional<PodMetaInJob>& value);

  void DocumentAppendItem_(
      document& doc, const std::string& key,
      const std::unordered_map<std::string, uint32_t>& value);

  void DocumentAppendItem_(document& doc, const std::string& key,
                           const ResourceView& value);

  void SubDocumentAppendItem_(sub_document& doc, const std::string& key,
                              const GresMap& value);

  template <typename... Ts, std::size_t... Is>
  document documentConstructor_(
      const std::array<std::string, sizeof...(Ts)>& fields,
      const std::tuple<Ts...>& values, std::index_sequence<Is...>);

  template <typename... Ts>
  document DocumentConstructor_(
      const std::array<std::string, sizeof...(Ts)>& fields,
      const std::tuple<Ts...>& values);
  template <typename T>
  T ViewValueOr_(const bsoncxx::document::element& view_value,
                 const T& default_value) {
    if (!view_value) {
      return default_value;
    }
    if (view_value.type() == BsonFieldTrait<T>::bson_type)
      return BsonFieldTrait<T>::get(view_value);

    return default_value;
  }
  template <typename T>
    requires std::is_arithmetic_v<T>
  T ViewGetArithmeticValue_(const bsoncxx::document::element& view_value) {
    switch (view_value.type()) {
    case bsoncxx::type::k_bool:
      return static_cast<T>(view_value.get_bool().value);
    case bsoncxx::type::k_int32:
      return static_cast<T>(view_value.get_int32().value);
    case bsoncxx::type::k_int64:
      return static_cast<T>(view_value.get_int64().value);
    case bsoncxx::type::k_double:
      return static_cast<T>(view_value.get_double().value);
    default:
      throw std::runtime_error(
          "Non-arithmetic type in ViewGetArithmeticValue_");
    }
  }

  template <typename T>
    requires std::is_arithmetic_v<T>
  T ViewValueOr_(const bsoncxx::document::element& view_value,
                 const T& default_value) {
    if (!view_value) {
      return default_value;
    }
    switch (view_value.type()) {
    case bsoncxx::type::k_bool:
      return static_cast<T>(view_value.get_bool().value);
    case bsoncxx::type::k_int32:
      return static_cast<T>(view_value.get_int32().value);
    case bsoncxx::type::k_int64:
      return static_cast<T>(view_value.get_int64().value);
    case bsoncxx::type::k_double:
      return static_cast<T>(view_value.get_double().value);
    default:
      return default_value;
    }
  }

  bool ViewValueOr_(const bsoncxx::document::element& view_value,
                    const bool& default_value) {
    if (!view_value) {
      return default_value;
    }
    if (view_value.type() == bsoncxx::type::k_bool)
      return view_value.get_bool().value;
    else
      return default_value;
  }

  mongocxx::client* GetClient_();
  mongocxx::client_session* GetSession_();

  void ViewToUser_(const bsoncxx::document::view& user_view, User* user);

  document UserToDocument_(const User& user);

  bsoncxx::builder::basic::document WckeyToDocument_(const Ctld::Wckey& wckey);

  void ViewToWckey_(const bsoncxx::document::view& wckey_view,
                    Ctld::Wckey* wckey);

  void ViewToAccount_(const bsoncxx::document::view& account_view,
                      Account* account);

  document AccountToDocument_(const Account& account);

  void ViewToQos_(const bsoncxx::document::view& qos_view, Qos* qos);

  document QosToDocument_(const Qos& qos);

  void ViewToTxn_(const bsoncxx::document::view& txn_view, Txn* txn);
  document TxnToDocument_(const Txn& txn);

  void ViewToLicenseResource_(const bsoncxx::document::view& resource_view,
                              LicenseResourceInDb* resource);
  document LicenseResourceToDocument_(const LicenseResourceInDb& resource);

  document JobInCtldToDocument_(JobInCtld* job);
  document JobInEmbeddedDbToDocument_(
      crane::grpc::JobInEmbeddedDb const& job_in_db);

  document StepInCtldToDocument_(StepInCtld* step);
  document StepInEmbeddedDbToDocument_(
      crane::grpc::StepInEmbeddedDb const& step);
  void ViewToStepInfo_(const bsoncxx::document::view& view,
                       crane::grpc::StepInfo* step_info);

  GresMap BsonToGresMap(const bsoncxx::document::view& doc);
  DedicatedResourceInNode BsonToDedicatedResourceInNode(
      const bsoncxx::document::view& doc);
  ResourceInNodeV3 BsonToResourceInNodeV3(const bsoncxx::document::view& doc);
  ResourceV3 BsonToResourceV3(const bsoncxx::document::view& doc);
  PodMetaInJob BsonToPodMeta(const bsoncxx::document::view& doc);
  ContainerMetaInJob BsonToContainerMeta(const bsoncxx::document::view& doc);

  void QosResourceViewFromDb_(const bsoncxx::document::view& qos_view,
                              const std::string& field, ResourceView* resource);

  std::string m_db_name_, m_connect_uri_;
  const std::string m_job_collection_name_{"job_table"};
  const std::string m_account_collection_name_{"acct_table"};
  const std::string m_user_collection_name_{"user_table"};
  const std::string m_qos_collection_name_{"qos_table"};
  const std::string m_txn_collection_name_{"txn_table"};
  const std::string m_wckey_collection_name_{"wckey_table"};
  const std::string m_license_resource_collection_name_{
      "license_resource_table"};

  const std::string m_migration_temp_collection_name_{"job_table_migrating"};
  const std::string m_metadata_collection_name_{"metadata_table"};
  const std::string m_summary_time_collection_name_{"summary_time_table"};
  const std::string m_acc_usage_hour_collection_name_{"acc_usage_hour_table"};
  const std::string m_acc_usage_day_collection_name_{"acc_usage_day_table"};
  const std::string m_acc_usage_month_collection_name_{"acc_usage_month_table"};
  static constexpr int MaxJobSummaryBatchSize =
      5000;  // Maximum number of items per gRPC streaming batch
  std::shared_ptr<spdlog::logger> m_logger_;

  std::unique_ptr<mongocxx::instance> m_instance_;
  std::unique_ptr<mongocxx::pool> m_connect_pool_;

  MongoServerVersion m_mongo_server_version_{};
  bool m_job_summary_enabled_{true};

  mongocxx::write_concern m_wc_majority_{};
  mongocxx::read_concern m_rc_local_{};
  mongocxx::read_preference m_rp_primary_{};
  std::atomic_bool m_thread_stop_{false};
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::MongodbClient> g_db_client;

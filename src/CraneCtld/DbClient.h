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
#include "protos/Crane.grpc.pb.h"
// Precompiled header comes first!

#include <spdlog/fmt/fmt.h>

#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/cursor.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>

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
struct BsonFieldTrait<int64_t> {
  static int64_t get(const bsoncxx::document::element& ele) {
    return ele.get_int64().value;
  }
  static constexpr bsoncxx::type bson_type = bsoncxx::type::k_int64;
};

template <>
struct BsonFieldTrait<int32_t> {
  static int32_t get(const bsoncxx::document::element& ele) {
    return ele.get_int32().value;
  }
  static constexpr bsoncxx::type bson_type = bsoncxx::type::k_int32;
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

 public:
  enum class EntityType {
    ACCOUNT = 0,
    USER = 1,
    QOS = 2,
  };

  enum class RollupType : std::uint8_t { HOUR, HOUR_TO_DAY, DAY_TO_MONTH };

  struct JobSummAggResult {
    double total_cpu_time = 0;
    int32_t total_count = 0;
  };
  struct JobSummKey {
    std::string account;
    std::string username;
    std::string qos;
    std::string wckey;

    template <typename H>
    friend H AbslHashValue(H h, const JobSummKey& key) {
      return H::combine(std::move(h), key.account, key.username, key.qos,
                        key.wckey);
    }
    bool operator==(const JobSummKey& other) const {
      return account == other.account && username == other.username &&
             qos == other.qos && wckey == other.wckey;
      ;
    }
  };

  struct JobSizeSummKey {
    std::string account;
    std::string wckey;
    uint32_t cpu_alloc;

    template <typename H>
    friend H AbslHashValue(H h, const JobSizeSummKey& key) {
      return H::combine(std::move(h), key.account, key.wckey, key.cpu_alloc);
    }
    bool operator==(const JobSizeSummKey& other) const {
      return account == other.account && wckey == other.wckey &&
             cpu_alloc == other.cpu_alloc;
      ;
    }
  };

  MongodbClient();  // Mongodb-c++ don't need to close the connection
  ~MongodbClient();
  bool Init();
  bool Connect();

  /* ----- Method of operating the job table ----------- */
  bool InsertRecoveredJob(
      crane::grpc::TaskInEmbeddedDb const& task_in_embedded_db);
  bool InsertJob(TaskInCtld* task);
  bool InsertJobs(const std::unordered_set<TaskInCtld*>& tasks);

  bool FetchJobRecords(const crane::grpc::QueryTasksInfoRequest* request,
                       crane::grpc::QueryTasksInfoReply* response,
                       size_t limit);

  bool CheckTaskDbIdExisted(int64_t task_db_id);

  /* ----- Method of operating the step table ----------- */
  bool InsertRecoveredStep(
      crane::grpc::StepInEmbeddedDb const& step_in_embedded_db);
  bool InsertSteps(const std::unordered_set<StepInCtld*>& steps);
  bool CheckStepExisted(job_id_t job_id, step_id_t step_id);

  /* ----- Method of operating the account table ----------- */
  bool InsertUser(const User& new_user);
  bool InsertAccount(const Account& new_account);
  bool InsertQos(const Qos& new_qos);

  bool DeleteEntity(EntityType type, const std::string& name);
  bool UpdateSummaryLastSuccessTimeSec(const std::string& type,
                                       int64_t last_success_sec);
  bool GetSummaryLastSuccessTimeTm(const std::string& type, std::tm& tm_last);
  std::tm GetRoundHourNowTm();
  bool NeedRollup(const std::tm& tm_last, const std::tm& tm_now,
                  RollupType type);
  bool RollupHourTable();
  bool RollupHourToDay();
  bool RollupDayToMonth();
  void ClusterRollupUsage();
  bool AggregateJobSummary(RollupType type, std::time_t start, std::time_t end);
  void QueryAndAggJobSizeSummary(
      const std::string& table, const std::string& time_field,
      std::time_t range_start, std::time_t range_end,
      const crane::grpc::QueryJobSizeSummaryItemRequest* request,
      absl::flat_hash_map<JobSizeSummKey, JobSummAggResult>& agg_map);
  void QueryAndAggJobSummary(
      const std::string& table, const std::string& time_field,
      std::time_t range_start, std::time_t range_end,
      const crane::grpc::QueryJobSummaryRequest* request,
      absl::flat_hash_map<JobSummKey, JobSummAggResult>& agg_map);
  void QueryJobSizeSummary(
      const crane::grpc::QueryJobSizeSummaryItemRequest* request,
      grpc::ServerWriter<::crane::grpc::QueryJobSizeSummaryItemReply>* stream);
  bool FetchJobSizeSummaryRecords(
      const crane::grpc::QueryJobSizeSummaryItemRequest* request,
      grpc::ServerWriter<::crane::grpc::QueryJobSizeSummaryItemReply>* stream);
  void QueryJobSummary(
      const crane::grpc::QueryJobSummaryRequest* request,
      grpc::ServerWriter<::crane::grpc::QueryJobSummaryReply>* stream);
  void HourJobSummAggregation(std::time_t start, std::time_t end,
                              const std::string& task_collection_name);
  void DayOrMonJobSummAggregation(const std::string& src_coll_str,
                                  const std::string& dst_coll_str,
                                  const std::string& src_time_field,
                                  const std::string& period_field,
                                  std::time_t period_start,
                                  std::time_t period_end);
  void MongoDbSumaryTh_(const std::shared_ptr<uvw::loop>& uvw_loop);
  uint64_t MillisecondsToNextHour();

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

  bool UpdateUser(const User& user);
  bool UpdateAccount(const Account& account);
  bool UpdateQos(const Qos& qos);

  bool InsertTxn(const Txn& txn);
  void SelectTxns(
      const std::unordered_map<std::string, std::string>& conditions,
      int64_t start_time, int64_t end_time, std::list<Txn>* res_txn);

  bool CommitTransaction(
      const mongocxx::client_session::with_transaction_cb& callback);

 private:
  bool CheckDefaultRootAccountUserAndInit_();

  template <typename V>
  void DocumentAppendItem_(document& doc, const std::string& key,
                           const V& value);

  template <typename... Ts, std::size_t... Is>
  document documentConstructor_(
      const std::array<std::string, sizeof...(Ts)>& fields,
      const std::tuple<Ts...>& values, std::index_sequence<Is...>);

  template <typename... Ts>
  document DocumentConstructor_(
      const std::array<std::string, sizeof...(Ts)>& fields,
      const std::tuple<Ts...>& values);
  template <typename ViewValue, typename T>
  T ViewValueOr_(const ViewValue& view_value, const T& default_value);

  mongocxx::client* GetClient_();
  mongocxx::client_session* GetSession_();

  void ViewToUser_(const bsoncxx::document::view& user_view, User* user);

  document UserToDocument_(const User& user);

  void ViewToAccount_(const bsoncxx::document::view& account_view,
                      Account* account);

  document AccountToDocument_(const Account& account);

  void ViewToQos_(const bsoncxx::document::view& qos_view, Qos* qos);

  document QosToDocument_(const Qos& qos);

  void ViewToTxn_(const bsoncxx::document::view& txn_view, Txn* txn);
  document TxnToDocument_(const Txn& txn);

  document TaskInCtldToDocument_(TaskInCtld* task);
  document TaskInEmbeddedDbToDocument_(
      crane::grpc::TaskInEmbeddedDb const& task);

  document StepInCtldToDocument_(StepInCtld* step);
  document StepInEmbeddedDbToDocument_(
      crane::grpc::StepInEmbeddedDb const& step);

  DeviceMap BsonToDeviceMap(const bsoncxx::document::view& doc);
  ContainerMetaInTask BsonToContainerMeta(const bsoncxx::document::view& doc);

  std::string m_db_name_, m_connect_uri_;
  const std::string m_task_collection_name_{"task_table"};
  const std::string m_account_collection_name_{"acct_table"};
  const std::string m_user_collection_name_{"user_table"};
  const std::string m_qos_collection_name_{"qos_table"};
  const std::string m_txn_collection_name_{"txn_table"};
  const std::string m_hour_job_summary_collection_name_{
      "hour_job_summary_table"};
  const std::string m_day_job_summary_collection_name_{"day_job_summary_table"};
  const std::string m_month_job_summary_collection_name_{
      "month_job_summary_table"};
  const std::string m_summary_time_collection_name_{"summary_time_table"};
  std::shared_ptr<spdlog::logger> m_logger_;

  std::unique_ptr<mongocxx::instance> m_instance_;
  std::unique_ptr<mongocxx::pool> m_connect_pool_;

  mongocxx::write_concern m_wc_majority_{};
  mongocxx::read_concern m_rc_local_{};
  mongocxx::read_preference m_rp_primary_{};
  std::mutex rollup_mutex_;
  std::thread m_mongodb_sum_thread_;

 private:
  std::atomic_bool m_thread_stop_{};
};

template <>
void MongodbClient::DocumentAppendItem_<std::list<std::string>>(
    document& doc, const std::string& key, const std::list<std::string>& value);

template <>
void MongodbClient::DocumentAppendItem_<User::AccountToAttrsMap>(
    document& doc, const std::string& key,
    const std::unordered_map<std::string, User::AttrsInAccount>& value);

template <>
void MongodbClient::SubDocumentAppendItem_<User::PartToAllowedQosMap>(
    sub_document& doc, const std::string& key,
    const User::PartToAllowedQosMap& value);

template <>
void MongodbClient::DocumentAppendItem_<DeviceMap>(document& doc,
                                                   const std::string& key,
                                                   const DeviceMap& value);

template <>
void MongodbClient::DocumentAppendItem_<
    std::unordered_map<std::string, uint32_t>>(
    document& doc, const std::string& key,
    const std::unordered_map<std::string, uint32_t>& value);

template <>
void MongodbClient::DocumentAppendItem_<std::optional<ContainerMetaInTask>>(
    document& doc, const std::string& key,
    const std::optional<ContainerMetaInTask>& value);

}  // namespace Ctld

inline std::unique_ptr<Ctld::MongodbClient> g_db_client;
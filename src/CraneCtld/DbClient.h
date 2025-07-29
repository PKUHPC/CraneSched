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
    WCKEY = 3,
  };

  MongodbClient();  // Mongodb-c++ don't need to close the connection
  bool Init();
  bool Connect();

  /* ----- Method of operating the job table ----------- */
  bool InsertRecoveredJob(
      crane::grpc::TaskInEmbeddedDb const& task_in_embedded_db);
  bool InsertJob(TaskInCtld* task);
  bool InsertJobs(const std::unordered_set<TaskInCtld*>& tasks);

  bool FetchJobRecords(
      const crane::grpc::QueryTasksInfoRequest* request,
      std::unordered_map<job_id_t, crane::grpc::TaskInfo>* job_info_map,
      size_t limit);

  bool FetchJobStepRecords(
      const crane::grpc::QueryTasksInfoRequest* request,
      std::unordered_map<job_id_t, crane::grpc::TaskInfo>* job_info_map);

  bool CheckTaskDbIdExisted(int64_t task_db_id);

  /* ----- Method of operating the step table ----------- */
  bool InsertRecoveredStep(
      crane::grpc::StepInEmbeddedDb const& step_in_embedded_db);
  bool InsertSteps(const std::unordered_set<StepInCtld*>& steps);
  bool CheckStepExisted(job_id_t job_id, step_id_t step_id);

  /* ----- Method of operating the account table ----------- */
  bool InsertUser(const User& new_user);
  bool InsertWckey(const Ctld::Wckey& new_wckey);
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

    if (opt == "$set") {
      updateItem.append(kvp("$set", [&](sub_document subDocument) {
        SubDocumentAppendItem_(subDocument, key, value);
        subDocument.append(kvp("mod_time", ToUnixSeconds(absl::Now())));
      }));
    } else {
      updateItem.append(
          kvp(opt,
              [&](sub_document subDocument) {
                SubDocumentAppendItem_(subDocument, key, value);
              }),
          kvp("$set", [](sub_document subDocument) {
            subDocument.append(kvp("mod_time", ToUnixSeconds(absl::Now())));
          }));
    }

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
    }

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

  bool CommitTransaction(
      const mongocxx::client_session::with_transaction_cb& callback);
  void CreateCollectionIndex(mongocxx::collection& coll,
                             const std::vector<std::string>& fields);
  bool InitTableIndexes();

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

  document TaskInCtldToDocument_(TaskInCtld* task);
  document TaskInEmbeddedDbToDocument_(
      crane::grpc::TaskInEmbeddedDb const& task);

  document StepInCtldToDocument_(StepInCtld* step);
  document StepInEmbeddedDbToDocument_(
      crane::grpc::StepInEmbeddedDb const& step);
  void ViewToStepInfo_(const bsoncxx::document::view& view,
                       crane::grpc::StepInfo* step_info);

  DeviceMap BsonToDeviceMap(const bsoncxx::document::view& doc);
  DedicatedResourceInNode BsonToDedicatedResourceInNode(
      const bsoncxx::document::view& doc);
  ResourceInNode BsonToResourceInNode(const bsoncxx::document::view& doc);
  ResourceV2 BsonToResourceV2(const bsoncxx::document::view& doc);
  ContainerMetaInTask BsonToContainerMeta(const bsoncxx::document::view& doc);

  std::string m_db_name_, m_connect_uri_;
  const std::string m_task_collection_name_{"task_table"};
  const std::string m_account_collection_name_{"acct_table"};
  const std::string m_user_collection_name_{"user_table"};
  const std::string m_qos_collection_name_{"qos_table"};
  const std::string m_txn_collection_name_{"txn_table"};
  const std::string m_wckey_collection_name_{"wckey_table"};
  std::shared_ptr<spdlog::logger> m_logger_;

  std::unique_ptr<mongocxx::instance> m_instance_;
  std::unique_ptr<mongocxx::pool> m_connect_pool_;

  mongocxx::write_concern m_wc_majority_{};
  mongocxx::read_concern m_rc_local_{};
  mongocxx::read_preference m_rp_primary_{};
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
#pragma once

#include <spdlog/fmt/fmt.h>

#include <algorithm>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>
#include <list>
#include <memory>
#include <mongocxx/client.hpp>
#include <mongocxx/cursor.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>
#include <source_location>
#include <string>

#include "CtldPublicDefs.h"
#include "crane/PublicHeader.h"

namespace Ctld {

class MongodbClient {
 public:
  using PartitionQosMap = std::unordered_map<
      std::string /*partition name*/,
      std::pair<std::string /*default qos*/,
                std::list<std::string> /*allowed qos list*/>>;

  enum class EntityType {
    ACCOUNT = 0,
    USER = 1,
    QOS = 2,
  };

  MongodbClient() = default;  // Mongodb-c++ don't need to close the connection

  bool Connect();

  void Init();

  /* ----- Method of operating the job table ----------- */
  bool InsertRecoveredJob(
      crane::grpc::TaskInEmbeddedDb const& task_in_embedded_db);
  bool InsertJob(TaskInCtld* task);

  bool FetchAllJobRecords(std::list<TaskInCtld>* task_list);

  [[deprecated]] bool UpdateJobRecordField(uint64_t job_db_inx,
                                           const std::string& field_name,
                                           const std::string& val);

  [[deprecated]] bool UpdateJobRecordFields(
      uint64_t job_db_inx, const std::list<std::string>& field_name,
      const std::list<std::string>& val);

  bool CheckTaskDbIdExisted(int64_t task_db_id);

  /* ----- Method of operating the account table ----------- */
  bool InsertUser(const User& new_user);
  bool InsertAccount(const Account& new_account);
  bool InsertQos(const Qos& new_qos);

  bool DeleteEntity(EntityType type, const std::string& name);

  template <typename T>
  bool SelectUser(const std::string& key, const T& value, User* user);
  template <typename T>
  bool SelectAccount(const std::string& key, const T& value, Account* account);
  template <typename T>
  bool SelectQos(const std::string& key, const T& value, Qos* qos);

  void SelectAllUser(std::list<User>* user_list);
  void SelectAllAccount(std::list<Account>* account_list);
  void SelectAllQos(std::list<Qos>* qos_list);

  template <typename T>
  bool UpdateEntityOne(const EntityType type, const std::string& opt,
                       const std::string& name, const std::string& key,
                       const T& value) {
    using bsoncxx::builder::basic::kvp;

    std::string coll_name;
    document filter, updateItem;

    filter.append(kvp("name", name));
    updateItem.append(
        kvp(opt,
            [&](sub_document subDocument) {
              // DocumentAppendItem(subDocument, key, value);
              subDocument.append(kvp(key, value));
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
  };

  bool UpdateUser(const User& user);
  bool UpdateAccount(const Account& account);
  bool UpdateQos(const Qos& qos);

  bool CommitTransaction(
      const mongocxx::client_session::with_transaction_cb& callback);

 private:
  using array = bsoncxx::builder::basic::array;
  using document = bsoncxx::builder::basic::document;
  using sub_array = bsoncxx::builder::basic::sub_array;
  using sub_document = bsoncxx::builder::basic::sub_document;

  static void PrintError_(
      const char* msg,
      const std::source_location loc = std::source_location::current()) {
    CRANE_ERROR_LOC(loc, "MongodbError: {}\n", msg);
  }

  template <typename V>
  void DocumentAppendItem_(document* doc, const std::string& key,
                           const V& value);

  template <typename... Ts, std::size_t... Is>
  document documentConstructor_(
      const std::array<std::string, sizeof...(Ts)>& fields,
      const std::tuple<Ts...>& values, std::index_sequence<Is...>);

  template <typename... Ts>
  document DocumentConstructor_(
      const std::array<std::string, sizeof...(Ts)>& fields,
      const std::tuple<Ts...>& values);

  mongocxx::client* GetClient_();
  mongocxx::client_session* GetSession_();

  void ViewToUser_(const bsoncxx::document::view& user_view, User* user);

  document UserToDocument_(const User& user);

  void ViewToAccount_(const bsoncxx::document::view& account_view,
                      Account* account);

  document AccountToDocument_(const Account& account);

  void ViewToQos_(const bsoncxx::document::view& qos_view, Qos* qos);

  document QosToDocument_(const Qos& qos);

  document TaskInCtldToDocument_(TaskInCtld* task);
  document TaskInEmbeddedDbToDocument_(
      crane::grpc::TaskInEmbeddedDb const& task);

  std::string m_db_name_, m_connect_uri_;
  const std::string m_job_collection_name_{"job_table"};
  const std::string m_account_collection_name_{"acct_table"};
  const std::string m_user_collection_name_{"user_table"};
  const std::string m_qos_collection_name_{"qos_table"};

  std::unique_ptr<mongocxx::instance> m_instance_;
  std::unique_ptr<mongocxx::pool> m_connect_pool_;

  mongocxx::write_concern m_wc_majority_{};
  mongocxx::read_concern m_rc_local_{};
  mongocxx::read_preference m_rp_primary_{};
};

template <>
void MongodbClient::DocumentAppendItem_<std::list<std::string>>(
    document* doc, const std::string& key, const std::list<std::string>& value);

template <>
void MongodbClient::DocumentAppendItem_<MongodbClient::PartitionQosMap>(
    document* doc, const std::string& key,
    const MongodbClient::PartitionQosMap& value);

}  // namespace Ctld

inline std::unique_ptr<Ctld::MongodbClient> g_db_client;
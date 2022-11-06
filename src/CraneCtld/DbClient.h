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
#include <string>

#include "CtldPublicDefs.h"
#include "crane/PublicHeader.h"

using bsoncxx::builder::basic::array;
using bsoncxx::builder::basic::document;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::sub_array;
using bsoncxx::builder::basic::sub_document;

namespace Ctld {

class MongodbClient {
 public:
  using PartitionQosMap = std::unordered_map<
      std::string /*partition name*/,
      std::pair<std::string /*default qos*/,
                std::list<std::string> /*allowed qos list*/>>;

  enum EntityType {
    Account = 0,
    User = 1,
    Qos = 2,
  };

  //  struct MongodbResult {
  //    bool ok{false};
  //    std::optional<std::string> reason;
  //  };

  struct MongodbFilter {
    enum RelationalOperator {
      Equal,
      Greater,
      Less,
      GreaterOrEqual,
      LessOrEqual
    };

    std::string object;
    RelationalOperator relateOperator;
    std::string value;
  };

  MongodbClient() = default;  // Mongodb-c++ don't need to close the connection

  bool Connect();

  void Init();

  /* ----- Method of operating the job table ----------- */
  bool GetMaxExistingJobId(uint64_t* job_id);

  bool GetLastInsertId(uint64_t* id);

  bool InsertJob(uint64_t* job_db_inx, uint64_t mod_timestamp,
                 const std::string& account, uint32_t cpu,
                 uint64_t memory_bytes, const std::string& job_name,
                 const std::string& env, uint32_t id_job, uid_t id_user,
                 uid_t id_group, const std::string& nodelist,
                 uint32_t nodes_alloc, const std::string& node_inx,
                 const std::string& partition_name, uint32_t priority,
                 uint64_t submit_timestamp, const std::string& script,
                 uint32_t state, uint32_t timelimit,
                 const std::string& work_dir,
                 const crane::grpc::TaskToCtld& task_to_ctld);

  bool FetchJobRecordsWithStates(
      std::list<Ctld::TaskInCtld>* task_list,
      const std::list<crane::grpc::TaskStatus>& states);

  bool UpdateJobRecordField(uint64_t job_db_inx, const std::string& field_name,
                            const std::string& val);

  bool UpdateJobRecordFields(uint64_t job_db_inx,
                             const std::list<std::string>& field_name,
                             const std::list<std::string>& val);

  /* ----- Method of operating the account table ----------- */
  bool InsertUser(const Ctld::User& new_user);
  bool InsertAccount(const Ctld::Account& new_account);
  bool InsertQos(const Ctld::Qos& new_qos);

  bool DeleteEntity(EntityType type, const std::string& name);

  template <typename T>
  bool SelectUser(const std::string& key, const T& value, Ctld::User* user);
  template <typename T>
  bool SelectAccount(const std::string& key, const T& value,
                     Ctld::Account* account);
  bool SelectQosByName(const std::string& name, Ctld::Qos* qos);

  void SelectAllUser(std::list<Ctld::User>& user_list);
  void SelectAllAccount(std::list<Ctld::Account>& account_list);
  void SelectAllQos(std::list<Ctld::Qos>& qos_list);

  template <typename T>
  bool UpdateEntityOne(EntityType type, const std::string& opt,
                       const std::string& name, const std::string& key,
                       const T& value) {
    std::shared_ptr<mongocxx::collection> coll;
    document filter, updateItem;

    filter.append(kvp("name", name));
    updateItem.append(kvp(opt, [&](sub_document subDocument) {
      // DocumentAppendItem(subDocument, key, value);
      subDocument.append(kvp(key, value));
    }));

    switch (type) {
      case MongodbClient::Account:
        coll = m_account_collection;
        break;
      case User:
        coll = m_user_collection;
        break;
      case Qos:
        coll = m_qos_collection;
        break;
    }

    bsoncxx::stdx::optional<mongocxx::result::update> result =
        coll->update_one(filter.view(), updateItem.view());

    if (!result || !result->modified_count()) {
      return false;
    }
    return true;
  };

  //  template <typename T>
  //  bool UpdatePartitionQosMap(const std::string& opt, const std::string&
  //  name,
  //                             const std::string& partition_name,
  //                             const std::string& key, const T& value) {
  //    document filter, updateItem;
  //
  //    Ctld::User user;
  ////    user.allowed_partition_qos_map,partition_name
  //    filter.append(kvp("name", name),
  //                  kvp("allowed_partition_qos_map."));
  //    updateItem.append(kvp(opt, [&](sub_document subDocument) {
  //      subDocument.append(kvp(key, value));
  //    }));
  //  }

  bool UpdateUser(Ctld::User& user);
  bool UpdateAccount(Ctld::Account& account);
  bool UpdateQos(const std::string& name, Ctld::Qos& qos);

  void ViewToUser(const bsoncxx::document::view& user_view, Ctld::User* user);

  document UserToDocument(const Ctld::User& user);

  void ViewToAccount(const bsoncxx::document::view& account_view,
                     Ctld::Account* account);

  document AccountToDocument(const Ctld::Account& account);

 private:
  static void PrintError_(const char* msg) {
    CRANE_ERROR("MongodbError: {}\n", msg);
  }

  template <typename V>
  void DocumentAppendItem(document& doc, const std::string& key,
                          const V& value);

  template <typename... Ts, std::size_t... Is>
  document DocumentConstructor_(
      const std::array<std::string, sizeof...(Ts)>& fields,
      const std::tuple<Ts...>& values, std::index_sequence<Is...>);

  template <typename... Ts>
  document DocumentConstructor(
      const std::array<std::string, sizeof...(Ts)>& fields,
      const std::tuple<Ts...>& values);

  std::string m_db_name;
  const std::string m_job_collection_name{"job_table"};
  const std::string m_account_collection_name{"acct_table"};
  const std::string m_user_collection_name{"user_table"};
  const std::string m_qos_collection_name{"qos_table"};

  std::unique_ptr<mongocxx::instance> m_dbInstance;
  std::unique_ptr<mongocxx::client> m_client;
  std::unique_ptr<mongocxx::database> m_database;
  std::shared_ptr<mongocxx::collection> m_job_collection, m_account_collection,
      m_user_collection, m_qos_collection;
};

#warning class outside
template <>
void MongodbClient::DocumentAppendItem<std::list<std::string>>(
    document& doc, const std::string& key, const std::list<std::string>& value);

template <>
void MongodbClient::DocumentAppendItem<MongodbClient::PartitionQosMap>(
    document& doc, const std::string& key,
    const MongodbClient::PartitionQosMap& value);

}  // namespace Ctld

inline std::unique_ptr<Ctld::MongodbClient> g_db_client;
#include <gtest/gtest.h>

#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/cursor.hpp>
#include <mongocxx/instance.hpp>

#include "CtldPublicDefs.h"

using namespace mongocxx;
using namespace mongocxx::options;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;
using bsoncxx::builder::stream::document;

class MongodbClient {
 public:
  MongodbClient() = default;

  ~MongodbClient();

  bool Connect();

  void Init();

  bool AddUser(const Ctld::User& new_user);
  bool AddAccount(const Ctld::Account& new_account);
  bool AddQos(const Ctld::Qos& new_qos);

  bool DeleteUser(uid_t uid);

  bool GetUserInfo(uid_t uid, Ctld::User* user);
  bool GetAccountInfo(const std::string& name, Ctld::Account* account);
  bool GetQosInfo(const std::string& name, Ctld::Qos* qos);

 private:
  const std::string m_db_name{"crane_db"};
  const std::string m_account_collection_name{"acct_table"};
  const std::string m_user_collection_name{"user_table"};
  const std::string m_qos_collection_name{"qos_table"};

  mongocxx::instance* m_dbInstance{nullptr};
  mongocxx::client* m_client = {nullptr};
  mongocxx::database* m_database = {nullptr};
  mongocxx::collection *m_account_collection{nullptr},
      *m_user_collection{nullptr}, *m_qos_collection{nullptr};

  stdx::optional<bsoncxx::document::value> find_One(
      mongocxx::collection* coll, document& filter,
      const mongocxx::v_noabi::options::find& options =
          mongocxx::v_noabi::options::find());
  cursor* find(mongocxx::collection* coll, document& filter,
               const mongocxx::v_noabi::options::find& options =
                   mongocxx::v_noabi::options::find());
  stdx::optional<result::insert_one> insert_One(
      mongocxx::collection* coll, document& doc,
      const mongocxx::v_noabi::options::insert& options = {});
  stdx::optional<result::update> update_One(
      mongocxx::collection* coll, document& filter, document& value,
      const mongocxx::v_noabi::options::update& options =
          mongocxx::v_noabi::options::update());
  stdx::optional<result::update> update_Many(
      mongocxx::collection* coll, document& filter, document& value,
      const mongocxx::v_noabi::options::update& options =
          mongocxx::v_noabi::options::update());
  stdx::optional<result::delete_result> delete_One(
      mongocxx::collection* coll, document& filter,
      const mongocxx::v_noabi::options::delete_options& options =
          mongocxx::v_noabi::options::delete_options());
  stdx::optional<result::delete_result> delete_Many(
      mongocxx::collection* coll, document& filter,
      const mongocxx::v_noabi::options::delete_options& options =
          mongocxx::v_noabi::options::delete_options());
  std::int64_t countDocument(mongocxx::collection* coll, document& filter,
                             const options::count& option = options::count());
};

MongodbClient::~MongodbClient() {
  delete m_dbInstance;
  delete m_client;
}

bool MongodbClient::Connect() {
  // default port 27017
  mongocxx::uri uri{"mongodb://crane:123456!!@localhost:27017"};
  m_dbInstance = new (std::nothrow) mongocxx::instance();
  m_client = new (std::nothrow) mongocxx::client(uri);

  if (!m_client) {
    CRANE_ERROR("Mongodb error: can't connect to localhost:27017");
    return false;
  }
  return true;
}

void MongodbClient::Init() {
  m_database = new mongocxx::database(m_client->database(m_db_name));

  if (!m_database->has_collection(m_account_collection_name)) {
    m_database->create_collection(m_account_collection_name);
  }
  m_account_collection = new mongocxx::collection(
      m_database->collection(m_account_collection_name));

  if (!m_database->has_collection(m_user_collection_name)) {
    m_database->create_collection(m_user_collection_name);
  }
  m_user_collection =
      new mongocxx::collection(m_database->collection(m_user_collection_name));

  if (!m_database->has_collection(m_qos_collection_name)) {
    m_database->create_collection(m_qos_collection_name);
  }
  m_qos_collection =
      new mongocxx::collection(m_database->collection(m_qos_collection_name));

  if (!m_account_collection || !m_user_collection || !m_qos_collection) {
    CRANE_ERROR("Mongodb Error: can't get instance of crane_db tables");
    std::exit(1);
  }
}

stdx::optional<bsoncxx::document::value> MongodbClient::find_One(
    mongocxx::collection* coll, document& filter, const class find& options) {
  stdx::optional<bsoncxx::document::value> ret;

  if (m_dbInstance && m_client) {
    return coll->find_one(filter.view(), options);
  }
  return ret;
}

cursor* MongodbClient::find(mongocxx::collection* coll, document& filter,
                            const mongocxx::v_noabi::options::find& options) {
  if (m_dbInstance && m_client) {
    auto c = coll->find(filter.view(), options);
    return new (std::nothrow) cursor(std::move(c));
  }

  return nullptr;
}

stdx::optional<result::insert_one> MongodbClient::insert_One(
    mongocxx::collection* coll, document& doc,
    const mongocxx::v_noabi::options::insert& options) {
  stdx::optional<result::insert_one> ret;

  if (m_dbInstance && m_client) {
    ret = coll->insert_one(doc.view(), options);
  }

  return ret;
}

stdx::optional<result::update> MongodbClient::update_One(
    mongocxx::collection* coll, document& filter, document& value,
    const mongocxx::v_noabi::options::update& options) {
  stdx::optional<result::update> ret;
  if (m_dbInstance && m_client) {
    ret = coll->update_one(filter.view(), make_document(kvp("$set", value)),
                           options);
  }

  return ret;
}

stdx::optional<result::update> MongodbClient::update_Many(
    mongocxx::collection* coll, document& filter, document& value,
    const mongocxx::v_noabi::options::update& options) {
  stdx::optional<result::update> ret;
  if (m_dbInstance && m_client) {
    ret = coll->update_many(filter.view(), make_document(kvp("$set", value)),
                            options);
  }

  return ret;
}

stdx::optional<result::delete_result> MongodbClient::delete_One(
    mongocxx::collection* coll, document& filter,
    const mongocxx::v_noabi::options::delete_options& options) {
  stdx::optional<result::delete_result> ret;

  if (m_dbInstance && m_client) {
    ret = coll->delete_one(filter.view(), options);
  }

  return ret;
}

stdx::optional<result::delete_result> MongodbClient::delete_Many(
    mongocxx::collection* coll, document& filter,
    const mongocxx::v_noabi::options::delete_options& options) {
  stdx::optional<result::delete_result> ret;

  if (m_dbInstance && m_client) {
    ret = coll->delete_many(filter.view(), options);
  }

  return ret;
}

std::int64_t MongodbClient::countDocument(mongocxx::collection* coll,
                                          document& filter,
                                          const options::count& option) {
  return coll->count_documents(filter.view(), option);
}

bool MongodbClient::AddUser(const Ctld::User& new_user) {
  // Avoid duplicate insertion
  bsoncxx::stdx::optional<bsoncxx::document::value> find_result =
      m_user_collection->find_one(document{}
                                  << "uid" << std::to_string(new_user.uid)
                                  << bsoncxx::builder::stream::finalize);
  if (find_result) {
    return false;
  }

  if (!new_user.account.empty()) {
    // update the user's account's users_list
    bsoncxx::stdx::optional<mongocxx::result::update> update_result =
        m_account_collection->update_one(
            document{} << "name" << new_user.account
                       << bsoncxx::builder::stream::finalize,
            document{} << "$addToSet" << bsoncxx::builder::stream::open_document
                       << "users" << std::to_string(new_user.uid)
                       << bsoncxx::builder::stream::close_document
                       << bsoncxx::builder::stream::finalize);

    if (!update_result || !update_result->modified_count()) {
      return false;
    }
  }

  // When there is no indefinite list of objects in the class, the flow based
  // method can be used, which is the most efficient More methods are shown on
  // the web https://www.nuomiphp.com/eplan/2742.html
  auto builder = bsoncxx::builder::stream::document{};
  auto array_context =
      builder << "deleted" << false << "uid" << std::to_string(new_user.uid)
              << "account" << new_user.account << "name" << new_user.name
              << "admin_level" << new_user.admin_level << "allowed_partition"
              << bsoncxx::builder::stream::open_array;

  for (const auto& partition : new_user.allowed_partition) {
    array_context << partition;
  }
  bsoncxx::document::value doc_value =
      array_context
      << bsoncxx::builder::stream::close_array
      << bsoncxx::builder::stream::
             finalize;  // Use bsoncxx::builder::stream::finalize to
                        // obtain a bsoncxx::document::value instance.

  stdx::optional<result::insert_one> ret;
  if (m_dbInstance && m_client) {
    ret = m_user_collection->insert_one(doc_value.view());
  }
  return true;
}

bool MongodbClient::AddAccount(const Ctld::Account& new_account) {
  // Avoid duplicate insertion
  bsoncxx::stdx::optional<bsoncxx::document::value> find_result =
      m_account_collection->find_one(document{}
                                     << "name" << new_account.name
                                     << bsoncxx::builder::stream::finalize);
  if (find_result) {
    return false;
  }

  if (!new_account.parent_account.empty()) {
    // update the parent account's child_account_list
    bsoncxx::stdx::optional<mongocxx::result::update> update_result =
        m_account_collection->update_one(
            document{} << "name" << new_account.parent_account
                       << bsoncxx::builder::stream::finalize,
            document{} << "$addToSet" << bsoncxx::builder::stream::open_document
                       << "child_account" << new_account.name
                       << bsoncxx::builder::stream::close_document
                       << bsoncxx::builder::stream::finalize);

    if (!update_result || !update_result->modified_count()) {
      return false;
    }
  }

  auto builder = bsoncxx::builder::stream::document{};
  auto array_context =
      builder
      << "deleted" << false << "name" << new_account.name << "description"
      << new_account.description
      // Use Empty list to seize a seat, not support to initial this member
      << "users" << bsoncxx::builder::stream::open_array
      << bsoncxx::builder::stream::close_array
      // Use Empty list to seize a seat, not support to initial this member
      << "child_account" << bsoncxx::builder::stream::open_array
      << bsoncxx::builder::stream::close_array << "parent_account"
      << new_account.parent_account << "qos" << new_account.qos
      << "allowed_partition" << bsoncxx::builder::stream::open_array;

  for (const auto& partition : new_account.allowed_partition) {
    array_context << partition;
  }
  bsoncxx::document::value doc_value = array_context
                                       << bsoncxx::builder::stream::close_array
                                       << bsoncxx::builder::stream::finalize;

  stdx::optional<result::insert_one> ret;
  if (m_dbInstance && m_client) {
    ret = m_account_collection->insert_one(doc_value.view());
  }
  //  return ret->inserted_id().get_bool();
  return true;
}

bool MongodbClient::AddQos(const Ctld::Qos& new_qos) {
  auto builder = bsoncxx::builder::stream::document{};
  bsoncxx::document::value doc_value =
      builder << "name" << new_qos.name << "description" << new_qos.description
              << "priority" << new_qos.priority << "max_jobs_per_user"
              << new_qos.max_jobs_per_user
              << bsoncxx::builder::stream::
                     finalize;  // Use bsoncxx::builder::stream::finalize to
                                // obtain a bsoncxx::document::value instance.
  stdx::optional<result::insert_one> ret;
  if (m_dbInstance && m_client) {
    ret = m_user_collection->insert_one(doc_value.view());
  }
  //  return ret->inserted_id().get_bool();
  return true;
}

bool MongodbClient::GetUserInfo(uid_t uid, Ctld::User* user) {
  bsoncxx::stdx::optional<bsoncxx::document::value> result =
      m_user_collection->find_one(document{}
                                  << "uid" << std::to_string(uid)
                                  << bsoncxx::builder::stream::finalize);
  if (result) {
    bsoncxx::document::view user_view = result->view();
    user->deleted = user_view["deleted"].get_bool();
    user->uid = std::stoi(std::string(user_view["uid"].get_utf8().value));
    user->name = user_view["name"].get_utf8().value;
    user->account = user_view["account"].get_utf8().value;
    user->admin_level =
        (Ctld::User::AdminLevel)user_view["admin_level"].get_int32().value;
    for (auto&& partition : user_view["allowed_partition"].get_array().value) {
      user->allowed_partition.emplace_back(partition.get_utf8().value);
    }
    std::cout << bsoncxx::to_json(*result) << "\n";
    return true;
  }
  return false;
}

bool MongodbClient::GetAccountInfo(const std::string& name,
                                   Ctld::Account* account) {
  bsoncxx::stdx::optional<bsoncxx::document::value> result =
      m_account_collection->find_one(
          document{} << "name" << name << bsoncxx::builder::stream::finalize);
  if (result) {
    bsoncxx::document::view account_view = result->view();
    account->deleted = account_view["deleted"].get_bool().value;
    account->name = account_view["name"].get_utf8().value;
    account->description = account_view["description"].get_utf8().value;
    for (auto&& user : account_view["users"].get_array().value) {
      account->users.emplace_back(user.get_utf8().value);
    }
    for (auto&& acct : account_view["child_account"].get_array().value) {
      account->child_account.emplace_back(acct.get_utf8().value);
    }
    for (auto&& partition :
         account_view["allowed_partition"].get_array().value) {
      account->allowed_partition.emplace_back(partition.get_utf8().value);
    }
    account->parent_account = account_view["parent_account"].get_utf8().value;
    account->qos = account_view["qos"].get_utf8().value;
    return true;
  }
  return false;
}

bool MongodbClient::GetQosInfo(const std::string& name, Ctld::Qos* qos) {
  bsoncxx::stdx::optional<bsoncxx::document::value> result =
      m_qos_collection->find_one(
          document{} << "name" << name << bsoncxx::builder::stream::finalize);
  if (result) {
    bsoncxx::document::view user_view = result->view();
    qos->name = user_view["name"].get_utf8().value;
    qos->description = user_view["description"].get_utf8().value;
    qos->priority = user_view["priority"].get_int32();
    qos->max_jobs_per_user = user_view["max_jobs_per_user"].get_int32();
    std::cout << bsoncxx::to_json(*result) << "\n";
    return true;
  }
  return false;
}

bool MongodbClient::DeleteUser(uid_t uid) {
  bsoncxx::stdx::optional<mongocxx::result::update> result =
      m_user_collection->update_one(
          document{} << "uid" << std::to_string(uid)
                     << bsoncxx::builder::stream::finalize,
          document{} << "$set" << bsoncxx::builder::stream::open_document
                     << "deleted" << true
                     << bsoncxx::builder::stream::close_document
                     << bsoncxx::builder::stream::finalize);

  if (!result || !result->modified_count()) {
    return false;
  }
  return true;
}

TEST(MongodbConnector, Simple) {
  MongodbClient client;
  ASSERT_TRUE(client.Connect());
  client.Init();

  Ctld::Account root_account;
  root_account.name = "China";
  root_account.description = "motherland";
  root_account.qos = "normal";
  root_account.allowed_partition.emplace_back("CPU");
  root_account.allowed_partition.emplace_back("MEM");
  root_account.allowed_partition.emplace_back("GPU");
  client.AddAccount(root_account);

  Ctld::Account child_account1, child_account2;
  child_account1.name = "Hunan";
  child_account1.parent_account = "China";
  child_account2.name = "CSU";
  child_account2.parent_account = "Hunan";
  client.AddAccount(child_account1);
  client.AddAccount(child_account2);

  Ctld::Account res_account;
  ASSERT_TRUE(client.GetAccountInfo("China", &res_account));
  ASSERT_TRUE(std::find(res_account.child_account.begin(),
                        res_account.child_account.end(), child_account1.name) !=
              res_account.child_account.end());

  Ctld::User user;
  user.uid = 888;
  user.account = "CSU";
  user.name = "test";
  user.admin_level = Ctld::User::Admin;
  user.allowed_partition.emplace_back("CPU");

  if (client.GetUserInfo(user.uid, &user))
    ASSERT_FALSE(client.AddUser(user));
  else
    ASSERT_TRUE(client.AddUser(user));
}
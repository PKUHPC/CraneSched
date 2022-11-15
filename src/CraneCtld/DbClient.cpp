#include "DbClient.h"

#include <absl/strings/str_join.h>

#include <bsoncxx/exception/exception.hpp>
#include <mongocxx/exception/exception.hpp>
#include <utility>

namespace Ctld {

bool MongodbClient::Connect() {
  m_dbInstance = std::make_unique<mongocxx::instance>();
  m_db_name = g_config.DbName;
  std::string uri_str;

  if (!g_config.DbUser.empty()) {
    uri_str = fmt::format("mongodb://{}:{}@{}:{}&replicaSet={}",
                          g_config.DbUser, g_config.DbPassword, g_config.DbHost,
                          g_config.DbPort, g_config.DbRSName);
  } else {
    uri_str = fmt::format("mongodb://{}:{}&replicaSet={}", g_config.DbHost,
                          g_config.DbPort, g_config.DbRSName);
  }
  m_connect_pool = std::make_unique<mongocxx::pool>(mongocxx::uri{uri_str});
  mongocxx::pool::entry client = m_connect_pool->acquire();
  m_client = std::make_unique<mongocxx::client>(mongocxx::uri{uri_str});

  try {
    std::vector<std::string> database_name = client->list_database_names();

    if (std::find(database_name.begin(), database_name.end(), m_db_name) ==
        database_name.end()) {
      CRANE_INFO(
          "Mongodb: database {} is not existed, crane will create the new "
          "database.",
          m_db_name);
    }
  } catch (const mongocxx::exception& e) {
    CRANE_CRITICAL(e.what());
    return false;
  }

  //  if (!client) {
  //    PrintError_("can't connect to localhost:27017");
  //    return false;
  //  }
  return true;
}

void MongodbClient::Init() {
  m_database =
      std::make_unique<mongocxx::database>(m_client->database(m_db_name));

  if (!m_database->has_collection(m_job_collection_name)) {
    m_database->create_collection(m_job_collection_name);
  }
  m_job_collection = std::make_unique<mongocxx::collection>(
      m_database->collection(m_job_collection_name));

  if (!m_database->has_collection(m_account_collection_name)) {
    m_database->create_collection(m_account_collection_name);
  }
  m_account_collection = std::make_unique<mongocxx::collection>(
      m_database->collection(m_account_collection_name));

  if (!m_database->has_collection(m_user_collection_name)) {
    m_database->create_collection(m_user_collection_name);
  }
  m_user_collection = std::make_unique<mongocxx::collection>(
      m_database->collection(m_user_collection_name));

  if (!m_database->has_collection(m_qos_collection_name)) {
    m_database->create_collection(m_qos_collection_name);
  }
  m_qos_collection = std::make_unique<mongocxx::collection>(
      m_database->collection(m_qos_collection_name));

  if (!m_account_collection || !m_user_collection || !m_qos_collection ||
      !m_job_collection) {
    PrintError_(
        fmt::format("Can't get instance of {} tables", m_db_name).c_str());
    std::exit(1);
  }

  wc_majority.acknowledge_level(mongocxx::write_concern::level::k_majority);
  rc_local.acknowledge_level(mongocxx::read_concern::level::k_local);
  rp_primary.mode(mongocxx::read_preference::read_mode::k_primary);

  // Start a client session for transaction
  m_client_session =
      std::make_unique<mongocxx::client_session>(m_client->start_session());
}

bool MongodbClient::GetMaxExistingJobId(uint64_t* job_id) {
#warning O(1)
  //  *job_id = m_job_collection->count_documents({});
  mongocxx::cursor cursor = m_job_collection->find({});
  *job_id = 0;
  for (auto view : cursor) {
    uint64_t id =
        std::strtoul(view["job_db_inx"].get_string().value.data(), nullptr, 10);
    if (id > *job_id) *job_id = id;
  }
  return true;
}

bool MongodbClient::GetLastInsertId(uint64_t* id) {
#warning O(1)
  //  *id = m_job_collection->count_documents({});
  mongocxx::cursor cursor = m_job_collection->find({});
  *id = 0;
  for (auto view : cursor) {
    *id =
        std::strtoul(view["job_db_inx"].get_string().value.data(), nullptr, 10);
  }
  return true;
}

bool MongodbClient::InsertJob(
    uint64_t* job_db_inx, uint64_t mod_timestamp, const std::string& account,
    uint32_t cpu, uint64_t memory_bytes, const std::string& job_name,
    const std::string& env, uint32_t id_job, uid_t id_user, uid_t id_group,
    const std::string& nodelist, uint32_t nodes_alloc,
    const std::string& node_inx, const std::string& partition_name,
    uint32_t priority, uint64_t submit_timestamp, const std::string& script,
    uint32_t state, uint32_t timelimit, const std::string& work_dir,
    const crane::grpc::TaskToCtld& task_to_ctld) {
  uint64_t last_id;
  if (!GetLastInsertId(&last_id)) {
    PrintError_("Failed to GetLastInsertId");
    return false;
  }
  *job_db_inx = last_id + 1;

  std::string task_to_ctld_str;
  task_to_ctld.SerializeToString(&task_to_ctld_str);

  auto builder = bsoncxx::builder::stream::document{};
  // Different from mariadb, the data type of each attribute column in mongodb
  // depends on the data type at the time of insertion. Since the function
  // UpdateJobRecordFields  treats all attributes indiscriminately, you need to
  // use the string data type for all attributes
  bsoncxx::document::value doc_value =
      builder << "job_db_inx" << std::to_string(last_id + 1) << "mod_time"
              << std::to_string(mod_timestamp) << "deleted"
              << "0"
              << "account" << account << "cpus_req" << std::to_string(cpu)
              << "mem_req" << std::to_string(memory_bytes) << "job_name"
              << job_name << "env" << env << "id_job" << std::to_string(id_job)
              << "id_user" << std::to_string(id_user) << "id_group"
              << std::to_string(id_group) << "nodelist" << nodelist
              << "nodes_alloc" << std::to_string(nodes_alloc) << "node_inx"
              << node_inx << "partition_name" << partition_name << "priority"
              << std::to_string(priority) << "time_eligible"
              << "0"
              << "time_start"
              << "0"
              << "time_end"
              << "0"
              << "time_suspended"
              << "0"
              << "script" << script << "state" << std::to_string(state)
              << "timelimit" << std::to_string(timelimit) << "time_submit"
              << std::to_string(submit_timestamp) << "work_dir" << work_dir
              << "task_to_ctld" << task_to_ctld_str
              << bsoncxx::builder::stream::finalize;

  if (m_dbInstance && m_client) {
    bsoncxx::stdx::optional<mongocxx::result::insert_one> ret;
    ret = m_job_collection->insert_one(doc_value.view());

    if (ret == bsoncxx::stdx::nullopt) {
      PrintError_("Failed to insert job record");
      return false;
    }
  } else {
    PrintError_("Database init failed");
    return false;
  }
  return true;
}

bool MongodbClient::FetchJobRecordsWithStates(
    std::list<Ctld::TaskInCtld>* task_list,
    const std::list<crane::grpc::TaskStatus>& states) {
  auto builder = bsoncxx::builder::stream::document{};
  auto array_context =
      builder << "$or"  // 'document {} << ' will lead to precondition failed
              << bsoncxx::builder::stream::open_array;

  for (auto state : states) {
    array_context << bsoncxx::builder::stream::open_document << "state"
                  << fmt::format("{}", state)
                  << bsoncxx::builder::stream::close_document;
  }
  bsoncxx::document::value doc_value = array_context
                                       << bsoncxx::builder::stream::close_array
                                       << bsoncxx::builder::stream::finalize;

  mongocxx::cursor cursor = m_job_collection->find(doc_value.view());

  // 0  job_db_inx    mod_time       deleted       account     cpus_req
  // 5  mem_req       job_name       env           id_job      id_user
  // 10 id_group      nodelist       nodes_alloc   node_inx    partition_name
  // 15 priority      time_eligible  time_start    time_end    time_suspended
  // 20 script        state          timelimit     time_submit work_dir
  // 25 submit_line   task_to_ctld

  for (auto view : cursor) {
    Ctld::TaskInCtld task;
    task.job_db_inx =
        std::strtoul(view["job_db_inx"].get_string().value.data(), nullptr, 10);
    task.resources.allocatable_resource.cpu_count =
        std::strtol(view["cpus_req"].get_string().value.data(), nullptr, 10);

    task.resources.allocatable_resource.memory_bytes =
        task.resources.allocatable_resource.memory_sw_bytes = std::strtoul(
            view["mem_req"].get_string().value.data(), nullptr, 10);
    task.name = view["job_name"].get_string().value;
    task.env = view["env"].get_string().value;
    task.task_id =
        std::strtol(view["id_job"].get_string().value.data(), nullptr, 10);
    task.uid =
        std::strtol(view["id_user"].get_string().value.data(), nullptr, 10);
    task.gid =
        std::strtol(view["id_group"].get_string().value.data(), nullptr, 10);
    task.allocated_craneds_regex = view["nodelist"].get_string().value.data();
    task.partition_name = view["partition_name"].get_string().value;
    task.start_time = absl::FromUnixSeconds(
        std::strtol(view["time_start"].get_string().value.data(), nullptr, 10));
    task.end_time = absl::FromUnixSeconds(
        std::strtol(view["time_end"].get_string().value.data(), nullptr, 10));

    task.meta = Ctld::BatchMetaInTask{};
    auto& batch_meta = std::get<Ctld::BatchMetaInTask>(task.meta);
    batch_meta.sh_script = view["script"].get_string().value;
    task.status = crane::grpc::TaskStatus(
        std::strtol(view["state"].get_string().value.data(), nullptr, 10));
    task.time_limit = absl::Seconds(
        std::strtoul(view["timelimit"].get_string().value.data(), nullptr, 10));
    task.cwd = view["work_dir"].get_string().value;
    if (view["submit_line"])
      task.cmd_line = view["submit_line"].get_string().value;
    task.task_to_ctld.ParseFromString(
        view["task_to_ctld"].get_string().value.data());

    task_list->emplace_back(std::move(task));
  }

  return true;
}

bool MongodbClient::UpdateJobRecordField(uint64_t job_db_inx,
                                         const std::string& field_name,
                                         const std::string& val) {
  uint64_t timestamp = ToUnixSeconds(absl::Now());

  document filter, update;

  filter.append(kvp("job_db_inx", (int64_t)job_db_inx)),
      update.append(kvp("$set", [&](sub_document subDocument) {
        subDocument.append(kvp("mod_time", std::to_string(timestamp)),
                           kvp(field_name, val));
      }));

  bsoncxx::stdx::optional<mongocxx::result::update> update_result =
      m_job_collection->update_one(filter.view(), update.view());
  return true;
}

bool MongodbClient::UpdateJobRecordFields(
    uint64_t job_db_inx, const std::list<std::string>& field_names,
    const std::list<std::string>& values) {
  CRANE_ASSERT(field_names.size() == values.size() && !field_names.empty());

  auto builder = bsoncxx::builder::stream::document{};
  auto array_context = builder << "$set"
                               << bsoncxx::builder::stream::open_document;

  for (auto it_k = field_names.begin(), it_v = values.begin();
       it_k != field_names.end() && it_v != values.end(); ++it_k, ++it_v) {
    array_context << *it_k << *it_v;
  }

  bsoncxx::document::value doc_value =
      array_context << bsoncxx::builder::stream::close_document
                    << bsoncxx::builder::stream::finalize;

  document filter;
  filter.append(kvp("job_db_inx", std::to_string(job_db_inx)));
  bsoncxx::stdx::optional<mongocxx::result::update> update_result =
      m_job_collection->update_one(filter.view(), doc_value.view());
  return true;
}

bool MongodbClient::InsertUser(const Ctld::User& new_user,
                               mongocxx::client_session* session) {
  document doc = UserToDocument(new_user);

  bsoncxx::stdx::optional<mongocxx::result::insert_one> ret =
      session->client()[m_db_name][m_user_collection_name].insert_one(
          *session, doc.view());

  if (ret != bsoncxx::stdx::nullopt)
    return true;
  else
    return false;
}

bool MongodbClient::InsertAccount(const Ctld::Account& new_account,
                                  mongocxx::client_session* session) {
  document doc = AccountToDocument(new_account);

  bsoncxx::stdx::optional<mongocxx::result::insert_one> ret =
      session->client()[m_db_name][m_account_collection_name].insert_one(
          *session, doc.view());

  if (ret != bsoncxx::stdx::nullopt)
    return true;
  else
    return false;
}

bool MongodbClient::InsertQos(const Ctld::Qos& new_qos) {
  document doc = QosToDocument(new_qos);

  mongocxx::pool::entry client = m_connect_pool->acquire();
  //  bsoncxx::stdx::optional<mongocxx::result::insert_one> ret =
  //      session.client()[m_db_name][m_qos_collection_name].insert_one(session,
  //      doc.view());
  bsoncxx::stdx::optional<mongocxx::result::insert_one> ret =
      (*client)[m_db_name][m_qos_collection_name].insert_one(doc.view());

  if (ret != bsoncxx::stdx::nullopt)
    return true;
  else
    return false;
}

bool MongodbClient::DeleteEntity(const MongodbClient::EntityType type,
                                 const std::string& name) {
  std::shared_ptr<mongocxx::collection> coll;

  switch (type) {
    case MongodbClient::Account:
      coll = m_account_collection;
      break;
    case MongodbClient::User:
      coll = m_user_collection;
      break;
    case MongodbClient::Qos:
      coll = m_qos_collection;
      break;
  }
  document filter;
  filter.append(kvp("name", name));
  bsoncxx::stdx::optional<mongocxx::result::delete_result> result =
      coll->delete_one(filter.view());

  if (result && result.value().deleted_count() == 1)
    return true;
  else
    return false;
}

template <typename T>
bool MongodbClient::SelectUser(const std::string& key, const T& value,
                               Ctld::User* user) {
  document doc;
  doc.append(kvp(key, value));
  bsoncxx::stdx::optional<bsoncxx::document::value> result =
      m_user_collection->find_one(doc.view());

  if (result) {
    bsoncxx::document::view user_view = result->view();
    ViewToUser(user_view, user);
    return true;
  }
  return false;
}

template <typename T>
bool MongodbClient::SelectAccount(const std::string& key, const T& value,
                                  Ctld::Account* account) {
  document filter;
  filter.append(kvp(key, value));
  bsoncxx::stdx::optional<bsoncxx::document::value> result =
      m_account_collection->find_one(filter.view());

  if (result) {
    bsoncxx::document::view account_view = result->view();
    ViewToAccount(account_view, account);
    return true;
  }
  return false;
}

bool MongodbClient::SelectQosByName(const std::string& name, Ctld::Qos* qos) {
  return false;
}

void MongodbClient::SelectAllUser(std::list<Ctld::User>* user_list) {
  mongocxx::cursor cursor = m_user_collection->find({});
  for (auto view : cursor) {
    Ctld::User user;
    ViewToUser(view, &user);
    user_list->emplace_back(user);
  }
}

void MongodbClient::SelectAllAccount(std::list<Ctld::Account>* account_list) {
  mongocxx::cursor cursor = m_account_collection->find({});
  for (auto view : cursor) {
    Ctld::Account account;
    ViewToAccount(view, &account);
    account_list->emplace_back(account);
  }
}

void MongodbClient::SelectAllQos(std::list<Ctld::Qos>* qos_list) {
  mongocxx::cursor cursor = m_qos_collection->find({});
  for (auto view : cursor) {
    Ctld::Qos qos;
    ViewToQos(view, &qos);
    qos_list->emplace_back(qos);
  }
}

bool MongodbClient::UpdateUser(
    const Ctld::User& user,
    std::optional<mongocxx::client_session*> opt_session) {
  document doc = UserToDocument(user), setDocument, filter;

  filter.append(kvp("name", user.name));
  setDocument.append(kvp("$set", doc));

  bsoncxx::stdx::optional<mongocxx::result::update> update_result =
      (*connect_client)[m_db_name][m_user_collection_name].update_one(
          connect_session, filter.view(), setDocument.view());

  //  bsoncxx::stdx::optional<mongocxx::result::update> update_result;
  //  if (opt_session) {
  //    mongocxx::client_session* session = opt_session.value();
  //    update_result =
  //        session->client()[m_db_name][m_user_collection_name].update_one(
  //            *session, filter.view(), setDocument.view());
  //  } else {
  //    mongocxx::pool::entry client = m_connect_pool->acquire();
  //    update_result = (*client)[m_db_name][m_user_collection_name].update_one(
  //        filter.view(), setDocument.view());
  //  }

  if (!update_result || !update_result->modified_count()) {
    return false;
  }
  return true;
}

bool MongodbClient::UpdateAccount(const Ctld::Account& account,
                                  mongocxx::client_session* session) {
  document doc = AccountToDocument(account), setDocument, filter;

  setDocument.append(kvp("$set", doc));
  filter.append(kvp("name", account.name));

  bsoncxx::stdx::optional<mongocxx::result::update> update_result =
      session->client()[m_db_name][m_account_collection_name].update_one(
          *session, filter.view(), setDocument.view());

  if (!update_result || !update_result->modified_count()) {
    return false;
  }
  return true;
}

bool MongodbClient::UpdateQos(const Ctld::Qos& qos) {
  document doc = QosToDocument(qos), setDocument, filter;

  setDocument.append(kvp("$set", doc));
  filter.append(kvp("name", qos.name));

  mongocxx::pool::entry client = m_connect_pool->acquire();
  //  bsoncxx::stdx::optional<mongocxx::result::update> update_result =
  //      session.client()[m_db_name][m_qos_collection_name].update_one(session,
  //      filter.view(), setDocument.view());
  bsoncxx::stdx::optional<mongocxx::result::update> update_result =
      (*client)[m_db_name][m_qos_collection_name].update_one(
          filter.view(), setDocument.view());

  if (!update_result || !update_result->modified_count()) {
    return false;
  }
  return true;
}

bool MongodbClient::CommitTransaction(
    const mongocxx::client_session::with_transaction_cb& callback) {
  // Use with_transaction to start a transaction, execute the callback,
  // and commit (or abort on error).

  //  thread_local mongocxx::pool::entry client = m_connect_pool->acquire();
  //  thread_local mongocxx::client_session session = client->start_session();

  try {
    mongocxx::options::transaction opts;
    opts.write_concern(wc_majority);
    opts.read_concern(rc_local);
    opts.read_preference(rp_primary);
    connect_session.with_transaction(callback, opts);
  } catch (const mongocxx::exception& e) {
    CRANE_ERROR("Database transaction failed: {}", e.what());
    return false;
  }
  return true;
}

template <typename V>
void MongodbClient::DocumentAppendItem(document* doc, const std::string& key,
                                       const V& value) {
  doc->append(kvp(key, value));
}

template <>
void MongodbClient::DocumentAppendItem<std::list<std::string>>(
    document* doc, const std::string& key,
    const std::list<std::string>& value) {
  doc->append(kvp(key, [&value](sub_array array) {
    for (const auto& v : value) {
      array.append(v);
    }
  }));
}

template <>
void MongodbClient::DocumentAppendItem<MongodbClient::PartitionQosMap>(
    document* doc, const std::string& key,
    const MongodbClient::PartitionQosMap& value) {
  doc->append(kvp(key, [&value](sub_document mapValueDocument) {
    for (const auto& mapItem : value) {
      auto mapKey = mapItem.first;
      auto mapValue = mapItem.second;

      mapValueDocument.append(kvp(mapKey, [&mapValue](sub_array pairArray) {
        pairArray.append(mapValue.first);  // pair->first, default qos

        array listArray;
        for (const auto& s : mapValue.second) listArray.append(s);
        pairArray.append(listArray);
      }));
    }
  }));
}

template <typename... Ts, std::size_t... Is>
document MongodbClient::DocumentConstructor_(
    const std::array<std::string, sizeof...(Ts)>& fields,
    const std::tuple<Ts...>& values, std::index_sequence<Is...>) {
  bsoncxx::builder::basic::document document;
  // Here we use the basic builder instead of stream builder
  // The efficiency of different construction methods is shown on the web
  // https://www.nuomiphp.com/eplan/2742.html
  (DocumentAppendItem(&document, std::get<Is>(fields), std::get<Is>(values)),
   ...);
  return document;
}

template <typename... Ts>
document MongodbClient::DocumentConstructor(
    std::array<std::string, sizeof...(Ts)> const& fields,
    std::tuple<Ts...> const& values) {
  return DocumentConstructor_(fields, values,
                              std::make_index_sequence<sizeof...(Ts)>{});
}

void MongodbClient::ViewToUser(const bsoncxx::document::view& user_view,
                               Ctld::User* user) {
  try {
    user->deleted = user_view["deleted"].get_bool();
    user->uid = user_view["uid"].get_int64().value;
    user->name = user_view["name"].get_string().value;
    user->account = user_view["account"].get_string().value;
    user->admin_level =
        (Ctld::User::AdminLevel)user_view["admin_level"].get_int32().value;

    for (auto&& partition :
         user_view["allowed_partition_qos_map"].get_document().view()) {
      std::string default_qos;
      std::list<std::string> allowed_qos_list;
      auto partition_array = partition.get_array().value;
      default_qos = partition_array[0].get_string().value.data();
      for (auto&& allowed_qos : partition_array[1].get_array().value) {
        allowed_qos_list.emplace_back(allowed_qos.get_string().value);
      }
      user->allowed_partition_qos_map[std::string(partition.key())] =
          std::pair<std::string, std::list<std::string>>{default_qos,
                                                         allowed_qos_list};
    }
  } catch (const bsoncxx::exception& e) {
    PrintError_(e.what());
  }
}

document MongodbClient::UserToDocument(const Ctld::User& user) {
  std::array<std::string, 7> fields{"mod_time",
                                    "deleted",
                                    "uid",
                                    "account",
                                    "name",
                                    "admin_level",
                                    "allowed_partition_qos_map"};
  std::tuple<int64_t, bool, int64_t, std::string, std::string, int32_t,
             PartitionQosMap>
      values{ToUnixSeconds(absl::Now()),
             false,
             user.uid,
             user.account,
             user.name,
             user.admin_level,
             user.allowed_partition_qos_map};
  return DocumentConstructor(fields, values);
}

void MongodbClient::ViewToAccount(const bsoncxx::document::view& account_view,
                                  Ctld::Account* account) {
  try {
    account->deleted = account_view["deleted"].get_bool().value;
    account->name = account_view["name"].get_string().value;
    account->description = account_view["description"].get_string().value;
    for (auto&& user : account_view["users"].get_array().value) {
      account->users.emplace_back(user.get_string().value);
    }
    for (auto&& acct : account_view["child_accounts"].get_array().value) {
      account->child_accounts.emplace_back(acct.get_string().value);
    }
    for (auto&& partition :
         account_view["allowed_partition"].get_array().value) {
      account->allowed_partition.emplace_back(partition.get_string().value);
    }
    account->parent_account = account_view["parent_account"].get_string().value;
    account->default_qos = account_view["default_qos"].get_string().value;
    for (auto& allowed_qos :
         account_view["allowed_qos_list"].get_array().value) {
      account->allowed_qos_list.emplace_back(allowed_qos.get_string());
    }
  } catch (const bsoncxx::exception& e) {
    PrintError_(e.what());
  }
}

document MongodbClient::AccountToDocument(const Ctld::Account& account) {
  std::array<std::string, 9> fields{
      "deleted",         "name",           "description",       "users",
      "child_accounts",  "parent_account", "allowed_partition", "default_qos",
      "allowed_qos_list"};
  std::tuple<bool, std::string, std::string, std::list<std::string>,
             std::list<std::string>, std::string, std::list<std::string>,
             std::string, std::list<std::string>>
      values{false,
             account.name,
             account.description,
             account.users,
             account.child_accounts,
             account.parent_account,
             account.allowed_partition,
             account.default_qos,
             account.allowed_qos_list};

  return DocumentConstructor(fields, values);
}

void MongodbClient::ViewToQos(const bsoncxx::document::view& qos_view,
                              Ctld::Qos* qos) {
  try {
    qos->deleted = qos_view["deleted"].get_bool().value;
    qos->name = qos_view["name"].get_string().value;
    qos->description = qos_view["description"].get_string().value;
    qos->priority = qos_view["priority"].get_int32().value;
    qos->max_jobs_per_user = qos_view["max_jobs_per_user"].get_int32().value;
  } catch (const bsoncxx::exception& e) {
    PrintError_(e.what());
  }
}

document MongodbClient::QosToDocument(const Ctld::Qos& qos) {
  std::array<std::string, 5> fields{"deleted", "name", "description",
                                    "priority", "max_jobs_per_user"};
  std::tuple<bool, std::string, std::string, int, int> values{
      false, qos.name, qos.description, qos.priority, qos.max_jobs_per_user};

  return DocumentConstructor(fields, values);
}
mongocxx::pool::entry MongodbClient::GetClient() {
  if (m_connect_pool) {
    return m_connect_pool->acquire();
  }
}

}  // namespace Ctld

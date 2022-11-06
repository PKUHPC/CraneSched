#include "DbClient.h"

#include <absl/strings/str_join.h>

#include <utility>

namespace Ctld {

bool MongodbClient::Connect() {
  m_dbInstance = std::make_unique<mongocxx::instance>();
  m_db_name = g_config.DbName;
  std::string uri_str;

  if (!g_config.DbUser.empty()) {
    uri_str =
        fmt::format("mongodb://{}:{}@{}:{}", g_config.DbUser,
                    g_config.DbPassword, g_config.DbHost, g_config.DbPort);
  } else {
    uri_str = fmt::format("mongodb://{}:{}", g_config.DbHost, g_config.DbPort);
  }

  m_client = std::make_unique<mongocxx::client>(mongocxx::uri{uri_str});
  std::vector<std::string> database_name = m_client->list_database_names();
  if (std::find(database_name.begin(), database_name.end(), m_db_name) ==
      database_name.end()) {
    CRANE_INFO(
        "Mongodb: database {} is not existed, crane will create the new "
        "database.",
        m_db_name);
  }

  if (!m_client) {
    PrintError_("can't connect to localhost:27017");
    return false;
  }
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
        fmt::format("can't get instance of {} tables", m_db_name).c_str());
    std::exit(1);
  }
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

bool MongodbClient::InsertUser(const Ctld::User& new_user) {
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
             new_user.uid,
             new_user.account,
             new_user.name,
             new_user.admin_level,
             new_user.allowed_partition_qos_map};
  document doc = DocumentConstructor(fields, values);

  doc.view()[""].type() == bsoncxx::type::k_array;

  bsoncxx::stdx::optional<mongocxx::result::insert_one> ret =
      m_user_collection->insert_one(doc.view());

  if (ret != bsoncxx::stdx::nullopt)
    return true;
  else
    return false;
}

bool MongodbClient::InsertAccount(const Ctld::Account& new_account) {
  std::array<std::string, 9> fields{
      "deleted",         "name",           "description",       "users",
      "child_account",   "parent_account", "allowed_partition", "default_qos",
      "allowed_qos_list"};
  std::tuple<bool, std::string, std::string, std::list<std::string>,
             std::list<std::string>, std::string, std::list<std::string>,
             std::string, std::list<std::string>>
      values{false,
             new_account.name,
             new_account.description,
             new_account.users,
             new_account.child_account,
             new_account.parent_account,
             new_account.allowed_partition,
             new_account.default_qos,
             new_account.allowed_qos_list};

  document doc = DocumentConstructor(fields, values);

  bsoncxx::stdx::optional<mongocxx::result::insert_one> ret =
      m_account_collection->insert_one(doc.view());

  if (ret != bsoncxx::stdx::nullopt)
    return true;
  else
    return false;
}

bool MongodbClient::InsertQos(const Ctld::Qos& new_qos) {
  //  std::array<std::string, 8> fields{"deleted",     "name",
  //                                    "description",     "users",
  //                                    "child_account", "parent_account",
  //                                    "default_qos", "allowed_qos_list"};
  //  std::tuple<bool, std::string, std::string, std::list<std::string>,
  //  std::list<std::string>, std::string , std::string, std::list<std::string>>
  //      values{false,
  //             new_qos.,
  //             new_account.description,
  //             new_account.users,
  //             new_account.child_account,
  //             new_account.parent_account,
  //             new_account.default_qos,
  //             new_account.allowed_qos_list};
  //
  //  document doc =DocumentConstructor(fields, values);
  //
  //  stdx::optional<result::insert_one> ret =
  //  m_account_collection->insert_one(doc.view());
  //
  //  if (ret != stdx::nullopt)
  //    return true;
  //  else
  //    return false;
}

bool MongodbClient::DeleteEntity(MongodbClient::EntityType type,
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

void MongodbClient::SelectAllUser(std::list<Ctld::User>& user_list) {
  mongocxx::cursor cursor = m_user_collection->find({});
  for (auto view : cursor) {
    Ctld::User user;
    int i;
    ViewToUser(view, &user);
    user_list.emplace_back(user);
  }
}

void MongodbClient::SelectAllAccount(std::list<Ctld::Account>& account_list) {
  mongocxx::cursor cursor = m_account_collection->find({});
  for (auto view : cursor) {
    Ctld::Account account;
    ViewToAccount(view, &account);
    account_list.emplace_back(account);
  }
}

void MongodbClient::SelectAllQos(std::list<Ctld::Qos>& qos_list) {}

bool MongodbClient::UpdateUser(Ctld::User& user) {
  std::array<std::string, 6> fields{"deleted",     "uid",
                                    "account",     "name",
                                    "admin_level", "allowed_partition_qos_map"};
  std::tuple<bool, int64_t, std::string, std::string, int32_t, PartitionQosMap>
      values{false,     user.uid,         user.account,
             user.name, user.admin_level, user.allowed_partition_qos_map};
  document doc = DocumentConstructor(fields, values), setDocument, filter;

  filter.append(kvp("name", user.name));
  setDocument.append(kvp("$set", doc));

  bsoncxx::stdx::optional<mongocxx::result::update> update_result =
      m_user_collection->update_one(filter.view(), setDocument.view());

  if (!update_result || !update_result->modified_count()) {
    return false;
  }
  return true;
}

bool MongodbClient::UpdateAccount(Ctld::Account& account) {
  std::array<std::string, 9> fields{
      "deleted",         "name",           "description",       "users",
      "child_account",   "parent_account", "allowed_partition", "default_qos",
      "allowed_qos_list"};
  std::tuple<bool, std::string, std::string, std::list<std::string>,
             std::list<std::string>, std::string, std::list<std::string>,
             std::string, std::list<std::string>>
      values{false,
             account.name,
             account.description,
             account.users,
             account.child_account,
             account.parent_account,
             account.allowed_partition,
             account.default_qos,
             account.allowed_qos_list};

  document doc = DocumentConstructor(fields, values), setDocument, filter;

  setDocument.append(kvp("$set", doc));
  filter.append(kvp("name", account.name));

  bsoncxx::stdx::optional<mongocxx::result::update> update_result =
      m_account_collection->update_one(filter.view(), setDocument.view());

  if (!update_result || !update_result->modified_count()) {
    return false;
  }
  return true;
}

bool MongodbClient::UpdateQos(const std::string& name, Ctld::Qos& qos) {
  return false;
}

template <typename V>
void MongodbClient::DocumentAppendItem(document& doc, const std::string& key,
                                       const V& value) {
  doc.append(kvp(key, value));
}

template <>
void MongodbClient::DocumentAppendItem<std::list<std::string>>(
    document& doc, const std::string& key,
    const std::list<std::string>& value) {
  doc.append(kvp(key, [&value](sub_array array) {
    for (auto&& v : value) {
      array.append(v);
    }
  }));
}

template <>
void MongodbClient::DocumentAppendItem<MongodbClient::PartitionQosMap>(
    document& doc, const std::string& key,
    const MongodbClient::PartitionQosMap& value) {
  doc.append(kvp(key, [&value](sub_document mapValueDocument) {
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
  (DocumentAppendItem(document, std::get<Is>(fields), std::get<Is>(values)),
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
  account->deleted = account_view["deleted"].get_bool().value;
  account->name = account_view["name"].get_string().value;
  account->description = account_view["description"].get_string().value;
  for (auto&& user : account_view["users"].get_array().value) {
    account->users.emplace_back(user.get_string().value);
  }
  for (auto&& acct : account_view["child_account"].get_array().value) {
    account->child_account.emplace_back(acct.get_string().value);
  }
  for (auto&& partition : account_view["allowed_partition"].get_array().value) {
    account->allowed_partition.emplace_back(partition.get_string().value);
  }
  account->parent_account = account_view["parent_account"].get_string().value;
  account->default_qos = account_view["default_qos"].get_string().value;
  for (auto& allowed_qos : account_view["allowed_qos_list"].get_array().value) {
    account->allowed_qos_list.emplace_back(allowed_qos.get_string());
  }
}

document MongodbClient::AccountToDocument(const Ctld::Account& account) {
  std::array<std::string, 9> fields{
      "deleted",         "name",           "description",       "users",
      "child_account",   "parent_account", "allowed_partition", "default_qos",
      "allowed_qos_list"};
  std::tuple<bool, std::string, std::string, std::list<std::string>,
             std::list<std::string>, std::string, std::list<std::string>,
             std::string, std::list<std::string>>
      values{false,
             account.name,
             account.description,
             account.users,
             account.child_account,
             account.parent_account,
             account.allowed_partition,
             account.default_qos,
             account.allowed_qos_list};

  return DocumentConstructor(fields, values);
}

}  // namespace Ctld

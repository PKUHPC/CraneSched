#include "DbClient.h"

#include <bsoncxx/exception/exception.hpp>
#include <mongocxx/exception/exception.hpp>

namespace Ctld {

using bsoncxx::builder::basic::kvp;

bool MongodbClient::Connect() {
  try {
    m_connect_pool_ =
        std::make_unique<mongocxx::pool>(mongocxx::uri{m_connect_uri_});
    mongocxx::pool::entry client = m_connect_pool_->acquire();

    std::vector<std::string> database_name = client->list_database_names();

    if (std::find(database_name.begin(), database_name.end(), m_db_name_) ==
        database_name.end()) {
      CRANE_INFO(
          "Mongodb: database {} is not existed, crane will create the new "
          "database.",
          m_db_name_);
    }
  } catch (const mongocxx::exception& e) {
    CRANE_CRITICAL(e.what());
    return false;
  }

  return CheckDefaultRootAccountUserAndInit_();
}

bool MongodbClient::CheckDefaultRootAccountUserAndInit_() {
  Qos qos;
  if (!SelectQos("name", kUnlimitedQosName, &qos)) {
    CRANE_TRACE("Default Qos {} not found, crane will create it",
                kUnlimitedQosName);

    qos.name = kUnlimitedQosName;
    qos.description = "Crane default qos for unlimited resource";
    qos.priority = 0;
    qos.max_jobs_per_user = std::numeric_limits<
        std::remove_reference<decltype(qos.max_jobs_per_user)>::type>::max();
    qos.max_running_tasks_per_user = std::numeric_limits<std::remove_reference<
        decltype(qos.max_running_tasks_per_user)>::type>::max();
    qos.max_time_limit_per_task = absl::Seconds(INT64_MAX);
    qos.max_cpus_per_user = std::numeric_limits<
        std::remove_reference<decltype(qos.max_cpus_per_user)>::type>::max();
    qos.max_cpus_per_account = std::numeric_limits<
        std::remove_reference<decltype(qos.max_cpus_per_account)>::type>::max();
    qos.reference_count = 1;

    if (!InsertQos(qos)) {
      CRANE_ERROR("Failed to insert default qos {}!", kUnlimitedQosName);
      return false;
    }
  }

  Account root_account;
  if (!SelectAccount("name", "ROOT", &root_account)) {
    CRANE_TRACE("Default account ROOT not found. insert ROOT account into DB.");

    root_account.name = "ROOT";
    root_account.description = "Crane default account for root user";
    root_account.default_qos = kUnlimitedQosName;
    root_account.allowed_qos_list.emplace_back(root_account.default_qos);
    root_account.users.emplace_back("root");

    if (!InsertAccount(root_account)) {
      CRANE_ERROR("Failed to insert default ROOT account!");
      return false;
    }
  }

  User root_user;
  if (!SelectUser("uid", 0, &root_user)) {
    CRANE_TRACE("Default user ROOT not found. Insert it into DB.");

    root_user.name = "root";
    root_user.default_account = "ROOT";
    root_user.admin_level = User::Admin;
    root_user.uid = 0;
    root_user.account_to_attrs_map[root_user.default_account] =
        User::AttrsInAccount{User::PartToAllowedQosMap{}, false};

    if (!InsertUser(root_user)) {
      CRANE_ERROR("Failed to insert default user ROOT!");
      return false;
    }
  }

  return true;
}

bool MongodbClient::InsertRecoveredJob(
    const crane::grpc::TaskInEmbeddedDb& task_in_embedded_db) {
  document doc = TaskInEmbeddedDbToDocument_(task_in_embedded_db);

  bsoncxx::stdx::optional<mongocxx::result::insert_one> ret =
      (*GetClient_())[m_db_name_][m_task_collection_name_].insert_one(
          *GetSession_(), doc.view());

  if (ret != bsoncxx::stdx::nullopt) return true;

  PrintError_("Failed to insert in-memory TaskInCtld.");
  return false;
}

bool MongodbClient::InsertJob(TaskInCtld* task) {
  document doc = TaskInCtldToDocument_(task);

  bsoncxx::stdx::optional<mongocxx::result::insert_one> ret =
      (*GetClient_())[m_db_name_][m_task_collection_name_].insert_one(
          *GetSession_(), doc.view());

  if (ret != bsoncxx::stdx::nullopt) return true;

  PrintError_("Failed to insert in-memory TaskInCtld.");
  return false;
}

bool MongodbClient::FetchJobRecords(
    std::vector<std::unique_ptr<Ctld::TaskInCtld>>* task_list, size_t limit,
    bool reverse) {
  mongocxx::options::find option;
  if (limit > 0) {
    option = option.limit(limit);
  }

  document doc;
  if (reverse) {
    doc.append(kvp("task_db_id", -1));
    option = option.sort(doc.view());
  }

  mongocxx::cursor cursor =
      (*GetClient_())[m_db_name_][m_task_collection_name_].find({}, option);

  // 0  task_id       task_db_id     mod_time       deleted       account
  // 5  cpus_req      mem_req        task_name      env           id_user
  // 10 id_group      nodelist       nodes_alloc   node_inx    partition_name
  // 15 priority      time_eligible  time_start    time_end    time_suspended
  // 20 script        state          timelimit     time_submit work_dir
  // 25 submit_line   exit_code      username       qos
  try {
    for (auto view : cursor) {
      auto task = std::make_unique<TaskInCtld>();

      task->SetTaskId(view["task_id"].get_int32().value);
      task->SetTaskDbId(view["task_db_id"].get_int64().value);

      task->nodes_alloc = view["nodes_alloc"].get_int32().value;
      task->node_num = 0;

      task->account = view["account"].get_string().value.data();
      task->SetUsername(view["username"].get_string().value.data());

      task->resources.allocatable_resource.cpu_count =
          view["cpus_req"].get_double().value;
      task->resources.allocatable_resource.memory_bytes =
          task->resources.allocatable_resource.memory_sw_bytes =
              view["mem_req"].get_int64().value;
      task->name = view["task_name"].get_string().value;
      task->env = view["env"].get_string().value;
      task->qos = view["qos"].get_string().value;
      task->uid = view["id_user"].get_int32().value;
      task->SetGid(view["id_group"].get_int32().value);
      task->allocated_craneds_regex =
          view["nodelist"].get_string().value.data();
      task->partition_id = view["partition_name"].get_string().value;
      task->SetStartTimeByUnixSecond(view["time_start"].get_int64().value);
      task->SetEndTimeByUnixSecond(view["time_end"].get_int64().value);

      if (task->type == crane::grpc::Batch) {
        task->meta = Ctld::BatchMetaInTask{};
        auto& batch_meta = std::get<Ctld::BatchMetaInTask>(task->meta);
        batch_meta.sh_script = view["script"].get_string().value;
      }

      task->SetStatus(static_cast<crane::grpc::TaskStatus>(
          view["state"].get_int32().value));
      task->time_limit = absl::Seconds(view["timelimit"].get_int64().value);
      task->cwd = view["work_dir"].get_string().value;
      if (view["submit_line"])
        task->cmd_line = view["submit_line"].get_string().value;
      task->SetExitCode(view["exit_code"].get_int32().value);

      // Todo: As for now, only Batch type is implemented and some data
      // resolving
      //  is hardcoded. Hard-coding for Batch task will be resolved when
      //  Interactive task is implemented.
      task->type = crane::grpc::Batch;

      task_list->emplace_back(std::move(task));
    }
  } catch (const bsoncxx::exception& e) {
    PrintError_(e.what());
  }

  return true;
}

bool MongodbClient::CheckTaskDbIdExisted(int64_t task_db_id) {
  document doc;
  doc.append(kvp("job_db_inx", task_db_id));

  bsoncxx::stdx::optional<bsoncxx::document::value> result =
      (*GetClient_())[m_db_name_][m_task_collection_name_].find_one(doc.view());

  if (result) {
    return true;
  }
  return false;
}

bool MongodbClient::InsertUser(const Ctld::User& new_user) {
  document doc = UserToDocument_(new_user);
  doc.append(kvp("creation_time", ToUnixSeconds(absl::Now())));

  bsoncxx::stdx::optional<mongocxx::result::insert_one> ret =
      (*GetClient_())[m_db_name_][m_user_collection_name_].insert_one(
          *GetSession_(), doc.view());

  if (ret != bsoncxx::stdx::nullopt)
    return true;
  else
    return false;
}

bool MongodbClient::InsertAccount(const Ctld::Account& new_account) {
  document doc = AccountToDocument_(new_account);
  doc.append(kvp("creation_time", ToUnixSeconds(absl::Now())));

  bsoncxx::stdx::optional<mongocxx::result::insert_one> ret =
      (*GetClient_())[m_db_name_][m_account_collection_name_].insert_one(
          *GetSession_(), doc.view());

  if (ret != bsoncxx::stdx::nullopt)
    return true;
  else
    return false;
}

bool MongodbClient::InsertQos(const Ctld::Qos& new_qos) {
  document doc = QosToDocument_(new_qos);
  doc.append(kvp("creation_time", ToUnixSeconds(absl::Now())));

  bsoncxx::stdx::optional<mongocxx::result::insert_one> ret =
      (*GetClient_())[m_db_name_][m_qos_collection_name_].insert_one(
          doc.view());

  if (ret != bsoncxx::stdx::nullopt)
    return true;
  else
    return false;
}

bool MongodbClient::DeleteEntity(const MongodbClient::EntityType type,
                                 const std::string& name) {
  std::string_view coll;

  switch (type) {
    case EntityType::ACCOUNT:
      coll = m_account_collection_name_;
      break;
    case EntityType::USER:
      coll = m_user_collection_name_;
      break;
    case EntityType::QOS:
      coll = m_qos_collection_name_;
      break;
  }
  document filter;
  filter.append(kvp("name", name));
  bsoncxx::stdx::optional<mongocxx::result::delete_result> result =
      (*GetClient_())[m_db_name_][coll].delete_one(filter.view());

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
      (*GetClient_())[m_db_name_][m_user_collection_name_].find_one(doc.view());

  if (result) {
    bsoncxx::document::view user_view = result->view();
    ViewToUser_(user_view, user);
    return true;
  }
  return false;
}

void MongodbClient::SelectAllUser(std::list<Ctld::User>* user_list) {
  mongocxx::cursor cursor =
      (*GetClient_())[m_db_name_][m_user_collection_name_].find({});
  for (auto view : cursor) {
    Ctld::User user;
    ViewToUser_(view, &user);
    user_list->emplace_back(user);
  }
}

void MongodbClient::SelectAllAccount(std::list<Ctld::Account>* account_list) {
  mongocxx::cursor cursor =
      (*GetClient_())[m_db_name_][m_account_collection_name_].find({});
  for (auto view : cursor) {
    Ctld::Account account;
    ViewToAccount_(view, &account);
    account_list->emplace_back(account);
  }
}

void MongodbClient::SelectAllQos(std::list<Ctld::Qos>* qos_list) {
  mongocxx::cursor cursor =
      (*GetClient_())[m_db_name_][m_qos_collection_name_].find({});
  for (auto view : cursor) {
    Ctld::Qos qos;
    ViewToQos_(view, &qos);
    qos_list->emplace_back(qos);
  }
}

bool MongodbClient::UpdateUser(const Ctld::User& user) {
  document doc = UserToDocument_(user), setDocument, filter;
  doc.append(kvp("mod_time", ToUnixSeconds(absl::Now())));
  setDocument.append(kvp("$set", doc));

  filter.append(kvp("name", user.name));

  bsoncxx::stdx::optional<mongocxx::result::update> update_result =
      (*GetClient_())[m_db_name_][m_user_collection_name_].update_one(
          *GetSession_(), filter.view(), setDocument.view());

  if (!update_result || !update_result->modified_count()) {
    return false;
  }
  return true;
}

bool MongodbClient::UpdateAccount(const Ctld::Account& account) {
  document doc = AccountToDocument_(account), setDocument, filter;
  doc.append(kvp("mod_time", ToUnixSeconds(absl::Now())));
  setDocument.append(kvp("$set", doc));

  filter.append(kvp("name", account.name));

  bsoncxx::stdx::optional<mongocxx::result::update> update_result =
      (*GetClient_())[m_db_name_][m_account_collection_name_].update_one(
          *GetSession_(), filter.view(), setDocument.view());

  if (!update_result || !update_result->modified_count()) {
    return false;
  }
  return true;
}

bool MongodbClient::UpdateQos(const Ctld::Qos& qos) {
  document doc = QosToDocument_(qos), setDocument, filter;
  doc.append(kvp("mod_time", ToUnixSeconds(absl::Now())));
  setDocument.append(kvp("$set", doc));

  filter.append(kvp("name", qos.name));

  bsoncxx::stdx::optional<mongocxx::result::update> update_result =
      (*GetClient_())[m_db_name_][m_qos_collection_name_].update_one(
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
  try {
    mongocxx::options::transaction opts;
    opts.write_concern(m_wc_majority_);
    opts.read_concern(m_rc_local_);
    opts.read_preference(m_rp_primary_);
    GetSession_()->with_transaction(callback, opts);
  } catch (const mongocxx::exception& e) {
    CRANE_ERROR("Database transaction failed: {}", e.what());
    return false;
  }
  return true;
}

template <typename V>
void MongodbClient::DocumentAppendItem_(document& doc, const std::string& key,
                                        const V& value) {
  doc.append(kvp(key, value));
}

template <>
void MongodbClient::DocumentAppendItem_<std::list<std::string>>(
    document& doc, const std::string& key,
    const std::list<std::string>& value) {
  doc.append(kvp(key, [&value](sub_array array) {
    for (const auto& v : value) {
      array.append(v);
    }
  }));
}

template <>
void MongodbClient::DocumentAppendItem_<User::AccountToAttrsMap>(
    document& doc, const std::string& key,
    const User::AccountToAttrsMap& value) {
  doc.append(kvp(key, [&value, this](sub_document mapValueDocument) {
    for (const auto& mapItem : value) {
      mapValueDocument.append(
          kvp(mapItem.first, [&mapItem, this](sub_document itemDoc) {
            itemDoc.append(kvp("blocked", mapItem.second.blocked));
            SubDocumentAppendItem_(itemDoc, "allowed_partition_qos_map",
                                   mapItem.second.allowed_partition_qos_map);
          }));
    }
  }));
}

template <>
void MongodbClient::SubDocumentAppendItem_<std::list<std::string>>(
    sub_document& doc, const std::string& key,
    const std::list<std::string>& value) {
  doc.append(kvp(key, [&value](sub_array array) {
    for (const std::string& v : value) array.append(v);
  }));
}

template <>
void MongodbClient::SubDocumentAppendItem_<User::PartToAllowedQosMap>(
    sub_document& doc, const std::string& key,
    const User::PartToAllowedQosMap& value) {
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
bsoncxx::builder::basic::document MongodbClient::documentConstructor_(
    const std::array<std::string, sizeof...(Ts)>& fields,
    const std::tuple<Ts...>& values, std::index_sequence<Is...>) {
  bsoncxx::builder::basic::document document;
  // Here we use the basic builder instead of stream builder
  // The efficiency of different construction methods is shown on the web
  // https://www.nuomiphp.com/eplan/2742.html
  (DocumentAppendItem_(document, std::get<Is>(fields), std::get<Is>(values)),
   ...);
  return document;
}

template <typename... Ts>
bsoncxx::builder::basic::document MongodbClient::DocumentConstructor_(
    std::array<std::string, sizeof...(Ts)> const& fields,
    std::tuple<Ts...> const& values) {
  return documentConstructor_(fields, values,
                              std::make_index_sequence<sizeof...(Ts)>{});
}

mongocxx::client* MongodbClient::GetClient_() {
  if (m_connect_pool_) {
    thread_local mongocxx::pool::entry entry{m_connect_pool_->acquire()};
    return &(*entry);
  }
  return nullptr;
}

mongocxx::client_session* MongodbClient::GetSession_() {
  if (m_connect_pool_) {
    thread_local mongocxx::client_session session =
        GetClient_()->start_session();
    return &session;
  }

  return nullptr;
}

void MongodbClient::ViewToUser_(const bsoncxx::document::view& user_view,
                                Ctld::User* user) {
  try {
    user->deleted = user_view["deleted"].get_bool();
    user->uid = user_view["uid"].get_int64().value;
    user->name = user_view["name"].get_string().value;
    user->default_account = user_view["default_account"].get_string().value;
    user->admin_level =
        (Ctld::User::AdminLevel)user_view["admin_level"].get_int32().value;
    for (auto&& acc : user_view["coordinator_accounts"].get_array().value) {
      user->coordinator_accounts.emplace_back(acc.get_string().value);
    }

    for (auto&& account_to_attrs_map_item :
         user_view["account_to_attrs_map"].get_document().view()) {
      User::PartToAllowedQosMap temp;
      for (auto&& partition :
           account_to_attrs_map_item["allowed_partition_qos_map"]
               .get_document()
               .view()) {
        std::string default_qos;
        std::list<std::string> allowed_qos_list;
        auto partition_array = partition.get_array().value;
        default_qos = partition_array[0].get_string().value.data();
        for (auto&& allowed_qos : partition_array[1].get_array().value) {
          allowed_qos_list.emplace_back(allowed_qos.get_string().value);
        }
        temp[std::string(partition.key())] =
            std::pair<std::string, std::list<std::string>>{
                default_qos, std::move(allowed_qos_list)};
      }
      user->account_to_attrs_map[std::string(account_to_attrs_map_item.key())] =
          User::AttrsInAccount{std::move(temp),
                               account_to_attrs_map_item["blocked"].get_bool()};
    }

  } catch (const bsoncxx::exception& e) {
    PrintError_(e.what());
  }
}

bsoncxx::builder::basic::document MongodbClient::UserToDocument_(
    const Ctld::User& user) {
  std::array<std::string, 7> fields{"deleted",
                                    "uid",
                                    "default_account",
                                    "name",
                                    "admin_level",
                                    "account_to_attrs_map",
                                    "coordinator_accounts"};
  std::tuple<bool, int64_t, std::string, std::string, int32_t,
             User::AccountToAttrsMap, std::list<std::string>>
      values{false,
             user.uid,
             user.default_account,
             user.name,
             user.admin_level,
             user.account_to_attrs_map,
             user.coordinator_accounts};
  return DocumentConstructor_(fields, values);
}

void MongodbClient::ViewToAccount_(const bsoncxx::document::view& account_view,
                                   Ctld::Account* account) {
  try {
    account->deleted = account_view["deleted"].get_bool().value;
    account->blocked = account_view["blocked"].get_bool().value;
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
    for (auto&& allowed_qos :
         account_view["allowed_qos_list"].get_array().value) {
      account->allowed_qos_list.emplace_back(allowed_qos.get_string());
    }
    for (auto&& user : account_view["coordinators"].get_array().value) {
      account->coordinators.emplace_back(user.get_string().value);
    }
  } catch (const bsoncxx::exception& e) {
    PrintError_(e.what());
  }
}

bsoncxx::builder::basic::document MongodbClient::AccountToDocument_(
    const Ctld::Account& account) {
  std::array<std::string, 11> fields{
      "deleted",     "blocked",          "name",           "description",
      "users",       "child_accounts",   "parent_account", "allowed_partition",
      "default_qos", "allowed_qos_list", "coordinators"};
  std::tuple<bool, bool, std::string, std::string, std::list<std::string>,
             std::list<std::string>, std::string, std::list<std::string>,
             std::string, std::list<std::string>, std::list<std::string>>
      values{false,
             account.blocked,
             account.name,
             account.description,
             account.users,
             account.child_accounts,
             account.parent_account,
             account.allowed_partition,
             account.default_qos,
             account.allowed_qos_list,
             account.coordinators};

  return DocumentConstructor_(fields, values);
}

void MongodbClient::ViewToQos_(const bsoncxx::document::view& qos_view,
                               Ctld::Qos* qos) {
  try {
    qos->deleted = qos_view["deleted"].get_bool().value;
    qos->name = qos_view["name"].get_string().value;
    qos->description = qos_view["description"].get_string().value;
    qos->reference_count = qos_view["reference_count"].get_int32().value;
    qos->priority = qos_view["priority"].get_int64().value;
    qos->max_jobs_per_user = qos_view["max_jobs_per_user"].get_int64().value;
    qos->max_cpus_per_user = qos_view["max_cpus_per_user"].get_int64().value;
    qos->max_time_limit_per_task =
        absl::Seconds(qos_view["max_time_limit_per_task"].get_int64().value);
  } catch (const bsoncxx::exception& e) {
    PrintError_(e.what());
  }
}

bsoncxx::builder::basic::document MongodbClient::QosToDocument_(
    const Ctld::Qos& qos) {
  std::array<std::string, 8> fields{
      "deleted",           "name",
      "description",       "reference_count",
      "priority",          "max_jobs_per_user",
      "max_cpus_per_user", "max_time_limit_per_task"};
  std::tuple<bool, std::string, std::string, int, int64_t, int64_t, int64_t,
             int64_t>
      values{false,
             qos.name,
             qos.description,
             qos.reference_count,
             qos.priority,
             qos.max_jobs_per_user,
             qos.max_cpus_per_user,
             absl::ToInt64Seconds(qos.max_time_limit_per_task)};

  return DocumentConstructor_(fields, values);
}

MongodbClient::document MongodbClient::TaskInEmbeddedDbToDocument_(
    const crane::grpc::TaskInEmbeddedDb& task) {
  auto const& task_to_ctld = task.task_to_ctld();
  auto const& persisted_part = task.persisted_part();

  // 0  task_id       task_id        mod_time       deleted       account
  // 5  cpus_req      mem_req        task_name      env           id_user
  // 10 id_group      nodelist       nodes_alloc   node_inx    partition_name
  // 15 priority      time_eligible  time_start    time_end    time_suspended
  // 20 script        state          timelimit     time_submit work_dir
  // 25 submit_line   exit_code      username

  std::array<std::string, 28> fields{
      "task_id",        "task_db_id",    "mod_time",    "deleted",
      "account",  // 0 - 4
      "cpus_req",       "mem_req",       "task_name",   "env",
      "id_user",  // 5 - 9
      "id_group",       "nodelist",      "nodes_alloc", "node_inx",
      "partition_name",  // 10 - 14
      "priority",       "time_eligible", "time_start",  "time_end",
      "time_suspended",  // 15 - 19
      "script",         "state",         "timelimit",   "time_submit",
      "work_dir",                                     // 20 - 24
      "submit_line",    "exit_code",     "username",  // 25
  };

  std::tuple<int32_t, task_db_id_t, int64_t, bool, std::string,   /*0-4*/
             double, int64_t, std::string, std::string, int32_t,  /*5-9*/
             int32_t, std::string, int32_t, int32_t, std::string, /*10-14*/
             int64_t, int64_t, int64_t, int64_t, int64_t,         /*15-19*/
             std::string, int32_t, int64_t, int64_t, std::string, /*20-24*/
             std::string, int32_t, std::string>
      values{// 0-4
             static_cast<int32_t>(persisted_part.task_id()),
             persisted_part.task_db_id(), absl::ToUnixSeconds(absl::Now()),
             false, task_to_ctld.account(),
             // 5-9
             task_to_ctld.resources().allocatable_resource().cpu_core_limit(),
             static_cast<int64_t>(task_to_ctld.resources()
                                      .allocatable_resource()
                                      .memory_limit_bytes()),
             task_to_ctld.name(), task_to_ctld.env(),
             static_cast<int32_t>(task_to_ctld.uid()),
             // 10-14
             static_cast<int32_t>(persisted_part.gid()),
             util::HostNameListToStr(persisted_part.craned_ids()), 0, 0,
             task_to_ctld.partition_name(),
             // 15-19
             0, 0, 0, 0, 0,
             // 20-24
             task_to_ctld.batch_meta().sh_script(), persisted_part.status(),
             task_to_ctld.time_limit().seconds(), 0, task_to_ctld.cwd(),
             // 25
             task_to_ctld.cmd_line(), persisted_part.exit_code(),
             persisted_part.username()};

  return DocumentConstructor_(fields, values);
}

MongodbClient::document MongodbClient::TaskInCtldToDocument_(TaskInCtld* task) {
  std::string script;
  if (task->type == crane::grpc::Batch)
    script = std::get<BatchMetaInTask>(task->meta).sh_script;

  // 0  task_id       task_id        mod_time       deleted       account
  // 5  cpus_req      mem_req        task_name      env           id_user
  // 10 id_group      nodelist       nodes_alloc   node_inx    partition_name
  // 15 priority      time_eligible  time_start    time_end    time_suspended
  // 20 script        state          timelimit     time_submit work_dir
  // 25 submit_line   exit_code      username

  std::array<std::string, 29> fields{
      "task_id",        "task_db_id",    "mod_time",    "deleted",
      "account",  // 0 - 4
      "cpus_req",       "mem_req",       "task_name",   "env",
      "id_user",  // 5 - 9
      "id_group",       "nodelist",      "nodes_alloc", "node_inx",
      "partition_name",  // 10 - 14
      "priority",       "time_eligible", "time_start",  "time_end",
      "time_suspended",  // 15 - 19
      "script",         "state",         "timelimit",   "time_submit",
      "work_dir",                                              // 20 - 24
      "submit_line",    "exit_code",     "username",    "qos"  // 25
  };

  std::tuple<int32_t, task_db_id_t, int64_t, bool, std::string,   /*0-4*/
             double, int64_t, std::string, std::string, int32_t,  /*5-9*/
             int32_t, std::string, int32_t, int32_t, std::string, /*10-14*/
             int64_t, int64_t, int64_t, int64_t, int64_t,         /*15-19*/
             std::string, int32_t, int64_t, int64_t, std::string, /*20-24*/
             std::string, int32_t, std::string, std::string>
      values{// 0-4
             static_cast<int32_t>(task->TaskId()), task->TaskDbId(),
             absl::ToUnixSeconds(absl::Now()), false, task->account,
             // 5-9
             task->resources.allocatable_resource.cpu_count,
             static_cast<int64_t>(
                 task->resources.allocatable_resource.memory_bytes),
             task->name, task->env, static_cast<int32_t>(task->uid),
             // 10-14
             static_cast<int32_t>(task->Gid()), task->allocated_craneds_regex,
             static_cast<int32_t>(task->nodes_alloc), 0, task->partition_id,
             // 15-19
             0, 0, static_cast<int64_t>(task->StartTimeInUnixSecond()),
             static_cast<int64_t>(task->EndTimeInUnixSecond()), 0,
             // 20-24
             script, task->Status(), absl::ToInt64Seconds(task->time_limit), 0,
             task->cwd,
             // 25
             task->cmd_line, task->ExitCode(), task->Username(), task->qos};

  return DocumentConstructor_(fields, values);
}

MongodbClient::MongodbClient() {
  m_instance_ = std::make_unique<mongocxx::instance>();
  m_db_name_ = g_config.DbName;
  std::string authentication;

  if (!g_config.DbUser.empty()) {
    authentication =
        fmt::format("{}:{}@", g_config.DbUser, g_config.DbPassword);
  }

  m_connect_uri_ = fmt::format(
      "mongodb://{}{}:{}/?replicaSet={}&maxPoolSize=1000", authentication,
      g_config.DbHost, g_config.DbPort, g_config.DbRSName);
  CRANE_TRACE("Mongodb connect uri: {}", m_connect_uri_);
  m_wc_majority_.acknowledge_level(mongocxx::write_concern::level::k_majority);
  m_rc_local_.acknowledge_level(mongocxx::read_concern::level::k_local);
  m_rp_primary_.mode(mongocxx::read_preference::read_mode::k_primary);
}

}  // namespace Ctld

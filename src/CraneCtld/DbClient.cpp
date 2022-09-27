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

  auto* task_to_ctld_str = new std::string();
  task_to_ctld.SerializeToString(task_to_ctld_str);

  auto builder = bsoncxx::builder::stream::document{};
  bsoncxx::document::value doc_value =
      builder << "job_db_inx" << std::to_string(last_id + 1) << "mod_time"
              << std::to_string(mod_timestamp) << "deleted" << false
              << "account" << account << "cpus_req" << std::to_string(cpu)
              << "mem_req" << std::to_string(memory_bytes) << "job_name"
              << job_name << "env" << env << "id_job" << std::to_string(id_job)
              << "id_user" << std::to_string(id_user) << "id_group"
              << std::to_string(id_group) << "nodelist" << nodelist
              << "nodes_alloc" << std::to_string(nodes_alloc) << "node_inx"
              << node_inx << "partition_name" << partition_name << "priority"
              << std::to_string(priority) << "time_eligible" << 0
              << "time_start" << 0 << "time_end" << 0 << "time_suspended" << 0
              << "script" << script << "state" << std::to_string(state)
              << "timelimit" << std::to_string(timelimit) << "time_submit"
              << std::to_string(submit_timestamp) << "work_dir" << work_dir
              << "task_to_ctld" << *task_to_ctld_str
              << bsoncxx::builder::stream::finalize;

  if (m_dbInstance && m_client) {
    stdx::optional<result::insert_one> ret;
    ret = m_job_collection->insert_one(doc_value.view());

    if (ret == stdx::nullopt) {
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
    task.partition_name = view["partition_name"].get_string().value;
    task.start_time =
        absl::FromUnixSeconds(view["time_start"].get_int32().value);
    task.end_time = absl::FromUnixSeconds(view["time_end"].get_int32().value);

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

  bsoncxx::stdx::optional<mongocxx::result::update> update_result =
      m_job_collection->update_one(
          document{} << "job_db_inx" << std::to_string(job_db_inx)
                     << bsoncxx::builder::stream::finalize,
          document{} << "$set" << bsoncxx::builder::stream::open_document
                     << "mod_time" << std::to_string(timestamp) << field_name
                     << val << bsoncxx::builder::stream::close_document
                     << bsoncxx::builder::stream::finalize);
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
  bsoncxx::stdx::optional<mongocxx::result::update> update_result =
      m_job_collection->update_one(
          document{} << "job_db_inx" << std::to_string(job_db_inx)
                     << bsoncxx::builder::stream::finalize,
          doc_value.view());
  return true;
}

MongodbClient::MongodbResult MongodbClient::AddUser(
    const Ctld::User& new_user) {
  // Avoid duplicate insertion
  bsoncxx::stdx::optional<bsoncxx::document::value> find_result =
      m_user_collection->find_one(document{}
                                  << "uid" << std::to_string(new_user.uid)
                                  << bsoncxx::builder::stream::finalize);
  if (find_result) {
    if (!find_result->view()["deleted"].get_bool()) {
      return MongodbClient::MongodbResult{
          false, fmt::format("The user {} already exists in the database",
                             new_user.name)};
    }
  }

  if (!new_user.account.empty()) {
    // update the user's account's users_list
    bsoncxx::stdx::optional<mongocxx::result::update> update_result =
        m_account_collection->update_one(
            document{} << "name" << new_user.account
                       << bsoncxx::builder::stream::finalize,
            document{} << "$addToSet" << bsoncxx::builder::stream::open_document
                       << "users" << new_user.name
                       << bsoncxx::builder::stream::close_document
                       << bsoncxx::builder::stream::finalize);

    if (!update_result || !update_result->modified_count()) {
      return MongodbClient::MongodbResult{
          false, fmt::format("The account {} doesn't exist in the database",
                             new_user.account)};
    }
  } else {
    return MongodbClient::MongodbResult{
        false, fmt::format("Please specify the user's account")};
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

  if (m_dbInstance && m_client) {
    if (find_result && find_result->view()["deleted"].get_bool()) {
      stdx::optional<result::update> ret = m_user_collection->update_one(
          document{} << "name" << new_user.name
                     << bsoncxx::builder::stream::finalize,
          document{} << "$set" << doc_value.view()
                     << bsoncxx::builder::stream::finalize);

      if (ret != stdx::nullopt)
        return MongodbClient::MongodbResult{true};
      else
        return MongodbClient::MongodbResult{
            false, "can't update the deleted user to database"};
    } else {
      stdx::optional<result::insert_one> ret;
      ret = m_user_collection->insert_one(doc_value.view());

      if (ret != stdx::nullopt)
        return MongodbClient::MongodbResult{true};
      else
        return MongodbClient::MongodbResult{
            false, "can't insert the user to database"};
    }
  } else {
    return MongodbClient::MongodbResult{false, "database init failed"};
  }
}

MongodbClient::MongodbResult MongodbClient::AddAccount(
    const Ctld::Account& new_account) {
  // Avoid duplicate insertion
  bsoncxx::stdx::optional<bsoncxx::document::value> find_result =
      m_account_collection->find_one(document{}
                                     << "name" << new_account.name
                                     << bsoncxx::builder::stream::finalize);
  if (find_result) {
    if (!find_result->view()["deleted"].get_bool()) {
      return MongodbClient::MongodbResult{
          false, fmt::format("The account {} already exists in the database",
                             new_account.name)};
    }
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
      return MongodbClient::MongodbResult{
          false,
          fmt::format("The parent account {} doesn't exist in the database",
                      new_account.parent_account)};
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

  if (m_dbInstance && m_client) {
    if (find_result && find_result->view()["deleted"].get_bool()) {
      stdx::optional<result::update> ret = m_account_collection->update_one(
          document{} << "name" << new_account.name
                     << bsoncxx::builder::stream::finalize,
          document{} << "$set" << doc_value.view()
                     << bsoncxx::builder::stream::finalize);

      if (ret != stdx::nullopt)
        return MongodbClient::MongodbResult{true};
      else
        return MongodbClient::MongodbResult{
            false, "can't update the deleted account to database"};
    } else {
      stdx::optional<result::insert_one> ret;
      ret = m_account_collection->insert_one(doc_value.view());
      if (ret != stdx::nullopt)
        return MongodbClient::MongodbResult{true};
      else
        return MongodbClient::MongodbResult{
            false, "can't insert the account to database"};
    }
  } else {
    return MongodbClient::MongodbResult{false, "database init failed"};
  }
}

MongodbClient::MongodbResult MongodbClient::AddQos(const Ctld::Qos& new_qos) {
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

  return MongodbClient::MongodbResult{ret != stdx::nullopt};
}

MongodbClient::MongodbResult MongodbClient::DeleteEntity(
    MongodbClient::EntityType type, const std::string& name) {
  std::shared_ptr<mongocxx::collection> coll;
  Ctld::Account account;
  Ctld::User user;
  std::string parent_name, child_attribute_name;
  switch (type) {
    case MongodbClient::Account:
      coll = m_account_collection;
      child_attribute_name = "child_account";
      // check whether the account has child
      if (GetExistedAccountInfo(name, &account)) {
        if (!account.child_account.empty() || !account.users.empty()) {
          return MongodbClient::MongodbResult{
              false, "This account has child account or users"};
        }
        parent_name = account.parent_account;
      }
      break;
    case MongodbClient::User:
      coll = m_user_collection;
      child_attribute_name = "users";
      if (GetExistedUserInfo(name, &user)) {
        parent_name = user.account;
      }
      break;
    case MongodbClient::Qos:
      coll = m_qos_collection;
      break;
  }

  if (!parent_name.empty()) {
    // delete form the parent account's child_account_list
    bsoncxx::stdx::optional<mongocxx::result::update> update_result =
        m_account_collection->update_one(
            document{} << "name" << parent_name
                       << bsoncxx::builder::stream::finalize,
            document{} << "$pull" << bsoncxx::builder::stream::open_document
                       << child_attribute_name << name
                       << bsoncxx::builder::stream::close_document
                       << bsoncxx::builder::stream::finalize);

    if (!update_result || !update_result->modified_count()) {
      return MongodbClient::MongodbResult{
          false,
          fmt::format(
              "Can't delete {} form the parent account's child_account_list",
              name)};
    }
  }

  bsoncxx::stdx::optional<mongocxx::result::update> result = coll->update_one(
      document{} << "name" << name << bsoncxx::builder::stream::finalize,
      document{} << "$set" << bsoncxx::builder::stream::open_document
                 << "deleted" << true
                 << bsoncxx::builder::stream::close_document
                 << bsoncxx::builder::stream::finalize);

  if (!result) {
    return MongodbResult{false, "can't update the value of this Entities"};
  } else if (!result->modified_count()) {
    if (result->matched_count()) {
      return MongodbClient::MongodbResult{false, "Entities has been deleted"};
    } else {
      return MongodbClient::MongodbResult{
          false, "Entities doesn't exist in the database"};
    }
  } else {
    return MongodbClient::MongodbResult{true};
  }
}

bool MongodbClient::GetUserInfo(const std::string& name, Ctld::User* user) {
  bsoncxx::stdx::optional<bsoncxx::document::value> result =
      m_user_collection->find_one(
          document{} << "name" << name << bsoncxx::builder::stream::finalize);
  if (result) {
    bsoncxx::document::view user_view = result->view();
    user->deleted = user_view["deleted"].get_bool();

    user->uid =
        std::strtol(user_view["uid"].get_string().value.data(), nullptr, 10);

    user->name = user_view["name"].get_string().value;
    user->account = user_view["account"].get_string().value;
    user->admin_level =
        (Ctld::User::AdminLevel)user_view["admin_level"].get_int32().value;
    for (auto&& partition : user_view["allowed_partition"].get_array().value) {
      user->allowed_partition.emplace_back(partition.get_string().value);
    }
    return true;
  }
  return false;
}

/*
 * Get the user info form mongodb and deletion flag marked false
 */
bool MongodbClient::GetExistedUserInfo(const std::string& name,
                                       Ctld::User* user) {
  if (GetUserInfo(name, user) && !user->deleted) {
    return true;
  } else {
    return false;
  }
}

bool MongodbClient::GetAllUserInfo(std::list<Ctld::User>& user_list) {
  mongocxx::cursor cursor = m_user_collection->find({});
  for (auto view : cursor) {
    Ctld::User user;
    user.deleted = view["deleted"].get_bool();

    user.uid = std::strtol(view["uid"].get_string().value.data(), nullptr, 10);

    user.name = view["name"].get_string().value;
    user.account = view["account"].get_string().value;
    user.admin_level =
        (Ctld::User::AdminLevel)view["admin_level"].get_int32().value;
    for (auto&& partition : view["allowed_partition"].get_array().value) {
      user.allowed_partition.emplace_back(partition.get_string().value);
    }
    user_list.emplace_back(user);
  }
  return true;
}

bool MongodbClient::GetAccountInfo(const std::string& name,
                                   Ctld::Account* account) {
  bsoncxx::stdx::optional<bsoncxx::document::value> result =
      m_account_collection->find_one(
          document{} << "name" << name << bsoncxx::builder::stream::finalize);
  if (result) {
    bsoncxx::document::view account_view = result->view();
    account->deleted = account_view["deleted"].get_bool().value;
    if (account->deleted) return false;
    account->name = account_view["name"].get_string().value;
    account->description = account_view["description"].get_string().value;
    for (auto&& user : account_view["users"].get_array().value) {
      account->users.emplace_back(user.get_string().value);
    }
    for (auto&& acct : account_view["child_account"].get_array().value) {
      account->child_account.emplace_back(acct.get_string().value);
    }
    for (auto&& partition :
         account_view["allowed_partition"].get_array().value) {
      account->allowed_partition.emplace_back(partition.get_string().value);
    }
    account->parent_account = account_view["parent_account"].get_string().value;
    account->qos = account_view["qos"].get_string().value;
    return true;
  }
  return false;
}

bool MongodbClient::GetExistedAccountInfo(const std::string& name,
                                          Ctld::Account* account) {
  if (GetAccountInfo(name, account) && !account->deleted) {
    return true;
  } else {
    return false;
  }
}

bool MongodbClient::GetAllAccountInfo(std::list<Ctld::Account>& account_list) {
  mongocxx::cursor cursor = m_account_collection->find({});
  for (auto view : cursor) {
    Ctld::Account account;
    account.deleted = view["deleted"].get_bool().value;
    account.name = view["name"].get_string().value;
    account.description = view["description"].get_string().value;
    for (auto&& user : view["users"].get_array().value) {
      account.users.emplace_back(user.get_string().value);
    }
    for (auto&& acct : view["child_account"].get_array().value) {
      account.child_account.emplace_back(acct.get_string().value);
    }
    for (auto&& partition : view["allowed_partition"].get_array().value) {
      account.allowed_partition.emplace_back(partition.get_string().value);
    }
    account.parent_account = view["parent_account"].get_string().value;
    account.qos = view["qos"].get_string().value;
    account_list.emplace_back(account);
  }
  return true;
}

bool MongodbClient::GetQosInfo(const std::string& name, Ctld::Qos* qos) {
  bsoncxx::stdx::optional<bsoncxx::document::value> result =
      m_qos_collection->find_one(
          document{} << "name" << name << bsoncxx::builder::stream::finalize);
  if (result) {
    bsoncxx::document::view user_view = result->view();
    qos->name = user_view["name"].get_string().value;
    qos->description = user_view["description"].get_string().value;
    qos->priority = user_view["priority"].get_int32();
    qos->max_jobs_per_user = user_view["max_jobs_per_user"].get_int32();
    std::cout << bsoncxx::to_json(*result) << "\n";
    return true;
  }
  return false;
}

MongodbClient::MongodbResult MongodbClient::SetUser(
    const Ctld::User& new_user) {
  Ctld::User last_user;
  if (!GetExistedUserInfo(new_user.name, &last_user)) {
    return MongodbClient::MongodbResult{
        false, fmt::format("user {} not exist", new_user.name)};
  }
  auto builder = bsoncxx::builder::stream::document{};
  auto array_context = builder << "$set"
                               << bsoncxx::builder::stream::open_document;

  bool toChange = false;
  if (new_user.admin_level != last_user.admin_level) {
    array_context << "admin_level" << new_user.admin_level;
    toChange = true;
  }
  if (!new_user.account.empty() && new_user.account != last_user.account) {
    // update the user's account's users_list
    bsoncxx::stdx::optional<mongocxx::result::update> update_result =
        m_account_collection->update_one(
            document{} << "name" << new_user.account
                       << bsoncxx::builder::stream::finalize,
            document{} << "$addToSet" << bsoncxx::builder::stream::open_document
                       << "users" << new_user.name
                       << bsoncxx::builder::stream::close_document
                       << bsoncxx::builder::stream::finalize);

    if (!update_result || !update_result->modified_count()) {
      return MongodbClient::MongodbResult{
          false, fmt::format("The account {} doesn't exist in the database",
                             new_user.account)};
    }

    if (!last_user.account.empty()) {
      // delete form the parent account's child_user_list
      update_result = m_account_collection->update_one(
          document{} << "name" << last_user.account
                     << bsoncxx::builder::stream::finalize,
          document{} << "$pull" << bsoncxx::builder::stream::open_document
                     << "users" << last_user.name
                     << bsoncxx::builder::stream::close_document
                     << bsoncxx::builder::stream::finalize);

      if (!update_result || !update_result->modified_count()) {
        return MongodbClient::MongodbResult{
            false,
            fmt::format(
                "Can't delete {} form the parent account's child_user_list",
                last_user.name)};
      }
    }

    array_context << "account" << new_user.account;
    toChange = true;
  }
  if (toChange) {
    bsoncxx::stdx::optional<mongocxx::result::update> update_result =
        m_user_collection->update_one(
            document{} << "name" << new_user.name
                       << bsoncxx::builder::stream::finalize,
            array_context << bsoncxx::builder::stream::close_document
                          << bsoncxx::builder::stream::finalize);

    if (!update_result || !update_result->modified_count()) {
      return MongodbClient::MongodbResult{
          false,
          fmt::format("Can't update user {}'s information", new_user.name)};
    }
  }
  return MongodbClient::MongodbResult{true};
}

MongodbClient::MongodbResult MongodbClient::SetAccount(
    const Ctld::Account& new_account) {
  Ctld::Account last_account;
  if (!GetExistedAccountInfo(new_account.name, &last_account)) {
    return MongodbClient::MongodbResult{
        false, fmt::format("account {} not exist", new_account.name)};
  }
  auto builder = bsoncxx::builder::stream::document{};
  auto array_context = builder << "$set"
                               << bsoncxx::builder::stream::open_document;
  bool toChange = false;
  if (!new_account.description.empty() &&
      new_account.description != last_account.description) {
    array_context << "description" << new_account.description;
    toChange = true;
  }
  if (!new_account.qos.empty() && new_account.qos != last_account.qos) {
    array_context << "qos" << new_account.qos;
    toChange = true;
  }
  if (new_account.parent_account != last_account.parent_account) {
    if (!new_account.parent_account.empty()) {
      // update the parent account's child_account_list
      bsoncxx::stdx::optional<mongocxx::result::update> update_result =
          m_account_collection->update_one(
              document{} << "name" << new_account.parent_account
                         << bsoncxx::builder::stream::finalize,
              document{} << "$addToSet"
                         << bsoncxx::builder::stream::open_document
                         << "child_account" << new_account.name
                         << bsoncxx::builder::stream::close_document
                         << bsoncxx::builder::stream::finalize);

      if (!update_result || !update_result->modified_count()) {
        return MongodbClient::MongodbResult{
            false,
            fmt::format("The parent account {} doesn't exist in the database",
                        new_account.parent_account)};
      }
    }

    if (!last_account.parent_account.empty()) {
      // delete form the parent account's child_account_list
      bsoncxx::stdx::optional<mongocxx::result::update> update_result =
          m_account_collection->update_one(
              document{} << "name" << last_account.parent_account
                         << bsoncxx::builder::stream::finalize,
              document{} << "$pull" << bsoncxx::builder::stream::open_document
                         << "child_account" << last_account.name
                         << bsoncxx::builder::stream::close_document
                         << bsoncxx::builder::stream::finalize);

      if (!update_result || !update_result->modified_count()) {
        return MongodbClient::MongodbResult{
            false,
            fmt::format(
                "Can't delete {} form the parent account's child_account_list",
                last_account.name)};
      }
    }

    array_context << "parent_account" << new_account.parent_account;
    toChange = true;
  }
  if (toChange) {
    bsoncxx::stdx::optional<mongocxx::result::update> update_result =
        m_account_collection->update_one(
            document{} << "name" << new_account.name
                       << bsoncxx::builder::stream::finalize,
            array_context << bsoncxx::builder::stream::close_document
                          << bsoncxx::builder::stream::finalize);

    if (!update_result || !update_result->modified_count()) {
      return MongodbClient::MongodbResult{
          false, fmt::format("Can't update account {}'s information",
                             new_account.name)};
    }
  }
  return MongodbClient::MongodbResult{true};
}

std::list<std::string> MongodbClient::GetUserAllowedPartition(
    const std::string& name) {
  bsoncxx::stdx::optional<bsoncxx::document::value> result =
      m_user_collection->find_one(
          document{} << "name" << name << bsoncxx::builder::stream::finalize);
  std::list<std::string> allowed_partition;
  if (result && !result->view()["deleted"].get_bool()) {
    bsoncxx::document::view user_view = result->view();
    for (auto&& partition : user_view["allowed_partition"].get_array().value) {
      allowed_partition.emplace_back(partition.get_string().value);
    }
    if (allowed_partition.empty()) {
      std::string parent{user_view["account"].get_string().value};
      if (!parent.empty()) {
        allowed_partition = GetAccountAllowedPartition(parent);
      }
    }
  }
  return allowed_partition;
}

std::list<std::string> MongodbClient::GetAccountAllowedPartition(
    const std::string& name) {
  std::list<std::string> allowed_partition;
  std::string parent = name;
  while (allowed_partition.empty() && !parent.empty()) {
    Ctld::Account account;
    GetExistedAccountInfo(parent, &account);
    allowed_partition = account.allowed_partition;
    parent = account.parent_account;
  }
  return allowed_partition;
}

bool MongodbClient::SetUserAllowedPartition(
    const std::string& name, const std::list<std::string>& partitions,
    crane::grpc::ModifyEntityRequest::Type type) {
  if (!GetExistedUserInfo(name, new Ctld::User)) {
    return false;
  }

  std::list<std::string> change_partitions = GetUserAllowedPartition(name);
  switch (type) {
    case crane::grpc::ModifyEntityRequest_Type_Add:
      for (auto&& partition : partitions) {
        auto it = std::find(change_partitions.begin(), change_partitions.end(),
                            partition);
        if (it == change_partitions.end()) {
          change_partitions.emplace_back(partition);
        }
      }
      break;
    case crane::grpc::ModifyEntityRequest_Type_Delete:
      for (auto&& partition : partitions) {
        auto it = std::find(change_partitions.begin(), change_partitions.end(),
                            partition);
        if (it != change_partitions.end()) {
          change_partitions.erase(it);  // delete the partition
        }
      }
      break;
    case crane::grpc::ModifyEntityRequest_Type_Overwrite:
      change_partitions.assign(partitions.begin(), partitions.end());
      break;
    default:
      break;
  }

  // clear all
  m_user_collection->update_one(
      document{} << "name" << name << bsoncxx::builder::stream::finalize,
      document{} << "$set" << bsoncxx::builder::stream::open_document
                 << "allowed_partition" << bsoncxx::builder::stream::open_array
                 << bsoncxx::builder::stream::close_array
                 << bsoncxx::builder::stream::close_document
                 << bsoncxx::builder::stream::finalize);

  // insert the new list
  for (auto&& partition : change_partitions) {
    bsoncxx::stdx::optional<mongocxx::result::update> result =
        m_user_collection->update_one(
            document{} << "name" << name << bsoncxx::builder::stream::finalize,
            document{} << "$addToSet" << bsoncxx::builder::stream::open_document
                       << "allowed_partition" << partition
                       << bsoncxx::builder::stream::close_document
                       << bsoncxx::builder::stream::finalize);

    if (!result || !result->modified_count()) {
      return false;
    }
  }
  return true;
}

bool MongodbClient::SetAccountAllowedPartition(
    const std::string& name, const std::list<std::string>& partitions,
    crane::grpc::ModifyEntityRequest::Type type) {
  if (!GetExistedAccountInfo(name, new Ctld::Account)) {
    return false;
  }

  std::list<std::string> change_partitions = GetAccountAllowedPartition(name);
  switch (type) {
    case crane::grpc::ModifyEntityRequest_Type_Add:
      for (auto&& partition : partitions) {
        auto it = std::find(change_partitions.begin(), change_partitions.end(),
                            partition);
        if (it == change_partitions.end()) {
          change_partitions.emplace_back(partition);
        }
      }
      break;
    case crane::grpc::ModifyEntityRequest_Type_Delete:
      for (auto&& partition : partitions) {
        auto it = std::find(change_partitions.begin(), change_partitions.end(),
                            partition);
        if (it != change_partitions.end()) {
          change_partitions.erase(it);  // delete the partition
        }
      }
      break;
    case crane::grpc::ModifyEntityRequest_Type_Overwrite:
      change_partitions.assign(partitions.begin(), partitions.end());
      break;
    default:
      break;
  }

  // clear all
  m_account_collection->update_one(
      document{} << "name" << name << bsoncxx::builder::stream::finalize,
      document{} << "$set" << bsoncxx::builder::stream::open_document
                 << "allowed_partition" << bsoncxx::builder::stream::open_array
                 << bsoncxx::builder::stream::close_array
                 << bsoncxx::builder::stream::close_document
                 << bsoncxx::builder::stream::finalize);

  // insert the new list
  for (auto&& partition : change_partitions) {
    bsoncxx::stdx::optional<mongocxx::result::update> result =
        m_account_collection->update_one(
            document{} << "name" << name << bsoncxx::builder::stream::finalize,
            document{} << "$addToSet" << bsoncxx::builder::stream::open_document
                       << "allowed_partition" << partition
                       << bsoncxx::builder::stream::close_document
                       << bsoncxx::builder::stream::finalize);

    if (!result || !result->modified_count()) {
      return false;
    }
  }
  return true;
}

}  // namespace Ctld

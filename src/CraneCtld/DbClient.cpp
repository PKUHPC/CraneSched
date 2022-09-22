#include "DbClient.h"

#include <absl/strings/str_join.h>

#include <utility>

namespace Ctld {

MariadbClient::~MariadbClient() {
  if (m_conn) {
    mysql_close(m_conn);
  }
}

bool MariadbClient::Init() {
  m_conn = mysql_init(nullptr);
  if (m_conn == nullptr) return false;

  // Reconnect when Mariadb closes connection after a long idle time (8 hours).
  my_bool reconnect = 1;
  mysql_options(m_conn, MYSQL_OPT_RECONNECT, &reconnect);

  return true;
}

void MariadbClient::SetUserAndPwd(const std::string& username,
                                  const std::string& password) {
  m_user = username;
  m_psw = password;
}

bool MariadbClient::Connect() {
  if (mysql_real_connect(m_conn, "127.0.0.1", m_user.c_str(), m_psw.c_str(),
                         nullptr, 3306, nullptr, 0) == nullptr) {
    PrintError_("Cannot connect to database");
    return false;
  }

  if (mysql_query(m_conn,
                  fmt::format("CREATE DATABASE IF NOT EXISTS {};", m_db_name)
                      .c_str())) {
    PrintError_(fmt::format("Cannot check the existence of {}", m_db_name));
    return false;
  }

  if (mysql_select_db(m_conn, m_db_name.c_str()) != 0) {
    PrintError_(fmt::format("Cannot select {}", m_db_name));
    return false;
  }

  if (mysql_query(
          m_conn,
          "CREATE TABLE IF NOT EXISTS job_table("
          "job_db_inx    bigint unsigned not null auto_increment primary key,"
          "mod_time        bigint unsigned default 0 not null,"
          "deleted         tinyint         default 0 not null,"
          "account         tinytext,"
          "cpus_req        int unsigned              not null,"
          "mem_req         bigint unsigned default 0 not null,"
          "job_name        tinytext                  not null,"
          "env             text,"
          "id_job          int unsigned              not null,"
          "id_user         int unsigned              not null,"
          "id_group        int unsigned              not null,"
          "nodelist        text,"
          "nodes_alloc     int unsigned              not null,"
          "node_inx        text,"
          "partition_name  tinytext                  not null,"
          "priority        int unsigned              not null,"
          "time_eligible   bigint unsigned default 0 not null,"
          "time_start      bigint unsigned default 0 not null,"
          "time_end        bigint unsigned default 0 not null,"
          "time_suspended  bigint unsigned default 0 not null,"
          "script          text                      not null default '',"
          "state           int unsigned              not null,"
          "timelimit       int unsigned    default 0 not null,"
          "time_submit     bigint unsigned default 0 not null,"
          "work_dir        text                      not null default '',"
          "submit_line     text,"
          "task_to_ctld    blob                      not null"
          ");")) {
    PrintError_("Cannot check the existence of job_table");
    return false;
  }

  return true;
}

bool MariadbClient::GetMaxExistingJobId(uint64_t* job_id) {
  if (mysql_query(m_conn,
                  "SELECT COALESCE(MAX(job_db_inx), 0) FROM job_table;")) {
    PrintError_("Cannot get the max id");
    return false;
  }

  MYSQL_RES* result = mysql_store_result(m_conn);
  if (result == nullptr) {
    PrintError_("Error in getting the max job id result");
    return false;
  }

  MYSQL_ROW row = mysql_fetch_row(result);
  unsigned long* lengths = mysql_fetch_lengths(result);

  if (lengths == nullptr) {
    PrintError_("Error in fetching rows of max id result");
    mysql_free_result(result);
    return false;
  }

  *job_id = strtoul(row[0], nullptr, 10);

  mysql_free_result(result);
  return true;
}

bool MariadbClient::GetLastInsertId(uint64_t* id) {
  if (mysql_query(m_conn, "SELECT LAST_INSERT_ID();")) {
    PrintError_("Cannot get last insert id");
    return false;
  }

  MYSQL_RES* result = mysql_store_result(m_conn);
  if (result == nullptr) {
    PrintError_("Error in getting the max job id result");
    return false;
  }

  MYSQL_ROW row = mysql_fetch_row(result);
  unsigned long* lengths = mysql_fetch_lengths(result);

  if (lengths == nullptr) {
    PrintError_("Error in fetching rows of max id result");
    mysql_free_result(result);
    return false;
  }

  *id = strtoul(row[0], nullptr, 10);

  mysql_free_result(result);
  return true;
}

bool MariadbClient::UpdateJobRecordField(uint64_t job_db_inx,
                                         const std::string& field_name,
                                         const std::string& val) {
  std::string query = fmt::format(
      "UPDATE job_table SET {} = '{}', mod_time = UNIX_TIMESTAMP() WHERE "
      "job_db_inx = {};",
      field_name, val, job_db_inx);

  if (mysql_query(m_conn, query.c_str())) {
    PrintError_("Failed to update job record");
    return false;
  }

  return true;
}

bool MariadbClient::UpdateJobRecordFields(
    uint64_t job_db_inx, const std::list<std::string>& field_names,
    const std::list<std::string>& values) {
  CRANE_ASSERT(field_names.size() == values.size() && !field_names.empty());

  std::vector<std::string> kvs;
  for (auto it_k = field_names.begin(), it_v = values.begin();
       it_k != field_names.end() && it_v != values.end(); ++it_k, ++it_v) {
    std::string piece = fmt::format("{} = '{}'", *it_k, *it_v);
    kvs.emplace_back(std::move(piece));
  }

  std::string formatter = absl::StrJoin(kvs, ", ");

  std::string query = fmt::format(
      "UPDATE job_table SET {}, mod_time = UNIX_TIMESTAMP() "
      "WHERE job_db_inx = {};",
      formatter, job_db_inx);

  if (mysql_query(m_conn, query.c_str())) {
    PrintError_("Failed to update job record");
    return false;
  }

  return true;
}

bool MariadbClient::FetchJobRecordsWithStates(
    std::list<TaskInCtld>* task_list,
    const std::list<crane::grpc::TaskStatus>& states) {
  std::vector<std::string> state_piece;
  for (auto state : states) {
    state_piece.emplace_back(fmt::format("state = {}", state));
  }
  std::string state_str = absl::StrJoin(state_piece, " or ");

  std::string query =
      fmt::format("SELECT * FROM job_table WHERE {};", state_str);

  if (mysql_query(m_conn, query.c_str())) {
    PrintError_("Failed to fetch job record");
    return false;
  }

  MYSQL_RES* result = mysql_store_result(m_conn);
  if (result == nullptr) {
    PrintError_("Error in getting `fetch job` result");
    return false;
  }

  uint32_t num_fields = mysql_num_fields(result);

  MYSQL_ROW row;
  // 0  job_db_inx    mod_time       deleted       account     cpus_req
  // 5  mem_req       job_name       env           id_job      id_user
  // 10 id_group      nodelist       nodes_alloc   node_inx    partition_name
  // 15 priority      time_eligible  time_start    time_end    time_suspended
  // 20 script        state          timelimit     time_submit work_dir
  // 25 submit_line   task_to_ctld

  while ((row = mysql_fetch_row(result))) {
    size_t* lengths = mysql_fetch_lengths(result);

    Ctld::TaskInCtld task;
    task.job_db_inx = std::strtoul(row[0], nullptr, 10);
    task.resources.allocatable_resource.cpu_count =
        std::strtoul(row[4], nullptr, 10);
    task.resources.allocatable_resource.memory_bytes =
        task.resources.allocatable_resource.memory_sw_bytes =
            std::strtoul(row[5], nullptr, 10);
    task.name = row[6];
    task.env = row[7];
    task.task_id = std::strtoul(row[8], nullptr, 10);
    task.uid = std::strtoul(row[9], nullptr, 10);
    task.gid = std::strtoul(row[10], nullptr, 10);
    if (row[11]) task.allocated_craneds_regex = row[11];
    task.partition_name = row[14];
    task.start_time = absl::FromUnixSeconds(std::strtol(row[17], nullptr, 10));
    task.end_time = absl::FromUnixSeconds(std::strtol(row[18], nullptr, 10));

    task.meta = Ctld::BatchMetaInTask{};
    auto& batch_meta = std::get<Ctld::BatchMetaInTask>(task.meta);
    batch_meta.sh_script = row[20];
    task.status = crane::grpc::TaskStatus(std::stoi(row[21]));
    task.time_limit = absl::Seconds(std::strtol(row[22], nullptr, 10));
    task.cwd = row[24];

    if (row[25]) task.cmd_line = row[25];

    bool ok = task.task_to_ctld.ParseFromArray(row[26], lengths[26]);

    task_list->emplace_back(std::move(task));
  }

  mysql_free_result(result);
  return true;
}

bool MariadbClient::InsertJob(
    uint64_t* job_db_inx, uint64_t mod_timestamp, const std::string& account,
    uint32_t cpu, uint64_t memory_bytes, const std::string& job_name,
    const std::string& env, uint32_t id_job, uid_t id_user, uid_t id_group,
    const std::string& nodelist, uint32_t nodes_alloc,
    const std::string& node_inx, const std::string& partition_name,
    uint32_t priority, uint64_t submit_timestamp, const std::string& script,
    uint32_t state, uint32_t timelimit, const std::string& work_dir,
    const crane::grpc::TaskToCtld& task_to_ctld) {
  size_t blob_size = task_to_ctld.ByteSizeLong();
  constexpr size_t blob_max_size = 8192;

  static char blob[blob_max_size];
  static char query[blob_max_size * 2];
  task_to_ctld.SerializeToArray(blob, blob_max_size);

  std::string query_head = fmt::format(
      "INSERT INTO job_table("
      "mod_time, deleted, account, cpus_req, mem_req, job_name, env, "
      "id_job, id_user, id_group, nodelist, nodes_alloc, node_inx, "
      "partition_name, priority, time_submit, script, state, timelimit, "
      " work_dir, task_to_ctld) "
      " VALUES({}, 0, '{}', {}, {}, '{}', '{}', {}, {}, {}, "
      "'{}', {}, '{}', '{}', {}, {}, '{}', {}, {}, "
      "'{}', '",
      mod_timestamp, account, cpu, memory_bytes, job_name, env, id_job, id_user,
      id_group, nodelist, nodes_alloc, node_inx, partition_name, priority,
      submit_timestamp, script, state, timelimit, work_dir);
  char* query_ptr = std::copy(query_head.c_str(),
                              query_head.c_str() + query_head.size(), query);
  size_t escaped_size =
      mysql_real_escape_string(m_conn, query_ptr, blob, blob_size);
  query_ptr += escaped_size;

  const char query_end[] = "')";
  query_ptr =
      std::copy(query_end, query_end + sizeof(query_end) - 1, query_ptr);

  if (mysql_real_query(m_conn, query, query_ptr - query)) {
    PrintError_("Failed to insert job record");
    return false;
  }

  uint64_t last_id;
  if (!GetLastInsertId(&last_id)) {
    PrintError_("Failed to GetLastInsertId");
    return false;
  }
  *job_db_inx = last_id;

  return true;
}

MongodbClient::~MongodbClient() {
  delete m_dbInstance;
  delete m_client;
}

bool MongodbClient::Connect() {
  // default port 27017
  mongocxx::uri uri{fmt::format("mongodb://{}:{}@{}:{}/{}",
                                g_config.MongodbUser, g_config.MongodbPassword,
                                g_config.MongodbHost, g_config.MongodbPort,
                                m_db_name)};
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

  if (!m_database->has_collection(m_job_collection_name)) {
    m_database->create_collection(m_job_collection_name);
  }
  m_job_collection =
      new mongocxx::collection(m_database->collection(m_job_collection_name));

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

  if (!m_account_collection || !m_user_collection || !m_qos_collection ||
      !m_job_collection) {
    CRANE_ERROR("Mongodb Error: can't get instance of {} tables", m_db_name);
    std::exit(1);
  }
}

bool MongodbClient::GetMaxExistingJobId(uint64_t* job_id) {
  mongocxx::cursor cursor = m_job_collection->find({});
  *job_id = 0;
  if (cursor.begin() == cursor.end()) return false;
  for (auto view : cursor) {
    int id = view["id_job"].get_int32().value;
    if (id > *job_id) *job_id = id;
  }
  return true;
}

bool MongodbClient::GetLastInsertId(uint64_t* id) {
  *id = m_job_collection->count_documents({});
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

  //  size_t blob_size = task_to_ctld.ByteSizeLong();
  //  constexpr size_t blob_max_size = 8192;
  //
  //  static char blob[blob_max_size];
  //  static char query[blob_max_size * 2];
  //  task_to_ctld.SerializeToArray(blob, blob_max_size);

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
  auto array_context = document{} << "state"
                                  << bsoncxx::builder::stream::open_array;

  for (auto state : states) {
    array_context << state;
  }
  bsoncxx::document::value doc_value = array_context
                                       << bsoncxx::builder::stream::close_array
                                       << bsoncxx::builder::stream::finalize;

  mongocxx::cursor cursor = m_job_collection->find({doc_value.view()});

  // 0  job_db_inx    mod_time       deleted       account     cpus_req
  // 5  mem_req       job_name       env           id_job      id_user
  // 10 id_group      nodelist       nodes_alloc   node_inx    partition_name
  // 15 priority      time_eligible  time_start    time_end    time_suspended
  // 20 script        state          timelimit     time_submit work_dir
  // 25 submit_line   task_to_ctld

  for (auto view : cursor) {
    Ctld::TaskInCtld task;
    task.job_db_inx =
        std::stoi(std::string(view["job_db_inx"].get_utf8().value));
    task.resources.allocatable_resource.cpu_count =
        std::stoi(std::string(view["cpus_req"].get_utf8().value));

    task.resources.allocatable_resource.memory_bytes =
        task.resources.allocatable_resource.memory_sw_bytes =
            std::stoi(std::string(view["mem_req"].get_utf8().value));
    task.name = view["job_name"].get_utf8().value;
    task.env = view["env"].get_utf8().value;
    task.task_id = std::stoi(std::string(view["id_job"].get_utf8().value));
    task.uid = std::stoi(std::string(view["id_user"].get_utf8().value));
    task.gid = std::stoi(std::string(view["id_group"].get_utf8().value));
    task.partition_name = view["partition_name"].get_utf8().value;
    task.start_time = absl::FromUnixSeconds(
        std::stoi(std::string(view["time_start"].get_utf8().value)));
    task.end_time = absl::FromUnixSeconds(
        std::stoi(std::string(view["time_end"].get_utf8().value)));

    task.meta = Ctld::BatchMetaInTask{};
    auto& batch_meta = std::get<Ctld::BatchMetaInTask>(task.meta);
    batch_meta.sh_script = view["script"].get_utf8().value;
    task.status = crane::grpc::TaskStatus(
        std::stoi(std::string(view["state"].get_utf8().value)));
    task.time_limit = absl::Seconds(
        std::stoi(std::string(view["timelimit"].get_utf8().value)));
    task.cwd = view["work_dir"].get_utf8().value;
    task.cmd_line = view["submit_line"].get_utf8().value;

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
          document{} << "set" << bsoncxx::builder::stream::open_document
                     << "mod_time" << std::to_string(timestamp) << field_name
                     << val << bsoncxx::builder::stream::close_document
                     << bsoncxx::builder::stream::finalize);
  return true;
}

bool MongodbClient::UpdateJobRecordFields(
    uint64_t job_db_inx, const std::list<std::string>& field_names,
    const std::list<std::string>& values) {
  CRANE_ASSERT(field_names.size() == values.size() && !field_names.empty());

  auto array_context = document{} << "set"
                                  << bsoncxx::builder::stream::open_array;

  for (auto it_k = field_names.begin(), it_v = values.begin();
       it_k != field_names.end() && it_v != values.end(); ++it_k, ++it_v) {
    array_context << *it_k << *it_v;
  }

  bsoncxx::document::value doc_value = array_context
                                       << bsoncxx::builder::stream::close_array
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
  mongocxx::collection* coll;
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

#warning Fix This!
    user->uid = std::stoi(std::string(user_view["uid"].get_utf8().value));

    user->name = user_view["name"].get_utf8().value;
    user->account = user_view["account"].get_utf8().value;
    user->admin_level =
        (Ctld::User::AdminLevel)user_view["admin_level"].get_int32().value;
    for (auto&& partition : user_view["allowed_partition"].get_array().value) {
      user->allowed_partition.emplace_back(partition.get_utf8().value);
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

#warning Fix This!
    user.uid = std::stoi(std::string(view["uid"].get_utf8().value));

    user.name = view["name"].get_utf8().value;
    user.account = view["account"].get_utf8().value;
    user.admin_level =
        (Ctld::User::AdminLevel)view["admin_level"].get_int32().value;
    for (auto&& partition : view["allowed_partition"].get_array().value) {
      user.allowed_partition.emplace_back(partition.get_utf8().value);
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
    account.name = view["name"].get_utf8().value;
    account.description = view["description"].get_utf8().value;
    for (auto&& user : view["users"].get_array().value) {
      account.users.emplace_back(user.get_utf8().value);
    }
    for (auto&& acct : view["child_account"].get_array().value) {
      account.child_account.emplace_back(acct.get_utf8().value);
    }
    for (auto&& partition : view["allowed_partition"].get_array().value) {
      account.allowed_partition.emplace_back(partition.get_utf8().value);
    }
    account.parent_account = view["parent_account"].get_utf8().value;
    account.qos = view["qos"].get_utf8().value;
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
    qos->name = user_view["name"].get_utf8().value;
    qos->description = user_view["description"].get_utf8().value;
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
      allowed_partition.emplace_back(partition.get_utf8().value);
    }
    if (allowed_partition.empty()) {
      std::string parent{user_view["account"].get_utf8().value};
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

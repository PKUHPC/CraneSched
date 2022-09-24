#include <absl/strings/str_join.h>
#include <gtest/gtest.h>
#include <mysql.h>
#include <spdlog/fmt/fmt.h>

#include "CtldPublicDefs.h"

const char* mysql_user = "";
const char* mysql_password = "";

class MariadbClient {
 public:
  MariadbClient() = default;

  ~MariadbClient() {
    if (m_conn) {
      mysql_close(m_conn);
    }
  }

  bool Init() {
    m_conn = mysql_init(nullptr);
    if (m_conn == nullptr) return false;

    // Reconnect when Mariadb closes connection after a long idle time (8
    // hours).
    my_bool reconnect = 1;
    mysql_options(m_conn, MYSQL_OPT_RECONNECT, &reconnect);

    return true;
  }

  bool Connect(const std::string& username, const std::string& password) {
    if (mysql_real_connect(m_conn, "127.0.0.1", username.c_str(),
                           password.c_str(), nullptr, 3306, nullptr,
                           0) == nullptr) {
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

  bool GetMaxExistingJobId(uint64_t* job_id) {
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

  bool GetLastInsertId(uint64_t* id) {
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
        mod_timestamp, account, cpu, memory_bytes, job_name, env, id_job,
        id_user, id_group, nodelist, nodes_alloc, node_inx, partition_name,
        priority, submit_timestamp, script, state, timelimit, work_dir);
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

  bool UpdateJobRecordField(uint64_t job_db_inx, const std::string& field_name,
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

  bool FetchJobRecordsWithStates(
      std::list<Ctld::TaskInCtld>* task_list,
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
      task.start_time =
          absl::FromUnixSeconds(std::strtol(row[17], nullptr, 10));
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

 private:
  void PrintError_(const std::string& msg) {
    CRANE_ERROR("{}: {}\n", msg, mysql_error(m_conn));
  }

  void PrintError_(const char* msg) {
    CRANE_ERROR("{}: {}\n", msg, mysql_error(m_conn));
  }

  MYSQL* m_conn{nullptr};
  const std::string m_db_name{"crane_db"};
};

TEST(MariadbConnector, Simple) {
  MariadbClient client;
  ASSERT_TRUE(client.Init());
  ASSERT_TRUE(client.Connect(mysql_user, mysql_password));

  crane::grpc::TaskToCtld task_to_ctld;
  task_to_ctld.set_name("riley_job_2");
  task_to_ctld.set_uid(1000);
  task_to_ctld.set_type(crane::grpc::Batch);
  task_to_ctld.set_ntasks_per_node(2);
  task_to_ctld.set_cmd_line("cmd_line");
  task_to_ctld.set_cwd("cwd");
  task_to_ctld.mutable_time_limit()->set_seconds(360);
  task_to_ctld.mutable_batch_meta()->set_sh_script("#sbatch");

  uint64_t job_id;
  uint64_t job_db_inx;
  ASSERT_TRUE(client.GetMaxExistingJobId(&job_id));
  ASSERT_TRUE(client.InsertJob(
      &job_db_inx, 0, std::string("Riley"), 2, 20480000,
      std::string("test_job"), std::string("PATH=XXXX"), 3, 1000, 1000,
      std::string("1"), 1, std::string("1"), std::string("CPU"), 0, 12312321,
      std::string("script"), 0, 1000, std::string("/"), task_to_ctld));

  ASSERT_TRUE(client.UpdateJobRecordField(job_db_inx, std::string("env"),
                                          std::string("PATH=ABDASDAS")));

  std::list<Ctld::TaskInCtld> task_list;
  client.FetchJobRecordsWithStates(
      &task_list,
      {crane::grpc::TaskStatus::Pending, crane::grpc::TaskStatus::Running});
  GTEST_LOG_(INFO) << "End of Test\n";
}
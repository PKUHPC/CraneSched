#pragma once

#include <mysql.h>
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

using namespace mongocxx;
using bsoncxx::builder::stream::document;

namespace Ctld {

class MongodbClient {
 public:
  enum EntityType {
    Account = 0,
    User = 1,
    Qos = 2,
  };
  struct MongodbResult {
    bool ok{false};
    std::optional<std::string> reason;
  };
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
  MongodbResult AddUser(const Ctld::User& new_user);
  MongodbResult AddAccount(const Ctld::Account& new_account);
  MongodbResult AddQos(const Ctld::Qos& new_qos);

  MongodbResult DeleteEntity(EntityType type, const std::string& name);

  bool GetUserInfo(const std::string& name, Ctld::User* user);
  bool GetExistedUserInfo(const std::string& name, Ctld::User* user);
  bool GetAllUserInfo(std::list<Ctld::User>& user_list);
  bool GetAccountInfo(const std::string& name, Ctld::Account* account);
  bool GetExistedAccountInfo(const std::string& name, Ctld::Account* account);
  bool GetAllAccountInfo(std::list<Ctld::Account>& account_list);
  bool GetQosInfo(const std::string& name, Ctld::Qos* qos);

  MongodbResult SetUser(const Ctld::User& new_user);
  MongodbResult SetAccount(const Ctld::Account& new_account);

  std::list<std::string> GetUserAllowedPartition(const std::string& name);
  std::list<std::string> GetAccountAllowedPartition(const std::string& name);

  bool SetUserAllowedPartition(const std::string& name,
                               const std::list<std::string>& partitions,
                               crane::grpc::ModifyEntityRequest::Type type);
  bool SetAccountAllowedPartition(const std::string& name,
                                  const std::list<std::string>& partitions,
                                  crane::grpc::ModifyEntityRequest::Type type);

 private:
  static void PrintError_(const char* msg) {
    CRANE_ERROR("MongodbError: {}\n", msg);
  }

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

}  // namespace Ctld

inline std::unique_ptr<Ctld::MongodbClient> g_db_client;
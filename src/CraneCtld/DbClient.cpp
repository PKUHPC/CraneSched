/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

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
    qos.max_jobs_per_user =
        std::numeric_limits<decltype(qos.max_jobs_per_user)>::max();
    qos.max_running_tasks_per_user =
        std::numeric_limits<decltype(qos.max_running_tasks_per_user)>::max();
    qos.max_time_limit_per_task = absl::Seconds(kTaskMaxTimeLimitSec);
    qos.max_cpus_per_user =
        std::numeric_limits<decltype(qos.max_cpus_per_user)>::max();
    qos.max_cpus_per_account =
        std::numeric_limits<decltype(qos.max_cpus_per_account)>::max();
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
    root_user.admin_level = User::Root;
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
  CRANE_LOGGER_ERROR(m_logger_, "Failed to insert in-memory TaskInCtld.");
  return false;
}

bool MongodbClient::InsertJob(TaskInCtld* task) {
  document doc = TaskInCtldToDocument_(task);

  bsoncxx::stdx::optional<mongocxx::result::insert_one> ret =
      (*GetClient_())[m_db_name_][m_task_collection_name_].insert_one(
          *GetSession_(), doc.view());

  if (ret != bsoncxx::stdx::nullopt) return true;

  CRANE_LOGGER_ERROR(m_logger_, "Failed to insert in-memory TaskInCtld.");
  return false;
}

bool MongodbClient::InsertJobs(const std::vector<TaskInCtld*>& tasks) {
  if (tasks.empty()) return false;
  std::vector<bsoncxx::document::value> documents;

  for (const auto& task : tasks) {
    document doc = TaskInCtldToDocument_(task);
    documents.push_back(doc.extract());
  }

  mongocxx::options::insert insert_options;
  insert_options.ordered(false);  // unordered to speed up the operation

  bsoncxx::stdx::optional<mongocxx::result::insert_many> ret =
      (*GetClient_())[m_db_name_][m_task_collection_name_].insert_many(
          *GetSession_(), documents, insert_options);

  if (ret != bsoncxx::stdx::nullopt && ret->inserted_count() == tasks.size())
    return true;

  CRANE_LOGGER_ERROR(m_logger_, "Failed to insert in-memory TaskInCtld.");
  return false;
}

bool MongodbClient::FetchJobRecords(
    const crane::grpc::QueryTasksInfoRequest* request,
    crane::grpc::QueryTasksInfoReply* response, size_t limit) {
  auto* task_list = response->mutable_task_info_list();

  document filter;

  bool has_submit_time_interval = request->has_filter_submit_time_interval();
  if (has_submit_time_interval) {
    const auto& interval = request->filter_submit_time_interval();
    filter.append(kvp("time_submit", [&interval](sub_document time_submit_doc) {
      if (interval.has_lower_bound()) {
        time_submit_doc.append(kvp("$gte", interval.lower_bound().seconds()));
      }
      if (interval.has_upper_bound()) {
        time_submit_doc.append(kvp("$lte", interval.upper_bound().seconds()));
      }
    }));
  }

  bool has_start_time_interval = request->has_filter_start_time_interval();
  if (has_start_time_interval) {
    const auto& interval = request->filter_start_time_interval();
    filter.append(kvp("time_start", [&interval](sub_document time_start_doc) {
      if (interval.has_lower_bound()) {
        time_start_doc.append(kvp("$gte", interval.lower_bound().seconds()));
      }
      if (interval.has_upper_bound()) {
        time_start_doc.append(kvp("$lte", interval.upper_bound().seconds()));
      }
    }));
  }

  bool has_end_time_interval = request->has_filter_end_time_interval();
  if (has_end_time_interval) {
    const auto& interval = request->filter_end_time_interval();
    filter.append(kvp("time_end", [&interval](sub_document time_end_doc) {
      if (interval.has_lower_bound()) {
        time_end_doc.append(kvp("$gte", interval.lower_bound().seconds()));
      }
      if (interval.has_upper_bound()) {
        time_end_doc.append(kvp("$lte", interval.upper_bound().seconds()));
      }
    }));
  }

  bool has_accounts_constraint = !request->filter_accounts().empty();
  if (has_accounts_constraint) {
    filter.append(kvp("account", [&request](sub_document account_doc) {
      array account_array;
      for (const auto& account : request->filter_accounts()) {
        account_array.append(account);
      }
      account_doc.append(kvp("$in", account_array));
    }));
  }

  bool has_users_constraint = !request->filter_users().empty();
  if (has_users_constraint) {
    filter.append(kvp("username", [&request](sub_document user_doc) {
      array user_array;
      for (const auto& user : request->filter_users()) {
        user_array.append(user);
      }
      user_doc.append(kvp("$in", user_array));
    }));
  }

  bool has_task_names_constraint = !request->filter_task_names().empty();
  if (has_task_names_constraint) {
    filter.append(kvp("task_name", [&request](sub_document task_name_doc) {
      array task_name_array;
      for (const auto& task_name : request->filter_task_names()) {
        task_name_array.append(task_name);
      }
      task_name_doc.append(kvp("$in", task_name_array));
    }));
  }

  bool has_qos_constraint = !request->filter_qos().empty();
  if (has_qos_constraint) {
    filter.append(kvp("qos", [&request](sub_document qos_doc) {
      array qos_array;
      for (const auto& qos : request->filter_qos()) {
        qos_array.append(qos);
      }
      qos_doc.append(kvp("$in", qos_array));
    }));
  }

  bool has_partitions_constraint = !request->filter_partitions().empty();
  if (has_partitions_constraint) {
    filter.append(kvp("partition_name", [&request](sub_document partition_doc) {
      array partition_array;
      for (const auto& partition : request->filter_partitions()) {
        partition_array.append(partition);
      }
      partition_doc.append(kvp("$in", partition_array));
    }));
  }

  bool has_task_ids_constraint = !request->filter_task_ids().empty();
  if (has_task_ids_constraint) {
    filter.append(kvp("task_id", [&request](sub_document task_id_doc) {
      array task_id_array;
      for (const auto& task_id : request->filter_task_ids()) {
        task_id_array.append(static_cast<std::int32_t>(task_id));
      }
      task_id_doc.append(kvp("$in", task_id_array));
    }));
  }

  bool has_task_status_constraint = !request->filter_task_states().empty();
  if (has_task_status_constraint) {
    filter.append(kvp("state", [&request](sub_document state_doc) {
      array state_array;
      for (const auto& state : request->filter_task_states()) {
        state_array.append(state);
      }
      state_doc.append(kvp("$in", state_array));
    }));
  }

  mongocxx::options::find option;
  option = option.limit(limit);

  document sort_doc;
  sort_doc.append(kvp("task_db_id", -1));
  option = option.sort(sort_doc.view());

  mongocxx::cursor cursor =
      (*GetClient_())[m_db_name_][m_task_collection_name_].find(filter.view(),
                                                                option);

  // 0  task_id       task_db_id     mod_time       deleted       account
  // 5  cpus_req      mem_req        task_name      env           id_user
  // 10 id_group      nodelist       nodes_alloc   node_inx    partition_name
  // 15 priority      time_eligible  time_start    time_end    time_suspended
  // 20 script        state          timelimit     time_submit work_dir
  // 25 submit_line   exit_code      username       qos        get_user_env
  // 30 type          extra_attr     reservation    exclusive  cpus_alloc
  // 35 mem_alloc     device_map     container

  try {
    for (auto view : cursor) {
      auto* task = task_list->Add();

      task->set_task_id(view["task_id"].get_int32().value);

      task->set_node_num(view["nodes_alloc"].get_int32().value);

      task->set_account(view["account"].get_string().value.data());
      task->set_username(view["username"].get_string().value.data());

      auto* mutable_req_res_view = task->mutable_req_res_view();
      auto* mutable_req_alloc_res =
          mutable_req_res_view->mutable_allocatable_res();
      mutable_req_alloc_res->set_cpu_core_limit(
          view["cpus_req"].get_double().value);
      mutable_req_alloc_res->set_memory_limit_bytes(
          view["mem_req"].get_int64().value);
      mutable_req_alloc_res->set_memory_sw_limit_bytes(
          view["mem_req"].get_int64().value);

      auto* mutable_allocated_res_view = task->mutable_allocated_res_view();
      auto* mutable_allocated_alloc_res =
          mutable_allocated_res_view->mutable_allocatable_res();
      mutable_allocated_alloc_res->set_cpu_core_limit(
          view["cpus_alloc"].get_double().value);
      mutable_allocated_alloc_res->set_memory_limit_bytes(
          view["mem_alloc"].get_int64().value);
      mutable_allocated_alloc_res->set_memory_sw_limit_bytes(
          view["mem_alloc"].get_int64().value);
      auto* device_map_ptr = mutable_allocated_res_view->mutable_device_map();
      *device_map_ptr = ToGrpcDeviceMap(BsonToDeviceMap(view));
      task->set_name(std::string(view["task_name"].get_string().value));
      task->set_qos(std::string(view["qos"].get_string().value));
      task->set_uid(view["id_user"].get_int32().value);
      task->set_gid(view["id_group"].get_int32().value);
      task->set_craned_list(view["nodelist"].get_string().value.data());
      task->set_partition(
          std::string(view["partition_name"].get_string().value));

      task->mutable_start_time()->set_seconds(
          view["time_start"].get_int64().value);
      task->mutable_end_time()->set_seconds(view["time_end"].get_int64().value);

      task->set_status(static_cast<crane::grpc::TaskStatus>(
          view["state"].get_int32().value));
      task->mutable_time_limit()->set_seconds(
          view["timelimit"].get_int64().value);
      task->mutable_submit_time()->set_seconds(
          view["time_submit"].get_int64().value);
      task->set_cwd(std::string(view["work_dir"].get_string().value));
      if (view["submit_line"])
        task->set_cmd_line(std::string(view["submit_line"].get_string().value));
      task->set_exit_code(view["exit_code"].get_int32().value);

      task->set_type((crane::grpc::TaskType)view["type"].get_int32().value);

      task->set_extra_attr(view["extra_attr"].get_string().value.data());

      task->set_priority(view["priority"].get_int64().value);

      if (view.find("reservation") != view.end()) {
        task->set_reservation(view["reservation"].get_string().value.data());
      }
      task->set_exclusive(view["exclusive"].get_bool().value);
      task->set_container(view["container"].get_string().value);
    }
  } catch (const bsoncxx::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
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

template <>
void MongodbClient::DocumentAppendItem_<DeviceMap>(document& doc,
                                                   const std::string& key,
                                                   const DeviceMap& value) {
  doc.append(kvp(key, [&value](sub_document mapValueDocument) {
    for (const auto& mapItem : value) {
      const auto& device_name = mapItem.first;
      const auto& pair_val = mapItem.second;
      uint64_t total = pair_val.first;
      const auto& type_count_map = pair_val.second;

      mapValueDocument.append(
          kvp(device_name, [&total, &type_count_map](sub_document deviceDoc) {
            deviceDoc.append(kvp("total", static_cast<int64_t>(total)));
            deviceDoc.append(
                kvp("type_count_map", [&type_count_map](sub_document typeDoc) {
                  for (const auto& typeItem : type_count_map) {
                    typeDoc.append(kvp(typeItem.first,
                                       static_cast<int64_t>(typeItem.second)));
                  }
                }));
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

template <typename ViewValue, typename T>
T MongodbClient::ViewValueOr_(const ViewValue& view_value,
                              const T& default_value) {
  if (view_value && view_value.type() == BsonFieldTrait<T>::bson_type)
    return BsonFieldTrait<T>::get(view_value);

  return default_value;
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

    user->cert_number = ViewValueOr_(user_view["cert_number"], std::string{""});

  } catch (const bsoncxx::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
}

bsoncxx::builder::basic::document MongodbClient::UserToDocument_(
    const Ctld::User& user) {
  std::array<std::string, 8> fields{"deleted",
                                    "uid",
                                    "default_account",
                                    "name",
                                    "admin_level",
                                    "account_to_attrs_map",
                                    "coordinator_accounts",
                                    "cert_number"};
  std::tuple<bool, int64_t, std::string, std::string, int32_t,
             User::AccountToAttrsMap, std::list<std::string>, std::string>
      values{user.deleted,
             user.uid,
             user.default_account,
             user.name,
             user.admin_level,
             user.account_to_attrs_map,
             user.coordinator_accounts,
             user.cert_number};
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
    CRANE_LOGGER_ERROR(m_logger_, e.what());
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
    qos->deleted = qos_view[Qos::FieldStringOfDeleted()].get_bool().value;
    qos->name = qos_view[Qos::FieldStringOfName()].get_string().value;
    qos->description =
        qos_view[Qos::FieldStringOfDescription()].get_string().value;
    qos->reference_count =
        qos_view[Qos::FieldStringOfReferenceCount()].get_int32().value;
    qos->priority = qos_view[Qos::FieldStringOfPriority()].get_int64().value;
    qos->max_jobs_per_user =
        qos_view[Qos::FieldStringOfMaxJobsPerUser()].get_int64().value;
    qos->max_cpus_per_user =
        qos_view[Qos::FieldStringOfMaxCpusPerUser()].get_int64().value;
    qos->max_time_limit_per_task = absl::Seconds(
        qos_view[Qos::FieldStringOfMaxTimeLimitPerTask()].get_int64().value);
  } catch (const bsoncxx::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
}

bsoncxx::builder::basic::document MongodbClient::QosToDocument_(
    const Ctld::Qos& qos) {
  std::array<std::string, 8> fields{
      Qos::FieldStringOfDeleted(),
      Qos::FieldStringOfName(),
      Qos::FieldStringOfDescription(),
      Qos::FieldStringOfReferenceCount(),
      Qos::FieldStringOfPriority(),
      Qos::FieldStringOfMaxJobsPerUser(),
      Qos::FieldStringOfMaxCpusPerUser(),
      Qos::FieldStringOfMaxTimeLimitPerTask(),
  };
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

DeviceMap MongodbClient::BsonToDeviceMap(const bsoncxx::document::view& doc) {
  DeviceMap device_map;

  try {
    auto device_map_elem = doc["device_map"];
    if (!device_map_elem || device_map_elem.type() != bsoncxx::type::k_document)
      return device_map;

    auto device_map_doc = device_map_elem.get_document().view();

    for (const auto& device_elem : device_map_doc) {
      std::string device_name = std::string(device_elem.key());
      if (device_elem.type() != bsoncxx::type::k_document) {
        CRANE_LOGGER_ERROR(m_logger_,
                           "device_map value: BSON type is not a document.");
        continue;
      }

      auto type_count_doc = device_elem.get_document().view();

      uint64_t total = 0;
      if (auto total_elem = type_count_doc["total"]) {
        if (total_elem.type() == bsoncxx::type::k_int64)
          total = static_cast<uint64_t>(total_elem.get_int64());
        else if (total_elem.type() == bsoncxx::type::k_int32)
          total = static_cast<uint64_t>(total_elem.get_int32());
        else
          CRANE_LOGGER_ERROR(m_logger_, "total: BSON type is not a number.");
      }

      std::unordered_map<std::string, uint64_t> type_count_map;
      if (auto type_map_elem = type_count_doc["type_count_map"];
          type_map_elem && type_map_elem.type() == bsoncxx::type::k_document) {
        auto type_map_doc = type_map_elem.get_document().view();
        for (const auto& type_elem : type_map_doc) {
          uint64_t val = 0;
          if (type_elem.type() == bsoncxx::type::k_int64)
            val = static_cast<uint64_t>(type_elem.get_int64());
          else if (type_elem.type() == bsoncxx::type::k_int32)
            val = static_cast<uint64_t>(type_elem.get_int32());
          else
            CRANE_LOGGER_ERROR(
                m_logger_, "type_count_map value: BSON type is not a number.");

          type_count_map[std::string(type_elem.key())] = val;
        }
      } else {
        CRANE_LOGGER_ERROR(m_logger_,
                           "type_count_map: BSON type is not a document.");
      }

      device_map[device_name] = {total, type_count_map};
    }
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }

  return device_map;
}

MongodbClient::document MongodbClient::TaskInEmbeddedDbToDocument_(
    const crane::grpc::TaskInEmbeddedDb& task) {
  auto const& task_to_ctld = task.task_to_ctld();
  auto const& runtime_attr = task.runtime_attr();

  auto resources = static_cast<ResourceV2>(runtime_attr.allocated_res());
  ResourceView allocated_res_view;
  allocated_res_view.SetToZero();
  allocated_res_view += resources;

  bsoncxx::builder::stream::document env_doc;
  for (const auto& entry : task_to_ctld.env()) {
    env_doc << entry.first << entry.second;
  }

  std::string env_str = bsoncxx::to_json(env_doc.view());

  // 0  task_id       task_db_id     mod_time       deleted       account
  // 5  cpus_req      mem_req        task_name      env           id_user
  // 10 id_group      nodelist       nodes_alloc   node_inx    partition_name
  // 15 priority      time_eligible  time_start    time_end    time_suspended
  // 20 script        state          timelimit     time_submit work_dir
  // 25 submit_line   exit_code      username       qos        get_user_env
  // 30 type          extra_attr     reservation   exclusive   cpus_alloc
  // 35 mem_alloc     device_map     container

  // clang-format off
  std::array<std::string, 38> fields{
    // 0 - 4
    "task_id",  "task_db_id", "mod_time",    "deleted",  "account",
    // 5 - 9
    "cpus_req", "mem_req",    "task_name",   "env",      "id_user",
    // 10 - 14
    "id_group", "nodelist",   "nodes_alloc", "node_inx", "partition_name",
    // 15 - 19
    "priority", "time_eligible", "time_start", "time_end", "time_suspended",
    // 20 - 24
    "script", "state", "timelimit", "time_submit", "work_dir",
    // 25 - 29
    "submit_line", "exit_code",  "username", "qos", "get_user_env",
    // 30 - 34
    "type", "extra_attr", "reservation", "exclusive", "cpus_alloc",
    // 35 - 39
    "mem_alloc", "device_map", "container",
  };
  // clang-format on

  std::tuple<int32_t, task_db_id_t, int64_t, bool, std::string,    /*0-4*/
             double, int64_t, std::string, std::string, int32_t,   /*5-9*/
             int32_t, std::string, int32_t, int32_t, std::string,  /*10-14*/
             int64_t, int64_t, int64_t, int64_t, int64_t,          /*15-19*/
             std::string, int32_t, int64_t, int64_t, std::string,  /*20-24*/
             std::string, int32_t, std::string, std::string, bool, /*25-29*/
             int32_t, std::string, std::string, bool, double,      /*30-34*/
             int64_t, DeviceMap, std::string>                      /*35-39*/
      values{                                                      // 0-4
             static_cast<int32_t>(runtime_attr.task_id()),
             runtime_attr.task_db_id(), absl::ToUnixSeconds(absl::Now()), false,
             task_to_ctld.account(),
             // 5-9
             task_to_ctld.req_resources().allocatable_res().cpu_core_limit(),
             static_cast<int64_t>(task_to_ctld.req_resources()
                                      .allocatable_res()
                                      .memory_limit_bytes()),
             task_to_ctld.name(), env_str,
             static_cast<int32_t>(task_to_ctld.uid()),
             // 10-14
             static_cast<int32_t>(task_to_ctld.gid()),
             util::HostNameListToStr(runtime_attr.craned_ids()),
             runtime_attr.craned_ids().size(), 0, task_to_ctld.partition_name(),
             // 15-19
             runtime_attr.cached_priority(), 0,
             runtime_attr.start_time().seconds(),
             runtime_attr.end_time().seconds(), 0,
             // 20-24
             task_to_ctld.batch_meta().sh_script(), runtime_attr.status(),
             task_to_ctld.time_limit().seconds(),
             runtime_attr.submit_time().seconds(), task_to_ctld.cwd(),
             // 25-29
             task_to_ctld.cmd_line(), runtime_attr.exit_code(),
             runtime_attr.username(), task_to_ctld.qos(),
             task_to_ctld.get_user_env(),
             // 30-34
             task_to_ctld.type(), task_to_ctld.extra_attr(),
             task_to_ctld.reservation(), task_to_ctld.exclusive(),
             allocated_res_view.CpuCount(),
             // 35-39
             static_cast<int64_t>(allocated_res_view.MemoryBytes()),
             allocated_res_view.GetDeviceMap(), task_to_ctld.container()};

  return DocumentConstructor_(fields, values);
}

MongodbClient::document MongodbClient::TaskInCtldToDocument_(TaskInCtld* task) {
  std::string script;
  if (task->type == crane::grpc::Batch)
    script = std::get<BatchMetaInTask>(task->meta).sh_script;

  bsoncxx::builder::stream::document env_doc;
  for (const auto& entry : task->env) {
    env_doc << entry.first << entry.second;
  }

  std::string env_str = bsoncxx::to_json(env_doc.view());

  // 0  task_id       task_db_id     mod_time       deleted       account
  // 5  cpus_req      mem_req        task_name      env           id_user
  // 10 id_group      nodelist       nodes_alloc   node_inx    partition_name
  // 15 priority      time_eligible  time_start    time_end    time_suspended
  // 20 script        state          timelimit     time_submit work_dir
  // 25 submit_line   exit_code      username       qos        get_user_env
  // 30 type          extra_attr     reservation    exclusive  cpus_alloc
  // 35 mem_alloc     device_map     container

  // clang-format off
  std::array<std::string, 38> fields{
      // 0 - 4
      "task_id",  "task_db_id", "mod_time",    "deleted",  "account",
      // 5 - 9
      "cpus_req", "mem_req",    "task_name",   "env",      "id_user",
      // 10 - 14
      "id_group", "nodelist",   "nodes_alloc", "node_inx", "partition_name",
      // 15 - 19
      "priority", "time_eligible", "time_start", "time_end", "time_suspended",
      // 20 - 24
      "script", "state", "timelimit", "time_submit", "work_dir",
      // 25 - 29
      "submit_line", "exit_code",  "username", "qos", "get_user_env",
      // 30 - 34
      "type", "extra_attr", "reservation", "exclusive", "cpus_alloc",
      // 35 - 39
      "mem_alloc", "device_map", "container",
  };
  // clang-format on

  std::tuple<int32_t, task_db_id_t, int64_t, bool, std::string,    /*0-4*/
             double, int64_t, std::string, std::string, int32_t,   /*5-9*/
             int32_t, std::string, int32_t, int32_t, std::string,  /*10-14*/
             int64_t, int64_t, int64_t, int64_t, int64_t,          /*15-19*/
             std::string, int32_t, int64_t, int64_t, std::string,  /*20-24*/
             std::string, int32_t, std::string, std::string, bool, /*25-29*/
             int32_t, std::string, std::string, bool, double,      /*30-34*/
             int64_t, DeviceMap, std::string>                      /*35-39*/
      values{                                                      // 0-4
             static_cast<int32_t>(task->TaskId()), task->TaskDbId(),
             absl::ToUnixSeconds(absl::Now()), false, task->account,
             // 5-9
             static_cast<double>(task->requested_node_res_view.CpuCount()),
             static_cast<int64_t>(task->requested_node_res_view.MemoryBytes()),
             task->name, env_str, static_cast<int32_t>(task->uid),
             // 10-14
             static_cast<int32_t>(task->gid), task->allocated_craneds_regex,
             static_cast<int32_t>(task->nodes_alloc), 0, task->partition_id,
             // 15-19
             static_cast<int64_t>(task->CachedPriority()), 0,
             task->StartTimeInUnixSecond(), task->EndTimeInUnixSecond(), 0,
             // 20-24
             script, task->Status(), absl::ToInt64Seconds(task->time_limit),
             task->SubmitTimeInUnixSecond(), task->cwd,
             // 25-29
             task->cmd_line, task->ExitCode(), task->Username(), task->qos,
             task->get_user_env,
             // 30-34
             task->type, task->extra_attr, task->reservation,
             task->TaskToCtld().exclusive(),
             task->allocated_res_view.CpuCount(),
             // 35-39
             static_cast<int64_t>(task->allocated_res_view.MemoryBytes()),
             task->allocated_res_view.GetDeviceMap(), task->container};
  return DocumentConstructor_(fields, values);
}

bool MongodbClient::UpdateSummaryLastSuccessTimeSec(const std::string& type,
                                                    int64_t last_success_sec) {
  try {
    auto summary_coll =
        (*GetClient_())[m_db_name_][m_summary_time_collection_name_];
    auto update_sec = static_cast<int64_t>(std::time(nullptr));

    auto filter = bsoncxx::builder::stream::document{}
                  << "_id" << type << bsoncxx::builder::stream::finalize;

    auto update = bsoncxx::builder::stream::document{}
                  << "$max" << bsoncxx::builder::stream::open_document
                  << "last_success_time" << last_success_sec
                  << bsoncxx::builder::stream::close_document << "$set"
                  << bsoncxx::builder::stream::open_document << "update_time"
                  << update_sec << bsoncxx::builder::stream::close_document
                  << bsoncxx::builder::stream::finalize;

    auto result = summary_coll.update_one(
        filter.view(), update.view(), mongocxx::options::update{}.upsert(true));
    return true;
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_,
                       "UpdateSummaryLastSuccessTimeSec failed: {}, type={}",
                       e.what(), type);
    return false;
  }
}

bool MongodbClient::GetSummaryLastSuccessTimeTm(const std::string& type,
                                                std::tm& tm_last) {
  // Default time
  std::tm default_time = {};
  default_time.tm_year = 2024 - 1900;
  default_time.tm_mon = 9 - 1;
  default_time.tm_mday = 1;

  try {
    auto summary_coll =
        (*GetClient_())[m_db_name_][m_summary_time_collection_name_];
    auto filter = bsoncxx::builder::stream::document{}
                  << "_id" << type << bsoncxx::builder::stream::finalize;
    auto doc_opt = summary_coll.find_one(filter.view());
    if (doc_opt) {
      auto doc = doc_opt->view();
      auto it = doc.find("last_success_time");
      if (it != doc.end() && it->type() == bsoncxx::type::k_int64) {
        int64_t last_sec = it->get_int64().value;
        auto tt = static_cast<std::time_t>(last_sec);
        std::tm tm_tmp = {};
        localtime_r(&tt, &tm_last);
      }
    } else {
      tm_last = default_time;
    }
    return true;
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(
        m_logger_,
        fmt::format("GetSummaryLastSuccessTimeTm failed: {}, type={}, coll={}",
                    e.what(), type, m_summary_time_collection_name_));
  }

  return false;
}

std::tm MongodbClient::GetRoundHourNowTm() {
  std::time_t now =
      std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  std::tm tm_now = *std::localtime(&now);
  tm_now.tm_min = 0;
  tm_now.tm_sec = 0;
  return tm_now;
}

bool MongodbClient::NeedRollup(const std::tm& tm_last, const std::tm& tm_now,
                               RollupType type) {
  // Compare year first
  if (tm_now.tm_year > tm_last.tm_year) return true;
  if (tm_now.tm_year < tm_last.tm_year) return false;

  switch (type) {
  case RollupType::HOUR:
    if (tm_now.tm_yday > tm_last.tm_yday) return true;
    if (tm_now.tm_yday < tm_last.tm_yday) return false;
    return tm_now.tm_hour > tm_last.tm_hour;

  case RollupType::HOUR_TO_DAY:
    return tm_now.tm_yday > tm_last.tm_yday;

  case RollupType::DAY_TO_MONTH:
    return tm_now.tm_mon > tm_last.tm_mon;

  default:
    return false;
  }
}

bool MongodbClient::RollupHourTable() {
  std::tm tm_last{};
  if (!GetSummaryLastSuccessTimeTm("hour", tm_last)) {
    CRANE_ERROR("Get summary lastSuccess time type hour err");
    return false;
  }

  std::tm tm_now = GetRoundHourNowTm();

  if (!NeedRollup(tm_last, tm_now, RollupType::HOUR)) {
    auto last_sec = static_cast<int64_t>(std::mktime(&tm_last));
    auto now_sec = static_cast<int64_t>(std::mktime(&tm_now));
    CRANE_INFO("Less than 1 hour since last aggregation, skip. {} -> {}",
               util::FormatTime(last_sec), util::FormatTime(now_sec));
    return false;
  }

  auto last_sec = static_cast<int64_t>(std::mktime(&tm_last));
  auto now_sec = static_cast<int64_t>(std::mktime(&tm_now));
  CRANE_INFO("Aggregating Hour from {} to {}", util::FormatTime(last_sec),
             util::FormatTime(now_sec));

  // Aggregate data in the hour window
  auto t0 = std::chrono::steady_clock::now();
  bool success = AggregateJobSummary(RollupType::HOUR, last_sec, now_sec);
  if (!success) {
    CRANE_ERROR("Aggregate Hour Table failed! Window: {} -> {}",
                util::FormatTime(last_sec), util::FormatTime(now_sec));
  }
  auto t1 = std::chrono::steady_clock::now();
  int64_t agg_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

  return success;
}

bool MongodbClient::RollupHourToDay() {
  std::tm tm_last{};
  if (!GetSummaryLastSuccessTimeTm("hour_to_day", tm_last)) {
    CRANE_ERROR("Get summary lastSuccess time type hour_to_day err");
    return false;
  }
  std::tm tm_now = GetRoundHourNowTm();

  if (!NeedRollup(tm_last, tm_now, RollupType::HOUR_TO_DAY)) {
    auto last_sec = static_cast<int64_t>(std::mktime(&tm_last));
    auto now_sec = static_cast<int64_t>(std::mktime(&tm_now));
    CRANE_INFO("Less than 1 day since last aggregation, skip. {} -> {}",
               util::FormatTime(last_sec), util::FormatTime(now_sec));
    return false;
  }

  auto last_sec = static_cast<int64_t>(std::mktime(&tm_last));
  auto now_sec = static_cast<int64_t>(std::mktime(&tm_now));
  CRANE_INFO("Aggregating Day from {} to {}", util::FormatTime(last_sec),
             util::FormatTime(now_sec));

  // Aggregate data from hour to day
  bool ok = AggregateJobSummary(RollupType::HOUR_TO_DAY, last_sec, now_sec);
  if (!ok) {
    CRANE_ERROR("Aggregate Job Summary (Hour->Day) failed! Window: {} -> {}",
                util::FormatTime(last_sec), util::FormatTime(now_sec));
  }
  return ok;
}

bool MongodbClient::RollupDayToMonth() {
  std::tm tm_last{};
  if (!GetSummaryLastSuccessTimeTm("day_to_month", tm_last)) {
    CRANE_ERROR("Get summary lastSuccess time type hour_to_day err");
    return false;
  }
  std::tm tm_now = GetRoundHourNowTm();

  if (!NeedRollup(tm_last, tm_now, RollupType::DAY_TO_MONTH)) {
    auto last_sec = static_cast<int64_t>(std::mktime(&tm_last));
    auto now_sec = static_cast<int64_t>(std::mktime(&tm_now));
    CRANE_INFO("Less than 1 month since last aggregation, skip. {} -> {}",
               util::FormatTime(last_sec), util::FormatTime(now_sec));
    return false;
  }

  auto last_sec = static_cast<int64_t>(std::mktime(&tm_last));
  auto now_sec = static_cast<int64_t>(std::mktime(&tm_now));
  CRANE_INFO("Aggregating Month from {} to {}", util::FormatTime(last_sec),
             util::FormatTime(now_sec));

  // Aggregate data from day to month
  bool success =
      AggregateJobSummary(RollupType::DAY_TO_MONTH, last_sec, now_sec);
  if (!success) {
    CRANE_ERROR("Aggregate Job Summary (Day->Month) failed! Window: {} -> {}",
                util::FormatTime(last_sec), util::FormatTime(now_sec));
  }
  return success;
}

void MongodbClient::ClusterRollupUsage() {
  std::lock_guard<std::mutex> lock(rollup_mutex_);

  auto start_total = std::chrono::steady_clock::now();
  auto start = std::chrono::steady_clock::now();
  RollupHourTable();
  auto end = std::chrono::steady_clock::now();
  auto dur1 = std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
                  .count();
  CRANE_INFO("RollupHourTable used {} ms", dur1);

  start = std::chrono::steady_clock::now();
  RollupHourToDay();
  end = std::chrono::steady_clock::now();
  auto dur2 = std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
                  .count();
  CRANE_INFO("RollupHourToDay used {} ms", dur2);

  start = std::chrono::steady_clock::now();
  RollupDayToMonth();
  end = std::chrono::steady_clock::now();
  auto dur3 = std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
                  .count();
  CRANE_INFO("RollupDayToMonth used {} ms", dur3);

  auto end_total = std::chrono::steady_clock::now();
  auto total = std::chrono::duration_cast<std::chrono::milliseconds>(
                   end_total - start_total)
                   .count();
  CRANE_INFO("ClusterRollupUsage total used {} ms", total);
}

bool MongodbClient::AggregateJobSummary(RollupType type, std::time_t start,
                                        std::time_t end) {
  try {
    std::string collection_name;
    std::vector<std::string> index_fields;

    // Select collection name and index fields based on aggregation type
    if (type == RollupType::HOUR) {
      collection_name = m_hour_job_summary_collection_name_;
      index_fields = {"hour",  "account",   "username",      "qos",
                      "wckey", "cpu_alloc", "partition_name"};
    } else if (type == RollupType::HOUR_TO_DAY) {
      collection_name = m_day_job_summary_collection_name_;
      index_fields = {"day",   "account",   "username",      "qos",
                      "wckey", "cpu_alloc", "partition_name"};
    } else if (type == RollupType::DAY_TO_MONTH) {
      collection_name = m_month_job_summary_collection_name_;
      index_fields = {"month", "account",   "username",      "qos",
                      "wckey", "cpu_alloc", "partition_name"};
    }

    auto job_summ_table = (*GetClient_())[m_db_name_][collection_name];
    auto index_builder = bsoncxx::builder::stream::document{};
    for (const auto& field : index_fields) {
      index_builder << field << 1;
    }
    job_summ_table.create_index(index_builder
                                << bsoncxx::builder::stream::finalize);
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, "Create index error: {}", e.what());
  }

  // Perform aggregation based on type
  if (type == RollupType::HOUR) {
    HourJobSummAggregation(start, end, m_task_collection_name_);
  } else if (type == RollupType::HOUR_TO_DAY) {
    DayOrMonJobSummAggregation(m_hour_job_summary_collection_name_,
                               m_day_job_summary_collection_name_, "hour",
                               "day", start, end);
  } else if (type == RollupType::DAY_TO_MONTH) {
    DayOrMonJobSummAggregation(m_day_job_summary_collection_name_,
                               m_month_job_summary_collection_name_, "day",
                               "month", start, end);
  }

  return true;
}

void MongodbClient::HourJobSummAggregation(
    std::time_t start, std::time_t end,
    const std::string& task_collection_name) {
  std::tm tm_start = *std::localtime(&start);
  tm_start.tm_min = 0;
  tm_start.tm_sec = 0;
  std::time_t cur_start = std::mktime(&tm_start);
  std::time_t cur_end = cur_start + 3600;

  try {
    auto jobs = (*GetClient_())[m_db_name_][task_collection_name];

    // Iterate through each hour interval within the specified range
    while (cur_end <= end) {
      mongocxx::pipeline pipeline;

      // Match jobs whose 'time_end' falls within the current hour interval
      pipeline.match(bsoncxx::builder::stream::document{}
                     << "time_end" << bsoncxx::builder::stream::open_document
                     << "$gte" << static_cast<int64_t>(cur_start) << "$lt"
                     << static_cast<int64_t>(cur_end)
                     << bsoncxx::builder::stream::close_document
                     << bsoncxx::builder::stream::finalize);

      // Add the 'hour' field representing the current hour interval
      pipeline.add_fields(bsoncxx::builder::stream::document{}
                          << "hour" << static_cast<int64_t>(cur_start)
                          << bsoncxx::builder::stream::finalize);

      pipeline.add_fields(
          bsoncxx::builder::stream::document{}
          << "cpu_alloc" << bsoncxx::builder::stream::open_document
          << "$multiply" << bsoncxx::builder::stream::open_array
          << "$nodes_alloc"
          << "$cpus_alloc" << bsoncxx::builder::stream::close_array
          << bsoncxx::builder::stream::close_document
          << bsoncxx::builder::stream::finalize);

      pipeline.add_fields(bsoncxx::builder::stream::document{}
                          << "wckey" << bsoncxx::builder::stream::open_document
                          << "$ifNull" << bsoncxx::builder::stream::open_array
                          << "$wckey" << "_default_"
                          << bsoncxx::builder::stream::close_array
                          << bsoncxx::builder::stream::close_document
                          << bsoncxx::builder::stream::finalize);

      // Compute the 'cpu_time' field as nodes_alloc * cpus_alloc * (time_end -
      // time_start)
      pipeline.add_fields(
          bsoncxx::builder::stream::document{}
          << "cpu_time" << bsoncxx::builder::stream::open_document
          << "$multiply" << bsoncxx::builder::stream::open_array
          << "$nodes_alloc"
          << "$cpus_alloc" << bsoncxx::builder::stream::open_document
          << "$subtract" << bsoncxx::builder::stream::open_array << "$time_end"
          << "$time_start" << bsoncxx::builder::stream::close_array
          << bsoncxx::builder::stream::close_document
          << bsoncxx::builder::stream::close_array
          << bsoncxx::builder::stream::close_document
          << bsoncxx::builder::stream::finalize);

      // Group jobs by hour, account, username, qos, wckey, cpu_alloc,
      // partition_name and calculate total CPU time and total job count for
      // each group
      pipeline.group(bsoncxx::builder::stream::document{}
                     << "_id" << bsoncxx::builder::stream::open_document
                     << "hour" << "$hour"
                     << "account" << "$account"
                     << "username" << "$username"
                     << "qos" << "$qos"
                     << "node" << "$qos"
                     << "wckey" << "$wckey"
                     << "cpu_alloc" << "$cpu_alloc"
                     << "partition_name" << "$partition_name"
                     << bsoncxx::builder::stream::close_document
                     << "total_cpu_time"
                     << bsoncxx::builder::stream::open_document << "$sum"
                     << "$cpu_time" << bsoncxx::builder::stream::close_document
                     << "total_count" << bsoncxx::builder::stream::open_document
                     << "$sum" << 1 << bsoncxx::builder::stream::close_document
                     << bsoncxx::builder::stream::finalize);

      // Reshape the result document for easier downstream usage
      pipeline.replace_root(bsoncxx::builder::stream::document{}
                            << "newRoot"
                            << bsoncxx::builder::stream::open_document << "hour"
                            << "$_id.hour"
                            << "account" << "$_id.account"
                            << "username" << "$_id.username"
                            << "qos" << "$_id.qos"
                            << "wckey" << "$_id.wckey"
                            << "cpu_alloc" << "$_id.cpu_alloc"
                            << "partition_name" << "$_id.partition_name"
                            << "total_cpu_time" << "$total_cpu_time"
                            << "total_count" << "$total_count"
                            << bsoncxx::builder::stream::close_document
                            << bsoncxx::builder::stream::finalize);

      // Merge the aggregation results into the summary collection
      pipeline.merge(bsoncxx::builder::stream::document{}
                     << "into" << m_hour_job_summary_collection_name_
                     << "whenMatched" << "replace"
                     << "whenNotMatched" << "insert"
                     << bsoncxx::builder::stream::finalize);

      auto cursor = jobs.aggregate(pipeline);
      // Force execution of the aggregation pipeline (including $merge/$out) by
      // iterating the cursor. No need to process the documents.
      for (auto&& doc : cursor) {}

      cur_start = cur_end;
      cur_end += 3600;
    }
  } catch (const std::exception& e) {
    UpdateSummaryLastSuccessTimeSec("hour", cur_end);
    CRANE_LOGGER_ERROR(m_logger_, "HourJobSummAggregation error: {}", e.what());
  }

  UpdateSummaryLastSuccessTimeSec("hour", cur_end);
}

void MongodbClient::DayOrMonJobSummAggregation(
    const std::string& src_coll_str, const std::string& dst_coll_str,
    const std::string& src_time_field, const std::string& period_field,
    std::time_t period_start, std::time_t period_end) {
  std::time_t cur_start;
  std::time_t cur_end = period_end;
  std::tm tm_end = {};
  std::tm tm_start = {};
  try {
    if (period_field == "day") {
      localtime_r(&period_start, &tm_start);
      tm_start.tm_hour = 0;
      tm_start.tm_min = 0;
      tm_start.tm_sec = 0;
      cur_start = std::mktime(&tm_start);

      localtime_r(&cur_start, &tm_end);
      tm_end.tm_sec = 0;
      tm_end.tm_min = 0;
      tm_end.tm_hour = 0;
      tm_end.tm_mday++;
      cur_end = std::mktime(&tm_end);
    } else if (period_field == "month") {
      localtime_r(&period_start, &tm_start);
      tm_start.tm_mday = 1;
      tm_start.tm_hour = 0;
      tm_start.tm_min = 0;
      tm_start.tm_sec = 0;
      cur_start = std::mktime(&tm_start);

      localtime_r(&cur_start, &tm_end);
      tm_end.tm_sec = 0;
      tm_end.tm_min = 0;
      tm_end.tm_hour = 0;
      tm_end.tm_mday = 1;
      tm_end.tm_mon++;
      cur_end = std::mktime(&tm_end);
    }
    auto src_coll = (*GetClient_())[m_db_name_][src_coll_str];
    while (cur_end <= period_end) {
      std::string group_period_field_ref = "$" + period_field;
      mongocxx::pipeline pipeline;
      pipeline.match(bsoncxx::builder::stream::document{}
                     << src_time_field
                     << bsoncxx::builder::stream::open_document << "$gte"
                     << static_cast<int64_t>(cur_start) << "$lt"
                     << static_cast<int64_t>(cur_end)
                     << bsoncxx::builder::stream::close_document
                     << bsoncxx::builder::stream::finalize);
      pipeline.add_fields(bsoncxx::builder::stream::document{}
                          << period_field << static_cast<int64_t>(cur_start)
                          << bsoncxx::builder::stream::finalize);
      pipeline.group(
          bsoncxx::builder::stream::document{}
          << "_id" << bsoncxx::builder::stream::open_document << period_field
          << bsoncxx::types::b_string{group_period_field_ref} << "account"
          << "$account"
          << "username" << "$username" << "qos" << "$qos" << "wckey" << "$wckey"
          << "cpu_alloc" << "$cpu_alloc" << "partition_name"
          << "$partition_name" << bsoncxx::builder::stream::close_document
          << "total_cpu_time" << bsoncxx::builder::stream::open_document
          << "$sum"
          << "$total_cpu_time" << bsoncxx::builder::stream::close_document
          << "total_count" << bsoncxx::builder::stream::open_document << "$sum"
          << "$total_count" << bsoncxx::builder::stream::close_document
          << bsoncxx::builder::stream::finalize);

      pipeline.replace_root(bsoncxx::builder::stream::document{}
                            << "newRoot"
                            << bsoncxx::builder::stream::open_document
                            << period_field << "$_id." + period_field
                            << "account" << "$_id.account"
                            << "username" << "$_id.username"
                            << "qos" << "$_id.qos"
                            << "wckey" << "$_id.wckey"
                            << "cpu_alloc" << "$_id.cpu_alloc"
                            << "partition_name" << "$_id.partition_name"
                            << "total_cpu_time" << "$total_cpu_time"
                            << "total_count" << "$total_count"
                            << bsoncxx::builder::stream::close_document
                            << bsoncxx::builder::stream::finalize);

      pipeline.merge(bsoncxx::builder::stream::document{}
                     << "into" << dst_coll_str << "whenMatched" << "replace"
                     << "whenNotMatched" << "insert"
                     << bsoncxx::builder::stream::finalize);
      src_coll.aggregate(pipeline);

      auto cursor = src_coll.aggregate(pipeline);
      for (auto&& doc : cursor) {
        std::cout << bsoncxx::to_json(doc) << std::endl;
      }
      // Advance window
      if (period_field == "day") {
        cur_start = cur_end;
        localtime_r(&cur_end, &tm_end);
        tm_end.tm_sec = 0;
        tm_end.tm_min = 0;
        tm_end.tm_hour = 0;
        tm_end.tm_mday++;
        cur_end = std::mktime(&tm_end);
      } else if (period_field == "month") {
        cur_start = cur_end;
        localtime_r(&cur_end, &tm_end);
        tm_end.tm_sec = 0;
        tm_end.tm_min = 0;
        tm_end.tm_hour = 0;
        tm_end.tm_mday = 1;
        tm_end.tm_mon++;
        cur_end = std::mktime(&tm_end);
      }
    }
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_,
                       "[mongodb] DayOrMonJobSummAggregation exception: {}",
                       e.what());
    if (period_field == "day") {
      UpdateSummaryLastSuccessTimeSec("hour_to_day", cur_end);
    } else if (period_field == "month") {
      UpdateSummaryLastSuccessTimeSec("day_to_month", cur_end);
    }
  }
  if (period_field == "day") {
    UpdateSummaryLastSuccessTimeSec("hour_to_day", cur_end);
  } else if (period_field == "month") {
    UpdateSummaryLastSuccessTimeSec("day_to_month", cur_end);
  }
}

void MongodbClient::QueryAndAggJobSizeSummary(
    const std::string& table, const std::string& time_field,
    std::time_t range_start, std::time_t range_end,
    const crane::grpc::QueryJobSizeSummaryItemRequest* request,
    absl::flat_hash_map<JobSizeSummKey, JobSummAggResult>& agg_map) {
  try {
    auto jobs = (*GetClient_())[m_db_name_][table];
    jobs.create_index(bsoncxx::builder::stream::document{}
                      << time_field << 1 << "account" << 1 << "username" << 1
                      << "wckey" << 1 << "cpu_alloc" << 1
                      << bsoncxx::builder::stream::finalize);
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, "Create index error: {}", e.what());
  }

  std::unordered_set<std::string> accounts(request->filter_accounts().begin(),
                                           request->filter_accounts().end());
  std::unordered_set<std::string> users(request->filter_users().begin(),
                                        request->filter_users().end());
  std::unordered_set<std::string> qoss(request->filter_qoss().begin(),
                                       request->filter_qoss().end());
  std::unordered_set<std::string> wckeys(request->filter_wckeys().begin(),
                                         request->filter_wckeys().end());
  std::vector<std::uint32_t> grouping_list(
      request->filter_grouping_list().begin(),
      request->filter_grouping_list().end());

  bsoncxx::builder::stream::document match_builder;
  match_builder << time_field << bsoncxx::builder::stream::open_document
                << "$gte" << static_cast<int64_t>(range_start) << "$lt"
                << static_cast<int64_t>(range_end)
                << bsoncxx::builder::stream::close_document;

  if (!accounts.empty()) {
    bsoncxx::builder::basic::array arr_builder;
    for (const auto& acc : accounts) arr_builder.append(acc);
    match_builder << "account" << bsoncxx::builder::stream::open_document
                  << "$in" << arr_builder.view()
                  << bsoncxx::builder::stream::close_document;
  }

  if (!users.empty()) {
    bsoncxx::builder::basic::array arr_builder;
    for (const auto& user : users) arr_builder.append(user);
    match_builder << "username" << bsoncxx::builder::stream::open_document
                  << "$in" << arr_builder.view()
                  << bsoncxx::builder::stream::close_document;
  }

  if (!qoss.empty()) {
    bsoncxx::builder::basic::array arr_builder;
    for (const auto& qos : qoss) arr_builder.append(qos);
    match_builder << "qos" << bsoncxx::builder::stream::open_document << "$in"
                  << arr_builder.view()
                  << bsoncxx::builder::stream::close_document;
  }

  if (!wckeys.empty()) {
    bsoncxx::builder::basic::array arr_builder;
    for (const auto& wk : wckeys) arr_builder.append(wk);
    match_builder << "wckey" << bsoncxx::builder::stream::open_document << "$in"
                  << arr_builder.view()
                  << bsoncxx::builder::stream::close_document;
  }

  // Build aggregation pipeline
  mongocxx::pipeline pipeline;
  pipeline.match(match_builder << bsoncxx::builder::stream::finalize);

  pipeline.group(
      bsoncxx::builder::stream::document{}
      << "_id" << bsoncxx::builder::stream::open_document << "account"
      << "$account"
      << "username" << "$username"
      << "wckey" << "$wckey" << "cpu_alloc" << "$cpu_alloc"
      << bsoncxx::builder::stream::close_document << "total_cpu_time"
      << bsoncxx::builder::stream::open_document << "$sum"
      << "$total_cpu_time" << bsoncxx::builder::stream::close_document
      << "total_count" << bsoncxx::builder::stream::open_document << "$sum"
      << "$total_count" << bsoncxx::builder::stream::close_document
      << bsoncxx::builder::stream::finalize);
  pipeline.project(bsoncxx::builder::stream::document{}
                   << "_id" << 1 << "total_cpu_time" << 1 << "total_count" << 1
                   << bsoncxx::builder::stream::finalize);

  auto options = mongocxx::options::aggregate{};
  options.allow_disk_use(true);
  auto coll = (*GetClient_())[m_db_name_][table];
  auto cursor = coll.aggregate(pipeline, options);

  // Read aggregation results into agg_map
  for (auto&& doc : cursor) {
    auto id = doc["_id"].get_document().view();
    std::string acc = std::string(id["account"].get_string().value);
    std::string wk = std::string(id["wckey"].get_string().value);
    uint32_t cpu_alloc = 0;
    auto cpu_alloc_elem = id["cpu_alloc"];
    if (cpu_alloc_elem) {
      if (cpu_alloc_elem.type() == bsoncxx::type::k_int32)
        cpu_alloc = static_cast<uint32_t>(cpu_alloc_elem.get_int32().value);
      else if (cpu_alloc_elem.type() == bsoncxx::type::k_int64)
        cpu_alloc = static_cast<uint32_t>(cpu_alloc_elem.get_int64().value);
      else if (cpu_alloc_elem.type() == bsoncxx::type::k_double)
        cpu_alloc = static_cast<uint32_t>(cpu_alloc_elem.get_double().value);
    }

    if (grouping_list.empty()) {
      agg_map[{acc, wk, cpu_alloc}].total_cpu_time +=
          doc["total_cpu_time"].get_double().value;
      agg_map[{acc, wk, cpu_alloc}].total_count +=
          doc["total_count"].get_int32().value;
    } else {
      int group_index = 0;
      for (const auto group : grouping_list) {
        if (cpu_alloc < group) {
          break;
        }
        group_index++;
      }

      agg_map[{acc, wk, grouping_list[group_index - 1]}].total_cpu_time +=
          doc["total_cpu_time"].get_double().value;
      agg_map[{acc, wk, grouping_list[group_index - 1]}].total_count +=
          doc["total_count"].get_int32().value;
    }
  }
}

void MongodbClient::QueryAndAggJobSummary(
    const std::string& table, const std::string& time_field,
    std::time_t range_start, std::time_t range_end,
    const crane::grpc::QueryJobSummaryRequest* request,
    absl::flat_hash_map<JobSummKey, JobSummAggResult>& agg_map) {
  auto jobs = (*GetClient_())[m_db_name_][table];
  try {
    jobs.create_index(bsoncxx::builder::stream::document{}
                      << time_field << 1 << "account" << 1 << "username" << 1
                      << "qos" << 1 << "wckey" << 1
                      << bsoncxx::builder::stream::finalize);
  } catch (const std::exception& e) {
    CRANE_ERROR("Create index error: {}", e.what());
  }

  std::unordered_set<std::string> accounts(request->filter_accounts().begin(),
                                           request->filter_accounts().end());
  std::unordered_set<std::string> users(request->filter_users().begin(),
                                        request->filter_users().end());
  std::unordered_set<std::string> qoss(request->filter_qoss().begin(),
                                       request->filter_qoss().end());
  std::unordered_set<std::string> wckeys(request->filter_wckeys().begin(),
                                         request->filter_wckeys().end());

  bsoncxx::builder::stream::document match_builder;
  match_builder << time_field << bsoncxx::builder::stream::open_document
                << "$gte" << static_cast<int64_t>(range_start) << "$lt"
                << static_cast<int64_t>(range_end)
                << bsoncxx::builder::stream::close_document;

  if (!accounts.empty()) {
    bsoncxx::builder::basic::array arr_builder;
    for (const auto& acc : accounts) arr_builder.append(acc);
    match_builder << "account" << bsoncxx::builder::stream::open_document
                  << "$in" << arr_builder.view()
                  << bsoncxx::builder::stream::close_document;
  }
  if (!users.empty()) {
    bsoncxx::builder::basic::array arr_builder;
    for (const auto& user : users) arr_builder.append(user);
    match_builder << "username" << bsoncxx::builder::stream::open_document
                  << "$in" << arr_builder.view()
                  << bsoncxx::builder::stream::close_document;
  }
  if (!qoss.empty()) {
    bsoncxx::builder::basic::array arr_builder;
    for (const auto& qos : qoss) arr_builder.append(qos);
    match_builder << "qos" << bsoncxx::builder::stream::open_document << "$in"
                  << arr_builder.view()
                  << bsoncxx::builder::stream::close_document;
  }
  if (!wckeys.empty()) {
    bsoncxx::builder::basic::array arr_builder;
    for (const auto& wk : wckeys) arr_builder.append(wk);
    match_builder << "wckey" << bsoncxx::builder::stream::open_document << "$in"
                  << arr_builder.view()
                  << bsoncxx::builder::stream::close_document;
  }

  // Build aggregation pipeline
  mongocxx::pipeline pipeline;
  pipeline.match(match_builder << bsoncxx::builder::stream::finalize);

  pipeline.group(
      bsoncxx::builder::stream::document{}
      << "_id" << bsoncxx::builder::stream::open_document << "account"
      << "$account"
      << "username" << "$username" << "qos" << "$qos"
      << "wckey" << "$wckey" << bsoncxx::builder::stream::close_document
      << "total_cpu_time" << bsoncxx::builder::stream::open_document << "$sum"
      << "$total_cpu_time" << bsoncxx::builder::stream::close_document
      << "total_count" << bsoncxx::builder::stream::open_document << "$sum"
      << "$total_count" << bsoncxx::builder::stream::close_document
      << bsoncxx::builder::stream::finalize);

  pipeline.project(bsoncxx::builder::stream::document{}
                   << "_id" << 1 << "total_cpu_time" << 1 << "total_count" << 1
                   << bsoncxx::builder::stream::finalize);

  auto options = mongocxx::options::aggregate{};
  options.allow_disk_use(true);
  auto coll = (*GetClient_())[m_db_name_][table];

  auto cursor = coll.aggregate(pipeline, options);

  // Read aggregation results into agg_map
  for (auto&& doc : cursor) {
    auto id = doc["_id"].get_document().view();
    std::string acc = std::string(id["account"].get_string().value);
    std::string user = std::string(id["username"].get_string().value);
    std::string qos = std::string(id["qos"].get_string().value);
    std::string wk = std::string(id["wckey"].get_string().value);
    uint32_t cpu_alloc = 0;
    auto cpu_alloc_level_elem = id["cpu_alloc"];
    if (cpu_alloc_level_elem) {
      if (cpu_alloc_level_elem.type() == bsoncxx::type::k_int32)
        cpu_alloc = cpu_alloc_level_elem.get_int32().value;
      else if (cpu_alloc_level_elem.type() == bsoncxx::type::k_int64)
        cpu_alloc = static_cast<int>(cpu_alloc_level_elem.get_int64().value);
    }

    agg_map[{acc, user, qos, wk}].total_cpu_time +=
        doc["total_cpu_time"].get_double().value;
    agg_map[{acc, user, qos, wk}].total_count +=
        doc["total_count"].get_int32().value;
  }
}

bool MongodbClient::FetchJobSizeSummaryRecords(
    const crane::grpc::QueryJobSizeSummaryItemRequest* request,
    grpc::ServerWriter<::crane::grpc::QueryJobSizeSummaryItemReply>* stream) {
  document filter;
  int max_data_size = 5000;
  auto grouping_list = request->filter_grouping_list();

  if (request->has_filter_start_time()) {
    int64_t start_time_sec = request->filter_start_time().seconds();
    filter.append(kvp("time_start", [&](sub_document time_start_doc) {
      time_start_doc.append(kvp("$gte", start_time_sec));
    }));
  }

  if (request->has_filter_end_time()) {
    int64_t end_time_sec = request->filter_end_time().seconds();
    filter.append(kvp("time_end", [&](sub_document time_end_doc) {
      time_end_doc.append(kvp("$gte", end_time_sec));
    }));
  }

  bool has_accounts_constraint = !request->filter_accounts().empty();
  if (has_accounts_constraint) {
    filter.append(kvp("account", [&request](sub_document account_doc) {
      array account_array;
      for (const auto& account : request->filter_accounts()) {
        account_array.append(account);
      }
      account_doc.append(kvp("$in", account_array));
    }));
  }

  bool has_users_constraint = !request->filter_users().empty();
  if (has_users_constraint) {
    filter.append(kvp("username", [&request](sub_document user_doc) {
      array user_array;
      for (const auto& user : request->filter_users()) {
        user_array.append(user);
      }
      user_doc.append(kvp("$in", user_array));
    }));
  }

  bool has_task_ids_constraint = !request->filter_job_ids().empty();
  if (has_task_ids_constraint) {
    filter.append(kvp("task_id", [&request](sub_document task_id_doc) {
      array task_id_array;
      for (const auto& task_id : request->filter_job_ids()) {
        task_id_array.append(static_cast<std::int32_t>(task_id));
      }
      task_id_doc.append(kvp("$in", task_id_array));
    }));
  }

  bool has_partitions_constraint = !request->filter_partitions().empty();
  if (has_partitions_constraint) {
    filter.append(kvp("partition_name", [&request](sub_document partition_doc) {
      array partition_array;
      for (const auto& partition : request->filter_partitions()) {
        partition_array.append(partition);
      }
      partition_doc.append(kvp("$in", partition_array));
    }));
  }

  absl::flat_hash_map<JobSizeSummKey, JobSummAggResult> agg_map;
  mongocxx::cursor cursor =
      (*GetClient_())[m_db_name_][m_task_collection_name_].find(filter.view());
  try {
    for (auto view : cursor) {
      std::string acc = view["account"].get_string().value.data();
      std::string user = view["username"].get_string().value.data();
      std::string qos = std::string(view["qos"].get_string().value);
      std::string wk = "";
      if (view["wckey"]) {
        wk = std::string(view["wckey"].get_string().value);
      }
      uint32_t cpu_alloc =
          static_cast<uint32_t>(view["cpus_alloc"].get_double().value *
                                view["nodes_alloc"].get_int32().value);
      double total_cpu_time = (view["time_end"].get_int64().value -
                               view["time_start"].get_int64().value) *
                              cpu_alloc;
      if (grouping_list.empty()) {
        agg_map[{acc, wk, cpu_alloc}].total_cpu_time += total_cpu_time;
        agg_map[{acc, wk, cpu_alloc}].total_count += 1;
      } else {
        int group_index = 0;
        for (const auto group : grouping_list) {
          if (cpu_alloc < group) {
            break;
          }
          group_index++;
        }
        if (group_index == 0) continue;
        agg_map[{acc, wk, grouping_list[group_index - 1]}].total_cpu_time +=
            total_cpu_time;
        agg_map[{acc, wk, grouping_list[group_index - 1]}].total_count += 1;
      }
    }
  } catch (const bsoncxx::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }

  crane::grpc::QueryJobSizeSummaryItemReply reply;
  for (const auto& kv : agg_map) {
    crane::grpc::JobSizeSummaryItem item;
    item.set_cluster(g_config.CraneClusterName);
    item.set_account(kv.first.account);
    item.set_wckey(kv.first.wckey);
    item.set_cpu_alloc(kv.first.cpu_alloc);
    item.set_total_cpu_time(kv.second.total_cpu_time);
    item.set_total_count(kv.second.total_count);
    reply.add_item_list()->CopyFrom(item);
    if (reply.item_list_size() >= max_data_size) {
      stream->Write(reply);
      reply.clear_item_list();
    }
  }
  if (reply.item_list_size() > 0) {
    stream->Write(reply);
  }

  return true;
}

void MongodbClient::QueryJobSummary(
    const crane::grpc::QueryJobSummaryRequest* request,
    grpc::ServerWriter<::crane::grpc::QueryJobSummaryReply>* stream) {
  int max_data_size = 5000;
  absl::flat_hash_map<JobSummKey, JobSummAggResult> agg_map;
  auto start_time = request->filter_start_time().seconds();
  auto end_time = request->filter_end_time().seconds();

  auto ranges = util::EfficientSplitTimeRange(start_time, end_time);

  // Query and aggregate for each interval type
  for (const auto& data : ranges) {
    std::string table;
    std::string time_field;
    if (data.type == "month") {
      table = m_month_job_summary_collection_name_;
      time_field = "month";
    } else if (data.type == "day") {
      table = m_day_job_summary_collection_name_;
      time_field = "day";
    } else if (data.type == "hour") {
      table = m_hour_job_summary_collection_name_;
      time_field = "hour";
    } else {
      continue;
    }

    QueryAndAggJobSummary(table, time_field, data.start, data.end, request,
                          agg_map);
  }

  crane::grpc::QueryJobSummaryReply reply;
  for (const auto& kv : agg_map) {
    crane::grpc::JobSummaryItem item;
    item.set_cluster(g_config.CraneClusterName);
    item.set_account(kv.first.account);
    item.set_username(kv.first.username);
    item.set_qos(kv.first.qos);
    item.set_wckey(kv.first.wckey);
    item.set_total_cpu_time(kv.second.total_cpu_time);
    item.set_total_count(kv.second.total_count);
    reply.add_item_list()->CopyFrom(item);
    if (reply.item_list_size() >= max_data_size) {
      stream->Write(reply);
      reply.clear_item_list();
    }
  }
  if (reply.item_list_size() > 0) {
    stream->Write(reply);
  }
}

void MongodbClient::QueryJobSizeSummary(
    const crane::grpc::QueryJobSizeSummaryItemRequest* request,
    grpc::ServerWriter<::crane::grpc::QueryJobSizeSummaryItemReply>* stream) {
  int max_data_size = 5000;

  auto start_time = request->filter_start_time().seconds();
  auto end_time = request->filter_end_time().seconds();
  absl::flat_hash_map<JobSizeSummKey, JobSummAggResult> agg_map;
  auto ranges = util::EfficientSplitTimeRange(start_time, end_time);

  // Query and aggregate for each interval type
  for (const auto& data : ranges) {
    std::string table;
    std::string time_field;
    if (data.type == "month") {
      table = m_month_job_summary_collection_name_;
      time_field = "month";
    } else if (data.type == "day") {
      table = m_day_job_summary_collection_name_;
      time_field = "day";
    } else if (data.type == "hour") {
      table = m_hour_job_summary_collection_name_;
      time_field = "hour";
    } else {
      continue;
    }

    QueryAndAggJobSizeSummary(table, time_field, data.start, data.end, request,
                              agg_map);
  }

  crane::grpc::QueryJobSizeSummaryItemReply reply;
  for (const auto& kv : agg_map) {
    crane::grpc::JobSizeSummaryItem item;
    item.set_cluster(g_config.CraneClusterName);
    item.set_account(kv.first.account);
    item.set_wckey(kv.first.wckey);
    item.set_cpu_alloc(kv.first.cpu_alloc);
    item.set_total_cpu_time(kv.second.total_cpu_time);
    item.set_total_count(kv.second.total_count);
    reply.add_item_list()->CopyFrom(item);
    if (reply.item_list_size() >= max_data_size) {
      stream->Write(reply);
      reply.clear_item_list();
    }
  }
  if (reply.item_list_size() > 0) {
    stream->Write(reply);
  }
}

uint64_t MongodbClient::MillisecondsToNextHour() {
  auto now = std::chrono::system_clock::now();
  time_t now_time_t = std::chrono::system_clock::to_time_t(now);
  std::tm tm_now{};
  localtime_r(&now_time_t, &tm_now);

  tm_now.tm_min = 0;
  tm_now.tm_sec = 0;
  tm_now.tm_hour += 1;
  time_t next_hour_time_t = mktime(&tm_now);

  auto next_hour = std::chrono::system_clock::from_time_t(next_hour_time_t);
  auto ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(next_hour - now)
          .count();
  return static_cast<uint64_t>(ms);
}

bool MongodbClient::Init() {
  std::shared_ptr<uvw::loop> loop = uvw::loop::create();
  auto mongodb_task_timer_handle = loop->resource<uvw::timer_handle>();

  mongodb_task_timer_handle->on<uvw::timer_event>(
      [this](const uvw::timer_event&, uvw::timer_handle&) {
        ClusterRollupUsage();
      });

  uint64_t first_delay_ms = MillisecondsToNextHour();
  uint64_t repeat_ms = 3600 * 1000;

  mongodb_task_timer_handle->start(std::chrono::milliseconds(first_delay_ms),
                                   std::chrono::milliseconds(repeat_ms));

  m_mongodb_sum_thread_ =
      std::thread([this, loop = std::move(loop)]() { MongoDbSumaryTh_(loop); });

  return true;
}

void MongodbClient::MongoDbSumaryTh_(
    const std::shared_ptr<uvw::loop>& uvw_loop) {
  util::SetCurrentThreadName("MongoDbSumTh");

  std::shared_ptr<uvw::idle_handle> idle_handle =
      uvw_loop->resource<uvw::idle_handle>();

  idle_handle->on<uvw::idle_event>(
      [this](const uvw::idle_event&, uvw::idle_handle& h) {
        if (m_thread_stop_) {
          h.parent().walk([](auto&& h) { h.close(); });
          h.parent().stop();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      });

  if (idle_handle->start() != 0) {
    CRANE_ERROR("Failed to start the idle event in reservation loop.");
  }

  uvw_loop->run();
}

MongodbClient::MongodbClient() {
  m_instance_ = std::make_unique<mongocxx::instance>();
  m_db_name_ = g_config.DbName;
  std::string authentication;

  if (!g_config.DbUser.empty()) {
    authentication =
        fmt::format("{}:{}@", g_config.DbUser, g_config.DbPassword);
  }

  g_runtime_status.db_logger = AddLogger(
      "mongodb", StrToLogLevel(g_config.CraneCtldDebugLevel).value(), true);
  m_logger_ = g_runtime_status.db_logger;
  m_connect_uri_ = fmt::format(
      "mongodb://{}{}:{}/?replicaSet={}&maxPoolSize=1000", authentication,
      g_config.DbHost, g_config.DbPort, g_config.DbRSName);
  CRANE_LOGGER_TRACE(
      m_logger_,
      "Mongodb connect uri: "
      "mongodb://{}:[passwd]@{}:{}/?replicaSet={}&maxPoolSize=1000",
      g_config.DbUser, g_config.DbHost, g_config.DbPort, g_config.DbRSName);
  m_wc_majority_.acknowledge_level(mongocxx::write_concern::level::k_majority);
  m_rc_local_.acknowledge_level(mongocxx::read_concern::level::k_local);
  m_rp_primary_.mode(mongocxx::read_preference::read_mode::k_primary);
}

MongodbClient::~MongodbClient() {
  m_thread_stop_ = true;
  if (m_mongodb_sum_thread_.joinable()) m_mongodb_sum_thread_.join();
}

}  // namespace Ctld

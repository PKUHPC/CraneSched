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
#include <optional>

#include "CtldPublicDefs.h"
#include "crane/Logger.h"

namespace Ctld {

using bsoncxx::builder::basic::kvp;

bool MongodbClient::Connect() {
  try {
    m_connect_pool_ =
        std::make_unique<mongocxx::pool>(mongocxx::uri{m_connect_uri_});
    mongocxx::pool::entry client = m_connect_pool_->acquire();

    std::vector<std::string> database_name = client->list_database_names();

    if (std::ranges::find(database_name, m_db_name_) == database_name.end()) {
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
  document job_doc = TaskInEmbeddedDbToDocument_(task_in_embedded_db);

  // Create filter by task_id
  document filter;
  filter.append(
      kvp("task_id",
          static_cast<int32_t>(task_in_embedded_db.runtime_attr().task_id())));

  // Use $set to update job fields, and $setOnInsert for steps array
  document update_doc;
  update_doc.append(kvp("$set", job_doc));
  update_doc.append(kvp("$setOnInsert", [](sub_document set_on_insert) {
    set_on_insert.append(kvp("steps", bsoncxx::types::b_array{}));
  }));

  try {
    bsoncxx::stdx::optional<mongocxx::result::update> ret =
        (*GetClient_())[m_db_name_][m_task_collection_name_].update_one(
            *GetSession_(), filter.view(), update_doc.view(),
            mongocxx::options::update{}.upsert(true));

    if (ret != bsoncxx::stdx::nullopt) return true;
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
  CRANE_LOGGER_ERROR(m_logger_, "Failed to insert in-memory TaskInCtld.");
  return false;
}

bool MongodbClient::InsertJob(TaskInCtld* task) {
  document job_doc = TaskInCtldToDocument_(task);

  // Create filter by task_id
  document filter;
  filter.append(kvp("task_id", static_cast<int32_t>(task->TaskId())));

  // Use $set to update job fields, and $setOnInsert for steps array
  document update_doc;
  update_doc.append(kvp("$set", job_doc));
  update_doc.append(kvp("$setOnInsert", [](sub_document set_on_insert) {
    set_on_insert.append(kvp("steps", bsoncxx::types::b_array{}));
  }));

  try {
    bsoncxx::stdx::optional<mongocxx::result::update> ret =
        (*GetClient_())[m_db_name_][m_task_collection_name_].update_one(
            *GetSession_(), filter.view(), update_doc.view(),
            mongocxx::options::update{}.upsert(true));

    if (ret != bsoncxx::stdx::nullopt) return true;
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }

  CRANE_LOGGER_ERROR(m_logger_, "Failed to insert in-memory TaskInCtld.");
  return false;
}

bool MongodbClient::InsertJobs(const std::unordered_set<TaskInCtld*>& tasks) {
  if (tasks.empty()) return false;

  mongocxx::options::bulk_write bulk_options;

  try {
    auto bulk =
        (*GetClient_())[m_db_name_][m_task_collection_name_].create_bulk_write(
            *GetSession_(), bulk_options);

    for (const auto& task : tasks) {
      document job_doc = TaskInCtldToDocument_(task);

      // Create filter by task_id
      document filter;
      filter.append(kvp("task_id", static_cast<int32_t>(task->TaskId())));

      // Use $set to update job fields, and $setOnInsert for steps array
      // This ensures job fields are always updated, but steps array is only
      // created if document doesn't exist
      document update_doc;
      update_doc.append(kvp("$set", job_doc));
      update_doc.append(kvp("$setOnInsert", [](sub_document set_on_insert) {
        set_on_insert.append(kvp("steps", bsoncxx::types::b_array{}));
      }));

      mongocxx::model::update_one update_op{filter.view(), update_doc.view()};
      update_op.upsert(true);

      bulk.append(update_op);
    }

    auto result = bulk.execute();
    if (result) {
      // Success if operations were performed (inserted or matched)
      return true;
    }
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }

  CRANE_LOGGER_ERROR(m_logger_, "Failed to insert in-memory TaskInCtld.");
  return false;
}

bool MongodbClient::FetchJobRecords(
    const crane::grpc::QueryTasksInfoRequest* request,
    std::unordered_map<job_id_t, crane::grpc::TaskInfo>* job_info_map,
    size_t limit) {
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

  bool has_task_ids_constraint = !request->filter_ids().empty();
  if (has_task_ids_constraint) {
    filter.append(kvp("task_id", [&request](sub_document task_id_doc) {
      array task_id_array;
      for (const auto& task_id : request->filter_ids() | std::views::keys) {
        task_id_array.append(static_cast<std::int32_t>(task_id));
      }
      task_id_doc.append(kvp("$in", task_id_array));
    }));
  }

  bool has_task_status_constraint = !request->filter_states().empty();
  if (has_task_status_constraint) {
    filter.append(kvp("state", [&request](sub_document state_doc) {
      array state_array;
      for (const auto& state : request->filter_states()) {
        state_array.append(state);
      }
      state_doc.append(kvp("$in", state_array));
    }));
  }

  bool has_nodename_list_constraint = !request->filter_nodename_list().empty();
  if (has_nodename_list_constraint) {
    filter.append(
        kvp("nodename_list", [&request](sub_document nodename_list_doc) {
          array nodename_list_array;
          for (const auto& nodename : request->filter_nodename_list()) {
            nodename_list_array.append(nodename);
          }
          nodename_list_doc.append(kvp("$in", nodename_list_array));
        }));
  }

  bool has_task_types_constraint = !request->filter_task_types().empty();
  if (has_task_types_constraint) {
    filter.append(kvp("type", [&request](sub_document type_doc) {
      array type_array;
      for (const auto& type : request->filter_task_types()) {
        type_array.append(static_cast<std::int32_t>(type));
      }
      type_doc.append(kvp("$in", type_array));
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
  // 10 id_group      nodelist       nodes_alloc    node_inx      partition_name
  // 15 priority      time_eligible  time_start     time_end      time_suspended
  // 20 script        state          timelimit      time_submit   work_dir
  // 25 submit_line   exit_code      username       qos           get_user_env
  // 30 type          extra_attr     reservation    exclusive     cpus_alloc
  // 35 mem_alloc     device_map     meta_pod       meta_container has_job_info
  // 40 nodename_list wckey
  try {
    for (auto view : cursor) {
      job_id_t job_id = view["task_id"].get_int32().value;
      crane::grpc::TaskInfo* job_info_ptr = nullptr;
      auto in_mem_job_it = job_info_map->find(job_id);
      bool has_in_mem_job_info = in_mem_job_it != job_info_map->end();
      bool has_db_job_info = ViewValueOr_(view["has_job_info"], false);
      if (!has_in_mem_job_info) {
        // Only got step info in db, no such jobinfo in db or memory, this is an
        // incomplete record, skip.
        if (!has_db_job_info) continue;
        crane::grpc::TaskInfo job_info;
        job_info.set_type(
            static_cast<crane::grpc::TaskType>(view["type"].get_int32().value));
        job_info.set_task_id(job_id);

        job_info.set_node_num(view["nodes_alloc"].get_int32().value);

        job_info.set_account(view["account"].get_string().value.data());
        job_info.set_username(view["username"].get_string().value.data());

        auto* mutable_req_res_view = job_info.mutable_req_res_view();
        auto* mutable_req_alloc_res =
            mutable_req_res_view->mutable_allocatable_res();
        mutable_req_alloc_res->set_cpu_core_limit(
            view["cpus_req"].get_double().value);
        mutable_req_alloc_res->set_memory_limit_bytes(
            view["mem_req"].get_int64().value);
        mutable_req_alloc_res->set_memory_sw_limit_bytes(
            view["mem_req"].get_int64().value);

        auto* mutable_allocated_res_view =
            job_info.mutable_allocated_res_view();
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
        job_info.set_name(std::string(view["task_name"].get_string().value));
        job_info.set_qos(std::string(view["qos"].get_string().value));
        job_info.set_uid(view["id_user"].get_int32().value);
        job_info.set_gid(view["id_group"].get_int32().value);
        job_info.set_craned_list(view["nodelist"].get_string().value.data());
        job_info.set_partition(
            std::string(view["partition_name"].get_string().value));

        job_info.mutable_start_time()->set_seconds(
            view["time_start"].get_int64().value);
        job_info.mutable_end_time()->set_seconds(
            view["time_end"].get_int64().value);

        job_info.set_status(static_cast<crane::grpc::TaskStatus>(
            view["state"].get_int32().value));
        job_info.mutable_time_limit()->set_seconds(
            view["timelimit"].get_int64().value);
        job_info.mutable_submit_time()->set_seconds(
            view["time_submit"].get_int64().value);
        job_info.set_cwd(std::string(view["work_dir"].get_string().value));
        if (view["submit_line"])
          job_info.set_cmd_line(
              std::string(view["submit_line"].get_string().value));
        job_info.set_exit_code(view["exit_code"].get_int32().value);

        job_info.set_extra_attr(view["extra_attr"].get_string().value.data());

        job_info.set_priority(view["priority"].get_int64().value);

        if (view.find("reservation") != view.end()) {
          job_info.set_reservation(
              view["reservation"].get_string().value.data());
        }
        job_info.set_exclusive(view["exclusive"].get_bool().value);

        if (job_info.type() == crane::grpc::Container) {
          if (auto pod_elem = view["meta_pod"];
              pod_elem && pod_elem.type() == bsoncxx::type::k_document) {
            job_info.mutable_pod_meta()->CopyFrom(
                static_cast<crane::grpc::PodTaskAdditionalMeta>(
                    BsonToPodMeta(view)));
          } else {
            CRANE_ERROR("Container job {} missing pod meta in db record!",
                        job_id);
          }
        }

        auto [it, present] = job_info_map->emplace(job_id, std::move(job_info));
        job_info_ptr = &it->second;
      } else {
        job_info_ptr = &in_mem_job_it->second;
      }
      auto steps_elem = view["steps"];
      if (!steps_elem || steps_elem.type() != bsoncxx::type::k_array) continue;
      for (const auto& elem : steps_elem.get_array().value) {
        auto* step_info = job_info_ptr->add_step_info_list();
        ViewToStepInfo_(elem.get_document().value, step_info);
        step_info->set_job_id(job_id);
      }
    }
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }

  return true;
}

bool MongodbClient::FetchJobStepRecords(
    const crane::grpc::QueryTasksInfoRequest* request,
    std::unordered_map<job_id_t, crane::grpc::TaskInfo>* job_info_map) {
  if (job_info_map->empty()) return true;
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

  filter.append(kvp("task_id", [&job_info_map](sub_document task_id_doc) {
    array task_id_array;
    for (const auto job_id : *job_info_map | std::views::keys) {
      task_id_array.append(static_cast<std::int32_t>(job_id));
    }
    task_id_doc.append(kvp("$in", task_id_array));
  }));

  bool has_task_status_constraint = !request->filter_states().empty();
  if (has_task_status_constraint) {
    filter.append(kvp("state", [&request](sub_document state_doc) {
      array state_array;
      for (const auto& state : request->filter_states()) {
        state_array.append(state);
      }
      state_doc.append(kvp("$in", state_array));
    }));
  }

  bool has_task_types_constraint = !request->filter_task_types().empty();
  if (has_task_types_constraint) {
    filter.append(kvp("type", [&request](sub_document type_doc) {
      array type_array;
      for (const auto& type : request->filter_task_types()) {
        type_array.append(static_cast<std::int32_t>(type));
      }
      type_doc.append(kvp("$in", type_array));
    }));
  }

  mongocxx::options::find option;

  document sort_doc;
  sort_doc.append(kvp("task_db_id", -1));
  option = option.sort(sort_doc.view());

  mongocxx::cursor cursor =
      (*GetClient_())[m_db_name_][m_task_collection_name_].find(filter.view(),
                                                                option);

  // 0  task_id       task_db_id     mod_time       deleted       account
  // 5  cpus_req      mem_req        task_name      env           id_user
  // 10 id_group      nodelist       nodes_alloc    node_inx      partition_name
  // 15 priority      time_eligible  time_start     time_end      time_suspended
  // 20 script        state          timelimit      time_submit   work_dir
  // 25 submit_line   exit_code      username       qos           get_user_env
  // 30 type          extra_attr     reservation    exclusive     cpus_alloc
  // 35 mem_alloc     device_map     meta_container has_job_info

  try {
    for (auto view : cursor) {
      job_id_t job_id = view["task_id"].get_int32().value;
      auto in_mem_job_it = job_info_map->find(job_id);
      bool has_in_mem_job_info = in_mem_job_it != job_info_map->end();
      if (!has_in_mem_job_info) {
        CRANE_ERROR(
            "Trying to fetch step records for non-existing job #{} in mem.",
            job_id);
        continue;
      }
      auto* job_info_ptr = &in_mem_job_it->second;

      auto steps_elem = view["steps"];
      if (!steps_elem || steps_elem.type() != bsoncxx::type::k_array) continue;
      for (const auto& elem : steps_elem.get_array().value) {
        auto* step_info = job_info_ptr->add_step_info_list();
        ViewToStepInfo_(elem.get_document().value, step_info);
        step_info->set_job_id(job_id);
      }

      auto* mutable_licenses = job_info_ptr->mutable_licenses_count();
      for (auto&& elem :
           ViewValueOr_(view["licenses_alloc"],
                        bsoncxx::builder::basic::make_document().view())) {
        mutable_licenses->emplace(std::string(elem.key()),
                                  elem.get_int32().value);
      }

      job_info_ptr->set_wckey(ViewValueOr_(view["wckey"], std::string("")));
    }
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }

  return true;
}

bool MongodbClient::CheckTaskDbIdExisted(int64_t task_db_id) {
  document doc;
  doc.append(kvp("job_db_inx", task_db_id));

  try {
    bsoncxx::stdx::optional<bsoncxx::document::value> result =
        (*GetClient_())[m_db_name_][m_task_collection_name_].find_one(
            doc.view());
    if (result) {
      return true;
    }
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }

  return false;
}

std::unordered_map<
    task_id_t, std::tuple<crane::grpc::TaskStatus, uint32_t, int64_t, int64_t>>
MongodbClient::FetchJobStatus(const std::unordered_set<task_id_t>& job_ids) {
  std::unordered_map<task_id_t, std::tuple<crane::grpc::TaskStatus, uint32_t,
                                           int64_t, int64_t>>
      result;

  if (job_ids.empty()) {
    return result;
  }

  try {
    document filter;
    filter.append(kvp("task_id", [&job_ids](sub_document task_id_doc) {
      array task_id_array;
      for (const auto& job_id : job_ids) {
        task_id_array.append(static_cast<std::int32_t>(job_id));
      }
      task_id_doc.append(kvp("$in", task_id_array));
    }));

    mongocxx::options::find options;
    document projection;
    projection.append(kvp("task_id", 1));
    projection.append(kvp("state", 1));
    projection.append(kvp("exit_code", 1));
    projection.append(kvp("time_end", 1));
    projection.append(kvp("time_start", 1));
    options.projection(projection.view());

    mongocxx::cursor cursor =
        (*GetClient_())[m_db_name_][m_task_collection_name_].find(filter.view(),
                                                                  options);

    for (auto view : cursor) {
      task_id_t task_id = view["task_id"].get_int32().value;
      crane::grpc::TaskStatus status =
          static_cast<crane::grpc::TaskStatus>(view["state"].get_int32().value);
      uint32_t exit_code = view["exit_code"].get_int32().value;
      int64_t time_end = view["time_end"].get_int64().value;
      int64_t time_start = view["time_start"].get_int64().value;

      result.emplace(task_id,
                     std::make_tuple(status, exit_code, time_end, time_start));
    }
  } catch (const std::exception& e) {
    CRANE_ERROR("Failed to fetch job status by IDs: {}", e.what());
  }

  return result;
}

bool MongodbClient::InsertRecoveredStep(
    crane::grpc::StepInEmbeddedDb const& step_in_embedded_db) {
  document step_doc = StepInEmbeddedDbToDocument_(step_in_embedded_db);
  job_id_t job_id = step_in_embedded_db.step_to_ctld().job_id();

  // Filter by task_id (job_id)
  document filter;
  filter.append(kvp("task_id", static_cast<int32_t>(job_id)));

  // Use $push to append step, and $setOnInsert to create minimal document if
  // needed
  document update_doc;
  update_doc.append(kvp("$push", [&step_doc](sub_document push_doc) {
    push_doc.append(kvp("steps", step_doc));
  }));
  update_doc.append(kvp("$setOnInsert", [&job_id](sub_document set_on_insert) {
    set_on_insert.append(kvp("task_id", static_cast<int32_t>(job_id)));
  }));

  try {
    bsoncxx::stdx::optional<mongocxx::result::update> ret =
        (*GetClient_())[m_db_name_][m_task_collection_name_].update_one(
            *GetSession_(), filter.view(), update_doc.view(),
            mongocxx::options::update{}.upsert(true));

    if (ret != bsoncxx::stdx::nullopt) return true;
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
  CRANE_LOGGER_ERROR(m_logger_, "Failed to insert in-memory StepInCtld.");
  return false;
}

bool MongodbClient::InsertSteps(const std::unordered_set<StepInCtld*>& steps) {
  if (steps.empty()) return false;

  std::unordered_map<job_id_t, std::vector<StepInCtld*>> steps_by_job;
  for (const auto& step : steps) {
    steps_by_job[step->job_id].push_back(step);
  }

  mongocxx::options::bulk_write bulk_options;

  try {
    auto bulk =
        (*GetClient_())[m_db_name_][m_task_collection_name_].create_bulk_write(
            *GetSession_(), bulk_options);

    for (const auto& [job_id, job_steps] : steps_by_job) {
      // Build array of step documents
      array steps_array;
      for (const auto& step : job_steps) {
        document step_doc = StepInCtldToDocument_(step);
        steps_array.append(step_doc);
      }

      // Filter by task_id (job_id)
      document filter;
      filter.append(kvp("task_id", static_cast<int32_t>(job_id)));

      // Combine $push and $setOnInsert
      // $push: append steps to existing document
      // $setOnInsert: create minimal document if it doesn't exist
      document update_doc;
      update_doc.append(kvp("$push", [&steps_array](sub_document push_doc) {
        push_doc.append(kvp("steps", [&steps_array](sub_document each_doc) {
          each_doc.append(kvp("$each", steps_array));
        }));
      }));
      update_doc.append(
          kvp("$setOnInsert", [&job_id](sub_document set_on_insert) {
            set_on_insert.append(kvp("task_id", static_cast<int32_t>(job_id)));
          }));

      mongocxx::model::update_one update_op{filter.view(), update_doc.view()};
      update_op.upsert(true);

      bulk.append(update_op);
    }

    auto result = bulk.execute();
    if (result) {
      return true;
    }
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }

  CRANE_LOGGER_ERROR(m_logger_, "Failed to insert in-memory StepInCtld.");
  return false;
}

bool MongodbClient::CheckStepExisted(job_id_t job_id, step_id_t step_id) {
  // Query for a job with the given task_id that contains a step with the given
  // step_id
  document filter;
  filter.append(kvp("task_id", static_cast<int32_t>(job_id)));
  filter.append(kvp("steps.step_id", static_cast<int32_t>(step_id)));

  try {
    bsoncxx::stdx::optional<bsoncxx::document::value> result =
        (*GetClient_())[m_db_name_][m_task_collection_name_].find_one(
            filter.view());
    if (result) {
      return true;
    }
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }

  return false;
}

bool MongodbClient::InsertUser(const Ctld::User& new_user) {
  document doc = UserToDocument_(new_user);
  doc.append(kvp("creation_time", ToUnixSeconds(absl::Now())));

  try {
    bsoncxx::stdx::optional<mongocxx::result::insert_one> ret =
        (*GetClient_())[m_db_name_][m_user_collection_name_].insert_one(
            *GetSession_(), doc.view());

    if (ret != bsoncxx::stdx::nullopt) return true;
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
  return false;
}

bool MongodbClient::InsertWckey(const Ctld::Wckey& new_wckey) {
  document doc = WckeyToDocument_(new_wckey);
  doc.append(kvp("creation_time", ToUnixSeconds(absl::Now())));
  try {
    bsoncxx::stdx::optional<mongocxx::result::insert_one> ret =
        (*GetClient_())[m_db_name_][m_wckey_collection_name_].insert_one(
            *GetSession_(), doc.view());

    if (ret != bsoncxx::stdx::nullopt) return true;
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
  return false;
}

bool MongodbClient::InsertAccount(const Ctld::Account& new_account) {
  document doc = AccountToDocument_(new_account);
  doc.append(kvp("creation_time", ToUnixSeconds(absl::Now())));

  try {
    bsoncxx::stdx::optional<mongocxx::result::insert_one> ret =
        (*GetClient_())[m_db_name_][m_account_collection_name_].insert_one(
            *GetSession_(), doc.view());

    if (ret != bsoncxx::stdx::nullopt) return true;
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
  return false;
}

bool MongodbClient::InsertQos(const Ctld::Qos& new_qos) {
  document doc = QosToDocument_(new_qos);
  doc.append(kvp("creation_time", ToUnixSeconds(absl::Now())));

  try {
    bsoncxx::stdx::optional<mongocxx::result::insert_one> ret =
        (*GetClient_())[m_db_name_][m_qos_collection_name_].insert_one(
            doc.view());

    if (ret != bsoncxx::stdx::nullopt) return true;
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
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
  case EntityType::WCKEY:
    coll = m_wckey_collection_name_;
    break;
  default:
    CRANE_ERROR("Invalid entity type {}.", static_cast<int>(type));
    return false;
  }
  document filter;
  filter.append(kvp("name", name));
  try {
    bsoncxx::stdx::optional<mongocxx::result::delete_result> result =
        (*GetClient_())[m_db_name_][coll].delete_one(filter.view());

    if (result && result.value().deleted_count() == 1) return true;
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
  return false;
}

template <typename T>
bool MongodbClient::SelectUser(const std::string& key, const T& value,
                               Ctld::User* user) {
  document doc;
  doc.append(kvp(key, value));
  try {
    bsoncxx::stdx::optional<bsoncxx::document::value> result =
        (*GetClient_())[m_db_name_][m_user_collection_name_].find_one(
            doc.view());

    if (result) {
      bsoncxx::document::view user_view = result->view();
      ViewToUser_(user_view, user);
      return true;
    }
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
  return false;
}

void MongodbClient::SelectAllUser(std::list<Ctld::User>* user_list) {
  try {
    mongocxx::cursor cursor =
        (*GetClient_())[m_db_name_][m_user_collection_name_].find({});
    for (auto view : cursor) {
      Ctld::User user;
      ViewToUser_(view, &user);
      user_list->emplace_back(user);
    }
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
}

void MongodbClient::SelectAllWckey(std::list<Ctld::Wckey>* wckey_list) {
  try {
    mongocxx::cursor cursor =
        (*GetClient_())[m_db_name_][m_wckey_collection_name_].find({});
    for (auto view : cursor) {
      Ctld::Wckey wckey;
      ViewToWckey_(view, &wckey);
      wckey_list->emplace_back(wckey);
    }
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
}

void MongodbClient::SelectAllAccount(std::list<Ctld::Account>* account_list) {
  try {
    mongocxx::cursor cursor =
        (*GetClient_())[m_db_name_][m_account_collection_name_].find({});
    for (auto view : cursor) {
      Ctld::Account account;
      ViewToAccount_(view, &account);
      account_list->emplace_back(account);
    }
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
}

void MongodbClient::SelectAllQos(std::list<Ctld::Qos>* qos_list) {
  try {
    mongocxx::cursor cursor =
        (*GetClient_())[m_db_name_][m_qos_collection_name_].find({});
    for (auto view : cursor) {
      Ctld::Qos qos;
      ViewToQos_(view, &qos);
      qos_list->emplace_back(qos);
    }
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
}

bool MongodbClient::UpdateUser(const Ctld::User& user) {
  document doc = UserToDocument_(user), set_document, filter;
  doc.append(kvp("mod_time", ToUnixSeconds(absl::Now())));
  set_document.append(kvp("$set", doc));

  filter.append(kvp("name", user.name));

  try {
    bsoncxx::stdx::optional<mongocxx::result::update> update_result =
        (*GetClient_())[m_db_name_][m_user_collection_name_].update_one(
            *GetSession_(), filter.view(), set_document.view());

    if (!update_result || update_result->modified_count() == 0) {
      return false;
    }
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
  return true;
}

bool MongodbClient::UpdateWckey(const Wckey& wckey) {
  document doc = WckeyToDocument_(wckey), set_document, filter;

  doc.append(kvp("mod_time", ToUnixSeconds(absl::Now())));
  set_document.append(kvp("$set", doc));

  filter.append(kvp("name", wckey.name));
  filter.append(kvp("user_name", wckey.user_name));

  try {
    auto update_result =
        (*GetClient_())[m_db_name_][m_wckey_collection_name_].update_one(
            *GetSession_(), filter.view(), set_document.view());

    if (!update_result || !update_result->modified_count()) {
      return false;
    }
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
  return true;
}

bool MongodbClient::UpdateAccount(const Ctld::Account& account) {
  document doc = AccountToDocument_(account), set_document, filter;
  doc.append(kvp("mod_time", ToUnixSeconds(absl::Now())));
  set_document.append(kvp("$set", doc));

  filter.append(kvp("name", account.name));

  try {
    bsoncxx::stdx::optional<mongocxx::result::update> update_result =
        (*GetClient_())[m_db_name_][m_account_collection_name_].update_one(
            *GetSession_(), filter.view(), set_document.view());

    if (!update_result || update_result->modified_count() == 0) {
      return false;
    }
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
  return true;
}

bool MongodbClient::UpdateQos(const Ctld::Qos& qos) {
  document doc = QosToDocument_(qos), set_document, filter;
  doc.append(kvp("mod_time", ToUnixSeconds(absl::Now())));
  set_document.append(kvp("$set", doc));

  filter.append(kvp("name", qos.name));

  try {
    bsoncxx::stdx::optional<mongocxx::result::update> update_result =
        (*GetClient_())[m_db_name_][m_qos_collection_name_].update_one(
            filter.view(), set_document.view());

    if (!update_result || update_result->modified_count() == 0) {
      return false;
    }
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
  return true;
}

bool MongodbClient::InsertTxn(const Txn& txn) {
  document doc = TxnToDocument_(txn);

  bsoncxx::stdx::optional<mongocxx::result::insert_one> ret =
      (*GetClient_())[m_db_name_][m_txn_collection_name_].insert_one(
          *GetSession_(), doc.view());

  return ret != bsoncxx::stdx::nullopt;
}

void MongodbClient::SelectTxns(
    const std::unordered_map<std::string, std::string>& conditions,
    int64_t start_time, int64_t end_time, std::list<Txn>* res_txn) {
  bsoncxx::builder::basic::document doc_builder;

  if (start_time != 0 || end_time != 0) {
    bsoncxx::builder::basic::document range_doc;
    if (start_time != 0) range_doc.append(kvp("$gte", start_time));
    if (end_time != 0) range_doc.append(kvp("$lte", end_time));
    doc_builder.append(kvp("creation_time", range_doc.view()));
  }

  for (const auto& [key, value] : conditions) {
    if (key == "action") {
      try {
        // TxnAction is stored as int32; match type to avoid numeric-collation
        // corner cases.
        int32_t v = static_cast<int32_t>(std::stol(value));
        doc_builder.append(kvp(key, v));
      } catch (const std::exception&) {
        CRANE_LOGGER_ERROR(
            m_logger_, "Invalid action value '{}'; ignoring filter.", value);
      }
    } else if (key == "info") {
      doc_builder.append(kvp(
          key, bsoncxx::builder::basic::make_document(kvp("$regex", value))));
    } else
      doc_builder.append(kvp(key, value));
  }

  mongocxx::options::find find_options;
  // TODO: page query?
  find_options.limit(1000);

  // Return newest-first deterministically.
  bsoncxx::builder::basic::document sort_doc;
  sort_doc.append(kvp("creation_time", -1));
  find_options.sort(sort_doc.view());
  mongocxx::cursor cursor =
      (*GetClient_())[m_db_name_][m_txn_collection_name_].find(
          doc_builder.view(), find_options);

  for (auto view : cursor) {
    Txn txn;
    ViewToTxn_(view, &txn);
    res_txn->emplace_back(txn);
  }
}

bool MongodbClient::InsertLicenseResource(const LicenseResourceInDb& resource) {
  document doc = LicenseResourceToDocument_(resource);
  doc.append(kvp("creation_time", ToUnixSeconds(absl::Now())));

  try {
    bsoncxx::stdx::optional<mongocxx::result::insert_one> ret =
        (*GetClient_())[m_db_name_][m_license_resource_collection_name_]
            .insert_one(*GetSession_(), doc.view());

    if (ret != bsoncxx::stdx::nullopt) return true;
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }

  return false;
}

bool MongodbClient::UpdateLicenseResource(const LicenseResourceInDb& resource) {
  document doc = LicenseResourceToDocument_(resource), set_document, filter;

  doc.append(kvp("mod_time", ToUnixSeconds(absl::Now())));
  set_document.append(kvp("$set", doc));

  filter.append(kvp("name", resource.name));
  filter.append(kvp("server", resource.server));

  try {
    bsoncxx::stdx::optional<mongocxx::result::update> update_result =
        (*GetClient_())[m_db_name_][m_license_resource_collection_name_]
            .update_one(*GetSession_(), filter.view(), set_document.view());

    if (!update_result || update_result->modified_count() == 0) {
      return false;
    }
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
    return false;
  }

  return true;
}

bool MongodbClient::DeleteLicenseResource(const std::string& resource_name,
                                          const std::string& server) {
  document filter;
  filter.append(kvp("name", resource_name));
  filter.append(kvp("server", server));

  try {
    bsoncxx::stdx::optional<mongocxx::result::delete_result> result =
        (*GetClient_())[m_db_name_][m_license_resource_collection_name_]
            .delete_one(*GetSession_(), filter.view());

    if (result && result.value().deleted_count() == 1) return true;
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }

  return false;
}

void MongodbClient::SelectAllLicenseResource(
    std::list<LicenseResourceInDb>* resource_list) {
  try {
    mongocxx::cursor cursor =
        (*GetClient_())[m_db_name_][m_license_resource_collection_name_].find(
            {});
    for (auto view : cursor) {
      LicenseResourceInDb resource;
      ViewToLicenseResource_(view, &resource);
      resource_list->emplace_back(resource);
    }
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
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
  doc.append(kvp(key, [&value, this](sub_document map_value_doc) {
    for (const auto& map_item : value) {
      map_value_doc.append(
          kvp(map_item.first, [&map_item, this](sub_document item_doc) {
            item_doc.append(kvp("blocked", map_item.second.blocked));
            SubDocumentAppendItem_(item_doc, "allowed_partition_qos_map",
                                   map_item.second.allowed_partition_qos_map);
          }));
    }
  }));
}

template <>
void MongodbClient::DocumentAppendItem_<
    std::unordered_map<std::string, std::string>>(
    document& doc, const std::string& key,
    const std::unordered_map<std::string, std::string>& value) {
  doc.append(kvp(key, [&value](sub_document subdoc) {
    for (const auto& [k, v] : value) {
      subdoc.append(kvp(k, v));
    }
  }));
}

template <>
void MongodbClient::SubDocumentAppendItem_<User::PartToAllowedQosMap>(
    sub_document& doc, const std::string& key,
    const User::PartToAllowedQosMap& value) {
  doc.append(kvp(key, [&value](sub_document map_value_document) {
    for (const auto& map_item : value) {
      auto map_key = map_item.first;
      auto map_value = map_item.second;

      map_value_document.append(
          kvp(map_key, [&map_value](sub_array pair_array) {
            pair_array.append(map_value.first);  // pair->first, default qos

            array list_array;
            for (const auto& s : map_value.second) list_array.append(s);
            pair_array.append(list_array);
          }));
    }
  }));
}

template <>
void MongodbClient::DocumentAppendItem_<DeviceMap>(document& doc,
                                                   const std::string& key,
                                                   const DeviceMap& value) {
  doc.append(kvp(key, [&value](sub_document map_value_document) {
    for (const auto& map_item : value) {
      const auto& device_name = map_item.first;
      const auto& pair_val = map_item.second;
      uint64_t total = pair_val.first;
      const auto& type_count_map = pair_val.second;

      map_value_document.append(
          kvp(device_name, [&total, &type_count_map](sub_document device_doc) {
            device_doc.append(kvp("total", static_cast<int64_t>(total)));
            device_doc.append(kvp("type_count_map", [&type_count_map](
                                                        sub_document type_doc) {
              for (const auto& type_item : type_count_map) {
                type_doc.append(kvp(type_item.first,
                                    static_cast<int64_t>(type_item.second)));
              }
            }));
          }));
    }
  }));
}

template <>
void MongodbClient::DocumentAppendItem_<std::vector<gid_t>>(
    document& doc, const std::string& key, const std::vector<gid_t>& value) {
  using bsoncxx::builder::basic::array;
  array arr_builder;

  for (const auto& v : value) {
    arr_builder.append(static_cast<int32_t>(v));
  }
  doc.append(kvp(key, arr_builder));
}

template <>
void MongodbClient::DocumentAppendItem_<DedicatedResourceInNode>(
    document& doc, const std::string& key,
    const DedicatedResourceInNode& value) {
  doc.append(kvp(key, [&value](sub_document subDoc) {
    for (const auto& [name, type_slots_map] : value.name_type_slots_map) {
      subDoc.append(kvp(name, [&type_slots_map](sub_document typeDoc) {
        for (const auto& [type, slots] : type_slots_map.type_slots_map) {
          typeDoc.append(kvp(type, [&slots](sub_array arr) {
            for (auto dev_id : slots) arr.append(dev_id);
          }));
        }
      }));
    }
  }));
}

template <>
void MongodbClient::DocumentAppendItem_<ResourceInNode>(
    document& doc, const std::string& key, const ResourceInNode& value) {
  document sub_doc{};
  sub_doc.append(
      kvp("cpu", static_cast<double>(value.allocatable_res.cpu_count)));
  sub_doc.append(kvp(
      "memory", static_cast<std::int64_t>(value.allocatable_res.memory_bytes)));

  DocumentAppendItem_(sub_doc, "gres", value.dedicated_res);
  doc.append(kvp(key, sub_doc));
}

template <>
void MongodbClient::DocumentAppendItem_<ResourceV2>(document& doc,
                                                    const std::string& key,
                                                    const ResourceV2& value) {
  document node_res_doc{};
  for (const auto& [node, res] : value.EachNodeResMap()) {
    DocumentAppendItem_(node_res_doc, node, res);
  }
  doc.append(kvp(key, node_res_doc));
}

template <>
void MongodbClient::DocumentAppendItem_<std::optional<ContainerMetaInTask>>(
    document& doc, const std::string& key,
    const std::optional<ContainerMetaInTask>& value) {
  if (!value.has_value()) {
    doc.append(kvp(key, bsoncxx::types::b_null{}));
    return;
  }

  const auto& v = value.value();

  doc.append(kvp(key, [&v](sub_document container_doc) {
    // Basic fields
    container_doc.append(kvp("name", v.name));
    container_doc.append(kvp("command", v.command));
    container_doc.append(kvp("workdir", v.workdir));

    container_doc.append(kvp("detached", v.detached));
    container_doc.append(kvp("tty", v.tty));
    container_doc.append(kvp("stdin", v.stdin));
    container_doc.append(kvp("stdin_once", v.stdin_once));

    // Serialize args array
    container_doc.append(kvp("args", [&v](sub_array args_array) {
      for (const auto& arg : v.args) {
        args_array.append(arg);
      }
    }));

    // Serialize labels map
    container_doc.append(kvp("labels", [&v](sub_document labels_doc) {
      for (const auto& label : v.labels) {
        labels_doc.append(kvp(label.first, label.second));
      }
    }));

    // Serialize annotations map
    container_doc.append(kvp("annotations", [&v](sub_document annotations_doc) {
      for (const auto& annotation : v.annotations) {
        annotations_doc.append(kvp(annotation.first, annotation.second));
      }
    }));

    // Serialize env map
    container_doc.append(kvp("env", [&v](sub_document env_doc) {
      for (const auto& env_var : v.env) {
        env_doc.append(kvp(env_var.first, env_var.second));
      }
    }));

    // Serialize mounts map
    container_doc.append(kvp("mounts", [&v](sub_document mounts_doc) {
      for (const auto& mount : v.mounts) {
        mounts_doc.append(kvp(mount.first, mount.second));
      }
    }));

    // Serialize ImageInfo
    container_doc.append(kvp("image_info", [&v](sub_document image_doc) {
      image_doc.append(kvp("image", v.image_info.image));
      image_doc.append(kvp("pull_policy", v.image_info.pull_policy));
      image_doc.append(kvp("server_address", v.image_info.server_address));
      // NOTE: We DO NOT serialize auth related fields (username/password) for
      // security, which means when a job is done, no credentials in disk.
    }));
  }));
}

template <>
void MongodbClient::DocumentAppendItem_<std::optional<PodMetaInTask>>(
    document& doc, const std::string& key,
    const std::optional<PodMetaInTask>& value) {
  if (!value.has_value()) {
    doc.append(kvp(key, bsoncxx::types::b_null{}));
    return;
  }

  const auto& v = value.value();
  doc.append(kvp(key, [&v](sub_document pod_doc) {
    pod_doc.append(kvp("name", v.name));

    pod_doc.append(kvp("labels", [&v](sub_document labels_doc) {
      for (const auto& label : v.labels) {
        labels_doc.append(kvp(label.first, label.second));
      }
    }));

    pod_doc.append(kvp("annotations", [&v](sub_document annotations_doc) {
      for (const auto& annotation : v.annotations) {
        annotations_doc.append(kvp(annotation.first, annotation.second));
      }
    }));

    pod_doc.append(kvp("namespace", [&v](sub_document ns_doc) {
      ns_doc.append(
          kvp("network", static_cast<int32_t>(v.namespace_option.network)));
      ns_doc.append(kvp("pid", static_cast<int32_t>(v.namespace_option.pid)));
      ns_doc.append(kvp("ipc", static_cast<int32_t>(v.namespace_option.ipc)));
      ns_doc.append(kvp("target_id", v.namespace_option.target_id));
    }));

    pod_doc.append(kvp("userns", v.userns));
    pod_doc.append(kvp("run_as_user", static_cast<int32_t>(v.run_as_user)));
    pod_doc.append(kvp("run_as_group", static_cast<int32_t>(v.run_as_group)));

    if (!v.port_mappings.empty()) {
      pod_doc.append(kvp("ports", [&v](sub_array ports_array) {
        for (const auto& pm : v.port_mappings) {
          ports_array.append([&pm](sub_document ports_doc) {
            ports_doc.append(
                kvp("protocol", static_cast<int32_t>(pm.protocol)));
            ports_doc.append(kvp("container_port", pm.container_port));
            ports_doc.append(kvp("host_port", pm.host_port));
            ports_doc.append(kvp("host_ip", pm.host_ip));
          });
        }
      }));
    }
  }));
}

template <>
void MongodbClient::DocumentAppendItem_<
    std::unordered_map<std::string, uint32_t>>(
    document& doc, const std::string& key,
    const std::unordered_map<std::string, uint32_t>& value) {
  doc.append(kvp(key, [&value](sub_document sub_doc) {
    for (const auto& [k, v] : value) {
      sub_doc.append(kvp(k, static_cast<int32_t>(v)));
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
          User::AttrsInAccount{
              .allowed_partition_qos_map = std::move(temp),
              .blocked = account_to_attrs_map_item["blocked"].get_bool()};
    }

    user->cert_number = ViewValueOr_(user_view["cert_number"], std::string{""});

    user->default_wckey =
        ViewValueOr_(user_view["default_wckey"], std::string{""});
  } catch (const bsoncxx::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
}

bsoncxx::builder::basic::document MongodbClient::UserToDocument_(
    const Ctld::User& user) {
  std::array<std::string, 9> fields{"deleted",
                                    "uid",
                                    "default_account",
                                    "name",
                                    "admin_level",
                                    "account_to_attrs_map",
                                    "coordinator_accounts",
                                    "cert_number",
                                    "default_wckey"};
  std::tuple<bool, int64_t, std::string, std::string, int32_t,
             User::AccountToAttrsMap, std::list<std::string>, std::string,
             std::string>
      values{user.deleted,
             user.uid,
             user.default_account,
             user.name,
             user.admin_level,
             user.account_to_attrs_map,
             user.coordinator_accounts,
             user.cert_number,
             user.default_wckey};
  return DocumentConstructor_(fields, values);
}

bsoncxx::builder::basic::document MongodbClient::WckeyToDocument_(
    const Ctld::Wckey& wckey) {
  std::array<std::string, 4> fields{
      "deleted",
      "name",
      "user_name",
      "is_default",
  };
  std::tuple<bool, std::string, std::string, bool> values{
      wckey.deleted, wckey.name, wckey.user_name, wckey.is_default};
  return DocumentConstructor_(fields, values);
}

void MongodbClient::ViewToWckey_(const bsoncxx::document::view& wckey_view,
                                 Ctld::Wckey* wckey) {
  try {
    wckey->deleted = wckey_view["deleted"].get_bool().value;
    wckey->name = wckey_view["name"].get_string().value;
    wckey->user_name = wckey_view["user_name"].get_string().value;
    wckey->is_default = wckey_view["is_default"].get_bool().value;
  } catch (const bsoncxx::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
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

void MongodbClient::ViewToTxn_(const bsoncxx::document::view& txn_view,
                               Txn* txn) {
  try {
    txn->actor = txn_view["actor"].get_string().value;
    txn->target = txn_view["target"].get_string().value;
    txn->action = static_cast<crane::grpc::TxnAction>(
        txn_view["action"].get_int32().value);
    txn->creation_time = txn_view["creation_time"].get_int64().value;
    txn->info = txn_view["info"].get_string().value;
  } catch (const bsoncxx::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
}

MongodbClient::document MongodbClient::TxnToDocument_(const Txn& txn) {
  std::array<std::string, 5> fields{
      "creation_time", "actor", "target", "action", "info",
  };
  std::tuple<int64_t, std::string, std::string, int32_t, std::string> values{
      txn.creation_time, txn.actor, txn.target, txn.action, txn.info};

  return DocumentConstructor_(fields, values);
}

void MongodbClient::ViewToLicenseResource_(
    const bsoncxx::document::view& resource_view,
    LicenseResourceInDb* resource) {
  try {
    resource->name = ViewValueOr_(resource_view["name"], std::string{});
    resource->server = ViewValueOr_(resource_view["server"], std::string{});
    resource->server_type =
        ViewValueOr_(resource_view["server_type"], std::string{});
    resource->type = static_cast<crane::grpc::LicenseResource::Type>(
        ViewValueOr_(resource_view["type"], 0));
    resource->allocated = ViewValueOr_(resource_view["allocated"], 0);
    resource->total_resource_count = ViewValueOr_(resource_view["count"], 0);
    resource->flags = ViewValueOr_(resource_view["flags"], 0);
    resource->last_consumed = ViewValueOr_(resource_view["last_consumed"], 0);
    resource->last_update = absl::FromUnixSeconds(
        ViewValueOr_(resource_view["last_update"], int64_t(0)));
    resource->description =
        ViewValueOr_(resource_view["description"], std::string{});

    for (auto&& elem :
         ViewValueOr_(resource_view["cluster_resources"],
                      bsoncxx::builder::basic::make_document().view())) {
      resource->cluster_resources.emplace(std::string(elem.key()),
                                          elem.get_int32().value);
    }

  } catch (const bsoncxx::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
}

MongodbClient::document MongodbClient::LicenseResourceToDocument_(
    const LicenseResourceInDb& resource) {
  std::array<std::string, 11> fields{
      "name",      "server",        "server_type",       "type",
      "allocated", "last_consumed", "cluster_resources", "count",
      "flags",     "last_update",   "description"};

  std::tuple<std::string, std::string, std::string, int32_t, int32_t, int32_t,
             std::unordered_map<std::string, uint32_t>, int32_t, int32_t,
             int64_t, std::string>
      values{resource.name,
             resource.server,
             resource.server_type,
             static_cast<int32_t>(resource.type),
             resource.allocated,
             resource.last_consumed,
             resource.cluster_resources,
             resource.total_resource_count,
             static_cast<int32_t>(resource.flags),
             absl::ToUnixSeconds(resource.last_update),
             resource.description};

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
DedicatedResourceInNode MongodbClient::BsonToDedicatedResourceInNode(
    const bsoncxx::document::view& doc) {
  DedicatedResourceInNode res;
  try {
    for (auto&& name_elem : doc) {
      std::string name = std::string(name_elem.key());
      auto type_doc = name_elem.get_document().view();
      TypeSlotsMap type_slots_map;
      for (auto&& type_elem : type_doc) {
        std::string type = std::string(type_elem.key());
        auto slots_array = type_elem.get_array().value;
        std::set<SlotId> slots;
        for (auto&& slot_elem : slots_array)
          slots.emplace(std::string(slot_elem.get_string().value));
        type_slots_map.type_slots_map[type] = std::move(slots);
      }
      res.name_type_slots_map[name] = std::move(type_slots_map);
    }
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
  return res;
}
ResourceInNode MongodbClient::BsonToResourceInNode(
    const bsoncxx::document::view& doc) {
  ResourceInNode res;
  try {
    res.allocatable_res.cpu_count =
        static_cast<cpu_t>(doc["cpu"].get_double().value);
    res.allocatable_res.memory_bytes = doc["memory"].get_int64().value;
    res.allocatable_res.memory_sw_bytes = doc["memory"].get_int64().value;
    if (doc["gres"]) {
      res.dedicated_res =
          BsonToDedicatedResourceInNode(doc["gres"].get_document().view());
    }
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
  return res;
}

ResourceV2 MongodbClient::BsonToResourceV2(const bsoncxx::document::view& doc) {
  ResourceV2 res;
  try {
    for (auto&& node_elem : doc) {
      std::string node_name = std::string(node_elem.key());
      res.AddResourceInNode(
          node_name,
          BsonToResourceInNode(node_elem.get_value().get_document().value));
    }
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
  return res;
}

ContainerMetaInTask MongodbClient::BsonToContainerMeta(
    const bsoncxx::document::view& doc) {
  ContainerMetaInTask result;

  try {
    auto container_elem = doc["meta_container"];
    if (!container_elem || container_elem.type() != bsoncxx::type::k_document) {
      CRANE_LOGGER_ERROR(m_logger_,
                         "Error in reading container metadata for a container "
                         "task: Unexpected document type.");
      return result;
    }

    auto container_doc = container_elem.get_document().view();

    // Parse ImageInfo
    if (auto image_info_elem = container_doc["image_info"];
        image_info_elem &&
        image_info_elem.type() == bsoncxx::type::k_document) {
      auto image_doc = image_info_elem.get_document().view();
      if (auto image_elem = image_doc["image"]) {
        result.image_info.image = image_elem.get_string().value;
      }
      if (auto pull_policy_elem = image_doc["pull_policy"]) {
        result.image_info.pull_policy = pull_policy_elem.get_string().value;
      }
      if (auto server_elem = image_doc["server_address"]) {
        result.image_info.server_address = server_elem.get_string().value;
      }
      // NOTE: We do not deserialize auth fields (username/password) for
      // security
    }

    // Parse basic fields
    if (auto name_elem = container_doc["name"]) {
      result.name = name_elem.get_string().value;
    }
    if (auto command_elem = container_doc["command"]) {
      result.command = command_elem.get_string().value;
    }
    if (auto workdir_elem = container_doc["workdir"]) {
      result.workdir = workdir_elem.get_string().value;
    }

    if (auto detached_elem = container_doc["detached"]) {
      result.detached = detached_elem.get_bool().value;
    }
    if (auto tty_elem = container_doc["tty"]) {
      result.tty = tty_elem.get_bool().value;
    }
    if (auto stdin_elem = container_doc["stdin"]) {
      result.stdin = stdin_elem.get_bool().value;
    }
    if (auto stdin_once_elem = container_doc["stdin_once"]) {
      result.stdin_once = stdin_once_elem.get_bool().value;
    }

    // Parse args array
    if (auto args_elem = container_doc["args"];
        args_elem && args_elem.type() == bsoncxx::type::k_array) {
      for (const auto& arg : args_elem.get_array().value) {
        result.args.emplace_back(arg.get_string().value);
      }
    }

    // Parse labels map
    if (auto labels_elem = container_doc["labels"];
        labels_elem && labels_elem.type() == bsoncxx::type::k_document) {
      for (const auto& label : labels_elem.get_document().view()) {
        result.labels[std::string(label.key())] = label.get_string().value;
      }
    }

    // Parse annotations map
    if (auto annotations_elem = container_doc["annotations"];
        annotations_elem &&
        annotations_elem.type() == bsoncxx::type::k_document) {
      for (const auto& annotation : annotations_elem.get_document().view()) {
        result.annotations[std::string(annotation.key())] =
            annotation.get_string().value;
      }
    }

    // Parse env map
    if (auto env_elem = container_doc["env"];
        env_elem && env_elem.type() == bsoncxx::type::k_document) {
      for (const auto& env_var : env_elem.get_document().view()) {
        result.env[std::string(env_var.key())] = env_var.get_string().value;
      }
    }

    // Parse mounts map
    if (auto mounts_elem = container_doc["mounts"];
        mounts_elem && mounts_elem.type() == bsoncxx::type::k_document) {
      for (const auto& mount : mounts_elem.get_document().view()) {
        result.mounts[std::string(mount.key())] = mount.get_string().value;
      }
    }

  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }

  return result;
}

PodMetaInTask MongodbClient::BsonToPodMeta(const bsoncxx::document::view& doc) {
  PodMetaInTask result;
  try {
    auto pod_elem = doc["meta_pod"];
    if (!pod_elem || pod_elem.type() != bsoncxx::type::k_document) {
      return result;
    }
    auto pod_doc = pod_elem.get_document().view();

    if (auto name_elem = pod_doc["name"]) {
      result.name = name_elem.get_string().value;
    }

    if (auto labels_elem = pod_doc["labels"];
        labels_elem && labels_elem.type() == bsoncxx::type::k_document) {
      for (const auto& label : labels_elem.get_document().view()) {
        result.labels[std::string(label.key())] = label.get_string().value;
      }
    }

    if (auto annotations_elem = pod_doc["annotations"];
        annotations_elem &&
        annotations_elem.type() == bsoncxx::type::k_document) {
      for (const auto& annotation : annotations_elem.get_document().view()) {
        result.annotations[std::string(annotation.key())] =
            annotation.get_string().value;
      }
    }

    if (auto ns_elem = pod_doc["namespace"];
        ns_elem && ns_elem.type() == bsoncxx::type::k_document) {
      auto ns_doc = ns_elem.get_document().view();
      if (auto network_elem = ns_doc["network"]) {
        result.namespace_option.network =
            static_cast<crane::grpc::PodTaskAdditionalMeta::NamespaceMode>(
                network_elem.get_int32().value);
      }
      if (auto pid_elem = ns_doc["pid"]) {
        result.namespace_option.pid =
            static_cast<crane::grpc::PodTaskAdditionalMeta::NamespaceMode>(
                pid_elem.get_int32().value);
      }
      if (auto ipc_elem = ns_doc["ipc"]) {
        result.namespace_option.ipc =
            static_cast<crane::grpc::PodTaskAdditionalMeta::NamespaceMode>(
                ipc_elem.get_int32().value);
      }
      if (auto target_id_elem = ns_doc["target_id"]) {
        result.namespace_option.target_id = target_id_elem.get_string().value;
      }
    }

    if (auto userns_elem = pod_doc["userns"]) {
      result.userns = userns_elem.get_bool().value;
    }
    if (auto run_as_user_elem = pod_doc["run_as_user"]) {
      result.run_as_user =
          static_cast<uid_t>(run_as_user_elem.get_int32().value);
    }
    if (auto run_as_group_elem = pod_doc["run_as_group"]) {
      result.run_as_group =
          static_cast<gid_t>(run_as_group_elem.get_int32().value);
    }

    if (auto ports_elem = pod_doc["ports"];
        ports_elem && ports_elem.type() == bsoncxx::type::k_array) {
      for (const auto& port_val : ports_elem.get_array().value) {
        if (port_val.type() != bsoncxx::type::k_document) continue;
        auto ports_doc = port_val.get_document().view();
        PodMetaInTask::PortMapping mapping{};
        if (auto proto_elem = ports_doc["protocol"]) {
          mapping.protocol = static_cast<
              crane::grpc::PodTaskAdditionalMeta::PortMapping::Protocol>(
              proto_elem.get_int32().value);
        }
        if (auto container_port_elem = ports_doc["container_port"]) {
          mapping.container_port = container_port_elem.get_int32().value;
        }
        if (auto host_port_elem = ports_doc["host_port"]) {
          mapping.host_port = host_port_elem.get_int32().value;
        }
        if (auto host_ip_elem = ports_doc["host_ip"]) {
          mapping.host_ip = host_ip_elem.get_string().value;
        }
        result.port_mappings.emplace_back(std::move(mapping));
      }
    } else if (ports_elem && ports_elem.type() == bsoncxx::type::k_document) {
      // Backward compatibility with single-port schema.
      PodMetaInTask::PortMapping mapping{};
      auto ports_doc = ports_elem.get_document().view();
      if (auto proto_elem = ports_doc["protocol"]) {
        mapping.protocol = static_cast<
            crane::grpc::PodTaskAdditionalMeta::PortMapping::Protocol>(
            proto_elem.get_int32().value);
      }
      if (auto container_port_elem = ports_doc["container_port"]) {
        mapping.container_port = container_port_elem.get_int32().value;
      }
      if (auto host_port_elem = ports_doc["host_port"]) {
        mapping.host_port = host_port_elem.get_int32().value;
      }
      if (auto host_ip_elem = ports_doc["host_ip"]) {
        mapping.host_ip = host_ip_elem.get_string().value;
      }
      result.port_mappings.emplace_back(std::move(mapping));
    }

  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, e.what());
  }
  return result;
}

MongodbClient::document MongodbClient::TaskInEmbeddedDbToDocument_(
    const crane::grpc::TaskInEmbeddedDb& task) {
  auto const& task_to_ctld = task.task_to_ctld();
  auto const& runtime_attr = task.runtime_attr();

  std::optional<PodMetaInTask> pod_meta{std::nullopt};
  std::optional<ContainerMetaInTask> container_meta{std::nullopt};
  if (task_to_ctld.type() == crane::grpc::TaskType::Container) {
    pod_meta = static_cast<PodMetaInTask>(task_to_ctld.pod_meta());
    container_meta =
        static_cast<ContainerMetaInTask>(task_to_ctld.container_meta());
  }

  auto resources = static_cast<ResourceV2>(runtime_attr.allocated_res());
  ResourceView allocated_res_view;
  allocated_res_view.SetToZero();
  allocated_res_view += resources;

  bsoncxx::builder::stream::document env_doc;
  for (const auto& entry : task_to_ctld.env()) {
    env_doc << entry.first << entry.second;
  }

  std::string env_str = bsoncxx::to_json(env_doc.view());

  bsoncxx::builder::basic::array nodename_list_array;
  for (const auto& nodename : runtime_attr.craned_ids()) {
    nodename_list_array.append(nodename);
  }

  // 0  task_id       task_db_id     mod_time       deleted       account
  // 5  cpus_req      mem_req        task_name      env           id_user
  // 10 id_group      nodelist       nodes_alloc   node_inx    partition_name
  // 15 priority      time_eligible  time_start    time_end    time_suspended
  // 20 script        state          timelimit     time_submit work_dir
  // 25 submit_line   exit_code      username       qos        get_user_env
  // 30 type          extra_attr     reservation   exclusive   cpus_alloc
  // 35 mem_alloc     device_map     meta_pod      meta_container has_job_info
  // 40 licenses_alloc nodename_list wckey

  // clang-format off
  std::array<std::string, 43> fields{
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
    "mem_alloc", "device_map", "meta_pod","meta_container", "has_job_info", 
    // 40 - 44
    "licenses_alloc", "nodename_list", "wckey"
  };
  // clang-format on

  std::tuple<int32_t, task_db_id_t, int64_t, bool, std::string,    /*0-4*/
             double, int64_t, std::string, std::string, int32_t,   /*5-9*/
             int32_t, std::string, int32_t, int32_t, std::string,  /*10-14*/
             int64_t, int64_t, int64_t, int64_t, int64_t,          /*15-19*/
             std::string, int32_t, int64_t, int64_t, std::string,  /*20-24*/
             std::string, int32_t, std::string, std::string, bool, /*25-29*/
             int32_t, std::string, std::string, bool, double,      /*30-34*/
             int64_t, DeviceMap, std::optional<PodMetaInTask>,     /*35-37*/
             std::optional<ContainerMetaInTask>, bool,             /*38-39*/
             std::unordered_map<std::string, uint32_t>,            /*40*/
             bsoncxx::array::value, std::string>                   /*41-42*/
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
             // 35-40
             static_cast<int64_t>(allocated_res_view.MemoryBytes()),
             allocated_res_view.GetDeviceMap(), pod_meta, container_meta,
             true /* Mark the document having complete job info */,
             // 40-44
             std::unordered_map<std::string, uint32_t>{
                 runtime_attr.actual_licenses().begin(),
                 runtime_attr.actual_licenses().end()},
             bsoncxx::array::value{nodename_list_array.view()},
             task_to_ctld.wckey()};

  return DocumentConstructor_(fields, values);
}

MongodbClient::document MongodbClient::TaskInCtldToDocument_(TaskInCtld* task) {
  std::string script;
  std::optional<ContainerMetaInTask> container_meta{std::nullopt};
  std::optional<PodMetaInTask> pod_meta{std::nullopt};

  if (task->type == crane::grpc::Batch)
    script = task->TaskToCtld().batch_meta().sh_script();
  else if (task->type == crane::grpc::Container) {
    // All container job has pod_meta
    pod_meta = task->pod_meta;

    // Jobs from ccon has container_meta
    if (std::holds_alternative<ContainerMetaInTask>(task->meta))
      container_meta = std::get<ContainerMetaInTask>(task->meta);

    // Jobs from cbatch has batch_meta
    if (task->TaskToCtld().has_batch_meta())
      script = task->TaskToCtld().batch_meta().sh_script();
  }

  // TODO: Interactive meta?

  bsoncxx::builder::stream::document env_doc;
  for (const auto& entry : task->env) {
    env_doc << entry.first << entry.second;
  }

  std::string env_str = bsoncxx::to_json(env_doc.view());

  bsoncxx::builder::basic::array nodename_list_array;
  for (const auto& nodename : task->CranedIds()) {
    nodename_list_array.append(nodename);
  }

  // 0  task_id       task_db_id     mod_time       deleted       account
  // 5  cpus_req      mem_req        task_name      env           id_user
  // 10 id_group      nodelist       nodes_alloc   node_inx    partition_name
  // 15 priority      time_eligible  time_start    time_end    time_suspended
  // 20 script        state          timelimit     time_submit work_dir
  // 25 submit_line   exit_code      username       qos        get_user_env
  // 30 type          extra_attr     reservation    exclusive  cpus_alloc
  // 35 mem_alloc     device_map     meta_pod     meta_container has_job_info
  // 40 licenses_alloc nodename_list wckey

  // clang-format off
  std::array<std::string, 43> fields{
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
      "mem_alloc", "device_map", "meta_pod", "meta_container", "has_job_info", 
      // 40 - 44
      "licenses_alloc", "nodename_list", "wckey"
  };
  // clang-format on

  std::tuple<int32_t, task_db_id_t, int64_t, bool, std::string,    /*0-4*/
             double, int64_t, std::string, std::string, int32_t,   /*5-9*/
             int32_t, std::string, int32_t, int32_t, std::string,  /*10-14*/
             int64_t, int64_t, int64_t, int64_t, int64_t,          /*15-19*/
             std::string, int32_t, int64_t, int64_t, std::string,  /*20-24*/
             std::string, int32_t, std::string, std::string, bool, /*25-29*/
             int32_t, std::string, std::string, bool, double,      /*30-34*/
             int64_t, DeviceMap, std::optional<PodMetaInTask>,     /*35-37*/
             std::optional<ContainerMetaInTask>, bool,             /*38-39*/
             std::unordered_map<std::string, uint32_t>,            /*40*/
             bsoncxx::array::value, std::string>                   /*41-42*/
      values{                                                      // 0-4
             static_cast<int32_t>(task->TaskId()), task->TaskDbId(),
             absl::ToUnixSeconds(absl::Now()), false, task->account,
             // 5-9
             task->requested_node_res_view.CpuCount(),
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
             task->allocated_res_view.GetDeviceMap(), pod_meta, container_meta,
             true /* Mark the document having complete job info */,
             // 40-44
             task->licenses_count,
             bsoncxx::array::value{nodename_list_array.view()}, task->wckey};

  return DocumentConstructor_(fields, values);
}

MongodbClient::document MongodbClient::StepInCtldToDocument_(StepInCtld* step) {
  std::string script;
  if (step->type == crane::grpc::Batch)
    script = step->StepToCtld().batch_meta().sh_script();
  else if (step->type == crane::grpc::Container &&
           step->StepToCtld().has_batch_meta())
    // Container job primary step submitted via cbatch --pod
    script = step->StepToCtld().batch_meta().sh_script();

  std::optional<PodMetaInTask> pod_meta{std::nullopt};
  std::optional<ContainerMetaInTask> container_meta{std::nullopt};
  if (step->pod_meta.has_value()) pod_meta = step->pod_meta;
  if (step->container_meta.has_value()) container_meta = step->container_meta;

  bsoncxx::builder::stream::document env_doc;
  for (const auto& entry : step->env) {
    env_doc << entry.first << entry.second;
  }

  std::string env_str = bsoncxx::to_json(env_doc.view());

  // 0  step_id         mod_time      deleted         cpus_req      mem_req
  // 5  step_name       env           id_user         id_group      nodelist
  // 10 nodes_alloc     node_inx      time_eligible   time_start    time_end
  // 15 time_suspended  script        state           timelimit     time_submit
  // 20 work_dir        submit_line   exit_code       get_user_env  type
  // 25 extra_attr      res_alloc     step_type       meta_pod meta_container

  // clang-format off
  std::array<std::string, 30> fields{
      // 0 - 4
      "step_id", "mod_time",    "deleted","cpus_req", "mem_req",
      // 5 - 9
      "step_name",   "env",      "id_user","id_group", "nodelist",
      // 10 - 14
        "nodes_alloc", "node_inx",  "time_eligible","time_start", "time_end",
      // 15 - 19
        "time_suspended","script", "state","timelimit", "time_submit",
      // 20 - 24
        "work_dir","submit_line", "exit_code","get_user_env","type",
      // 25 - 29
         "extra_attr", "res_alloc", "step_type", "meta_pod", "meta_container",
  };

  // clang-format on
  std::tuple<int32_t, int64_t, bool,                             /*0-4*/
             double, int64_t, std::string, std::string, int32_t, /*5-9*/
             std::vector<gid_t>, std::string, int32_t, int32_t,
             int64_t,                                             /*10-14*/
             int64_t, int64_t, int64_t, std::string, int32_t,     /*15-19*/
             int64_t, int64_t, std::string, std::string, int32_t, /*20-24*/
             bool, int32_t, std::string, ResourceV2, int32_t,     /*25-29*/
             std::optional<PodMetaInTask>,                        /*30-30*/
             std::optional<ContainerMetaInTask>>                  /*31-31*/
      values{                                                     // 0-4
             static_cast<int32_t>(step->StepId()),
             absl::ToUnixSeconds(absl::Now()), false,
             step->requested_node_res_view.CpuCount(),
             static_cast<int64_t>(step->requested_node_res_view.MemoryBytes()),
             // 5-9
             step->name, env_str, static_cast<int32_t>(step->uid), step->gids,
             util::HostNameListToStr(step->CranedIds()),
             // 10-14
             static_cast<int32_t>(step->CranedIds().size()), 0, 0,
             ToUnixSeconds(step->StartTime()), ToUnixSeconds(step->EndTime()),
             // 15-19
             0, script, step->Status(), absl::ToInt64Seconds(step->time_limit),
             ToUnixSeconds(step->SubmitTime()),
             // 20-24
             step->StepToCtld().cwd(), step->StepToCtld().cmd_line(),
             step->ExitCode(), step->get_user_env, step->type,
             // 25-28
             step->StepToCtld().extra_attr(), step->AllocatedRes(),
             step->StepType(), pod_meta, container_meta};

  return DocumentConstructor_(fields, values);
}

MongodbClient::document MongodbClient::StepInEmbeddedDbToDocument_(
    crane::grpc::StepInEmbeddedDb const& step) {
  const auto& step_to_ctld = step.step_to_ctld();
  const auto& runtime_attr = step.runtime_attr();

  std::string script;
  if (step_to_ctld.type() == crane::grpc::Batch)
    script = step_to_ctld.batch_meta().sh_script();
  else if (step_to_ctld.type() == crane::grpc::Container &&
           step_to_ctld.has_batch_meta())
    script = step_to_ctld.batch_meta().sh_script();

  std::optional<PodMetaInTask> pod_meta{std::nullopt};
  std::optional<ContainerMetaInTask> container_meta{std::nullopt};
  if (step_to_ctld.type() == crane::grpc::Container) {
    if (step_to_ctld.has_container_meta()) {
      container_meta =
          static_cast<ContainerMetaInTask>(step_to_ctld.container_meta());
    }
  }

  bsoncxx::builder::stream::document env_doc;
  for (const auto& entry : step_to_ctld.env()) {
    env_doc << entry.first << entry.second;
  }

  std::string env_str = bsoncxx::to_json(env_doc.view());

  // 0  step_id         mod_time      deleted       cpus_req      mem_req
  // 5  step_name       env           id_user       id_group      nodelist
  // 10 nodes_alloc     node_inx      time_eligible time_start    time_end
  // 15 time_suspended  script        state         timelimit     time_submit
  // 20 work_dir        submit_line   exit_code     get_user_env  type
  // 25 extra_attr      res_alloc     step_type     meta_pod     meta_container

  // clang-format off
  std::array<std::string, 30> fields{
      // 0 - 4
      "step_id", "mod_time",    "deleted","cpus_req", "mem_req",
      // 5 - 9
         "step_name",   "env",      "id_user","id_group", "nodelist",
      // 10 - 14
       "nodes_alloc", "node_inx",  "time_eligible","time_start", "time_end",
      // 15 - 19
       "time_suspended","script", "state","timelimit", "time_submit",
      // 20 - 24
       "work_dir","submit_line", "exit_code","get_user_env","type",
      // 25 - 29
         "extra_attr", "res_alloc", "step_type", "meta_pod", "meta_container",
  };

  // clang-format on
  std::tuple<int32_t, int64_t, bool, double, int64_t, /*0-4*/
             std::string, std::string, int32_t, std::vector<gid_t>,
             std::string,                                      /*5-9*/
             int32_t, int32_t, int64_t, int64_t, int64_t,      /*10-14*/
             int64_t, std::string, int32_t, int64_t, int64_t,  /*15-19*/
             std::string, std::string, int32_t, bool, int32_t, /*20-24*/
             std::string, ResourceV2, int32_t,
             std::optional<PodMetaInTask>,       /*25-28*/
             std::optional<ContainerMetaInTask>> /*29-29*/

      values{
          // 0-4
          static_cast<int32_t>(runtime_attr.step_id()),
          absl::ToUnixSeconds(absl::Now()), false,
          step_to_ctld.req_resources_per_task()
                  .allocatable_res()
                  .cpu_core_limit() *
              step_to_ctld.ntasks_per_node(),
          static_cast<int64_t>(step_to_ctld.req_resources_per_task()
                                   .allocatable_res()
                                   .memory_limit_bytes()),
          // 5-9
          step_to_ctld.name(), env_str, step_to_ctld.uid(),
          std::vector<gid_t>(step_to_ctld.gid().begin(),
                             step_to_ctld.gid().end()),
          util::HostNameListToStr(runtime_attr.craned_ids()),
          // 10-14
          runtime_attr.craned_ids_size(), 0, 0,
          runtime_attr.start_time().seconds(),
          runtime_attr.end_time().seconds(),
          // 15-19
          0, script, runtime_attr.status(), step_to_ctld.time_limit().seconds(),
          runtime_attr.submit_time().seconds(),
          // 20-24
          step_to_ctld.cwd(), step_to_ctld.cmd_line(), runtime_attr.exit_code(),
          step_to_ctld.get_user_env(), step_to_ctld.type(),
          // 25-28
          step_to_ctld.extra_attr(), ResourceV2(runtime_attr.allocated_res()),
          runtime_attr.step_type(), pod_meta, container_meta};

  return DocumentConstructor_(fields, values);
}

void MongodbClient::ViewToStepInfo_(const bsoncxx::document::view& view,
                                    crane::grpc::StepInfo* step_info) {
  // 0  step_id         mod_time      deleted       cpus_req      mem_req
  // 5  step_name       env           id_user       id_group      nodelist
  // 10 nodes_alloc     node_inx      time_eligible time_start    time_end
  // 15 time_suspended  script        state         timelimit     time_submit
  // 20 work_dir        submit_line   exit_code     get_user_env  type
  // 25 extra_attr      res_alloc     step_type     meta_container
  step_id_t step_id = view["step_id"].get_int32().value;
  step_info->set_step_id(step_id);
  auto* mutable_req_res_view = step_info->mutable_req_res_view();
  auto* mutable_req_alloc_res = mutable_req_res_view->mutable_allocatable_res();
  mutable_req_alloc_res->set_cpu_core_limit(
      view["cpus_req"].get_double().value);
  mutable_req_alloc_res->set_memory_limit_bytes(
      view["mem_req"].get_int64().value);
  mutable_req_alloc_res->set_memory_sw_limit_bytes(
      view["mem_req"].get_int64().value);

  step_info->set_name(view["step_name"].get_string().value);

  step_info->set_uid(view["id_user"].get_int32().value);
  auto* proto_gid = step_info->mutable_gid();
  for (auto&& gid : view["id_group"].get_array().value) {
    if (gid.type() == bsoncxx::type::k_int32)
      proto_gid->Add(gid.get_int32());
    else if (gid.type() == bsoncxx::type::k_int64)
      proto_gid->Add(gid.get_int64());
    else {
      CRANE_LOGGER_ERROR(m_logger_, "gid type error");
    }
  }

  step_info->set_craned_list(view["nodelist"].get_string().value.data());
  step_info->set_node_num(view["nodes_alloc"].get_int32().value);

  step_info->mutable_start_time()->set_seconds(
      view["time_start"].get_int64().value);
  step_info->mutable_end_time()->set_seconds(
      view["time_end"].get_int64().value);

  step_info->set_status(
      static_cast<crane::grpc::TaskStatus>(view["state"].get_int32().value));
  step_info->mutable_time_limit()->set_seconds(
      view["timelimit"].get_int64().value);
  step_info->mutable_submit_time()->set_seconds(
      view["time_submit"].get_int64().value);
  step_info->set_cwd(std::string(view["work_dir"].get_string().value));
  if (view["submit_line"])
    step_info->set_cmd_line(
        std::string(view["submit_line"].get_string().value));
  step_info->set_exit_code(view["exit_code"].get_int32().value);

  step_info->set_type(
      static_cast<crane::grpc::TaskType>(view["type"].get_int32().value));

  step_info->set_extra_attr(view["extra_attr"].get_string().value.data());
  *step_info->mutable_allocated_res_view() =
      static_cast<crane::grpc::ResourceView>(
          BsonToResourceV2(view["res_alloc"].get_document().value).View());
  step_info->set_step_type(
      static_cast<crane::grpc::StepType>(view["step_type"].get_int32().value));

  // NOTE: type == Container doesn't necessarily means it's a container!
  if (step_info->type() == crane::grpc::Container &&
      view["meta_container"].type() != bsoncxx::type::k_null) {
    auto* meta_info = step_info->mutable_container_meta();
    auto container_meta = BsonToContainerMeta(view);
    *meta_info = std::move(
        static_cast<crane::grpc::ContainerTaskAdditionalMeta>(container_meta));
  }
}

void MongodbClient::CreateCollectionIndex(
    mongocxx::collection& coll, const std::vector<std::string>& fields) {
  bsoncxx::builder::stream::document index_builder;
  for (const auto& field : fields) {
    index_builder << field << 1;  // 1 for ascending order
  }

  // Generate a readable index name
  std::string idx_name;
  for (size_t i = 0; i < fields.size(); ++i) {
    if (i > 0) idx_name += "_";
    idx_name += fields[i] + "_1";
  }

  mongocxx::options::index index_options;
  index_options.name(idx_name);
  coll.create_index(index_builder.view(), index_options);
}

bool MongodbClient::InitTableIndexes() {
  try {
    // Create index for the raw task table
    auto raw_table = (*GetClient_())[m_db_name_][m_task_collection_name_];
    CreateCollectionIndex(raw_table, {"nodename_list"});
    return true;
  } catch (const std::exception& e) {
    CRANE_LOGGER_ERROR(m_logger_, "Create index error: {}", e.what());
    return false;
  }
}

bool MongodbClient::Init() {
  if (!InitTableIndexes()) {
    CRANE_LOGGER_ERROR(m_logger_, "Init table indexes failed!");
    return false;
  }
  return true;
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

}  // namespace Ctld

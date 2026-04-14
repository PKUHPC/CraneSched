/**
 * Copyright (c) 2025 Peking University and Peking University
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

#include "LuaJobHandler.h"

#include "AccountManager.h"
#include "CranedMetaContainer.h"
#include "JobScheduler.h"

namespace Ctld {

#ifdef HAVE_LUA

const std::vector<std::string> LuaJobHandler::kReqFxns = {"crane_job_submit",
                                                          "crane_job_modify"};
#endif

CraneRichError LuaJobHandler::JobSubmit(const std::string& lua_script,
                                        JobInCtld* job) {
  CraneRichError result = FormatRichErr(CraneErrCode::SUCCESS, "");
#ifdef HAVE_LUA
  auto lua_env = std::make_unique<crane::LuaEnvironment>();
  if (!lua_env->Init(lua_script))
    return FormatRichErr(CraneErrCode::ERR_LUA_FAILED,
                         "Failed to init lua environment");

  RegisterGlobalFunctions_(*lua_env);
  RegisterTypes_(*lua_env);
  // crane.jobs
  sol::table jobs_table = lua_env->GetLuaState().create_table();
  jobs_table[sol::metatable_key] = lua_env->GetLuaState().create_table_with(
      "__pairs", [](sol::this_state ts, sol::table self) {
        sol::state_view lua(ts);
        sol::object next = lua["next"];

        crane::grpc::QueryJobsInfoRequest req;
        std::unordered_map<job_id_t, crane::grpc::JobInfo> job_info_map;
        g_job_scheduler->QueryJobsInRam(&req, &job_info_map);

        sol::table all_jobs = lua.create_table();
        for (auto& [job_id, job_info] : job_info_map) {
          all_jobs[job_id] = job_info;
        }

        return std::make_tuple(next, all_jobs, sol::nil);
      });
  lua_env->GetCraneTable()["jobs"] = jobs_table;

  RegisterGlobalVariables_(*lua_env);

  /*
   *  All lua script functions should have been verified during
   *   initialization:
   */
  if (!lua_env->LoadLuaScript(kReqFxns))
    return FormatRichErr(CraneErrCode::ERR_LUA_FAILED,
                         "Failed to load lua script");

  std::vector<crane::grpc::PartitionInfo> part_list;
  PushPartitionList_(job->Username(), job->account, &part_list);
  auto& lua_state = lua_env->GetLuaState();
  sol::function submit = lua_state["crane_job_submit"];
  sol::protected_function_result lua_result = submit(job, part_list, job->uid);

  if (!lua_result.valid()) {
    sol::error err = lua_result;
    CRANE_ERROR("{}", err.what());
    return FormatRichErr(CraneErrCode::ERR_LUA_FAILED, err.what());
  }

  int rc = lua_result.get<int>();

  if (rc != 0)
    return FormatRichErr(static_cast<CraneErrCode>(rc), lua_env->GetUserMsg());
#endif

  return result;
}

CraneRichError LuaJobHandler::JobModify(const std::string& lua_script,
                                        JobInCtld* job) {
  CraneRichError result = FormatRichErr(CraneErrCode::SUCCESS, "");
#ifdef HAVE_LUA
  auto lua_env = std::make_unique<crane::LuaEnvironment>();
  if (!lua_env->Init(lua_script))
    return FormatRichErr(CraneErrCode::ERR_LUA_FAILED,
                         "Failed to init lua environment");

  RegisterGlobalFunctions_(*lua_env);
  RegisterTypes_(*lua_env);
  RegisterGlobalVariables_(*lua_env);

  /*
   *  All lua script functions should have been verified during
   *   initialization:
   */
  if (!lua_env->LoadLuaScript(kReqFxns))
    return FormatRichErr(CraneErrCode::ERR_LUA_FAILED,
                         "Failed to load lua script");

  crane::grpc::JobInfo job_info;
  job->SetFieldsOfJobInfo(&job_info);
  std::vector<crane::grpc::PartitionInfo> part_list;
  PushPartitionList_(job->Username(), job->account, &part_list);

  auto& lua_state = lua_env->GetLuaState();
  sol::function modify = lua_state["crane_job_modify"];
  sol::protected_function_result lua_result =
      modify(job, job_info, part_list, job->uid);

  if (!lua_result.valid()) {
    sol::error err = lua_result;
    CRANE_ERROR("{}", err.what());
    return FormatRichErr(CraneErrCode::ERR_LUA_FAILED, err.what());
  }

  int rc = lua_result.get<int>();

  if (rc != 0)
    return FormatRichErr(static_cast<CraneErrCode>(rc), lua_env->GetUserMsg());
#endif

  return result;
}

#ifdef HAVE_LUA

void LuaJobHandler::RegisterGlobalFunctions_(
    const crane::LuaEnvironment& lua_env) {
  lua_env.GetCraneTable().set_function(
      "get_qos_priority",
      [](const std::string& qos_name) -> std::optional<uint32_t> {
        auto qos = g_account_manager->GetExistedQosInfo(qos_name);
        if (!qos) {
          CRANE_ERROR("Invalid QOS name: {}", qos_name);
          return std::nullopt;
        }
        return qos->priority;
      });

  lua_env.GetCraneTable().set_function(
      "get_job_env_field",
      [](JobInCtld& job, const std::string& env_name) -> std::string {
        auto iter = job.env.find(env_name);
        if (iter == job.env.end()) return std::string{""};
        return iter->second;
      });

  lua_env.GetCraneTable().set_function(
      "set_job_env_field",
      [](const std::string& name, const std::string& value, JobInCtld* job) {
        job->env.insert_or_assign(name, value);
      });

  lua_env.GetCraneTable().set_function(
      "get_resv",
      [](const std::string& resv_name)
          -> std::optional<crane::grpc::ReservationInfo> {
        auto reply = g_meta_container->QueryResvInfo(resv_name);
        if (!reply.reservation_info_list().empty()) {
          return reply.reservation_info_list()[0];
        }

        return std::nullopt;
      });
}

void LuaJobHandler::RegisterTypes_(const crane::LuaEnvironment& lua_env) {
  // clang-format off

  lua_env.GetLuaState().new_usertype<ResourceView>("ResourceView",
    "cpu_count", sol::property([](const ResourceView& rv) {
      return rv.CpuCountDouble();
    }),
    "memory_bytes", sol::property([](const ResourceView& rv) {
      return rv.GetMemoryBytes();
    }),
    "device_map", sol::property(
      [&](const ResourceView& rv) {
        sol::table tbl = lua_env.GetLuaState().create_table();
        for (const auto& [gres_name, gres_count] : rv.GetGresMap()) {
          sol::table entry = lua_env.GetLuaState().create_table();
          entry["total_count"] = gres_count.total;

          sol::table typed = lua_env.GetLuaState().create_table();
          for (const auto& [typed_name, typed_count] : gres_count.specified) {
            typed[typed_name] = typed_count;
          }
          entry["typed"] = typed;
          tbl[gres_name] = entry;
        }
        return tbl;
      })
  );

  // job_desc
  lua_env.GetLuaState().new_usertype<JobInCtld>("JobInCtld",
    "time_limit", sol::property([](const JobInCtld& t) {
      return  absl::ToInt64Seconds(t.time_limit);
    }, [](JobInCtld& t, int64_t time_limit) {
      t.time_limit = absl::Seconds(time_limit);
    }),
    "partition_id", &JobInCtld::partition_id,
    "req_node_res_view", &JobInCtld::req_node_res_view,
    "req_task_res_view", &JobInCtld::req_task_res_view,
    "req_total_res_view", &JobInCtld::req_total_res_view,
    "type", &JobInCtld::type, "uid", &JobInCtld::uid,
    "gid", &JobInCtld::gid, "account", &JobInCtld::account,
    "name", &JobInCtld::name, "qos", &JobInCtld::qos,
    "node_num", &JobInCtld::node_num,
    // TODO: expose ntasks_per_node_min to Lua
    "ntasks_per_node_min", &JobInCtld::ntasks_per_node_min,
    "ntasks_per_node_max", &JobInCtld::ntasks_per_node_max,
    "included_nodes", sol::property(
          [](const JobInCtld& t) {
            return t.included_nodes | std::ranges::to<std::vector>();
          },
          [](JobInCtld& t, const std::vector<std::string>& nodes) {
            t.included_nodes.clear();
            t.included_nodes.insert(nodes.begin(), nodes.end());
          }),
      "excluded_nodes", sol::property(
          [](const JobInCtld& t) {
            return t.excluded_nodes | std::ranges::to<std::vector>();
          },
          [](JobInCtld& t, const std::vector<std::string>& nodes) {
            t.excluded_nodes.clear();
            t.excluded_nodes.insert(nodes.begin(), nodes.end());
          }),
      "requeue_if_failed", &JobInCtld::requeue_if_failed,
      "get_user_env", &JobInCtld::get_user_env,
      "cmd_line", &JobInCtld::cmd_line,
      "env", sol::property(
          [&](const JobInCtld& t) {
            sol::table tbl = lua_env.GetLuaState().create_table();
            for (const auto& [name, value] : t.env) tbl[name] = value;
            return tbl;
          },
          [](JobInCtld& t, const sol::table& tbl) {
            t.env.clear();
            for (const auto& [name, value] : tbl) {
              t.env[name.as<std::string>()] = value.as<std::string>();
            }
          }),
      "cwd", &JobInCtld::cwd, "extra_attr", &JobInCtld::extra_attr,
      "reservation", &JobInCtld::reservation,
      "begin_time", sol::property(
        [](const JobInCtld& t) -> int64_t {
          if (t.begin_time == absl::InfinitePast()) return 0;
          return absl::ToUnixSeconds(t.begin_time);
        },
      [](JobInCtld& t, int64_t unix_seconds) {
          if (unix_seconds == 0)
            t.begin_time = absl::InfinitePast();
          else
            t.begin_time = absl::FromUnixSeconds(unix_seconds);
        }),
      "exclusive", &JobInCtld::exclusive,
      "licenses_count", sol::property(
          [&](const JobInCtld& t) {
            sol::table tbl = lua_env.GetLuaState().create_table();
            for (const auto& [name, value] : t.licenses_count) {
              tbl[name] = value;
            }
            return tbl;
          },
          [](JobInCtld& t, const sol::table& tbl) {
            t.licenses_count.clear();
            for (const auto& [name, value] : tbl) {
              t.licenses_count.insert_or_assign(name.as<std::string>(),
                                       value.as<uint32_t>());
            }
        }));

  // crane.jobs
  using JobInfo = crane::grpc::JobInfo;
  lua_env.GetLuaState().new_usertype<JobInfo>("JobInfo",
    "type", sol::property([](const JobInfo& t) {
      return static_cast<int>(t.type());
    }),
    "job_id", sol::property([](const JobInfo& t) {
      return t.job_id();
    }),
    "name", sol::property([](JobInfo& t) {
      return t.name();
    }),
    "partition", sol::property([](const JobInfo& t) {
      return t.partition();
    }),
    "uid", sol::property([](const JobInfo& t) {
      return t.uid();
    }),
    "time_limit", sol::property([](const JobInfo& t) {
      return google::protobuf::util::TimeUtil::DurationToSeconds(t.time_limit());
    }),
    "end_time", sol::property([](const JobInfo& t) {
      return t.end_time().seconds();
    }),
    "submit_time", sol::property([](const JobInfo& t) {
      return t.submit_time().seconds();
    }),
    "account", sol::property([](const JobInfo& t) {
      return t.account();
    }),
    "node_num", sol::property([](const JobInfo& t) {
      return t.node_num();
    }),
    "cmd_line", sol::property([](const JobInfo& t) {
      return t.cmd_line();
    }),
    "cwd", sol::property([](const JobInfo& t) {
      return t.cwd();
    }),
    "username", sol::property([](const JobInfo& t) {
      return t.username();
    }),
    "qos", sol::property([](const JobInfo& t) {
      return t.qos();
    }),
    "req_total_res_view", sol::property([](const JobInfo& t) {
      return static_cast<ResourceView>(t.req_total_res_view());
    }),
    "licenses_count", sol::property([&](const JobInfo& t) {
      sol::table tbl = lua_env.GetLuaState().create_table();
            for (const auto& [name, value] : t.licenses_count()) {
              tbl[name] = value;
            }
            return tbl;
    }),
    "req_nodes", sol::property([](const JobInfo& t) {
      return t.req_nodes() | std::ranges::to<std::vector>();
    }),
    "exclude_nodes", sol::property([](const JobInfo& t) {
      return t.exclude_nodes() | std::ranges::to<std::vector>();
    }),
    "extra_attr", sol::property([](const JobInfo& t) {
      return t.extra_attr();
    }),
    "reservation", sol::property([](const JobInfo& t) {
      return t.reservation();
    }),
    "held", sol::property([](const JobInfo& t) {
      return t.held();
    }),
    "status", sol::property([](const JobInfo& t) {
      return t.status();
    }),
    "exit_code", sol::property([](const JobInfo& t) {
      return t.exit_code();
    }),
    "priority", sol::property([](const JobInfo& t) {
      return t.priority();
    }),
    "pending_reason", sol::property([](const JobInfo& t) {
      if (t.has_pending_reason())
        return t.pending_reason();

      return std::string{""};
    }),
    "craned_list", sol::property([](const JobInfo& t) {
      if (t.has_craned_list())
        return t.craned_list();

      return std::string{""};
    }),
    "elapsed_time", sol::property([](const JobInfo& t) {
      return google::protobuf::util::TimeUtil::DurationToSeconds(t.elapsed_time());
    }),
    "execution_node", sol::property([](const JobInfo& t) {
      return t.execution_node() | std::ranges::to<std::vector>();
    }),
    "exclusive", sol::property([](const JobInfo& t) {
      return t.exclusive();
    }),
    "allocated_res_view", sol::property([](const JobInfo& t) {
      return static_cast<ResourceView>(t.allocated_res_view());
    }),
    "env", sol::property([&](const JobInfo& t) {
      sol::table tbl = lua_env.GetLuaState().create_table();
      for (const auto& [name, value] : t.env()) tbl[name] = value;
        return tbl;
      })
  );

  using PartitionInfo = crane::grpc::PartitionInfo;
  lua_env.GetLuaState().new_usertype<PartitionInfo>("PartitionInfo",
    "hostlist", sol::property([](const PartitionInfo& p) {
      return p.hostlist();
    }),
    "state", sol::property([](const PartitionInfo& p) {
      return static_cast<uint32_t>(p.state());
    }),
    "name", sol::property([](const PartitionInfo& p) {
      return p.name();
    }),
    "total_nodes", sol::property([](const PartitionInfo& p) {
      return p.total_nodes();
    }),
    "alive_nodes", sol::property([](const PartitionInfo& p) {
      return p.alive_nodes();
    }),
    "res_total", sol::property([](const PartitionInfo& p) {
      return static_cast<ResourceView>(p.res_total());
    }),
    "res_avail", sol::property([](const PartitionInfo& p) {
      return static_cast<ResourceView>(p.res_avail());
    }),
    "res_alloc", sol::property([](const PartitionInfo& p) {
      return static_cast<ResourceView>(p.res_alloc());
    }),
    "allowed_accounts", sol::property(
      [](const PartitionInfo& p) {
        return p.allowed_accounts() | std::ranges::to<std::vector>();
      }),
    "denied_accounts", sol::property(
      [](const PartitionInfo& p) {
        return p.denied_accounts() | std::ranges::to<std::vector>();
      }),
    "default_mem_per_cpu", sol::property([](const PartitionInfo& p) {
      return p.default_mem_per_cpu();
    }),
    "max_mem_per_cpu", sol::property([](const PartitionInfo& p) {
      return p.max_mem_per_cpu();
    }),
    "default_mem_per_node", sol::property([](const PartitionInfo& p) {
      return p.default_mem_per_node();
    }),
    "max_mem_per_node", sol::property([](const PartitionInfo& p) {
      return p.max_mem_per_node();
    })
  );

  using ReservationInfo = crane::grpc::ReservationInfo;
  lua_env.GetLuaState().new_usertype<ReservationInfo>("ReservationInfo",
    "reservation_name", sol::property([](const ReservationInfo& r) {
      return r.reservation_name();
    }),
    "start_time", sol::property([](const ReservationInfo& r) {
      return r.start_time().seconds();
    }),
    "duration", sol::property([](const ReservationInfo& r) {
      return google::protobuf::util::TimeUtil::DurationToSeconds(r.duration());
    }),
    "partition", sol::property([](const ReservationInfo& r) {
      return r.partition();
    }),
    "craned_regex", sol::property([](const ReservationInfo& r) {
      return r.craned_regex();
    }),
    "res_total", sol::property([](const ReservationInfo& r) {
      return static_cast<ResourceView>(r.res_total());
    }),
    "res_avail", sol::property([](const ReservationInfo& r) {
      return static_cast<ResourceView>(r.res_avail());
    }),
    "res_alloc", sol::property([](const ReservationInfo& r) {
      return static_cast<ResourceView>(r.res_alloc());
    }),
    "allowed_accounts", sol::property(
      [](const ReservationInfo& r) {
        return r.allowed_accounts() | std::ranges::to<std::vector>();
    }),
    "denied_accounts", sol::property(
      [](const ReservationInfo& r) {
        return r.denied_accounts() | std::ranges::to<std::vector>();
      }),
      "allowed_users", sol::property([](const ReservationInfo& r) {
        return r.allowed_users() | std::ranges::to<std::vector>();
      }),
      "denied_users", sol::property([](const ReservationInfo& r) {
        return r.denied_users() | std::ranges::to<std::vector>();
    })
  );

  // clang-format on
}

void LuaJobHandler::RegisterGlobalVariables_(
    const crane::LuaEnvironment& lua_env) {
  // crane.reservations
  sol::table resv_table = lua_env.GetLuaState().create_table();
  resv_table[sol::metatable_key] = lua_env.GetLuaState().create_table_with(
      "__pairs", [](sol::this_state ts, sol::table /*self*/) {
        sol::state_view lua(ts);
        sol::object next = lua["next"];

        auto reply = g_meta_container->QueryAllResvInfo();
        sol::table reservations_tbl = lua.create_table();
        for (const auto& resv : reply.reservation_info_list()) {
          reservations_tbl[resv.reservation_name()] = resv;
        }

        return std::make_tuple(next, reservations_tbl, sol::nil);
      });
  lua_env.GetCraneTable()["reservations"] = resv_table;
}

void LuaJobHandler::PushPartitionList_(
    const std::string& user_name, const std::string& account,
    std::vector<crane::grpc::PartitionInfo>* part_list) {
  auto user = g_account_manager->GetExistedUserInfo(user_name);
  if (!user) {
    CRANE_ERROR("user {} is null", user_name);
    return;
  }
  std::string actual_account = account;
  if (actual_account.empty()) actual_account = user->default_account;

  auto partition_info_reply = g_meta_container->QueryAllPartitionInfo();
  part_list->reserve(partition_info_reply.partition_info_list_size());
  for (const auto& partition : partition_info_reply.partition_info_list()) {
    auto iter = user->account_to_attrs_map.find(actual_account);
    if (iter == user->account_to_attrs_map.end()) continue;
    if (!iter->second.allowed_partition_qos_map.contains(partition.name()))
      continue;
    if (!partition.allowed_accounts().empty() &&
        !std::ranges::contains(partition.allowed_accounts(), actual_account))
      continue;

    part_list->emplace_back(partition);
  }
}
#endif

}  // namespace Ctld

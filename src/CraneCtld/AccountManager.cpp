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

#include "AccountManager.h"

#include "crane/PasswordEntry.h"

namespace Ctld {

AccountManager::AccountManager() { InitDataMap_(); }

AccountManager::Result AccountManager::AddUser(User&& new_user) {
  // When you add a new user, you can only associate it with the default account
  if (new_user.account_to_attrs_map.size() != 1 ||
      new_user.account_to_attrs_map.begin()->first !=
          new_user.default_account) {
    CRANE_ERROR("The added user does not comply with system rules");
    return Result{false, "Crane system error"};
  }

  if (new_user.default_account.empty()) {
    // User must specify an account
    return Result{false, fmt::format("Please specify the user's account")};
  }

  util::write_lock_guard user_guard(m_rw_user_mutex_);
  util::write_lock_guard account_guard(m_rw_account_mutex_);

  std::string object_account = new_user.default_account;
  const std::string name = new_user.name;

  // Avoid duplicate insertion
  const User* find_user = GetUserInfoNoLock_(name);
  if (find_user && !find_user->deleted) {
    if (find_user->account_to_attrs_map.contains(object_account)) {
      return Result{false,
                    fmt::format("The user '{}' already have account '{}'", name,
                                object_account)};
    }
  }

  // Check whether the account exists
  const Account* find_account = GetExistedAccountInfoNoLock_(object_account);
  if (!find_account) {
    return Result{false, fmt::format("unknown account '{}'", object_account)};
  }

  const std::list<std::string>& parent_allowed_partition =
      find_account->allowed_partition;
  // Check if user's allowed partition is a subset of parent's allowed
  // partition
  for (auto&& [partition, qos] : new_user.account_to_attrs_map[object_account]
                                     .allowed_partition_qos_map) {
    if (std::find(parent_allowed_partition.begin(),
                  parent_allowed_partition.end(),
                  partition) == parent_allowed_partition.end()) {
      return Result{false,
                    fmt::format("Partition '{}' is not allowed in account '{}'",
                                partition, find_account->name)};
    }
  }

  return AddUser_(find_user, find_account, std::move(new_user));
}

AccountManager::Result AccountManager::AddAccount(Account&& new_account) {
  util::write_lock_guard account_guard(m_rw_account_mutex_);
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);

  const std::string name = new_account.name;

  // Avoid duplicate insertion
  const Account* find_account = GetAccountInfoNoLock_(name);
  if (find_account && !find_account->deleted) {
    return Result{
        false,
        fmt::format("The account '{}' already exists in the database", name)};
  }

  const Account* find_parent = nullptr;
  if (!new_account.parent_account.empty()) {
    // Check whether the account's parent account exists
    find_parent = GetExistedAccountInfoNoLock_(new_account.parent_account);
    if (!find_parent) {
      return Result{
          false,
          fmt::format("The parent account '{}' doesn't exist in the database",
                      new_account.parent_account)};
    }

    // check allowed partition authority
    for (const auto& par : new_account.allowed_partition) {
      if (std::find(find_parent->allowed_partition.begin(),
                    find_parent->allowed_partition.end(),
                    par) == find_parent->allowed_partition.end()) {  // not find
        return Result{
            false,
            fmt::format(
                "Parent account '{}' does not have access to partition '{}'",
                new_account.parent_account, par)};
      }
    }

    // check allowed qos list authority
    for (const auto& qos : new_account.allowed_qos_list) {
      if (std::find(find_parent->allowed_qos_list.begin(),
                    find_parent->allowed_qos_list.end(),
                    qos) == find_parent->allowed_qos_list.end()) {  // not find
        return Result{
            false,
            fmt::format("Parent account '{}' does not have access to qos '{}'",
                        new_account.parent_account, qos)};
      }
    }
  } else {  // No parent account
    // Check whether partitions exists
    for (const auto& p : new_account.allowed_partition) {
      if (!g_config.Partitions.contains(p)) {
        return Result{false, fmt::format("Partition '{}' does not exist", p)};
      }
    }

    for (const auto& qos : new_account.allowed_qos_list) {
      const Qos* find_qos = GetExistedQosInfoNoLock_(qos);
      if (!find_qos) {
        return Result{false, fmt::format("Qos '{}' does not exist", qos)};
      }
    }
  }

  if (!new_account.default_qos.empty()) {
    if (std::find(new_account.allowed_qos_list.begin(),
                  new_account.allowed_qos_list.end(),
                  new_account.default_qos) ==
        new_account.allowed_qos_list.end())
      return Result{
          false,
          fmt::format("default qos '{}' not included in allowed qos list",
                      new_account.default_qos)};
  }

  return AddAccount_(find_account, find_parent, std::move(new_account));
}

AccountManager::Result AccountManager::AddQos(const Qos& new_qos) {
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);

  const Qos* find_qos = GetQosInfoNoLock_(new_qos.name);
  if (find_qos && !find_qos->deleted) {
    return Result{false, fmt::format("Qos '{}' already exists in the database",
                                     new_qos.name)};
  }

  return AddQos_(find_qos, new_qos);
}

AccountManager::Result AccountManager::DeleteUser(const std::string& name,
                                                  const std::string& account) {
  if (!account.empty()) {
    return RemoveUserFromAccount(name, account);
  }

  util::write_lock_guard user_guard(m_rw_user_mutex_);
  util::write_lock_guard account_guard(m_rw_account_mutex_);

  const User* user = GetExistedUserInfoNoLock_(name);
  if (!user) {
    return Result{false,
                  fmt::format("User '{}' doesn't exist in the database", name)};
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        // delete form the parent accounts' users list
        for (const auto& kv : user->account_to_attrs_map) {
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$pull", kv.first,
                                       /*account name*/ "users", name);
        }
        for (const std::string& coordinatorAccount :
             user->coordinator_accounts) {
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$pull", coordinatorAccount,
                                       /*account name*/ "coordinators", name);
        }

        // Delete the user
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::USER, "$set",
                                     name, "deleted", true);
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  for (const auto& kv : user->account_to_attrs_map) {
    m_account_map_[kv.first]->users.remove(name);
    m_account_map_[kv.first]->coordinators.remove(
        name);  // No inspection required
  }
  m_user_map_[name]->deleted = true;

  return Result{true};
}

AccountManager::Result AccountManager::RemoveUserFromAccount(
    const std::string& name, const std::string& account) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);
  util::write_lock_guard account_guard(m_rw_account_mutex_);

  const User* user = GetExistedUserInfoNoLock_(name);
  if (!user) {
    return Result{false, fmt::format("Unknown user '{}'", name)};
  }
  if (!user->account_to_attrs_map.contains(account)) {
    return Result{false, fmt::format("User '{}' doesn't belong to account '{}'",
                                     name, account)};
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        // delete form the parent accounts' users list
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                     "$pull", account, /*account name*/ "users",
                                     name);
        if (std::find(user->coordinator_accounts.begin(),
                      user->coordinator_accounts.end(),
                      account) != user->coordinator_accounts.end()) {
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$pull", account,
                                       /*account name*/ "coordinators", name);
        }

        // Delete the account from user account_map
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::USER, "$unset",
                                     name, "account_to_attrs_map." + account,
                                     std::string(""));
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  m_account_map_[account]->users.remove(name);
  m_account_map_[account]->coordinators.remove(name);  // No inspection required
  m_user_map_[name]->account_to_attrs_map.erase(account);

  User res_user(*user);

  if (res_user.default_account == account &&
      !m_user_map_[name]->account_to_attrs_map.empty()) {
    res_user.default_account =
        m_user_map_[name]->account_to_attrs_map.begin()->first;
  }

  return Result{true};
}

AccountManager::Result AccountManager::DeleteAccount(const std::string& name) {
  util::write_lock_guard account_guard(m_rw_account_mutex_);
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);
  const Account* account = GetExistedAccountInfoNoLock_(name);

  if (!account) {
    return Result{
        false, fmt::format("Account '{}' doesn't exist in the database", name)};
  }

  if (!account->child_accounts.empty() || !account->users.empty()) {
    return Result{false, "This account has child account or users"};
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        if (!account->parent_account.empty()) {
          // delete form the parent account's child account list
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$pull", account->parent_account,
                                       "child_accounts", name);
        }
        // Delete the account
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT, "$set",
                                     name, "deleted", true);
        for (const auto& qos : account->allowed_qos_list) {
          IncQosReferenceCountInDb_(qos, -1);
        }
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  if (!account->parent_account.empty()) {
    m_account_map_[account->parent_account]->child_accounts.remove(name);
  }
  m_account_map_[name]->deleted = true;

  for (const auto& qos : account->allowed_qos_list) {
    m_qos_map_[qos]->reference_count--;
  }

  return Result{true};
}

AccountManager::Result AccountManager::DeleteQos(const std::string& name) {
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);
  const Qos* qos = GetExistedQosInfoNoLock_(name);

  if (!qos) {
    return Result{false, fmt::format("Qos '{}' not exists in database", name)};
  } else if (qos->reference_count != 0) {
    return Result{false,
                  fmt::format("There still has {} references to this qos",
                              qos->reference_count)};
  }

  if (!g_db_client->UpdateEntityOne(MongodbClient::EntityType::QOS, "$set",
                                    name, "deleted", true)) {
    return Result{false, fmt::format("Delete qos '{}' failed", name)};
  }
  m_qos_map_[name]->deleted = true;

  return Result{true};
}

AccountManager::UserMutexSharedPtr AccountManager::GetExistedUserInfo(
    const std::string& name) {
  m_rw_user_mutex_.lock_shared();

  const User* user = GetExistedUserInfoNoLock_(name);
  if (!user) {
    m_rw_user_mutex_.unlock_shared();
    return UserMutexSharedPtr{nullptr};
  } else {
    return UserMutexSharedPtr{user, &m_rw_user_mutex_};
  }
}

AccountManager::UserMapMutexSharedPtr AccountManager::GetAllUserInfo() {
  m_rw_user_mutex_.lock_shared();

  if (m_user_map_.empty()) {
    m_rw_user_mutex_.unlock_shared();
    return UserMapMutexSharedPtr{nullptr};
  } else {
    return UserMapMutexSharedPtr{&m_user_map_, &m_rw_user_mutex_};
  }
}

AccountManager::AccountMutexSharedPtr AccountManager::GetExistedAccountInfo(
    const std::string& name) {
  m_rw_account_mutex_.lock_shared();

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    m_rw_account_mutex_.unlock_shared();
    return AccountMutexSharedPtr{nullptr};
  } else {
    return AccountMutexSharedPtr{account, &m_rw_account_mutex_};
  }
}

AccountManager::AccountMapMutexSharedPtr AccountManager::GetAllAccountInfo() {
  m_rw_account_mutex_.lock_shared();

  if (m_account_map_.empty()) {
    m_rw_account_mutex_.unlock_shared();
    return AccountMapMutexSharedPtr{nullptr};
  } else {
    return AccountMapMutexSharedPtr{&m_account_map_, &m_rw_account_mutex_};
  }
}

AccountManager::QosMutexSharedPtr AccountManager::GetExistedQosInfo(
    const std::string& name) {
  m_rw_qos_mutex_.lock_shared();

  const Qos* qos = GetExistedQosInfoNoLock_(name);
  if (!qos) {
    m_rw_qos_mutex_.unlock_shared();
    return QosMutexSharedPtr{nullptr};
  } else {
    return QosMutexSharedPtr{qos, &m_rw_qos_mutex_};
  }
}

AccountManager::QosMapMutexSharedPtr AccountManager::GetAllQosInfo() {
  m_rw_qos_mutex_.lock_shared();

  if (m_qos_map_.empty()) {
    m_rw_qos_mutex_.unlock_shared();
    return QosMapMutexSharedPtr{nullptr};
  } else {
    return QosMapMutexSharedPtr{&m_qos_map_, &m_rw_qos_mutex_};
  }
}

AccountManager::Result AccountManager::ModifyUser(
    const crane::grpc::ModifyEntityRequest_OperatorType& operatorType,
    const std::string& name, const std::string& partition, std::string account,
    const crane::grpc::ModifyEntityRequest_ModifyField& modifyField,
    const std::string& value, bool force) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);

  const User* p = GetExistedUserInfoNoLock_(name);

  if (!p) {
    return Result{false, fmt::format("Unknown user '{}'", name)};
  }

  if (account.empty()) {
    account = p->default_account;
  }

  switch (operatorType) {
  case crane::grpc::ModifyEntityRequest_OperatorType_Add:
    switch (modifyField) {
    case crane::grpc::ModifyEntityRequest_ModifyField_Partition: {
      util::write_lock_guard account_guard(m_rw_account_mutex_);
      const Account* account_ptr = GetExistedAccountInfoNoLock_(account);
      if (!account_ptr) {
        return Result{false, fmt::format("Unknown account '{}'", account)};
      }
      auto result = CheckAddUserAllowedPartition(*p, *account_ptr, value);
      return !result.ok ? result
                        : AddUserAllowedPartition_(*p, *account_ptr, value);
    }

    case crane::grpc::ModifyEntityRequest_ModifyField_Qos: {
      util::write_lock_guard account_guard(m_rw_account_mutex_);
      util::write_lock_guard qos_guard(m_rw_qos_mutex_);
      const Account* account_ptr = GetExistedAccountInfoNoLock_(account);
      if (!account_ptr) {
        return Result{false, fmt::format("Unknown account '{}'", account)};
      }
      auto result = CheckAddUserAllowedQos(*p, *account_ptr, partition, value);
      return !result.ok ? result
                        : AddUserAllowedQos_(*p, *account_ptr, partition, value);
    }
    default:
      break;
    }

  case crane::grpc::ModifyEntityRequest_OperatorType_Overwrite:
    switch (modifyField) {
    case crane::grpc::ModifyEntityRequest_ModifyField_AdminLevel: {
      User::AdminLevel new_level;
      auto result = CheckSetUserAdminLevel(*p, value, &new_level);

      return !result.ok ? result : SetUserAdminLevel_(name, new_level);
    }

    case crane::grpc::ModifyEntityRequest_ModifyField_DefaultQos: {
      auto result = CheckSetUserDefaultQos(*p, account, partition, value);
      return !result.ok ? result
                        : SetUserDefaultQos_(*p, account, partition, value);
    }

    case crane::grpc::ModifyEntityRequest_ModifyField_Partition: {
      util::write_lock_guard account_guard(m_rw_account_mutex_);
      const Account* account_ptr = GetExistedAccountInfoNoLock_(account);
      if (!account_ptr) {
        return Result{false, fmt::format("Unknown account '{}'", account)};
      }
      auto result = CheckSetUserAllowedPartition(*p, *account_ptr, value);
      return !result.ok ? result
                        : SetUserAllowedPartition_(*p, *account_ptr, value);
    }

    case crane::grpc::ModifyEntityRequest_ModifyField_Qos: {
      util::write_lock_guard account_guard(m_rw_account_mutex_);
      util::write_lock_guard qos_guard(m_rw_qos_mutex_);
      const Account* account_ptr = GetExistedAccountInfoNoLock_(account);
      if (!account_ptr) {
        return Result{false, fmt::format("Unknown account '{}'", account)};
      }
      auto result = CheckSetUserAllowedQos(*p, *account_ptr, partition, value, force);
      return !result.ok ? result : SetUserAllowedQos_(*p, *account_ptr, partition, value, force);
    }

    default:
      break;
    }

  case crane::grpc::ModifyEntityRequest_OperatorType_Delete:
    switch (modifyField) {
    case crane::grpc::ModifyEntityRequest_ModifyField_Partition:
    {
      auto result = CheckDeleteUserAllowedPartition(*p, account, value);
      return !result.ok ? result : DeleteUserAllowedPartition_(*p, account, value);
    }
      

    case crane::grpc::ModifyEntityRequest_ModifyField_Qos:
    {
      auto result = CheckDeleteUserAllowedQos(*p, account, partition, value, force);
      return !result.ok ? result : DeleteUserAllowedQos_(*p, value, account, partition, force);
    }
      
    default:
      break;
    }

  default:
    break;
  }

  return Result{true};
}

AccountManager::Result AccountManager::ModifyAccount(
    const crane::grpc::ModifyEntityRequest_OperatorType& operatorType,
    const std::string& name,
    const crane::grpc::ModifyEntityRequest_ModifyField& modifyField,
    const std::string& value, bool force) {
  switch (operatorType) {
  case crane::grpc::ModifyEntityRequest_OperatorType_Add:
    switch (modifyField) {
    case crane::grpc::ModifyEntityRequest_ModifyField_Partition:
      return AddAccountAllowedPartition_(name, value);
    case crane::grpc::ModifyEntityRequest_ModifyField_Qos:
      return AddAccountAllowedQos_(name, value);
    default:
      break;
    }

  case crane::grpc::ModifyEntityRequest_OperatorType_Overwrite:
    switch (modifyField) {
    case crane::grpc::ModifyEntityRequest_ModifyField_Description:
      return SetAccountDescription_(name, value);
    case crane::grpc::ModifyEntityRequest_ModifyField_Partition:
      return SetAccountAllowedPartition_(name, value, force);
    case crane::grpc::ModifyEntityRequest_ModifyField_Qos:
      return SetAccountAllowedQos_(name, value, force);
    case crane::grpc::ModifyEntityRequest_ModifyField_DefaultQos:
      return SetAccountDefaultQos_(name, value);
    default:
      break;
    }

  case crane::grpc::ModifyEntityRequest_OperatorType_Delete:
    switch (modifyField) {
    case crane::grpc::ModifyEntityRequest_ModifyField_Partition:
      return DeleteAccountAllowedPartition_(name, value, force);
    case crane::grpc::ModifyEntityRequest_ModifyField_Qos:
      return DeleteAccountAllowedQos_(name, value, force);
    default:
      break;
    }

  default:
    break;
  }

  return Result{true};
}

AccountManager::Result AccountManager::ModifyQos(
    const std::string& name,
    const crane::grpc::ModifyEntityRequest_ModifyField& modifyField,
    const std::string& value) {
  std::string item = "";
  switch (modifyField) {
  case crane::grpc::ModifyEntityRequest_ModifyField_Description:
    item = "description";
    break;
  case crane::grpc::ModifyEntityRequest_ModifyField_Priority:
    item = "priority";
    break;
  case crane::grpc::ModifyEntityRequest_ModifyField_MaxJobsPerUser:
    item = "max-jobs-per-user";
    break;
  case crane::grpc::ModifyEntityRequest_ModifyField_MaxCpusPerUser:
    item = "max-cpus-per-user";
    break;
  case crane::grpc::ModifyEntityRequest_ModifyField_MaxTimeLimitPerTask:
    item = "max-time-limit-per-task";
    break;
  default:
    return Result{false, fmt::format("Field '{}' can't be modify", item)};
  }

  bool value_is_number{false};
  int64_t value_number;
  if (item != Qos::FieldStringOfDescription()) {
    bool ok = util::ConvertStringToInt64(value, &value_number);
    if (!ok) return Result{false, "Failed to convert value to integer"};

    value_is_number = true;

    if (item == Qos::FieldStringOfMaxTimeLimitPerTask() &&
        !CheckIfTimeLimitSecIsValid(value_number))
      return Result{false, "Invalid time limit value"};
  }

  util::write_lock_guard qos_guard(m_rw_qos_mutex_);

  const Qos* p = GetExistedQosInfoNoLock_(name);
  if (!p) {
    return Result{false, fmt::format("Qos '{}' not existed in database", name)};
  }

  if (item == "description") {
    if (!g_db_client->UpdateEntityOne(MongodbClient::EntityType::QOS, "$set",
                                      name, item, value)) {
      return Result{false, "Fail to update the database"};
    }
  } else {
    /* uint32 Type Stores data based on long(int64_t) */
    if (!g_db_client->UpdateEntityOne(MongodbClient::EntityType::QOS, "$set",
                                      name, item, value_number)) {
      return Result{false, "Fail to update the database"};
    }
  }

  // To avoid frequently judging item, obtain the modified qos of the Mongodb
  Qos qos;
  g_db_client->SelectQos("name", name, &qos);
  *m_qos_map_[name] = std::move(qos);

  return Result{true};
}

AccountManager::Result AccountManager::BlockAccount(const std::string& name,
                                                    bool block) {
  util::write_lock_guard account_guard(m_rw_account_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    return Result{false, fmt::format("Unknown account '{}'", name)};
  }

  if (account->blocked == block) {
    return Result{true};
  }

  if (!g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT, "$set",
                                    name, "blocked", block)) {
    return Result{false, "Can't update the database"};
  }
  m_account_map_[name]->blocked = block;

  return Result{true};
}

AccountManager::Result AccountManager::BlockUser(const std::string& name,
                                                 const std::string& account,
                                                 bool block) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);

  const User* user = GetExistedUserInfoNoLock_(name);
  if (!user) {
    return Result{false, fmt::format("Unknown user '{}'", name)};
  }
  if (!user->account_to_attrs_map.contains(account)) {
    return Result{false, fmt::format("User '{}' doesn't belong to account '{}'",
                                     name, account)};
  }

  if (user->account_to_attrs_map.at(account).blocked == block) {
    return Result{true};
  }

  if (!g_db_client->UpdateEntityOne(
          MongodbClient::EntityType::USER, "$set", name,
          "account_to_attrs_map." + account + ".blocked", block)) {
    return Result{false, "Can't update the database"};
  }
  m_user_map_[name]->account_to_attrs_map[account].blocked = block;

  return Result{true};
}

bool AccountManager::CheckUserPermissionToPartition(
    const std::string& name, const std::string& account,
    const std::string& partition) {
  UserMutexSharedPtr user_share_ptr = GetExistedUserInfo(name);
  if (!user_share_ptr) {
    return false;
  }

  if (user_share_ptr->uid == 0 ||
      user_share_ptr->account_to_attrs_map.at(account)
          .allowed_partition_qos_map.contains(partition)) {
    return true;
  }
  return false;
}

result::result<void, std::string> AccountManager::CheckEnableState(
    const std::string& account, const std::string& user) {
  util::read_lock_guard user_guard(m_rw_user_mutex_);
  util::read_lock_guard account_guard(m_rw_account_mutex_);
  std::string p_str = account;
  const Account* p_account;
  do {
    p_account = GetExistedAccountInfoNoLock_(p_str);
    if (p_account->blocked) {
      return result::fail(
          fmt::format("Ancestor account '{}' is blocked", p_account->name));
    }
    p_str = p_account->parent_account;
  } while (!p_str.empty());

  const User* p_user = GetExistedUserInfoNoLock_(user);
  if (p_user->account_to_attrs_map.at(account).blocked) {
    return result::fail(fmt::format("User '{}' is blocked", p_user->name));
  }
  return {};
}

result::result<void, std::string> AccountManager::CheckAndApplyQosLimitOnTask(
    const std::string& user, const std::string& account, TaskInCtld* task) {
  util::read_lock_guard user_guard(m_rw_user_mutex_);
  util::read_lock_guard qos_guard(m_rw_qos_mutex_);

  const User* user_share_ptr = GetExistedUserInfoNoLock_(user);
  if (!user_share_ptr) {
    return result::fail(fmt::format("Unknown user '{}'", user));
  }

  if (task->uid != 0) {
    auto partition_it = user_share_ptr->account_to_attrs_map.at(account)
                            .allowed_partition_qos_map.find(task->partition_id);
    if (partition_it == user_share_ptr->account_to_attrs_map.at(account)
                            .allowed_partition_qos_map.end())
      return result::fail("Partition is not allowed for this user.");

    if (task->qos.empty()) {
      // Default qos
      task->qos = partition_it->second.first;
      if (task->qos.empty())
        return result::fail(
            fmt::format("The user '{}' has no QOS available for this partition "
                        "'{}' to be used",
                        task->Username(), task->partition_id));
    } else {
      // Check whether task.qos in the qos list
      if (std::find(partition_it->second.second.begin(),
                    partition_it->second.second.end(),
                    task->qos) == partition_it->second.second.end()) {
        return result::fail(fmt::format(
            "The qos '{}' you set is not in partition's allowed qos list",
            task->qos));
      }
    }
  } else {
    if (task->qos.empty()) {
      task->qos = kUnlimitedQosName;
    }
  }

  const Qos* qos_share_ptr = GetExistedQosInfoNoLock_(task->qos);
  if (!qos_share_ptr)
    return result::fail(fmt::format("Unknown QOS '{}'", task->qos));

  task->qos_priority = qos_share_ptr->priority;

  if (task->time_limit >= absl::Seconds(kTaskMaxTimeLimitSec)) {
    task->time_limit = qos_share_ptr->max_time_limit_per_task;
  } else if (task->time_limit > qos_share_ptr->max_time_limit_per_task)
    return result::fail("time-limit reached the user's limit.");

  if (static_cast<double>(task->cpus_per_task) >
      qos_share_ptr->max_cpus_per_user)
    return result::fail("cpus-per-task reached the user's limit.");

  return {};
}

AccountManager::Result AccountManager::FindUserLevelAccountsOfUid(
    uint32_t uid, User::AdminLevel* level, std::list<std::string>* accounts) {
  PasswordEntry entry(uid);
  if (!entry.Valid()) {
    return Result{false, fmt::format("Uid {} not found.", uid)};
  }

  UserMutexSharedPtr ptr = GetExistedUserInfo(entry.Username());
  if (!ptr) {
    return Result{false, fmt::format("User {} is not a user of Crane.",
                                     entry.Username())};
  }
  if (level != nullptr) *level = ptr->admin_level;
  if (accounts != nullptr) {
    for (const auto& [acct, item] : ptr->account_to_attrs_map) {
      accounts->emplace_back(acct);
    }
  }

  return Result{true};
}

result::result<void, std::string> AccountManager::CheckUidIsAdmin(
    uint32_t uid) {
  PasswordEntry entry(uid);
  if (!entry.Valid()) {
    return result::failure(fmt::format("Uid {} not found.", uid));
  }

  util::read_lock_guard user_guard(m_rw_user_mutex_);
  const User* ptr = GetExistedUserInfoNoLock_(entry.Username());
  if (!ptr) {
    return result::failure(
        fmt::format("User {} is not a user of Crane.", entry.Username()));
  }

  if (ptr->admin_level >= User::Operator) return {};

  return result::failure(
      fmt::format("User {} has insufficient privilege.", entry.Username()));
}

AccountManager::Result AccountManager::CheckAddUserAllowedPartition(
    const User& user, const Account& account,
    const std::string& partition) {
  const std::string name = user.name;
  const std::string account_name = account.name;

  if (!user.account_to_attrs_map.contains(account_name)) {
    return Result{false,
                  fmt::format("User '{}' doesn't belong to account '{}' ", name,
                              account_name)};
  }

  if (!g_config.Partitions.contains(partition)) {
    return Result{false, fmt::format("Partition '{}' not existed", partition)};
  }

  if (std::find(account.allowed_partition.begin(),
                account.allowed_partition.end(),
                partition) == account.allowed_partition.end()) {
    return Result{false, fmt::format("User '{}''s account '{}' is not allowed "
                                     "to use the partition '{}'",
                                     name, account_name, partition)};
  }

  if (user.account_to_attrs_map.at(account_name).allowed_partition_qos_map.contains(
          partition)) {
    return Result{
        false, fmt::format("The partition '{}' is already in "
                           "user '{}''s allowed partition under account '{}'",
                           partition, name, account_name)};
  }

  return Result{true};
}

AccountManager::Result AccountManager::CheckAddUserAllowedQos(
    const User& user, const Account& account, const std::string& partition,
    const std::string& qos) {
  const std::string name = user.name;
  const std::string account_name = account.name;


  if (!user.account_to_attrs_map.contains(account_name)) {
    return Result{false, fmt::format("User '{}' doesn't belong to account '{}'",
                                     name, account_name)};
  }

  // check if the qos existed
  if (!GetExistedQosInfoNoLock_(qos)) {
    return Result{false, fmt::format("Qos '{}' not existed", qos)};
  }

  // check if account has access to new qos
  if (std::find(account.allowed_qos_list.begin(),
                account.allowed_qos_list.end(),
                qos) == account.allowed_qos_list.end()) {
    return Result{
        false,
        fmt::format("Sorry, account '{}' is not allowed to use the qos '{}'",
                    account_name, qos)};
  }

  // check if add item already the user's allowed qos
  if (partition.empty()) {
    bool is_allowed = false;
    for (auto& [par, pair] :
         user.account_to_attrs_map.at(account_name).allowed_partition_qos_map) {
      const std::list<std::string>& list = pair.second;
      if (std::find(list.begin(), list.end(), qos) == list.end()) {
        is_allowed = true;
        break;
      }
    }
    if (!is_allowed) {
      return Result{false, fmt::format("Qos '{}' is already in user '{}''s "
                                       "allowed qos of all partition",
                                       qos, name)};
    }
  } else {
    auto iter =
        user.account_to_attrs_map.at(account_name).allowed_partition_qos_map.find(
            partition);
    if (iter == user.account_to_attrs_map.at(account_name)
                    .allowed_partition_qos_map.end()) {
      return Result{
          false,
          fmt::format("Partition '{}' is not in user '{}''s allowed partition",
                      partition, name)};
    }
    const std::list<std::string>& list = iter->second.second;
    if (std::find(list.begin(), list.end(), qos) != list.end()) {
      return Result{false, fmt::format("Qos '{}' is already in user '{}''s "
                                       "allowed qos of partition '{}'",
                                       qos, name, partition)};
    }
  }

  return Result{true};
}

AccountManager::Result AccountManager::CheckSetUserAdminLevel(
    const User& user, const std::string& level, User::AdminLevel* new_level) {
  const std::string name = user.name;

  if (level == "none") {
    *new_level = User::None;
  } else if (level == "operator") {
    *new_level = User::Operator;
  } else if (level == "admin") {
    *new_level = User::Admin;
  } else {
    return Result{false, fmt::format("Unknown admin level '{}'", level)};
  }

  if (*new_level == user.admin_level) {
    return Result{false,
                  fmt::format("User '{}' is already a {} role", name, level)};
  }

  return Result{true};
}

AccountManager::Result AccountManager::CheckSetUserDefaultQos(
    const User& user, const std::string& account, const std::string& partition,
    const std::string& qos) {
  const std::string name = user.name;

  if (!user.account_to_attrs_map.contains(account)) {
    return Result{false, fmt::format("User '{}' doesn't belong to account '{}'",
                                     name, account)};
  }

  if (partition.empty()) {
    bool is_allowed = false;
    for (auto& [par, pair] :
         user.account_to_attrs_map.at(account).allowed_partition_qos_map) {
      if (std::find(pair.second.begin(), pair.second.end(), qos) !=
              pair.second.end() &&
          qos != pair.first) {
        is_allowed = true;
        break;
      }
    }

    if (!is_allowed) {
      return Result{false, fmt::format("Qos '{}' not in allowed qos list "
                                       "or is already the default qos",
                                       qos)};
    }
  } else {
    auto iter =
        user.account_to_attrs_map.at(account).allowed_partition_qos_map.find(
            partition);
    if (iter == user.account_to_attrs_map.at(account)
                    .allowed_partition_qos_map.end()) {
      return Result{
          false,
          fmt::format("Partition '{}' is not in user '{}''s allowed partition",
                      partition, name)};
    }

    if (std::find(iter->second.second.begin(), iter->second.second.end(),
                  qos) == iter->second.second.end()) {
      return Result{false,
                    fmt::format("Qos '{}' not in allowed qos list", qos)};
    }

    if (iter->second.first == qos) {
      return Result{false,
                    fmt::format("Qos '{}' is already the default qos", qos)};
    }
  }

  return Result{true};
}

AccountManager::Result AccountManager::CheckSetUserAllowedPartition(const User& user,
                                    const Account& account,
                                    const std::string& partitions) {
  const std::string name = user.name;
  const std::string account_name = account.name;

  std::vector<std::string> partition_vec =
      absl::StrSplit(partitions, ',', absl::SkipEmpty());

  if (!user.account_to_attrs_map.contains(account_name)) {
    return Result{false, fmt::format("User '{}' doesn't belong to account '{}'",
                                     name, account_name)};
  }

  for (const auto& par : partition_vec) {
    // check if partition existed
    if (!g_config.Partitions.contains(par)) {
      return Result{false, fmt::format("Partition '{}' not existed", par)};
    }
    // check if account has access to new partition
    if (std::find(account.allowed_partition.begin(),
                  account.allowed_partition.end(),
                  par) == account.allowed_partition.end()) {
      return Result{false,
                    fmt::format("User '{}''s account '{}' is not allowed "
                                "to use the partition '{}'",
                                name, user.default_account, par)};
    }
  }

  return Result{true};
}

AccountManager::Result AccountManager::CheckSetUserAllowedQos(const User& user, const Account& account,const std::string& partition,
                              const std::string& qos_list_str, bool force) {
  const std::string name = user.name;
  const std::string account_name = account.name;


  if (!user.account_to_attrs_map.contains(account_name)) {
    return Result{false, fmt::format("User '{}' doesn't belong to account '{}'",
                                     name, account_name)};
  }

  std::vector<std::string> qos_vec =
      absl::StrSplit(qos_list_str, ',', absl::SkipEmpty());

  for (const auto& qos : qos_vec) {
    // check if qos existed
    if (!GetExistedQosInfoNoLock_(qos)) {
      return Result{false, fmt::format("Qos '{}' not existed", qos)};
    }
    // check if account has access to new qos
    if (std::find(account.allowed_qos_list.begin(),
                  account.allowed_qos_list.end(),
                  qos) == account.allowed_qos_list.end()) {
      return Result{
          false,
          fmt::format("Sorry, account '{}' is not allowed to use the qos '{}'",
                      account_name, qos)};
    }
  }

  std::unordered_map<std::string,
                       std::pair<std::string, std::list<std::string>>>
        cache_allowed_partition_qos_map;
  if (partition.empty()) {
      cache_allowed_partition_qos_map =
          user.account_to_attrs_map.at(account_name).allowed_partition_qos_map;
  } else {
      auto iter =
          user.account_to_attrs_map.at(account_name).allowed_partition_qos_map.find(
              partition);
      if (iter ==
          user.account_to_attrs_map.at(account_name).allowed_partition_qos_map.end()) {
        return Result{
            false, fmt::format(
                       "Partition '{}' is not in user '{}''s allowed partition",
                       partition, name)};
      }
      cache_allowed_partition_qos_map.insert({iter->first, iter->second});
  }

  for (auto& [par, pair] : cache_allowed_partition_qos_map) {
    if (std::find(qos_vec.begin(), qos_vec.end(), pair.first) == qos_vec.end()) {
      if (!force && !pair.first.empty()) {
        return Result{
            false,
            fmt::format("Qos '{}' is default qos of partition '{}',but not "
                        "found in new qos list.Ignoring this constraint with "
                        "forced operation, the default qos is randomly "
                        "replaced with one of the items in the new qos list",
                        pair.first, par)};
      }
    }
  }

  return Result{true};
}

AccountManager::Result AccountManager::CheckDeleteUserAllowedPartition(const User& user,
                                         const std::string& account,
                                         const std::string& partition) {
  const std::string name = user.name;

  if (!user.account_to_attrs_map.contains(account)) {
    return Result{false, fmt::format("User '{}' doesn't belong to account '{}'",
                                     name, account)};
  }

  if (!user.account_to_attrs_map.at(account)
           .allowed_partition_qos_map.contains(partition)) {
    return Result{
        false,
        fmt::format(
            "Partition '{}' is not in user '{}''s allowed partition list",
            partition, name)};
  }

  return Result{true};
}

AccountManager::Result AccountManager::CheckDeleteUserAllowedQos(const User& user, const std::string& account, const std::string& partition,
                                   const std::string& qos, bool force) {
  
  const std::string name = user.name;
  
  if (!user.account_to_attrs_map.contains(account)) {
    return Result{false, fmt::format("User '{}' doesn't belong to account '{}'",
                                     name, account)};
  }

  if (partition.empty()) {
    bool is_allowed = false;
    for (auto& [par, pair] : user.account_to_attrs_map.at(account).allowed_partition_qos_map) {
      if (std::find(pair.second.begin(), pair.second.end(), qos) != pair.second.end()) {
        is_allowed = true;
        if (pair.first == qos && !force) {
            return Result{
                false, fmt::format(
                           "Qos '{}' is default qos of partition '{}'.Ignoring "
                           "this constraint with forced deletion, the deleted "
                           "default qos is randomly replaced with one of the "
                           "remaining items in the qos list",
                           qos, par)};
        }
      }
      if (!is_allowed) {
        return Result{
            false, fmt::format(
                      "Qos '{}' not in allowed qos list of all partition", qos)};
      }
    }
  } else {
    // Delete the qos of a specified partition
    auto iter =
        user.account_to_attrs_map.at(account).allowed_partition_qos_map.find(
            partition);

    if (iter ==
        user.account_to_attrs_map.at(account).allowed_partition_qos_map.end()) {
      return Result{false,
                    fmt::format("Partition '{}' not in allowed partition list",
                                partition)};
    }

    if (std::find(iter->second.second.begin(), iter->second.second.end(),
                  qos) == iter->second.second.end()) {
      return Result{
          false,
          fmt::format("Qos '{}' not in allowed qos list of partition '{}'", qos,
                      partition)};
    }

    if (qos == iter->second.first && !force) {
      return Result{
            false,
            fmt::format("Qos '{}' is default qos of partition '{}'.Ignoring "
                        "this constraint with forced deletion, the deleted "
                        "default qos is randomly replaced with one of the "
                        "remaining items in the qos list",
                        qos, partition)};
    }
  }

  return Result{true};
}

AccountManager::Result AccountManager::HasPermissionToAccount(
    uint32_t uid, const std::string& account, bool read_only_priv,
    User::AdminLevel* level_of_uid) {
  PasswordEntry entry(uid);
  if (!entry.Valid()) {
    return Result{false, fmt::format("Uid {} not existed", uid)};
  }

  util::read_lock_guard user_guard(m_rw_user_mutex_);
  util::read_lock_guard account_guard(m_rw_account_mutex_);

  const User* user = GetExistedUserInfoNoLock_(entry.Username());
  if (!user) {
    return Result{false, fmt::format("User '{}' is not a user of Crane",
                                     entry.Username())};
  }

  if (level_of_uid != nullptr) *level_of_uid = user->admin_level;

  if (user->admin_level == User::None) {
    if (account.empty())
      return Result{false, fmt::format("No account is specified for user {}",
                                       entry.Username())};

    if (read_only_priv) {
      // In current implementation, if a user is the coordinator of an
      // account, it must exist in user->account_to_attrs_map.
      // This is guaranteed by the procedure of adding coordinator, where the
      // coordinator is specified only when adding a user to an account.
      // Thus, user->account_to_attrs_map must cover all the accounts he
      // coordinates, and it's ok to skip checking user->coordinator_accounts.
      for (const auto& [acc, item] : user->account_to_attrs_map)
        if (acc == account || PaternityTestNoLock_(acc, account))
          return Result{true};
    } else {
      for (const auto& acc : user->coordinator_accounts)
        if (acc == account || PaternityTestNoLock_(acc, account))
          return Result{true};
    }

    return Result{
        false,
        fmt::format("User {} is not allowed to access"
                    "account {} out of the subtree of his permitted accounts.",
                    entry.Username(), account)};
  }

  return Result{true};
}

AccountManager::Result AccountManager::HasPermissionToUser(
    uint32_t uid, const std::string& target_user, bool read_only_priv,
    User::AdminLevel* level_of_uid) {
  PasswordEntry source_user_entry(uid);
  if (!source_user_entry.Valid()) {
    return Result{false, fmt::format("Uid {} not existed", uid)};
  }

  util::read_lock_guard user_guard(m_rw_user_mutex_);
  util::read_lock_guard account_guard(m_rw_account_mutex_);

  const User* source_user_ptr =
      GetExistedUserInfoNoLock_(source_user_entry.Username());
  if (!source_user_ptr)
    return Result{false, fmt::format("User {} is not a user of Crane",
                                     source_user_entry.Username())};

  const User* target_user_ptr = GetExistedUserInfoNoLock_(target_user);
  if (!target_user_ptr)
    return Result{false,
                  fmt::format("User {} is not a user of Crane", target_user)};

  if (level_of_uid != nullptr) *level_of_uid = source_user_ptr->admin_level;

  if (source_user_ptr->admin_level != User::None ||
      target_user == source_user_entry.Username())
    return Result{true};

  std::vector<std::string> accounts_under_permission_vec;
  if (read_only_priv)
    for (const auto& [acc, acct_item] : source_user_ptr->account_to_attrs_map)
      accounts_under_permission_vec.emplace_back(acc);
  else
    for (const auto& acc : source_user_ptr->coordinator_accounts)
      accounts_under_permission_vec.emplace_back(acc);

  for (const auto& [target_user_acc, item] :
       target_user_ptr->account_to_attrs_map) {
    for (const auto& acc_under_perm : accounts_under_permission_vec)
      if (acc_under_perm == target_user_acc ||
          PaternityTestNoLock_(acc_under_perm, target_user_acc))
        return Result{true};
  }

  return Result{false, fmt::format("User {} is not permitted to access user {} "
                                   "out of subtrees of his permitted accounts",
                                   source_user_entry.Username(), target_user)};
}

void AccountManager::InitDataMap_() {
  util::write_lock_guard user_guard(m_rw_user_mutex_);
  util::write_lock_guard account_guard(m_rw_account_mutex_);
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);

  std::list<User> user_list;
  g_db_client->SelectAllUser(&user_list);
  for (auto& user : user_list) {
    m_user_map_[user.name] = std::make_unique<User>(user);
  }

  std::list<Account> account_list;
  g_db_client->SelectAllAccount(&account_list);
  for (auto& account : account_list) {
    m_account_map_[account.name] = std::make_unique<Account>(account);
  }

  std::list<Qos> qos_list;
  g_db_client->SelectAllQos(&qos_list);
  for (auto& qos : qos_list) {
    m_qos_map_[qos.name] = std::make_unique<Qos>(qos);
  }
}

/**
 *
 * @param name
 * @note copy user info from m_user_map_
 * @return bool
 */
const User* AccountManager::GetUserInfoNoLock_(const std::string& name) {
  auto find_res = m_user_map_.find(name);
  if (find_res == m_user_map_.end()) {
    return nullptr;
  } else {
    return find_res->second.get();
  }
}

/*
 * Get the user info form mongodb and deletion flag marked false
 */
const User* AccountManager::GetExistedUserInfoNoLock_(const std::string& name) {
  const User* user = GetUserInfoNoLock_(name);
  if (user && !user->deleted) {
    return user;
  } else {
    return nullptr;
  }
}

const Account* AccountManager::GetAccountInfoNoLock_(const std::string& name) {
  auto find_res = m_account_map_.find(name);
  if (find_res == m_account_map_.end()) {
    return nullptr;
  } else {
    return find_res->second.get();
  }
}

const Account* AccountManager::GetExistedAccountInfoNoLock_(
    const std::string& name) {
  const Account* account = GetAccountInfoNoLock_(name);
  if (account && !account->deleted) {
    return account;
  } else {
    return nullptr;
  }
}

const Qos* AccountManager::GetQosInfoNoLock_(const std::string& name) {
  auto find_res = m_qos_map_.find(name);
  if (find_res == m_qos_map_.end()) {
    return nullptr;
  } else {
    return find_res->second.get();
  }
}

const Qos* AccountManager::GetExistedQosInfoNoLock_(const std::string& name) {
  const Qos* qos = GetQosInfoNoLock_(name);
  if (qos && !qos->deleted) {
    return qos;
  } else {
    return nullptr;
  }
}

bool AccountManager::IncQosReferenceCountInDb_(const std::string& name,
                                               int num) {
  return g_db_client->UpdateEntityOne(MongodbClient::EntityType::QOS, "$inc",
                                      name, "reference_count", num);
}

AccountManager::Result AccountManager::AddUserAllowedPartition_(
    const User& user, const Account& account,
    const std::string& partition) {
  const std::string name = user.name;
  const std::string account_name = account.name;

  User res_user(user);

  // Update the map
  res_user.account_to_attrs_map[account_name].allowed_partition_qos_map[partition] =
      std::pair<std::string, std::list<std::string>>{
          account.default_qos,
          std::list<std::string>{account.allowed_qos_list}};

  // Update to database
  if (!g_db_client->UpdateUser(res_user)) {
    return Result{false, "Fail to update data in database"};
  }

  m_user_map_[name]->account_to_attrs_map[account_name].allowed_partition_qos_map =
      res_user.account_to_attrs_map[account_name].allowed_partition_qos_map;

  return Result{true};
}

AccountManager::Result AccountManager::AddUserAllowedQos_(
    const User& user, const Account& account,
    const std::string& partition, const std::string& qos) {
  const std::string name = user.name;
  const std::string account_name = account.name;

  User res_user(user);

  if (partition.empty()) {
    // add to all partition
    for (auto& [par, pair] :
         res_user.account_to_attrs_map[account_name].allowed_partition_qos_map) {
      std::list<std::string>& list = pair.second;
      if (std::find(list.begin(), list.end(), qos) == list.end()) {
        if (pair.first.empty()) {
          pair.first = qos;
        }
        list.emplace_back(qos);
      }
    }
  } else {
    // add to exacted partition
    auto iter =
        res_user.account_to_attrs_map[account_name].allowed_partition_qos_map.find(
            partition);
    std::list<std::string>& list = iter->second.second;
    if (iter->second.first.empty()) {
      iter->second.first = qos;
    }
    list.push_back(qos);
  }

  // Update to database
  if (!g_db_client->UpdateUser(res_user)) {
    return Result{false, "Fail to update data in database"};
  }

  m_user_map_[name]->account_to_attrs_map[account_name].allowed_partition_qos_map =
      res_user.account_to_attrs_map[account_name].allowed_partition_qos_map;

  return Result{true};
}

AccountManager::Result AccountManager::SetUserAdminLevel_(
    const std::string& name, const User::AdminLevel& new_level) {
  // Update to database
  if (!g_db_client->UpdateEntityOne(MongodbClient::EntityType::USER, "$set",
                                    name, "admin_level",
                                    static_cast<int>(new_level))) {
    return Result{false, "Fail to update data in database"};
  }

  m_user_map_[name]->admin_level = new_level;

  return Result{true};
}

AccountManager::Result AccountManager::SetUserDefaultQos_(
    const User& user, const std::string& account,
    const std::string& partition, const std::string& qos) {
  const std::string name = user.name;

  User res_user(user);

  if (partition.empty()) {
    for (auto& [par, pair] :
         res_user.account_to_attrs_map[account].allowed_partition_qos_map) {
      if (std::find(pair.second.begin(), pair.second.end(), qos) !=
              pair.second.end() &&
          qos != pair.first) {
        pair.first = qos;
      }
    }
  } else {
    auto iter =
        res_user.account_to_attrs_map[account].allowed_partition_qos_map.find(
            partition);

    iter->second.first = qos;
  }

  // Update to database
  if (!g_db_client->UpdateUser(res_user)) {
    return Result{false, "Fail to update data in database"};
  }

  m_user_map_[name]->account_to_attrs_map[account].allowed_partition_qos_map =
      res_user.account_to_attrs_map[account].allowed_partition_qos_map;

  return Result{true};
}

AccountManager::Result AccountManager::SetUserAllowedPartition_(
    const User& user, const Account& account,
    const std::string& partitions) {
  const std::string name = user.name;
  const std::string account_name = account.name;

  std::vector<std::string> partition_vec =
      absl::StrSplit(partitions, ',', absl::SkipEmpty());

  User res_user(user);
  // Update the map
  res_user.account_to_attrs_map[account_name]
      .allowed_partition_qos_map.clear();  // clear the partitions
  for (const auto& par : partition_vec) {
    res_user.account_to_attrs_map[account_name].allowed_partition_qos_map[par] =
        std::pair<std::string, std::list<std::string>>{
            account.default_qos,
            std::list<std::string>{account.allowed_qos_list}};
  }

  // Update to database
  if (!g_db_client->UpdateUser(res_user)) {
    return Result{false, "Fail to update data in database"};
  }

  m_user_map_[name]->account_to_attrs_map[account_name].allowed_partition_qos_map =
      res_user.account_to_attrs_map[account_name].allowed_partition_qos_map;

  return Result{true};
}

AccountManager::Result AccountManager::SetUserAllowedQos_(
    const User& user, const Account& account,
    const std::string& partition, const std::string& qos_list_str, bool force) {
  
  const std::string name = user.name;
  const std::string account_name = account.name;

  std::vector<std::string> qos_vec =
      absl::StrSplit(qos_list_str, ',', absl::SkipEmpty());

  User res_user(user);
  if (partition.empty()) {
    // Set the qos of all partition
    for (auto& [par, pair] :
         res_user.account_to_attrs_map[account_name].allowed_partition_qos_map) {
      if (std::find(qos_vec.begin(), qos_vec.end(), pair.first) ==
          qos_vec.end()) {
          pair.first = qos_vec.empty() ? "" : qos_vec.front();
      }
      pair.second.assign(qos_vec.begin(), qos_vec.end());
    }
  } else {
    // Set the qos of a specified partition
    auto iter =
        res_user.account_to_attrs_map[account_name].allowed_partition_qos_map.find(
            partition);

    if (std::find(qos_vec.begin(), qos_vec.end(), iter->second.first) ==
        qos_vec.end()) {
        iter->second.first = qos_vec.empty() ? "" : qos_vec.front();
    }
    iter->second.second.assign(qos_vec.begin(), qos_vec.end());
  }

  // Update to database
  if (!g_db_client->UpdateUser(res_user)) {
    return Result{false, "Fail to update data in database"};
  }

  m_user_map_[name]->account_to_attrs_map[account_name].allowed_partition_qos_map =
      res_user.account_to_attrs_map[account_name].allowed_partition_qos_map;

  return Result{true};
}

AccountManager::Result AccountManager::DeleteUserAllowedPartition_(
    const User& user, const std::string& account,
    const std::string& partition) {
  
  const std::string name = user.name;

  // Update to database
  if (!g_db_client->UpdateEntityOne(
          Ctld::MongodbClient::EntityType::USER, "$unset", name,
          "account_to_attrs_map." + account + ".allowed_partition_qos_map." +
              partition,
          std::string(""))) {
    return Result{false, "Fail to update data in database"};
  }

  m_user_map_[name]
      ->account_to_attrs_map[account]
      .allowed_partition_qos_map.erase(partition);

  return Result{true};
}

AccountManager::Result AccountManager::DeleteUserAllowedQos_(
    const User& user, const std::string& qos, const std::string& account,
    const std::string& partition, bool force) {

  const std::string name = user.name;
  
  User res_user(user);

  if (partition.empty()) {
    // Delete the qos of all partition
    for (auto& [par, pair] :
         res_user.account_to_attrs_map[account].allowed_partition_qos_map) {
      if (std::find(pair.second.begin(), pair.second.end(), qos) !=
          pair.second.end()) {
        pair.second.remove(qos);
        if (pair.first == qos) {
            pair.first = pair.second.empty() ? "" : pair.second.front();
        }
      }
    }
  } else {
    // Delete the qos of a specified partition
    auto iter =
        res_user.account_to_attrs_map[account].allowed_partition_qos_map.find(
            partition);

    iter->second.second.remove(qos);

    if (qos == iter->second.first) {
      iter->second.first =
            iter->second.second.empty() ? "" : iter->second.second.front();
    }
  }

  // Update to database
  if (!g_db_client->UpdateUser(res_user)) {
    return Result{false, "Fail to update data in database"};
  }

  m_user_map_[name]->account_to_attrs_map[account].allowed_partition_qos_map =
      res_user.account_to_attrs_map[account].allowed_partition_qos_map;

  return Result{true};
}

AccountManager::Result AccountManager::AddUser_(const User* find_user,
                                                const Account* find_account,
                                                User&& new_user) {
  const std::string object_account = new_user.default_account;
  const std::string name = new_user.name;

  bool add_coordinator = false;
  if (!new_user.coordinator_accounts.empty()) {
    add_coordinator = true;
  }

  User res_user;
  if (find_user && !find_user->deleted) {
    res_user = *find_user;
    res_user.account_to_attrs_map[object_account] =
        new_user.account_to_attrs_map[object_account];
    if (add_coordinator) {
      res_user.coordinator_accounts.push_back(object_account);
    }
  } else {
    res_user = std::move(new_user);
  }

  const std::list<std::string>& parent_allowed_partition =
      find_account->allowed_partition;
  if (!res_user.account_to_attrs_map[object_account]
           .allowed_partition_qos_map.empty()) {
    for (auto&& [partition, qos] : res_user.account_to_attrs_map[object_account]
                                       .allowed_partition_qos_map) {
      qos.first = find_account->default_qos;
      qos.second = find_account->allowed_qos_list;
    }
  } else {
    // Inherit
    for (const auto& partition : parent_allowed_partition) {
      res_user.account_to_attrs_map[object_account]
          .allowed_partition_qos_map[partition] =
          std::pair<std::string, std::list<std::string>>{
              find_account->default_qos,
              std::list<std::string>{find_account->allowed_qos_list}};
    }
  }
  res_user.account_to_attrs_map[object_account].blocked = false;

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        // Update the user's account
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                     "$addToSet", object_account, "users",
                                     name);
        if (add_coordinator) {
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$addToSet", object_account,
                                       "coordinators", name);
        }

        if (find_user) {
          // There is a same user but was deleted or user would like to add user
          // to a new account,here will overwrite it with the same name
          g_db_client->UpdateUser(res_user);
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::USER, "$set",
                                       name, "creation_time",
                                       ToUnixSeconds(absl::Now()));
        } else {
          // Insert the new user
          g_db_client->InsertUser(res_user);
        }
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  m_account_map_[object_account]->users.emplace_back(name);
  if (add_coordinator) {
    m_account_map_[object_account]->coordinators.emplace_back(name);
  }
  m_user_map_[name] = std::make_unique<User>(std::move(res_user));

  return Result{true};
}

AccountManager::Result AccountManager::AddAccount_(const Account* find_account,
                                                   const Account* find_parent,
                                                   Account&& new_account) {
  const std::string name = new_account.name;

  if (find_parent != nullptr) {
    if (new_account.allowed_partition.empty()) {
      // Inherit
      new_account.allowed_partition =
          std::list<std::string>{find_parent->allowed_partition};
    }

    if (new_account.allowed_qos_list.empty()) {
      // Inherit
      new_account.allowed_qos_list =
          std::list<std::string>{find_parent->allowed_qos_list};
    }
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        if (!new_account.parent_account.empty()) {
          // update the parent account's child_account_list
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$addToSet", new_account.parent_account,
                                       "child_accounts", name);
        }

        if (find_account) {
          // There is a same account but was deleted,here will delete the
          // original account and overwrite it with the same name
          g_db_client->UpdateAccount(new_account);
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$set", name, "creation_time",
                                       ToUnixSeconds(absl::Now()));
        } else {
          // Insert the new account
          g_db_client->InsertAccount(new_account);
        }
        for (const auto& qos : new_account.allowed_qos_list) {
          IncQosReferenceCountInDb_(qos, 1);
        }
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }
  if (!new_account.parent_account.empty()) {
    m_account_map_[new_account.parent_account]->child_accounts.emplace_back(
        name);
  }
  for (const auto& qos : new_account.allowed_qos_list) {
    m_qos_map_[qos]->reference_count++;
  }
  m_account_map_[name] = std::make_unique<Account>(std::move(new_account));

  return Result{true};
}

AccountManager::Result AccountManager::AddQos_(const Qos* find_qos,
                                               const Qos& new_qos) {
  if (find_qos) {
    // There is a same qos but was deleted,here will delete the original
    // qos and overwrite it with the same name
    mongocxx::client_session::with_transaction_cb callback =
        [&](mongocxx::client_session* session) {
          g_db_client->UpdateQos(new_qos);
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::QOS, "$set",
                                       new_qos.name, "creation_time",
                                       ToUnixSeconds(absl::Now()));
        };

    if (!g_db_client->CommitTransaction(callback)) {
      return Result{false, "Can't update the deleted qos to database"};
    }
  } else {
    // Insert the new qos
    if (!g_db_client->InsertQos(new_qos)) {
      return Result{false, "Can't insert the new qos to database"};
    }
  }

  m_qos_map_[new_qos.name] = std::make_unique<Qos>(new_qos);

  return Result{true};
}

AccountManager::Result AccountManager::AddAccountAllowedPartition_(
    const std::string& name, const std::string& partition) {
  util::write_lock_guard account_guard(m_rw_account_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    return Result{false, fmt::format("Unknown account '{}'", name)};
  }

  // check if the partition existed
  if (g_config.Partitions.find(partition) == g_config.Partitions.end()) {
    return Result{false, fmt::format("Partition '{}' not existed", partition)};
  }
  // Check if parent account has access to the partition
  if (!account->parent_account.empty()) {
    const Account* parent =
        GetExistedAccountInfoNoLock_(account->parent_account);
    if (std::find(parent->allowed_partition.begin(),
                  parent->allowed_partition.end(),
                  partition) == parent->allowed_partition.end()) {
      return Result{false, fmt::format("Parent account '{}' does not "
                                       "have access to partition '{}'",
                                       account->parent_account, partition)};
    }
  }

  if (std::find(account->allowed_partition.begin(),
                account->allowed_partition.end(),
                partition) != account->allowed_partition.end()) {
    return Result{
        false,
        fmt::format("Partition '{}' is already in allowed partition list",
                    partition)};
  }

  if (!g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                    "$addToSet", name, "allowed_partition",
                                    partition)) {
    return Result{false, "Can't update the  database"};
  }
  m_account_map_[name]->allowed_partition.emplace_back(partition);

  return Result{true};
}

AccountManager::Result AccountManager::AddAccountAllowedQos_(
    const std::string& name, const std::string& qos) {
  util::write_lock_guard account_guard(m_rw_account_mutex_);
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    return Result{false, fmt::format("Unknown account '{}'", name)};
  }

  // check if the qos existed
  if (!GetExistedQosInfoNoLock_(qos)) {
    return Result{false, fmt::format("Qos '{}' not existed", qos)};
  }

  // Check if parent account has access to the qos
  if (!account->parent_account.empty()) {
    const Account* parent =
        GetExistedAccountInfoNoLock_(account->parent_account);

    if (std::find(parent->allowed_qos_list.begin(),
                  parent->allowed_qos_list.end(),
                  qos) == parent->allowed_qos_list.end()) {
      return Result{
          false,
          fmt::format("Parent account '{}' does not have access to qos '{}'",
                      account->parent_account, qos)};
    }
  }

  if (std::find(account->allowed_qos_list.begin(),
                account->allowed_qos_list.end(),
                qos) != account->allowed_qos_list.end()) {
    return Result{false,
                  fmt::format("Qos '{}' is already in allowed qos list", qos)};
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        if (account->default_qos.empty()) {
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$set", name, "default_qos", qos);
        }
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                     "$addToSet", name, "allowed_qos_list",
                                     qos);
        IncQosReferenceCountInDb_(qos, 1);
      };

  // Update to database
  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  if (account->default_qos.empty()) {
    m_account_map_[name]->default_qos = qos;
  }
  m_account_map_[name]->allowed_qos_list.emplace_back(qos);
  m_qos_map_[qos]->reference_count++;

  return Result{true};
}

AccountManager::Result AccountManager::SetAccountDescription_(
    const std::string& name, const std::string& description) {
  util::write_lock_guard account_guard(m_rw_account_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    return Result{false, fmt::format("Unknown account '{}'", name)};
  }

  if (description == account->description) {
    return Result{false, "Description content not change"};
  }

  if (!g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT, "$set",
                                    name, "description", description)) {
    return Result{false, "Can't update the  database"};
  }

  m_account_map_[name]->description = description;

  return Result{true};
}

AccountManager::Result AccountManager::SetAccountDefaultQos_(
    const std::string& name, const std::string& qos) {
  util::write_lock_guard account_guard(m_rw_account_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    return Result{false, fmt::format("Unknown account '{}'", name)};
  }

  if (account->default_qos == qos) {
    return Result{false,
                  fmt::format("Qos '{}' is already the default qos", qos)};
  }

  if (std::find(account->allowed_qos_list.begin(),
                account->allowed_qos_list.end(),
                qos) == account->allowed_qos_list.end()) {
    return Result{false, fmt::format("Qos '{}' not in allowed qos list", qos)};
  }

  if (!g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT, "$set",
                                    name, "default_qos", qos)) {
    return Result{false, "Can't update the database"};
  }
  m_account_map_[name]->default_qos = qos;

  return Result{true};
}

AccountManager::Result AccountManager::SetAccountAllowedPartition_(
    const std::string& name, const std::string& partitions, bool force) {
  std::vector<std::string> partition_vec =
      absl::StrSplit(partitions, ',', absl::SkipEmpty());

  util::write_lock_guard user_guard(m_rw_user_mutex_);
  util::write_lock_guard account_guard(m_rw_account_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    return Result{false, fmt::format("Unknown account '{}'", name)};
  }
  // check if the partition existed
  for (const auto& p : partition_vec) {
    if (!g_config.Partitions.contains(p)) {
      return Result{false, fmt::format("Partition '{}' not existed", p)};
    }
  }

  // Check if parent account has access to the partition
  if (!account->parent_account.empty()) {
    const Account* parent =
        GetExistedAccountInfoNoLock_(account->parent_account);
    for (const auto& p : partition_vec) {
      if (std::find(parent->allowed_partition.begin(),
                    parent->allowed_partition.end(),
                    p) == parent->allowed_partition.end()) {
        return Result{false, fmt::format("Parent account '{}' does not "
                                         "have access to partition '{}'",
                                         account->parent_account, p)};
      }
    }
  }

  std::list<std::string> deleted_partition;
  for (const auto& par : account->allowed_partition) {
    if (std::find(partition_vec.begin(), partition_vec.end(), par) ==
        partition_vec.end()) {
      if (!force && IsAllowedPartitionOfAnyNodeNoLock_(account, par)) {
        return Result{
            false,
            fmt::format("partition '{}' in allowed partition list before is "
                        "used by some descendant node of the account "
                        "'{}'.Ignoring this constraint with forced operation",
                        par, name)};
      }
      deleted_partition.emplace_back(par);
    }
  }

  int add_num = 0;
  for (const auto& par : partition_vec) {
    if (std::find(account->allowed_partition.begin(),
                  account->allowed_partition.end(),
                  par) == account->allowed_partition.end()) {
      add_num++;
    }
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        for (const auto& par : deleted_partition) {
          DeleteAccountAllowedPartitionFromDBNoLock_(account->name, par);
        }

        if (add_num > 0) {
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$set", name, "allowed_partition",
                                       partition_vec);
        }
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  for (const auto& par : deleted_partition) {
    DeleteAccountAllowedPartitionFromMapNoLock_(account->name, par);
  }
  m_account_map_[name]->allowed_partition.assign(partition_vec.begin(),
                                                 partition_vec.end());

  return Result{true};
}

AccountManager::Result AccountManager::SetAccountAllowedQos_(
    const std::string& name, const std::string& qos_list_str, bool force) {
  std::vector<std::string> qos_vec =
      absl::StrSplit(qos_list_str, ',', absl::SkipEmpty());

  util::write_lock_guard user_guard(m_rw_user_mutex_);
  util::write_lock_guard account_guard(m_rw_account_mutex_);
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    return Result{false, fmt::format("Unknown account '{}'", name)};
  }
  // check if the qos existed
  for (const auto& qos : qos_vec) {
    if (!GetExistedQosInfoNoLock_(qos)) {
      return Result{false, fmt::format("Qos '{}' not existed", qos)};
    }
  }

  // Check if parent account has access to the qos
  if (!account->parent_account.empty()) {
    const Account* parent =
        GetExistedAccountInfoNoLock_(account->parent_account);
    for (const auto& qos : qos_vec) {
      if (std::find(parent->allowed_qos_list.begin(),
                    parent->allowed_qos_list.end(),
                    qos) == parent->allowed_qos_list.end()) {
        return Result{
            false,
            fmt::format("Parent account '{}' does not have access to qos '{}'",
                        account->parent_account, qos)};
      }
    }
  }

  std::list<std::string> deleted_qos;
  for (const auto& qos : account->allowed_qos_list) {
    if (std::find(qos_vec.begin(), qos_vec.end(), qos) == qos_vec.end()) {
      if (!force && IsDefaultQosOfAnyNodeNoLock_(account, qos)) {
        return Result{
            false,
            fmt::format("partition '{}' in allowed partition list before is "
                        "used by some descendant node of the account '{}' or "
                        "itself.Ignoring this constraint with forced operation",
                        qos, name)};
      }
      deleted_qos.emplace_back(qos);
    }
  }

  std::list<std::string> add_qos;
  for (const auto& qos : qos_vec) {
    if (std::find(account->allowed_qos_list.begin(),
                  account->allowed_qos_list.end(),
                  qos) == account->allowed_qos_list.end()) {
      add_qos.emplace_back(qos);
    }
  }

  std::list<int> change_num;
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        for (const auto& qos : deleted_qos) {
          int num = DeleteAccountAllowedQosFromDBNoLock_(account->name, qos);
          IncQosReferenceCountInDb_(qos, -num);
          change_num.emplace_back(num);
        }

        if (!add_qos.empty()) {
          Account temp;
          g_db_client->SelectAccount("name", name, &temp);
          if (temp.default_qos.empty()) {
            g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                         "$set", name, "default_qos",
                                         qos_vec.front());
          }

          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$set", name, "allowed_qos_list",
                                       qos_vec);
          for (const auto& qos : add_qos) {
            IncQosReferenceCountInDb_(qos, 1);
          }
        }
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  for (const auto& qos : deleted_qos) {
    DeleteAccountAllowedQosFromMapNoLock_(account->name, qos);
    m_qos_map_[qos]->reference_count -= change_num.front();
    change_num.pop_front();
  }

  if (!add_qos.empty()) {
    if (account->default_qos.empty()) {
      m_account_map_[name]->default_qos = qos_vec.front();
    }
    m_account_map_[name]->allowed_qos_list.assign(qos_vec.begin(),
                                                  qos_vec.end());
    for (const auto& qos : add_qos) {
      m_qos_map_[qos]->reference_count++;
    }
  }

  return Result{true};
}

AccountManager::Result AccountManager::DeleteAccountAllowedPartition_(
    const std::string& name, const std::string& partition, bool force) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);
  util::write_lock_guard account_guard(m_rw_account_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    return Result{false, fmt::format("Unknown account '{}'", name)};
  }

  if (std::find(account->allowed_partition.begin(),
                account->allowed_partition.end(),
                partition) == account->allowed_partition.end()) {
    return Result{
        false,
        fmt::format(
            "Partition '{}' not in allowed partition list of account '{}'",
            partition, name)};
  }

  if (!force && IsAllowedPartitionOfAnyNodeNoLock_(account, partition)) {
    return Result{
        false, fmt::format(
                   "partition '{}' is used by some descendant node of the "
                   "account '{}'.Ignoring this constraint with forced deletion",
                   partition, name)};
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        DeleteAccountAllowedPartitionFromDBNoLock_(account->name, partition);
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  DeleteAccountAllowedPartitionFromMapNoLock_(account->name, partition);

  return Result{true};
}

AccountManager::Result AccountManager::DeleteAccountAllowedQos_(
    const std::string& name, const std::string& qos, bool force) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);
  util::write_lock_guard account_guard(m_rw_account_mutex_);
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    return Result{false, fmt::format("Unknown account '{}'", name)};
  }

  if (std::find(account->allowed_qos_list.begin(),
                account->allowed_qos_list.end(),
                qos) == account->allowed_qos_list.end()) {
    return Result{
        false, fmt::format("Qos '{}' is not in account '{}''s allowed qos list",
                           qos, name)};
  }

  if (!force && IsDefaultQosOfAnyNodeNoLock_(account, qos)) {
    return Result{
        false,
        fmt::format("Someone is using qos '{}' as default qos.Ignoring this "
                    "constraint with forced deletion, the deleted default "
                    "qos is randomly replaced with one of the remaining "
                    "items in the qos list",
                    qos)};
  }

  int change_num;
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        change_num = DeleteAccountAllowedQosFromDBNoLock_(account->name, qos);
        IncQosReferenceCountInDb_(qos, -change_num);
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  DeleteAccountAllowedQosFromMapNoLock_(account->name, qos);
  m_qos_map_[qos]->reference_count -= change_num;

  return Result{true};
}

/**
 * @note need read lock(m_rw_user_mutex_ && m_rw_account_mutex_)
 * @param account
 * @param partition
 * @return
 */
bool AccountManager::IsAllowedPartitionOfAnyNodeNoLock_(
    const Account* account, const std::string& partition, int depth) {
  if (depth > 0 && std::find(account->allowed_partition.begin(),
                             account->allowed_partition.end(),
                             partition) != account->allowed_partition.end()) {
    return true;
  }
  for (const auto& child : account->child_accounts) {
    if (IsAllowedPartitionOfAnyNodeNoLock_(GetExistedAccountInfoNoLock_(child),
                                           partition, depth + 1)) {
      return true;
    }
  }
  for (const auto& user : account->users) {
    const User* p = GetExistedUserInfoNoLock_(user);
    for (const auto& item : p->account_to_attrs_map) {
      if (item.second.allowed_partition_qos_map.contains(partition)) {
        return true;
      }
    }
  }
  return false;
}

/**
 * @note need read lock(m_rw_user_mutex_ && m_rw_account_mutex_)
 * @param account
 * @param qos
 * @return
 */
bool AccountManager::IsDefaultQosOfAnyNodeNoLock_(const Account* account,
                                                  const std::string& qos) {
  if (account->default_qos == qos) {
    return true;
  }
  for (const auto& child : account->child_accounts) {
    if (IsDefaultQosOfAnyNodeNoLock_(GetExistedAccountInfoNoLock_(child),
                                     qos)) {
      return true;
    }
  }

  for (const auto& user : account->users) {
    if (IsDefaultQosOfAnyPartitionNoLock_(GetExistedUserInfoNoLock_(user),
                                          qos)) {
      return true;
    }
  }
  return false;
}

bool AccountManager::IsDefaultQosOfAnyPartitionNoLock_(const User* user,
                                                       const std::string& qos) {
  for (const auto& [account, item] : user->account_to_attrs_map) {
    if (std::any_of(item.allowed_partition_qos_map.begin(),
                    item.allowed_partition_qos_map.end(),
                    [&qos](const auto& p) { return p.second.first == qos; })) {
      return true;
    }
  }
  return false;
}

/**
 * @note need read lock(m_rw_user_mutex_ && m_rw_account_mutex_)
 * @param name
 * @param qos
 * @return
 */
int AccountManager::DeleteAccountAllowedQosFromDBNoLock_(
    const std::string& name, const std::string& qos) {
  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    CRANE_ERROR(
        "Operating on a non-existent account '{}', please check this item in "
        "database and restart cranectld",
        name);
    return false;
  }

  auto iter = std::find(account->allowed_qos_list.begin(),
                        account->allowed_qos_list.end(), qos);
  if (iter == account->allowed_qos_list.end()) {
    return false;
  }

  int change_num = 1;
  for (const auto& child : account->child_accounts) {
    change_num += DeleteAccountAllowedQosFromDBNoLock_(child, qos);
  }

  for (const auto& user : account->users) {
    DeleteUserAllowedQosOfAllPartitionFromDBNoLock_(user, name, qos);
  }
  g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT, "$pull",
                               name, "allowed_qos_list", qos);

  if (account->default_qos == qos) {
    std::list<std::string> temp{account->allowed_qos_list};
    temp.remove(qos);
    std::string new_default = temp.empty() ? "" : temp.front();
    g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT, "$set",
                                 name, "default_qos", new_default);
  }
  return change_num;
}

/**
 * @note need write lock(m_rw_account_mutex_) and write lock(m_rw_user_mutex_)
 * @param name
 * @param qos
 * @return
 */
bool AccountManager::DeleteAccountAllowedQosFromMapNoLock_(
    const std::string& name, const std::string& qos) {
  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    CRANE_ERROR(
        "Operating on a non-existent account '{}', please check this item in "
        "database and restart cranectld",
        name);
    return false;
  }

  if (std::find(account->allowed_qos_list.begin(),
                account->allowed_qos_list.end(),
                qos) == account->allowed_qos_list.end()) {
    return false;
  }

  for (const auto& child : account->child_accounts) {
    DeleteAccountAllowedQosFromMapNoLock_(child, qos);
  }

  for (const auto& user : account->users) {
    DeleteUserAllowedQosOfAllPartitionFromMapNoLock_(user, name, qos);
  }
  m_account_map_[name]->allowed_qos_list.remove(qos);
  if (account->default_qos == qos) {
    m_account_map_[name]->default_qos = account->allowed_qos_list.empty()
                                            ? ""
                                            : account->allowed_qos_list.front();
  }
  return true;
}

/**
 * @note need read lock(m_rw_user_mutex_)
 * @param name
 * @param account
 * @param qos
 * @return
 */
bool AccountManager::DeleteUserAllowedQosOfAllPartitionFromDBNoLock_(
    const std::string& name, const std::string& account,
    const std::string& qos) {
  const User* p = GetExistedUserInfoNoLock_(name);
  if (!p) {
    CRANE_ERROR(
        "Operating on a non-existent user '{}', please check this item in "
        "database and restart cranectld",
        name);
    return false;
  }
  if (!p->account_to_attrs_map.contains(account)) {
    CRANE_ERROR("User '{}' doesn't belong to account '{}'", name, account);
    return false;
  }
  User user(*p);

  for (auto& [par, pair] :
       user.account_to_attrs_map[account].allowed_partition_qos_map) {
    auto iter = std::find(pair.second.begin(), pair.second.end(), qos);
    if (iter != pair.second.end()) {
      pair.second.remove(qos);
    }

    if (pair.first == qos) {
      pair.first = pair.second.empty() ? "" : pair.second.front();
    }
  }
  g_db_client->UpdateUser(user);
  return true;
}

/**
 * @note need write lock(m_rw_user_mutex_)
 * @param name
 * @param account
 * @param qos
 * @return
 */
bool AccountManager::DeleteUserAllowedQosOfAllPartitionFromMapNoLock_(
    const std::string& name, const std::string& account,
    const std::string& qos) {
  const User* p = GetExistedUserInfoNoLock_(name);
  if (!p) {
    CRANE_ERROR(
        "Operating on a non-existent user '{}', please check this item in "
        "database and restart cranectld",
        name);
    return false;
  }
  if (!p->account_to_attrs_map.contains(account)) {
    CRANE_ERROR("User '{}' doesn't belong to account '{}'", name, account);
    return false;
  }
  User user(*p);

  for (auto& [par, qos_pair] :
       user.account_to_attrs_map[account].allowed_partition_qos_map) {
    qos_pair.second.remove(qos);
    if (qos_pair.first == qos) {
      qos_pair.first = qos_pair.second.empty() ? "" : qos_pair.second.front();
    }
  }
  m_user_map_[user.name]
      ->account_to_attrs_map[account]
      .allowed_partition_qos_map =
      user.account_to_attrs_map[account].allowed_partition_qos_map;
  return true;
}

/**
 * @note need read lock(m_rw_account_mutex_)
 * @param name
 * @param partition
 * @return
 */
bool AccountManager::DeleteAccountAllowedPartitionFromDBNoLock_(
    const std::string& name, const std::string& partition) {
  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    CRANE_ERROR(
        "Operating on a non-existent account '{}', please check this item in "
        "database and restart cranectld",
        name);
    return false;
  }

  auto iter = std::find(account->allowed_partition.begin(),
                        account->allowed_partition.end(), partition);
  if (iter == account->allowed_partition.end()) {
    return false;
  }

  for (const auto& child : account->child_accounts) {
    DeleteAccountAllowedPartitionFromDBNoLock_(child, partition);
  }

  for (const auto& user : account->users) {
    g_db_client->UpdateEntityOne(Ctld::MongodbClient::EntityType::USER,
                                 "$unset", user,
                                 "account_to_attrs_map." + name +
                                     ".allowed_partition_qos_map." + partition,
                                 std::string(""));
  }

  g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT, "$pull",
                               account->name, "allowed_partition", partition);
  return true;
}

/**
 * @note need write lock(m_rw_account_mutex_) and write lock(m_rw_user_mutex_)
 * @param name
 * @param partition
 * @return
 */
bool AccountManager::DeleteAccountAllowedPartitionFromMapNoLock_(
    const std::string& name, const std::string& partition) {
  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    CRANE_ERROR(
        "Operating on a non-existent account '{}', please check this item in "
        "database and restart cranectld",
        name);
    return false;
  }

  if (std::find(account->allowed_partition.begin(),
                account->allowed_partition.end(),
                partition) == account->allowed_partition.end()) {
    return false;
  }

  for (const auto& child : account->child_accounts) {
    DeleteAccountAllowedPartitionFromMapNoLock_(child, partition);
  }

  for (const auto& user : account->users) {
    m_user_map_[user]
        ->account_to_attrs_map[name]
        .allowed_partition_qos_map.erase(partition);
  }
  m_account_map_[account->name]->allowed_partition.remove(partition);

  return true;
}

bool AccountManager::PaternityTestNoLock_(const std::string& parent,
                                          const std::string& child) {
  if (parent == child || GetExistedAccountInfoNoLock_(parent) == nullptr ||
      GetExistedAccountInfoNoLock_(child) == nullptr) {
    return false;
  }
  return PaternityTestNoLockDFS_(parent, child);
}

bool AccountManager::PaternityTestNoLockDFS_(const std::string& parent,
                                             const std::string& child) {
  for (const auto& child_of_account : m_account_map_[parent]->child_accounts) {
    if (child_of_account == child ||
        PaternityTestNoLockDFS_(child_of_account, child)) {
      return true;
    }
  }
  return false;
}

}  // namespace Ctld
